# server/tcp_server.py
"""
Dual-protocol TCP server.

Supports two wire protocols on the same port (6379):
  1. RESP2  — Redis wire protocol. redis-cli, redis-py, ioredis work out of the box.
  2. Binary — PulseDB compact binary protocol (used by the internal SDK).

Protocol is auto-detected from the first byte of each connection.

Security:
  Set PULSEDB_REQUIREPASS env var to require AUTH before any command over TCP.
  The HTTP layer uses X-API-Key independently.
"""

import asyncio
import struct
import os
from server.resp import decode_command, encode, encode_simple, encode_error
import ssl
from server.protocol import decode_message, encode_message, TYPE_RESPONSE, TYPE_ERROR
from server.commands import execute
from server.config import REQUIRE_PASS, TCP_HOST, TCP_PORT, TLS_CERT, TLS_KEY


# ---------------------------------------------------------------------------
# Peekable reader — avoids touching asyncio private internals (_buffer hack)
# ---------------------------------------------------------------------------

class _PeekedReader:
    """
    Wraps asyncio.StreamReader and prepends one already-read byte.
    This lets us peek at the first byte for protocol detection without
    touching the private _buffer attribute.
    """
    def __init__(self, reader: asyncio.StreamReader, peeked: bytes):
        self._reader = reader
        self._peeked = peeked   # at most 1 byte

    async def readexactly(self, n: int) -> bytes:
        if self._peeked:
            if n == 1:
                data, self._peeked = self._peeked, b""
                return data
            rest = await self._reader.readexactly(n - 1)
            data, self._peeked = self._peeked + rest, b""
            return data
        return await self._reader.readexactly(n)

    async def readline(self) -> bytes:
        if self._peeked:
            rest = await self._reader.readline()
            data, self._peeked = self._peeked + rest, b""
            return data
        return await self._reader.readline()

    async def read(self, n: int) -> bytes:
        if self._peeked:
            data, self._peeked = self._peeked, b""
            return data
        return await self._reader.read(n)


# ---------------------------------------------------------------------------
# RESP2 command handler
# ---------------------------------------------------------------------------

async def _handle_resp_command(command: str, args: list, authenticated: bool) -> tuple[bytes, bool]:
    """
    Handle one RESP command. Returns (response_bytes, new_authenticated_state).
    AUTH is handled here before anything else.
    """
    # AUTH command — must be processed before any auth gate
    if command == "AUTH":
        if not REQUIRE_PASS:
            return encode_error("ERR Client sent AUTH, but no password is set. "
                                "Did you mean ACL SETUSER with >password?"), True
        password = args[0] if args else ""
        if password == REQUIRE_PASS:
            return encode_simple("OK"), True
        return encode_error("WRONGPASS invalid username-password pair or user is "
                            "disabled."), False

    # Auth gate — reject everything except QUIT if not authenticated
    if REQUIRE_PASS and not authenticated:
        return encode_error("NOAUTH Authentication required."), False

    # PING
    if command == "PING":
        msg = args[0] if args else "PONG"
        return encode_simple(msg), authenticated

    # QUIT
    if command == "QUIT":
        return encode_simple("OK"), authenticated

    # SELECT — single DB only
    if command == "SELECT":
        if args and args[0] != "0":
            return encode_error("PulseDB does not support multiple databases"), authenticated
        return encode_simple("OK"), authenticated

    # COMMAND — minimal compat shim so redis-py doesn't crash on connect
    if command == "COMMAND":
        return b"*0\r\n", authenticated

    # DBSIZE
    if command == "DBSIZE":
        from server.store import store
        count = store.dbsize()
        return f":{count}\r\n".encode(), authenticated

    # FLUSHDB / FLUSHALL
    if command in ("FLUSHDB", "FLUSHALL"):
        from server.store import store
        store.flush()
        return encode_simple("OK"), authenticated

    # All other commands → execute()
    try:
        result = await execute(command, args)
        return encode(result), authenticated
    except Exception as e:
        return encode_error(str(e)), authenticated


# ---------------------------------------------------------------------------
# Protocol detection
# ---------------------------------------------------------------------------

def _is_binary_protocol(first_byte: bytes) -> bool:
    """Binary protocol type bytes are 0x00, 0x01, 0x02; RESP is printable ASCII."""
    return first_byte in (b"\x00", b"\x01", b"\x02")


# ---------------------------------------------------------------------------
# Per-connection handler
# ---------------------------------------------------------------------------

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    address = writer.get_extra_info("peername")
    print(f"[TCP] New connection from {address}")

    try:
        first_byte = await reader.readexactly(1)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        writer.close()
        return

    use_binary = _is_binary_protocol(first_byte)

    try:
        if use_binary:
            await _binary_loop(reader, writer, first_byte)
        else:
            # Wrap reader so the peeked byte is transparently re-inserted
            peeked_reader = _PeekedReader(reader, first_byte)
            await _resp_loop(peeked_reader, writer)   # type: ignore[arg-type]
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[TCP] Error on {address}: {e}")
    finally:
        print(f"[TCP] Closed connection from {address}")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# RESP2 loop
# ---------------------------------------------------------------------------

async def _resp_loop(reader, writer: asyncio.StreamWriter):
    """Handle a RESP2 client. Enforces REQUIREPASS if configured."""
    authenticated = not bool(REQUIRE_PASS)   # auto-authenticated if no password set

    while True:
        command, args = await decode_command(reader)
        if command is None or args is None:
            break

        response, authenticated = await _handle_resp_command(command, args, authenticated)
        writer.write(response)
        await writer.drain()

        if command == "QUIT":
            break


# ---------------------------------------------------------------------------
# Binary protocol loop
# ---------------------------------------------------------------------------

async def _binary_loop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, first_byte: bytes):
    """Handle a PulseDB binary protocol client."""
    try:
        remaining_header = await reader.readexactly(4)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return

    header = first_byte + remaining_header
    _, length = struct.unpack("!BI", header)
    await _binary_handle_one(reader, writer, length)

    while True:
        msg_type, data = await decode_message(reader)
        if msg_type is None or data is None:
            break

        parts = data.split()
        if not parts:
            continue

        try:
            result = await execute(parts[0], parts[1:])
            response = encode_message(TYPE_RESPONSE, result)
        except Exception as e:
            response = encode_message(TYPE_ERROR, str(e))

        writer.write(response)
        await writer.drain()


async def _binary_handle_one(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, length: int):
    try:
        data_bytes = await reader.readexactly(length)
    except asyncio.IncompleteReadError:
        return

    parts = data_bytes.decode("utf-8", errors="replace").split()
    if not parts:
        return

    try:
        result = await execute(parts[0], parts[1:])
        response = encode_message(TYPE_RESPONSE, result)
    except Exception as e:
        response = encode_message(TYPE_ERROR, str(e))

    writer.write(response)
    await writer.drain()


# ---------------------------------------------------------------------------
# Server entry point — returns the server object so main.py can shut it down
# ---------------------------------------------------------------------------

_tcp_server = None


async def start_tcp_server(host: str = TCP_HOST, port: int = TCP_PORT):
    global _tcp_server
    
    ssl_context = None
    if TLS_CERT and TLS_KEY and os.path.exists(TLS_CERT) and os.path.exists(TLS_KEY):
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=TLS_CERT, keyfile=TLS_KEY)
        
    _tcp_server = await asyncio.start_server(handle_client, host, port, ssl=ssl_context)
    addr = _tcp_server.sockets[0].getsockname()
    protocol = "rediss://" if ssl_context else "redis://"
    print(f"[TCP] PulseDB listening on {protocol}{addr[0]}:{addr[1]} (auth={'on' if REQUIRE_PASS else 'off'})")
    async with _tcp_server:
        await _tcp_server.serve_forever()


async def stop_tcp_server():
    if _tcp_server:
        _tcp_server.close()
        await _tcp_server.wait_closed()
        print("[TCP] Server stopped.")
