# server/tcp_server.py
"""
Dual-protocol TCP server.

Supports two wire protocols on the same port (6379):
  1. RESP2  — the Redis wire protocol. Detected by first byte being '*' or
              any printable ASCII (inline commands). redis-cli, redis-py,
              ioredis, etc. all work out of the box.
  2. Binary — PulseDB's own compact binary protocol (for SDK internal use).
              Detected by first byte being 0x00, 0x01, or 0x02.

This means ANY existing Redis client can connect to PulseDB with zero changes.
"""

import asyncio
from server.resp import decode_command, encode, encode_simple, encode_error
from server.protocol import decode_message, encode_message, TYPE_RESPONSE, TYPE_ERROR
from server.commands import execute


# ---------------------------------------------------------------------------
# RESP-specific command handling
# ---------------------------------------------------------------------------

# Redis commands that map to internal PulseDB commands
_RESP_COMMAND_MAP = {
    "PING":    ("PING", []),
    "QUIT":    ("QUIT", []),
    "SELECT":  ("SELECT", []),   # acknowledged but ignored (no multi-db)
    "COMMAND": ("COMMAND", []),  # minimal compat shim
    "DBSIZE":  ("DBSIZE", []),
    "FLUSHDB": ("FLUSHDB", []),
    "FLUSHALL":("FLUSHALL", []),
}


async def _handle_resp_command(command: str, args: list) -> bytes:
    """Translate a RESP command to a PulseDB execute() call and encode the result."""

    # Special-case commands that don't map to execute()
    if command == "PING":
        msg = args[0] if args else "PONG"
        return encode_simple(msg)

    if command == "QUIT":
        return encode_simple("OK")

    if command in ("SELECT", "AUTH"):
        # We only have DB 0. Accept SELECT 0, reject others.
        if command == "SELECT" and args and args[0] != "0":
            return encode_error("PulseDB does not support multiple databases")
        return encode_simple("OK")

    if command == "COMMAND":
        # Minimal compat: return empty array so redis-py doesn't crash on connect
        return b"*0\r\n"

    if command == "DBSIZE":
        # Return total key count across all shards
        from server.store import store
        count = sum(len(shard.data) for shard in store.shards)
        return f":{count}\r\n".encode()

    if command in ("FLUSHDB", "FLUSHALL"):
        from server.store import store
        for shard in store.shards:
            with shard.lock:
                shard.data.clear()
                shard.expiry.clear()
        return encode_simple("OK")

    # All other commands go through the normal execute() path
    try:
        result = await execute(command, args)
        return encode(result)
    except Exception as e:
        return encode_error(str(e))


# ---------------------------------------------------------------------------
# Protocol detection + per-connection handler
# ---------------------------------------------------------------------------

def _is_binary_protocol(first_byte: bytes) -> bool:
    """
    Our binary protocol type bytes are 0x00, 0x01, 0x02.
    RESP and inline text always start with printable ASCII or '*', '+', '-', ':', '$'.
    """
    return first_byte in (b"\x00", b"\x01", b"\x02")


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    address = writer.get_extra_info("peername")
    print(f"[TCP] New connection from {address}")

    try:
        # Peek at the first byte to decide which protocol to use
        first_byte = await reader.readexactly(1)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        writer.close()
        return

    use_binary = _is_binary_protocol(first_byte)

    try:
        if use_binary:
            await _binary_loop(reader, writer, first_byte)
        else:
            await _resp_loop(reader, writer, first_byte)
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


async def _resp_loop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, first_byte: bytes):
    """Handle a RESP2 client (redis-cli, redis-py, ioredis, etc.)"""
    # Put the peeked byte back by prepending to a new reader
    # We do this by creating a buffered version — feed the byte back
    reader._buffer[0:0] = first_byte  # type: ignore[attr-defined]

    while True:
        command, args = await decode_command(reader)
        if command is None:
            break

        response = await _handle_resp_command(command, args)
        writer.write(response)
        await writer.drain()

        # QUIT closes the connection
        if command == "QUIT":
            break


async def _binary_loop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, first_byte: bytes):
    """Handle a PulseDB binary protocol client (internal SDK)."""
    # Reconstruct the full 5-byte header: we already read 1 byte
    try:
        remaining_header = await reader.readexactly(4)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return

    import struct
    header = first_byte + remaining_header
    msg_type, length = struct.unpack("!BI", header)

    # Process first message
    await _binary_handle_one(reader, writer, length)

    # Continue processing subsequent messages
    while True:
        msg_type, data = await decode_message(reader)
        if msg_type is None or data is None:
            break

        parts = data.split()
        if not parts:
            continue

        command = parts[0]
        args = parts[1:]

        try:
            result = await execute(command, args)
            response = encode_message(TYPE_RESPONSE, result)
        except Exception as e:
            response = encode_message(TYPE_ERROR, str(e))

        writer.write(response)
        await writer.drain()


async def _binary_handle_one(reader, writer, length):
    """Process one already-partially-read binary message."""
    import struct
    try:
        data_bytes = await reader.readexactly(length)
    except asyncio.IncompleteReadError:
        return

    data = data_bytes.decode("utf-8", errors="replace")
    parts = data.split()
    if not parts:
        return

    command = parts[0]
    args = parts[1:]
    try:
        result = await execute(command, args)
        response = encode_message(TYPE_RESPONSE, result)
    except Exception as e:
        response = encode_message(TYPE_ERROR, str(e))

    writer.write(response)
    await writer.drain()


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------

async def start_tcp_server(host: str = "0.0.0.0", port: int = 6379):
    server = await asyncio.start_server(handle_client, host, port)
    addr = server.sockets[0].getsockname()
    print(f"[TCP] PulseDB listening on {addr} (RESP2 + Binary Protocol)")
    async with server:
        await server.serve_forever()
