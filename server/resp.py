# Copyright (c) 2026 G Kavinrajan. All rights reserved.
# Licensed under the Business Source License 1.1

# server/resp.py
"""
RESP2 (Redis Serialization Protocol v2) encoder and decoder.

This allows any Redis client (redis-py, ioredis, redis-cli, etc.)
to connect to PulseDB without any code changes.

Protocol reference: https://redis.io/docs/reference/protocol-spec/

Types:
  Simple String:  +OK\r\n
  Error:          -ERR message\r\n
  Integer:        :1000\r\n
  Bulk String:    $6\r\nfoobar\r\n
  Null Bulk:      $-1\r\n
  Array:          *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
  Null Array:     *-1\r\n
"""

import asyncio

CRLF = b"\r\n"


# ---------------------------------------------------------------------------
# Encoder — Python objects → RESP2 bytes
# ---------------------------------------------------------------------------

def encode(value) -> bytes:
    """Encode a Python value to RESP2 wire format."""
    if value is None:
        return b"$-1\r\n"                          # Null bulk string

    if isinstance(value, bool):
        return b":1\r\n" if value else b":0\r\n"

    if isinstance(value, int):
        return f":{value}\r\n".encode()

    if isinstance(value, float):
        # Return as bulk string (RESP2 has no float type)
        s = str(value).encode()
        return b"$" + str(len(s)).encode() + CRLF + s + CRLF

    if isinstance(value, str):
        # Detect error strings
        if value.startswith("ERROR:") or value.startswith("ERR "):
            return f"-{value}\r\n".encode()
        if value == "OK":
            return b"+OK\r\n"
        if value == "NULL":
            return b"$-1\r\n"
        # General bulk string
        encoded = value.encode("utf-8")
        return b"$" + str(len(encoded)).encode() + CRLF + encoded + CRLF

    if isinstance(value, bytes):
        return b"$" + str(len(value)).encode() + CRLF + value + CRLF

    if isinstance(value, (list, tuple)):
        parts = [f"*{len(value)}\r\n".encode()]
        for item in value:
            parts.append(encode(item))
        return b"".join(parts)

    if isinstance(value, dict):
        # Encode as flat array: key1, val1, key2, val2, ...
        items = []
        for k, v in value.items():
            items.extend([k, v])
        return encode(items)

    # Fallback: convert to string
    return encode(str(value))


def encode_simple(msg: str) -> bytes:
    """Encode a simple string (+OK, +PONG, etc.)"""
    return f"+{msg}\r\n".encode()


def encode_error(msg: str) -> bytes:
    """Encode an error response."""
    return f"-ERR {msg}\r\n".encode()


# ---------------------------------------------------------------------------
# Decoder — RESP2 wire bytes → Python (command, args)
# ---------------------------------------------------------------------------

class RESPDecodeError(Exception):
    pass


async def decode_command(reader: asyncio.StreamReader):
    """
    Read and decode one full RESP2 command from the stream.
    Returns (command: str, args: list[str]) or (None, None) on disconnect.

    A command is always a RESP Array of Bulk Strings:
      *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    """
    try:
        first_byte = await reader.readexactly(1)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None, None

    if first_byte != b"*":
        # Not a RESP array — either inline command or bad client
        # Try to handle inline (e.g., from telnet): read until \n
        rest = await reader.readline()
        line = (first_byte + rest).strip()
        if not line:
            return None, None
        parts = line.split()
        if not parts:
            return None, None
        command = parts[0].decode("utf-8", errors="replace").upper()
        args = parts[1:]
        return command, args

    # Read array length
    count_line = await reader.readline()
    count = int(count_line.strip())

    if count <= 0:
        return None, None

    elements: list[bytes | None] = []
    for _ in range(count):
        type_byte = await reader.readexactly(1)
        if type_byte == b"$":
            # Bulk string
            length_line = await reader.readline()
            length = int(length_line.strip())
            if length == -1:
                elements.append(None)
            else:
                data = await reader.readexactly(length)
                await reader.readexactly(2)  # consume \r\n
                elements.append(data)
        else:
            # Unexpected type inside command array
            raw_line = await reader.readline()
            elements.append((type_byte + raw_line).strip())

    if not elements:
        return None, None

    command_str = elements[0].decode("utf-8", errors="replace").upper() if elements[0] else ""
    args = [e for e in elements[1:] if e is not None]
    return command_str, args
