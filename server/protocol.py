# server/protocol.py

import struct

# Simple binary protocol:
# [1 byte type] [4 bytes length] [length bytes data]
# Type: 0=Request, 1=Response, 2=Error

TYPE_REQUEST = 0
TYPE_RESPONSE = 1
TYPE_ERROR = 2

def encode_message(msg_type, data):
    if isinstance(data, str):
        data = data.encode()
    elif isinstance(data, list):
        # Very simple serialization for lists (comma separated for now)
        data = ",".join(map(str, data)).encode()
    elif data is None:
        data = b"NULL"
    else:
        data = str(data).encode()
        
    length = len(data)
    return struct.pack("!BI", msg_type, length) + data

async def decode_message(reader):
    header = await reader.read(5)
    if not header:
        return None, None
    
    msg_type, length = struct.unpack("!BI", header)
    data = await reader.read(length)
    return msg_type, data.decode()
