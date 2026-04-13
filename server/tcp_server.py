# server/tcp_server.py

import asyncio
from server.protocol import decode_message, encode_message, TYPE_RESPONSE, TYPE_ERROR
from server.commands import execute

async def handle_client(reader, writer):
    address = writer.get_extra_info('peername')
    print(f"New TCP connection from {address}")
    
    try:
        while True:
            msg_type, data = await decode_message(reader)
            if msg_type is None:
                break
            
            # Simple protocol: command args... (space separated)
            parts = data.split()
            if not parts:
                continue
                
            command = parts[0]
            args = parts[1:]
            
            try:
                result = execute(command, args)
                response = encode_message(TYPE_RESPONSE, result)
            except Exception as e:
                response = encode_message(TYPE_ERROR, str(e))
                
            writer.write(response)
            await writer.drain()
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Error handling TCP client {address}: {e}")
    finally:
        print(f"Closing TCP connection from {address}")
        writer.close()
        await writer.wait_closed()

async def start_tcp_server(host='0.0.0.0', port=6379):
    server = await asyncio.start_server(handle_client, host, port)
    addr = server.sockets[0].getsockname()
    print(f'Serving TCP on {addr}')

    async with server:
        await server.serve_forever()
