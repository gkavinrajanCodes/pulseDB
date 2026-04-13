import asyncio
import time
from fastapi import FastAPI, WebSocket, Request, HTTPException, Depends, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator

from server.commands import execute
from server.ttl import start_ttl_thread
from server.pubsub import pubsub
from server.tcp_server import start_tcp_server
from server.persistence import start_persistence
from server.store import store

app = FastAPI(title="PulseDB Cloud", version="1.0.0")

# --- Prometheus Metrics ---
Instrumentator().instrument(app).expose(app)

# --- API Key Security ---
API_KEY = "pulse-db-secret-key"
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    raise HTTPException(status_code=403, detail="Invalid or missing API key")


# --- Rate Limiting (simple sliding window per IP) ---
_rate_store: dict = {}
RATE_LIMIT_PER_SEC = 50  # requests per IP per second


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    last = _rate_store.get(client_ip, 0)
    if now - last < (1.0 / RATE_LIMIT_PER_SEC):
        return JSONResponse(
            status_code=429,
            content={"detail": "Too Many Requests"},
        )
    _rate_store[client_ip] = now
    return await call_next(request)


# --- Models ---
class CommandRequest(BaseModel):
    command: str
    args: list


# --- Startup ---
@app.on_event("startup")
async def startup_event():
    # 1. Persistence: load snapshot + replay WAL synchronously
    start_persistence(store)
    # 2. Background TTL cleanup thread
    start_ttl_thread()
    # 3. TCP server as asyncio background task
    asyncio.create_task(start_tcp_server())
    print("[Server] PulseDB Cloud started.")


# --- Health Checks ---
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {"status": "ready", "node": "node1"}


# --- Command Endpoint ---
@app.post("/command")
async def run_command(req: CommandRequest, api_key: str = Depends(get_api_key)):
    try:
        result = await execute(req.command, req.args)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Pub/Sub REST hint ---
@app.get("/subscribe/{channel}")
async def subscribe_hint(channel: str, api_key: str = Depends(get_api_key)):
    return {
        "channel": channel,
        "status": "Connect via WebSocket",
        "url": f"ws://<host>/ws/subscribe/{channel}",
    }


# --- WebSocket Pub/Sub ---
@app.websocket("/ws/subscribe/{channel}")
async def websocket_subscribe(websocket: WebSocket, channel: str):
    await websocket.accept()
    queue = pubsub.subscribe(channel)
    try:
        while True:
            message = await queue.get()
            await websocket.send_text(str(message))
    except Exception as e:
        print(f"[WS] Error on channel '{channel}': {e}")
    finally:
        pubsub.unsubscribe(channel, queue)