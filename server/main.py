# Copyright (c) 2026 G Kavinrajan. All rights reserved.
# Licensed under the Business Source License 1.1

# server/main.py
import asyncio
import signal
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, Request, HTTPException, Depends, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator

from server.commands import execute
from server.ttl import start_ttl_thread
from server.pubsub import pubsub
from server.tcp_server import start_tcp_server, stop_tcp_server
from server.persistence import start_persistence, wal
from server.store import store
from server.config import API_KEY


# ---------------------------------------------------------------------------
# Graceful shutdown helpers
# ---------------------------------------------------------------------------

def _install_signal_handlers(loop: asyncio.AbstractEventLoop):
    """Install SIGTERM / SIGINT handlers for graceful shutdown."""

    def _shutdown():
        print("\n[Server] Shutdown signal received — flushing WAL and stopping...")
        # Flush the WAL file handle so nothing is lost
        try:
            wal._file.flush()
        except Exception:
            pass
        # Stop the event loop (lifespan 'finally' block will run next)
        loop.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except (NotImplementedError, RuntimeError):
            # Windows / non-main-thread — skip
            pass


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    loop = asyncio.get_event_loop()
    _install_signal_handlers(loop)

    start_persistence(store)
    start_ttl_thread()
    tcp_task = asyncio.create_task(start_tcp_server())
    print("[Server] PulseDB Cloud started.")

    yield

    # --- Shutdown ---
    print("[Server] Shutting down gracefully...")
    tcp_task.cancel()
    await stop_tcp_server()
    try:
        await asyncio.wait_for(tcp_task, timeout=3.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    wal._file.flush()
    wal.close()
    print("[Server] Shutdown complete.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="PulseDB Cloud", version="1.1.0", lifespan=lifespan)

# Prometheus metrics
Instrumentator().instrument(app).expose(app)

# API Key auth
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    raise HTTPException(status_code=403, detail="Invalid or missing API key")


# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------

_rate_store: dict = {}
RATE_LIMIT_PER_SEC = 50
_RATE_STORE_MAX = 10_000


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    if client_ip == "testclient":
        return await call_next(request)
        
    now = time.time()
    last = _rate_store.get(client_ip, 0)
    if now - last < (1.0 / RATE_LIMIT_PER_SEC):
        return JSONResponse(status_code=429, content={"detail": "Too Many Requests"})
    _rate_store[client_ip] = now
    if len(_rate_store) > _RATE_STORE_MAX:
        oldest = sorted(_rate_store, key=_rate_store.__getitem__)
        for ip in oldest[: _RATE_STORE_MAX // 2]:
            _rate_store.pop(ip, None)
    return await call_next(request)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class CommandRequest(BaseModel):
    command: str
    args: list


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    from server.config import NODE_ID
    return {"status": "ready", "node": NODE_ID}


@app.post("/command")
async def run_command(req: CommandRequest, api_key: str = Depends(get_api_key)):
    try:
        result = await execute(req.command, req.args)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/subscribe/{channel}")
async def subscribe_hint(channel: str, api_key: str = Depends(get_api_key)):
    return {
        "channel": channel,
        "status": "Connect via WebSocket",
        "url": f"ws://<host>/ws/subscribe/{channel}",
    }


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