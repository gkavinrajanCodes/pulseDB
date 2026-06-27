# sdk/pulsedb/client.py
"""
Synchronous PulseDB client (wraps the async client).

Usage:
    from pulsedb import PulseDB

    db = PulseDB(host="localhost", port=8000, api_key="pulse-db-secret-key")
    db.set("key", "value", ttl=3600)
    val = db.get("key")
"""

import asyncio
from typing import Optional, List, Any

from .async_client import AsyncPulseDB


def _run(coro):
    """Run a coroutine in a new event loop (sync bridge)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Inside an existing event loop (e.g., Jupyter) — use a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


class PulseDB:
    """
    Synchronous PulseDB client.

    Wraps AsyncPulseDB to provide a blocking API for use in sync codebases,
    scripts, Django views, Flask routes, etc.

    For async codebases (FastAPI, aiohttp), use AsyncPulseDB directly.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8000,
        api_key: str = "pulse-db-secret-key",
        tls: bool = False,
        timeout: float = 5.0,
    ):
        self._async = AsyncPulseDB(
            host=host, port=port, api_key=api_key, tls=tls, timeout=timeout
        )

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> str:
        return _run(self._async.set(key, value, ttl))

    def get(self, key: str) -> Optional[str]:
        return _run(self._async.get(key))

    def delete(self, *keys: str) -> str:
        return _run(self._async.delete(*keys))

    def exists(self, key: str) -> bool:
        return _run(self._async.exists(key))

    def expire(self, key: str, seconds: float) -> int:
        return _run(self._async.expire(key, seconds))

    def ttl(self, key: str) -> int:
        return _run(self._async.ttl(key))

    def mset(self, mapping: dict) -> str:
        return _run(self._async.mset(mapping))

    def mget(self, *keys: str) -> List[Optional[str]]:
        return _run(self._async.mget(*keys))

    def keys(self, pattern: str = "*") -> List[str]:
        return _run(self._async.keys(pattern))

    def dbsize(self) -> int:
        return _run(self._async.dbsize())

    def incr(self, key: str) -> int:
        return _run(self._async.incr(key))

    def incrby(self, key: str, amount: int) -> int:
        return _run(self._async.incrby(key, amount))

    def decr(self, key: str) -> int:
        return _run(self._async.decr(key))

    def decrby(self, key: str, amount: int) -> int:
        return _run(self._async.decrby(key, amount))

    def publish(self, channel: str, message: str) -> str:
        return _run(self._async.publish(channel, message))

    def ping(self) -> str:
        return _run(self._async.ping())

    def flush(self) -> str:
        return _run(self._async.flush())

    def info(self) -> str:
        return _run(self._async.info())

    def close(self):
        _run(self._async.close())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
