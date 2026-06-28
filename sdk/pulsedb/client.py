# sdk/pulsedb/client.py
"""
Synchronous PulseDB client (wraps the async client).

Usage:
    from pulsedb import PulseDB

    db = PulseDB(host="localhost", port=6379)
    db.set("key", "value", ttl=3600)
    val = db.get("key")

    # Vector Engine Usage
    db.vectors.upsert("doc1", [0.1, 0.2, 0.3], metadata={"author": "John"})
    results = db.vectors.search([0.1, 0.2, 0.3], top_k=5, filter={"author": "John"})
"""

import asyncio
from typing import Optional, List, Any, Dict

from .async_client import AsyncPulseDB


import threading

_loop = asyncio.new_event_loop()
_thread = threading.Thread(target=_loop.run_forever, daemon=True)
_thread.start()

def _run(coro):
    """Run a coroutine in the background event loop (sync bridge)."""
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result()


class VectorNamespace:
    """
    Provides a beautiful, Pythonic API for the PulseDB AI Memory Engine.
    Transparently packs Python floats into C++ binary bytes and serializes metadata.
    """
    def __init__(self, async_namespace):
        self._async = async_namespace

    def upsert(self, id: str, vector: List[float], metadata: Optional[Dict[str, Any]] = None) -> str:
        """Insert or update a vector embedding with optional metadata."""
        return _run(self._async.upsert(id, vector, metadata))

    def upsert_batch(self, items: List[Dict[str, Any]]) -> int:
        """Bulk-insert multiple vectors in a single network round-trip."""
        return _run(self._async.upsert_batch(items))

    def get(self, id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a vector and its metadata by ID."""
        return _run(self._async.get(id))

    def search(self, query: List[float], top_k: int = 5, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Perform a blazing fast similarity search, optionally pre-filtering by metadata."""
        return _run(self._async.search(query, top_k, filter))

    def count(self) -> int:
        """Get the total number of vectors in the AI Memory Engine."""
        return _run(self._async.count())

    def delete(self, id: str) -> str:
        """Delete a vector from the AI Memory Engine."""
        return _run(self._async.delete(id))


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
        port: int = 6379,
        timeout: float = 10.0,
    ):
        self._async = AsyncPulseDB(
            host=host, port=port, timeout=timeout
        )
        self.vectors = VectorNamespace(self._async.vectors)

    def execute_command(self, command: str, *args) -> Any:
        return _run(self._async.execute_command(command, *args))

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

    def hmset(self, key: str, mapping: dict) -> str:
        return _run(self._async.hmset(key, mapping))

    def hgetall(self, key: str) -> List[str]:
        return _run(self._async.hgetall(key))

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
