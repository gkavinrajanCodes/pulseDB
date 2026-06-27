# sdk/pulsedb/async_client.py
"""
Async PulseDB client using the ultra-fast Binary Protocol (RESP2 over TCP).

Usage:
    import asyncio
    from pulsedb import AsyncPulseDB

    async def main():
        db = AsyncPulseDB(host="localhost", port=6379)
        await db.set("key", "value", ttl=3600)
        val = await db.get("key")

        # Vector Engine Usage
        await db.vectors.upsert("doc1", [0.1, 0.2, 0.3], metadata={"author": "John"})
        results = await db.vectors.search([0.1, 0.2, 0.3], top_k=5, filter={"author": "John"})

    asyncio.run(main())
"""

import json
import asyncio
from typing import Optional, List, Any, Dict

import redis
import redis.asyncio as redis_async
import numpy as np

from .exceptions import CommandError, ConnectionError, TimeoutError


class AsyncVectorNamespace:
    """
    Provides a beautiful, Pythonic API for the PulseDB AI Memory Engine.
    Transparently packs Python floats into C++ binary bytes and serializes metadata.
    """
    def __init__(self, db: "AsyncPulseDB"):
        self.db = db

    async def upsert(self, id: str, vector: List[float], metadata: Optional[Dict[str, Any]] = None) -> str:
        """Insert or update a vector embedding with optional metadata."""
        blob = np.array(vector, dtype=np.float32).tobytes()
        args: List[Any] = [id, blob]
        if metadata is not None:
            args.extend(["METADATA", json.dumps(metadata)])
            
        try:
            return await self.db.execute_command("VECTOR.BSET", *args)
        except Exception as e:
            if "dimension mismatch" in str(e).lower():
                raise CommandError(f"Vector dimension mismatch: {e}")
            raise CommandError(f"Failed to upsert vector: {e}")

    async def get(self, id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a vector and its metadata by ID."""
        result = await self.db.execute_command("VECTOR.GET", id)
        if result == "NULL" or result is None:
            return None
        if isinstance(result, (bytes, bytearray)):
            result = result.decode("utf-8")
        if isinstance(result, str):
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return None # fallback
        return result if isinstance(result, dict) else None

    async def search(self, query: List[float], top_k: int = 5, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Perform a blazing fast similarity search, optionally pre-filtering by metadata."""
        blob = np.array(query, dtype=np.float32).tobytes()
        args: List[Any] = [blob, "TOP_K", top_k]
        if filter is not None:
            args.extend(["FILTER", json.dumps(filter)])
            
        results = await self.db.execute_command("VECTOR.BSEARCH", *args)
        if not results:
            return []
            
        parsed = []
        # Results return as flat array: [key1, score1, key2, score2, ...]
        for i in range(0, len(results), 2):
            doc_id = results[i]
            if isinstance(doc_id, (bytes, bytearray)):
                doc_id = doc_id.decode("utf-8")
            score = float(results[i+1])
            parsed.append({"id": doc_id, "score": score})
            
        return parsed

    async def count(self) -> int:
        """Get the total number of vectors in the AI Memory Engine."""
        return int(await self.db.execute_command("VECTOR.COUNT"))

    async def delete(self, id: str) -> str:
        """Delete a vector from the AI Memory Engine."""
        return await self.db.execute_command("VECTOR.DEL", id)


class AsyncPulseDB:
    """
    Async TCP client for PulseDB Cloud.

    All methods are coroutines. Use with await inside an async function.
    For sync usage, see PulseDB (sync_client.py).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        timeout: float = 10.0,
    ):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._client: Optional[redis_async.Redis] = None
        
        # Initialize Vector AI Namespace
        self.vectors = AsyncVectorNamespace(self)

    def _get_client(self) -> redis_async.Redis:
        if self._client is None:
            # We use protocol=2 for backwards compatibility with our custom RESP2 router
            self._client = redis_async.Redis(
                host=self._host,
                port=self._port,
                socket_timeout=self._timeout,
                decode_responses=True,
                protocol=2
            )
        return self._client

    async def execute_command(self, command: str, *args) -> Any:
        """Execute a raw command."""
        client = self._get_client()
        try:
            result = await client.execute_command(command, *args)
            if isinstance(result, str) and result.startswith("ERROR:"):
                raise CommandError(result[7:])
            return result
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError(f"Cannot connect to PulseDB at {self._host}:{self._port}: {e}") from e
        except redis.exceptions.TimeoutError as e:
            raise TimeoutError(f"Command '{command}' timed out") from e
        except redis.exceptions.ResponseError as e:
            err_msg = str(e)
            if err_msg.startswith("ERROR:"):
                raise CommandError(err_msg[7:])
            raise CommandError(err_msg)

    # ------------------------------------------------------------------
    # Core KV operations
    # ------------------------------------------------------------------

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> str:
        """Set key to value. Optionally set TTL in seconds."""
        args = [key, str(value)]
        if ttl is not None:
            args += ["EX", str(int(ttl))]
        return await self.execute_command("SET", *args)

    async def get(self, key: str) -> Optional[str]:
        """Get value for key. Returns None if key doesn't exist."""
        result = await self.execute_command("GET", key)
        return None if result == "NULL" else result

    async def delete(self, *keys: str) -> str:
        """Delete one or more keys."""
        return await self.execute_command("DEL", *keys)

    async def exists(self, key: str) -> bool:
        """Return True if the key exists."""
        return bool(await self.execute_command("EXISTS", key))

    async def expire(self, key: str, seconds: float) -> int:
        """Set TTL on a key. Returns 1 if set, 0 if key not found."""
        return await self.execute_command("EXPIRE", key, str(seconds))

    async def ttl(self, key: str) -> int:
        """Get remaining TTL in seconds. -1 = no TTL. -2 = key not found."""
        return await self.execute_command("TTL", key)

    async def mset(self, mapping: dict) -> str:
        """Set multiple keys at once."""
        args = []
        for k, v in mapping.items():
            args += [k, str(v)]
        return await self.execute_command("MSET", *args)

    async def mget(self, *keys: str) -> List[Optional[str]]:
        """Get multiple keys at once. Returns list with None for missing keys."""
        results = await self.execute_command("MGET", *keys)
        if isinstance(results, list):
            return [None if v == "NULL" else v for v in results]
        return results

    async def keys(self, pattern: str = "*") -> List[str]:
        """Return all keys matching a glob pattern."""
        result = await self.execute_command("KEYS", pattern)
        return result if isinstance(result, list) else []

    async def dbsize(self) -> int:
        """Return total number of keys."""
        return int(await self.execute_command("DBSIZE"))

    # ------------------------------------------------------------------
    # Hash operations
    # ------------------------------------------------------------------

    async def hmset(self, key: str, mapping: dict) -> str:
        """Set multiple fields in a hash."""
        args = [key]
        for k, v in mapping.items():
            args.extend([k, str(v)])
        return await self.execute_command("HMSET", *args)

    async def hgetall(self, key: str) -> List[str]:
        """Get all fields and values in a hash as a flat list."""
        result = await self.execute_command("HGETALL", key)
        if isinstance(result, dict):
            flat = []
            for k, v in result.items():
                flat.extend([k, str(v)])
            return flat
        return result if isinstance(result, list) else []

    # ------------------------------------------------------------------
    # Numeric operations
    # ------------------------------------------------------------------

    async def incr(self, key: str) -> int:
        """Increment integer value of key by 1."""
        return int(await self.execute_command("INCR", key))

    async def incrby(self, key: str, amount: int) -> int:
        """Increment integer value of key by amount."""
        return int(await self.execute_command("INCRBY", key, str(amount)))

    async def decr(self, key: str) -> int:
        """Decrement integer value of key by 1."""
        return int(await self.execute_command("DECR", key))

    async def decrby(self, key: str, amount: int) -> int:
        """Decrement integer value of key by amount."""
        return int(await self.execute_command("DECRBY", key, str(amount)))

    # ------------------------------------------------------------------
    # Pub/Sub
    # ------------------------------------------------------------------

    async def publish(self, channel: str, message: str) -> str:
        """Publish a message to a channel."""
        return await self.execute_command("PUBLISH", channel, message)

    # ------------------------------------------------------------------
    # Admin
    # ------------------------------------------------------------------

    async def ping(self) -> str:
        """Ping the server. Returns 'PONG' if alive."""
        return await self.execute_command("PING")

    async def flush(self) -> str:
        """Delete all keys in the database."""
        return await self.execute_command("FLUSHDB")

    async def info(self) -> str:
        """Get server info string."""
        return await self.execute_command("INFO")

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    async def close(self):
        if self._client:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
