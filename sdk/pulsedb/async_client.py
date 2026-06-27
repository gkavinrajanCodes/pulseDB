# sdk/pulsedb/async_client.py
"""
Async PulseDB client.

Usage:
    import asyncio
    from pulsedb import AsyncPulseDB

    async def main():
        db = AsyncPulseDB(host="localhost", port=8000, api_key="pulse-db-secret-key")
        await db.set("key", "value", ttl=3600)
        val = await db.get("key")

    asyncio.run(main())
"""

import asyncio
import json
from typing import Optional, List, Any

import httpx

from .exceptions import CommandError, AuthenticationError, ConnectionError, TimeoutError


class AsyncPulseDB:
    """
    Async HTTP client for PulseDB Cloud.

    All methods are coroutines. Use with await inside an async function.
    For sync usage, see PulseDB (sync_client.py).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8000,
        api_key: str = "pulse-db-secret-key",
        tls: bool = False,
        timeout: float = 5.0,
    ):
        scheme = "https" if tls else "http"
        self._base_url = f"{scheme}://{host}:{port}"
        self._headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        }
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers=self._headers,
                timeout=self._timeout,
            )
        return self._client

    async def _cmd(self, command: str, *args) -> Any:
        """Send a command and return the parsed result."""
        client = await self._get_client()
        try:
            resp = await client.post(
                "/command",
                json={"command": command, "args": [str(a) for a in args]},
            )
        except httpx.ConnectError as e:
            raise ConnectionError(f"Cannot connect to PulseDB at {self._base_url}: {e}") from e
        except httpx.TimeoutException as e:
            raise TimeoutError(f"Command '{command}' timed out") from e

        if resp.status_code == 403:
            raise AuthenticationError("Invalid API key")
        if resp.status_code == 429:
            raise CommandError("Rate limit exceeded")
        if resp.status_code >= 500:
            raise CommandError(f"Server error: {resp.text}")

        data = resp.json()
        result = data.get("result")
        if isinstance(result, str) and result.startswith("ERROR:"):
            raise CommandError(result[7:])
        return result

    # ------------------------------------------------------------------
    # Core KV operations
    # ------------------------------------------------------------------

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> str:
        """Set key to value. Optionally set TTL in seconds."""
        args = [key, str(value)]
        if ttl is not None:
            args += ["EX", str(int(ttl))]
        return await self._cmd("SET", *args)

    async def get(self, key: str) -> Optional[str]:
        """Get value for key. Returns None if key doesn't exist."""
        result = await self._cmd("GET", key)
        return None if result == "NULL" else result

    async def delete(self, *keys: str) -> str:
        """Delete one or more keys."""
        return await self._cmd("DEL", *keys)

    async def exists(self, key: str) -> bool:
        """Return True if the key exists."""
        return bool(await self._cmd("EXISTS", key))

    async def expire(self, key: str, seconds: float) -> int:
        """Set TTL on a key. Returns 1 if set, 0 if key not found."""
        return await self._cmd("EXPIRE", key, str(seconds))

    async def ttl(self, key: str) -> int:
        """Get remaining TTL in seconds. -1 = no TTL. -2 = key not found."""
        return await self._cmd("TTL", key)

    async def mset(self, mapping: dict) -> str:
        """Set multiple keys at once."""
        args = []
        for k, v in mapping.items():
            args += [k, str(v)]
        return await self._cmd("MSET", *args)

    async def mget(self, *keys: str) -> List[Optional[str]]:
        """Get multiple keys at once. Returns list with None for missing keys."""
        results = await self._cmd("MGET", *keys)
        if isinstance(results, list):
            return [None if v == "NULL" else v for v in results]
        return results

    async def keys(self, pattern: str = "*") -> List[str]:
        """Return all keys matching a glob pattern."""
        result = await self._cmd("KEYS", pattern)
        return result if isinstance(result, list) else []

    async def dbsize(self) -> int:
        """Return total number of keys."""
        return int(await self._cmd("DBSIZE"))

    # ------------------------------------------------------------------
    # Numeric operations
    # ------------------------------------------------------------------

    async def incr(self, key: str) -> int:
        """Increment integer value of key by 1."""
        return int(await self._cmd("INCR", key))

    async def incrby(self, key: str, amount: int) -> int:
        """Increment integer value of key by amount."""
        return int(await self._cmd("INCRBY", key, str(amount)))

    async def decr(self, key: str) -> int:
        """Decrement integer value of key by 1."""
        return int(await self._cmd("DECR", key))

    async def decrby(self, key: str, amount: int) -> int:
        """Decrement integer value of key by amount."""
        return int(await self._cmd("DECRBY", key, str(amount)))

    # ------------------------------------------------------------------
    # Pub/Sub
    # ------------------------------------------------------------------

    async def publish(self, channel: str, message: str) -> str:
        """Publish a message to a channel."""
        return await self._cmd("PUBLISH", channel, message)

    # ------------------------------------------------------------------
    # Admin
    # ------------------------------------------------------------------

    async def ping(self) -> str:
        """Ping the server. Returns 'PONG' if alive."""
        return await self._cmd("PING")

    async def flush(self) -> str:
        """Delete all keys in the database."""
        return await self._cmd("FLUSHDB")

    async def info(self) -> str:
        """Get server info string."""
        return await self._cmd("INFO")

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
