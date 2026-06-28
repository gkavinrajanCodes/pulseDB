# sdk/pulsedb/__init__.py
"""
PulseDB Python SDK

Connects to PulseDB over the high-performance TCP Binary Protocol (port 6379).

Usage (sync):
    from pulsedb import PulseDB

    db = PulseDB(host="localhost", port=6379)
    db.set("user:123", "alice", ttl=3600)
    print(db.get("user:123"))  # "alice"

    # AI Memory Engine (Vector Search)
    db.vectors.upsert("doc1", [0.1, 0.2, 0.3], metadata={"category": "news"})
    results = db.vectors.search([0.1, 0.2, 0.3], top_k=5, filter={"category": "news"})

Usage (async):
    from pulsedb import AsyncPulseDB

    async def main():
        async with AsyncPulseDB(host="localhost", port=6379) as db:
            await db.set("counter", 0)
            await db.incr("counter")
            await db.vectors.upsert("doc1", [0.1, 0.2, 0.3])
"""

from .client import PulseDB
from .async_client import AsyncPulseDB
from .exceptions import (
    PulseDBError,
    ConnectionError,
    AuthenticationError,
    CommandError,
    TimeoutError,
)

__version__ = "1.1.0"
__all__ = [
    "PulseDB",
    "AsyncPulseDB",
    "PulseDBError",
    "ConnectionError",
    "AuthenticationError",
    "CommandError",
    "TimeoutError",
]
