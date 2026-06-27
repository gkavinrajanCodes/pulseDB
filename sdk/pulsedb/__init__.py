# sdk/pulsedb/__init__.py
"""
PulseDB Python SDK

Usage (sync):
    from pulsedb import PulseDB
    db = PulseDB(host="localhost", port=8000, api_key="your-key")
    db.set("user:123", "alice", ttl=3600)
    print(db.get("user:123"))  # "alice"

Usage (async):
    from pulsedb import AsyncPulseDB

    async def main():
        async with AsyncPulseDB(host="localhost", api_key="your-key") as db:
            await db.set("counter", 0)
            await db.incr("counter")
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
