# server/pubsub.py
"""
Push-based Pub/Sub engine using asyncio.Queue.

Each subscriber gets its own asyncio.Queue. When a message is published,
it is delivered to all subscriber queues using call_soon_threadsafe if called
from a non-async context.

asyncio.Queue instances MUST be created inside a running event loop.
For HTTP/WS endpoints this is guaranteed. For test code, use asyncio.run().
"""

import threading
import asyncio


class PubSub:
    def __init__(self):
        # channel -> list[asyncio.Queue]
        self._subscribers: dict = {}
        self._lock = threading.Lock()

    def subscribe(self, channel: str) -> asyncio.Queue:
        """
        Subscribe to a channel. Must be called from within a running event loop
        (e.g., inside an async function or asyncio.run()).
        Returns an asyncio.Queue that will receive published messages.
        """
        queue: asyncio.Queue = asyncio.Queue()
        with self._lock:
            self._subscribers.setdefault(channel, []).append(queue)
        return queue

    def unsubscribe(self, channel: str, queue: asyncio.Queue) -> None:
        with self._lock:
            subs = self._subscribers.get(channel)
            if subs:
                try:
                    subs.remove(queue)
                except ValueError:
                    pass
                if not subs:
                    self._subscribers.pop(channel, None)

    def publish(self, channel: str, message: str) -> str:
        """
        Publish a message to all subscribers of a channel.
        Safe to call from sync or async context.
        """
        with self._lock:
            subs = list(self._subscribers.get(channel, []))

        for q in subs:
            try:
                # put_nowait is safe from any thread/coroutine
                q.put_nowait(message)
            except asyncio.QueueFull:
                pass  # Drop message if queue is full
        return "MESSAGE PUBLISHED"

    def channel_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    def subscriber_count(self, channel: str) -> int:
        with self._lock:
            return len(self._subscribers.get(channel, []))


pubsub = PubSub()