# server/types.py
"""
Rich data types for PulseDB: Lists and Hashes.

Stored separately from the string KV store to keep the hot path fast.
Each type has its own sharded structure with per-shard locking.
"""

import threading
import hashlib
from collections import deque


# ---------------------------------------------------------------------------
# List Store
# ---------------------------------------------------------------------------

class ListStore:
    """
    Implements Redis-compatible list operations.
    Internally uses collections.deque for O(1) push/pop at both ends.
    """

    def __init__(self, num_shards: int = 16):
        self._shards: list[dict] = [{} for _ in range(num_shards)]
        self._locks: list[threading.Lock] = [threading.Lock() for _ in range(num_shards)]
        self._num_shards = num_shards

    def _shard(self, key: str):
        idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % self._num_shards
        return self._shards[idx], self._locks[idx]

    def lpush(self, key: str, *values) -> int:
        data, lock = self._shard(key)
        with lock:
            if key not in data:
                data[key] = deque()
            for v in values:
                data[key].appendleft(str(v))
            return len(data[key])

    def rpush(self, key: str, *values) -> int:
        data, lock = self._shard(key)
        with lock:
            if key not in data:
                data[key] = deque()
            for v in values:
                data[key].append(str(v))
            return len(data[key])

    def lpop(self, key: str) -> str | None:
        data, lock = self._shard(key)
        with lock:
            lst = data.get(key)
            if not lst:
                return None
            val = lst.popleft()
            if not lst:
                del data[key]
            return val

    def rpop(self, key: str) -> str | None:
        data, lock = self._shard(key)
        with lock:
            lst = data.get(key)
            if not lst:
                return None
            val = lst.pop()
            if not lst:
                del data[key]
            return val

    def lrange(self, key: str, start: int, stop: int) -> list:
        data, lock = self._shard(key)
        with lock:
            lst = data.get(key)
            if not lst:
                return []
            lst_list = list(lst)
            length = len(lst_list)
            # Normalize negative indices
            if start < 0:
                start = max(0, length + start)
            if stop < 0:
                stop = length + stop
            stop = min(stop, length - 1)
            if start > stop:
                return []
            return lst_list[start: stop + 1]

    def llen(self, key: str) -> int:
        data, lock = self._shard(key)
        with lock:
            return len(data.get(key, []))

    def lindex(self, key: str, index: int) -> str | None:
        data, lock = self._shard(key)
        with lock:
            lst = data.get(key)
            if not lst:
                return None
            try:
                return list(lst)[index]
            except IndexError:
                return None

    def lset(self, key: str, index: int, value: str) -> str:
        data, lock = self._shard(key)
        with lock:
            lst = data.get(key)
            if not lst:
                return "ERROR: no such key"
            lst_list = list(lst)
            try:
                lst_list[index] = value
            except IndexError:
                return "ERROR: index out of range"
            data[key] = deque(lst_list)
            return "OK"

    def lrem(self, key: str, count: int, value: str) -> int:
        data, lock = self._shard(key)
        with lock:
            lst = data.get(key)
            if not lst:
                return 0
            lst_list = list(lst)
            removed = 0
            new_list = []
            items = lst_list if count >= 0 else reversed(lst_list)
            for item in lst_list:
                if item == value and (count == 0 or removed < abs(count)):
                    removed += 1
                else:
                    new_list.append(item)
            data[key] = deque(new_list)
            return removed


# ---------------------------------------------------------------------------
# Hash Store
# ---------------------------------------------------------------------------

class HashStore:
    """
    Implements Redis-compatible hash (HSET/HGET/HDEL/HGETALL) operations.
    Each key maps to a plain dict of field→value.
    """

    def __init__(self, num_shards: int = 16):
        self._shards: list[dict] = [{} for _ in range(num_shards)]
        self._locks: list[threading.Lock] = [threading.Lock() for _ in range(num_shards)]
        self._num_shards = num_shards

    def _shard(self, key: str):
        idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % self._num_shards
        return self._shards[idx], self._locks[idx]

    def hset(self, key: str, field: str, value: str) -> int:
        """Returns 1 if field is new, 0 if updated."""
        data, lock = self._shard(key)
        with lock:
            h = data.setdefault(key, {})
            is_new = field not in h
            h[field] = str(value)
            return 1 if is_new else 0

    def hmset(self, key: str, mapping: dict) -> str:
        data, lock = self._shard(key)
        with lock:
            h = data.setdefault(key, {})
            for f, v in mapping.items():
                h[f] = str(v)
        return "OK"

    def hget(self, key: str, field: str) -> str | None:
        data, lock = self._shard(key)
        with lock:
            return data.get(key, {}).get(field)

    def hmget(self, key: str, *fields) -> list:
        data, lock = self._shard(key)
        with lock:
            h = data.get(key, {})
            return [h.get(f) for f in fields]

    def hdel(self, key: str, *fields) -> int:
        data, lock = self._shard(key)
        with lock:
            h = data.get(key, {})
            removed = sum(1 for f in fields if h.pop(f, None) is not None)
            if not h:
                data.pop(key, None)
            return removed

    def hgetall(self, key: str) -> dict:
        data, lock = self._shard(key)
        with lock:
            return dict(data.get(key, {}))

    def hkeys(self, key: str) -> list:
        data, lock = self._shard(key)
        with lock:
            return list(data.get(key, {}).keys())

    def hvals(self, key: str) -> list:
        data, lock = self._shard(key)
        with lock:
            return list(data.get(key, {}).values())

    def hlen(self, key: str) -> int:
        data, lock = self._shard(key)
        with lock:
            return len(data.get(key, {}))

    def hexists(self, key: str, field: str) -> int:
        data, lock = self._shard(key)
        with lock:
            return 1 if field in data.get(key, {}) else 0

    def hincrby(self, key: str, field: str, by: int) -> int:
        data, lock = self._shard(key)
        with lock:
            h = data.setdefault(key, {})
            current = int(h.get(field, 0))
            new_val = current + by
            h[field] = str(new_val)
            return new_val


# ---------------------------------------------------------------------------
# Global singletons
# ---------------------------------------------------------------------------
list_store = ListStore()
hash_store = HashStore()
