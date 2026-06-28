# Copyright (c) 2026 G Kavinrajan. All rights reserved.
# Licensed under the Business Source License 1.1

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
            h[field] = value
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


# ---------------------------------------------------------------------------
# Sorted Set Store
# ---------------------------------------------------------------------------

class SortedSetStore:
    """
    Implements Redis-compatible sorted set operations.

    Uses Python's built-in `bisect` module for O(log N) inserts and range queries.
    Each sorted set is stored as a dict of member→score and a sorted list of
    (score, member) tuples for range queries.
    """

    def __init__(self, num_shards: int = 16):
        import bisect
        self._bisect = bisect
        self._shards: list[dict] = [{}  for _ in range(num_shards)]
        self._locks: list[threading.Lock] = [threading.Lock() for _ in range(num_shards)]
        self._num_shards = num_shards

    def _shard(self, key: str):
        idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % self._num_shards
        return self._shards[idx], self._locks[idx]

    def _get_zset(self, data: dict, key: str) -> dict:
        """Return or create the zset structure: {"scores": {member: score}, "sorted": [(score, member)]}"""
        if key not in data:
            data[key] = {"scores": {}, "sorted": []}
        return data[key]

    def zadd(self, key: str, mapping: dict) -> int:
        """Add members with scores. Returns number of NEW members added."""
        data, lock = self._shard(key)
        with lock:
            zset = self._get_zset(data, key)
            added = 0
            for member, score in mapping.items():
                score = float(score)
                old_score = zset["scores"].get(member)
                if old_score is not None:
                    # Remove old entry from sorted list
                    idx = self._bisect.bisect_left(zset["sorted"], (old_score, member))
                    if idx < len(zset["sorted"]) and zset["sorted"][idx] == (old_score, member):
                        zset["sorted"].pop(idx)
                else:
                    added += 1
                zset["scores"][member] = score
                self._bisect.insort(zset["sorted"], (score, member))
            return added

    def zscore(self, key: str, member: str):
        """Get score of a member. Returns None if not found."""
        data, lock = self._shard(key)
        with lock:
            zset = data.get(key)
            if not zset:
                return None
            return zset["scores"].get(member)

    def zrank(self, key: str, member: str):
        """Get 0-based rank of member (ascending). Returns None if not found."""
        data, lock = self._shard(key)
        with lock:
            zset = data.get(key)
            if not zset or member not in zset["scores"]:
                return None
            score = zset["scores"][member]
            idx = self._bisect.bisect_left(zset["sorted"], (score, member))
            return idx

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> list:
        """Return members in ascending score order by rank range [start, stop]."""
        data, lock = self._shard(key)
        with lock:
            zset = data.get(key)
            if not zset:
                return []
            length = len(zset["sorted"])
            if start < 0:
                start = max(0, length + start)
            if stop < 0:
                stop = length + stop
            stop = min(stop, length - 1)
            if start > stop:
                return []
            items = zset["sorted"][start: stop + 1]
            if withscores:
                result = []
                for score, member in items:
                    result.extend([member, str(score)])
                return result
            return [member for _, member in items]

    def zrevrange(self, key: str, start: int, stop: int, withscores: bool = False) -> list:
        """Return members in descending score order by rank range [start, stop]."""
        data, lock = self._shard(key)
        with lock:
            zset = data.get(key)
            if not zset:
                return []
            reversed_list = list(reversed(zset["sorted"]))
            length = len(reversed_list)
            if start < 0:
                start = max(0, length + start)
            if stop < 0:
                stop = length + stop
            stop = min(stop, length - 1)
            if start > stop:
                return []
            items = reversed_list[start: stop + 1]
            if withscores:
                result = []
                for score, member in items:
                    result.extend([member, str(score)])
                return result
            return [member for _, member in items]

    def zrangebyscore(self, key: str, min_score: float, max_score: float, withscores: bool = False) -> list:
        """Return members with scores between min_score and max_score (inclusive)."""
        data, lock = self._shard(key)
        with lock:
            zset = data.get(key)
            if not zset:
                return []
            lo = self._bisect.bisect_left(zset["sorted"], (min_score, ""))
            hi = self._bisect.bisect_right(zset["sorted"], (max_score, "\xff\xff\xff\xff"))
            items = zset["sorted"][lo:hi]
            if withscores:
                result = []
                for score, member in items:
                    result.extend([member, str(score)])
                return result
            return [member for _, member in items]

    def zrem(self, key: str, *members) -> int:
        """Remove one or more members. Returns count of removed members."""
        data, lock = self._shard(key)
        with lock:
            zset = data.get(key)
            if not zset:
                return 0
            removed = 0
            for member in members:
                score = zset["scores"].pop(member, None)
                if score is not None:
                    idx = self._bisect.bisect_left(zset["sorted"], (score, member))
                    if idx < len(zset["sorted"]) and zset["sorted"][idx] == (score, member):
                        zset["sorted"].pop(idx)
                    removed += 1
            if not zset["scores"]:
                data.pop(key, None)
            return removed

    def zcard(self, key: str) -> int:
        """Return the number of members in the sorted set."""
        data, lock = self._shard(key)
        with lock:
            zset = data.get(key)
            return len(zset["scores"]) if zset else 0

    def zincrby(self, key: str, increment: float, member: str) -> float:
        """Increment the score of a member."""
        data, lock = self._shard(key)
        with lock:
            zset = self._get_zset(data, key)
            old_score = zset["scores"].get(member)
            if old_score is not None:
                idx = self._bisect.bisect_left(zset["sorted"], (old_score, member))
                if idx < len(zset["sorted"]) and zset["sorted"][idx] == (old_score, member):
                    zset["sorted"].pop(idx)
            new_score = (old_score or 0.0) + float(increment)
            zset["scores"][member] = new_score
            self._bisect.insort(zset["sorted"], (new_score, member))
            return new_score

    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """Count members with scores between min and max (inclusive)."""
        return len(self.zrangebyscore(key, min_score, max_score))


zset_store = SortedSetStore()
