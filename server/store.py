# server/store.py
"""
Sharded in-memory key-value store with:
  - 16 shards for reduced lock contention
  - Lazy TTL expiry (checked on read) + background sweep
  - LRU eviction when max_memory is set
"""

import time
import threading
import hashlib
from collections import OrderedDict


class Shard:
    def __init__(self, max_keys: int = 0):
        """
        max_keys: maximum number of keys in this shard before LRU eviction kicks in.
                  0 means unlimited.
        """
        self.data: OrderedDict = OrderedDict()  # key -> value (LRU ordered)
        self.expiry: dict = {}                  # key -> expiry timestamp
        self.lock = threading.Lock()
        self.max_keys = max_keys

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_expired(self, key: str) -> bool:
        exp = self.expiry.get(key)
        return exp is not None and time.time() > exp

    def _delete_internal(self, key: str):
        self.data.pop(key, None)
        self.expiry.pop(key, None)

    def _touch(self, key: str):
        """Move key to end of OrderedDict (most-recently-used)."""
        if key in self.data:
            self.data.move_to_end(key)

    def _evict_if_needed(self):
        """Evict the least-recently-used key if over the limit."""
        if self.max_keys and len(self.data) > self.max_keys:
            # Evict from the front (least recently used)
            lru_key, _ = next(iter(self.data.items()))
            self._delete_internal(lru_key)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set(self, key: str, value, ttl=None) -> str:
        with self.lock:
            self.data[key] = value
            self._touch(key)
            if ttl is not None:
                self.expiry[key] = time.time() + float(ttl)
            else:
                self.expiry.pop(key, None)
            self._evict_if_needed()
        return "OK"

    def get(self, key: str):
        with self.lock:
            if self._is_expired(key):
                self._delete_internal(key)
                return None
            value = self.data.get(key)
            if value is not None:
                self._touch(key)
            return value

    def delete(self, key: str) -> str:
        with self.lock:
            self._delete_internal(key)
        return "OK"

    def expire(self, key: str, ttl: float) -> int:
        with self.lock:
            if key in self.data:
                self.expiry[key] = time.time() + ttl
                return 1
            return 0

    def ttl(self, key: str) -> int:
        """Returns remaining TTL in seconds. -1 = no TTL. -2 = key not found."""
        with self.lock:
            if key not in self.data:
                return -2
            exp = self.expiry.get(key)
            if exp is None:
                return -1
            remaining = int(exp - time.time())
            return max(0, remaining)

    def exists(self, key: str) -> int:
        with self.lock:
            if self._is_expired(key):
                self._delete_internal(key)
                return 0
            return 1 if key in self.data else 0

    def keys(self) -> list:
        """Return all non-expired keys in this shard."""
        with self.lock:
            now = time.time()
            return [
                k for k in self.data
                if k not in self.expiry or self.expiry[k] > now
            ]

    def cleanup_expired(self, now: float):
        with self.lock:
            to_delete = [k for k, exp in self.expiry.items() if now > exp]
            for k in to_delete:
                self._delete_internal(k)


class Store:
    def __init__(self, num_shards: int = 16, max_memory_keys: int = 0):
        """
        num_shards:      Number of hash shards (default 16).
        max_memory_keys: Max total keys before LRU eviction. 0 = unlimited.
                         Per-shard limit = max_memory_keys // num_shards.
        """
        self.num_shards = num_shards
        per_shard = (max_memory_keys // num_shards) if max_memory_keys else 0
        self.shards = [Shard(max_keys=per_shard) for _ in range(num_shards)]

    def _get_shard(self, key: str) -> Shard:
        idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % self.num_shards
        return self.shards[idx]

    # ------------------------------------------------------------------
    # Core KV operations
    # ------------------------------------------------------------------

    def set(self, key: str, value, ttl=None) -> str:
        return self._get_shard(key).set(key, value, ttl)

    def get(self, key: str):
        return self._get_shard(key).get(key)

    def delete(self, key: str) -> str:
        return self._get_shard(key).delete(key)

    def exists(self, key: str) -> int:
        return self._get_shard(key).exists(key)

    def expire(self, key: str, ttl) -> int:
        return self._get_shard(key).expire(key, float(ttl))

    def ttl(self, key: str) -> int:
        return self._get_shard(key).ttl(key)

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def mset(self, mapping: dict) -> str:
        for key, value in mapping.items():
            self._get_shard(key).set(key, value)
        return "OK"

    def mget(self, keys: list) -> list:
        return [self._get_shard(k).get(k) for k in keys]

    # ------------------------------------------------------------------
    # Key listing
    # ------------------------------------------------------------------

    def keys(self, pattern: str = "*") -> list:
        """Return all keys matching a simple glob pattern (* only)."""
        import fnmatch
        all_keys = []
        for shard in self.shards:
            all_keys.extend(shard.keys())
        if pattern == "*":
            return all_keys
        return [k for k in all_keys if fnmatch.fnmatch(k, pattern)]

    def dbsize(self) -> int:
        return sum(len(s.keys()) for s in self.shards)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def cleanup_expired(self):
        now = time.time()
        for shard in self.shards:
            shard.cleanup_expired(now)

    def flush(self):
        for shard in self.shards:
            with shard.lock:
                shard.data.clear()
                shard.expiry.clear()


# Global singleton (used by commands.py, ttl.py, persistence.py, etc.)
store = Store()