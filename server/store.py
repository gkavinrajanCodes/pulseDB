# server/store.py

import time
import threading
import hashlib

class Shard:
    def __init__(self):
        self.data = {}
        self.expiry = {}
        self.lock = threading.Lock()

    def set(self, key, value, ttl=None):
        with self.lock:
            self.data[key] = value
            if ttl:
                self.expiry[key] = time.time() + ttl
            else:
                self.expiry.pop(key, None)
        return "OK"

    def mset(self, mapping):
        # Note: This is not atomic across shards, but we can make it atomic per shard
        # In this implementation, we just call set for each key
        for key, value in mapping.items():
            self.set(key, value)
        return "OK"

    def get(self, key):
        with self.lock:
            if key in self.expiry:
                if time.time() > self.expiry[key]:
                    self._delete_internal(key)
                    return None
            return self.data.get(key)

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def delete(self, key):
        with self.lock:
            self._delete_internal(key)
        return "OK"

    def expire(self, key, ttl):
        with self.lock:
            if key in self.data:
                self.expiry[key] = time.time() + ttl
                return 1
            return 0

    def _delete_internal(self, key):
        self.data.pop(key, None)
        self.expiry.pop(key, None)

    def cleanup_expired(self, now):
        with self.lock:
            keys_to_delete = [
                key for key, exp in self.expiry.items()
                if now > exp
            ]
            for key in keys_to_delete:
                self._delete_internal(key)

class Store:
    def __init__(self, num_shards=16):
        self.num_shards = num_shards
        self.shards = [Shard() for _ in range(num_shards)]

    def _get_shard(self, key):
        shard_idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % self.num_shards
        return self.shards[shard_idx]

    def set(self, key, value, ttl=None):
        return self._get_shard(key).set(key, value, ttl)

    def mset(self, mapping):
        for key, value in mapping.items():
            self._get_shard(key).set(key, value)
        return "OK"

    def get(self, key):
        return self._get_shard(key).get(key)

    def mget(self, keys):
        return [self._get_shard(key).get(key) for key in keys]

    def delete(self, key):
        return self._get_shard(key).delete(key)

    def expire(self, key, ttl):
        return self._get_shard(key).expire(key, ttl)

    def cleanup_expired(self):
        now = time.time()
        for shard in self.shards:
            shard.cleanup_expired(now)

store = Store()