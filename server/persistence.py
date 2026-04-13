# server/persistence.py

import os
import json
import time
import asyncio
import threading

WAL_FILE = os.getenv("WAL_FILE", "pulsedb.wal")
SNAPSHOT_FILE = os.getenv("SNAPSHOT_FILE", "pulsedb.snapshot")
SNAPSHOT_INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", "60"))


class WAL:
    def __init__(self, filename=WAL_FILE):
        self.filename = filename
        self._file = open(self.filename, "a")
        self._lock = threading.Lock()

    def log(self, command, args):
        entry = json.dumps({"command": command, "args": list(args)}) + "\n"
        with self._lock:
            self._file.write(entry)
            self._file.flush()

    def recover_sync(self, store):
        """
        Replay WAL using direct store operations (bypassing async execute).
        Called at startup before the event loop is running.
        """
        if not os.path.exists(self.filename):
            print("No WAL file found. Starting fresh.")
            return

        print(f"Recovering state from WAL: {self.filename} ...")
        recovered = 0
        with open(self.filename, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    cmd = entry["command"].upper()
                    args = entry["args"]
                    self._apply(store, cmd, args)
                    recovered += 1
                except Exception as e:
                    print(f"  [WAL] Skipping bad entry: {e}")
        print(f"  Recovered {recovered} entries from WAL.")

    def _apply(self, store, cmd, args):
        """Apply a single WAL entry directly to the store."""
        if cmd == "SET":
            key = args[0]
            value = args[1]
            ttl = int(args[2]) if len(args) > 2 else None
            store.set(key, value, ttl)
        elif cmd == "MSET":
            store.mset({args[i]: args[i + 1] for i in range(0, len(args), 2)})
        elif cmd == "DEL":
            store.delete(args[0])
        elif cmd == "EXPIRE":
            store.expire(args[0], int(args[1]))

    def save_snapshot(self, store):
        """Save a point-in-time snapshot of all store data."""
        all_data = {}
        for shard in store.shards:
            with shard.lock:
                all_data.update(shard.data)
        with open(SNAPSHOT_FILE, "w") as f:
            json.dump(all_data, f)
        print(f"[Snapshot] Saved {len(all_data)} keys.")

    def load_snapshot(self, store):
        """Load data from a snapshot file into the store."""
        if not os.path.exists(SNAPSHOT_FILE):
            return
        print(f"Loading snapshot from {SNAPSHOT_FILE} ...")
        with open(SNAPSHOT_FILE, "r") as f:
            data = json.load(f)
        for k, v in data.items():
            store.set(k, v)
        print(f"  Loaded {len(data)} keys from snapshot.")

    def close(self):
        with self._lock:
            self._file.close()


wal = WAL()


def _snapshot_loop(store):
    while True:
        time.sleep(SNAPSHOT_INTERVAL)
        try:
            wal.save_snapshot(store)
        except Exception as e:
            print(f"[Snapshot] Error: {e}")


def start_persistence(store):
    """Load previous state and start background snapshotting."""
    # 1. Load snapshot first (faster than WAL replay for large datasets)
    wal.load_snapshot(store)
    # 2. Replay WAL on top of snapshot
    wal.recover_sync(store)
    # 3. Start background snapshot thread
    t = threading.Thread(target=_snapshot_loop, args=(store,), daemon=True)
    t.start()
    print("[Persistence] WAL recovery and snapshotting started.")
