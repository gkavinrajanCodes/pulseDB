# server/persistence.py
import os
import json
import time
import threading

from server.config import WAL_FILE, SNAPSHOT_FILE, SNAPSHOT_INTERVAL, VECTOR_INDEX_FILE, VECTOR_META_FILE
from server.vector import vector_index


class WAL:
    def __init__(self, filename: str = WAL_FILE):
        self.filename = filename
        self._file = open(self.filename, "a")
        self._lock = threading.Lock()

    def log(self, command: str, args: list):
        entry = json.dumps({"command": command, "args": list(args)}) + "\n"
        with self._lock:
            self._file.write(entry)
            self._file.flush()

    def recover_sync(self, store):
        """Replay WAL at startup (synchronous — before event loop starts)."""
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
                    self._apply(store, entry["command"].upper(), entry["args"])
                    recovered += 1
                except Exception as e:
                    print(f"  [WAL] Skipping bad entry: {e}")
        print(f"  Recovered {recovered} entries from WAL.")

    def _apply(self, store, cmd: str, args: list):
        """Apply one WAL entry directly to the store (no async needed)."""
        if cmd == "SET":
            ttl = int(args[2]) if len(args) > 2 else None
            store.set(args[0], args[1], ttl)
        elif cmd == "MSET":
            store.mset({args[i]: args[i + 1] for i in range(0, len(args), 2)})
        elif cmd == "DEL":
            store.delete(args[0])
        elif cmd == "EXPIRE":
            store.expire(args[0], int(args[1]))

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def save_snapshot(self, store):
        """
        Save a point-in-time snapshot, then compact the WAL.
        After compaction the WAL only contains entries written after this snapshot,
        so it never grows unboundedly.
        """
        # 1. Collect all live data
        all_data = {}
        for shard in store.shards:
            with shard.lock:
                all_data.update(dict(shard.data))

        # 2. Write snapshot atomically (write to tmp, then rename)
        tmp_path = SNAPSHOT_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(all_data, f)
        os.replace(tmp_path, SNAPSHOT_FILE)
        print(f"[Snapshot] Saved {len(all_data)} keys → {SNAPSHOT_FILE}")

        # 2b. Snapshot the Vector Index
        try:
            vector_index.save(VECTOR_INDEX_FILE, VECTOR_META_FILE)
        except Exception as e:
            print(f"[Snapshot] Error saving vector index: {e}")

        # 3. Compact the WAL — truncate it now that the snapshot is durable
        self._compact_wal()

    def _compact_wal(self):
        """Truncate the WAL file after a successful snapshot."""
        with self._lock:
            try:
                self._file.close()
                # Overwrite with empty file
                open(self.filename, "w").close()
                # Re-open for appending
                self._file = open(self.filename, "a")
                print(f"[WAL] Compacted — WAL reset after snapshot.")
            except Exception as e:
                print(f"[WAL] Compaction error: {e}")
                # Re-open in append mode regardless
                self._file = open(self.filename, "a")

    def load_snapshot(self, store):
        """Load data from the snapshot file into the store."""
        if not os.path.exists(SNAPSHOT_FILE):
            return
        print(f"Loading snapshot from {SNAPSHOT_FILE} ...")
        with open(SNAPSHOT_FILE, "r") as f:
            data = json.load(f)
        for k, v in data.items():
            store.set(k, v)
        print(f"  Loaded {len(data)} keys from snapshot.")
        
        # Load Vector Index
        try:
            vector_index.load(VECTOR_INDEX_FILE, VECTOR_META_FILE)
        except Exception as e:
            print(f"[Snapshot] Error loading vector index: {e}")

    def close(self):
        with self._lock:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass


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
    wal.load_snapshot(store)
    wal.recover_sync(store)
    t = threading.Thread(target=_snapshot_loop, args=(store,), daemon=True)
    t.start()
    print("[Persistence] WAL recovery and snapshotting started.")
