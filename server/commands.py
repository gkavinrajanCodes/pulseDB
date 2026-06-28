# server/commands.py
"""
PulseDB command executor.

Handles routing (single-node or cluster), persistence (WAL), replication,
and dispatches to the in-memory store or pub/sub engine.
"""

from server.store import store
from server.pubsub import pubsub
from server.persistence import wal
from server.cluster import cluster_manager
from server.vector import vector_index
from server.data_types import list_store, hash_store, zset_store
import numpy as np



async def execute(command: str, args: list, persist: bool = True):
    command = command.upper()

    # ------------------------------------------------------------------
    # Binary Protocol Handling (AI Memory Layer)
    # ------------------------------------------------------------------
    if command == "VECTOR.BSET":
        if len(args) < 2:
            return "ERROR: VECTOR.BSET requires key and binary blob"
        key = args[0].decode("utf-8", errors="replace") if isinstance(args[0], bytes) else args[0]
        blob = args[1]
        if not isinstance(blob, bytes):
            return "ERROR: BSET payload must be binary bytes"
            
        metadata = None
        if len(args) >= 4 and (args[2].upper() == b"METADATA" or args[2] == "METADATA"):
            import json
            try:
                meta_str = args[3].decode("utf-8") if isinstance(args[3], bytes) else args[3]
                metadata = json.loads(meta_str)
            except Exception:
                return "ERROR: Invalid METADATA JSON"
                
        try:
            vector = np.frombuffer(blob, dtype=np.float32).tolist()
            return vector_index.set(key, vector, metadata)
        except Exception as e:
            return f"ERROR: Invalid binary blob: {e}"

    elif command == "VECTOR.BSET_BATCH":
        # VECTOR.BSET_BATCH <json_array>
        # Each element: {"id": str, "blob": hex_encoded_bytes, "metadata": dict (optional)}
        if len(args) < 1:
            return "ERROR: VECTOR.BSET_BATCH requires a JSON payload"
        import json
        try:
            payload_str = args[0].decode("utf-8") if isinstance(args[0], bytes) else args[0]
            items = json.loads(payload_str)
        except Exception:
            return "ERROR: Invalid BSET_BATCH JSON payload"
        if not isinstance(items, list):
            return "ERROR: BSET_BATCH payload must be a JSON array"

        inserted = 0
        errors = []
        for item in items:
            try:
                key = str(item["id"])
                blob = bytes.fromhex(item["blob"])
                metadata = item.get("metadata")
                vector = np.frombuffer(blob, dtype=np.float32).tolist()
                result = vector_index.set(key, vector, metadata)
                if result == "OK":
                    inserted += 1
                else:
                    errors.append(f"{key}: {result}")
            except Exception as e:
                errors.append(f"{item.get('id', '?')}: {e}")

        if errors:
            return f"PARTIAL: {inserted} inserted, errors: {'; '.join(errors[:3])}"
        return f"OK:{inserted}"

    elif command == "VECTOR.BSEARCH":
        try:
            blob = args[0]
            if not isinstance(blob, bytes):
                return "ERROR: BSEARCH payload must be binary bytes"
            
            top_k_str = args[2].decode("utf-8") if isinstance(args[2], bytes) else args[2]
            top_k = int(top_k_str)
            
            filter_dict = None
            if len(args) >= 5 and (args[3].upper() == b"FILTER" or args[3] == "FILTER"):
                import json
                try:
                    filter_str = args[4].decode("utf-8") if isinstance(args[4], bytes) else args[4]
                    filter_dict = json.loads(filter_str)
                except Exception:
                    return "ERROR: Invalid FILTER JSON"
            
            query = np.frombuffer(blob, dtype=np.float32).tolist()
            results = vector_index.search(query, top_k, filter_dict)
            
            flat = []
            for k, score in results:
                flat.extend([k, f"{score:.6f}"])
            return flat
        except Exception as e:
            return f"ERROR: {str(e)}"
            
    # Decode arguments for all standard text-based commands
    args_str = []
    for a in args:
        if isinstance(a, bytes):
            args_str.append(a.decode("utf-8", errors="replace"))
        else:
            args_str.append(a)
    args = args_str

    # ------------------------------------------------------------------
    # Cluster routing: for single-key commands, forward to the owner node
    # ------------------------------------------------------------------
    if command in ("SET", "GET", "DEL", "EXPIRE", "TTL", "EXISTS", "INCR", "DECR", "APPEND", "GETSET"):
        if args:
            key = args[0]
            if not cluster_manager.is_local(key):
                target = cluster_manager.get_target_node(key)
                return await cluster_manager.forward_command(target, command, args)

    # ------------------------------------------------------------------
    # Write commands
    # ------------------------------------------------------------------
    if command == "SET":
        key = args[0]
        value = args[1]
        ttl = None
        # Parse SET key value [EX seconds] [PX milliseconds]
        i = 2
        while i < len(args):
            opt = args[i].upper()
            if opt == "EX" and i + 1 < len(args):
                ttl = float(args[i + 1])
                i += 2
            elif opt == "PX" and i + 1 < len(args):
                ttl = float(args[i + 1]) / 1000.0
                i += 2
            else:
                # Positional TTL (legacy PulseDB style)
                try:
                    ttl = float(args[i])
                except ValueError:
                    pass
                i += 1
        if persist:
            wal.log(command, args)
            nodes = cluster_manager.get_nodes(key)
            for replica in nodes[1:]:
                await cluster_manager.forward_command(replica, command, args)
        return store.set(key, value, ttl)

    elif command == "MSET":
        if persist:
            wal.log(command, args)
        return store.mset({args[i]: args[i + 1] for i in range(0, len(args), 2)})

    elif command in ("DEL", "DELETE"):
        key = args[0]
        if persist:
            wal.log("DEL", args)
        return store.delete(key)

    elif command == "EXPIRE":
        key = args[0]
        ttl = float(args[1])
        if persist:
            wal.log(command, args)
        return store.expire(key, ttl)

    elif command == "INCR":
        key = args[0]
        val = store.get(key)
        if val is None:
            val = 0
        try:
            new_val = int(val) + 1
        except (ValueError, TypeError):
            return "ERROR: value is not an integer"
        store.set(key, str(new_val))
        if persist:
            wal.log(command, args)
        return new_val

    elif command == "INCRBY":
        key = args[0]
        by = int(args[1])
        val = store.get(key)
        if val is None:
            val = 0
        try:
            new_val = int(val) + by
        except (ValueError, TypeError):
            return "ERROR: value is not an integer"
        store.set(key, str(new_val))
        if persist:
            wal.log(command, args)
        return new_val

    elif command == "DECR":
        key = args[0]
        val = store.get(key)
        if val is None:
            val = 0
        try:
            new_val = int(val) - 1
        except (ValueError, TypeError):
            return "ERROR: value is not an integer"
        store.set(key, str(new_val))
        if persist:
            wal.log(command, args)
        return new_val

    elif command == "DECRBY":
        key = args[0]
        by = int(args[1])
        val = store.get(key)
        if val is None:
            val = 0
        try:
            new_val = int(val) - by
        except (ValueError, TypeError):
            return "ERROR: value is not an integer"
        store.set(key, str(new_val))
        if persist:
            wal.log(command, args)
        return new_val

    elif command == "APPEND":
        key = args[0]
        suffix = args[1]
        existing = store.get(key)
        existing_str = str(existing) if existing is not None else ""
        appended_val = existing_str + str(suffix)
        store.set(key, appended_val)
        return len(appended_val)

    elif command == "GETSET":
        key = args[0]
        new_value = args[1]
        old_value = store.get(key)
        store.set(key, new_value)
        if persist:
            wal.log(command, args)
        return old_value

    # ------------------------------------------------------------------
    # Read commands
    # ------------------------------------------------------------------
    elif command == "GET":
        value = store.get(args[0])
        return value if value is not None else "NULL"

    elif command == "MGET":
        results = store.mget(args)
        return [v if v is not None else "NULL" for v in results]

    elif command == "EXISTS":
        return store.exists(args[0])

    elif command == "TTL":
        return store.ttl(args[0])

    elif command == "KEYS":
        pattern = args[0] if args else "*"
        return store.keys(pattern)

    elif command == "DBSIZE":
        return store.dbsize()

    elif command == "TYPE":
        # All values are strings in PulseDB v1
        val = store.get(args[0])
        return "string" if val is not None else "none"

    # ------------------------------------------------------------------
    # Pub/Sub commands
    # ------------------------------------------------------------------
    elif command == "PUBLISH":
        channel = args[0]
        message = args[1]
        return pubsub.publish(channel, message)

    elif command == "SUBSCRIBE":
        channel = args[0]
        return f"Subscribed to '{channel}'. Use WebSocket /ws/subscribe/{channel} for real-time messages."

    elif command == "PUBSUB":
        sub_cmd = args[0].upper() if args else ""
        if sub_cmd == "CHANNELS":
            return list(pubsub._subscribers.keys())
        if sub_cmd == "NUMSUB" and len(args) > 1:
            return [args[1], pubsub.subscriber_count(args[1])]
        return []

    # ------------------------------------------------------------------
    # Admin commands
    # ------------------------------------------------------------------
    elif command == "FLUSHDB":
        store.flush()
        return "OK"

    elif command == "PING":
        msg = args[0] if args else "PONG"
        return msg

    elif command == "INFO":
        from server.store import store as s
        total_keys = s.dbsize()
        return (
            f"# Server\r\npulsedb_version:1.1.0\r\n"
            f"# Keyspace\r\ndb0:keys={total_keys},expires=0\r\n"
        )

    # ------------------------------------------------------------------
    # Vector search commands (AI Memory Layer)
    # ------------------------------------------------------------------
    elif command == "VECTOR.SET":
        # VECTOR.SET key dim1 dim2 ... dimN
        if len(args) < 2:
            return "ERROR: VECTOR.SET requires key and at least one dimension"
        key = args[0]
        try:
            vector = [float(x) for x in args[1:]]
        except ValueError:
            return "ERROR: vector dimensions must be floats"
        return vector_index.set(key, vector)

    elif command == "VECTOR.GET":
        vec = vector_index.get(args[0])
        if vec is None:
            return "NULL"
        import json
        return json.dumps(vec)

    elif command == "VECTOR.DEL":
        return vector_index.delete(args[0])

    elif command == "VECTOR.SEARCH":
        # VECTOR.SEARCH dim1 dim2 ... dimN TOP_K k
        try:
            top_k_idx = next(
                i for i, a in enumerate(args) if a.upper() == "TOP_K"
            )
            top_k = int(args[top_k_idx + 1])
            vector_args = args[:top_k_idx]
        except (StopIteration, IndexError, ValueError):
            vector_args = args
            top_k = 5
        try:
            query = [float(x) for x in vector_args]
        except ValueError:
            return "ERROR: query vector dimensions must be floats"
        try:
            results = vector_index.search(query, top_k)
        except ValueError as e:
            return f"ERROR: {str(e)}"
            
        flat = []
        for key, score in results:
            flat.extend([key, f"{score:.6f}"])
        return flat

    elif command == "VECTOR.COUNT":
        return vector_index.count()

    # ------------------------------------------------------------------
    # List commands
    # ------------------------------------------------------------------
    elif command == "LPUSH":
        return list_store.lpush(args[0], *args[1:])

    elif command == "RPUSH":
        return list_store.rpush(args[0], *args[1:])

    elif command == "LPOP":
        v = list_store.lpop(args[0])
        return v if v is not None else "NULL"

    elif command == "RPOP":
        v = list_store.rpop(args[0])
        return v if v is not None else "NULL"

    elif command == "LRANGE":
        return list_store.lrange(args[0], int(args[1]), int(args[2]))

    elif command == "LLEN":
        return list_store.llen(args[0])

    elif command == "LINDEX":
        v = list_store.lindex(args[0], int(args[1]))
        return v if v is not None else "NULL"

    elif command == "LSET":
        return list_store.lset(args[0], int(args[1]), args[2])

    elif command == "LREM":
        return list_store.lrem(args[0], int(args[1]), args[2])

    # ------------------------------------------------------------------
    # Hash commands
    # ------------------------------------------------------------------
    elif command == "HSET":
        # HSET key field value [field value ...]
        key = args[0]
        added = 0
        for i in range(1, len(args), 2):
            added += hash_store.hset(key, args[i], args[i + 1])
        return added

    elif command == "HMSET":
        key = args[0]
        mapping = {args[i]: args[i + 1] for i in range(1, len(args), 2)}
        return hash_store.hmset(key, mapping)

    elif command == "HGET":
        v = hash_store.hget(args[0], args[1])
        return v if v is not None else "NULL"

    elif command == "HMGET":
        results = hash_store.hmget(args[0], *args[1:])
        return [v if v is not None else "NULL" for v in results]

    elif command == "HDEL":
        return hash_store.hdel(args[0], *args[1:])

    elif command == "HGETALL":
        h = hash_store.hgetall(args[0])
        # RESP flat array: field1, val1, field2, val2, ...
        flat = []
        for f, v in h.items():
            flat.extend([f, str(v) if v is not None else ""])
        return flat

    elif command == "HKEYS":
        return hash_store.hkeys(args[0])

    elif command == "HVALS":
        return hash_store.hvals(args[0])

    elif command == "HLEN":
        return hash_store.hlen(args[0])

    elif command == "HEXISTS":
        return hash_store.hexists(args[0], args[1])

    elif command == "HINCRBY":
        return hash_store.hincrby(args[0], args[1], int(args[2]))

    # ------------------------------------------------------------------
    # Sorted Set commands (ZADD / ZRANGE / ZRANGEBYSCORE / ZRANK / ZSCORE)
    # ------------------------------------------------------------------
    elif command == "ZADD":
        # ZADD key score member [score member ...]
        if len(args) < 3:
            return "ERROR: ZADD requires key score member"
        key = args[0]
        try:
            mapping = {args[i+1]: float(args[i]) for i in range(1, len(args)-1, 2)}
        except (ValueError, IndexError):
            return "ERROR: ZADD score must be a float"
        return zset_store.zadd(key, mapping)

    elif command == "ZSCORE":
        v = zset_store.zscore(args[0], args[1])
        return str(v) if v is not None else "NULL"

    elif command == "ZRANK":
        v = zset_store.zrank(args[0], args[1])
        return v if v is not None else "NULL"

    elif command == "ZRANGE":
        withscores = len(args) > 3 and args[3].upper() == "WITHSCORES"
        return zset_store.zrange(args[0], int(args[1]), int(args[2]), withscores=withscores)

    elif command == "ZREVRANGE":
        withscores = len(args) > 3 and args[3].upper() == "WITHSCORES"
        return zset_store.zrevrange(args[0], int(args[1]), int(args[2]), withscores=withscores)

    elif command == "ZRANGEBYSCORE":
        withscores = "WITHSCORES" in [a.upper() for a in args[3:]]
        try:
            min_s = float("-inf") if args[1] == "-inf" else float(args[1])
            max_s = float("+inf") if args[2] == "+inf" else float(args[2])
        except ValueError:
            return "ERROR: ZRANGEBYSCORE min/max must be floats or -inf/+inf"
        return zset_store.zrangebyscore(args[0], min_s, max_s, withscores=withscores)

    elif command == "ZREM":
        return zset_store.zrem(args[0], *args[1:])

    elif command == "ZCARD":
        return zset_store.zcard(args[0])

    elif command == "ZINCRBY":
        return zset_store.zincrby(args[0], float(args[1]), args[2])

    elif command == "ZCOUNT":
        try:
            min_s = float("-inf") if args[1] == "-inf" else float(args[1])
            max_s = float("+inf") if args[2] == "+inf" else float(args[2])
        except ValueError:
            return "ERROR: ZCOUNT min/max must be floats or -inf/+inf"
        return zset_store.zcount(args[0], min_s, max_s)

    else:
        return f"ERROR: Unknown command '{command}'"