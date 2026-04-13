# server/commands.py

from server.store import store
from server.pubsub import pubsub
from server.persistence import wal
from server.cluster import cluster_manager


async def execute(command, args, persist=True):
    command = command.upper()

    # Routing logic (for single-key commands)
    if command in ("SET", "GET", "DEL", "EXPIRE"):
        key = args[0]
        if not cluster_manager.is_local(key):
            target_node = cluster_manager.get_target_node(key)
            return await cluster_manager.forward_command(target_node, command, args)

    # --- Mutation commands ---
    if command == "SET":
        key = args[0]
        value = args[1]
        ttl = int(args[2]) if len(args) > 2 else None
        if persist:
            wal.log(command, args)
            # Replication: send to replicas (fire-and-forget is fine here)
            nodes = cluster_manager.get_nodes(key)
            for replica in nodes[1:]:
                await cluster_manager.forward_command(replica, command, args)
        return store.set(key, value, ttl)

    elif command == "MSET":
        if persist:
            wal.log(command, args)
        return store.mset({args[i]: args[i + 1] for i in range(0, len(args), 2)})

    elif command == "EXPIRE":
        key = args[0]
        ttl = int(args[1])
        if persist:
            wal.log(command, args)
        return store.expire(key, ttl)

    elif command == "DEL":
        key = args[0]
        if persist:
            wal.log(command, args)
        return store.delete(key)

    # --- Read commands ---
    elif command == "GET":
        key = args[0]
        value = store.get(key)
        return value if value is not None else "NULL"

    elif command == "MGET":
        results = store.mget(args)
        return [v if v is not None else "NULL" for v in results]

    # --- Pub/Sub commands ---
    elif command == "PUBLISH":
        channel = args[0]
        message = args[1]
        return pubsub.publish(channel, message)

    elif command == "SUBSCRIBE":
        # SUBSCRIBE via HTTP is not meaningful (use WebSocket).
        # Return a message count for the channel.
        channel = args[0]
        return f"Subscribed to '{channel}'. Use WebSocket /ws/subscribe/{channel} for real-time messages."

    else:
        return f"ERROR: Unknown command '{command}'"