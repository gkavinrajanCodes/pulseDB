# server/cluster.py

import hashlib
import bisect
import asyncio
from server.protocol import encode_message, decode_message, TYPE_REQUEST


class ConsistentHash:
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = []
        self.nodes = {}  # hash -> node_name
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        for i in range(self.replicas):
            h = self._hash(f"{node}:{i}")
            bisect.insort(self.ring, h)
            self.nodes[h] = node

    def remove_node(self, node):
        for i in range(self.replicas):
            h = self._hash(f"{node}:{i}")
            idx = bisect.bisect_left(self.ring, h)
            if idx < len(self.ring) and self.ring[idx] == h:
                self.ring.pop(idx)
                self.nodes.pop(h, None)  # Fixed: use .pop() instead of del

    def get_node(self, key):
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect_left(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        return self.nodes[self.ring[idx]]

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)


class ClusterManager:
    def __init__(self, current_node_id, all_nodes):
        self.current_node_id = current_node_id
        self.chash = ConsistentHash(all_nodes)
        self.all_nodes = all_nodes
        # Node address map: node_id -> (host, tcp_port)
        self.node_addresses = self._build_address_map(all_nodes)

    def _build_address_map(self, nodes):
        """Build a simple address map from env or defaults."""
        import os
        addr_map = {}
        for i, node in enumerate(nodes):
            host = os.getenv(f"{node.upper()}_HOST", "localhost")
            port = int(os.getenv(f"{node.upper()}_PORT", str(6379 + i)))
            addr_map[node] = (host, port)
        return addr_map

    def is_local(self, key):
        node = self.chash.get_node(key)
        return node == self.current_node_id

    def get_target_node(self, key):
        return self.chash.get_node(key)

    def get_nodes(self, key, count=2):
        """Returns primary + replicas (primary first)."""
        if not self.chash.ring:
            return []
        h = self.chash._hash(key)
        idx = bisect.bisect_left(self.chash.ring, h)

        nodes = []
        seen = set()
        attempts = 0
        while len(nodes) < count and attempts < len(self.chash.ring):
            if idx == len(self.chash.ring):
                idx = 0
            node = self.chash.nodes[self.chash.ring[idx]]
            if node not in seen:
                nodes.append(node)
                seen.add(node)
            idx += 1
            attempts += 1
        return nodes

    async def forward_command(self, node_id, command, args):
        """Forward a command to another node via TCP."""
        if node_id == self.current_node_id:
            # Avoid import cycle: local execution
            from server.commands import execute
            return await execute(command, args, persist=False)

        addr = self.node_addresses.get(node_id)
        if not addr:
            return f"ERROR: Unknown node '{node_id}'"

        host, port = addr
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=2.0
            )
            msg = f"{command} {' '.join(str(a) for a in args)}"
            writer.write(encode_message(TYPE_REQUEST, msg))
            await writer.drain()

            _msg_type, response_data = await asyncio.wait_for(
                decode_message(reader), timeout=2.0
            )
            writer.close()
            await writer.wait_closed()
            return response_data
        except asyncio.TimeoutError:
            return f"ERROR: Timeout forwarding to {node_id}"
        except Exception as e:
            return f"ERROR: Forwarding failed to {node_id}: {e}"


# Load configuration from environment
import os
MY_NODE_ID = os.getenv("NODE_ID", "node1")
ALL_NODES = os.getenv("CLUSTER_NODES", "node1").split(",")

cluster_manager = ClusterManager(MY_NODE_ID, ALL_NODES)
