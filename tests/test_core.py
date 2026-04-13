import pytest
import asyncio
import os
import sys

# Pytest requires finding the server package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ["NODE_ID"] = "node1"
os.environ["CLUSTER_NODES"] = "node1"

from server.store import Store
from server.pubsub import PubSub
from server.cluster import ConsistentHash, ClusterManager
from server.commands import execute
from server.protocol import encode_message, decode_message, TYPE_REQUEST, TYPE_RESPONSE, TYPE_ERROR
from server.persistence import WAL


@pytest.fixture
def store():
    return Store()


def test_store_basic(store):
    store.set("k1", "hello")
    assert store.get("k1") == "hello"
    
    store.set("k2", "world", ttl=100)
    assert store.get("k2") == "world"

    store.delete("k1")
    assert store.get("k1") is None

    store.mset({"a": "1", "b": "2", "c": "3"})
    assert store.mget(["a", "b", "c"]) == ["1", "2", "3"]
    assert store.mget(["a", "missing"]) == ["1", None]

    store.set("exp_key", "v")
    assert store.expire("exp_key", 100) == 1
    assert store.expire("no_such_key", 100) == 0


@pytest.mark.asyncio
async def test_pubsub():
    ps = PubSub()
    q = ps.subscribe("test-chan")
    assert isinstance(q, asyncio.Queue)

    ps.publish("test-chan", "hello")
    msg1 = await asyncio.wait_for(q.get(), timeout=1.0)
    assert msg1 == "hello"

    ps.unsubscribe("test-chan", q)
    assert "test-chan" not in ps._subscribers


def test_cluster_hashing():
    ch = ConsistentHash(["A", "B", "C"])
    n1 = ch.get_node("some_key")
    assert n1 in ["A", "B", "C"]
    assert ch.get_node("some_key") == n1

    ch.remove_node(n1)
    n3 = ch.get_node("some_key")
    remaining_nodes = [x for x in ["A", "B", "C"] if x != n1]
    assert n3 in remaining_nodes

    cm = ClusterManager("node1", ["node1"])
    assert cm.is_local("anykey")


@pytest.mark.asyncio
async def test_commands():
    from server.store import store as global_store

    assert await execute("SET", ["e_k1", "v1"]) == "OK"
    assert await execute("GET", ["e_k1"]) == "v1"
    assert await execute("GET", ["nonexistent"]) == "NULL"
    assert await execute("MSET", ["mk1", "mv1", "mk2", "mv2"]) == "OK"
    assert await execute("MGET", ["mk1", "mk2", "mk_missing"]) == ["mv1", "mv2", "NULL"]
    assert await execute("DEL", ["e_k1"]) == "OK"
    assert await execute("GET", ["e_k1"]) == "NULL"
    
    await execute("SET", ["expire_me", "v"])
    assert await execute("EXPIRE", ["expire_me", "100"]) == 1

    assert await execute("PUBLISH", ["ch", "msg"]) == "MESSAGE PUBLISHED"
    r = await execute("SUBSCRIBE", ["ch"])
    assert isinstance(r, str)

    assert "ERROR" in str(await execute("UNKNOWNCMD", []))


@pytest.mark.asyncio
async def test_protocol():
    encoded = encode_message(TYPE_RESPONSE, "OK")
    assert isinstance(encoded, bytes) and len(encoded) > 5

    reader = asyncio.StreamReader()
    reader.feed_data(encode_message(TYPE_RESPONSE, "HELLO"))
    t, data = await decode_message(reader)
    assert data == "HELLO"
    assert t == TYPE_RESPONSE

    reader2 = asyncio.StreamReader()
    reader2.feed_data(encode_message(TYPE_REQUEST, None))
    t2, data2 = await decode_message(reader2)
    assert data2 == "NULL"


def test_persistence(tmp_path):
    wal_file = tmp_path / "test.wal"
    snap_file = tmp_path / "test.snapshot"
    
    import server.persistence as _p_mod
    original_snap = _p_mod.SNAPSHOT_FILE
    _p_mod.SNAPSHOT_FILE = str(snap_file)

    try:
        w = WAL(filename=str(wal_file))
        w.log("SET", ["p_key", "p_val"])
        w.log("DEL", ["p_key2"])

        fs = Store()
        w.recover_sync(fs)
        assert fs.get("p_key") == "p_val"
        
        w.save_snapshot(fs)
        assert snap_file.exists()

        fs2 = Store()
        w.load_snapshot(fs2)
        assert fs2.get("p_key") == "p_val"
    finally:
        _p_mod.SNAPSHOT_FILE = original_snap
        w.close()
