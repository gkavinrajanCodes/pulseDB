import pytest
import asyncio
from server.commands import execute
from server.data_types import list_store, hash_store


@pytest.fixture(autouse=True)
def cleanup():
    # Clear internal states before each test
    for shard in list_store._shards:
        shard.clear()
    for shard in hash_store._shards:
        shard.clear()
    yield


@pytest.mark.asyncio
async def test_list_operations():
    # Basic Push/Pop
    await execute("LPUSH", ["mylist", "c", "b", "a"])
    assert await execute("LLEN", ["mylist"]) == 3
    
    assert await execute("LPOP", ["mylist"]) == "a"
    assert await execute("RPOP", ["mylist"]) == "c"
    
    assert await execute("LRANGE", ["mylist", "0", "-1"]) == ["b"]
    
    # Empty list cases
    assert await execute("LPOP", ["empty_list"]) == "NULL"
    assert await execute("RPOP", ["empty_list"]) == "NULL"
    assert await execute("LLEN", ["empty_list"]) == 0
    assert await execute("LRANGE", ["empty_list", "0", "10"]) == []
    
    # Index and Set
    await execute("RPUSH", ["mylist", "x", "y", "z"])  # b, x, y, z
    assert await execute("LINDEX", ["mylist", "1"]) == "x"
    assert await execute("LINDEX", ["mylist", "10"]) == "NULL"
    
    assert await execute("LSET", ["mylist", "1", "changed"]) == "OK"
    assert await execute("LINDEX", ["mylist", "1"]) == "changed"
    
    # LSET out of bounds / non-existent
    assert await execute("LSET", ["empty_list", "0", "val"]) == "ERROR: no such key"
    assert await execute("LSET", ["mylist", "10", "val"]) == "ERROR: index out of range"
    
    # LREM
    await execute("LPUSH", ["rem_list", "a", "b", "a", "c", "a"])
    # remove 2 occurrences of 'a' from left
    assert await execute("LREM", ["rem_list", "2", "a"]) == 2
    assert await execute("LRANGE", ["rem_list", "0", "-1"]) == ["c", "b", "a"]


@pytest.mark.asyncio
async def test_hash_operations():
    # Basic Hash
    assert await execute("HSET", ["myhash", "f1", "v1", "f2", "v2"]) == 2
    assert await execute("HGET", ["myhash", "f1"]) == "v1"
    assert await execute("HGET", ["myhash", "f_missing"]) == "NULL"
    assert await execute("HGET", ["missing_hash", "f1"]) == "NULL"
    
    assert await execute("HLEN", ["myhash"]) == 2
    assert await execute("HEXISTS", ["myhash", "f1"]) == 1
    assert await execute("HEXISTS", ["myhash", "f_missing"]) == 0
    
    # HMSET & HMGET
    await execute("HMSET", ["myhash", "f3", "v3", "f4", "v4"])
    assert await execute("HMGET", ["myhash", "f1", "f3", "fx"]) == ["v1", "v3", "NULL"]
    
    # HGETALL
    flat = await execute("HGETALL", ["myhash"])
    assert len(flat) == 8  # 4 fields * 2 (key, val)
    assert "f1" in flat
    assert "v1" in flat
    assert await execute("HGETALL", ["missing_hash"]) == []
    
    # Keys/Vals
    keys = await execute("HKEYS", ["myhash"])
    assert sorted(keys) == ["f1", "f2", "f3", "f4"]
    
    # HINCRBY
    assert await execute("HINCRBY", ["counters", "clicks", "5"]) == 5
    assert await execute("HINCRBY", ["counters", "clicks", "3"]) == 8
    
    # HDEL
    assert await execute("HDEL", ["myhash", "f1", "f2", "fx"]) == 2
    assert await execute("HLEN", ["myhash"]) == 2
