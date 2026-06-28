import pytest
from server.commands import execute


@pytest.fixture(autouse=True)
def cleanup():
    from server.data_types import zset_store
    # Clean the key under test
    zset_store.zrem("leaderboard", "alice", "bob", "charlie", "dave")
    yield


@pytest.mark.asyncio
async def test_zadd_and_zscore():
    assert await execute("ZADD", ["leaderboard", "100", "alice", "200", "bob"]) == 2
    assert await execute("ZSCORE", ["leaderboard", "alice"]) == "100.0"
    assert await execute("ZSCORE", ["leaderboard", "bob"]) == "200.0"
    assert await execute("ZSCORE", ["leaderboard", "nobody"]) == "NULL"


@pytest.mark.asyncio
async def test_zrank():
    await execute("ZADD", ["leaderboard", "100", "alice", "200", "bob", "50", "charlie"])
    # Ascending: charlie(50) < alice(100) < bob(200)
    assert await execute("ZRANK", ["leaderboard", "charlie"]) == 0
    assert await execute("ZRANK", ["leaderboard", "alice"]) == 1
    assert await execute("ZRANK", ["leaderboard", "bob"]) == 2
    assert await execute("ZRANK", ["leaderboard", "nobody"]) == "NULL"


@pytest.mark.asyncio
async def test_zrange():
    await execute("ZADD", ["leaderboard", "100", "alice", "200", "bob", "50", "charlie"])
    members = await execute("ZRANGE", ["leaderboard", "0", "-1"])
    assert members == ["charlie", "alice", "bob"]

    # WITHSCORES
    with_scores = await execute("ZRANGE", ["leaderboard", "0", "-1", "WITHSCORES"])
    assert with_scores == ["charlie", "50.0", "alice", "100.0", "bob", "200.0"]


@pytest.mark.asyncio
async def test_zrevrange():
    await execute("ZADD", ["leaderboard", "100", "alice", "200", "bob", "50", "charlie"])
    members = await execute("ZREVRANGE", ["leaderboard", "0", "-1"])
    assert members == ["bob", "alice", "charlie"]


@pytest.mark.asyncio
async def test_zrangebyscore():
    await execute("ZADD", ["leaderboard", "100", "alice", "200", "bob", "50", "charlie", "300", "dave"])
    members = await execute("ZRANGEBYSCORE", ["leaderboard", "100", "250"])
    assert set(members) == {"alice", "bob"}

    # Unbounded ranges
    all_above = await execute("ZRANGEBYSCORE", ["leaderboard", "150", "+inf"])
    assert set(all_above) == {"bob", "dave"}


@pytest.mark.asyncio
async def test_zrem_and_zcard():
    await execute("ZADD", ["leaderboard", "100", "alice", "200", "bob"])
    assert await execute("ZCARD", ["leaderboard"]) == 2
    assert await execute("ZREM", ["leaderboard", "alice"]) == 1
    assert await execute("ZCARD", ["leaderboard"]) == 1
    assert await execute("ZSCORE", ["leaderboard", "alice"]) == "NULL"


@pytest.mark.asyncio
async def test_zincrby():
    await execute("ZADD", ["leaderboard", "100", "alice"])
    new_score = await execute("ZINCRBY", ["leaderboard", "50", "alice"])
    assert float(new_score) == 150.0
    assert await execute("ZSCORE", ["leaderboard", "alice"]) == "150.0"


@pytest.mark.asyncio
async def test_zcount():
    await execute("ZADD", ["leaderboard", "100", "alice", "200", "bob", "50", "charlie", "300", "dave"])
    assert await execute("ZCOUNT", ["leaderboard", "100", "250"]) == 2
    assert await execute("ZCOUNT", ["leaderboard", "-inf", "+inf"]) == 4
    assert await execute("ZCOUNT", ["leaderboard", "400", "500"]) == 0
