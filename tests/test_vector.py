import pytest
import asyncio
from server.commands import execute
from server.vector import vector_index

@pytest.fixture(autouse=True)
def cleanup():
    vector_index.clear()
    yield

@pytest.mark.asyncio
async def test_vector_basic():
    # SET
    assert await execute("VECTOR.SET", ["doc1", "1.0", "0.0"]) == "OK"
    assert await execute("VECTOR.SET", ["doc2", "0.5", "0.5"]) == "OK"
    assert await execute("VECTOR.COUNT", []) == 2
    
    # GET
    import json
    assert json.loads(await execute("VECTOR.GET", ["doc1"])) == {"vector": [1.0, 0.0], "metadata": {}}
    assert await execute("VECTOR.GET", ["doc_missing"]) == "NULL"
    
    # SEARCH
    results = await execute("VECTOR.SEARCH", ["1.0", "0.0", "TOP_K", "1"])
    # Expected: ["doc1", "1.000000"]
    assert len(results) == 2
    assert results[0] == "doc1"
    assert float(results[1]) > 0.99
    
    # DEL
    assert await execute("VECTOR.DEL", ["doc1"]) == 1
    assert await execute("VECTOR.COUNT", []) == 1

@pytest.mark.asyncio
async def test_vector_edge_cases():
    # Invalid floats
    res = await execute("VECTOR.SET", ["bad", "1.0", "abc"])
    assert res.startswith("ERROR: vector dimensions must be floats")
    
    # Missing dimensions
    res = await execute("VECTOR.SET", ["bad"])
    assert res.startswith("ERROR: VECTOR.SET requires key and at least one dimension")
    
    # Bad query
    res = await execute("VECTOR.SEARCH", ["abc", "TOP_K", "1"])
    assert res.startswith("ERROR: query vector dimensions must be floats")
    
    # Empty search gracefully returns empty
    # Wait, we need to add tests for mismatched dimensions once HNSW is implemented in Phase 2
