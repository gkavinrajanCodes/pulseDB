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

@pytest.mark.asyncio
async def test_vector_filter_operators():
    """Test advanced metadata filter operators: $in, $gt, $lte, $contains, $ne."""
    from server.vector import VectorIndex

    idx = VectorIndex()

    idx.set("a", [1.0, 0.0], metadata={"category": "sports", "year": 2024, "tags": ["python", "fast"]})
    idx.set("b", [0.9, 0.1], metadata={"category": "tech",   "year": 2021, "tags": ["go", "fast"]})
    idx.set("c", [0.8, 0.2], metadata={"category": "sports", "year": 2019, "tags": ["rust"]})
    idx.set("d", [0.7, 0.3], metadata={"category": "news",   "year": 2022, "tags": ["python"]})

    query = [1.0, 0.0]

    # $in — return only sports and news
    results = idx.search(query, top_k=4, filter_dict={"category": {"$in": ["sports", "news"]}})
    ids = [r[0] for r in results]
    assert "a" in ids and "c" in ids and "d" in ids
    assert "b" not in ids

    # $gt — return only articles after 2020
    results = idx.search(query, top_k=4, filter_dict={"year": {"$gt": 2020}})
    ids = [r[0] for r in results]
    assert "a" in ids and "b" in ids and "d" in ids
    assert "c" not in ids

    # $lte — return articles up to and including 2021
    results = idx.search(query, top_k=4, filter_dict={"year": {"$lte": 2021}})
    ids = [r[0] for r in results]
    assert "b" in ids and "c" in ids
    assert "a" not in ids and "d" not in ids

    # $contains — return only docs with 'python' in their tags list
    results = idx.search(query, top_k=4, filter_dict={"tags": {"$contains": "python"}})
    ids = [r[0] for r in results]
    assert "a" in ids and "d" in ids
    assert "b" not in ids and "c" not in ids

    # $ne — return all except sports
    results = idx.search(query, top_k=4, filter_dict={"category": {"$ne": "sports"}})
    ids = [r[0] for r in results]
    assert "b" in ids and "d" in ids
    assert "a" not in ids and "c" not in ids

    # Exact match still works (backward compatibility)
    results = idx.search(query, top_k=4, filter_dict={"category": "tech"})
    ids = [r[0] for r in results]
    assert ids == ["b"]

