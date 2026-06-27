import pytest
import asyncio
from pulsedb.async_client import AsyncPulseDB
from pulsedb.client import PulseDB

async def _cleanup():
    async with AsyncPulseDB(host="localhost", port=6379) as db:
        try:
            await db.flush()
        except Exception:
            pass

@pytest.mark.asyncio
async def test_async_sdk_core():
    await _cleanup()
    async with AsyncPulseDB(host="localhost", port=6379) as db:
        await db.set("test_async", "val")
        assert await db.get("test_async") == "val"
        
        # Test lists via raw execute
        await db.execute_command("LPUSH", "mylist_test_async", "a", "b")
        assert await db.execute_command("LLEN", "mylist_test_async") == 2
        
        # Test hashes
        await db.hmset("myhash", {"f1": "v1", "f2": "v2"})
        assert await db.execute_command("HGET", "myhash", "f1") == "v1"

def test_sync_sdk_core():
    asyncio.run(_cleanup())
    with PulseDB(host="localhost", port=6379) as db:
        db.set("test_sync", "val2")
        assert db.get("test_sync") == "val2"
        db.hmset("sync_hash", {"f1": "sync"})
        flat = db.hgetall("sync_hash")
        assert "f1" in flat and "sync" in flat

# We test the new Vector Namespace and LangChain wrapper
def test_langchain_wrapper():
    asyncio.run(_cleanup())
    from sdk.langchain_pulsedb.vectorstore import PulseDBVectorStore
    
    class DummyEmbeddings:
        def embed_documents(self, texts):
            # dim = 2
            return [[1.0, 0.0] for _ in texts]
        def embed_query(self, query):
            return [1.0, 0.0]
            
    store = PulseDBVectorStore(
        embedding=DummyEmbeddings(),
        host="localhost",
        port=6379
    )
    
    store.add_texts(
        ["doc A", "doc B", "doc C"], 
        [{"source": "test1", "cat": "news"}, {"source": "test2", "cat": "sports"}, {"source": "test3", "cat": "sports"}]
    )
    
    # Standard Search
    results = store.similarity_search("query", k=3)
    assert len(results) == 3
    assert results[0].page_content in ["doc A", "doc B", "doc C"]
    assert "source" in results[0].metadata
    
    # Hybrid Search (Filter)
    filtered = store.similarity_search("query", k=2, filter={"cat": "sports"})
    assert len(filtered) == 2
    for doc in filtered:
        assert doc.metadata["cat"] == "sports"
        assert doc.page_content in ["doc B", "doc C"]
