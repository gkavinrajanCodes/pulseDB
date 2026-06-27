import pytest
import asyncio
from httpx import ASGITransport, AsyncClient

from pulsedb.async_client import AsyncPulseDB
from pulsedb.client import PulseDB
from server.main import app

@pytest.fixture
def mock_httpx(monkeypatch):
    # Bind httpx.AsyncClient directly to the FastAPI app, bypassing network
    transport = ASGITransport(app=app, client=("testclient", 12345))
    
    async def mock_get_client(self):
        if self._client is None or self._client.is_closed:
            self._client = AsyncClient(
                transport=transport, 
                base_url="http://testserver", 
                headers=self._headers
            )
        return self._client
    
    monkeypatch.setattr(AsyncPulseDB, "_get_client", mock_get_client)
    yield

@pytest.mark.asyncio
async def test_async_sdk_core(mock_httpx):
    async with AsyncPulseDB(api_key="pulse-db-secret-key") as db:
        await db.set("test_async", "val")
        assert await db.get("test_async") == "val"
        
        # Test lists via raw execute
        await db.execute_command("LPUSH", "mylist", "a", "b")
        assert await db.execute_command("LLEN", "mylist") == 2
        
        # Test hashes
        await db.hmset("myhash", {"f1": "v1", "f2": "v2"})
        assert await db.execute_command("HGET", "myhash", "f1") == "v1"

def test_sync_sdk_core(mock_httpx):
    with PulseDB(api_key="pulse-db-secret-key") as db:
        db.set("test_sync", "val2")
        assert db.get("test_sync") == "val2"
        db.hmset("sync_hash", {"f1": "sync"})
        flat = db.hgetall("sync_hash")
        assert "f1" in flat and "sync" in flat

# We can also test the LangChain wrapper since it uses the client
def test_langchain_wrapper(mock_httpx):
    from sdk.langchain_pulsedb.vectorstore import PulseDBVectorStore
    
    class DummyEmbeddings:
        def embed_documents(self, texts):
            return [[1.0, 0.0] for _ in texts]
        def embed_query(self, query):
            return [1.0, 0.0]
            
    store = PulseDBVectorStore(
        embedding=DummyEmbeddings(),
        api_key="pulse-db-secret-key"
    )
    
    # Wait, the vector store uses the sync client which will use the mock
    store.add_texts(["doc A", "doc B"], [{"source": "test1"}, {"source": "test2"}])
    
    results = store.similarity_search("query", k=1)
    assert len(results) == 1
    assert results[0].page_content in ["doc A", "doc B"]
    assert "source" in results[0].metadata
