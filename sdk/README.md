# PulseDB Python SDK

The official Python client for PulseDB Cloud — the high-performance distributed in-memory database and AI memory layer.

## Installation

```bash
pip install pulsedb
```

## Features
- **Drop-in Redis replacement**: PulseDB speaks RESP2 natively, meaning it works with any existing Redis client (`redis-py`, etc). This SDK is for developers who want the native, highly optimized PulseDB binary protocol.
- **Async First**: First-class `asyncio` support with `AsyncPulseDB`.
- **AI Native**: Built-in vector search and LangChain integration.
- **Zero Dependencies**: Pure Python, built on `httpx`.

## Usage (Sync)

```python
from pulsedb import PulseDB

# Connect to a PulseDB instance
db = PulseDB(host="localhost", port=8000, api_key="your-api-key")

# Key-Value
db.set("user:1", "alice", ttl=3600)
print(db.get("user:1"))  # "alice"

# Counters
db.incr("page_views")
print(db.get("page_views"))

# Pub/Sub
db.publish("updates", "hello world")
```

## Usage (Async)

```python
import asyncio
from pulsedb import AsyncPulseDB

async def main():
    async with AsyncPulseDB(host="localhost", api_key="your-api-key") as db:
        await db.set("user:1", "alice")
        print(await db.get("user:1"))

asyncio.run(main())
```

## LangChain Integration (Vector Search)

PulseDB includes a native LangChain VectorStore adapter. It stores embeddings in its in-memory vector index, and metadata in its Hash store, providing an ultra-fast AI memory layer.

```python
from langchain_openai import OpenAIEmbeddings
from pulsedb.langchain_pulsedb import PulseDBVectorStore

# Initialize the vector store
store = PulseDBVectorStore(
    embedding=OpenAIEmbeddings(),
    host="localhost",
    api_key="your-api-key",
    collection_name="my_documents"
)

# Add documents
store.add_texts(
    texts=["PulseDB is really fast", "Vector search is built in"],
    metadatas=[{"source": "docs"}, {"source": "features"}]
)

# Search
results = store.similarity_search("How fast is it?", k=1)
print(results[0].page_content)  # "PulseDB is really fast"
```
