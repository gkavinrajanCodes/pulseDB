# Getting Started

PulseDB is designed to be completely frictionless. You can deploy it locally in 60 seconds.

## 1. Run the Server

The easiest way to run PulseDB is via the official Docker image. 

```bash
docker run -d \
  -p 6379:6379 \
  -p 8000:8000 \
  -v pulsedb_data:/app/data \
  --name pulsedb \
  ghcr.io/gkavinrajancodes/pulsedb:latest
```

This exposes the raw TCP RESP2 protocol on port `6379`, and the HTTP metrics/dashboard on port `8000`.

## 2. Install the SDK

While you can use standard `redis-py` to talk to PulseDB, we strongly recommend our official SDK for typed vector operations.

```bash
pip install pulsedb
```

## 3. Basic Key-Value Operations

Use PulseDB just like Redis to store temporary state, chat history, or rate limits.

```python
import asyncio
from pulsedb.async_client import AsyncPulseDB

async def main():
    client = AsyncPulseDB(host="localhost", port=6379)
    
    # Store a user session
    await client.set("session:123", "active", ttl=3600)
    
    # Retrieve it
    val = await client.get("session:123")
    print(val) # "active"
    
asyncio.run(main())
```

## 4. Vector Search with Metadata

This is where PulseDB shines. Let's insert a document embedding and search for it.

```python
async def vector_demo():
    client = AsyncPulseDB(host="localhost", port=6379)
    
    # Insert an AI embedding with metadata
    await client.vectors.upsert(
        id="doc_1",
        vector=[0.1, 0.2, 0.3],
        metadata={"author": "kavin", "department": "engineering"}
    )
    
    # Search for similar vectors, strictly filtered by department
    results = await client.vectors.search(
        query=[0.1, 0.25, 0.3],
        top_k=5,
        filter={"department": "engineering"}
    )
    
    print(results)
    # [{'id': 'doc_1', 'score': 0.992, 'metadata': {'author': 'kavin', 'department': 'engineering'}}]
```
