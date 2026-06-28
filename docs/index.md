# Welcome to PulseDB

**An enterprise-grade, in-memory database with a native AI Vector Engine.**

PulseDB is designed for modern AI application developers who need extremely fast session caching (like Redis) *and* robust semantic search (like Pinecone) without the operational headache of managing multiple databases.

---

## Why PulseDB?

In standard architectures, developers suffer from "Database Sprawl":

- **Redis:** Used for rate limiting, session storage, and chat history.
- **Pinecone / Milvus:** Used for storing document embeddings for RAG pipelines.
- **PostgreSQL:** Used for relational metadata.

**PulseDB replaces this entire stack with a single, lightweight binary.**

### ⚡ Unified Architecture
Because PulseDB runs the KV store and the HNSW Vector Engine in the exact same memory space, there is zero network latency between your cache and your embeddings.

### ⚡ True Hybrid Search
Unlike other databases that perform "post-filtering" (which ruins recall accuracy), PulseDB evaluates metadata filters (`$in`, `$gt`, `$contains`) directly inside the C++ HNSW graph traversal via a custom callback mechanism.

### ⚡ Blazing Fast Ingestion
Thanks to `asyncio` multiplexing and the custom `VECTOR.BSET_BATCH` protocol, PulseDB can ingest over **20,000 vectors per second** on a single node.

---

## Next Steps

- **[Getting Started](getting_started.md):** Spin up the Docker container and run your first Vector Search.
- **[Architecture Deep Dive](architecture.md):** Learn how the RESP2 protocol and Write-Ahead Log (WAL) ensure durability.
