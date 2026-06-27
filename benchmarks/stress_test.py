"""
PulseDB Extreme Stress Test

Tests ingestion throughput, search latency (QPS), and concurrent connection handling
using the standard redis-py asyncio client.
"""
import asyncio
import time
import uuid
import numpy as np
import redis.asyncio as redis

# Configuration
VECTOR_DIM = 1536  # OpenAI text-embedding-3-small dimension
NUM_VECTORS = 10_000
NUM_QUERIES = 1_000
CONCURRENCY = 50
HOST = "localhost"
PORT = 6379

def generate_random_vector(dim: int) -> list[float]:
    # HNSW needs normalized vectors for cosine similarity
    vec = np.random.rand(dim).astype(np.float32)
    vec /= np.linalg.norm(vec)
    return vec.tobytes()

async def ingest_worker(client: redis.Redis, queue: asyncio.Queue, progress: dict):
    while True:
        batch = await queue.get()
        if batch is None:
            queue.task_done()
            break
        
        try:
            # Pipeline the batch for maximum throughput
            async with client.pipeline(transaction=False) as pipe:
                for doc_id, blob in batch:
                    pipe.execute_command("VECTOR.BSET", doc_id, blob)
                await pipe.execute()
            
            progress["count"] += len(batch)
            if progress["count"] % 500 == 0:
                print(f"  Ingested {progress['count']} vectors...", flush=True)
        except Exception as e:
            print(f"Worker Error: {e}")
        
        queue.task_done()

async def search_worker(client: redis.Redis, queue: asyncio.Queue, latencies: list[float]):
    while True:
        query_blob = await queue.get()
        if query_blob is None:
            queue.task_done()
            break
            
        start_t = time.perf_counter()
        # VECTOR.BSEARCH <blob> TOP_K 5
        await client.execute_command("VECTOR.BSEARCH", query_blob, "TOP_K", "5")
        end_t = time.perf_counter()
        
        latencies.append((end_t - start_t) * 1000)  # ms
        queue.task_done()

async def run_benchmark():
    print("========================================")
    print("      PulseDB Extreme Stress Test       ")
    print("========================================")
    
    print(f"\nConnecting to PulseDB on redis://{HOST}:{PORT}...")
    client = redis.Redis(host=HOST, port=PORT, decode_responses=True, protocol=2, socket_timeout=10.0)
    
    try:
        await client.ping()
        print("Connected successfully!\n")
    except Exception as e:
        print(f"Failed to connect to PulseDB: {e}")
        print("Please ensure the server is running.")
        return

    # Clear existing data
    await client.execute_command("FLUSHDB")
    
    print(f"--- Phase 1: High-Throughput Ingestion ---")
    print(f"Generating {NUM_VECTORS} vectors ({VECTOR_DIM} dimensions)...")
    
    # Pre-generate to avoid timing the random generator
    batches = []
    batch_size = 50
    current_batch = []
    for i in range(NUM_VECTORS):
        current_batch.append((f"doc_{i}", generate_random_vector(VECTOR_DIM)))
        if len(current_batch) == batch_size:
            batches.append(current_batch)
            current_batch = []
    if current_batch:
        batches.append(current_batch)
        
    print("Starting ingestion...")
    ingest_queue = asyncio.Queue()
    for b in batches:
        ingest_queue.put_nowait(b)
        
    progress = {"count": 0}
    workers = []
    
    start_time = time.perf_counter()
    for _ in range(CONCURRENCY):
        workers.append(asyncio.create_task(ingest_worker(client, ingest_queue, progress)))
        ingest_queue.put_nowait(None)  # Poison pill
        
    await ingest_queue.join()
    end_time = time.perf_counter()
    
    total_time = end_time - start_time
    vps = NUM_VECTORS / total_time
    print(f"-> Ingested {NUM_VECTORS} vectors in {total_time:.2f} seconds.")
    print(f"-> Throughput: {vps:.2f} Vectors/Sec")
    
    count = await client.execute_command("VECTOR.COUNT")
    print(f"-> Server confirms {count} active vectors in memory.\n")
    
    
    print(f"--- Phase 2: QPS & Latency Search ---")
    print(f"Running {NUM_QUERIES} queries concurrently...")
    
    search_queue = asyncio.Queue()
    for _ in range(NUM_QUERIES):
        search_queue.put_nowait(generate_random_vector(VECTOR_DIM))
        
    latencies = []
    search_workers = []
    
    start_time = time.perf_counter()
    for _ in range(CONCURRENCY):
        search_workers.append(asyncio.create_task(search_worker(client, search_queue, latencies)))
        search_queue.put_nowait(None)
        
    await search_queue.join()
    end_time = time.perf_counter()
    
    total_time = end_time - start_time
    qps = NUM_QUERIES / total_time
    avg_latency = sum(latencies) / len(latencies)
    p50 = np.percentile(latencies, 50)
    p95 = np.percentile(latencies, 95)
    p99 = np.percentile(latencies, 99)
    
    print(f"-> Executed {NUM_QUERIES} searches in {total_time:.2f} seconds.")
    print(f"-> Throughput: {qps:.2f} QPS (Queries Per Second)")
    print(f"-> Average Latency: {avg_latency:.2f} ms")
    print(f"-> p50 Latency:     {p50:.2f} ms")
    print(f"-> p95 Latency:     {p95:.2f} ms")
    print(f"-> p99 Latency:     {p99:.2f} ms")
    
    print("\n--- Phase 3: The Connection Bomb ---")
    print("Testing TCP Server connection limits with 500 concurrent connections...")
    
    bomb_clients = []
    try:
        for i in range(500):
            c = redis.Redis(host=HOST, port=PORT, decode_responses=True, protocol=2, socket_timeout=10.0)
            bomb_clients.append(c)
        
        # Fire 500 PINGs simultaneously
        bomb_tasks = [c.ping() for c in bomb_clients]
        start_time = time.perf_counter()
        await asyncio.gather(*bomb_tasks)
        end_time = time.perf_counter()
        print(f"-> Successfully handled 500 concurrent connections in {(end_time - start_time)*1000:.2f} ms without crashing.")
    except Exception as e:
        print(f"-> Connection Bomb Failed: {e}")
    finally:
        for c in bomb_clients:
            await c.aclose()
            
    await client.aclose()
    print("\n========================================")
    print("          Benchmark Complete            ")
    print("========================================")


if __name__ == "__main__":
    asyncio.run(run_benchmark())
