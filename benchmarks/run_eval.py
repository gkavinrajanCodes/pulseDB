import asyncio
import time
import uuid
import numpy as np
import pandas as pd
import os
from pulsedb.async_client import AsyncPulseDB

os.makedirs("benchmarks/data", exist_ok=True)

async def measure_kv_throughput(client, concurrencies):
    results = []
    print("\n[+] Running KV Throughput Benchmark...")
    for c in concurrencies:
        num_ops = 50000
        ops_per_task = num_ops // c
        
        async def worker():
            for i in range(ops_per_task):
                key = f"bench:kv:{uuid.uuid4().hex[:8]}"
                await client.set(key, "val")
                
        start = time.perf_counter()
        tasks = [worker() for _ in range(c)]
        await asyncio.gather(*tasks)
        end = time.perf_counter()
        
        qps = num_ops / (end - start)
        print(f"  -> Concurrency {c:03d}: {qps:8.2f} ops/sec")
        results.append({"concurrency": c, "qps": qps})
    
    df = pd.DataFrame(results)
    df.to_csv("benchmarks/data/kv_throughput.csv", index=False)

async def measure_vector_batch(client, batch_sizes):
    results = []
    dim = 2
    num_vectors = 20000
    print("\n[+] Running Vector Batch Ingestion Benchmark...")
    vectors = np.random.rand(num_vectors, dim).astype(np.float32)
    payload = [{"id": f"v:{i}", "vector": vectors[i].tolist()} for i in range(num_vectors)]
    
    for bs in batch_sizes:
        start = time.perf_counter()
        for chunk in [payload[i:i+bs] for i in range(0, len(payload), bs)]:
            await client.vectors.upsert_batch(chunk)
        end = time.perf_counter()
        
        vps = num_vectors / (end - start)
        print(f"  -> Batch Size {bs:4d}: {vps:8.2f} vectors/sec")
        results.append({"batch_size": bs, "vectors_per_sec": vps})
        
    df = pd.DataFrame(results)
    df.to_csv("benchmarks/data/vector_batch.csv", index=False)

async def measure_vector_search_latency(client, top_ks):
    results = []
    raw_latencies = []
    dim = 2
    num_queries = 2000
    print("\n[+] Running Vector Search Latency Benchmark (Capturing CDF)...")
    queries = np.random.rand(num_queries, dim).astype(np.float32)
    
    for k in top_ks:
        latencies = []
        for i in range(num_queries):
            start = time.perf_counter()
            await client.vectors.search(queries[i].tolist(), top_k=k)
            end = time.perf_counter()
            latencies.append((end - start) * 1000)  # ms
            
        p50 = np.percentile(latencies, 50)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
        avg = np.mean(latencies)
        print(f"  -> Top-{k:02d} | Avg: {avg:.2f}ms | p99: {p99:.2f}ms")
        
        results.append({"top_k": k, "avg_ms": avg, "p50_ms": p50, "p95_ms": p95, "p99_ms": p99})
        for lat in latencies:
            raw_latencies.append({"top_k": k, "latency_ms": lat})
        
    df = pd.DataFrame(results)
    df.to_csv("benchmarks/data/vector_latency.csv", index=False)
    df_raw = pd.DataFrame(raw_latencies)
    df_raw.to_csv("benchmarks/data/vector_latency_raw_cdf.csv", index=False)

async def measure_throughput_latency_tradeoff(client, concurrencies):
    results = []
    print("\n[+] Running Throughput-Latency Tradeoff (Vector Search)...")
    dim = 2
    for c in concurrencies:
        num_ops = 5000
        ops_per_task = num_ops // c
        latencies = []
        
        async def worker():
            query = np.random.rand(dim).astype(np.float32).tolist()
            for _ in range(ops_per_task):
                start_t = time.perf_counter()
                await client.vectors.search(query, top_k=10)
                latencies.append((time.perf_counter() - start_t) * 1000)
                
        start = time.perf_counter()
        tasks = [worker() for _ in range(c)]
        await asyncio.gather(*tasks)
        end = time.perf_counter()
        
        qps = num_ops / (end - start)
        p99 = np.percentile(latencies, 99) if latencies else 0
        print(f"  -> Concurrency {c:03d}: {qps:8.2f} QPS | p99 Latency: {p99:.2f}ms")
        results.append({"concurrency": c, "qps": qps, "p99_latency_ms": p99})
        
    df = pd.DataFrame(results)
    df.to_csv("benchmarks/data/tradeoff.csv", index=False)

async def main():
    # Make sure server is running locally on 6379
    client = AsyncPulseDB(host="localhost", port=6379)
    try:
        await client.execute_command("PING")
    except Exception:
        print("ERROR: Please start PulseDB server on port 6379 before benchmarking.")
        return
        
    # Run Experiments
    await measure_kv_throughput(client, concurrencies=[1, 5, 10, 20, 50, 100])
    await measure_vector_batch(client, batch_sizes=[10, 100, 500, 1000, 5000])
    await measure_vector_search_latency(client, top_ks=[1, 5, 10, 50, 100])
    await measure_throughput_latency_tradeoff(client, concurrencies=[1, 2, 5, 10, 20, 50])
    
    print("\n[+] All evaluations complete. CSVs saved to benchmarks/data/")

if __name__ == "__main__":
    asyncio.run(main())
