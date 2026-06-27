import asyncio
import httpx
import time

URL = "http://127.0.0.1:8000/command"
API_KEY = "pulse-db-secret-key"
TOTAL_REQUESTS = 10000

async def send_request(client, i):
    payload = {
        "command": "SET",
        "args": [f"key{i}", "value"]
    }
    await client.post(URL, json=payload, headers={"X-API-Key": API_KEY})

async def run_benchmark():
    async with httpx.AsyncClient() as client:
        start = time.time()

        tasks = [
            send_request(client, i)
            for i in range(TOTAL_REQUESTS)
        ]

        await asyncio.gather(*tasks)

        end = time.time()

        total_time = end - start
        ops = TOTAL_REQUESTS / total_time

        print(f"Total Time: {total_time:.2f}s")
        print(f"Throughput: {ops:.2f} ops/sec")

if __name__ == "__main__":
    asyncio.run(run_benchmark())