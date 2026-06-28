# Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install build tools needed for hnswlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV NODE_ID=node1
ENV CLUSTER_NODES=node1

# Expose HTTP REST API port and high-performance TCP Binary Protocol port
EXPOSE 8000 6379

# Entrypoint starts uvicorn which internally launches the TCP server on :6379
CMD ["uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8000"]
