# Copyright (c) 2026 G Kavinrajan. All rights reserved.
# Licensed under the Business Source License 1.1

# server/config.py
"""
Centralized configuration loaded from environment variables.
All server settings live here — import this instead of scattering os.getenv() calls.
"""
import os

# --- Security ---
API_KEY: str = os.getenv("PULSEDB_API_KEY", "pulse-db-secret-key")
REQUIRE_PASS: str = os.getenv("PULSEDB_REQUIREPASS", "")   # empty = no TCP auth required

# --- TLS ---
TLS_CERT: str = os.getenv("PULSEDB_TLS_CERT", "")
TLS_KEY: str = os.getenv("PULSEDB_TLS_KEY", "")

# --- Cluster ---
NODE_ID: str = os.getenv("NODE_ID", "node1")
CLUSTER_NODES: list[str] = os.getenv("CLUSTER_NODES", "node1").split(",")

# --- Persistence ---
WAL_FILE: str = os.getenv("WAL_FILE", "pulsedb.wal")
SNAPSHOT_FILE: str = os.getenv("SNAPSHOT_FILE", "pulsedb.snapshot")
SNAPSHOT_INTERVAL: int = int(os.getenv("SNAPSHOT_INTERVAL", "5"))
VECTOR_INDEX_FILE: str = os.getenv("VECTOR_INDEX_FILE", "pulsedb.hnsw")
VECTOR_META_FILE: str = os.getenv("VECTOR_META_FILE", "pulsedb_hnsw.meta.json")

# --- Networking ---
TCP_HOST: str = os.getenv("TCP_HOST", "0.0.0.0")
TCP_PORT: int = int(os.getenv("TCP_PORT", "6379"))
HTTP_PORT: int = int(os.getenv("HTTP_PORT", "8000"))

# --- Memory ---
MAX_MEMORY_KEYS: int = int(os.getenv("MAX_MEMORY_KEYS", "0"))   # 0 = unlimited
