# server/vector.py
"""
In-memory vector search engine for PulseDB.

Stores embedding vectors alongside metadata and supports approximate
nearest-neighbor search using cosine similarity.

Commands added to PulseDB:
  VECTOR.SET  key dim1 dim2 ... dimN
  VECTOR.GET  key
  VECTOR.DEL  key
  VECTOR.SEARCH  dim1 dim2 ... dimN  TOP_K  k
  VECTOR.COUNT

This is Phase 1 — brute-force cosine similarity over all stored vectors.
Phase 2 will replace with a proper HNSW index for O(log n) search.

Install numpy for ~10x faster math:
  pip install numpy
"""

import threading
import math
from typing import Optional

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False


class VectorIndex:
    def __init__(self):
        self._store: dict[str, list[float]] = {}  # key -> embedding vector
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def set(self, key: str, vector: list[float]) -> str:
        with self._lock:
            self._store[key] = vector
        return "OK"

    def get(self, key: str) -> Optional[list[float]]:
        with self._lock:
            return self._store.get(key)

    def delete(self, key: str) -> int:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return 1
            return 0

    def count(self) -> int:
        with self._lock:
            return len(self._store)

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(self, query: list[float], top_k: int = 5) -> list[tuple[str, float]]:
        """
        Return the top_k most similar keys to the query vector,
        sorted by cosine similarity (highest first).

        Returns: [(key, score), ...]
        """
        with self._lock:
            if not self._store:
                return []

            if _HAS_NUMPY:
                return self._search_numpy(query, top_k)
            return self._search_pure(query, top_k)

    def _cosine_similarity_pure(self, a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        mag_a = math.sqrt(sum(x * x for x in a))
        mag_b = math.sqrt(sum(x * x for x in b))
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)

    def _search_pure(self, query: list[float], top_k: int) -> list[tuple[str, float]]:
        scores = []
        for key, vec in self._store.items():
            if len(vec) != len(query):
                continue
            score = self._cosine_similarity_pure(query, vec)
            scores.append((key, score))
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]

    def _search_numpy(self, query: list[float], top_k: int) -> list[tuple[str, float]]:
        q = np.array(query, dtype=np.float32)
        q_norm = np.linalg.norm(q)
        if q_norm == 0:
            return []

        keys = list(self._store.keys())
        dim = len(query)
        valid_keys = [k for k in keys if len(self._store[k]) == dim]
        if not valid_keys:
            return []

        matrix = np.array([self._store[k] for k in valid_keys], dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        matrix = matrix / norms
        q_unit = q / q_norm

        scores = matrix @ q_unit  # cosine similarities
        top_indices = np.argsort(scores)[::-1][:top_k]
        return [(valid_keys[i], float(scores[i])) for i in top_indices]


# Global singleton
vector_index = VectorIndex()
