# server/vector.py
"""
AI Memory Layer — Vector Search Engine using HNSW.

Provides O(log N) similarity search using hnswlib.
The dimensionality of the index is determined by the first vector inserted.
"""

import threading
from typing import Any
import numpy as np

try:
    import hnswlib  # type: ignore
except ImportError:
    hnswlib = None

class VectorIndex:
    """
    HNSW-based vector index for fast similarity search.
    Maps string keys to integer IDs internally for hnswlib compatibility.
    """

    def __init__(self, space: str = 'cosine'):
        self._space = space
        self._dim = None
        self._index: Any = None
        self._lock = threading.RLock()
        
        # hnswlib requires integer labels. We map string keys <-> integer IDs.
        self._key_to_id: dict[str, int] = {}
        self._id_to_key: dict[int, str] = {}
        self._next_id = 0
        
        # Keep the raw vectors so we can retrieve them (hnswlib doesn't store the raw vectors in a queryable way easily)
        self._store: dict[str, list[float]] = {}

    def _init_index(self, dim: int):
        """Initialize the HNSW index on the first insertion."""
        self._dim = dim
        # pyrefly: ignore [missing-attribute]
        self._index = hnswlib.Index(space=self._space, dim=self._dim)
        # Initialize with max_elements=10000. We will dynamically resize as needed.
        self._index.init_index(max_elements=10000, ef_construction=200, M=16)

    def _resize_if_needed(self):
        """Double the capacity of the index if it's full."""
        if self._index is not None and self._index.element_count >= self._index.max_elements:
            self._index.resize_index(self._index.max_elements * 2)

    def set(self, key: str, vector: list[float]) -> str:
        """Insert or update a vector in the index."""
        if hnswlib is None:
            raise RuntimeError("hnswlib is not installed. Run 'pip install hnswlib'")

        with self._lock:
            vec_dim = len(vector)
            
            # Initialize on first insert
            if self._index is None:
                self._init_index(vec_dim)
            
            # Dimension enforcement
            if vec_dim != self._dim:
                return f"ERROR: vector dimension mismatch (expected {self._dim}, got {vec_dim})"
            
            self._resize_if_needed()
            
            # Assign or reuse integer ID
            if key in self._key_to_id:
                label = self._key_to_id[key]
            else:
                label = self._next_id
                self._next_id += 1
                self._key_to_id[key] = label
                self._id_to_key[label] = key
            
            # Add to index and raw store
            np_vector = np.array([vector], dtype=np.float32)
            # pyrefly: ignore [missing-attribute]
            self._index.add_items(np_vector, np.array([label]))
            self._store[key] = vector
            
            return "OK"

    def get(self, key: str) -> list[float] | None:
        """Get the raw vector for a key."""
        with self._lock:
            return self._store.get(key)

    def delete(self, key: str) -> int:
        """Mark a vector as deleted. (hnswlib supports soft-deletion)."""
        with self._lock:
            if key not in self._key_to_id:
                return 0
            
            label = self._key_to_id[key]
            if self._index is not None:
                self._index.mark_deleted(label)
            
            del self._store[key]
            del self._key_to_id[key]
            del self._id_to_key[label]
            
            return 1

    def search(self, query: list[float], top_k: int = 5) -> list[tuple[str, float]]:
        """
        Search for the top-k most similar vectors using HNSW.
        Returns a list of (key, similarity_score) tuples.
        """
        if hnswlib is None:
            raise RuntimeError("hnswlib is not installed.")

        with self._lock:
            if self._index is None or self._index.element_count == 0:
                return []
            
            vec_dim = len(query)
            if vec_dim != self._dim:
                raise ValueError(f"query dimension mismatch (expected {self._dim}, got {vec_dim})")
            
            # Ensure top_k doesn't exceed total non-deleted elements
            actual_k = min(top_k, len(self._store))
            if actual_k == 0:
                return []
                
            np_query = np.array([query], dtype=np.float32)
            
            # knn_query returns (labels, distances)
            # Distance for 'cosine' space is (1 - cosine_similarity).
            labels, distances = self._index.knn_query(np_query, k=actual_k)
            
            results = []
            for label, dist in zip(labels[0], distances[0]):
                # If deleted items somehow show up, skip them
                if label not in self._id_to_key:
                    continue
                key = self._id_to_key[label]
                # Convert distance back to similarity (1 - distance)
                similarity = 1.0 - dist
                results.append((key, float(similarity)))
            
            return results

    def count(self) -> int:
        """Return the number of active vectors."""
        with self._lock:
            return len(self._store)

    def clear(self):
        """Clear all data and destroy the index."""
        with self._lock:
            self._dim = None
            self._index = None
            self._key_to_id.clear()
            self._id_to_key.clear()
            self._next_id = 0
            self._store.clear()

# Global singleton for the AI Memory Layer
vector_index = VectorIndex()
