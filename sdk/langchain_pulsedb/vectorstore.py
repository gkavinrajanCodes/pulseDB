import json
import uuid
from typing import Any, Iterable, List, Optional, Tuple

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from pulsedb import PulseDB


class PulseDBVectorStore(VectorStore):
    """PulseDB VectorStore wrapper for LangChain."""

    def __init__(
        self,
        embedding: Embeddings,
        client: Optional[PulseDB] = None,
        host: str = "localhost",
        port: int = 8000,
        api_key: str = "pulse-db-secret-key",
        collection_name: str = "langchain",
    ):
        self._embedding = embedding
        self._client = client or PulseDB(host=host, port=port, api_key=api_key)
        self._collection = collection_name

    def _get_key(self, doc_id: str) -> str:
        return f"{self._collection}:{doc_id}"

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """Run more texts through the embeddings and add to the vectorstore."""
        texts = list(texts)
        if not texts:
            return []

        embeddings = self._embedding.embed_documents(texts)
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in texts]
        if metadatas is None:
            metadatas = [{} for _ in texts]

        for text, metadata, doc_id, embedding in zip(texts, metadatas, ids, embeddings):
            key = self._get_key(doc_id)
            
            # Store the embedding
            self._client._async._cmd("VECTOR.SET", key, *embedding)
            
            # Store the document data as a hash
            doc_data = {
                "text": text,
                "metadata": json.dumps(metadata)
            }
            self._client.hmset(f"{key}:data", doc_data)

        return ids

    def similarity_search(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> List[Document]:
        """Return docs most similar to query."""
        results = self.similarity_search_with_score(query, k=k, **kwargs)
        return [doc for doc, _ in results]

    def similarity_search_with_score(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> List[Tuple[Document, float]]:
        """Return docs most similar to query, along with scores."""
        embedding = self._embedding.embed_query(query)
        
        # Search the vector index
        raw_results = self._client._async._cmd("VECTOR.SEARCH", *embedding, "TOP_K", k)
        
        # Raw results come as [key1, score1, key2, score2, ...]
        if not isinstance(raw_results, list) or not raw_results:
            return []

        docs_with_scores = []
        for i in range(0, len(raw_results), 2):
            key = raw_results[i]
            score = float(raw_results[i+1])
            
            # Only process keys in our collection
            if not key.startswith(f"{self._collection}:"):
                continue

            # Fetch document data
            data = self._client.hgetall(f"{key}:data")
            if not data:
                continue

            # data is a flat list [field1, val1, field2, val2...]
            # Convert to dict
            doc_dict = {data[j]: data[j+1] for j in range(0, len(data), 2)}
            
            text = doc_dict.get("text", "")
            metadata = json.loads(doc_dict.get("metadata", "{}"))
            
            doc = Document(page_content=text, metadata=metadata)
            docs_with_scores.append((doc, score))

        return docs_with_scores

    @classmethod
    def from_texts(
        cls,
        texts: List[str],
        embedding: Embeddings,
        metadatas: Optional[List[dict]] = None,
        **kwargs: Any,
    ) -> "PulseDBVectorStore":
        """Return VectorStore initialized from texts and embeddings."""
        store = cls(embedding, **kwargs)
        store.add_texts(texts, metadatas)
        return store
