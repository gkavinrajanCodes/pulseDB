import uuid
from typing import Any, Iterable, List, Optional, Tuple, Dict

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
        port: int = 6379,
        collection_name: str = "langchain",
    ):
        self._embedding = embedding
        self._client = client or PulseDB(host=host, port=port)
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
            
            # Embed the text directly into the PulseDB Hybrid Search metadata dictionary
            doc_metadata = metadata.copy()
            doc_metadata["_text"] = text
            
            # Use the blazing fast TCP Vector Namespace
            self._client.vectors.upsert(key, embedding, metadata=doc_metadata)

        return ids

    def similarity_search(
        self, query: str, k: int = 4, filter: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> List[Document]:
        """Return docs most similar to query."""
        results = self.similarity_search_with_score(query, k=k, filter=filter, **kwargs)
        return [doc for doc, _ in results]

    def similarity_search_with_score(
        self, query: str, k: int = 4, filter: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> List[Tuple[Document, float]]:
        """Return docs most similar to query, along with scores."""
        embedding = self._embedding.embed_query(query)
        
        # Search the vector index using the native TCP Binary Protocol
        raw_results = self._client.vectors.search(embedding, top_k=k, filter=filter)
        
        docs_with_scores = []
        for res in raw_results:
            key = res["id"]
            score = res["score"]
            
            # Only process keys in our collection
            if not key.startswith(f"{self._collection}:"):
                continue

            # Fetch the metadata dictionary
            doc_data = self._client.vectors.get(key)
            if not doc_data:
                continue

            metadata = doc_data.get("metadata", {})
            text = metadata.pop("_text", "")
            
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
