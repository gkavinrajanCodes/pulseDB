"""
examples/rag_chatbot.py
========================
Demonstrates PulseDB as the vector store backend for a LangChain RAG (Retrieval-Augmented
Generation) chatbot.

The chatbot:
  1. Ingests a small corpus of documents into PulseDB using upsert_batch().
  2. Accepts a user question.
  3. Retrieves the most semantically similar document chunks using Hybrid Search.
  4. Generates an answer using an LLM (OpenAI GPT-4o by default).

Requirements:
    pip install pulsedb langchain-openai langchain-core
    export OPENAI_API_KEY="sk-..."

    # Start PulseDB server:
    # docker run -d -p 6379:6379 -p 8000:8000 ghcr.io/gkavinrajancodes/pulsedb:latest
"""

import os
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# PulseDB LangChain integration
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from sdk.langchain_pulsedb.vectorstore import PulseDBVectorStore

# ---------------------------------------------------------------------------
# 1. Sample Document Corpus
# ---------------------------------------------------------------------------

DOCUMENTS = [
    {
        "text": "PulseDB is a high-performance in-memory database with a native AI Vector Engine using HNSW.",
        "metadata": {"source": "pulsedb_docs", "topic": "overview"}
    },
    {
        "text": "HNSW (Hierarchical Navigable Small World) is a graph-based algorithm for approximate nearest neighbor search.",
        "metadata": {"source": "research_paper", "topic": "vector_search"}
    },
    {
        "text": "PulseDB supports Hybrid Search, which combines metadata pre-filtering with vector similarity search in a single C++ callback.",
        "metadata": {"source": "pulsedb_docs", "topic": "hybrid_search"}
    },
    {
        "text": "LangChain is a framework for building LLM-powered applications. PulseDB integrates as a LangChain VectorStore.",
        "metadata": {"source": "langchain_docs", "topic": "integration"}
    },
    {
        "text": "Redis is an open-source in-memory data store. PulseDB is wire-compatible with Redis via the RESP2 protocol.",
        "metadata": {"source": "pulsedb_docs", "topic": "compatibility"}
    },
    {
        "text": "ZADD, ZRANGE, and ZRANGEBYSCORE are sorted set commands for implementing leaderboards and priority queues in PulseDB.",
        "metadata": {"source": "pulsedb_docs", "topic": "sorted_sets"}
    },
]


# ---------------------------------------------------------------------------
# 2. Build the Vector Store
# ---------------------------------------------------------------------------

def build_vectorstore() -> PulseDBVectorStore:
    embeddings = OpenAIEmbeddings()
    store = PulseDBVectorStore(embedding=embeddings, host="localhost", port=6379)

    texts = [doc["text"] for doc in DOCUMENTS]
    metadatas = [doc["metadata"] for doc in DOCUMENTS]

    print(f"[+] Ingesting {len(texts)} documents into PulseDB...")
    store.add_texts(texts, metadatas)
    print("[+] Documents indexed successfully!\n")
    return store


# ---------------------------------------------------------------------------
# 3. Build the RAG Chain
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a helpful assistant. Answer the user's question using ONLY
the context provided below. If the context doesn't contain enough information, say so.

Context:
{context}
"""

def format_docs(docs):
    return "\n\n".join(f"[{doc.metadata.get('source', 'unknown')}] {doc.page_content}" for doc in docs)


def build_rag_chain(vectorstore: PulseDBVectorStore, filter: dict | None = None):
    retriever = vectorstore.as_retriever(search_kwargs={"k": 3, "filter": filter})
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "{question}")
    ])

    chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    return chain


# ---------------------------------------------------------------------------
# 4. Run the Chatbot
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: Set OPENAI_API_KEY environment variable.")
        exit(1)

    store = build_vectorstore()

    questions = [
        ("What makes PulseDB different from Redis?", None),
        ("How does Hybrid Search work?", {"source": "pulsedb_docs"}),
        ("What is HNSW?", {"topic": "vector_search"}),
    ]

    for question, filter_dict in questions:
        print(f"Q: {question}")
        if filter_dict:
            print(f"   (filtering by: {filter_dict})")
        chain = build_rag_chain(store, filter=filter_dict)
        answer = chain.invoke(question)
        print(f"A: {answer}\n{'─'*60}\n")
