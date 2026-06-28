import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

os.makedirs("benchmarks/plots", exist_ok=True)
sns.set_theme(style="whitegrid")

def plot_kv_throughput():
    try:
        df = pd.read_csv("benchmarks/data/kv_throughput.csv")
    except FileNotFoundError:
        return
        
    plt.figure(figsize=(8, 5))
    sns.lineplot(data=df, x="concurrency", y="qps", marker="o", linewidth=2.5)
    plt.title("KV Store Throughput vs. Concurrency", fontsize=14, pad=15)
    plt.xlabel("Concurrent Clients", fontsize=12)
    plt.ylabel("Throughput (Requests / sec)", fontsize=12)
    plt.ylim(bottom=0)
    plt.tight_layout()
    plt.savefig("benchmarks/plots/kv_throughput.pdf")
    plt.close()

def plot_vector_batch():
    try:
        df = pd.read_csv("benchmarks/data/vector_batch.csv")
    except FileNotFoundError:
        return
        
    plt.figure(figsize=(8, 5))
    sns.barplot(data=df, x="batch_size", y="vectors_per_sec", color="#4C72B0")
    plt.title("Vector Batch Ingestion Performance", fontsize=14, pad=15)
    plt.xlabel("Batch Size", fontsize=12)
    plt.ylabel("Throughput (Vectors / sec)", fontsize=12)
    plt.tight_layout()
    plt.savefig("benchmarks/plots/vector_batch.pdf")
    plt.close()

def plot_vector_latency():
    try:
        df = pd.read_csv("benchmarks/data/vector_latency.csv")
    except FileNotFoundError:
        return
        
    plt.figure(figsize=(8, 5))
    sns.lineplot(data=df, x="top_k", y="avg_ms", label="Average", marker="o", linewidth=2.5)
    sns.lineplot(data=df, x="top_k", y="p99_ms", label="p99 (Tail)", marker="s", linewidth=2.5, linestyle="--")
    plt.title("HNSW Vector Search Latency", fontsize=14, pad=15)
    plt.xlabel("Top-K Retrievals", fontsize=12)
    plt.ylabel("Latency (Milliseconds)", fontsize=12)
    plt.ylim(bottom=0)
    plt.legend()
    plt.tight_layout()
    plt.savefig("benchmarks/plots/vector_latency.pdf")
    plt.close()

if __name__ == "__main__":
    print("[+] Generating PDF Plots for USENIX Paper...")
    plot_kv_throughput()
    plot_vector_batch()
    plot_vector_latency()
    print("  -> Saved to benchmarks/plots/")
