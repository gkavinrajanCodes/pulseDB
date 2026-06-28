import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

os.makedirs("benchmarks/plots", exist_ok=True)

# ---------------------------------------------------------
# USENIX Paper Styling
# ---------------------------------------------------------
plt.rcParams.update({
    "font.family": "serif",
    "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
    "font.size": 14,
    "axes.labelsize": 16,
    "axes.titlesize": 18,
    "xtick.labelsize": 14,
    "ytick.labelsize": 14,
    "legend.fontsize": 14,
    "lines.linewidth": 2.5,
    "figure.dpi": 300,
    "axes.grid": True,
    "grid.alpha": 0.5,
    "grid.linestyle": "--"
})

def plot_usenix_cdf():
    try:
        df = pd.read_csv("benchmarks/data/vector_latency_raw_cdf.csv")
    except FileNotFoundError:
        print("Missing CDF data")
        return

    plt.figure(figsize=(8, 5))
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
    styles = ['-', '--', ':']
    top_ks_to_plot = [1, 10, 100]
    
    for i, k in enumerate(top_ks_to_plot):
        data = df[df['top_k'] == k]['latency_ms'].sort_values()
        y = np.arange(1, len(data) + 1) / len(data)
        plt.plot(data, y, label=f'Top-{k}', color=colors[i], linestyle=styles[i])

    plt.xscale('log')
    plt.xlabel("Search Latency (ms) [Log Scale]")
    plt.ylabel("CDF")
    plt.title("CDF of HNSW Vector Search Latency")
    plt.xlim(left=0.1)
    plt.ylim(0, 1.05)
    plt.legend(loc='lower right')
    plt.tight_layout()
    plt.savefig("benchmarks/plots/usenix_latency_cdf.pdf")
    plt.close()

def plot_usenix_tradeoff():
    try:
        df = pd.read_csv("benchmarks/data/tradeoff.csv")
    except FileNotFoundError:
        print("Missing tradeoff data")
        return

    fig, ax1 = plt.subplots(figsize=(8, 5))

    color_qps = '#1f77b4'
    ax1.set_xlabel('Concurrent Clients')
    ax1.set_ylabel('Throughput (Queries/sec)', color=color_qps)
    ax1.plot(df['concurrency'], df['qps'], marker='o', color=color_qps, label='Throughput')
    ax1.tick_params(axis='y', labelcolor=color_qps)
    ax1.set_ylim(bottom=0)

    ax2 = ax1.twinx()  
    color_lat = '#d62728'
    ax2.set_ylabel('p99 Latency (ms)', color=color_lat)
    ax2.plot(df['concurrency'], df['p99_latency_ms'], marker='s', linestyle='--', color=color_lat, label='p99 Latency')
    ax2.tick_params(axis='y', labelcolor=color_lat)
    ax2.set_ylim(bottom=0)

    fig.tight_layout()
    plt.title("Throughput vs. Latency Tradeoff (Vector Search)")
    
    # Combine legends
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc='upper left')
    
    plt.savefig("benchmarks/plots/usenix_tradeoff.pdf")
    plt.close()

if __name__ == "__main__":
    print("[+] Generating USENIX Academic Plots...")
    plot_usenix_cdf()
    plot_usenix_tradeoff()
    print("  -> Saved to benchmarks/plots/")
