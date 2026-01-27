#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "pandas"]
# ///
"""
Plot CPU and memory usage from bench_monitor CSV output.

Usage:
    uv run scripts/plot_monitor.py timeseries.csv
    uv run scripts/plot_monitor.py timeseries.csv output.png
"""
import sys

import matplotlib.pyplot as plt
import pandas as pd


def main():
    if len(sys.argv) < 2:
        print("Usage: plot_monitor.py <csv_file> [output.png]")
        sys.exit(1)

    df = pd.read_csv(sys.argv[1])

    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.set_xlabel("Elapsed (s)")
    ax1.set_ylabel("CPU %", color="tab:blue")
    ax1.plot(df["elapsed_s"], df["cpu_percent"], color="tab:blue", label="CPU")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()
    ax2.set_ylabel("Memory (MB)", color="tab:red")
    ax2.plot(df["elapsed_s"], df["rss_mb"], color="tab:red", label="Memory")
    ax2.tick_params(axis="y", labelcolor="tab:red")

    fig.tight_layout()
    output = sys.argv[2] if len(sys.argv) > 2 else "chart.png"
    plt.savefig(output, dpi=150)
    print(f"Saved to {output}")
    plt.show()


if __name__ == "__main__":
    main()
