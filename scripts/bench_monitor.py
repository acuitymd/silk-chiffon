#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["psutil"]
# ///
"""
Monitor a command's CPU and memory usage over time.

Usage:
    uv run scripts/bench_monitor.py -- ./target/release/silk_chiffon transform input.arrow output.parquet
    uv run scripts/bench_monitor.py --interval 0.1 -- command args...
"""

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass

import psutil


@dataclass
class Sample:
    timestamp: float
    rss_mb: float
    cpu_percent: float


def monitor_process(cmd: list[str], interval: float = 0.05) -> tuple[int, list[Sample]]:
    samples: list[Sample] = []
    start = time.perf_counter()

    proc = subprocess.Popen(cmd)
    ps = psutil.Process(proc.pid)

    # prime cpu_percent (first call always returns 0)
    try:
        ps.cpu_percent()
    except psutil.NoSuchProcess:
        pass

    while proc.poll() is None:
        try:
            mem = ps.memory_info()
            cpu = ps.cpu_percent()
            samples.append(Sample(
                timestamp=time.perf_counter() - start,
                rss_mb=mem.rss / 1024 / 1024,
                cpu_percent=cpu,
            ))
        except psutil.NoSuchProcess:
            break
        time.sleep(interval)

    return proc.returncode or 0, samples


def print_results(samples: list[Sample], exit_code: int) -> None:
    if not samples:
        print("No samples collected (process exited too fast)")
        return

    duration = samples[-1].timestamp
    peak_rss = max(s.rss_mb for s in samples)
    avg_rss = sum(s.rss_mb for s in samples) / len(samples)
    avg_cpu = sum(s.cpu_percent for s in samples) / len(samples)
    peak_cpu = max(s.cpu_percent for s in samples)

    print(f"\n{'=' * 50}")
    print(f"Exit code:    {exit_code}")
    print(f"Duration:     {duration:.2f}s")
    print(f"Samples:      {len(samples)}")
    print(f"{'=' * 50}")
    print("Memory (RSS):")
    print(f"  Peak:       {peak_rss:.1f} MB")
    print(f"  Average:    {avg_rss:.1f} MB")
    print("CPU:")
    print(f"  Peak:       {peak_cpu:.1f}%")
    print(f"  Average:    {avg_cpu:.1f}%")
    print(f"{'=' * 50}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor command CPU/memory usage")
    parser.add_argument("--interval", type=float, default=0.05, help="Sample interval in seconds")
    parser.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run (after --)")

    args = parser.parse_args()

    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd:
        parser.print_help()
        return 1

    print(f"Running: {' '.join(cmd)}")
    print(f"Sampling every {args.interval}s...")

    exit_code, samples = monitor_process(cmd, args.interval)
    print_results(samples, exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
