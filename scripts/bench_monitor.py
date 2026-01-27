#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["psutil"]
# ///
"""
Monitor a command's CPU and memory usage over time.

Usage:
    uv run scripts/bench_monitor.py -- ./target/release/silk_chiffon transform input.arrow output.parquet
    uv run scripts/bench_monitor.py --interval 1 -- command args...
    uv run scripts/bench_monitor.py --csv timeseries.csv -- command args...
"""

import argparse
import csv
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psutil


@dataclass
class Sample:
    timestamp: float
    rss_mb: float
    cpu_percent: float


def monitor_process(cmd: list[str], interval: float = 1) -> tuple[int, list[Sample]]:
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
            samples.append(
                Sample(
                    timestamp=time.perf_counter() - start,
                    rss_mb=mem.rss / 1024 / 1024,
                    cpu_percent=cpu,
                )
            )
        except psutil.NoSuchProcess:
            break
        time.sleep(interval)

    return proc.returncode or 0, samples


def write_csv(samples: list[Sample], path: Path) -> None:
    """Write samples to a CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["elapsed_s", "cpu_percent", "rss_mb"])
        for s in samples:
            writer.writerow(
                [
                    f"{s.timestamp:.3f}",
                    f"{s.cpu_percent:.1f}",
                    f"{s.rss_mb:.1f}",
                ]
            )


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
    parser.add_argument(
        "--interval", type=float, default=1, help="Sample interval in seconds"
    )
    parser.add_argument(
        "--csv", type=Path, metavar="PATH", help="Write time-series data to CSV file"
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress console output (only write CSV)",
    )
    parser.add_argument(
        "cmd", nargs=argparse.REMAINDER, help="Command to run (after --)"
    )

    args = parser.parse_args()

    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd:
        parser.print_help()
        return 1

    if not args.quiet:
        print(f"Running: {' '.join(cmd)}")
        print(f"Sampling every {args.interval}s...")

    exit_code, samples = monitor_process(cmd, args.interval)

    if args.csv:
        write_csv(samples, args.csv)
        if not args.quiet:
            print(f"Wrote {len(samples)} samples to {args.csv}")

    if not args.quiet:
        print_results(samples, exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
