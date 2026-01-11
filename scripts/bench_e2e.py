#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["psutil"]
# ///
"""
End-to-end benchmark comparing sequential vs parallel parquet writers.

Generates test Arrow files, transforms them to Parquet with both writers,
and reports throughput + memory usage.

Usage:
    uv run scripts/bench_e2e.py
    uv run scripts/bench_e2e.py --rows 10000000 --cols 9
    uv run scripts/bench_e2e.py --quick  # smaller test for quick validation
    uv run scripts/bench_e2e.py --rows 800000000  # 800M rows
"""

import argparse
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import psutil


@dataclass
class Sample:
    timestamp: float
    rss_mb: float
    cpu_percent: float


@dataclass
class BenchResult:
    name: str
    duration_s: float
    throughput_mbs: float
    peak_rss_mb: float
    avg_rss_mb: float
    avg_cpu: float


def monitor_process(
    cmd: list[str], interval: float = 0.02
) -> tuple[int, float, list[Sample], str]:
    """Run command and collect samples. Returns (exit_code, duration, samples, stderr)."""
    samples: list[Sample] = []
    start = time.perf_counter()

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ps = psutil.Process(proc.pid)

    try:
        ps.cpu_percent()  # prime
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

    duration = time.perf_counter() - start
    _, stderr = proc.communicate()
    return proc.returncode or 0, duration, samples, stderr.decode()


def generate_input_file(
    silk_bin: Path,
    output_path: Path,
    rows: int,
    cols: int,
    input_format: str = "parquet",
    compression: str | None = None,
) -> int:
    """Generate an input file with specified dimensions. Returns file size in bytes."""

    script_dir = Path(__file__).parent.parent
    seed_candidates = [
        script_dir / "tests" / "files" / "people.file.arrow",
        script_dir / "tests" / "files" / "people.stream.arrow",
    ]
    seed_path = None
    for c in seed_candidates:
        if c.exists():
            seed_path = c
            break

    if not seed_path:
        print("Error: No seed file found in tests/files/")
        sys.exit(1)

    # use SQL to generate synthetic data from generate_series
    # use modulo to keep int32 values in range even with large row counts
    col_exprs = []
    for i in range(cols):
        match i % 5:
            case 0:
                col_exprs.append(
                    f"CAST((x % 100000000) * {i + 1} AS INTEGER) as int32_{i}"
                )
            case 1:
                col_exprs.append(f"CAST(x * {i + 1} AS BIGINT) as int64_{i}")
            case 2:
                col_exprs.append(f"CAST(x * {i + 1}.0 AS DOUBLE) as float64_{i}")
            case 3:
                col_exprs.append(f"'value_{i}_' || CAST(x AS VARCHAR) as string_{i}")
            case 4:
                col_exprs.append(f"CAST(x * 1000000 AS BIGINT) as timestamp_{i}")

    select_clause = ", ".join(col_exprs)
    query = f"SELECT {select_clause} FROM generate_series(1, {rows}) as t(x)"

    cmd = [
        str(silk_bin),
        "transform",
        "--from",
        str(seed_path),
        "-q",
        query,
        "--to",
        str(output_path),
        "--overwrite",
    ]

    if input_format == "arrow" and compression:
        cmd.extend(["--arrow-compression", compression])
    elif input_format == "parquet" and compression:
        cmd.extend(["--parquet-compression", compression])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error generating test file: {result.stderr}")
        sys.exit(1)

    return output_path.stat().st_size


def run_benchmark(
    silk_bin: Path,
    input_path: Path,
    output_path: Path,
    encoder: str,
    row_group_size: int,
    parquet_compression: str | None = None,
) -> BenchResult:
    """Run a single benchmark and return results."""
    cmd = [
        str(silk_bin),
        "transform",
        "--from",
        str(input_path),
        "--to",
        str(output_path),
        "--overwrite",
        "--parquet-row-group-size",
        str(row_group_size),
        "--parquet-encoder",
        encoder,
    ]
    if parquet_compression:
        cmd.extend(["--parquet-compression", parquet_compression])

    exit_code, duration, samples, stderr = monitor_process(cmd)

    if exit_code != 0:
        print(f"Benchmark failed with exit code {exit_code}")
        if stderr:
            print(f"stderr:\n{stderr}")
        sys.exit(1)

    if not samples:
        return BenchResult(
            name=encoder,
            duration_s=duration,
            throughput_mbs=0,
            peak_rss_mb=0,
            avg_rss_mb=0,
            avg_cpu=0,
        )

    input_size_mb = input_path.stat().st_size / 1024 / 1024
    peak_rss = max(s.rss_mb for s in samples)
    avg_rss = sum(s.rss_mb for s in samples) / len(samples)
    avg_cpu = sum(s.cpu_percent for s in samples) / len(samples)

    return BenchResult(
        name=encoder,
        duration_s=duration,
        throughput_mbs=input_size_mb / duration if duration > 0 else 0,
        peak_rss_mb=peak_rss,
        avg_rss_mb=avg_rss,
        avg_cpu=avg_cpu,
    )


def print_results(
    results: list[BenchResult], input_size_mb: float, rows: int, cols: int
) -> None:
    """Print benchmark comparison table."""
    print(f"\n{'=' * 70}")
    print(
        f"Configuration: {rows:,} rows × {cols} cols ({input_size_mb:.1f} MB Arrow file)"
    )
    print(f"{'=' * 70}")
    print(
        f"{'Encoder':<12} {'Duration':>10} {'Throughput':>14} {'Peak RSS':>12} {'Avg CPU':>10}"
    )
    print(f"{'-' * 70}")

    for r in results:
        print(
            f"{r.name:<12} "
            f"{r.duration_s:>9.2f}s "
            f"{r.throughput_mbs:>10.1f} MB/s "
            f"{r.peak_rss_mb:>10.1f} MB "
            f"{r.avg_cpu:>9.1f}%"
        )

    if len(results) == 2:
        seq, par = results[0], results[1]
        speedup = seq.duration_s / par.duration_s if par.duration_s > 0 else 0
        mem_ratio = par.peak_rss_mb / seq.peak_rss_mb if seq.peak_rss_mb > 0 else 0
        print(f"{'-' * 70}")
        print(f"Speedup: {speedup:.2f}x | Memory ratio: {mem_ratio:.2f}x")

    print(f"{'=' * 70}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="End-to-end parquet writer benchmark")
    parser.add_argument("--rows", type=int, default=10_000_000, help="Number of rows")
    parser.add_argument("--cols", type=int, default=9, help="Number of columns")
    parser.add_argument(
        "--row-group-size", type=int, default=1_000_000, help="Row group size"
    )
    parser.add_argument("--quick", action="store_true", help="Quick test with 1M rows")
    parser.add_argument("--bin", type=str, help="Path to silk-chiffon binary")
    parser.add_argument(
        "--input-format",
        type=str,
        default="parquet",
        choices=["arrow", "parquet"],
        help="Input file format (default: parquet)",
    )
    parser.add_argument(
        "--input-compression",
        type=str,
        default=None,
        help="Input file compression (default: none)",
    )
    parser.add_argument(
        "--parquet-compression",
        type=str,
        default=None,
        help="Output parquet compression (default: none)",
    )
    parser.add_argument(
        "--sequential-only", action="store_true", help="Only run sequential"
    )
    parser.add_argument(
        "--parallel-only", action="store_true", help="Only run parallel"
    )

    args = parser.parse_args()

    if args.quick:
        args.rows = 1_000_000
        args.cols = 5

    if args.bin:
        silk_bin = Path(args.bin)
    else:
        candidates = [
            Path("target/release/silk-chiffon"),
            Path("./silk-chiffon"),
        ]
        silk_bin = None
        for c in candidates:
            if c.exists():
                silk_bin = c.resolve()
                break

        if not silk_bin:
            print(
                "Error: silk-chiffon binary not found. Build with: cargo build --release"
            )
            return 1

    print(f"Using binary: {silk_bin}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        input_ext = ".parquet" if args.input_format == "parquet" else ".arrow"
        input_path = tmp / f"test{input_ext}"
        output_path = tmp / "output.parquet"

        compression = (
            args.input_compression if args.input_compression != "none" else None
        )
        print(f"Generating test data: {args.rows:,} rows × {args.cols} cols...")
        print(f"  (Format: {args.input_format}, compression: {compression or 'none'})")
        input_size = generate_input_file(
            silk_bin,
            input_path,
            args.rows,
            args.cols,
            input_format=args.input_format,
            compression=compression,
        )
        input_size_mb = input_size / 1024 / 1024
        print(f"Created {input_path.name}: {input_size_mb:.1f} MB")

        results = []
        run_sequential = not args.parallel_only
        run_parallel = not args.sequential_only

        if run_sequential:
            print("\nRunning benchmark (sequential encoder)...")
            result = run_benchmark(
                silk_bin,
                input_path,
                output_path,
                encoder="sequential",
                row_group_size=args.row_group_size,
                parquet_compression=args.parquet_compression,
            )
            results.append(result)

        if run_parallel:
            print("\nRunning benchmark (parallel encoder)...")
            result = run_benchmark(
                silk_bin,
                input_path,
                output_path,
                encoder="parallel",
                row_group_size=args.row_group_size,
                parquet_compression=args.parquet_compression,
            )
            results.append(result)

        print_results(results, input_size_mb, args.rows, args.cols)

    return 0


if __name__ == "__main__":
    sys.exit(main())
