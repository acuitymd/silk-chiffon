#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["psutil"]
# ///
"""Benchmark Arrow-to-Parquet conversion: DuckDB vs silk-chiffon."""

import argparse
import csv
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import psutil


@dataclass
class BenchmarkResult:
    name: str
    wall_time: float
    peak_rss_mb: float
    avg_cpu_percent: float
    peak_cpu_percent: float
    file_size: int
    samples: list[dict] = field(default_factory=list)


def check_duckdb_cli() -> None:
    """Verify DuckDB CLI is available."""
    try:
        result = subprocess.run(["duckdb", "--version"], capture_output=True, text=True)
        if result.returncode != 0:
            print("Error: DuckDB CLI not found. Install with: brew install duckdb")
            sys.exit(1)
        print(f"DuckDB version: {result.stdout.strip()}")
    except FileNotFoundError:
        print("Error: DuckDB CLI not found. Install with: brew install duckdb")
        sys.exit(1)


def run_command(
    cmd: list[str], description: str, capture: bool = False
) -> subprocess.CompletedProcess:
    """Run a command with timing info."""
    print(f"  {description}...")
    start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=capture, text=True)
    elapsed = time.perf_counter() - start
    if result.returncode != 0:
        print(f"    FAILED ({elapsed:.2f}s)")
        if capture:
            print(f"    stdout: {result.stdout}")
            print(f"    stderr: {result.stderr}")
        sys.exit(1)
    print(f"    done ({elapsed:.2f}s)")
    return result


def monitor_process(
    cmd: list[str], interval: float = 0.05
) -> tuple[int, float, list[dict]]:
    """Monitor a process and collect time-series samples."""
    samples = []
    start = time.perf_counter()

    proc = subprocess.Popen(cmd)
    ps = psutil.Process(proc.pid)

    try:
        ps.cpu_percent()
    except psutil.NoSuchProcess:
        pass

    while proc.poll() is None:
        try:
            mem = ps.memory_info()
            cpu = ps.cpu_percent()
            samples.append(
                {
                    "elapsed_s": time.perf_counter() - start,
                    "rss_mb": mem.rss / 1024 / 1024,
                    "cpu_percent": cpu,
                }
            )
        except psutil.NoSuchProcess:
            break
        time.sleep(interval)

    wall_time = time.perf_counter() - start
    return proc.returncode or 0, wall_time, samples


def build_native(project_root: Path) -> Path:
    """Build silk-chiffon with native optimizations."""
    print("\n[1/5] Building silk-chiffon with native profile...")
    run_command(["just", "build-native"], "cargo build --profile native", capture=True)
    binary = project_root / "target" / "native" / "silk-chiffon"
    if not binary.exists():
        print(f"Binary not found at {binary}")
        sys.exit(1)
    return binary


def build_release(project_root: Path) -> Path:
    """Build silk-chiffon with release profile."""
    print("\n[1/5] Building silk-chiffon with release profile...")
    run_command(["just", "build"], "cargo build --release", capture=True)
    binary = project_root / "target" / "release" / "silk-chiffon"
    if not binary.exists():
        print(f"Binary not found at {binary}")
        sys.exit(1)
    return binary


def generate_test_data(output_dir: Path, rows: int, threads: int) -> Path:
    """Generate an Arrow IPC stream file with diverse column types via DuckDB."""
    print(f"\n[2/5] Generating test data ({rows:,} rows)...")
    arrow_file = output_dir / "test_data.arrow"

    sql_file = output_dir / "generate_data.sql"
    sql = f"""
INSTALL nanoarrow FROM community;
LOAD nanoarrow;
SET threads = {threads};

COPY (
    SELECT
        (i % 127)::TINYINT as int8_col,
        (i % 32767)::SMALLINT as int16_col,
        (i % 2147483647)::INTEGER as int32_col,
        i::BIGINT as int64_col,
        (i % 1000000000)::BIGINT as npi,
        ((i * 37) % 1000000000)::BIGINT as target_npi,
        (i * 1.5)::DOUBLE as float64_col,
        '2020-01-01'::TIMESTAMP + INTERVAL (i % 1000000) SECOND as ts_col,
        (i % 2 = 0)::BOOLEAN as bool_col,
        '2020-01-01'::DATE + (i % 1000)::INTEGER as date_col,

        concat('low_', (i % 10)::VARCHAR) as string_low_card,
        concat('high_', i::VARCHAR) as string_high_card,

        [(i % 5), (i % 5) + 1, (i % 5) + 2]::BIGINT[] as list_low_card,
        [i, i+1, i+2]::BIGINT[] as list_high_card,

        {{'low': concat('cat_', (i % 20)::VARCHAR), 'high': concat('id_', i::VARCHAR)}} as struct_mixed,

        {{'a': (i % 5)::INTEGER, 'b': concat('v', (i % 8)::VARCHAR)}} as struct_low_card,

        [
            {{'x': (i % 10)::INTEGER, 'y': concat('p', (i % 5)::VARCHAR)}},
            {{'x': ((i+1) % 10)::INTEGER, 'y': concat('p', ((i+1) % 5)::VARCHAR)}}
        ] as list_of_struct,

        MAP([concat('k', (i % 3)::VARCHAR)], [(i % 10)::INTEGER]) as map_low_card,

        MAP([concat('key', (i % 5)::VARCHAR)], [i::INTEGER]) as map_high_card_val,

        [[(i % 5), (i % 5) + 1], [(i % 5) + 2, (i % 5) + 3]]::BIGINT[][] as list_of_list

    FROM generate_series(1, {rows}) t(i)
) TO '{arrow_file}' (FORMAT 'arrow');
"""
    sql_file.write_text(sql)

    print(f"  Executing DuckDB query to generate {rows:,} rows...")
    start = time.perf_counter()
    result = subprocess.run(
        ["duckdb", "-c", f".read {sql_file}"],
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - start

    if result.returncode != 0:
        print(f"    FAILED: {result.stderr}")
        sys.exit(1)

    print(f"    done ({elapsed:.2f}s)")
    sql_file.unlink()

    file_size = arrow_file.stat().st_size
    print(f"  Arrow file size: {file_size / (1024**3):.2f} GiB")

    return arrow_file


def run_duckdb_conversion(
    arrow_file: Path,
    output_file: Path,
    threads: int,
    memory_limit_mib: int,
) -> BenchmarkResult:
    """Run DuckDB Arrow-to-Parquet conversion with monitoring."""
    print("\n[3/5] Running DuckDB conversion...")

    sql_file = output_file.parent / "duckdb_convert.sql"
    sql = f"""
LOAD nanoarrow;
SET threads = {threads};
SET memory_limit = '{memory_limit_mib} MiB';

COPY (SELECT * FROM '{arrow_file}' ORDER BY npi, target_npi)
TO '{output_file}'
(FORMAT 'parquet', COMPRESSION 'lz4_raw', ROW_GROUP_SIZE 1048576, PARQUET_VERSION 'V2', PRESERVE_ORDER true);
"""
    sql_file.write_text(sql)

    print("  Warm-up run...")
    warmup_output = output_file.parent / "warmup_duckdb.parquet"
    warmup_sql = f"""
LOAD nanoarrow;
SET threads = {threads};
SET memory_limit = '{memory_limit_mib} MiB';

COPY (SELECT * FROM '{arrow_file}' ORDER BY npi, target_npi)
TO '{warmup_output}'
(FORMAT 'parquet', COMPRESSION 'lz4_raw', ROW_GROUP_SIZE 1048576, PARQUET_VERSION 'V2', PRESERVE_ORDER true);
"""
    warmup_sql_file = output_file.parent / "warmup_duckdb.sql"
    warmup_sql_file.write_text(warmup_sql)

    start = time.perf_counter()
    result = subprocess.run(
        ["duckdb", "-c", f".read {warmup_sql_file}"], capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"    FAILED: {result.stderr}")
        sys.exit(1)
    warmup_time = time.perf_counter() - start
    print(f"    warmup done ({warmup_time:.2f}s)")
    warmup_output.unlink()
    warmup_sql_file.unlink()

    print("  Monitored run...")
    duckdb_cmd = ["duckdb", "-c", f".read {sql_file}"]
    exit_code, wall_time, samples = monitor_process(duckdb_cmd)

    if exit_code != 0:
        print(f"DuckDB conversion failed with exit code {exit_code}")
        sys.exit(1)

    sql_file.unlink()

    if samples:
        peak_rss = max(s["rss_mb"] for s in samples)
        avg_cpu = sum(s["cpu_percent"] for s in samples) / len(samples)
        peak_cpu = max(s["cpu_percent"] for s in samples)
    else:
        peak_rss = avg_cpu = peak_cpu = 0

    file_size = output_file.stat().st_size
    print(f"    done ({wall_time:.2f}s, peak RSS: {peak_rss:.0f} MB)")

    return BenchmarkResult(
        name="DuckDB",
        wall_time=wall_time,
        peak_rss_mb=peak_rss,
        avg_cpu_percent=avg_cpu,
        peak_cpu_percent=peak_cpu,
        file_size=file_size,
        samples=samples,
    )


def run_silk_conversion(
    silk_cmd: list[str],
    arrow_file: Path,
    output_file: Path,
) -> BenchmarkResult:
    """Run silk-chiffon Arrow-to-Parquet conversion with monitoring."""
    print("\n[4/5] Running silk-chiffon conversion...")

    print("  Warm-up run...")
    warmup_output = output_file.parent / "warmup_silk.parquet"
    warmup_cmd = [
        *silk_cmd,
        "transform",
        "--from",
        str(arrow_file),
        "--to",
        str(warmup_output),
        "--parquet-compression",
        "lz4",
        "--parquet-row-group-size",
        "1048576",
        "--parquet-writer-version",
        "v2",
        "--sort-by",
        "npi, target_npi",
        "--overwrite",
    ]
    start = time.perf_counter()
    result = subprocess.run(warmup_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"    FAILED: {result.stderr}")
        sys.exit(1)
    warmup_time = time.perf_counter() - start
    print(f"    warmup done ({warmup_time:.2f}s)")
    warmup_output.unlink()

    print("  Monitored run...")
    full_cmd = [
        *silk_cmd,
        "transform",
        "--from",
        str(arrow_file),
        "--to",
        str(output_file),
        "--parquet-compression",
        "lz4",
        "--parquet-row-group-size",
        "1048576",
        "--parquet-writer-version",
        "v2",
        "--sort-by",
        "npi, target_npi",
        "--overwrite",
    ]
    exit_code, wall_time, samples = monitor_process(full_cmd)

    if exit_code != 0:
        print(f"silk-chiffon conversion failed with exit code {exit_code}")
        sys.exit(1)

    if samples:
        peak_rss = max(s["rss_mb"] for s in samples)
        avg_cpu = sum(s["cpu_percent"] for s in samples) / len(samples)
        peak_cpu = max(s["cpu_percent"] for s in samples)
    else:
        peak_rss = avg_cpu = peak_cpu = 0

    file_size = output_file.stat().st_size
    print(f"    done ({wall_time:.2f}s, peak RSS: {peak_rss:.0f} MB)")

    return BenchmarkResult(
        name="silk-chiffon",
        wall_time=wall_time,
        peak_rss_mb=peak_rss,
        avg_cpu_percent=avg_cpu,
        peak_cpu_percent=peak_cpu,
        file_size=file_size,
        samples=samples,
    )


def inspect_parquet(silk_cmd: list[str], parquet_file: Path) -> dict[str, Any]:
    """Get parquet file metadata using silk-chiffon inspect."""
    result = subprocess.run(
        [
            *silk_cmd,
            "inspect",
            "parquet",
            str(parquet_file),
            "--pages",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def compare_outputs(
    silk_cmd: list[str],
    duckdb_file: Path,
    silk_file: Path,
) -> dict[str, Any]:
    """Compare parquet outputs and return differences."""
    print("\n[5/5] Comparing outputs...")

    duck_meta = inspect_parquet(silk_cmd, duckdb_file)
    silk_meta = inspect_parquet(silk_cmd, silk_file)

    comparison = {
        "row_count": {
            "duckdb": duck_meta["rows"],
            "silk": silk_meta["rows"],
            "match": duck_meta["rows"] == silk_meta["rows"],
        },
        "num_columns": {
            "duckdb": duck_meta["num_columns"],
            "silk": silk_meta["num_columns"],
            "match": duck_meta["num_columns"] == silk_meta["num_columns"],
        },
        "num_row_groups": {
            "duckdb": duck_meta["num_row_groups"],
            "silk": silk_meta["num_row_groups"],
            "match": duck_meta["num_row_groups"] == silk_meta["num_row_groups"],
        },
        "file_size": {
            "duckdb": duck_meta["file_size"],
            "silk": silk_meta["file_size"],
            "diff_percent": abs(duck_meta["file_size"] - silk_meta["file_size"])
            / duck_meta["file_size"]
            * 100,
        },
        "compression": {
            "duckdb": duck_meta["compression"],
            "silk": silk_meta["compression"],
            "match": duck_meta["compression"] == silk_meta["compression"],
        },
        "format_version": {
            "duckdb": duck_meta["format_version"],
            "silk": silk_meta["format_version"],
            "match": duck_meta["format_version"] == silk_meta["format_version"],
        },
        "created_by": {
            "duckdb": duck_meta.get("created_by"),
            "silk": silk_meta.get("created_by"),
        },
    }

    duck_cols = {c["name"]: c for c in duck_meta["row_groups"][0]["columns"]}
    silk_cols = {c["name"]: c for c in silk_meta["row_groups"][0]["columns"]}

    column_diffs = []
    dict_comparison = []

    nested_prefixes = [
        "list_low_card",
        "list_high_card",
        "struct_mixed",
        "struct_low_card",
        "list_of_struct",
        "map_low_card",
        "map_high_card_val",
        "list_of_list",
    ]

    for col_name in sorted(duck_cols.keys()):
        if col_name not in silk_cols:
            column_diffs.append(f"Column {col_name} missing in silk output")
            continue

        duck_col = duck_cols[col_name]
        silk_col = silk_cols[col_name]

        if duck_col["compression"] != silk_col["compression"]:
            column_diffs.append(
                f"{col_name}: compression differs ({duck_col['compression']} vs {silk_col['compression']})"
            )

        is_nested = any(
            col_name.startswith(p) or f".{p}" in col_name for p in nested_prefixes
        )
        duck_dict = duck_col.get("has_dictionary", False)
        silk_dict = silk_col.get("has_dictionary", False)

        if is_nested or duck_dict != silk_dict:
            dict_comparison.append(
                {
                    "column": col_name,
                    "duckdb_dict": duck_dict,
                    "silk_dict": silk_dict,
                    "match": duck_dict == silk_dict,
                }
            )

        if duck_dict != silk_dict:
            column_diffs.append(
                f"{col_name}: dictionary encoding differs (DuckDB={duck_dict}, silk={silk_dict})"
            )

    comparison["column_diffs"] = column_diffs
    comparison["dict_comparison"] = dict_comparison

    all_match = (
        comparison["row_count"]["match"]
        and comparison["num_columns"]["match"]
        and comparison["compression"]["match"]
        and len(column_diffs) == 0
    )
    comparison["all_critical_match"] = all_match

    print(f"  Row count match: {comparison['row_count']['match']}")
    print(f"  Compression match: {comparison['compression']['match']}")
    print(f"  File size diff: {comparison['file_size']['diff_percent']:.1f}%")

    return comparison


def write_timeseries_csv(samples: list[dict], path: Path) -> None:
    """Write time-series samples to CSV."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["elapsed_s", "cpu_percent", "rss_mb"])
        writer.writeheader()
        for s in samples:
            writer.writerow(
                {
                    "elapsed_s": f"{s['elapsed_s']:.3f}",
                    "cpu_percent": f"{s['cpu_percent']:.1f}",
                    "rss_mb": f"{s['rss_mb']:.1f}",
                }
            )


def generate_report(
    output_dir: Path,
    duckdb_result: BenchmarkResult,
    silk_result: BenchmarkResult,
    comparison: dict[str, Any],
    config: dict[str, Any],
) -> Path:
    """Generate markdown report."""
    report_path = output_dir / "report.md"

    def fmt_bytes(b: int) -> str:
        if b >= 1024**3:
            return f"{b / 1024**3:.2f} GiB"
        elif b >= 1024**2:
            return f"{b / 1024**2:.2f} MiB"
        elif b >= 1024:
            return f"{b / 1024:.2f} KiB"
        return f"{b} B"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# Benchmark Report: DuckDB vs silk-chiffon",
        "",
        f"**Generated**: {timestamp}",
        "",
        "## Configuration",
        "",
    ]

    if config.get("input_file"):
        lines.append(f"- **Input file**: `{config['input_file']}`")
    else:
        lines.append(f"- **Rows**: {config['rows']:,}")

    lines.extend(
        [
            f"- **Threads**: {config['threads']}",
            f"- **Memory limit (DuckDB)**: {config['memory_limit_mib']} MiB",
            "- **Compression**: LZ4 (raw)",
            "- **Row group size**: 1,048,576",
            "- **Parquet version**: V2",
            "",
            "## Performance Results",
            "",
            "| Metric | DuckDB | silk-chiffon | Difference |",
            "|--------|--------|--------------|------------|",
            f"| Wall time | {duckdb_result.wall_time:.2f}s | {silk_result.wall_time:.2f}s | {(silk_result.wall_time - duckdb_result.wall_time) / duckdb_result.wall_time * 100:+.1f}% |",
            f"| Peak RSS | {duckdb_result.peak_rss_mb:.0f} MB | {silk_result.peak_rss_mb:.0f} MB | {(silk_result.peak_rss_mb - duckdb_result.peak_rss_mb) / duckdb_result.peak_rss_mb * 100:+.1f}% |",
            f"| Peak CPU | {duckdb_result.peak_cpu_percent:.0f}% | {silk_result.peak_cpu_percent:.0f}% | - |",
            f"| Avg CPU | {duckdb_result.avg_cpu_percent:.0f}% | {silk_result.avg_cpu_percent:.0f}% | - |",
            f"| Output size | {fmt_bytes(duckdb_result.file_size)} | {fmt_bytes(silk_result.file_size)} | {(silk_result.file_size - duckdb_result.file_size) / duckdb_result.file_size * 100:+.1f}% |",
            "",
            "## Output Comparison",
            "",
            "| Property | DuckDB | silk-chiffon | Match |",
            "|----------|--------|--------------|-------|",
            f"| Row count | {comparison['row_count']['duckdb']:,} | {comparison['row_count']['silk']:,} | {'✓' if comparison['row_count']['match'] else '✗'} |",
            f"| Columns | {comparison['num_columns']['duckdb']} | {comparison['num_columns']['silk']} | {'✓' if comparison['num_columns']['match'] else '✗'} |",
            f"| Row groups | {comparison['num_row_groups']['duckdb']} | {comparison['num_row_groups']['silk']} | {'✓' if comparison['num_row_groups']['match'] else '✗'} |",
            f"| Compression | {comparison['compression']['duckdb']} | {comparison['compression']['silk']} | {'✓' if comparison['compression']['match'] else '✗'} |",
            f"| Format version | {comparison['format_version']['duckdb']} | {comparison['format_version']['silk']} | {'✓' if comparison['format_version']['match'] else '✗'} |",
            "",
        ]
    )

    if comparison.get("dict_comparison"):
        lines.extend(
            [
                "## Dictionary Encoding (Nested Columns)",
                "",
                "| Column | DuckDB Dict | silk-chiffon Dict | Match |",
                "|--------|-------------|-------------------|-------|",
            ]
        )
        for dc in comparison["dict_comparison"]:
            duck_str = "✓" if dc["duckdb_dict"] else "✗"
            silk_str = "✓" if dc["silk_dict"] else "✗"
            match_str = "✓" if dc["match"] else "**✗**"
            lines.append(f"| {dc['column']} | {duck_str} | {silk_str} | {match_str} |")
        lines.append("")

    if comparison["column_diffs"]:
        lines.extend(
            [
                "### Column Differences",
                "",
            ]
        )
        for diff in comparison["column_diffs"]:
            lines.append(f"- {diff}")
        lines.append("")

    lines.extend(
        [
            "## Created By",
            "",
            f"- **DuckDB**: {comparison['created_by']['duckdb']}",
            f"- **silk-chiffon**: {comparison['created_by']['silk']}",
            "",
            "## Files",
            "",
        ]
    )

    if config.get("input_file"):
        lines.append(f"- `{config['input_file']}` - Input Arrow file")
    else:
        lines.append("- `test_data.arrow` - Input Arrow IPC stream")

    lines.extend(
        [
            "- `duckdb_output.parquet` - DuckDB output",
            "- `silk_output.parquet` - silk-chiffon output",
            "- `duckdb_timeseries.csv` - DuckDB CPU/memory over time",
            "- `silk_timeseries.csv` - silk-chiffon CPU/memory over time",
            "",
        ]
    )

    with open(report_path, "w") as f:
        f.write("\n".join(lines))

    return report_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark Arrow-to-Parquet: DuckDB vs silk-chiffon"
    )
    parser.add_argument(
        "--threads", type=int, default=12, help="Number of threads (default: 12)"
    )
    parser.add_argument(
        "--memory-limit",
        type=int,
        default=None,
        help="Memory limit in MiB for DuckDB (default: 75%% of available RAM)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000_000,
        help="Number of rows to generate (default: 50,000,000)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Use existing Arrow file instead of generating test data",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmark_output"),
        help="Output directory (default: benchmark_output)",
    )
    parser.add_argument(
        "--skip-build", action="store_true", help="Skip building silk-chiffon"
    )
    parser.add_argument(
        "--release",
        action="store_true",
        help="Use release build instead of native (target/release vs target/native)",
    )
    parser.add_argument(
        "--cargo-run",
        action="store_true",
        help="Use 'cargo run --' instead of native binary (for quick testing)",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip generating test data (reuse existing)",
    )
    parser.add_argument(
        "--silk-only",
        action="store_true",
        help="Run only silk-chiffon benchmark (skip DuckDB)",
    )
    parser.add_argument(
        "--duckdb-only",
        action="store_true",
        help="Run only DuckDB benchmark (skip silk-chiffon)",
    )

    args = parser.parse_args()

    # auto-detect memory limit if not specified (75% of available RAM)
    if args.memory_limit is None:
        available_mib = psutil.virtual_memory().available // (1024 * 1024)
        args.memory_limit = int(available_mib * 0.75)

    project_root = Path(__file__).parent.parent
    output_dir = args.output_dir.absolute()

    # duckdb needed for benchmark or for generating test data
    needs_duckdb = (
        not args.silk_only
        or not args.input
        and not (args.skip_generate and (output_dir / "test_data.arrow").exists())
    )
    if needs_duckdb:
        check_duckdb_cli()

    needs_silk = not args.duckdb_only
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Benchmark: DuckDB vs silk-chiffon (Arrow → Parquet)")
    print("=" * 60)
    print(f"Threads:      {args.threads}")
    if not args.silk_only:
        print(f"Memory limit: {args.memory_limit} MiB (DuckDB only)")
    if args.input:
        print(f"Input file:   {args.input}")
    else:
        print(f"Rows:         {args.rows:,}")
    print(f"Output dir:   {output_dir}")
    print("=" * 60)

    silk_cmd = None
    if needs_silk:
        if args.cargo_run:
            silk_cmd = ["cargo", "run", "--"]
            print("\n[1/5] Using cargo run for silk-chiffon")
        elif args.skip_build:
            profile = "release" if args.release else "native"
            silk_binary = project_root / "target" / profile / "silk-chiffon"
            if not silk_binary.exists():
                print(f"Binary not found at {silk_binary}, building...")
                silk_binary = build_release(project_root) if args.release else build_native(project_root)
            else:
                print(f"\n[1/5] Using existing binary: {silk_binary}")
            silk_cmd = [str(silk_binary)]
        else:
            silk_binary = build_release(project_root) if args.release else build_native(project_root)
            silk_cmd = [str(silk_binary)]
    else:
        print("\n[1/5] Skipping silk-chiffon build (--duckdb-only)")

    if args.input:
        arrow_file = args.input.absolute()
        if not arrow_file.exists():
            print(f"Error: Input file not found: {arrow_file}")
            return 1
        print(f"\n[2/5] Using input file: {arrow_file}")
        print(f"  File size: {arrow_file.stat().st_size / (1024**3):.2f} GiB")
    elif args.skip_generate:
        arrow_file = output_dir / "test_data.arrow"
        if arrow_file.exists():
            print(f"\n[2/5] Using existing test data: {arrow_file}")
            print(f"  File size: {arrow_file.stat().st_size / (1024**3):.2f} GiB")
        else:
            arrow_file = generate_test_data(output_dir, args.rows, args.threads)
    else:
        arrow_file = generate_test_data(output_dir, args.rows, args.threads)

    silk_output = output_dir / "silk_output.parquet"
    duckdb_output = output_dir / "duckdb_output.parquet"

    silk_result = None
    if needs_silk and silk_cmd:
        silk_result = run_silk_conversion(silk_cmd, arrow_file, silk_output)
        write_timeseries_csv(silk_result.samples, output_dir / "silk_timeseries.csv")

    duckdb_result = None
    if not args.silk_only:
        duckdb_result = run_duckdb_conversion(
            arrow_file, duckdb_output, args.threads, args.memory_limit
        )
        write_timeseries_csv(duckdb_result.samples, output_dir / "duckdb_timeseries.csv")

    print("\n" + "=" * 60)
    if args.silk_only:
        print("BENCHMARK COMPLETE (silk-only)")
    elif args.duckdb_only:
        print("BENCHMARK COMPLETE (duckdb-only)")
    else:
        print("BENCHMARK COMPLETE")
    print("=" * 60)
    print("\nResults:")

    if duckdb_result:
        print(
            f"  DuckDB:       {duckdb_result.wall_time:.2f}s, {duckdb_result.peak_rss_mb:.0f} MB peak"
        )
    if silk_result:
        print(
            f"  silk-chiffon: {silk_result.wall_time:.2f}s, {silk_result.peak_rss_mb:.0f} MB peak"
        )

    if silk_result and duckdb_result and silk_cmd:
        speedup = duckdb_result.wall_time / silk_result.wall_time
        if speedup >= 1:
            print(f"\n  silk-chiffon is {speedup:.2f}x faster")
        else:
            print(f"\n  DuckDB is {1 / speedup:.2f}x faster")

        assert silk_cmd is not None
        comparison = compare_outputs(silk_cmd, duckdb_output, silk_output)
        config = {
            "rows": args.rows,
            "threads": args.threads,
            "memory_limit_mib": args.memory_limit,
            "input_file": str(arrow_file) if args.input else None,
        }
        report_path = generate_report(
            output_dir, duckdb_result, silk_result, comparison, config
        )
        print(f"\nReport: {report_path}")

    print(f"Output: {output_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
