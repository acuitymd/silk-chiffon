#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "psutil",
#     "google-cloud-bigquery",
#     "google-cloud-bigquery-storage",
#     "pyarrow",
#     "matplotlib",
#     "rich",
# ]
# ///
"""
Benchmark partitioned splitting: silk-chiffon (FairSpill vs Reserved pool) vs DuckDB vs DataFusion.

Downloads data slices from BigQuery, runs up to 3 splitting scenarios per input file,
collects resource usage time-series, and generates comparison reports + graphs.

Usage:
    uv run scripts/benchmark_memory_pool.py \\
        --bq-table project.dataset.table \\
        --filters "region = 'us'" "region = 'eu'" \\
        --multi-split region,category,year \\
        --single-split category \\
        --two-pass year,category

    uv run scripts/benchmark_memory_pool.py \\
        --skip-download --skip-build \\
        --multi-split region,category,year \\
        --single-split category
"""

import argparse
import csv
import gzip
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery
import matplotlib.pyplot as plt
import psutil
import pyarrow as pa
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


@dataclass
class BenchmarkResult:
    tool: str
    pool: str
    scenario: str
    input_label: str
    wall_time: float
    peak_rss_mb: float
    avg_cpu_percent: float
    peak_cpu_percent: float
    exit_code: int
    error: str | None = None
    samples: list[dict] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.exit_code != 0

    @property
    def time_display(self) -> str:
        if self.exit_code == 0:
            return f"{self.wall_time:.2f}"
        if self.exit_code == 137 or (self.error and "OOM" in self.error.upper()):
            return "OOM"
        return f"FAILED (exit {self.exit_code})"


def monitor_process(
    cmd: list[str], cwd: str | Path | None = None, interval: float = 0.05
) -> dict:
    samples: list[dict] = []
    start = time.perf_counter()

    proc = subprocess.Popen(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    ps = psutil.Process(proc.pid)

    try:
        ps.cpu_percent()
    except psutil.NoSuchProcess:
        pass

    while proc.poll() is None:
        try:
            mem = ps.memory_info()
            cpu = ps.cpu_percent()
            try:
                children = ps.children(recursive=True)
                for child in children:
                    try:
                        cmem = child.memory_info()
                        mem = type(mem)(
                            mem.rss + cmem.rss,
                            mem.vms + cmem.vms,
                            *mem[2:],
                        )
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
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
    stdout, stderr = proc.communicate()

    peak_rss = max((s["rss_mb"] for s in samples), default=0)
    avg_cpu = (
        sum(s["cpu_percent"] for s in samples) / len(samples) if samples else 0
    )
    peak_cpu = max((s["cpu_percent"] for s in samples), default=0)

    combined_output = stderr.decode(errors="replace")
    if proc.returncode and proc.returncode != 0:
        stdout_str = stdout.decode(errors="replace").strip()
        if stdout_str:
            combined_output = stdout_str + "\n" + combined_output

    return {
        "exit_code": proc.returncode or 0,
        "wall_time": wall_time,
        "peak_rss_mb": peak_rss,
        "avg_cpu_percent": avg_cpu,
        "peak_cpu_percent": peak_cpu,
        "samples": samples,
        "stderr": combined_output,
        "cmd": cmd,
    }


def _offset_samples(samples: list[dict], offset: float) -> list[dict]:
    return [{**s, "elapsed_s": s["elapsed_s"] + offset} for s in samples]


def _merge_two_pass(r1: dict, r2: dict, all_samples: list[dict], total_wall: float,
                    max_peak_rss: float, cpu_totals: list[float],
                    peak_cpus: list[float]) -> dict:
    if r2["exit_code"] != 0:
        return {
            "exit_code": r2["exit_code"],
            "wall_time": total_wall + r2["wall_time"],
            "peak_rss_mb": max(max_peak_rss, r2["peak_rss_mb"]),
            "avg_cpu_percent": r1["avg_cpu_percent"],
            "peak_cpu_percent": max(*peak_cpus, r2["peak_cpu_percent"]),
            "samples": all_samples + _offset_samples(r2["samples"], total_wall),
            "stderr": r2["stderr"],
        }
    return {}


def _sanitize_label(filter_expr: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", filter_expr).strip("_").lower()


def download_bq_data(
    table: str, output_dir: Path, filters: list[str]
) -> dict[str, Path]:
    project = table.split(".")[0]
    client = bigquery.Client(project=project)
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    files = {}

    for filt in filters:
        label = _sanitize_label(filt)
        arrow_file = data_dir / f"{label}.arrow"
        if arrow_file.exists():
            size_mb = arrow_file.stat().st_size / 1024 / 1024
            console.print(f"  {label}: already exists ({size_mb:.0f} MB), skipping")
            files[label] = arrow_file
            continue

        with console.status(f"  {label}: querying BigQuery (WHERE {filt})..."):
            query = f"SELECT * FROM `{table}` WHERE {filt}"
            start = time.perf_counter()
            df = client.query(query).to_arrow()
            elapsed = time.perf_counter() - start

        console.print(f"  {label}: fetched {df.num_rows:,} rows in {elapsed:.1f}s")

        with console.status(f"  {label}: writing {arrow_file}..."):
            with pa.ipc.new_file(str(arrow_file), df.schema) as writer:
                writer.write_table(df)

        size_mb = arrow_file.stat().st_size / 1024 / 1024
        console.print(f"  {label}: [green]{size_mb:.0f} MB[/green]")
        files[label] = arrow_file

    return files


def build_silk(project_root: Path) -> Path:
    with console.status("Building silk-chiffon (just build-native)..."):
        result = subprocess.run(
            ["just", "build-native"],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
    if result.returncode != 0:
        console.print(f"[red]Build failed:[/red] {result.stderr}")
        sys.exit(1)

    binary = project_root / "target" / "native" / "silk-chiffon"
    if not binary.exists():
        console.print(f"[red]Binary not found at {binary}[/red]")
        sys.exit(1)
    console.print(f"  binary: {binary}")
    return binary


def find_silk_binary(project_root: Path) -> Path:
    for profile in ["native", "release"]:
        binary = project_root / "target" / profile / "silk-chiffon"
        if binary.exists():
            return binary
    console.print("[red]No silk-chiffon binary found. Run without --skip-build.[/red]")
    sys.exit(1)


def check_tool(name: str) -> bool:
    return shutil.which(name) is not None


DUCKDB_VERSION = "1.4.4"


def ensure_duckdb(output_dir: Path) -> Path:
    binary = output_dir / "duckdb"
    if binary.exists():
        console.print(f"  duckdb {DUCKDB_VERSION} already downloaded")
        return binary

    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "darwin":
        slug = "duckdb_cli-osx-universal.gz"
    elif system == "linux" and machine in ("x86_64", "amd64"):
        slug = "duckdb_cli-linux-amd64.gz"
    elif system == "linux" and machine in ("aarch64", "arm64"):
        slug = "duckdb_cli-linux-aarch64.gz"
    else:
        console.print(f"[red]Unsupported platform: {system}/{machine}[/red]")
        sys.exit(1)

    url = f"https://github.com/duckdb/duckdb/releases/download/v{DUCKDB_VERSION}/{slug}"
    gz_path = output_dir / slug

    with console.status(f"Downloading duckdb {DUCKDB_VERSION}..."):
        urllib.request.urlretrieve(url, gz_path)
        with gzip.open(gz_path, "rb") as f_in, open(binary, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        gz_path.unlink()
        binary.chmod(0o755)

    console.print(f"  duckdb {DUCKDB_VERSION}: {binary}")
    return binary


def _silk_template(cols: list[str], ext: str = "parquet") -> str:
    if len(cols) == 1:
        return "{{" + cols[0] + "}}." + ext
    parts = "/".join("{{" + c + "}}" for c in cols[:-1])
    return parts + "/{{" + cols[-1] + "}}." + ext


def run_silk_split(
    silk_bin: Path,
    input_path: Path,
    output_dir: Path,
    by_cols: list[str],
    reserve_spec: str | None = None,
    output_ext: str = "parquet",
    partition_strategy: str | None = None,
) -> dict:
    run_dir = output_dir / "silk_output"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)

    template = _silk_template(by_cols, output_ext)
    cmd = [
        str(silk_bin),
        "transform",
        "--from", str(input_path),
        "--to-many", str(run_dir / template),
        "--by", ",".join(by_cols),
        "--overwrite",
    ]
    if reserve_spec:
        cmd.extend(["--non-spillable-reserve", reserve_spec])
    if partition_strategy:
        cmd.extend(["--partition-strategy", partition_strategy])

    return monitor_process(cmd)


def run_two_pass_silk(
    silk_bin: Path,
    input_path: Path,
    output_dir: Path,
    pass1_col: str,
    pass2_col: str,
    reserve_spec: str | None = None,
) -> dict:
    pass1_dir = output_dir / "silk_pass1"
    pass2_dir = output_dir / "silk_pass2"
    for d in [pass1_dir, pass2_dir]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    console.print(f"\n      pass 1: split by {pass1_col}")
    r1 = run_silk_split(silk_bin, input_path, pass1_dir, [pass1_col],
                        reserve_spec, output_ext="arrow",
                        partition_strategy="nosort-multi")
    if r1["exit_code"] != 0:
        r1["stderr"] = f"pass 1 (split by {pass1_col}) failed:\n{r1['stderr']}"
        return r1

    pass1_files = sorted(pass1_dir.rglob("*.arrow"))
    console.print(f"      pass 1 done: {len(pass1_files)} files")
    total_wall = r1["wall_time"]
    max_peak_rss = r1["peak_rss_mb"]
    cpu_totals = [r1["avg_cpu_percent"] * r1["wall_time"]]
    peak_cpus = [r1["peak_cpu_percent"]]
    all_samples = list(r1["samples"])

    for idx, f in enumerate(pass1_files):
        size_mb = f.stat().st_size / 1024 / 1024
        console.print(f"      pass 2 [{idx+1}/{len(pass1_files)}]: {f.name} ({size_mb:.0f} MB)")
        fdir = pass2_dir / f.stem
        fdir.mkdir(parents=True, exist_ok=True)

        r2 = run_silk_split(silk_bin, f, fdir, [pass2_col], reserve_spec)
        err = _merge_two_pass(r1, r2, all_samples, total_wall, max_peak_rss,
                              cpu_totals, peak_cpus)
        if err:
            err["stderr"] = f"pass 2 ({f.name}) failed:\n{err['stderr']}"
            return err

        total_wall += r2["wall_time"]
        max_peak_rss = max(max_peak_rss, r2["peak_rss_mb"])
        cpu_totals.append(r2["avg_cpu_percent"] * r2["wall_time"])
        peak_cpus.append(r2["peak_cpu_percent"])
        all_samples.extend(_offset_samples(r2["samples"], total_wall - r2["wall_time"]))

    weighted_avg_cpu = sum(cpu_totals) / total_wall if total_wall > 0 else 0
    return {
        "exit_code": 0,
        "wall_time": total_wall,
        "peak_rss_mb": max_peak_rss,
        "avg_cpu_percent": weighted_avg_cpu,
        "peak_cpu_percent": max(peak_cpus),
        "samples": all_samples,
        "stderr": "",
    }


DUCKDB_MEMORY_PCT = 60
DUCKDB_THREADS: int | None = None
DUCKDB_NO_PRESERVE_ORDER = False


def _duckdb_settings() -> str:
    total_bytes = psutil.virtual_memory().total
    limit_gb = total_bytes * DUCKDB_MEMORY_PCT / 100 / 1_000_000_000
    lines = [f"SET memory_limit='{limit_gb:.1f}GB';"]
    if DUCKDB_THREADS is not None:
        lines.append(f"SET threads={DUCKDB_THREADS};")
    if DUCKDB_NO_PRESERVE_ORDER:
        lines.append("SET preserve_insertion_order=false;")
    return "\n".join(lines)


def run_duckdb_split(
    duckdb_bin: Path, input_path: Path, output_dir: Path, partition_by_cols: list[str]
) -> dict:
    run_dir = output_dir / "duckdb_output"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)

    cols = ", ".join(partition_by_cols)
    sql = f"""{_duckdb_settings()}
INSTALL nanoarrow FROM community;
LOAD nanoarrow;
COPY (SELECT * FROM read_arrow('{input_path}'))
TO '{run_dir}' (FORMAT PARQUET, PARTITION_BY ({cols}));
"""
    sql_file = output_dir / "duckdb_split.sql"
    sql_file.write_text(sql)

    cmd = [str(duckdb_bin), "-c", f".read {sql_file}"]
    result = monitor_process(cmd)
    sql_file.unlink(missing_ok=True)
    return result


def run_two_pass_duckdb(
    duckdb_bin: Path, input_path: Path, output_dir: Path, pass1_col: str, pass2_col: str
) -> dict:
    pass1_dir = output_dir / "duckdb_pass1"
    pass2_dir = output_dir / "duckdb_pass2"
    for d in [pass1_dir, pass2_dir]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    sql_file = output_dir / "duckdb_2pass.sql"
    console.print(f"\n      pass 1: split by {pass1_col}")
    sql_file.write_text(f"""{_duckdb_settings()}
INSTALL nanoarrow FROM community;
LOAD nanoarrow;
COPY (SELECT * FROM read_arrow('{input_path}'))
TO '{pass1_dir}' (FORMAT ARROW, PARTITION_BY ({pass1_col}));
""")
    r1 = monitor_process([str(duckdb_bin), "-c", f".read {sql_file}"])
    sql_file.unlink(missing_ok=True)

    if r1["exit_code"] != 0:
        r1["stderr"] = f"pass 1 (split by {pass1_col}) failed:\n{r1['stderr']}"
        return r1

    pass1_files = list(pass1_dir.rglob("*.arrow"))
    if not pass1_files:
        return {**r1, "exit_code": 1, "stderr": "No arrow files from pass 1"}
    console.print(f"      pass 1 done: {len(pass1_files)} files")

    total_wall = r1["wall_time"]
    max_peak_rss = r1["peak_rss_mb"]
    cpu_totals = [r1["avg_cpu_percent"] * r1["wall_time"]]
    peak_cpus = [r1["peak_cpu_percent"]]
    all_samples = list(r1["samples"])

    for idx, pf in enumerate(pass1_files):
        pdir = pass2_dir / pf.parent.name
        pdir.mkdir(parents=True, exist_ok=True)
        size_mb = pf.stat().st_size / 1024 / 1024
        console.print(f"      pass 2 [{idx+1}/{len(pass1_files)}]: {pf.parent.name}/{pf.name} ({size_mb:.0f} MB)")

        sql_file.write_text(f"""{_duckdb_settings()}
INSTALL nanoarrow FROM community;
LOAD nanoarrow;
COPY (SELECT * FROM read_arrow('{pf}'))
TO '{pdir}' (FORMAT PARQUET, PARTITION_BY ({pass2_col}));
""")
        r2 = monitor_process([str(duckdb_bin), "-c", f".read {sql_file}"])
        sql_file.unlink(missing_ok=True)

        err = _merge_two_pass(r1, r2, all_samples, total_wall, max_peak_rss,
                              cpu_totals, peak_cpus)
        if err:
            err["stderr"] = f"pass 2 ({pf.parent.name}/{pf.name}) failed:\n{err['stderr']}"
            return err

        total_wall += r2["wall_time"]
        max_peak_rss = max(max_peak_rss, r2["peak_rss_mb"])
        cpu_totals.append(r2["avg_cpu_percent"] * r2["wall_time"])
        peak_cpus.append(r2["peak_cpu_percent"])
        all_samples.extend(_offset_samples(r2["samples"], total_wall - r2["wall_time"]))

    weighted_avg_cpu = sum(cpu_totals) / total_wall if total_wall > 0 else 0
    return {
        "exit_code": 0,
        "wall_time": total_wall,
        "peak_rss_mb": max_peak_rss,
        "avg_cpu_percent": weighted_avg_cpu,
        "peak_cpu_percent": max(peak_cpus),
        "samples": all_samples,
        "stderr": "",
    }


def run_datafusion_split(
    input_path: Path, output_dir: Path, partition_by_cols: list[str]
) -> dict:
    run_dir = output_dir / "datafusion_output"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)

    cols = ", ".join(partition_by_cols)
    sql = f"""
CREATE EXTERNAL TABLE t STORED AS ARROW LOCATION '{input_path}';
COPY (SELECT * FROM t) TO '{run_dir}' STORED AS PARQUET PARTITIONED BY ({cols});
"""
    sql_file = output_dir / "datafusion_split.sql"
    sql_file.write_text(sql)

    cmd = ["datafusion-cli", "--file", str(sql_file)]
    result = monitor_process(cmd)
    sql_file.unlink(missing_ok=True)
    return result


def run_two_pass_datafusion(
    input_path: Path, output_dir: Path, pass1_col: str, pass2_col: str
) -> dict:
    pass1_dir = output_dir / "datafusion_pass1"
    pass2_dir = output_dir / "datafusion_pass2"
    for d in [pass1_dir, pass2_dir]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    sql_file = output_dir / "datafusion_2pass.sql"
    console.print(f"\n      pass 1: split by {pass1_col}")
    sql_file.write_text(f"""
CREATE EXTERNAL TABLE t STORED AS ARROW LOCATION '{input_path}';
COPY (SELECT * FROM t) TO '{pass1_dir}' STORED AS PARQUET PARTITIONED BY ({pass1_col});
""")
    r1 = monitor_process(["datafusion-cli", "--file", str(sql_file)])
    sql_file.unlink(missing_ok=True)

    if r1["exit_code"] != 0:
        r1["stderr"] = f"pass 1 (split by {pass1_col}) failed:\n{r1['stderr']}"
        return r1

    pass1_files = list(pass1_dir.rglob("*.parquet"))
    if not pass1_files:
        return {**r1, "exit_code": 1, "stderr": "No parquet files from pass 1"}
    console.print(f"      pass 1 done: {len(pass1_files)} files")

    total_wall = r1["wall_time"]
    max_peak_rss = r1["peak_rss_mb"]
    cpu_totals = [r1["avg_cpu_percent"] * r1["wall_time"]]
    peak_cpus = [r1["peak_cpu_percent"]]
    all_samples = list(r1["samples"])

    for idx, pf in enumerate(pass1_files):
        pdir = pass2_dir / pf.parent.name
        pdir.mkdir(parents=True, exist_ok=True)
        size_mb = pf.stat().st_size / 1024 / 1024
        console.print(f"      pass 2 [{idx+1}/{len(pass1_files)}]: {pf.parent.name}/{pf.name} ({size_mb:.0f} MB)")

        sql_file.write_text(f"""
CREATE EXTERNAL TABLE t_{pf.stem} STORED AS PARQUET LOCATION '{pf}';
COPY (SELECT * FROM t_{pf.stem}) TO '{pdir}' STORED AS PARQUET PARTITIONED BY ({pass2_col});
""")
        r2 = monitor_process(["datafusion-cli", "--file", str(sql_file)])
        sql_file.unlink(missing_ok=True)

        err = _merge_two_pass(r1, r2, all_samples, total_wall, max_peak_rss,
                              cpu_totals, peak_cpus)
        if err:
            err["stderr"] = f"pass 2 ({pf.parent.name}/{pf.name}) failed:\n{err['stderr']}"
            return err

        total_wall += r2["wall_time"]
        max_peak_rss = max(max_peak_rss, r2["peak_rss_mb"])
        cpu_totals.append(r2["avg_cpu_percent"] * r2["wall_time"])
        peak_cpus.append(r2["peak_cpu_percent"])
        all_samples.extend(_offset_samples(r2["samples"], total_wall - r2["wall_time"]))

    weighted_avg_cpu = sum(cpu_totals) / total_wall if total_wall > 0 else 0
    return {
        "exit_code": 0,
        "wall_time": total_wall,
        "peak_rss_mb": max_peak_rss,
        "avg_cpu_percent": weighted_avg_cpu,
        "peak_cpu_percent": max(peak_cpus),
        "samples": all_samples,
        "stderr": "",
    }


def make_result(
    tool: str, pool: str, scenario: str, input_label: str, metrics: dict
) -> BenchmarkResult:
    error = None
    if metrics["exit_code"] != 0:
        parts = []
        cmd = metrics.get("cmd")
        if cmd:
            parts.append(f"cmd: {' '.join(cmd)}")
        stderr = metrics.get("stderr", "").strip()
        if stderr:
            parts.append(stderr)
        error = "\n".join(parts) if parts else f"exit code {metrics['exit_code']}"

    return BenchmarkResult(
        tool=tool,
        pool=pool,
        scenario=scenario,
        input_label=input_label,
        wall_time=metrics["wall_time"],
        peak_rss_mb=metrics["peak_rss_mb"],
        avg_cpu_percent=metrics["avg_cpu_percent"],
        peak_cpu_percent=metrics["peak_cpu_percent"],
        exit_code=metrics["exit_code"],
        error=error,
        samples=metrics["samples"],
    )


def _run_all_tools(
    scenario: str,
    scenario_label: str,
    input_label: str,
    input_path: Path,
    output_dir: Path,
    silk_bin: Path,
    reserve_spec: str,
    engines: set[str],
    silk_fn=None,
    duckdb_fn=None,
    datafusion_fn=None,
) -> list[BenchmarkResult]:
    results = []

    runs = []
    if "silk" in engines and silk_fn:
        runs.append(("silk-chiffon", "FairSpill", lambda: silk_fn(silk_bin, input_path, output_dir / f"{scenario}_silk_fair", None)))
        runs.append(("silk-chiffon", f"Reserved {reserve_spec}", lambda: silk_fn(silk_bin, input_path, output_dir / f"{scenario}_silk_reserved", reserve_spec)))
    if "duckdb" in engines and duckdb_fn:
        runs.append(("duckdb", "-", lambda: duckdb_fn(input_path, output_dir / f"{scenario}_duckdb")))
    if "datafusion" in engines and datafusion_fn:
        runs.append(("datafusion", "-", lambda: datafusion_fn(input_path, output_dir / f"{scenario}_datafusion")))

    for tool, pool, fn in runs:
        label = f"{tool} ({pool})" if pool != "-" else tool
        sdir = output_dir / f"{scenario}_{tool.replace('-', '')}_{pool.split()[0].lower()}"
        sdir.mkdir(parents=True, exist_ok=True)

        console.print(f"  [bold]{scenario_label}[/bold] {label}...", end=" ")
        m = fn()

        r = make_result(tool, pool, scenario, input_label, m)
        results.append(r)

        if r.failed:
            console.print(f"[red]{r.time_display}[/red] (peak RSS: {r.peak_rss_mb:.0f} MB)")
            if r.error:
                # indent stderr, truncate long output
                stderr_lines = r.error.strip().splitlines()
                shown = stderr_lines[:20]
                for line in shown:
                    console.print(f"    [dim]{line}[/dim]")
                if len(stderr_lines) > 20:
                    console.print(f"    [dim]... ({len(stderr_lines) - 20} more lines)[/dim]")
        else:
            console.print(f"[green]{r.wall_time:.2f}s[/green]  RSS: {r.peak_rss_mb:.0f} MB  CPU: {r.avg_cpu_percent:.0f}%")

    return results


def _results_table(results: list[BenchmarkResult], scenario_name: str) -> Table:
    table = Table(title=scenario_name, show_lines=False)
    table.add_column("Input", style="cyan")
    table.add_column("Tool")
    table.add_column("Pool")
    table.add_column("Time (s)", justify="right")
    table.add_column("Peak RSS (MB)", justify="right")
    table.add_column("Avg CPU%", justify="right")
    table.add_column("Peak CPU%", justify="right")

    for r in results:
        time_style = "red" if r.failed else "green"
        time_str = r.time_display
        rss_str = f"{r.peak_rss_mb:.0f}"
        avg_cpu = f"{r.avg_cpu_percent:.0f}" if not r.failed else "-"
        peak_cpu = f"{r.peak_cpu_percent:.0f}" if not r.failed else "-"
        table.add_row(
            r.input_label, r.tool, r.pool,
            f"[{time_style}]{time_str}[/{time_style}]",
            rss_str, avg_cpu, peak_cpu,
        )

    return table


def write_timeseries(results: list[BenchmarkResult], output_dir: Path) -> None:
    ts_dir = output_dir / "timeseries"
    ts_dir.mkdir(parents=True, exist_ok=True)

    for r in results:
        pool_tag = r.pool.lower().replace(" ", "_").replace("%", "pct")
        tool_tag = r.tool.replace("-", "")
        fname = f"{r.input_label}_{r.scenario}_{tool_tag}_{pool_tag}.csv"
        path = ts_dir / fname

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["elapsed_s", "cpu_percent", "rss_mb"])
            writer.writeheader()
            for s in r.samples:
                writer.writerow(
                    {
                        "elapsed_s": f"{s['elapsed_s']:.3f}",
                        "cpu_percent": f"{s['cpu_percent']:.1f}",
                        "rss_mb": f"{s['rss_mb']:.1f}",
                    }
                )


def generate_report(
    all_results: list[BenchmarkResult], scenario_names: dict[str, str], output_dir: Path
) -> Path:
    report_path = output_dir / "report.md"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# Benchmark Report: Partitioned Splitting",
        "",
        f"**Generated**: {timestamp}",
        "",
        "**Tools**: silk-chiffon (FairSpill, Reserved), DuckDB, DataFusion",
        "",
    ]

    scenarios = sorted(set(r.scenario for r in all_results))
    for scenario_key in scenarios:
        scenario_results = [r for r in all_results if r.scenario == scenario_key]
        if not scenario_results:
            continue

        name = scenario_names.get(scenario_key, scenario_key)
        lines.append(f"## {name}")
        lines.append("")
        lines.append("| Input     | Tool         | Pool         | Time (s) | Peak RSS (MB) | Avg CPU% | Peak CPU% |")
        lines.append("| --------- | ------------ | ------------ | -------- | ------------- | -------- | --------- |")

        labels = sorted(set(r.input_label for r in scenario_results))
        for label in labels:
            label_results = [r for r in scenario_results if r.input_label == label]
            for r in label_results:
                time_col = r.time_display
                rss_col = f"{r.peak_rss_mb:.0f}"
                avg_cpu = f"{r.avg_cpu_percent:.0f}" if not r.failed else "-"
                peak_cpu = f"{r.peak_cpu_percent:.0f}" if not r.failed else "-"
                lines.append(
                    f"| {label:<9} "
                    f"| {r.tool:<12} "
                    f"| {r.pool:<12} "
                    f"| {time_col:<8} "
                    f"| {rss_col:<13} "
                    f"| {avg_cpu:<8} "
                    f"| {peak_cpu:<9} |"
                )

        lines.append("")

    with open(report_path, "w") as f:
        f.write("\n".join(lines))

    return report_path


TOOL_COLORS = {
    ("silk-chiffon", "FairSpill"): "tab:blue",
    ("duckdb", "-"): "tab:orange",
    ("datafusion", "-"): "tab:red",
}

RESERVED_COLOR = "tab:green"


def _color_for(r: BenchmarkResult) -> str:
    key = (r.tool, r.pool)
    if key in TOOL_COLORS:
        return TOOL_COLORS[key]
    if "Reserved" in r.pool:
        return RESERVED_COLOR
    return "tab:gray"


def _label_for(r: BenchmarkResult) -> str:
    if r.pool == "-":
        return r.tool
    return f"{r.tool} ({r.pool})"


def plot_scenario(
    scenario_key: str, scenario_name: str, input_label: str,
    results: list[BenchmarkResult], output_dir: Path
) -> None:
    graphs_dir = output_dir / "graphs"
    graphs_dir.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.set_xlabel("Elapsed (s)")
    ax.set_ylabel("RSS (MB)")
    ax.set_title(f"Memory: {input_label} - {scenario_name}")

    for r in results:
        if not r.samples:
            continue
        xs = [s["elapsed_s"] for s in r.samples]
        ys = [s["rss_mb"] for s in r.samples]
        style = "--" if r.failed else "-"
        ax.plot(xs, ys, color=_color_for(r), linestyle=style, label=_label_for(r), linewidth=1.5)

    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(graphs_dir / f"{input_label}_{scenario_key}_memory.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.set_xlabel("Elapsed (s)")
    ax.set_ylabel("CPU %")
    ax.set_title(f"CPU: {input_label} - {scenario_name}")

    for r in results:
        if not r.samples:
            continue
        xs = [s["elapsed_s"] for s in r.samples]
        ys = [s["cpu_percent"] for s in r.samples]
        style = "--" if r.failed else "-"
        ax.plot(xs, ys, color=_color_for(r), linestyle=style, label=_label_for(r), linewidth=1.5)

    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(graphs_dir / f"{input_label}_{scenario_key}_cpu.png", dpi=150)
    plt.close(fig)


def generate_graphs(
    all_results: list[BenchmarkResult], scenario_names: dict[str, str], output_dir: Path
) -> None:
    labels = sorted(set(r.input_label for r in all_results))
    scenarios = sorted(set(r.scenario for r in all_results))

    for label in labels:
        for scenario in scenarios:
            subset = [
                r for r in all_results
                if r.input_label == label and r.scenario == scenario
            ]
            if subset:
                name = scenario_names.get(scenario, scenario)
                plot_scenario(scenario, name, label, subset, output_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark partitioned splitting: silk-chiffon vs DuckDB vs DataFusion"
    )

    data_group = parser.add_argument_group("data source")
    data_group.add_argument(
        "--bq-table",
        help="Fully-qualified BigQuery table (project.dataset.table)",
    )
    data_group.add_argument(
        "--filters", nargs="+",
        help="SQL WHERE clauses; each produces a separate input file "
             "(e.g., \"region = 'us'\" \"region = 'eu'\")",
    )
    data_group.add_argument(
        "--input-files", nargs="+", type=Path,
        help="Use existing Arrow files instead of downloading from BigQuery",
    )
    data_group.add_argument(
        "--skip-download", action="store_true",
        help="Reuse existing Arrow files in output-dir/data/",
    )

    scenario_group = parser.add_argument_group("scenarios (at least one required)")
    scenario_group.add_argument(
        "--multi-split", type=str,
        help="Comma-separated columns for multi-column split scenario "
             "(e.g., region,category,year)",
    )
    scenario_group.add_argument(
        "--single-split", type=str,
        help="Column for single-column split scenario (e.g., category)",
    )
    scenario_group.add_argument(
        "--two-pass", type=str,
        help="Two comma-separated columns for 2-pass scenario: split by first, "
             "then by second (e.g., year,category)",
    )

    parser.add_argument(
        "--engines", type=str, default="silk,duckdb,datafusion",
        help="Comma-separated list of engines to run (default: silk,duckdb,datafusion)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("benchmark_pool_output"),
        help="Working directory (default: benchmark_pool_output)",
    )
    parser.add_argument(
        "--skip-build", action="store_true",
        help="Use existing silk binary",
    )
    parser.add_argument(
        "--duckdb-memory-pct", type=int, default=60,
        help="DuckDB memory_limit as percent of total RAM (default: 60)",
    )
    parser.add_argument(
        "--duckdb-threads", type=int, default=None,
        help="DuckDB threads (default: duckdb default)",
    )
    parser.add_argument(
        "--duckdb-no-preserve-order", action="store_true",
        help="SET preserve_insertion_order=false in DuckDB",
    )
    parser.add_argument(
        "--reserve-spec", type=str, default="10%%",
        help="--non-spillable-reserve value (default: 10%%%%)",
    )

    args = parser.parse_args()

    if not args.input_files and not args.skip_download:
        if not args.bq_table:
            parser.error("--bq-table is required unless using --input-files or --skip-download")
        if not args.filters:
            parser.error("--filters is required when downloading from BigQuery")

    if not any([args.multi_split, args.single_split, args.two_pass]):
        parser.error("At least one scenario is required: --multi-split, --single-split, or --two-pass")

    if args.two_pass:
        parts = args.two_pass.split(",")
        if len(parts) != 2:
            parser.error("--two-pass requires exactly two comma-separated columns")

    return args


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parent.parent
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    engines = {e.strip() for e in args.engines.split(",")}

    global DUCKDB_MEMORY_PCT, DUCKDB_THREADS, DUCKDB_NO_PRESERVE_ORDER
    DUCKDB_MEMORY_PCT = args.duckdb_memory_pct
    DUCKDB_THREADS = args.duckdb_threads
    DUCKDB_NO_PRESERVE_ORDER = args.duckdb_no_preserve_order

    multi_cols = args.multi_split.split(",") if args.multi_split else None
    single_col = args.single_split
    two_pass_cols = args.two_pass.split(",") if args.two_pass else None

    scenario_names: dict[str, str] = {}
    if multi_cols:
        scenario_names["multi_split"] = f"Multi-column split ({', '.join(multi_cols)})"
    if single_col:
        scenario_names["single_split"] = f"Single-column split ({single_col})"
    if two_pass_cols:
        scenario_names["two_pass"] = f"2-pass split ({two_pass_cols[0]}, then {two_pass_cols[1]})"

    duckdb_bin = ensure_duckdb(output_dir) if "duckdb" in engines else None

    if "datafusion" in engines and not check_tool("datafusion-cli"):
        console.print("[yellow]datafusion-cli not found, disabling datafusion[/yellow]")
        engines.discard("datafusion")

    config_lines = []
    for v in scenario_names.values():
        config_lines.append(v)
    config_lines.append(f"engines: {', '.join(sorted(engines))}")
    config_lines.append(f"reserve: {args.reserve_spec}")
    config_lines.append(f"output:  {output_dir}")
    if duckdb_bin:
        config_lines.append(f"duckdb:  {duckdb_bin}")
    datafusion_status = "[green]available[/green]" if "datafusion" in engines else "[dim]disabled[/dim]"
    config_lines.append(f"datafusion: {datafusion_status}")

    console.print(Panel("\n".join(config_lines), title="Benchmark: Partitioned Splitting"))

    input_files: dict[str, Path] = {}

    if args.input_files:
        for f in args.input_files:
            f = f.resolve()
            if not f.exists():
                console.print(f"[red]ERROR: {f} not found[/red]")
                return 1
            label = f.stem
            size_mb = f.stat().st_size / 1024 / 1024
            console.print(f"  {label}: {size_mb:.0f} MB")
            input_files[label] = f
    elif args.skip_download:
        data_dir = output_dir / "data"
        arrow_files = sorted(data_dir.glob("*.arrow"))
        if not arrow_files:
            console.print(f"[red]ERROR: no .arrow files found in {data_dir}[/red]")
            return 1
        for f in arrow_files:
            label = f.stem
            size_mb = f.stat().st_size / 1024 / 1024
            console.print(f"  {label}: {size_mb:.0f} MB")
            input_files[label] = f
    else:
        console.print(f"\nDownloading from [bold]{args.bq_table}[/bold]...")
        input_files = download_bq_data(args.bq_table, output_dir, args.filters)

    if not input_files:
        console.print("[red]ERROR: no input files[/red]")
        return 1

    if "silk" in engines:
        if args.skip_build:
            silk_bin = find_silk_binary(project_root)
            console.print(f"Using binary: {silk_bin}")
        else:
            silk_bin = build_silk(project_root)
    else:
        silk_bin = None

    all_results: list[BenchmarkResult] = []

    for label, input_path in input_files.items():
        label_dir = output_dir / label
        label_dir.mkdir(parents=True, exist_ok=True)

        size_mb = input_path.stat().st_size / 1024 / 1024
        console.rule(f"[bold]{label}[/bold] ({size_mb:.0f} MB)")

        if multi_cols:
            results = _run_all_tools(
                scenario="multi_split",
                scenario_label=f"Multi-split ({','.join(multi_cols)})",
                input_label=label,
                input_path=input_path,
                output_dir=label_dir,
                silk_bin=silk_bin,
                reserve_spec=args.reserve_spec,

                engines=engines,
                silk_fn=lambda b, i, o, r: run_silk_split(b, i, o, multi_cols, r),
                duckdb_fn=lambda i, o: run_duckdb_split(duckdb_bin, i, o, multi_cols),
                datafusion_fn=lambda i, o: run_datafusion_split(i, o, multi_cols),
            )
            all_results.extend(results)

        if single_col:
            results = _run_all_tools(
                scenario="single_split",
                scenario_label=f"Single-split ({single_col})",
                input_label=label,
                input_path=input_path,
                output_dir=label_dir,
                silk_bin=silk_bin,
                reserve_spec=args.reserve_spec,

                engines=engines,
                silk_fn=lambda b, i, o, r: run_silk_split(b, i, o, [single_col], r),
                duckdb_fn=lambda i, o: run_duckdb_split(duckdb_bin, i, o, [single_col]),
                datafusion_fn=lambda i, o: run_datafusion_split(i, o, [single_col]),
            )
            all_results.extend(results)

        if two_pass_cols:
            p1, p2 = two_pass_cols
            results = _run_all_tools(
                scenario="two_pass",
                scenario_label=f"2-pass ({p1}, then {p2})",
                input_label=label,
                input_path=input_path,
                output_dir=label_dir,
                silk_bin=silk_bin,
                reserve_spec=args.reserve_spec,

                engines=engines,
                silk_fn=lambda b, i, o, r: run_two_pass_silk(b, i, o, p1, p2, r),
                duckdb_fn=lambda i, o: run_two_pass_duckdb(duckdb_bin, i, o, p1, p2),
                datafusion_fn=lambda i, o: run_two_pass_datafusion(i, o, p1, p2),
            )
            all_results.extend(results)

    with console.status("Generating outputs..."):
        write_timeseries(all_results, output_dir)
        report_path = generate_report(all_results, scenario_names, output_dir)
        generate_graphs(all_results, scenario_names, output_dir)

    console.print()
    for scenario_key in sorted(set(r.scenario for r in all_results)):
        scenario_results = [r for r in all_results if r.scenario == scenario_key]
        name = scenario_names.get(scenario_key, scenario_key)
        console.print(_results_table(scenario_results, name))
        console.print()

    succeeded = sum(1 for r in all_results if not r.failed)
    failed = sum(1 for r in all_results if r.failed)
    console.print(f"Report:     {report_path}")
    console.print(f"Graphs:     {output_dir / 'graphs'}/")
    console.print(f"Timeseries: {output_dir / 'timeseries'}/")
    console.print(f"Total runs: {len(all_results)} ({succeeded} succeeded, {failed} failed)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
