#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.14"
# dependencies = ["duckdb==1.4.4"]
# ///
"""Benchmark a partitioned, sorted export, stage by stage, across silk-chiffon builds.

The workload is one `silk-chiffon transform` that splits its input into one file per
`event_year,category_id`, sorts each file by five more columns, and writes LZ4 Parquet. All
seven columns are integers or dates, cast by a `--query` on the way in.

Every stage runs the export's own flags and casts, so the stages differ only in what they
leave out:

    read       input -> one Arrow IPC file. Reading, decoding, and the casts.
    partition  input -> one Parquet file per event_year/category_id, unsorted.
    sort       input -> one sorted Parquet file, not partitioned.
    export     input -> partitioned and sorted.

`generate` writes synthetic input: a skewed set of categories over six years, with two
nullable columns, grouped by category and date. `run` times the stages for each build.

Every option that takes a list runs each combination. `--memory-budget 60GiB,4GiB,256MiB`
checks that the export still finishes as the budget shrinks: the report shows which runs
passed and each run's peak RSS against its budget. `--sort-engine parallel,datafusion` times
both sort implementations of a build that has the flag.

Only `generate` and the output digests need DuckDB, so with --no-validate the script runs on
any Python 3.12 or later without uv.

Usage:
    uv run scripts/bench_export.py generate --rows 36000000 --out /scratch/input
    uv run scripts/bench_export.py run --input /scratch/input --work /scratch/work \\
        --bin old=./silk-chiffon-main --bin new=./silk-chiffon-branch
    uv run scripts/bench_export.py run --input /scratch/input --work /scratch/work \\
        --stages export --bin old=./silk-chiffon-main --bin new=./silk-chiffon-branch \\
        --partitions 10 --reserve none,50% --memory-budget 60GiB,4GiB,256MiB
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from itertools import product
from pathlib import Path

STAGES = ("read", "partition", "sort", "export")
# set from --transform-args
EXTRA_TRANSFORM_ARGS: list[str] = []
PARTITION_BY = "event_year,category_id"
SORT_BY = "entity_id,event_date,site_id,account_id,region_id"
# narrows the input's 64-bit integers, and drops its eighth column
CAST_QUERY = (
    "SELECT CAST(entity_id AS INT) AS entity_id, CAST(event_date AS DATE) AS event_date,"
    " CAST(event_year AS SMALLINT) AS event_year, CAST(site_id AS INT) AS site_id,"
    " CAST(account_id AS INT) AS account_id, CAST(category_id AS INT) AS category_id,"
    " CAST(region_id AS SMALLINT) AS region_id FROM data"
)
COLUMNS = (
    "entity_id, event_date, event_year, site_id, account_id, category_id, region_id"
)
OUTPUT_TEMPLATE = (
    "event_year={{ event_year }}/category_id={{ category_id }}/data.parquet"
)
# categories are spaced this far apart, starting at --first-category
CATEGORY_STRIDE = 3600


@dataclass(frozen=True)
class Build:
    name: str
    path: Path
    # older builds take --from-many where newer ones take --from-pattern
    has_from_pattern: bool
    has_sort_engine: bool

    @classmethod
    def probe(cls, spec: str) -> Build:
        name, sep, path = spec.partition("=")
        if not sep or not name or not path:
            raise SystemExit(f"--bin takes NAME=PATH, got {spec!r}")
        binary = Path(path).expanduser().resolve()
        help_text = subprocess.run(
            [binary, "transform", "--help"], capture_output=True, text=True, check=True
        ).stdout
        return cls(
            name=name,
            path=binary,
            has_from_pattern="--from-pattern" in help_text,
            has_sort_engine="--sort-engine" in help_text,
        )

    def version(self) -> str:
        out = subprocess.run(
            [self.path, "--version"], capture_output=True, text=True, check=True
        )
        return out.stdout.strip()

    def input_args(self, source: Source) -> list[str]:
        pattern = str(source.local / "*.parquet")
        return ["--from-pattern" if self.has_from_pattern else "--from-many", pattern]


@dataclass(frozen=True)
class Source:
    label: str
    local: Path


@dataclass(frozen=True)
class Settings:
    partitions: int
    reserve: str | None
    memory_budget: str
    # --sort-engine value; None leaves the build's default
    sort_engine: str | None = None

    def label(self) -> str:
        label = f"p{self.partitions}/reserve={self.reserve or 'none'}/mem={self.memory_budget}"
        if self.sort_engine is not None:
            label += f"/sort={self.sort_engine}"
        return label


@dataclass
class Measurement:
    build: str
    stage: str
    source: str
    settings: str
    repeat: int
    wall_s: float
    cpu_s: float
    peak_rss_bytes: int
    peak_spill_bytes: int
    exit_code: int
    output_bytes: int = 0
    rows: int | None = None
    files: int | None = None
    digest: int | None = None
    command: list[str] = field(default_factory=list)


def transform_command(
    build: Build, stage: str, source: Source, settings: Settings, out: Path, spill: Path
) -> list[str]:
    cmd = [str(build.path), "transform", *build.input_args(source)]
    match stage:
        case "read":
            cmd += ["--to", str(out / "data.arrow")]
        case "partition":
            cmd += ["--by", PARTITION_BY, "--to-many", str(out / OUTPUT_TEMPLATE)]
        case "sort":
            cmd += ["--sort-by", SORT_BY, "--to", str(out / "data.parquet")]
        case "export":
            cmd += [
                "--by",
                PARTITION_BY,
                "--sort-by",
                SORT_BY,
                "--to-many",
                str(out / OUTPUT_TEMPLATE),
            ]
        case _:
            raise ValueError(stage)
    if stage != "read":
        cmd += ["--parquet-compression", "lz4"]
    cmd += [
        "--query",
        CAST_QUERY,
        "--target-partitions",
        str(settings.partitions),
        "--memory-budget",
        settings.memory_budget,
        "--spill-path",
        str(spill),
    ]
    if settings.reserve is not None:
        cmd += ["--non-spillable-reserve", settings.reserve]
    if settings.sort_engine is not None:
        cmd += ["--sort-engine", settings.sort_engine]
    cmd += EXTRA_TRANSFORM_ARGS
    return cmd


def directory_bytes(path: Path) -> int:
    total = 0
    stack = [path]
    while stack:
        try:
            entries = list(os.scandir(stack.pop()))
        except FileNotFoundError:
            continue
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                else:
                    total += entry.stat(follow_symlinks=False).st_size
            except FileNotFoundError:
                pass
    return total


def run_measured(
    cmd: list[str], log: Path, spill: Path
) -> tuple[int, float, float, int, int]:
    """Run `cmd` alone; return its exit code, wall and CPU seconds, peak RSS, peak spill."""
    peak_spill = 0
    done = threading.Event()

    def watch_spill() -> None:
        nonlocal peak_spill
        while not done.wait(0.25):
            peak_spill = max(peak_spill, directory_bytes(spill))

    watcher = threading.Thread(target=watch_spill, daemon=True)
    with open(log, "w") as log_file:
        start = time.perf_counter()
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        watcher.start()
        _, status, usage = os.wait4(proc.pid, 0)
        wall = time.perf_counter() - start
    done.set()
    watcher.join()
    proc.returncode = os.waitstatus_to_exitcode(status)
    # ru_maxrss is bytes on macOS and KiB on Linux
    rss = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
    return proc.returncode, wall, usage.ru_utime + usage.ru_stime, rss, peak_spill


def summarize_output(out: Path) -> tuple[int, int, int]:
    """Rows, files, and an order-independent digest of a stage's Parquet output."""
    import duckdb

    files = sorted(out.rglob("*.parquet"))
    if not files:
        return 0, 0, 0
    rows, digest = (
        duckdb.connect()
        .execute(
            f"SELECT count(*), coalesce(sum(hash({COLUMNS}) >> 8), 0)"
            f" FROM read_parquet('{out}/**/*.parquet', hive_partitioning = false)"
        )
        .fetchone()
    )
    return rows, len(files), int(digest)


def run_matrix(
    builds: list[Build],
    sources: list[Source],
    stages: list[str],
    settings_list: list[Settings],
    repeats: int,
    work: Path,
    validate: bool,
    results: list[Measurement],
    results_path: Path,
) -> None:
    cases = [
        (source, stage, settings, build)
        for source, stage, settings in product(sources, stages, settings_list)
        for build in builds
        if settings.sort_engine is None or build.has_sort_engine
    ]
    total = len(cases) * repeats
    done = 0
    for repeat in range(repeats):
        # alternate the build order each repeat so drift (heat, page cache) hits both
        order = cases if repeat % 2 == 0 else _reverse_builds(cases)
        for source, stage, settings, build in order:
            out = work / "out"
            spill = work / "spill"
            for path in (out, spill):
                shutil.rmtree(path, ignore_errors=True)
                path.mkdir(parents=True)
            cmd = transform_command(build, stage, source, settings, out, spill)
            log = (
                work
                / "logs"
                / f"{build.name}-{source.label}-{stage}-{settings.label().replace('/', '-')}-{repeat}.log"
            )
            log.parent.mkdir(parents=True, exist_ok=True)
            code, wall, cpu, rss, spill_peak = run_measured(cmd, log, spill)
            m = Measurement(
                build=build.name,
                stage=stage,
                source=source.label,
                settings=settings.label(),
                repeat=repeat,
                wall_s=round(wall, 3),
                cpu_s=round(cpu, 3),
                peak_rss_bytes=rss,
                peak_spill_bytes=spill_peak,
                exit_code=code,
                output_bytes=directory_bytes(out),
                command=cmd,
            )
            # the read stage writes Arrow IPC, which DuckDB reads only through an extension
            if code == 0 and validate and repeat == 0 and stage != "read":
                m.rows, m.files, m.digest = summarize_output(out)
            results.append(m)
            done += 1
            status = "ok" if code == 0 else f"FAILED ({code}), see {log}"
            print(
                f"[{done}/{total}] {build.name:>6} {source.label} {stage:<9} {settings.label():<34}"
                f" {wall:8.2f}s wall {cpu:9.2f}s cpu {rss / 2**30:6.2f} GiB rss {status}",
                flush=True,
            )
            results_path.write_text(json.dumps([asdict(r) for r in results], indent=2))
    shutil.rmtree(work / "out", ignore_errors=True)
    shutil.rmtree(work / "spill", ignore_errors=True)


def _reverse_builds(cases: list[tuple]) -> list[tuple]:
    grouped: dict[tuple, list[tuple]] = {}
    for case in cases:
        grouped.setdefault(case[:3], []).append(case)
    return [case for group in grouped.values() for case in reversed(group)]


def check_outputs(results: list[Measurement]) -> list[str]:
    """Builds given the same stage and input must write the same rows."""
    problems = []
    groups: dict[tuple, list[Measurement]] = {}
    for r in results:
        if r.digest is not None:
            groups.setdefault((r.source, r.stage, r.settings), []).append(r)
    for key, group in groups.items():
        if len({(r.rows, r.files, r.digest) for r in group}) > 1:
            detail = ", ".join(
                f"{r.build}: {r.rows} rows in {r.files} files" for r in group
            )
            problems.append(f"{'/'.join(key)} outputs differ ({detail})")
    return problems


def budget_bytes(spec: str) -> int | None:
    """Bytes of a plain --memory-budget size such as 16GiB; None for the keyword forms."""
    units = {
        "KIB": 2**10,
        "MIB": 2**20,
        "GIB": 2**30,
        "KB": 10**3,
        "MB": 10**6,
        "GB": 10**9,
    }
    text = spec.replace(" ", "").upper()
    for unit, factor in units.items():
        if text.endswith(unit):
            try:
                return int(float(text[: -len(unit)]) * factor)
            except ValueError:
                return None
    return None


def report(results: list[Measurement], baseline: str | None) -> str:
    rows: list[list[str]] = []
    groups: dict[tuple, dict[str, list[Measurement]]] = {}
    for r in results:
        groups.setdefault((r.source, r.stage, r.settings), {}).setdefault(
            r.build, []
        ).append(r)
    for (source, stage, settings), by_build in groups.items():
        base = [m for m in by_build.get(baseline, []) if m.exit_code == 0]
        base_wall = statistics.median(m.wall_s for m in base) if base else None
        for build, runs in by_build.items():
            ms = [m for m in runs if m.exit_code == 0]
            passed = f"{len(ms)}/{len(runs)}"
            if not ms:
                rows.append([source, stage, settings, build, passed] + [""] * 10)
                continue
            wall = statistics.median(m.wall_s for m in ms)
            cpu = statistics.median(m.cpu_s for m in ms)
            rows_done = next((m.rows for m in ms if m.rows is not None), None)
            rss = max(m.peak_rss_bytes for m in ms)
            budget = budget_bytes(settings.split("mem=")[1].split("/")[0])
            rows.append(
                [
                    source,
                    stage,
                    settings,
                    build,
                    passed,
                    f"{wall:.2f}",
                    f"{min(m.wall_s for m in ms):.2f}-{max(m.wall_s for m in ms):.2f}",
                    f"{cpu:.1f}",
                    f"{cpu / wall:.1f}",
                    f"{rows_done / wall / 1e6:.2f}" if rows_done else "",
                    f"{rss / 2**30:.2f}",
                    f"{100 * rss / budget:.0f}%" if budget else "",
                    f"{max(m.peak_spill_bytes for m in ms) / 2**30:.2f}",
                    f"{statistics.median(m.output_bytes for m in ms) / 2**30:.2f}",
                    f"{wall / base_wall:.2f}x" if base_wall else "",
                ]
            )
    header = [
        "Source",
        "Stage",
        "Settings",
        "Build",
        "Passed",
        "Wall s (median)",
        "Wall range",
        "CPU s",
        "Cores busy",
        "M rows/s",
        "Peak RSS GiB",
        "RSS / budget",
        "Peak spill GiB",
        "Output GiB",
        f"vs {baseline}" if baseline else "",
    ]
    widths = [max(len(r[i]) for r in [header, *rows]) for i in range(len(header))]

    def line(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(w) for c, w in zip(cells, widths)) + " |"

    return "\n".join(
        [
            line(header),
            "| " + " | ".join("-" * w for w in widths) + " |",
            *map(line, rows),
        ]
    )


def generate(args: argparse.Namespace) -> None:
    """Write synthetic input as ZSTD Parquet.

    There are 135 categories, the largest holding two thirds of the rows, over six years,
    so an export writes several hundred files of very different sizes. Each entity has 2.4
    rows on average. account_id is 16% null and site_id 10%. Rows come out grouped by
    category and date, as input clustered on those columns would.
    """
    import duckdb

    out: Path = args.out
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty")
    out.mkdir(parents=True, exist_ok=True)
    entities = max(1, int(args.rows / 2.4))
    # spread the ids over a range that still fits the export's INT cast
    entity_stride = max(1, 641_000_000 // entities)
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    con.execute("SELECT setseed(?)", [args.seed])
    con.execute(
        f"""
        COPY (
            WITH draws AS (
                SELECT
                    CASE WHEN random() < 0.67 THEN 0
                         ELSE 1 + CAST(floor(134 * pow(random(), 2)) AS BIGINT) END AS category,
                    2021 + CAST(floor(random() * 6) AS BIGINT) AS event_year,
                    CAST(floor(random() * 365) AS BIGINT) AS day,
                    CAST(floor(random() * {entities}) AS BIGINT) AS entity,
                    random() AS u_site, random() AS u_account, random() AS u_region
                FROM range({args.rows})
            )
            SELECT
                entity * {entity_stride} + 15 AS entity_id,
                category * {CATEGORY_STRIDE} + {args.first_category} AS category_id,
                CASE WHEN u_account < 0.16 THEN NULL
                     ELSE 1003000000 + CAST(floor(u_account * 1080000) AS BIGINT) * 917
                     END AS account_id,
                CASE WHEN u_site < 0.10 THEN NULL
                     ELSE CAST(floor(u_site * 150000) AS BIGINT) * 2083 END AS site_id,
                make_date(CAST(event_year AS INT), 1, 1) + CAST(day AS INT) AS event_date,
                event_year,
                CASE WHEN u_region < 0.0015 THEN NULL
                     ELSE CAST(floor(u_region * 880) AS BIGINT) END AS region_id,
                CAST({args.first_category} AS BIGINT) AS batch_id
            FROM draws
            ORDER BY category_id, event_date
        ) TO '{out}' (FORMAT parquet, COMPRESSION zstd, PER_THREAD_OUTPUT true)
        """
    )
    files = sorted(out.glob("*.parquet"))
    size = sum(f.stat().st_size for f in files)
    print(
        f"wrote {args.rows} rows to {len(files)} files, {size / 2**30:.2f} GiB, in {out}"
    )


def settings_from(args: argparse.Namespace) -> list[Settings]:
    reserves = [None if r == "none" else r for r in args.reserve.split(",")]
    partitions = [int(p) for p in args.partitions.split(",")]
    engines = [None if e == "default" else e for e in args.sort_engine.split(",")]
    return [
        Settings(p, r, budget, engine)
        for p, r, budget, engine in product(
            partitions, reserves, args.memory_budget.split(","), engines
        )
    ]


def finish(
    args: argparse.Namespace, builds: list[Build], results: list[Measurement]
) -> None:
    results_path: Path = args.results
    payload = {
        "host": {
            "platform": platform.platform(),
            "cpus": os.cpu_count(),
            "python": sys.version.split()[0],
        },
        "builds": {
            b.name: {"path": str(b.path), "version": b.version()} for b in builds
        },
        "measurements": [asdict(r) for r in results],
    }
    results_path.write_text(json.dumps(payload, indent=2))
    print()
    print(report(results, args.baseline or builds[0].name))
    problems = check_outputs(results)
    failed = [r for r in results if r.exit_code != 0]
    for p in problems:
        print(f"OUTPUT MISMATCH: {p}")
    for r in failed:
        print(f"FAILED: {r.build} {r.source} {r.stage} {r.settings} exit {r.exit_code}")
    print(f"\nresults: {results_path}")
    if problems or failed:
        raise SystemExit(1)


def cmd_run(args: argparse.Namespace) -> None:
    builds = [Build.probe(spec) for spec in args.bin]
    work: Path = args.work
    work.mkdir(parents=True, exist_ok=True)
    results: list[Measurement] = []
    run_matrix(
        builds,
        [Source(label=args.input.name, local=args.input)],
        args.stages.split(","),
        settings_from(args),
        args.repeats,
        work,
        not args.no_validate,
        results,
        args.results,
    )
    finish(args, builds, results)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark a partitioned, sorted export across silk-chiffon builds."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="write synthetic input")
    gen.add_argument("--rows", type=int, default=36_000_000)
    gen.add_argument(
        "--first-category", type=int, default=2503, help="the lowest category_id"
    )
    gen.add_argument("--seed", type=float, default=0.42)
    gen.add_argument("--out", type=Path, required=True)
    gen.set_defaults(func=generate)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--bin",
            action="append",
            required=True,
            metavar="NAME=PATH",
            help="a silk-chiffon binary to time; repeat for each build",
        )
        p.add_argument(
            "--baseline", help="build the others are compared with (default: first)"
        )
        p.add_argument("--stages", default=",".join(STAGES))
        p.add_argument(
            "--partitions",
            default="10,20",
            help="--target-partitions values",
        )
        p.add_argument(
            "--reserve",
            default="none,50%",
            help="--non-spillable-reserve values; none leaves the flag out",
        )
        p.add_argument(
            "--memory-budget",
            default="100GiB",
            help="--memory-budget values. List smaller ones, such as 16GiB,2GiB,256MiB,"
            " to check that the export still finishes when it has to spill",
        )
        p.add_argument(
            "--sort-engine",
            default="default",
            help="--sort-engine values, such as parallel,datafusion; builds without the"
            " flag skip them. default leaves the flag out",
        )
        p.add_argument("--repeats", type=int, default=3)
        p.add_argument(
            "--work", type=Path, required=True, help="output and spill go here"
        )
        p.add_argument(
            "--results", type=Path, default=Path("bench_export_results.json")
        )
        p.add_argument(
            "--no-validate", action="store_true", help="skip the output digests"
        )
        p.add_argument(
            "--transform-args",
            default="",
            help="more arguments for every transform, such as"
            " '--spill-compression zstd'",
        )

    run = sub.add_parser("run", help="time each build's stages on local input")
    run.add_argument(
        "--input", type=Path, required=True, help="directory of Parquet files"
    )
    common(run)
    run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    EXTRA_TRANSFORM_ARGS.extend(shlex.split(getattr(args, "transform_args", "")))
    args.func(args)


if __name__ == "__main__":
    main()
