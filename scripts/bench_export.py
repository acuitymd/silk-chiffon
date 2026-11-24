#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.14"
# dependencies = ["duckdb==1.4.4"]
# ///
"""Benchmark silk-chiffon transforms that sort, across builds, sort engines, and memory budgets.

Three data sets, which --data picks for `generate` and for `run`. `export` and `mixed`
cover two of the three ways the sort holds rows. `late-keys` is made to be slow for a sort
that sizes its records by the keys of the first batch it is given.

export: eight integer and date columns, of which a `--query` keeps seven and casts them
to narrower types on the way in. The sort holds rows like these whole in the records it
sorts. The workload is one transform that splits its input into one file per
`event_year,category_id`, sorts each file by five more columns, and writes LZ4 Parquet.
Every stage runs the export's own flags and casts, so the stages differ only in what they
leave out:

    read       input -> one Arrow IPC file. Reading, decoding, and the casts.
    partition  input -> one Parquet file per event_year/category_id, unsorted.
    sort       input -> one sorted Parquet file, not partitioned.
    export     input -> partitioned and sorted.

`generate` writes a skewed set of categories over six years, with two nullable columns,
grouped by category and date.

mixed: thirteen columns of integers, a date, a timestamp, a decimal, a float, a boolean,
and strings of low, medium, and high cardinality. Every path starts with the same 17
bytes. The sort keeps rows like these in row format beside the records it sorts, and
decodes them as it sends them. Each stage sorts by one key and writes one LZ4 Parquet
file:

    ints5     five fixed-width columns, one of them nullable
    mixed6    strings, a date, a decimal, and a timestamp, in mixed directions
    strings5  four strings, path among them, and an integer
    wide8     eight columns of seven types, three descending, one nullable

`generate` makes every value a hash of the row number, so any machine writes the same
rows.

late-keys: an id, a path, an amount, and a note. Each row group starts with 16,384 rows
whose path is empty or null, and the rest of its rows have paths of 70 bytes that share
their first 58. A partition of the input starts at a row group, so every partition's first
batch has only short keys. A sort whose records hold as much of a key as the first batch's
keys needed then holds the same few bytes of every long path, and has to read both whole
keys from row format to compare two of them. Its one stage, `path`, sorts by `path,id`
and writes one LZ4 Parquet file. `generate` derives every value from the file and row
number, so any machine writes the same rows.

`run` times each stage for each build. Every option that takes a list runs each
combination: `--memory-budget 60GiB,4GiB,256MiB` checks that a sort still finishes as the
budget shrinks, and `--sort-engine parallel,datafusion` times both sorts of a build that
has the flag. The builds run in alternating order from one repeat to the next, so drift
such as heat and the page cache falls on all of them. The report gives each case's median
wall time, peak RSS against its budget, peak spill, and its time against the baseline
build.

The first repeat of each case is checked, unless --check none:

- The output's rows are counted and hashed into a digest that does not depend on their
  order. Every case of a stage must write the same rows, whatever the build, engine,
  budget, reserve, or partitions.
- At the last budget listed, the rows out of order are counted in every sorted output
  file. This check is the slow part. List budgets largest first to give it the smallest.
  --check digest skips it.

A transform that fails, outputs that differ, or a row out of order makes the script exit 1.

Only `generate` and the checks need DuckDB, so with --check none the script runs on any
Python 3.12 or later without uv.

Usage:
    uv run scripts/bench_export.py generate --rows 36000000 --out /scratch/input
    uv run scripts/bench_export.py run --input /scratch/input --work /scratch/work \\
        --bin old=./silk-chiffon-main --bin new=./silk-chiffon-branch
    uv run scripts/bench_export.py run --input /scratch/input --work /scratch/work \\
        --stages export --bin old=./silk-chiffon-main --bin new=./silk-chiffon-branch \\
        --partitions 10 --reserve none,50% --memory-budget 60GiB,4GiB,256MiB
    uv run scripts/bench_export.py generate --data mixed --rows 100000000 --out /scratch/mixed
    uv run scripts/bench_export.py run --data mixed --input /scratch/mixed \\
        --work /scratch/work --bin new=./target/release/silk-chiffon \\
        --sort-engine parallel,datafusion --memory-budget 60GiB,4GiB,1GiB --reserve none
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

# set from --transform-args
EXTRA_TRANSFORM_ARGS: list[str] = []


@dataclass(frozen=True)
class Stage:
    # --sort-by, when the stage sorts
    sort_by: str | None = None
    # --by, when the stage writes one file per partition
    partition_by: str | None = None
    # one Arrow IPC file instead of LZ4 Parquet
    arrow: bool = False


@dataclass(frozen=True)
class DataSet:
    stages: dict[str, Stage]
    # every column, in the order the digest hashes them
    columns: str
    # --query, applied on the way in
    query: str | None = None
    rows: int = 0


EXPORT_PARTITION_BY = "event_year,category_id"
EXPORT_SORT_BY = "entity_id,event_date,site_id,account_id,region_id"
# categories are spaced this far apart, starting at --first-category
CATEGORY_STRIDE = 3600

DATA_SETS = {
    "export": DataSet(
        stages={
            "read": Stage(arrow=True),
            "partition": Stage(partition_by=EXPORT_PARTITION_BY),
            "sort": Stage(sort_by=EXPORT_SORT_BY),
            "export": Stage(sort_by=EXPORT_SORT_BY, partition_by=EXPORT_PARTITION_BY),
        },
        columns="entity_id, event_date, event_year, site_id, account_id, category_id,"
        " region_id",
        # narrows the input's 64-bit integers, and drops its eighth column
        query=(
            "SELECT CAST(entity_id AS INT) AS entity_id, CAST(event_date AS DATE) AS event_date,"
            " CAST(event_year AS SMALLINT) AS event_year, CAST(site_id AS INT) AS site_id,"
            " CAST(account_id AS INT) AS account_id, CAST(category_id AS INT) AS category_id,"
            " CAST(region_id AS SMALLINT) AS region_id FROM data"
        ),
        rows=36_000_000,
    ),
    "mixed": DataSet(
        stages={
            "ints5": Stage(sort_by="tenant,event_date,quantity,active,id"),
            "mixed6": Stage(
                sort_by="region,event_date:desc,city,amount:desc,created_at,id"
            ),
            "strings5": Stage(sort_by="region,city,name,path,id"),
            "wide8": Stage(
                sort_by="tenant,region,active:desc,event_date,score:desc,name,created_at:desc,id"
            ),
        },
        columns="id, tenant, region, city, name, path, event_date, created_at, amount,"
        " score, active, quantity, note",
        rows=100_000_000,
    ),
    "late-keys": DataSet(
        stages={"path": Stage(sort_by="path,id")},
        columns="id, path, amount, note",
        rows=50_000_000,
    ),
}

# every long path starts with these 58 bytes
LATE_KEY_PREFIX = "/archive/regions/north-america/accounts/item-records/2024/"
# rows in a row group of the late-keys data, and the short paths that start each one
LATE_KEY_ROW_GROUP = 122_880
LATE_KEY_SHORT_ROWS = 16_384
LATE_KEY_FILES = 16


def output_template(partition_by: str) -> str:
    return (
        "/".join(f"{column}={{{{ {column} }}}}" for column in partition_by.split(","))
        + "/data.parquet"
    )


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
    out_of_order: int | None = None
    misplaced: int | None = None
    command: list[str] = field(default_factory=list)


def sql_string(path: Path) -> str:
    """A path as a SQL string literal, for statements that take no parameters."""
    return "'" + str(path).replace("'", "''") + "'"


def quoted(name: str) -> str:
    """A column name as a DuckDB identifier, whatever characters it holds."""
    return '"' + name.replace('"', '""') + '"'


def transform_command(
    build: Build,
    data: DataSet,
    stage: Stage,
    source: Source,
    settings: Settings,
    out: Path,
    spill: Path,
) -> list[str]:
    cmd = [str(build.path), "transform", *build.input_args(source)]
    if stage.partition_by is not None:
        cmd += [
            "--by",
            stage.partition_by,
            "--to-many",
            str(out / output_template(stage.partition_by)),
        ]
    elif stage.arrow:
        cmd += ["--to", str(out / "data.arrow")]
    else:
        cmd += ["--to", str(out / "data.parquet")]
    if stage.sort_by is not None:
        cmd += ["--sort-by", stage.sort_by]
    if not stage.arrow:
        cmd += ["--parquet-compression", "lz4"]
    if data.query is not None:
        cmd += ["--query", data.query]
    cmd += [
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


def out_of_order_sql(sort_by: str) -> str:
    """Counts rows that sort before the row above them in the same file, in the files
    given as the one parameter. `col` is ascending with nulls last and `col:desc` is
    descending with nulls first, as silk-chiffon reads --sort-by."""
    columns = [part.split(":") for part in sort_by.split(",")]
    lags = ", ".join(
        f"lag({quoted(name)}) OVER (PARTITION BY filename ORDER BY file_row_number)"
        f" AS {quoted('p_' + name)}"
        for name, *_ in columns
    )
    cases = []
    for name, *direction in columns:
        descending = direction == ["desc"]
        cur, prev = quoted(name), quoted(f"p_{name}")
        # 1 when the row above belongs after this one by this column, -1 when before
        null_above, null_here = (-1, 1) if descending else (1, -1)
        above_greater = -1 if descending else 1
        cases.append(
            f"""CASE WHEN {prev} IS NULL AND {cur} IS NULL THEN 0
                     WHEN {prev} IS NULL THEN {null_above}
                     WHEN {cur} IS NULL THEN {null_here}
                     WHEN {prev} > {cur} THEN {above_greater}
                     WHEN {prev} < {cur} THEN {-above_greater}
                     ELSE 0 END"""
        )
    first_difference = "CASE " + " ".join(
        f"WHEN ({case}) <> 0 THEN ({case})" for case in cases
    )
    return f"""
        SELECT count(*) FROM (
            SELECT file_row_number, {", ".join(quoted(name) for name, *_ in columns)}, {lags}
            FROM read_parquet(?, file_row_number = true, filename = true,
                              hive_partitioning = false)
        ) WHERE file_row_number > 0 AND ({first_difference} ELSE 0 END) = 1
    """


def misplaced_sql(partition_by: str) -> str:
    """Counts rows whose file path lacks a `col=value` directory for each partition
    column, in the files given as the one parameter."""
    # read with hive_partitioning off, the values come from the file, so a row written
    # under another partition's directory still matches the input's digest
    placed = " AND ".join(
        f"contains(filename, '/{column}=' || CAST({quoted(column)} AS VARCHAR) || '/')"
        for column in partition_by.split(",")
    )
    return f"""
        SELECT count(*)
        FROM read_parquet(?, filename = true, hive_partitioning = false)
        WHERE NOT ({placed})
    """


def check(args: argparse.Namespace) -> None:
    """Prints the rows, files, digest, and optionally the rows out of order and the
    rows outside their partition's directory, of the Parquet files under one output
    directory, as JSON."""
    import duckdb

    data = DATA_SETS[args.data]
    files = len(list(args.out.rglob("*.parquet")))
    checked: dict[str, int] = {"rows": 0, "files": files, "digest": 0}
    if files:
        con = duckdb.connect()
        # the path goes in as a parameter, never into the SQL text
        pattern = [f"{args.out}/**/*.parquet"]
        rows, digest = con.execute(
            f"SELECT count(*), coalesce(sum(hash({data.columns}) >> 8), 0)"
            " FROM read_parquet(?, hive_partitioning = false)",
            pattern,
        ).fetchone()
        checked |= {"rows": rows, "digest": int(digest)}
        if args.sort_by:
            checked["out_of_order"] = con.execute(
                out_of_order_sql(args.sort_by), pattern
            ).fetchone()[0]
        if args.partition_by:
            checked["misplaced"] = con.execute(
                misplaced_sql(args.partition_by), pattern
            ).fetchone()[0]
    print(json.dumps(checked))


def check_output(
    data: str, out: Path, sort_by: str | None, partition_by: str | None
) -> dict[str, int]:
    # In its own process: on Linux a child's peak RSS is never reported below the
    # parent's peak when the child started, so DuckDB in this process would inflate
    # the peak RSS of every transform after it.
    command = [sys.executable, __file__, "check", "--data", data, str(out)]
    if sort_by is not None:
        command += ["--sort-by", sort_by]
    if partition_by is not None:
        command += ["--partition-by", partition_by]
    done = subprocess.run(command, capture_output=True, text=True, check=True)
    return json.loads(done.stdout.strip().splitlines()[-1])


def run_matrix(
    builds: list[Build],
    data_name: str,
    sources: list[Source],
    stages: list[str],
    settings_list: list[Settings],
    repeats: int,
    work: Path,
    check_mode: str,
    deadline: float | None,
    results: list[Measurement],
    results_path: Path,
) -> None:
    data = DATA_SETS[data_name]
    cases = [
        (source, stage, settings, build)
        for source, stage, settings in product(sources, stages, settings_list)
        for build in builds
        if settings.sort_engine is None or build.has_sort_engine
    ]
    if not cases:
        raise SystemExit(
            "no case to run: every build given lacks the --sort-engine flag that was asked for"
        )
    # the order check is the slow part, so only the last budget listed gets it
    order_budget = settings_list[-1].memory_budget
    total = len(cases) * repeats
    done = 0
    for repeat in range(repeats):
        # alternate the build order each repeat so drift (heat, page cache) hits both
        order = cases if repeat % 2 == 0 else _reverse_builds(cases)
        for source, stage_name, settings, build in order:
            if deadline is not None and time.time() >= deadline:
                print(f"deadline reached after {done} of {total} runs", flush=True)
                return
            stage = data.stages[stage_name]
            out = work / "out"
            spill = work / "spill"
            for path in (out, spill):
                shutil.rmtree(path, ignore_errors=True)
                path.mkdir(parents=True)
            cmd = transform_command(build, data, stage, source, settings, out, spill)
            log = (
                work
                / "logs"
                / f"{build.name}-{source.label}-{stage_name}-{settings.label().replace('/', '-')}-{repeat}.log"
            )
            log.parent.mkdir(parents=True, exist_ok=True)
            code, wall, cpu, rss, spill_peak = run_measured(cmd, log, spill)
            m = Measurement(
                build=build.name,
                stage=stage_name,
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
            checks = ""
            # DuckDB reads Arrow IPC only through an extension
            if code == 0 and check_mode != "none" and repeat == 0 and not stage.arrow:
                order_by = (
                    stage.sort_by
                    if check_mode == "order" and settings.memory_budget == order_budget
                    else None
                )
                checked = check_output(data_name, out, order_by, stage.partition_by)
                m.rows, m.files, m.digest = (
                    checked["rows"],
                    checked["files"],
                    checked["digest"],
                )
                m.out_of_order = checked.get("out_of_order")
                m.misplaced = checked.get("misplaced")
                checks = (
                    f" rows={m.rows}"
                    + (
                        f" out_of_order={m.out_of_order}"
                        if m.out_of_order is not None
                        else ""
                    )
                    + (f" misplaced={m.misplaced}" if m.misplaced is not None else "")
                )
            results.append(m)
            done += 1
            status = "ok" if code == 0 else f"FAILED ({code}), see {log}"
            print(
                f"[{done}/{total}] {build.name:>6} {source.label} {stage_name:<9} {settings.label():<34}"
                f" {wall:8.2f}s wall {cpu:9.2f}s cpu {rss / 2**30:6.2f} GiB rss {status}{checks}",
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


def check_outputs(
    results: list[Measurement], input_rows: int | None = None
) -> list[str]:
    """Every case of a stage must write the same rows, none out of order and none
    outside their partition's directory."""
    problems = []
    groups: dict[tuple, list[Measurement]] = {}
    for r in results:
        # cases agreeing with each other proves nothing when they all drop the same rows
        if input_rows is not None and r.rows is not None and r.rows != input_rows:
            problems.append(
                f"{r.build} {r.source} {r.stage} {r.settings}: {r.rows} rows written, input has {input_rows}"
            )
        if r.digest is not None:
            groups.setdefault((r.source, r.stage), []).append(r)
        if r.out_of_order:
            problems.append(
                f"{r.build} {r.source} {r.stage} {r.settings}: {r.out_of_order} rows out of order"
            )
        if r.misplaced:
            problems.append(
                f"{r.build} {r.source} {r.stage} {r.settings}: {r.misplaced} rows outside their partition's directory"
            )
    for key, group in groups.items():
        if len({(r.rows, r.files, r.digest) for r in group}) > 1:
            detail = ", ".join(
                f"{r.build} {r.settings}: {r.rows} rows in {r.files} files, digest {r.digest}"
                for r in group
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


def generate_export(con, args: argparse.Namespace, rows: int) -> None:
    """There are 135 categories, the largest holding two thirds of the rows, over six
    years, so an export writes several hundred files of very different sizes. Each entity
    has 2.4 rows on average. account_id is 16% null and site_id 10%. Rows come out
    grouped by category and date, as input clustered on those columns would."""
    entities = max(1, int(rows / 2.4))
    # spread the ids over a range that still fits the export's INT cast
    entity_stride = max(1, 641_000_000 // entities)
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
                FROM range({rows})
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
        ) TO {sql_string(args.out)} (FORMAT parquet, COMPRESSION zstd, PER_THREAD_OUTPUT true)
        """
    )


def generate_mixed(con, args: argparse.Namespace, rows: int) -> None:
    """Three columns hold nulls: score, quantity, and note."""
    con.execute(
        f"""
        COPY (
            SELECT
                CAST(hash(i, 1) % 1000000000000 AS BIGINT) AS id,
                CAST(pow((hash(i, 2) % 1000000) / 1000000.0, 3) * 2000 AS INTEGER) AS tenant,
                ['amer-north', 'amer-south', 'amer-central', 'emea-west', 'emea-east',
                 'emea-north', 'apac-south', 'apac-east', 'latam', 'anz', 'mea', 'other'
                ][1 + CAST(hash(i, 3) % 12 AS INTEGER)] AS region,
                'city-' || lpad(CAST(hash(i, 4) % 20000 AS VARCHAR), 5, '0') AS city,
                substr(md5(CAST(hash(i, 5) % 30000000 AS VARCHAR)), 1,
                       6 + CAST(hash(i, 6) % 27 AS INTEGER)) AS name,
                '/api/v2/accounts/' || lpad(CAST(hash(i, 2) % 2000 AS VARCHAR), 4, '0')
                    || '/items/' || lpad(CAST(hash(i, 7) % 5000000 AS VARCHAR), 9, '0') AS path,
                DATE '2020-01-01' + CAST(hash(i, 8) % 2200 AS INTEGER) AS event_date,
                TIMESTAMP '2020-01-01'
                    + to_microseconds(CAST(hash(i, 9) % 190000000000000 AS BIGINT)) AS created_at,
                CAST((hash(i, 10) % 2000000000) / 100.0 - 5000000 AS DECIMAL(18, 2)) AS amount,
                CASE WHEN hash(i, 11) % 20 = 0 THEN NULL
                     ELSE (hash(i, 12) % 100000000) / 1000.0 END AS score,
                hash(i, 13) % 3 = 0 AS active,
                CASE WHEN hash(i, 14) % 10 = 0 THEN NULL
                     ELSE CAST(hash(i, 15) % 500 AS SMALLINT) END AS quantity,
                CASE WHEN hash(i, 16) % 3 = 0 THEN NULL
                     ELSE repeat('n', 10 + CAST(hash(i, 17) % 50 AS INTEGER)) END AS note
            FROM range({rows}) AS t(i)
        ) TO {sql_string(args.out)} (FORMAT parquet, COMPRESSION zstd, PER_THREAD_OUTPUT true)
        """
    )


def generate_late_keys(con, args: argparse.Namespace, rows: int) -> None:
    """One file at a time, in row order, so that every row group starts with its short
    paths."""
    files = max(1, min(LATE_KEY_FILES, rows))
    first = 0
    for file in range(files):
        # the first files take one row more when the rows do not divide evenly
        file_rows = rows // files + (1 if file < rows % files else 0)
        con.execute(
            f"""
            COPY (
                SELECT
                    CAST({first} + i AS BIGINT) AS id,
                    CASE WHEN i % {LATE_KEY_ROW_GROUP} < {LATE_KEY_SHORT_ROWS}
                         THEN CASE WHEN i % 2 = 0 THEN NULL ELSE '' END
                         ELSE '{LATE_KEY_PREFIX}'
                              || lpad(CAST(hash({file}, i) % 1000000000000 AS VARCHAR), 12, '0')
                         END AS path,
                    CAST((hash({file}, i, 1) % 100000000) / 100.0 AS DECIMAL(18, 2)) AS amount,
                    repeat('n', 10 + CAST(hash({file}, i, 2) % 30 AS INTEGER)) AS note
                FROM range({file_rows}) AS t(i)
                ORDER BY i
            ) TO {sql_string(args.out / f"data_{file:02}.parquet")}
                (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {LATE_KEY_ROW_GROUP})
            """
        )
        first += file_rows


def generate(args: argparse.Namespace) -> None:
    """Write synthetic input for one data set as ZSTD Parquet."""
    import duckdb

    out: Path = args.out
    # `run` reads every Parquet file here, so files left from another run would be
    # transformed and counted too
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty")
    out.mkdir(parents=True, exist_ok=True)
    rows = args.rows or DATA_SETS[args.data].rows
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    match args.data:
        case "export":
            generate_export(con, args, rows)
        case "mixed":
            generate_mixed(con, args, rows)
        case "late-keys":
            generate_late_keys(con, args, rows)
    files = sorted(out.glob("*.parquet"))
    size = sum(f.stat().st_size for f in files)
    print(f"wrote {rows} rows to {len(files)} files, {size / 2**30:.2f} GiB, in {out}")


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
    args: argparse.Namespace,
    builds: list[Build],
    results: list[Measurement],
    input_rows: int | None,
) -> None:
    results_path: Path = args.results
    payload = {
        "host": {
            "platform": platform.platform(),
            "cpus": os.cpu_count(),
            "python": sys.version.split()[0],
        },
        "data": args.data,
        "builds": {
            b.name: {"path": str(b.path), "version": b.version()} for b in builds
        },
        "measurements": [asdict(r) for r in results],
    }
    results_path.write_text(json.dumps(payload, indent=2))
    print()
    print(report(results, args.baseline or builds[0].name))
    problems = check_outputs(results, input_rows)
    failed = [r for r in results if r.exit_code != 0]
    for p in problems:
        print(f"OUTPUT PROBLEM: {p}")
    for r in failed:
        print(f"FAILED: {r.build} {r.source} {r.stage} {r.settings} exit {r.exit_code}")
    print(f"\nresults: {results_path}")
    if problems or failed:
        raise SystemExit(1)


def cmd_run(args: argparse.Namespace) -> None:
    data = DATA_SETS[args.data]
    stages = args.stages.split(",") if args.stages else list(data.stages)
    unknown = [stage for stage in stages if stage not in data.stages]
    if unknown:
        raise SystemExit(
            f"--data {args.data} has no stage {', '.join(unknown)};"
            f" its stages are {', '.join(data.stages)}"
        )
    builds = [Build.probe(spec) for spec in args.bin]
    work: Path = args.work
    work.mkdir(parents=True, exist_ok=True)
    results: list[Measurement] = []
    # no stage filters rows, so every output must hold all of the input's
    input_rows = (
        check_output(args.data, args.input, None, None)["rows"]
        if args.check != "none"
        else None
    )
    run_matrix(
        builds,
        args.data,
        [Source(label=args.input.name, local=args.input)],
        stages,
        settings_from(args),
        args.repeats,
        work,
        args.check,
        args.deadline,
        results,
        args.results,
    )
    finish(args, builds, results, input_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def data_option(p: argparse.ArgumentParser) -> None:
        p.add_argument("--data", choices=DATA_SETS, default="export")

    gen = sub.add_parser("generate", help="write synthetic input")
    data_option(gen)
    gen.add_argument(
        "--rows",
        type=int,
        help="default: 36000000 for export, 100000000 for mixed, 50000000 for late-keys",
    )
    gen.add_argument(
        "--first-category",
        type=int,
        default=2503,
        help="the lowest category_id of the export data",
    )
    gen.add_argument("--seed", type=float, default=0.42, help="for the export data")
    gen.add_argument("--out", type=Path, required=True)
    gen.set_defaults(func=generate)

    run = sub.add_parser("run", help="time each build's stages on local input")
    data_option(run)
    run.add_argument(
        "--input", type=Path, required=True, help="directory of Parquet files"
    )
    run.add_argument(
        "--bin",
        action="append",
        required=True,
        metavar="NAME=PATH",
        help="a silk-chiffon binary to time; repeat for each build",
    )
    run.add_argument(
        "--baseline", help="build the others are compared with (default: first)"
    )
    run.add_argument("--stages", help="default: every stage of the data set")
    run.add_argument(
        "--partitions",
        default="10,20",
        help="--target-partitions values",
    )
    run.add_argument(
        "--reserve",
        default="none,50%",
        help="--non-spillable-reserve values; none leaves the flag out",
    )
    run.add_argument(
        "--memory-budget",
        default="100GiB",
        help="--memory-budget values, largest first. List smaller ones, such as"
        " 16GiB,2GiB,256MiB, to check that a sort still finishes when it has to spill",
    )
    run.add_argument(
        "--sort-engine",
        default="default",
        help="--sort-engine values, such as parallel,datafusion; builds without the"
        " flag skip them. default leaves the flag out",
    )
    run.add_argument("--repeats", type=int, default=3)
    run.add_argument(
        "--work", type=Path, required=True, help="output and spill go here"
    )
    run.add_argument("--results", type=Path, default=Path("bench_export_results.json"))
    run.add_argument(
        "--check",
        choices=("order", "digest", "none"),
        default="order",
        help="what to check of each case's first repeat: rows and digest, and at the"
        " last budget listed the order (order); rows and digest only (digest); nothing"
        " (none)",
    )
    run.add_argument(
        "--deadline", type=float, help="epoch seconds; start no run after it"
    )
    run.add_argument(
        "--transform-args",
        default="",
        help="more arguments for every transform, such as '--spill-compression zstd'",
    )
    run.set_defaults(func=cmd_run)

    checker = sub.add_parser(
        "check", help="rows, files, digest, and order of one run's output"
    )
    data_option(checker)
    checker.add_argument("out", type=Path, help="the directory the transform wrote to")
    checker.add_argument(
        "--sort-by", help="also count rows out of order by this --sort-by"
    )
    checker.add_argument(
        "--partition-by",
        help="also count rows outside their directory for this --partition-by",
    )
    checker.set_defaults(func=check)

    args = parser.parse_args()
    EXTRA_TRANSFORM_ARGS.extend(shlex.split(getattr(args, "transform_args", "")))
    args.func(args)


if __name__ == "__main__":
    main()
