#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.14"
# dependencies = ["duckdb==1.4.4"]
# ///
"""Time sorts on five to eight columns of mixed types, across silk-chiffon sort engines.

`generate` writes rows of thirteen columns: integers, a date, a timestamp, a decimal, a
float, a boolean, and strings of low, medium, and high cardinality, one of them with a long
shared prefix. Three columns hold nulls. Every value is a hash of the row number, so any
machine writes the same rows.

`run` sorts that data by each key with each engine, memory budget, and reserve, writing one
LZ4 Parquet file, and records wall time, CPU time, and peak memory. With --validate it also
checks each output's row count, an order-independent digest, and that no row is out of
order. The order check runs at the smallest budget only, as it is the slow part; --digest
checks the count and digest alone.

This is the companion of bench_export.py, whose rows are a few narrow fixed-width columns.
The sort holds rows like those whole in the records it sorts. Rows like these it keeps
beside their keys, and that path has its own costs.

Usage:
    uv run scripts/bench_mixed.py generate --rows 100000000 --out /scratch/mixed
    uv run scripts/bench_mixed.py run --input /scratch/mixed --work /scratch/work \\
        --bin new=./target/release/silk-chiffon --results mixed.json --validate
    uv run scripts/bench_mixed.py run --input /scratch/mixed --work /scratch/work \\
        --bin new=./target/release/silk-chiffon --results mixed.json \\
        --keys mixed6,strings5 --sort-engine parallel --memory-budget 60GiB,4GiB --reserve none,50%
"""

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

KEYS = {
    # fixed-width columns only, one of them nullable
    "ints5": "tenant,event_date,quantity,active,id",
    # strings, a date, a decimal, and a timestamp, in mixed directions
    "mixed6": "region,event_date:desc,city,amount:desc,created_at,id",
    # strings, two of them with long shared prefixes
    "strings5": "region,city,name,path,id",
    # eight columns of seven types, three descending, one nullable
    "wide8": "tenant,region,active:desc,event_date,score:desc,name,created_at:desc,id",
}

COLUMNS = (
    "id, tenant, region, city, name, path, event_date, created_at, amount, score, active,"
    " quantity, note"
)


def sql_string(path: Path) -> str:
    """A path as a SQL string literal, for statements that take no parameters."""
    return "'" + str(path).replace("'", "''") + "'"


def generate(args: argparse.Namespace) -> None:
    import duckdb

    args.out.mkdir(parents=True, exist_ok=True)
    # `run` reads every Parquet file here, so files left from another run would be
    # sorted and counted too
    if any(args.out.glob("*.parquet")):
        raise SystemExit(
            f"{args.out} already holds Parquet files; give an empty directory"
        )
    con = duckdb.connect()
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
            FROM range({args.rows}) AS t(i)
        ) TO {sql_string(args.out)} (FORMAT parquet, COMPRESSION zstd, PER_THREAD_OUTPUT true,
                           OVERWRITE_OR_IGNORE true)
        """
    )
    files = list(args.out.glob("*.parquet"))
    size = sum(file.stat().st_size for file in files)
    print(
        f"wrote {args.rows} rows to {len(files)} files, {size / 2**30:.2f} GiB, in {args.out}"
    )


def quoted(name: str) -> str:
    """A column name as a DuckDB identifier, whatever characters it holds."""
    return '"' + name.replace('"', '""') + '"'


def out_of_order_sql(key: str) -> str:
    """Counts rows that sort before the row above them, in the file given as the one
    parameter. `col` is ascending with nulls last and `col:desc` is descending with
    nulls first, as silk-chiffon reads --sort-by."""
    columns = [part.split(":") for part in key.split(",")]
    lags = ", ".join(
        f"lag({quoted(name)}) OVER (ORDER BY file_row_number) AS {quoted('p_' + name)}"
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
            FROM read_parquet(?, file_row_number = true)
        ) WHERE file_row_number > 0 AND ({first_difference} ELSE 0 END) = 1
    """


def check(args: argparse.Namespace) -> None:
    import duckdb

    con = duckdb.connect()
    # the path goes in as a parameter, never into the SQL text
    rows, digest = con.execute(
        f"SELECT count(*), sum(hash({COLUMNS}) >> 8) FROM read_parquet(?)",
        [str(args.path)],
    ).fetchone()
    checked = {"rows": rows, "digest": str(digest)}
    if args.order:
        checked["out_of_order"] = con.execute(
            out_of_order_sql(args.key), [str(args.path)]
        ).fetchone()[0]
    print(json.dumps(checked))


def validate(path: Path, key: str, check_order: bool) -> dict:
    # In its own process: on Linux a child's peak RSS is never reported below what its
    # parent held when it started, so DuckDB's memory here would inflate later runs.
    command = [sys.executable, __file__, "check", str(path), key]
    if check_order:
        command.append("--order")
    done = subprocess.run(command, capture_output=True, text=True, check=True)
    return json.loads(done.stdout.strip().splitlines()[-1])


def run(args: argparse.Namespace) -> None:
    label, binary = args.bin.split("=", 1)
    failures: list[str] = []
    keys = args.keys.split(",")
    budgets = args.memory_budget.split(",")
    reserves = args.reserve.split(",")
    engines = args.sort_engine.split(",")
    cases = [
        (key, reserve, budget, engine)
        for key in keys
        for reserve in reserves
        for budget in budgets
        for engine in engines
    ]
    measurements = []
    for index, (key, reserve, budget, engine) in enumerate(cases, 1):
        if args.deadline is not None and time.time() >= args.deadline:
            print(f"deadline reached before case {index} of {len(cases)}", flush=True)
            break
        for name in ("out", "spill"):
            shutil.rmtree(args.work / name, ignore_errors=True)
            (args.work / name).mkdir(parents=True)
        out = args.work / "out" / "sorted.parquet"
        command = [
            binary, "transform", "--from-pattern", f"{args.input}/*.parquet",
            "--sort-by", KEYS[key], "--to", str(out), "--parquet-compression", "lz4",
            "--overwrite", "--target-partitions", str(args.partitions),
            "--memory-budget", budget, "--spill-path", str(args.work / "spill"),
            "--sort-engine", engine,
        ]  # fmt: skip
        if reserve != "none":
            command += ["--non-spillable-reserve", reserve]
        command += shlex.split(args.transform_args)
        log = args.work / "last.log"
        started = time.monotonic()
        with log.open("wb") as sink:
            pid = os.posix_spawnp(
                binary, command, os.environ,
                file_actions=[(os.POSIX_SPAWN_DUP2, sink.fileno(), 1),
                              (os.POSIX_SPAWN_DUP2, sink.fileno(), 2)],
            )  # fmt: skip
            _, status, usage = os.wait4(pid, 0)
        wall = time.monotonic() - started
        exit_code = os.waitstatus_to_exitcode(status)
        measurement = {
            "build": label, "key": key, "sort_by": KEYS[key], "engine": engine,
            "memory_budget": budget, "reserve": reserve, "partitions": args.partitions,
            "wall_s": wall, "cpu_s": usage.ru_utime + usage.ru_stime,
            # macOS reports bytes, Linux kilobytes
            "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
            "exit_code": exit_code,
        }  # fmt: skip
        settings = f"{key}/reserve={reserve}/mem={budget}/sort={engine}"
        if exit_code == 0:
            measurement["output_bytes"] = out.stat().st_size
            if args.validate or args.digest:
                # the order check is the slow part, so only the smallest budget gets it
                check_order = args.validate and budget == budgets[-1]
                measurement |= validate(out, KEYS[key], check_order=check_order)
                if measurement.get("out_of_order", 0) > 0:
                    failures.append(
                        f"{settings}: {measurement['out_of_order']} rows out of order"
                    )
            checks = " ".join(
                f"{name}={measurement[name]}"
                for name in ("rows", "digest", "out_of_order")
                if name in measurement
            )
            print(
                f"[{index}/{len(cases)}] {label} {settings:<46} {wall:7.2f}s wall"
                f" {measurement['cpu_s']:8.2f}s cpu {measurement['peak_rss_bytes'] / 2**30:6.2f} GiB rss"
                f" ok {checks}",
                flush=True,
            )
        else:
            tail = log.read_text(errors="replace").strip().splitlines()[-1:] or [""]
            print(
                f"[{index}/{len(cases)}] {label} {settings:<46} {wall:7.2f}s wall FAILED"
                f" ({exit_code}) {tail[0][:140]}",
                flush=True,
            )
            failures.append(f"{settings}: exit {exit_code}")
        measurements.append(measurement)
        args.results.write_text(json.dumps(measurements, indent=1))
    for failure in failures:
        print(f"FAILED: {failure}")
    if failures:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="write the mixed-type rows")
    gen.add_argument("--rows", type=int, default=100_000_000)
    gen.add_argument("--out", type=Path, required=True)
    gen.set_defaults(func=generate)
    bench = sub.add_parser("run", help="time each key, engine, budget, and reserve")
    bench.add_argument("--bin", required=True, metavar="NAME=PATH")
    bench.add_argument("--input", type=Path, required=True)
    bench.add_argument("--work", type=Path, required=True)
    bench.add_argument("--results", type=Path, required=True)
    bench.add_argument("--keys", default=",".join(KEYS))
    bench.add_argument(
        "--memory-budget", default="60GiB,4GiB,1GiB", help="largest first"
    )
    bench.add_argument("--reserve", default="none")
    bench.add_argument("--sort-engine", default="datafusion,parallel")
    bench.add_argument("--partitions", type=int, default=10)
    bench.add_argument(
        "--transform-args",
        default="",
        help="more arguments for every transform, such as '--spill-compression zstd'",
    )
    bench.add_argument(
        "--validate", action="store_true", help="digest and order checks"
    )
    bench.add_argument(
        "--digest", action="store_true", help="row count and digest only"
    )
    bench.add_argument(
        "--deadline", type=float, help="epoch seconds; start no case after it"
    )
    bench.set_defaults(func=run)
    checker = sub.add_parser("check", help="row count, digest, and order of one output")
    checker.add_argument("path", type=Path)
    checker.add_argument("key", help="the --sort-by the output was written with")
    checker.add_argument("--order", action="store_true")
    checker.set_defaults(func=check)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
