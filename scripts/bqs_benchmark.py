#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#   "duckdb==1.4.4",
#   "google-api-core==2.30.0",
#   "google-auth==2.49.0",
#   "google-cloud-bigquery-storage==2.28.0",
#   "googleapis-common-protos==1.73.0",
#   "grpcio==1.78.0",
#   "grpcio-status==1.71.2",
#   "psutil==7.0.0",
#   "proto-plus==1.27.1",
#   "protobuf==5.29.6",
#   "pyarrow==20.0.0",
# ]
# ///

"""Run a counterbalanced Silk Chiffon and Python Storage Read campaign."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import queue
import random
import re
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

MINIMUM_DECISION_PAIRS = 10
PRACTICAL_SPEEDUP_PCT = 5.0
MAX_RELATIVE_SCALED_MAD_PCT = 10.0
MAX_RANGE_PCT = 25.0
BOOTSTRAP_SAMPLES = 10_000
DEFAULT_ROWS = 5_000_000
DEFAULT_STREAMS = 12
DEFAULT_MAXIMUM_BYTES_BILLED = 10 * 1024**3
DEFAULT_DEADLINE_SECONDS = 3_600.0
DEFAULT_SELECTED_FIELDS = (
    "id",
    "cohort",
    "metric",
    "event_time",
    "label",
    "nullable_value",
    "payload",
)
DEFAULT_ROW_RESTRICTION = "cohort < 896"
_FIXTURE_COHORTS = 1_024
_FIXTURE_SELECTED_COHORTS = 896
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]+")


@dataclass(frozen=True)
class BenchmarkContract:
    project: str
    table: str
    location: str
    snapshot: str
    source_rows: int
    selected_fields: tuple[str, ...]
    row_restriction: str
    expected_rows: int
    requested_streams: int
    wire_compression: str = "zstd"
    response_compression: str = "none"
    output_container: str = "arrow-ipc-file"
    output_compression: str = "zstd"
    measurement_scope: str = "process-start-through-writer-close"

    def __post_init__(self) -> None:
        for name in ("project", "table", "location", "snapshot"):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
        if self.source_rows <= 0:
            raise ValueError("source_rows must be positive")
        if not self.selected_fields or any(not field for field in self.selected_fields):
            raise ValueError("selected_fields must contain at least one field")
        if not self.row_restriction:
            raise ValueError("row_restriction must not be empty")
        if self.expected_rows <= 0 or self.expected_rows > self.source_rows:
            raise ValueError("expected_rows must be within the source row count")
        if self.requested_streams <= 0:
            raise ValueError("requested_streams must be positive")
        if self.wire_compression != "zstd":
            raise ValueError(
                "the matched benchmark contract requires Zstandard wire compression"
            )
        if self.response_compression != "none":
            raise ValueError(
                "the matched benchmark contract disables response compression"
            )
        if self.output_compression != "zstd":
            raise ValueError(
                "the matched benchmark contract requires Zstandard output compression"
            )

    def identity_sha256(self) -> str:
        canonical = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class ProcessResult:
    command: tuple[str, ...]
    return_code: int
    wall_seconds: float
    cpu_seconds: float
    peak_rss_bytes: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ValidationResult:
    rows: int
    distinct_ids: int
    minimum_id: int
    maximum_id: int
    sum_id: int
    schema: tuple[tuple[str, str, bool], ...]
    digest: tuple[str, str, str, str]
    file_sha256: str
    file_bytes: int


def pair_order(index: int) -> tuple[str, str]:
    if index < 0:
        raise ValueError("pair index cannot be negative")
    return ("silk", "python") if index % 2 == 0 else ("python", "silk")


def logical_digest(
    rows: Iterable[dict[str, object]],
    columns: Sequence[str],
) -> tuple[str, str]:
    """Return an order-independent two-domain digest for small test fixtures."""
    totals = [0, 0]
    modulus = 1 << 256
    for row in rows:
        encoded = json.dumps(
            [row[column] for column in columns],
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        for index, domain in enumerate((b"silk-bqs-1\0", b"silk-bqs-2\0")):
            totals[index] = (
                totals[index]
                + int.from_bytes(hashlib.sha256(domain + encoded).digest(), "big")
            ) % modulus
    return tuple(f"{value:064x}" for value in totals)  # type: ignore[return-value]


def expected_filtered_ids(rows: int, cohorts: int, selected: int) -> dict[str, int]:
    if rows <= 0 or cohorts <= 0 or selected <= 0 or selected > cohorts:
        raise ValueError("invalid fixture row expectation")
    full_blocks, remainder = divmod(rows, cohorts)
    tail = min(remainder, selected)
    selected_rows = full_blocks * selected + tail
    block_offsets = cohorts * selected * full_blocks * (full_blocks - 1) // 2
    within_blocks = full_blocks * selected * (selected - 1) // 2
    tail_sum = tail * (2 * full_blocks * cohorts + tail - 1) // 2
    maximum = (
        (full_blocks * cohorts + tail - 1)
        if tail
        else ((full_blocks - 1) * cohorts + selected - 1)
    )
    return {
        "rows": selected_rows,
        "minimum": 0,
        "maximum": maximum,
        "sum": block_offsets + within_blocks + tail_sum,
    }


def _aggregate(values: Sequence[float]) -> dict[str, float | int]:
    if not values or any(not math.isfinite(value) or value <= 0 for value in values):
        raise ValueError("benchmark measurements must be finite and positive")
    mean = statistics.fmean(values)
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    scaled_mad = 1.4826 * mad
    return {
        "samples": len(values),
        "mean": mean,
        "median": median,
        "standard_deviation": statistics.stdev(values) if len(values) > 1 else 0.0,
        "relative_scaled_mad_pct": scaled_mad / median * 100.0,
        "minimum": min(values),
        "maximum": max(values),
        "range_pct_of_mean": (max(values) - min(values)) / mean * 100.0,
    }


def _aggregate_signed(values: Sequence[float]) -> dict[str, float | int]:
    if not values or any(not math.isfinite(value) for value in values):
        raise ValueError("benchmark measurements must be finite")
    mean = statistics.fmean(values)
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    return {
        "samples": len(values),
        "mean": mean,
        "median": median,
        "standard_deviation": statistics.stdev(values) if len(values) > 1 else 0.0,
        "median_absolute_deviation": mad,
        "minimum": min(values),
        "maximum": max(values),
    }


def _bootstrap_interval(values: Sequence[float], seed: str) -> tuple[float, float]:
    generator = random.Random(int(hashlib.sha256(seed.encode()).hexdigest(), 16))
    estimates = []
    for _ in range(BOOTSTRAP_SAMPLES):
        sample = [values[generator.randrange(len(values))] for _ in values]
        estimates.append(statistics.fmean(sample))
    estimates.sort()
    return (
        estimates[int(0.025 * (len(estimates) - 1))],
        estimates[int(0.975 * (len(estimates) - 1))],
    )


def summarize_paired_seconds(
    *,
    silk: Sequence[float],
    python: Sequence[float],
    seed: str,
) -> dict[str, object]:
    if len(silk) != len(python) or not silk:
        raise ValueError(
            "paired benchmark vectors must be nonempty and equal in length"
        )
    silk_summary = _aggregate(silk)
    python_summary = _aggregate(python)
    paired_speedups = [
        (python_seconds / silk_seconds - 1.0) * 100.0
        for silk_seconds, python_seconds in zip(silk, python, strict=True)
    ]
    result: dict[str, object] = {
        "silk_seconds": silk_summary,
        "python_seconds": python_summary,
        "paired_silk_speedup_pct": _aggregate_signed(paired_speedups),
        "paired_silk_speedup_pct_values": paired_speedups,
        "winner": None,
    }
    if len(paired_speedups) < MINIMUM_DECISION_PAIRS:
        result["reason"] = "a winner requires at least 10 counterbalanced pairs"
        return result
    lower, upper = _bootstrap_interval(paired_speedups, seed)
    result["paired_silk_speedup_ci95_pct"] = {"lower": lower, "upper": upper}
    unstable = (
        max(
            float(silk_summary["relative_scaled_mad_pct"]),
            float(python_summary["relative_scaled_mad_pct"]),
        )
        > MAX_RELATIVE_SCALED_MAD_PCT
        or max(
            float(silk_summary["range_pct_of_mean"]),
            float(python_summary["range_pct_of_mean"]),
        )
        > MAX_RANGE_PCT
    )
    if unstable:
        result["reason"] = "measurement dispersion exceeds the decision limit"
    elif lower > PRACTICAL_SPEEDUP_PCT:
        result["winner"] = "silk"
        result["reason"] = (
            "the paired interval clears the five-percent decision boundary"
        )
    elif upper < -PRACTICAL_SPEEDUP_PCT:
        result["winner"] = "python"
        result["reason"] = (
            "the paired interval clears the five-percent decision boundary"
        )
    else:
        result["reason"] = "the paired interval does not clear the decision boundary"
    return result


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def run_monitored(
    command: Sequence[str],
    artifact_directory: Path,
    *,
    deadline_seconds: float,
    environment: dict[str, str] | None = None,
) -> ProcessResult:
    if deadline_seconds <= 0:
        raise ValueError("deadline_seconds must be positive")
    artifact_directory.mkdir(parents=True, exist_ok=True)
    metrics = artifact_directory / "gnu-time.txt"
    if metrics.exists():
        raise FileExistsError(metrics)
    measured_command = [
        "/usr/bin/time",
        "--output",
        str(metrics),
        "--format",
        "%U\n%S\n%M\n%x",
        "--",
        *command,
    ]
    started = time.perf_counter()
    process = subprocess.Popen(
        measured_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=deadline_seconds)
    except (KeyboardInterrupt, subprocess.TimeoutExpired):
        _terminate_process_group(process)
        process.communicate()
        raise
    wall_seconds = time.perf_counter() - started
    values = metrics.read_text().splitlines()
    if len(values) != 4:
        raise RuntimeError(f"GNU time returned an invalid metrics record: {values!r}")
    user_seconds, system_seconds = map(float, values[:2])
    maximum_rss_kib = int(values[2])
    measured_return_code = int(values[3])
    if measured_return_code != process.returncode:
        raise RuntimeError("GNU time and subprocess return codes disagree")
    return ProcessResult(
        command=tuple(command),
        return_code=process.returncode,
        wall_seconds=wall_seconds,
        cpu_seconds=user_seconds + system_seconds,
        peak_rss_bytes=maximum_rss_kib * 1024,
        stdout=stdout,
        stderr=stderr,
    )


def _run_json(command: Sequence[str]) -> object:
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def _run_text(command: Sequence[str]) -> str:
    return subprocess.run(
        command, check=True, capture_output=True, text=True
    ).stdout.strip()


def _validate_identifier(value: str, name: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} contains unsupported characters")
    return value


def verify_google_environment(
    project: str, expected_account: str | None
) -> dict[str, str]:
    active_account = _run_text(
        ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"]
    )
    configured_project = _run_text(["gcloud", "config", "get-value", "project"])
    if not active_account:
        raise RuntimeError("gcloud has no active account")
    if expected_account is not None and active_account != expected_account:
        raise RuntimeError(
            f"active gcloud account {active_account!r} does not match {expected_account!r}"
        )
    if configured_project != project:
        raise RuntimeError(
            f"configured gcloud project {configured_project!r} does not match {project!r}"
        )
    subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return {"active_account": active_account, "configured_project": configured_project}


class TemporaryFixture:
    def __init__(
        self,
        *,
        project: str,
        dataset: str,
        location: str,
        rows: int,
        maximum_bytes_billed: int,
        table_name: str,
    ) -> None:
        self.project = _validate_identifier(project, "project")
        self.dataset = _validate_identifier(dataset, "dataset")
        self.location = _validate_identifier(location, "location")
        self.rows = rows
        self.maximum_bytes_billed = maximum_bytes_billed
        self.table_name = _validate_identifier(table_name, "table")
        self.created = False

    @property
    def dotted_table(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table_name}"

    @property
    def resource(self) -> str:
        return (
            f"projects/{self.project}/datasets/{self.dataset}/tables/{self.table_name}"
        )

    def create(self) -> dict[str, object]:
        if self.rows < DEFAULT_ROWS:
            raise ValueError(f"live fixture requires at least {DEFAULT_ROWS:,} rows")
        if self.maximum_bytes_billed <= 0:
            raise ValueError("maximum_bytes_billed must be positive")
        _run_json(
            [
                "bq",
                f"--project_id={self.project}",
                f"--location={self.location}",
                "--format=prettyjson",
                "show",
                "--dataset",
                f"{self.project}:{self.dataset}",
            ]
        )
        sql = f"""
CREATE TABLE `{self.dotted_table}`
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR))
AS
WITH generated AS (
  SELECT block * 100000 + offset AS id
  FROM UNNEST(GENERATE_ARRAY(0, DIV({self.rows - 1}, 100000))) AS block
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, 99999)) AS offset
)
SELECT
  id,
  MOD(id, {_FIXTURE_COHORTS}) AS cohort,
  CAST(id * 1.25 AS FLOAT64) AS metric,
  TIMESTAMP_ADD(TIMESTAMP '2020-01-01 00:00:00+00', INTERVAL MOD(id, 31536000) SECOND) AS event_time,
  FORMAT('row-%012d', id) AS label,
  IF(MOD(id, 17) = 0, NULL, MOD(id * 31, 1000003)) AS nullable_value,
  REPEAT(FORMAT('%016x', id), 8) AS payload,
  REPEAT(FORMAT('ignored-%016x', id), 8) AS ignored_payload
FROM generated
WHERE id < {self.rows}
""".strip()
        command = [
            "bq",
            f"--project_id={self.project}",
            f"--location={self.location}",
            "query",
            "--nouse_legacy_sql",
            f"--maximum_bytes_billed={self.maximum_bytes_billed}",
            "--format=prettyjson",
            sql,
        ]
        self.created = True
        subprocess.run(command, check=True)
        metadata = self.metadata()
        if int(str(metadata["numRows"])) != self.rows:
            raise RuntimeError("BigQuery fixture row count does not match its request")
        return metadata

    def metadata(self) -> dict[str, object]:
        value = _run_json(
            [
                "bq",
                f"--project_id={self.project}",
                "--format=prettyjson",
                "show",
                f"{self.project}:{self.dataset}.{self.table_name}",
            ]
        )
        if not isinstance(value, dict):
            raise TypeError("BigQuery returned invalid table metadata")
        return value

    def server_snapshot(self) -> str:
        value = _run_json(
            [
                "bq",
                f"--project_id={self.project}",
                f"--location={self.location}",
                "query",
                "--nouse_legacy_sql",
                "--maximum_bytes_billed=1",
                "--format=prettyjson",
                "SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', CURRENT_TIMESTAMP()) AS snapshot",
            ]
        )
        if not isinstance(value, list) or len(value) != 1:
            raise RuntimeError("BigQuery returned an invalid server timestamp")
        snapshot = value[0].get("snapshot")
        if not isinstance(snapshot, str) or not snapshot.endswith("Z"):
            raise RuntimeError("BigQuery returned an invalid server timestamp")
        from google.protobuf.timestamp_pb2 import Timestamp

        timestamp = Timestamp()
        timestamp.FromJsonString(snapshot)
        return timestamp.ToJsonString()

    def cleanup(self) -> None:
        if not self.created:
            return
        completed = subprocess.run(
            [
                "bq",
                f"--project_id={self.project}",
                "rm",
                "-f",
                "-t",
                f"{self.project}:{self.dataset}.{self.table_name}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0 and "Not found" not in completed.stderr:
            raise RuntimeError(
                f"failed to remove owned BigQuery fixture: {completed.stderr.strip()}"
            )
        self.created = False


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_output(path: Path, contract: BenchmarkContract) -> ValidationResult:
    import duckdb
    import pyarrow as pa
    from pyarrow import ipc

    with path.open("rb") as source:
        reader = ipc.open_file(pa.PythonFile(source, mode="r"))
        schema = tuple(
            (field.name, str(field.type), field.nullable) for field in reader.schema
        )

        def batches() -> Iterator[object]:
            for index in range(reader.num_record_batches):
                yield reader.get_batch(index)

        stream = pa.RecordBatchReader.from_batches(reader.schema, batches())
        connection = duckdb.connect(config={"threads": "4", "memory_limit": "8GB"})
        connection.register("benchmark_output", stream)
        quoted = ", ".join(
            '"' + field.replace('"', '""') + '"' for field in contract.selected_fields
        )
        row = connection.execute(
            f"""
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT id) AS distinct_ids,
  MIN(id) AS minimum_id,
  MAX(id) AS maximum_id,
  CAST(SUM(id) AS VARCHAR) AS sum_id,
  CAST(BIT_XOR(HASH('silk-bqs-1', {quoted})) AS VARCHAR) AS xor_1,
  CAST(SUM(HASH('silk-bqs-1', {quoted})) AS VARCHAR) AS sum_1,
  CAST(BIT_XOR(HASH('silk-bqs-2', {quoted})) AS VARCHAR) AS xor_2,
  CAST(SUM(HASH('silk-bqs-2', {quoted})) AS VARCHAR) AS sum_2
FROM benchmark_output
"""
        ).fetchone()
    if row is None:
        raise RuntimeError("output validation returned no aggregate row")
    result = ValidationResult(
        rows=int(row[0]),
        distinct_ids=int(row[1]),
        minimum_id=int(row[2]),
        maximum_id=int(row[3]),
        sum_id=int(row[4]),
        schema=schema,
        digest=(str(row[5]), str(row[6]), str(row[7]), str(row[8])),
        file_sha256=_sha256_file(path),
        file_bytes=path.stat().st_size,
    )
    expected = expected_filtered_ids(
        contract.source_rows,
        _FIXTURE_COHORTS,
        _FIXTURE_SELECTED_COHORTS,
    )
    observed = {
        "rows": result.rows,
        "minimum": result.minimum_id,
        "maximum": result.maximum_id,
        "sum": result.sum_id,
    }
    if observed != expected or result.distinct_ids != expected["rows"]:
        raise RuntimeError(
            f"output row invariants differ: observed={observed}, expected={expected}, "
            f"distinct_ids={result.distinct_ids}"
        )
    if result.rows != contract.expected_rows:
        raise RuntimeError("output row count differs from the benchmark contract")
    if (
        tuple(name for name, _type, _nullable in result.schema)
        != contract.selected_fields
    ):
        raise RuntimeError("output schema fields differ from the benchmark projection")
    return result


def silk_command(binary: Path, output: Path, contract: BenchmarkContract) -> list[str]:
    reference = (
        f"bqs:///{contract.table}?snapshot={contract.snapshot}"
        f"&location={contract.location}"
    )
    projection = ", ".join(contract.selected_fields)
    return [
        str(binary),
        "transform",
        "--from",
        reference,
        "--to",
        str(output),
        "--output-format",
        "arrow",
        "--overwrite",
        "--query",
        f"SELECT {projection} FROM data",
        "--thread-budget",
        str(contract.requested_streams),
        "--target-partitions",
        str(contract.requested_streams),
        "--bqs-session-project",
        contract.project,
        "--bqs-quota-project",
        contract.project,
        "--bqs-row-restriction",
        contract.row_restriction,
        "--bqs-max-stream-count",
        str(contract.requested_streams),
        "--bqs-arrow-wire-compression",
        contract.wire_compression,
        "--bqs-response-compression",
        contract.response_compression,
        "--arrow-format",
        "file",
        "--arrow-compression",
        contract.output_compression,
    ]


def python_command(
    script: Path,
    output: Path,
    contract_path: Path,
) -> list[str]:
    return [
        sys.executable,
        str(script),
        "python-transfer",
        "--contract",
        str(contract_path),
        "--output",
        str(output),
    ]


def _load_contract(path: Path) -> BenchmarkContract:
    value = json.loads(path.read_text())
    value["selected_fields"] = tuple(value["selected_fields"])
    return BenchmarkContract(**value)


def _python_transfer(contract: BenchmarkContract, output: Path) -> dict[str, object]:
    import google.auth
    import pyarrow as pa
    from google.cloud.bigquery_storage import (
        ArrowSerializationOptions,
        BigQueryReadClient,
        DataFormat,
        ReadSession,
    )
    from google.protobuf.timestamp_pb2 import Timestamp

    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    credentials, adc_project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
        quota_project_id=contract.project,
    )
    client = BigQueryReadClient(credentials=credentials)
    try:
        requested = ReadSession(table=contract.table, data_format=DataFormat.ARROW)
        timestamp = Timestamp()
        timestamp.FromJsonString(contract.snapshot)
        requested.table_modifiers.snapshot_time = timestamp
        requested.read_options.selected_fields = list(contract.selected_fields)
        requested.read_options.row_restriction = contract.row_restriction
        requested.read_options.arrow_serialization_options.buffer_compression = (
            ArrowSerializationOptions.CompressionCodec.ZSTD
        )
        session = client.create_read_session(
            parent=f"projects/{contract.project}",
            read_session=requested,
            max_stream_count=contract.requested_streams,
        )
        returned_snapshot = None
        wire_session = ReadSession.pb(session)
        if wire_session.table_modifiers.HasField("snapshot_time"):
            returned_snapshot = (
                wire_session.table_modifiers.snapshot_time.ToJsonString()
            )
        if session.table != contract.table or returned_snapshot != contract.snapshot:
            raise RuntimeError(
                "Python session source differs from the benchmark contract"
            )
        if not session.streams:
            raise RuntimeError("BigQuery returned no Storage Read streams")
        schema = pa.ipc.read_schema(
            pa.py_buffer(session.arrow_schema.serialized_schema)
        )
        stream_queue: queue.Queue[object] = queue.Queue()
        for read_stream in session.streams:
            stream_queue.put(read_stream)
        batch_queue: queue.Queue[object] = queue.Queue(maxsize=100)
        stop = threading.Event()
        sentinel = object()
        errors: queue.Queue[BaseException] = queue.Queue()
        rows_written = 0
        active_lock = threading.Lock()
        active_reads: set[object] = set()

        def cancel_active_reads() -> None:
            with active_lock:
                reads = tuple(active_reads)
            for read_rows in reads:
                wrapped = getattr(read_rows, "_wrapped", None)
                cancel = getattr(wrapped, "cancel", None)
                if callable(cancel):
                    cancel()

        def read_worker() -> None:
            try:
                while not stop.is_set():
                    try:
                        read_stream = stream_queue.get_nowait()
                    except queue.Empty:
                        return
                    read_rows = client.read_rows(read_stream.name, timeout=900)
                    with active_lock:
                        active_reads.add(read_rows)
                    try:
                        for page in read_rows.rows(session).pages:
                            batch = page.to_arrow()
                            while not stop.is_set():
                                try:
                                    batch_queue.put(batch, timeout=0.1)
                                    break
                                except queue.Full:
                                    continue
                    finally:
                        with active_lock:
                            active_reads.discard(read_rows)
            except BaseException as error:  # noqa: BLE001
                errors.put(error)
                stop.set()
                cancel_active_reads()

        def write_worker() -> None:
            nonlocal rows_written
            try:
                with (
                    output.open("xb") as destination,
                    pa.ipc.new_file(
                        destination,
                        schema,
                        options=pa.ipc.IpcWriteOptions(compression="zstd"),
                    ) as writer,
                ):
                    while True:
                        batch = batch_queue.get()
                        if batch is sentinel:
                            return
                        writer.write_batch(batch)
                        rows_written += batch.num_rows
            except BaseException as error:  # noqa: BLE001
                errors.put(error)
                stop.set()
                cancel_active_reads()

        writer = threading.Thread(target=write_worker, name="arrow-writer", daemon=True)
        readers = [
            threading.Thread(
                target=read_worker, name=f"read-stream-{index}", daemon=True
            )
            for index in range(min(contract.requested_streams, len(session.streams)))
        ]
        writer.start()
        for reader in readers:
            reader.start()
        for reader in readers:
            reader.join()
        while writer.is_alive():
            try:
                batch_queue.put(sentinel, timeout=0.1)
                break
            except queue.Full:
                if stop.is_set():
                    try:
                        batch_queue.get_nowait()
                    except queue.Empty:
                        pass
        writer.join()
        if not errors.empty():
            raise errors.get()
        if rows_written != contract.expected_rows:
            raise RuntimeError(
                f"Python wrote {rows_written} rows, expected {contract.expected_rows}"
            )
        return {
            "adc_project": adc_project,
            "returned_streams": len(session.streams),
            "rows": rows_written,
        }
    except BaseException:
        output.unlink(missing_ok=True)
        raise
    finally:
        client.transport.close()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    temporary.replace(path)


def provenance(binary: Path) -> dict[str, object]:
    repository = Path(__file__).resolve().parent.parent

    def git(*arguments: str) -> str:
        return _run_text(["git", "-C", str(repository), *arguments])

    production_diff = subprocess.run(
        [
            "git",
            "-C",
            str(repository),
            "diff",
            "--quiet",
            "HEAD",
            "--",
            "Cargo.toml",
            "Cargo.lock",
            "crates",
            "src",
        ],
        check=False,
    )
    dependencies = {
        package: importlib.metadata.version(package)
        for package in [
            "google-cloud-bigquery-storage",
            "google-api-core",
            "google-auth",
            "grpcio",
            "protobuf",
            "pyarrow",
            "duckdb",
            "psutil",
        ]
    }
    return {
        "silk_revision": git("rev-parse", "HEAD"),
        "silk_tree": git("rev-parse", "HEAD^{tree}"),
        "silk_production_sources_match_head": production_diff.returncode == 0,
        "silk_binary_sha256": _sha256_file(binary),
        "benchmark_script_sha256": _sha256_file(Path(__file__).resolve()),
        "python": platform.python_version(),
        "python_dependencies": dependencies,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
    }


def _run_client(
    client: str,
    *,
    pair_directory: Path,
    binary: Path,
    script: Path,
    contract: BenchmarkContract,
    contract_path: Path,
    deadline_seconds: float,
) -> dict[str, object]:
    client_directory = pair_directory / client
    output = client_directory / "data.arrow"
    command = (
        silk_command(binary, output, contract)
        if client == "silk"
        else python_command(script, output, contract_path)
    )
    process = run_monitored(
        command,
        client_directory,
        deadline_seconds=deadline_seconds,
        environment=os.environ.copy(),
    )
    if process.return_code != 0:
        raise RuntimeError(
            f"{client} failed with {process.return_code}:\n{process.stderr[-4000:]}"
        )
    validation = validate_output(output, contract)
    result = {
        "client": client,
        "process": asdict(process),
        "validation": asdict(validation),
    }
    _write_json(client_directory / "result.json", result)
    output.unlink()
    return result


def run_campaign(args: argparse.Namespace) -> dict[str, object]:
    if not args.allow_live_costs:
        raise RuntimeError("live benchmark costs require --allow-live-costs")
    if args.pairs <= 0:
        raise ValueError("pairs must be positive")
    if args.rows < DEFAULT_ROWS:
        raise ValueError(f"rows must be at least {DEFAULT_ROWS:,}")
    environment = verify_google_environment(args.project, args.expected_account)
    binary = args.binary.resolve()
    if not binary.is_file():
        raise FileNotFoundError(binary)
    result_path = args.result.resolve()
    table_name = f"silk_bqs_bench_{int(time.time())}_{os.getpid()}"
    fixture = TemporaryFixture(
        project=args.project,
        dataset=args.dataset,
        location=args.location,
        rows=args.rows,
        maximum_bytes_billed=args.maximum_bytes_billed,
        table_name=table_name,
    )
    script = Path(__file__).resolve()
    started = time.time_ns()
    try:
        metadata = fixture.create()
        snapshot = fixture.server_snapshot()
        expected = expected_filtered_ids(
            args.rows,
            _FIXTURE_COHORTS,
            _FIXTURE_SELECTED_COHORTS,
        )
        contract = BenchmarkContract(
            project=args.project,
            table=fixture.resource,
            location=args.location,
            snapshot=snapshot,
            source_rows=args.rows,
            selected_fields=DEFAULT_SELECTED_FIELDS,
            row_restriction=DEFAULT_ROW_RESTRICTION,
            expected_rows=expected["rows"],
            requested_streams=args.streams,
        )
        with tempfile.TemporaryDirectory(prefix="silk-bqs-benchmark-") as directory:
            root = Path(directory)
            contract_path = root / "contract.json"
            _write_json(contract_path, asdict(contract))
            pairs = []
            for index in range(args.pairs):
                pair_directory = root / f"pair-{index + 1:02}"
                order = pair_order(index)
                print(
                    f"pair {index + 1}/{args.pairs}: {' then '.join(order)}", flush=True
                )
                clients = {}
                for client in order:
                    clients[client] = _run_client(
                        client,
                        pair_directory=pair_directory,
                        binary=binary,
                        script=script,
                        contract=contract,
                        contract_path=contract_path,
                        deadline_seconds=args.deadline_seconds,
                    )
                silk_validation = clients["silk"]["validation"]
                python_validation = clients["python"]["validation"]
                if (
                    silk_validation["schema"] != python_validation["schema"]
                    or silk_validation["digest"] != python_validation["digest"]
                ):
                    raise RuntimeError("Silk and Python outputs differ logically")
                pairs.append(
                    {
                        "number": index + 1,
                        "order": order,
                        "clients": clients,
                    }
                )
            silk_seconds = [
                pair["clients"]["silk"]["process"]["wall_seconds"] for pair in pairs
            ]
            python_seconds = [
                pair["clients"]["python"]["process"]["wall_seconds"] for pair in pairs
            ]
            comparison = summarize_paired_seconds(
                silk=silk_seconds,
                python=python_seconds,
                seed=contract.identity_sha256(),
            )
            fixture.cleanup()
            result: dict[str, object] = {
                "schema": "silk-chiffon.bqs-performance-campaign.v1",
                "started_unix_ns": started,
                "finished_unix_ns": time.time_ns(),
                "environment": environment,
                "provenance": provenance(binary),
                "fixture": {
                    "table": fixture.resource,
                    "rows": int(metadata["numRows"]),
                    "logical_bytes": int(str(metadata["numBytes"])),
                    "expiration_time_ms": int(str(metadata["expirationTime"])),
                    "owned_and_removed": True,
                },
                "contract": {
                    **asdict(contract),
                    "identity_sha256": contract.identity_sha256(),
                },
                "client_profiles": {
                    "silk": {
                        "interface": "silk-chiffon transform",
                        "arrow_record_batch_rows": 122_880,
                        "arrow_writer_queue_items": 16,
                        "record_batch_policy": "coalesced",
                    },
                    "python": {
                        "interface": "BigQueryReadClient",
                        "arrow_writer_queue_items": 100,
                        "record_batch_policy": "native-storage-pages",
                    },
                },
                "pairs": pairs,
                "comparison": comparison,
            }
            _write_json(result_path, result)
            return result
    finally:
        fixture.cleanup()


def _campaign_parser(subparsers: argparse._SubParsersAction[Any]) -> None:
    campaign = subparsers.add_parser("campaign")
    campaign.add_argument(
        "--project",
        default=os.environ.get("SILK_CHIFFON_BQS_BENCH_PROJECT"),
        required=os.environ.get("SILK_CHIFFON_BQS_BENCH_PROJECT") is None,
    )
    campaign.add_argument(
        "--dataset",
        default=os.environ.get("SILK_CHIFFON_BQS_BENCH_DATASET"),
        required=os.environ.get("SILK_CHIFFON_BQS_BENCH_DATASET") is None,
    )
    campaign.add_argument(
        "--location",
        default=os.environ.get("SILK_CHIFFON_BQS_BENCH_LOCATION", "us-central1"),
    )
    campaign.add_argument(
        "--expected-account",
        default=os.environ.get("SILK_CHIFFON_BQS_BENCH_ACCOUNT"),
    )
    campaign.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    campaign.add_argument("--streams", type=int, default=DEFAULT_STREAMS)
    campaign.add_argument("--pairs", type=int, default=MINIMUM_DECISION_PAIRS)
    campaign.add_argument(
        "--maximum-bytes-billed",
        type=int,
        default=DEFAULT_MAXIMUM_BYTES_BILLED,
    )
    campaign.add_argument(
        "--deadline-seconds", type=float, default=DEFAULT_DEADLINE_SECONDS
    )
    campaign.add_argument(
        "--binary",
        type=Path,
        default=Path("target/native/silk-chiffon"),
    )
    campaign.add_argument(
        "--result",
        type=Path,
        default=Path("target/bqs-benchmark/campaign.json"),
    )
    campaign.add_argument("--allow-live-costs", action="store_true")


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    subparsers = command.add_subparsers(dest="command", required=True)
    _campaign_parser(subparsers)
    transfer = subparsers.add_parser("python-transfer", help=argparse.SUPPRESS)
    transfer.add_argument("--contract", type=Path, required=True)
    transfer.add_argument("--output", type=Path, required=True)
    return command


def main() -> None:
    args = parser().parse_args()
    if args.command == "python-transfer":
        result = _python_transfer(_load_contract(args.contract), args.output)
        print(json.dumps(result, sort_keys=True))
        return
    result = run_campaign(args)
    print(json.dumps(result["comparison"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
