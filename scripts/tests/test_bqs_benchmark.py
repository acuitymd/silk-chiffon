import importlib.util
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "bqs_benchmark",
    ROOT / "scripts" / "bqs_benchmark.py",
)
assert SPEC is not None and SPEC.loader is not None
bqs_benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bqs_benchmark
SPEC.loader.exec_module(bqs_benchmark)


class BenchmarkContractTests(unittest.TestCase):
    def contract(self) -> object:
        return bqs_benchmark.BenchmarkContract(
            project="example-project",
            table="projects/example-project/datasets/data/tables/fixture",
            location="us-central1",
            snapshot="2026-08-16T12:00:00Z",
            source_rows=5_000_000,
            selected_fields=("id", "cohort", "payload"),
            row_restriction="cohort < 896",
            expected_rows=4_375_000,
            requested_streams=12,
        )

    def test_contract_requires_a_complete_matched_transfer_profile(self) -> None:
        contract = self.contract()

        self.assertEqual(contract.wire_compression, "zstd")
        self.assertEqual(contract.output_compression, "zstd")
        self.assertEqual(
            contract.measurement_scope, "process-start-through-writer-close"
        )
        self.assertEqual(len(contract.identity_sha256()), 64)

    def test_silk_command_applies_every_matched_setting_explicitly(self) -> None:
        command = bqs_benchmark.silk_command(
            Path("/tmp/silk-chiffon"),
            Path("/tmp/output.arrow"),
            self.contract(),
        )

        for flag, value in [
            ("--thread-budget", "12"),
            ("--target-partitions", "12"),
            ("--bqs-max-stream-count", "12"),
            ("--bqs-arrow-wire-compression", "zstd"),
            ("--bqs-response-compression", "none"),
            ("--bqs-row-restriction", "cohort < 896"),
            ("--arrow-format", "file"),
            ("--arrow-compression", "zstd"),
        ]:
            with self.subTest(flag=flag):
                index = command.index(flag)
                self.assertEqual(command[index + 1], value)
        self.assertIn("SELECT id, cohort, payload FROM data", command)
        self.assertTrue(
            command[command.index("--from") + 1].endswith(
                "?snapshot=2026-08-16T12:00:00Z&location=us-central1"
            )
        )

    def test_contract_rejects_invalid_or_ambiguous_values(self) -> None:
        valid = {
            "project": "example-project",
            "table": "projects/example-project/datasets/data/tables/fixture",
            "location": "us-central1",
            "snapshot": "2026-08-16T12:00:00Z",
            "source_rows": 5_000_000,
            "selected_fields": ("id",),
            "row_restriction": "cohort < 896",
            "expected_rows": 4_375_000,
            "requested_streams": 12,
        }
        for name, value in [
            ("selected_fields", ()),
            ("row_restriction", ""),
            ("expected_rows", 0),
            ("requested_streams", 0),
        ]:
            invalid = {**valid, name: value}
            with self.subTest(name=name), self.assertRaises(ValueError):
                bqs_benchmark.BenchmarkContract(**invalid)


class CampaignTests(unittest.TestCase):
    def test_pair_order_is_counterbalanced(self) -> None:
        self.assertEqual(
            [bqs_benchmark.pair_order(index) for index in range(4)],
            [
                ("silk", "python"),
                ("python", "silk"),
                ("silk", "python"),
                ("python", "silk"),
            ],
        )

    def test_winner_requires_enough_stable_paired_evidence(self) -> None:
        decisive = bqs_benchmark.summarize_paired_seconds(
            silk=[8.0, 8.2, 7.9, 8.1, 8.0, 8.1, 7.8, 8.2, 7.9, 8.0],
            python=[10.0, 10.2, 10.1, 10.3, 9.9, 10.1, 10.0, 10.2, 9.8, 10.1],
            seed="decisive",
        )
        too_few = bqs_benchmark.summarize_paired_seconds(
            silk=[8.0, 8.1],
            python=[10.0, 10.1],
            seed="too-few",
        )

        self.assertEqual(decisive["winner"], "silk")
        self.assertIsNone(too_few["winner"])
        self.assertIn("at least 10", too_few["reason"])

    def test_logical_validation_is_independent_of_batch_and_row_order(self) -> None:
        rows = [
            {"id": 2, "label": "two"},
            {"id": 1, "label": "one"},
            {"id": 3, "label": None},
        ]
        reordered = [rows[2], rows[0], rows[1]]

        self.assertEqual(
            bqs_benchmark.logical_digest(rows, ("id", "label")),
            bqs_benchmark.logical_digest(reordered, ("id", "label")),
        )

    def test_fixture_expectations_handle_partial_cohorts(self) -> None:
        self.assertEqual(
            bqs_benchmark.expected_filtered_ids(2_050, 1_024, 896),
            {
                "rows": 1_794,
                "minimum": 0,
                "maximum": 2_049,
                "sum": sum(value for value in range(2_050) if value % 1_024 < 896),
            },
        )


class ProcessTests(unittest.TestCase):
    def test_monitored_process_records_child_resource_usage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = bqs_benchmark.run_monitored(
                ["python3", "-c", "print(sum(range(10000000)))"],
                Path(directory),
                deadline_seconds=10,
            )

        self.assertEqual(result.return_code, 0)
        self.assertGreater(result.wall_seconds, 0)
        self.assertGreater(result.cpu_seconds, 0)
        self.assertGreater(result.peak_rss_bytes, 0)
        self.assertIn("49999995000000", result.stdout)

    def test_timeout_terminates_the_child_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child_pid = root / "child.pid"
            command = [
                "python3",
                "-c",
                (
                    "import pathlib, subprocess, sys, time; "
                    "child = subprocess.Popen(['sleep', '30']); "
                    "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); "
                    "time.sleep(30)"
                ),
                str(child_pid),
            ]
            started = time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                bqs_benchmark.run_monitored(
                    command,
                    root / "artifacts",
                    deadline_seconds=0.2,
                )
            self.assertLess(time.monotonic() - started, 3)
            pid = int(child_pid.read_text())
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                stat = Path(f"/proc/{pid}/stat")
                if not stat.exists() or stat.read_text().split()[2] == "Z":
                    break
                time.sleep(0.01)
            else:
                self.fail("timed-out descendant remained alive")


if __name__ == "__main__":
    unittest.main()
