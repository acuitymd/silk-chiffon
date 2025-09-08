use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

#[test]
fn test_help_command() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Convert Arrow format to Parquet format",
        ))
        .stdout(predicate::str::contains(
            "Convert Arrow format to DuckDB format",
        ))
        .stdout(predicate::str::contains(
            "Convert Arrow format to Arrow format",
        ))
        .stdout(predicate::str::contains(
            "Partition Arrow data into multiple Arrow files",
        ))
        .stdout(predicate::str::contains(
            "Partition Arrow data into multiple Parquet files",
        ));
}

#[test]
fn test_version_command() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("silk_chiffon"));
}

#[test]
fn test_parquet_help() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args(["arrow-to-parquet", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Convert Arrow format to Parquet format.",
        ))
        .stdout(predicate::str::contains(
            "Sort the data by one or more columns",
        ))
        .stdout(predicate::str::contains("compression"));
}

#[test]
fn test_duckdb_help() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args(["arrow-to-duckdb", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Convert Arrow format to DuckDB format.",
        ))
        .stdout(predicate::str::contains(
            "Sort the data by one or more columns",
        ));
}

#[test]
fn test_arrow_help() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args(["arrow-to-arrow", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Convert Arrow format to Arrow format.",
        ))
        .stdout(predicate::str::contains(
            "Sort the data by one or more columns",
        ));
}

#[test]
fn test_missing_subcommand() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Usage:"));
}

#[test]
fn test_parquet_missing_args() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.arg("arrow-to-parquet")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_duckdb_missing_args() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.arg("arrow-to-duckdb")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_arrow_missing_args() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.arg("arrow-to-arrow")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_parquet_non_existent_input() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.parquet");

    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args([
        "arrow-to-parquet",
        "/non/existent/file.arrow",
        output_path.to_str().unwrap(),
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("No such file or directory"));
}

#[test]
fn test_arrow_non_existent_input() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.arrow");

    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args([
        "arrow-to-arrow",
        "/non/existent/file.arrow",
        output_path.to_str().unwrap(),
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("No such file or directory"));
}

#[test]
fn test_duckdb_non_existent_input() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.db");

    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args([
        "arrow-to-duckdb",
        "/non/existent/file.arrow",
        output_path.to_str().unwrap(),
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("No such file or directory"));
}

#[test]
fn test_partition_arrow_to_arrow_help() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args(["partition-arrow-to-arrow", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Partition Arrow data into multiple Arrow files",
        ))
        .stdout(predicate::str::contains("Column to partition by"));
}

#[test]
fn test_partition_arrow_to_parquet_help() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args(["partition-arrow-to-parquet", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Partition Arrow data into multiple Parquet files",
        ))
        .stdout(predicate::str::contains("Column to partition by"));
}

#[test]
fn test_partition_arrow_to_arrow_missing_args() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.arg("partition-arrow-to-arrow")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_partition_arrow_to_parquet_missing_args() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.arg("partition-arrow-to-parquet")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_partition_arrow_to_arrow_non_existent_input() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args([
        "partition-arrow-to-arrow",
        "/non/existent/file.arrow",
        "--by",
        "category",
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("No such file or directory"));
}

#[test]
fn test_partition_arrow_to_parquet_non_existent_input() {
    let mut cmd = Command::cargo_bin("silk-chiffon").unwrap();
    cmd.args([
        "partition-arrow-to-parquet",
        "/non/existent/file.arrow",
        "--by",
        "category",
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("No such file or directory"));
}
