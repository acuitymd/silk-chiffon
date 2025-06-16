use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

#[test]
fn test_help_command() {
    let mut cmd = Command::cargo_bin("daisy").unwrap();
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
        ));
}

#[test]
fn test_version_command() {
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("daisy"));
}

#[test]
fn test_parquet_help() {
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.args(["parquet", "--help"])
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
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.args(["duckdb", "--help"])
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
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.args(["arrow", "--help"])
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
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Usage:"));
}

#[test]
fn test_parquet_missing_args() {
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.arg("parquet")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_duckdb_missing_args() {
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.arg("duckdb")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_arrow_missing_args() {
    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.arg("arrow")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_parquet_non_existent_input() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.parquet");

    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.args([
        "parquet",
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

    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.args([
        "arrow",
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

    let mut cmd = Command::cargo_bin("daisy").unwrap();
    cmd.args([
        "duckdb",
        "/non/existent/file.arrow",
        output_path.to_str().unwrap(),
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("No such file or directory"));
}
