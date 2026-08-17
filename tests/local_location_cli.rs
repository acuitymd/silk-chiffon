use assert_cmd::cargo;
use predicates::prelude::*;
use silk_chiffon::utils::test_data::{TestBatch, TestFile};
use tempfile::TempDir;
use url::Url;

#[test]
fn transform_accepts_local_file_urls_and_creates_parent_directories() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("nested/output.parquet");
    let input_url = Url::from_file_path(&input).unwrap().to_string();
    let output_url = Url::from_file_path(&output).unwrap().to_string();

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            &input_url,
            "--to",
            &output_url,
            "--create-dirs",
        ])
        .assert()
        .success();

    assert!(output.exists());
}

#[test]
fn inspect_accepts_a_local_file_url() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let input_url = Url::from_file_path(&input).unwrap().to_string();

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args(["detect", &input_url, "--format", "text"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Arrow IPC"));
}

#[test]
fn inspect_rejects_noncanonical_file_urls() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let canonical_url = Url::from_file_path(&input).unwrap().to_string();

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    for invalid in [
        canonical_url.replacen("file:///", "file:/", 1),
        canonical_url.replacen("file:///", "file://localhost/", 1),
        canonical_url.replacen("file:///", "FILE:///", 1),
        canonical_url.replacen("file:///", "file:////", 1),
    ] {
        cargo::cargo_bin_cmd!("silk-chiffon")
            .args(["detect", &invalid, "--format", "text"])
            .assert()
            .failure();
    }
}
