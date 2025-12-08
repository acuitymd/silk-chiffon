use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::{FileWriter, StreamWriter};
use assert_cmd::cargo;
use predicates::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn simple_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn create_batch(schema: &Arc<Schema>, ids: &[i32], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn write_arrow_file(path: &Path, schema: &Arc<Schema>, batches: Vec<RecordBatch>) {
    let file = File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, schema).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
}

fn write_arrow_stream(path: &Path, schema: &Arc<Schema>, batches: Vec<RecordBatch>) {
    let file = File::create(path).unwrap();
    let mut writer = StreamWriter::try_new(file, schema).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
}

#[test]
fn test_inspect_help() {
    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Inspect file metadata"));
}

#[test]
fn test_inspect_missing_file_arg() {
    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.arg("inspect")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required").or(predicate::str::contains("Usage:")));
}

#[test]
fn test_inspect_nonexistent_file() {
    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "/nonexistent/path/file.arrow"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("failed to inspect file"));
}

#[test]
fn test_inspect_arrow_file() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_path = temp_dir.path().join("test.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    write_arrow_file(&arrow_path, &schema, vec![batch]);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", arrow_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Format: Arrow IPC (file)"))
        .stdout(predicate::str::contains("Rows: 3"))
        .stdout(predicate::str::contains("Schema (2 fields)"))
        .stdout(predicate::str::contains("id"))
        .stdout(predicate::str::contains("name"))
        .stdout(predicate::str::contains("Int32"))
        .stdout(predicate::str::contains("Utf8"));
}

#[test]
fn test_inspect_arrow_stream() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_path = temp_dir.path().join("test.arrows");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    write_arrow_stream(&arrow_path, &schema, vec![batch]);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", arrow_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Format: Arrow IPC (stream)"))
        .stdout(predicate::str::contains("Rows: 5"))
        .stdout(predicate::str::contains("Schema (2 fields)"));
}

#[test]
fn test_inspect_arrow_file_multiple_batches() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_path = temp_dir.path().join("multi_batch.arrow");

    let schema = simple_schema();
    let batch1 = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    let batch2 = create_batch(&schema, &[4, 5], &["d", "e"]);
    let batch3 = create_batch(&schema, &[6, 7, 8, 9], &["f", "g", "h", "i"]);
    write_arrow_file(&arrow_path, &schema, vec![batch1, batch2, batch3]);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", arrow_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Rows: 9"));
}

#[test]
fn test_inspect_arrow_stream_multiple_batches() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_path = temp_dir.path().join("multi_batch.arrows");

    let schema = simple_schema();
    let batch1 = create_batch(&schema, &[1, 2], &["a", "b"]);
    let batch2 = create_batch(&schema, &[3, 4, 5, 6], &["c", "d", "e", "f"]);
    write_arrow_stream(&arrow_path, &schema, vec![batch1, batch2]);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", arrow_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Rows: 6"));
}

#[test]
fn test_inspect_empty_arrow_file() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_path = temp_dir.path().join("empty.arrow");

    let schema = simple_schema();
    write_arrow_file(&arrow_path, &schema, vec![]);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", arrow_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Format: Arrow IPC (file)"))
        .stdout(predicate::str::contains("Rows: 0"))
        .stdout(predicate::str::contains("Schema (2 fields)"));
}

#[test]
fn test_inspect_nullable_fields() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_path = temp_dir.path().join("nullable.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("required_field", DataType::Int32, false),
        Field::new("nullable_field", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    )
    .unwrap();

    write_arrow_file(&arrow_path, &schema, vec![batch]);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", arrow_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("(not null)"))
        .stdout(predicate::str::contains("(nullable)"));
}

#[test]
fn test_inspect_invalid_file() {
    let temp_dir = TempDir::new().unwrap();
    let invalid_path = temp_dir.path().join("invalid.arrow");

    std::fs::write(&invalid_path, b"this is not an arrow file").unwrap();

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", invalid_path.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("failed to inspect file"));
}
