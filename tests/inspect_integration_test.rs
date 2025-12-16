//! Integration tests for the inspect command.
//!
//! Creates temp files in each format and verifies the inspect command works correctly.

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::{FileWriter, StreamWriter};
use assert_cmd::cargo::cargo_bin_cmd;
use parquet::arrow::ArrowWriter;
use predicates::prelude::*;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

mod test_helpers {
    use super::*;

    pub fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    pub fn create_batch(schema: &SchemaRef, ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn write_parquet_file(
        path: &std::path::Path,
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) {
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), None).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
    }

    pub fn write_arrow_file(path: &std::path::Path, schema: &SchemaRef, batches: Vec<RecordBatch>) {
        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, schema).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }

    pub fn write_arrow_stream(
        path: &std::path::Path,
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) {
        let file = File::create(path).unwrap();
        let mut writer = StreamWriter::try_new(file, schema).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }
}

// =============================================================================
// Parquet inspection tests
// =============================================================================

#[test]
fn test_inspect_parquet_default() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "parquet",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Parquet"))
    .stdout(predicate::str::contains("Rows:"))
    .stdout(predicate::str::contains("Columns"));
}

#[test]
fn test_inspect_parquet_schema() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "parquet",
        file.to_str().unwrap(),
        "--schema",
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Schema"))
    .stdout(predicate::str::contains("id"))
    .stdout(predicate::str::contains("name"));
}

#[test]
fn test_inspect_parquet_stats() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "parquet",
        file.to_str().unwrap(),
        "--stats",
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Column Statistics"));
}

#[test]
fn test_inspect_parquet_row_groups() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "parquet",
        file.to_str().unwrap(),
        "--row-groups",
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Row Group"));
}

#[test]
fn test_inspect_parquet_json() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "parquet",
        file.to_str().unwrap(),
        "--format",
        "json",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"format\":"))
    .stdout(predicate::str::contains("\"rows\":"));
}

// =============================================================================
// Arrow IPC file inspection tests
// =============================================================================

#[test]
fn test_inspect_arrow_file_default() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "arrow",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Arrow IPC"))
    .stdout(predicate::str::contains("file"))
    .stdout(predicate::str::contains("Rows:"))
    .stdout(predicate::str::contains("Columns"));
}

#[test]
fn test_inspect_arrow_file_schema() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "arrow",
        file.to_str().unwrap(),
        "--schema",
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Schema"))
    .stdout(predicate::str::contains("id"))
    .stdout(predicate::str::contains("name"));
}

#[test]
fn test_inspect_arrow_file_batches() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.arrow");

    let schema = test_helpers::simple_schema();
    let batch1 = test_helpers::create_batch(&schema, &[1, 2], &["a", "b"]);
    let batch2 = test_helpers::create_batch(&schema, &[3, 4], &["c", "d"]);
    test_helpers::write_arrow_file(&file, &schema, vec![batch1, batch2]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "arrow",
        file.to_str().unwrap(),
        "--batches",
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Record Batches"));
}

#[test]
fn test_inspect_arrow_file_json() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "arrow",
        file.to_str().unwrap(),
        "--format",
        "json",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"format\":"))
    .stdout(predicate::str::contains("\"rows\":"));
}

// =============================================================================
// Arrow IPC stream inspection tests
// =============================================================================

#[test]
fn test_inspect_arrow_stream_default() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.stream.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_stream(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "arrow",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Arrow IPC"))
    .stdout(predicate::str::contains("stream"))
    .stdout(predicate::str::contains("Columns"));
}

#[test]
fn test_inspect_arrow_stream_row_count() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.stream.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_stream(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "arrow",
        file.to_str().unwrap(),
        "--row-count",
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Rows:"));
}

#[test]
fn test_inspect_arrow_stream_json() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.stream.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_stream(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "arrow",
        file.to_str().unwrap(),
        "--format",
        "json",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"format\":"));
}

// =============================================================================
// Vortex file inspection tests
// =============================================================================

mod vortex_tests {
    use super::*;
    use silk_chiffon::sinks::data_sink::DataSink;
    use silk_chiffon::sinks::vortex::{VortexSink, VortexSinkOptions};

    fn write_vortex_file(path: &std::path::Path, schema: &SchemaRef, batches: Vec<RecordBatch>) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sink =
                VortexSink::create(path.to_path_buf(), schema, VortexSinkOptions::new()).unwrap();
            for batch in batches {
                sink.write_batch(batch).await.unwrap();
            }
            sink.finish().await.unwrap();
        });
    }

    // =========================================================================
    // Vortex file tests
    // =========================================================================

    #[test]
    fn test_inspect_vortex_file_default() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.vortex");

        let schema = test_helpers::simple_schema();
        let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
        write_vortex_file(&file, &schema, vec![batch]);

        let mut cmd = cargo_bin_cmd!("silk-chiffon");
        cmd.args([
            "inspect",
            "vortex",
            file.to_str().unwrap(),
            "--format",
            "text",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Vortex"))
        .stdout(predicate::str::contains("Rows:"))
        .stdout(predicate::str::contains("Columns"));
    }

    #[test]
    fn test_inspect_vortex_file_schema() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.vortex");

        let schema = test_helpers::simple_schema();
        let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
        write_vortex_file(&file, &schema, vec![batch]);

        let mut cmd = cargo_bin_cmd!("silk-chiffon");
        cmd.args([
            "inspect",
            "vortex",
            file.to_str().unwrap(),
            "--schema",
            "--format",
            "text",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Schema"))
        .stdout(predicate::str::contains("id"))
        .stdout(predicate::str::contains("name"));
    }

    #[test]
    fn test_inspect_vortex_file_json() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.vortex");

        let schema = test_helpers::simple_schema();
        let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
        write_vortex_file(&file, &schema, vec![batch]);

        let mut cmd = cargo_bin_cmd!("silk-chiffon");
        cmd.args([
            "inspect",
            "vortex",
            file.to_str().unwrap(),
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"format\":"))
        .stdout(predicate::str::contains("\"rows\":"));
    }
}

// =============================================================================
// Identify subcommand tests
// =============================================================================

#[test]
fn test_inspect_identify_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "identify",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Parquet"));
}

#[test]
fn test_inspect_identify_arrow_file() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "identify",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Arrow IPC"))
    .stdout(predicate::str::contains("file"));
}

#[test]
fn test_inspect_identify_arrow_stream() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.stream.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_stream(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "identify",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Arrow IPC"))
    .stdout(predicate::str::contains("stream"));
}

#[test]
fn test_inspect_identify_vortex_file() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.vortex");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

    use silk_chiffon::sinks::data_sink::DataSink;
    use silk_chiffon::sinks::vortex::{VortexSink, VortexSinkOptions};

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut sink =
            VortexSink::create(file.to_path_buf(), &schema, VortexSinkOptions::new()).unwrap();
        sink.write_batch(batch).await.unwrap();
        sink.finish().await.unwrap();
    });

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "identify",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Vortex"))
    .stdout(predicate::str::contains("file"));
}

#[test]
fn test_inspect_identify_json_output() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&file, &schema, vec![batch]);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "identify",
        file.to_str().unwrap(),
        "--format",
        "json",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"format\":"));
}

#[test]
fn test_inspect_identify_unknown_file() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.txt");
    std::fs::write(&file, "hello world").unwrap();

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "inspect",
        "identify",
        file.to_str().unwrap(),
        "--format",
        "text",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("Unknown"));
}

// =============================================================================
// Error handling tests
// =============================================================================

#[test]
fn test_inspect_parquet_nonexistent_file() {
    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "parquet", "/nonexistent/path/file.parquet"])
        .assert()
        .failure();
}

#[test]
fn test_inspect_arrow_nonexistent_file() {
    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "arrow", "/nonexistent/path/file.arrow"])
        .assert()
        .failure();
}

#[test]
fn test_inspect_vortex_nonexistent_file() {
    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "vortex", "/nonexistent/path/file.vortex"])
        .assert()
        .failure();
}

#[test]
fn test_inspect_parquet_invalid_file() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("invalid.parquet");
    std::fs::write(&file, "not a parquet file").unwrap();

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "parquet", file.to_str().unwrap()])
        .assert()
        .failure();
}

#[test]
fn test_inspect_arrow_invalid_file() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("invalid.arrow");
    std::fs::write(&file, "not an arrow file").unwrap();

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "arrow", file.to_str().unwrap()])
        .assert()
        .failure();
}
