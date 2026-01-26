//! Integration tests for the inspect command.
//!
//! Creates temp files in each format and verifies the inspect command works correctly.

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use silk_chiffon::utils::test_data::{TestBatch, TestFile};
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

// =============================================================================
// Parquet inspection tests
// =============================================================================

#[test]
fn test_inspect_parquet_default() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&file, &batch);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "parquet", file.to_str().unwrap(), "-f", "text"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Parquet"))
        .stdout(predicate::str::contains("Rows"))
        .stdout(predicate::str::contains("Columns"));
}

#[test]
fn test_inspect_parquet_schema() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&file, &batch);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "parquet", file.to_str().unwrap(), "-f", "text"])
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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&file, &batch);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "parquet", file.to_str().unwrap(), "-f", "text"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Column Statistics"));
}

#[test]
fn test_inspect_parquet_row_groups() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&file, &batch);

    let mut cmd = cargo_bin_cmd!("silk-chiffon");
    cmd.args(["inspect", "parquet", file.to_str().unwrap(), "-f", "text"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Row Group"));
}

#[test]
fn test_inspect_parquet_json() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&file, &batch);

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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&file, &batch);

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
    .stdout(predicate::str::contains("Rows"))
    .stdout(predicate::str::contains("Columns"));
}

#[test]
fn test_inspect_arrow_file_batches() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.arrow");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
    TestFile::write_arrow(&file, &[batch1, batch2]);

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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&file, &batch);

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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_stream(&file, &[batch]);

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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_stream(&file, &[batch]);

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
    .stdout(predicate::str::contains("Rows"));
}

#[test]
fn test_inspect_arrow_stream_json() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("test.stream.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_stream(&file, &[batch]);

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

        let schema = TestBatch::simple_schema();
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
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

        let schema = TestBatch::simple_schema();
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
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

        let schema = TestBatch::simple_schema();
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&file, &batch);

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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&file, &batch);

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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_stream(&file, &[batch]);

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

    let schema = TestBatch::simple_schema();
    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);

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

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&file, &batch);

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

// =============================================================================
// Output parity tests (text vs JSON should contain same data)
// =============================================================================

mod parity_tests {
    use super::*;
    use serde_json::Value;
    use std::process::Command;

    fn get_json_output(args: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_silk-chiffon"))
            .args(args)
            .output()
            .expect("failed to execute command");
        let stdout = String::from_utf8_lossy(&output.stdout);
        serde_json::from_str(&stdout).expect("invalid JSON output")
    }

    #[test]
    fn test_parquet_parity_basic_fields() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        // verify required fields exist in JSON
        assert_eq!(json["format"], "parquet");
        assert_eq!(json["rows"], 3);
        assert!(json["num_row_groups"].as_u64().is_some());
        assert!(json["compressed_size"].as_u64().is_some());
        assert!(json["uncompressed_size"].as_u64().is_some());
        assert!(json["compression"].as_str().is_some());

        // schema has field metadata (data_type, nullable)
        let schema_fields = json["schema"].as_array().unwrap();
        assert_eq!(schema_fields.len(), 2);
        for field in schema_fields {
            assert!(field["name"].as_str().is_some());
            assert!(field["data_type"].as_str().is_some());
            assert!(field["nullable"].as_bool().is_some());
        }

        // columns has per-column stats
        let columns = json["columns"].as_array().unwrap();
        assert_eq!(columns.len(), 2);
        for col in columns {
            assert!(col["name"].as_str().is_some());
        }
    }

    #[test]
    fn test_parquet_parity_row_groups() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        let row_groups = json["row_groups"].as_array().unwrap();
        assert!(!row_groups.is_empty());

        for rg in row_groups {
            assert!(rg["index"].as_u64().is_some());
            assert!(rg["num_rows"].as_u64().is_some());
            assert!(rg["compressed_size"].as_u64().is_some());
            assert!(rg["uncompressed_size"].as_u64().is_some());

            let columns = rg["columns"].as_array().unwrap();
            for col in columns {
                assert!(col["name"].as_str().is_some());
                assert!(col["compression"].as_str().is_some());
                assert!(col["encodings"].as_array().is_some());
            }
        }
    }

    #[test]
    fn test_arrow_file_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.arrow");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_arrow_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "arrow",
            file.to_str().unwrap(),
            "--row-count",
            "--format",
            "json",
        ]);

        assert_eq!(json["format"], "arrow");
        assert_eq!(json["variant"], "file");
        assert_eq!(json["rows"], 3);
        assert_eq!(json["record_batches"], 1);

        let schema_arr = json["schema"].as_array().unwrap();
        assert_eq!(schema_arr.len(), 2);

        for col in schema_arr {
            assert!(col["name"].as_str().is_some());
            assert!(col["data_type"].as_str().is_some());
            assert!(col["nullable"].as_bool().is_some());
        }
    }

    #[test]
    fn test_arrow_stream_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.arrows");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_arrow_stream(&file, &[batch]);

        let json = get_json_output(&[
            "inspect",
            "arrow",
            file.to_str().unwrap(),
            "--row-count",
            "--format",
            "json",
        ]);

        assert_eq!(json["format"], "arrow");
        assert_eq!(json["variant"], "stream");
        assert_eq!(json["rows"], 3);
        assert_eq!(json["record_batches"], 1);
    }

    #[test]
    fn test_vortex_parity() {
        use silk_chiffon::sinks::data_sink::DataSink;
        use silk_chiffon::sinks::vortex::{VortexSink, VortexSinkOptions};

        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.vortex");

        let schema = TestBatch::simple_schema();
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sink =
                VortexSink::create(file.to_path_buf(), &schema, VortexSinkOptions::new()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();
        });

        let json = get_json_output(&[
            "inspect",
            "vortex",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        assert_eq!(json["format"], "vortex");
        assert_eq!(json["rows"], 3);

        let schema_arr = json["schema"].as_array().unwrap();
        assert_eq!(schema_arr.len(), 2);

        for col in schema_arr {
            assert!(col["name"].as_str().is_some());
            assert!(col["data_type"].as_str().is_some());
            assert!(col["nullable"].as_bool().is_some());
        }
    }

    #[test]
    fn test_identify_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "identify",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        assert_eq!(json["format"], "parquet");
    }

    #[test]
    fn test_parquet_stats_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        // verify column stats are present
        let columns = json["columns"].as_array().unwrap();
        for col in columns {
            assert!(col["total_null_count"].as_u64().is_some());
            assert!(col["total_compressed_size"].as_u64().is_some());
            assert!(col["total_uncompressed_size"].as_u64().is_some());
        }
    }

    #[test]
    fn test_parquet_encodings_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        // verify encodings are present in row group columns
        let row_groups = json["row_groups"].as_array().unwrap();
        for rg in row_groups {
            let columns = rg["columns"].as_array().unwrap();
            for col in columns {
                let encodings = col["encodings"].as_array().unwrap();
                assert!(!encodings.is_empty(), "encodings should not be empty");
            }
        }
    }

    #[test]
    fn test_parquet_row_group_statistics_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        let row_groups = json["row_groups"].as_array().unwrap();
        for rg in row_groups {
            let columns = rg["columns"].as_array().unwrap();
            for col in columns {
                // statistics object should exist for each column
                let stats = &col["statistics"];
                assert!(stats.is_object(), "statistics should be an object");
                // these fields should be present (even if null)
                assert!(stats.get("min").is_some());
                assert!(stats.get("max").is_some());
                assert!(stats.get("null_count").is_some());
            }
        }
    }

    #[test]
    fn test_arrow_record_batches_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.arrow");

        let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
        let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
        TestFile::write_arrow(&file, &[batch1, batch2]);

        let json = get_json_output(&[
            "inspect",
            "arrow",
            file.to_str().unwrap(),
            "--row-count",
            "--batches",
            "--format",
            "json",
        ]);

        // should have 2 batches
        assert_eq!(json["record_batches"], 2);
        // total rows should be 4
        assert_eq!(json["rows"], 4);
    }

    #[test]
    fn test_parquet_multiple_row_groups_parity() {
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let schema = TestBatch::simple_schema();
        let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
        let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);

        // write with small row group size to force multiple row groups
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_size(2)
            .build();

        let f = File::create(&file).unwrap();
        let mut writer = ArrowWriter::try_new(f, Arc::clone(&schema), Some(props)).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.close().unwrap();

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        let row_groups = json["row_groups"].as_array().unwrap();
        assert_eq!(row_groups.len(), 2, "should have 2 row groups");

        // each row group should have index, num_rows, sizes
        for (i, rg) in row_groups.iter().enumerate() {
            assert_eq!(rg["index"], i as u64);
            assert_eq!(rg["num_rows"], 2);
        }
    }

    #[test]
    fn test_parquet_metadata_parity() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        // metadata should be present as an object
        assert!(json["metadata"].is_object(), "metadata should be an object");
        // parquet files written by arrow have ARROW:schema metadata
        assert!(json["metadata"].get("ARROW:schema").is_some());
    }

    #[test]
    fn test_arrow_metadata_parity() {
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.arrow");

        // create schema with custom metadata
        let mut metadata = HashMap::new();
        metadata.insert("custom_key".to_string(), "custom_value".to_string());

        let schema = Arc::new(
            Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ])
            .with_metadata(metadata),
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        TestFile::write_arrow_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "arrow",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        // metadata should contain our custom key
        assert!(json["metadata"].is_object());
        assert_eq!(json["metadata"]["custom_key"], "custom_value");
    }

    #[test]
    fn test_vortex_stats_parity() {
        use silk_chiffon::sinks::data_sink::DataSink;
        use silk_chiffon::sinks::vortex::{VortexSink, VortexSinkOptions};

        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.vortex");

        let schema = TestBatch::simple_schema();
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sink =
                VortexSink::create(file.to_path_buf(), &schema, VortexSinkOptions::new()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();
        });

        let json = get_json_output(&[
            "inspect",
            "vortex",
            file.to_str().unwrap(),
            "--stats",
            "--format",
            "json",
        ]);

        // statistics should be present (array of field stats)
        let stats = json["statistics"].as_array().unwrap();
        assert!(!stats.is_empty());
        for stat in stats {
            assert!(stat["field"].as_str().is_some());
            assert!(stat["stats"].is_object());
        }
    }

    #[test]
    fn test_vortex_layout_parity() {
        use silk_chiffon::sinks::data_sink::DataSink;
        use silk_chiffon::sinks::vortex::{VortexSink, VortexSinkOptions};

        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.vortex");

        let schema = TestBatch::simple_schema();
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sink =
                VortexSink::create(file.to_path_buf(), &schema, VortexSinkOptions::new()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();
        });

        let json = get_json_output(&[
            "inspect",
            "vortex",
            file.to_str().unwrap(),
            "--layout",
            "--format",
            "json",
        ]);

        // layout info includes detailed segment array
        assert!(json["num_segments"].as_u64().is_some());
        assert!(json["total_size"].as_u64().is_some());

        let segments = json["segments"].as_array().unwrap();
        assert!(!segments.is_empty());
        for seg in segments {
            assert!(seg["index"].as_u64().is_some());
            assert!(seg["offset"].as_u64().is_some());
            assert!(seg["length"].as_u64().is_some());
            assert!(seg["alignment"].as_u64().is_some());
        }
    }

    #[test]
    fn test_parquet_all_flags_combined() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.parquet");

        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        TestFile::write_parquet_batch(&file, &batch);

        let json = get_json_output(&[
            "inspect",
            "parquet",
            file.to_str().unwrap(),
            "--format",
            "json",
        ]);

        // all sections should be present
        assert!(json["columns"].is_array());
        assert!(json["row_groups"].is_array());
        assert!(json["metadata"].is_object());
        assert!(json["rows"].as_u64().is_some());
        assert!(json["compression"].as_str().is_some());

        // row groups should have full column details with encodings
        let rg = &json["row_groups"][0];
        let col = &rg["columns"][0];
        assert!(col["encodings"].is_array());
        assert!(col["statistics"].is_object());
    }

    #[test]
    fn test_arrow_all_flags_combined() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.arrow");

        let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
        let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
        TestFile::write_arrow(&file, &[batch1, batch2]);

        let json = get_json_output(&[
            "inspect",
            "arrow",
            file.to_str().unwrap(),
            "--batches",
            "--row-count",
            "--format",
            "json",
        ]);

        // all sections should be present
        assert!(json["schema"].is_array());
        assert!(json["metadata"].is_object());
        assert_eq!(json["rows"], 4);
        assert_eq!(json["record_batches"], 2);
        assert_eq!(json["variant"], "file");
    }

    #[test]
    fn test_vortex_all_flags_combined() {
        use silk_chiffon::sinks::data_sink::DataSink;
        use silk_chiffon::sinks::vortex::{VortexSink, VortexSinkOptions};

        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.vortex");

        let schema = TestBatch::simple_schema();
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sink =
                VortexSink::create(file.to_path_buf(), &schema, VortexSinkOptions::new()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();
        });

        let json = get_json_output(&[
            "inspect",
            "vortex",
            file.to_str().unwrap(),
            "--schema",
            "--stats",
            "--layout",
            "--format",
            "json",
        ]);

        // all sections should be present
        assert!(json["schema"].is_array());
        assert_eq!(json["rows"], 3);
        assert_eq!(json["format"], "vortex");
    }
}
