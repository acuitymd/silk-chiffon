use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use duckdb::Connection;
use std::fs::File;
use std::process::Command;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_duckdb_conversion_basic() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "people",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);
    assert!(output_path.exists(), "Database file was not created");

    let conn = Connection::open(&output_path).unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM people", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_duckdb_table_already_exists() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "people",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "people",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("already exists"));
}

#[test]
fn test_duckdb_with_drop_table() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![100])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch1).unwrap();
    writer.finish().unwrap();

    Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "data",
        ])
        .output()
        .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![200])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch2).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "data",
            "--drop-table",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    let conn = Connection::open(&output_path).unwrap();
    let value: i32 = conn
        .query_row("SELECT value FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 200);
}

#[test]
fn test_duckdb_with_sorting() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 1, 2])),
            Arc::new(StringArray::from(vec!["Charlie", "Alice", "Bob"])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "sorted_data",
            "--sort-by",
            "id",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let conn = Connection::open(&output_path).unwrap();
    let mut stmt = conn.prepare("SELECT id FROM sorted_data").unwrap();
    let ids: Vec<i32> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
}
