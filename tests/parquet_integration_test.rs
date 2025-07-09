use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use std::fs::File;
use std::process::Command;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_parquet_conversion_basic() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.parquet");

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
            "parquet",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");

    let file = File::open(&output_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[test]
fn test_parquet_conversion_with_sorting() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.parquet");

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
            "parquet",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--sort-by",
            "id",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");

    let file = File::open(&output_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);

    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[test]
fn test_parquet_conversion_with_compression() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.parquet");

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
            "parquet",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--compression",
            "zstd",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");

    let file = File::open(&output_path).unwrap();
    let _reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
}

#[test]
fn test_parquet_conversion_with_bloom_filter_all() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Alice", "Bob",
            ])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    // Test with default FPP
    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "parquet",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--bloom-all",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");

    // Test with custom FPP
    let output_path2 = temp_dir.path().join("output2.parquet");
    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "parquet",
            input_path.to_str().unwrap(),
            output_path2.to_str().unwrap(),
            "--bloom-all",
            "fpp=0.001",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path2.exists(), "Output file was not created");
}

#[test]
fn test_parquet_conversion_with_bloom_filter_columns() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![25, 30, 35])),
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
            "parquet",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--bloom-column",
            "id",
            "--bloom-column",
            "name:fpp=0.001",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");
}
