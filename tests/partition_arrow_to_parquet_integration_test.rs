use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use silk_chiffon::{PartitionArrowToParquetArgs, QueryDialect};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

fn create_test_data() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "C"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef,
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10])) as ArrayRef,
            Arc::new(StringArray::from(vec!["A", "C", "B", "C", "A"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![60, 70, 80, 90, 100])) as ArrayRef,
        ],
    )
    .unwrap();

    (schema, vec![batch1, batch2])
}

fn write_test_arrow_file(path: &Path, schema: &Schema, batches: Vec<RecordBatch>) {
    let file = File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, schema).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
}

#[tokio::test]
async fn test_partition_arrow_to_parquet_basic() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = PartitionArrowToParquetArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{value}}.parquet", output_dir.display()),
        query: None,
        dialect: QueryDialect::default(),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: silk_chiffon::ParquetCompression::Snappy,
        statistics: silk_chiffon::ParquetStatistics::Page,
        max_row_group_size: 1_048_576,
        writer_version: silk_chiffon::ParquetWriterVersion::V2,
        no_dictionary: false,
        write_sorted_metadata: false,
        bloom_all: None,
        bloom_column: vec![],
        list_outputs: silk_chiffon::ListOutputsFormat::None,
        exclude_columns: vec![],
    };

    silk_chiffon::commands::partition_arrow_to_parquet::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("A.parquet").exists());
    assert!(output_dir.join("B.parquet").exists());
    assert!(output_dir.join("C.parquet").exists());

    let file = File::open(output_dir.join("A.parquet")).unwrap();
    let parquet_reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
    let parquet_batches: Vec<_> = parquet_reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(!parquet_batches.is_empty());
}

#[tokio::test]
async fn test_partition_arrow_to_parquet_with_bloom_filters() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = PartitionArrowToParquetArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{value}}.parquet", output_dir.display()),
        query: None,
        dialect: QueryDialect::default(),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: silk_chiffon::ParquetCompression::None,
        statistics: silk_chiffon::ParquetStatistics::Page,
        max_row_group_size: 1_048_576,
        writer_version: silk_chiffon::ParquetWriterVersion::V2,
        no_dictionary: false,
        write_sorted_metadata: false,
        bloom_all: Some(silk_chiffon::AllColumnsBloomFilterConfig { fpp: 0.01 }),
        bloom_column: vec![],
        list_outputs: silk_chiffon::ListOutputsFormat::None,
        exclude_columns: vec![],
    };

    silk_chiffon::commands::partition_arrow_to_parquet::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("A.parquet").exists());
    assert!(output_dir.join("B.parquet").exists());
    assert!(output_dir.join("C.parquet").exists());
}

#[tokio::test]
async fn test_partition_arrow_to_parquet_with_sorted_metadata() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = PartitionArrowToParquetArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{value}}.parquet", output_dir.display()),
        query: None,
        dialect: QueryDialect::default(),
        record_batch_size: 122_880,
        sort_by: Some("value".parse().unwrap()),
        create_dirs: true,
        overwrite: false,
        compression: silk_chiffon::ParquetCompression::None,
        statistics: silk_chiffon::ParquetStatistics::Page,
        max_row_group_size: 1_048_576,
        writer_version: silk_chiffon::ParquetWriterVersion::V2,
        no_dictionary: false,
        write_sorted_metadata: true,
        bloom_all: None,
        bloom_column: vec![],
        list_outputs: silk_chiffon::ListOutputsFormat::None,
        exclude_columns: vec![],
    };

    silk_chiffon::commands::partition_arrow_to_parquet::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("A.parquet").exists());
}
