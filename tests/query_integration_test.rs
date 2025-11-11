use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use clio::{Input, OutputPath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use silk_chiffon::QueryDialect;
use silk_chiffon::{
    ArrowCompression, ArrowToArrowArgs, ArrowToParquetArgs, ParquetCompression, ParquetStatistics,
    ParquetWriterVersion, SortColumn, SortDirection, SortSpec, utils::arrow_io::ArrowIPCFormat,
};
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

mod test_helpers {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::FileReader;
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    pub fn create_test_data() -> (Arc<Schema>, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "Alice", "Bob", "Charlie", "David", "Eve",
                ])),
                Arc::new(Int32Array::from(vec![100, 50, 150, 200, 75])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10])),
                Arc::new(StringArray::from(vec![
                    "Frank", "Grace", "Henry", "Ivy", "Jack",
                ])),
                Arc::new(Int32Array::from(vec![125, 80, 95, 110, 160])),
            ],
        )
        .unwrap();

        (schema, vec![batch1, batch2])
    }

    pub fn write_test_arrow_file(path: &Path) -> anyhow::Result<()> {
        let (schema, batches) = create_test_data();
        let file = File::create(path)?;
        let mut writer = FileWriter::try_new(file, &schema)?;

        for batch in batches {
            writer.write(&batch)?;
        }

        writer.finish()?;
        Ok(())
    }

    pub fn read_arrow_file(path: &Path) -> anyhow::Result<Vec<RecordBatch>> {
        let file = File::open(path)?;
        let reader = FileReader::try_new(file, None)?;
        reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

#[tokio::test]
async fn test_arrow_with_filter_query() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.arrow");

    test_helpers::write_test_arrow_file(&input_path).unwrap();

    let args = ArrowToArrowArgs {
        input: Input::new(&input_path).unwrap(),
        output: OutputPath::new(&output_path).unwrap(),
        query: Some("SELECT * FROM data WHERE amount > 100".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        compression: ArrowCompression::None,
        record_batch_size: 122_880,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::arrow_to_arrow::run(args)
        .await
        .unwrap();

    let results = test_helpers::read_arrow_file(&output_path).unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    for batch in results {
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..amounts.len() {
            assert!(amounts.value(i) > 100);
        }
    }
}

#[tokio::test]
async fn test_arrow_with_projection_query() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.arrow");

    test_helpers::write_test_arrow_file(&input_path).unwrap();

    let args = ArrowToArrowArgs {
        input: Input::new(&input_path).unwrap(),
        output: OutputPath::new(&output_path).unwrap(),
        query: Some("SELECT id, name FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        compression: ArrowCompression::None,
        record_batch_size: 122_880,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::arrow_to_arrow::run(args)
        .await
        .unwrap();

    let results = test_helpers::read_arrow_file(&output_path).unwrap();
    assert!(!results.is_empty());

    for batch in results {
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
    }
}

#[tokio::test]
async fn test_arrow_with_aggregation_query() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.arrow");

    test_helpers::write_test_arrow_file(&input_path).unwrap();

    let args = ArrowToArrowArgs {
        input: Input::new(&input_path).unwrap(),
        output: OutputPath::new(&output_path).unwrap(),
        query: Some(
            "SELECT COUNT(*) as total_count, SUM(amount) as total_amount FROM data".to_string(),
        ),
        dialect: QueryDialect::default(),
        sort_by: None,
        compression: ArrowCompression::None,
        record_batch_size: 122_880,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::arrow_to_arrow::run(args)
        .await
        .unwrap();

    let results = test_helpers::read_arrow_file(&output_path).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);

    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 10);

    let sum = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(sum.value(0), 1145);
}

#[tokio::test]
async fn test_arrow_with_query_and_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.arrow");

    test_helpers::write_test_arrow_file(&input_path).unwrap();

    let args = ArrowToArrowArgs {
        input: Input::new(&input_path).unwrap(),
        output: OutputPath::new(&output_path).unwrap(),
        query: Some("SELECT * FROM data WHERE amount > 100".to_string()),
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "amount".to_string(),
                direction: SortDirection::Descending,
            }],
        }),
        compression: ArrowCompression::None,
        record_batch_size: 122_880,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::arrow_to_arrow::run(args)
        .await
        .unwrap();

    let results = test_helpers::read_arrow_file(&output_path).unwrap();

    let mut amounts = Vec::new();
    for batch in results {
        let amount_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..amount_array.len() {
            amounts.push(amount_array.value(i));
        }
    }

    for i in 1..amounts.len() {
        assert!(amounts[i - 1] >= amounts[i]);
        assert!(amounts[i] > 100);
    }
}

#[tokio::test]
async fn test_parquet_with_query() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.parquet");

    test_helpers::write_test_arrow_file(&input_path).unwrap();

    let args = ArrowToParquetArgs {
        input: Input::new(&input_path).unwrap(),
        output: OutputPath::new(&output_path).unwrap(),
        query: Some(
            "SELECT id, name, amount * 1.1 as adjusted_amount FROM data WHERE amount >= 100"
                .to_string(),
        ),
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec::default()),
        compression: ParquetCompression::None,
        write_sorted_metadata: false,
        bloom_all: None,
        bloom_column: vec![],
        max_row_group_size: 1_048_576,
        statistics: ParquetStatistics::Page,
        record_batch_size: 122_880,
        no_dictionary: false,
        writer_version: ParquetWriterVersion::V2,
    };

    silk_chiffon::commands::arrow_to_parquet::run(args)
        .await
        .unwrap();

    assert!(output_path.exists());

    let file = std::fs::File::open(&output_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();

        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(2).name(), "adjusted_amount");
    }

    assert_eq!(total_rows, 6); // Rows with amount >= 100
}

#[tokio::test]
async fn test_invalid_query_returns_error() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.arrow");

    test_helpers::write_test_arrow_file(&input_path).unwrap();

    let args = ArrowToArrowArgs {
        input: Input::new(&input_path).unwrap(),
        output: OutputPath::new(&output_path).unwrap(),
        query: Some("SELECT nonexistent_column FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        compression: ArrowCompression::None,
        record_batch_size: 122_880,
        output_ipc_format: ArrowIPCFormat::File,
    };

    let result = silk_chiffon::commands::arrow_to_arrow::run(args).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Failed to parse SQL query") || err_msg.contains("nonexistent_column")
    );
}

#[tokio::test]
async fn test_type_casting_int64_to_int32() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![100i64, 200, 300, 400, 500])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let args = ArrowToArrowArgs {
        input: Input::new(&input_path).unwrap(),
        output: OutputPath::new(&output_path).unwrap(),
        query: Some(
            "SELECT CAST(id AS INT) as id, CAST(value AS INT) as value FROM data".to_string(),
        ),
        dialect: QueryDialect::default(),
        sort_by: None,
        compression: ArrowCompression::None,
        record_batch_size: 122_880,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::arrow_to_arrow::run(args)
        .await
        .unwrap();

    let results = test_helpers::read_arrow_file(&output_path).unwrap();
    assert!(!results.is_empty());

    for batch in results {
        assert_eq!(batch.num_columns(), 2);

        assert_eq!(batch.column(0).data_type(), &DataType::Int32);
        assert_eq!(batch.column(1).data_type(), &DataType::Int32);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(id_array.values(), &[1i32, 2, 3, 4, 5]);
        assert_eq!(value_array.values(), &[100i32, 200, 300, 400, 500]);
    }
}
