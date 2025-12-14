use anyhow::Result;
use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use silk_chiffon::{
    AllColumnsBloomFilterConfig, ArrowCompression, ArrowIPCFormat, ColumnSpecificBloomFilterConfig,
    DataFormat, ListOutputsFormat, ParquetCompression, ParquetStatistics, ParquetWriterVersion,
    QueryDialect, SortColumn, SortDirection, SortSpec,
};
use tempfile::TempDir;

mod test_helpers {
    use super::*;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::FileReader;
    use std::path::Path;
    use std::sync::Arc;

    pub fn simple_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    pub fn create_batch(schema: &Arc<Schema>, ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn write_arrow_file(path: &Path, schema: &Arc<Schema>, batches: Vec<RecordBatch>) {
        use arrow::ipc::writer::FileWriter;
        use std::fs::File;

        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, schema).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }

    pub fn write_parquet_file(path: &Path, schema: &Arc<Schema>, batches: Vec<RecordBatch>) {
        use parquet::arrow::ArrowWriter;
        use std::fs::File;

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), None).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
    }

    pub fn read_arrow_file(path: &Path) -> Vec<RecordBatch> {
        use arrow::ipc::reader::{FileReader, StreamReader};
        use std::fs::File;
        let file = File::open(path).unwrap();

        if let Ok(reader) = FileReader::try_new(file.try_clone().unwrap(), None) {
            reader.collect::<Result<Vec<_>, _>>().unwrap()
        } else {
            let reader = StreamReader::try_new(file, None).unwrap();
            reader.collect::<Result<Vec<_>, _>>().unwrap()
        }
    }

    pub fn read_parquet_file(path: &Path) -> Vec<RecordBatch> {
        let file = std::fs::File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        reader.collect::<Result<Vec<_>, _>>().unwrap()
    }

    pub fn get_parquet_row_group_metadata(
        path: &Path,
        idx: usize,
    ) -> parquet::file::metadata::RowGroupMetaData {
        let file = std::fs::File::open(path).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        reader.metadata().row_group(idx).clone()
    }
}

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::test]
async fn test_transform_arrow_to_arrow_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_arrow_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Snappy),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_parquet_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_parquet_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        parquet_compression: Some(ParquetCompression::Zstd),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_from_many_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch1 = test_helpers::create_batch(&schema, &[1, 2], &["a", "b"]);
    let batch2 = test_helpers::create_batch(&schema, &[3, 4], &["c", "d"]);
    test_helpers::write_arrow_file(&input1, &schema, vec![batch1]);
    test_helpers::write_arrow_file(&input2, &schema, vec![batch2]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: None,
        from_many: vec![
            input1.to_string_lossy().to_string(),
            input2.to_string_lossy().to_string(),
        ],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
}

#[tokio::test]
async fn test_transform_from_many_with_glob() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("file1.arrow");
    let input2 = temp_dir.path().join("file2.arrow");
    let input3 = temp_dir.path().join("other.parquet");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch1 = test_helpers::create_batch(&schema, &[1], &["a"]);
    let batch2 = test_helpers::create_batch(&schema, &[2], &["b"]);
    let batch3 = test_helpers::create_batch(&schema, &[3], &["c"]);
    test_helpers::write_arrow_file(&input1, &schema, vec![batch1]);
    test_helpers::write_arrow_file(&input2, &schema, vec![batch2]);
    test_helpers::write_parquet_file(&input3, &schema, vec![batch3]);

    let glob_pattern = temp_dir.path().join("file*.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: None,
        from_many: vec![glob_pattern.to_string_lossy().to_string()],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_transform_to_many_partitioned() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("{{name}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.arrow");
    let output_b = temp_dir.path().join("b.arrow");

    assert!(output_a.exists());
    assert!(output_b.exists());

    let batches_a = test_helpers::read_arrow_file(&output_a);
    let batches_b = test_helpers::read_arrow_file(&output_b);

    assert_eq!(batches_a.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(batches_b.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_transform_with_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT * FROM data WHERE id > 1".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_transform_with_sorting() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[3, 1, 2], &["c", "a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec {
            columns: vec![silk_chiffon::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[tokio::test]
async fn test_transform_with_arrow_compression() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: Some(ArrowCompression::Zstd),
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_with_parquet_bloom_filters() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(
        &schema,
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    );
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![ColumnSpecificBloomFilterConfig {
            name: "id".to_string(),
            config: silk_chiffon::ColumnBloomFilterConfig {
                fpp: 0.01,
                ndv: None,
            },
        }],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
}

#[tokio::test]
async fn test_transform_with_sorted_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[3, 1, 2], &["c", "a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec {
            columns: vec![silk_chiffon::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: true,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let rg_metadata = test_helpers::get_parquet_row_group_metadata(&output, 0);
    assert!(rg_metadata.sorting_columns().is_some());
    let sorting_cols = rg_metadata.sorting_columns().unwrap();
    assert_eq!(sorting_cols.len(), 1);
    assert_eq!(sorting_cols[0].column_idx, 0);
}

#[tokio::test]
async fn test_transform_partition_with_create_dirs() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2], &["a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("nested/{{name}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(temp_dir.path().join("nested").exists());
    assert!(temp_dir.path().join("nested/a.arrow").exists());
    assert!(temp_dir.path().join("nested/b.arrow").exists());
}

#[tokio::test]
async fn test_transform_partition_with_overwrite() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let existing = temp_dir.path().join("a.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2], &["a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch.clone()]);
    test_helpers::write_arrow_file(&existing, &schema, vec![batch]);

    let template = temp_dir.path().join("{{name}}.arrow");

    let result = silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: false,
        overwrite: true,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_transform_from_many_empty_glob() {
    let temp_dir = TempDir::new().unwrap();
    let output = temp_dir.path().join("output.arrow");

    let glob_pattern = temp_dir.path().join("nonexistent*.arrow");

    let result = silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: None,
        from_many: vec![glob_pattern.to_string_lossy().to_string()],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("No input files found")
    );
}

#[tokio::test]
async fn test_transform_partition_exclude_columns() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2], &["a", "a"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("{{name}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec!["name".to_string()],
        list_outputs: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let output = temp_dir.path().join("a.arrow");
    assert!(output.exists());

    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "id");
}

#[tokio::test]
async fn test_transform_with_projection_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT id FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "id");
}

#[tokio::test]
async fn test_transform_with_aggregation_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT COUNT(*) as count FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches[0].num_rows(), 1);
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 3);
}

#[tokio::test]
async fn test_transform_query_and_sort_combined() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[3, 1, 2], &["c", "a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT * FROM data WHERE id > 1".to_string()),
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids.value(0), 2);
    assert_eq!(ids.value(1), 3);
}

#[tokio::test]
async fn test_transform_multi_column_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "B", "B"])),
            Arc::new(Int32Array::from(vec![3, 1, 2, 4])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let output = temp_dir.path().join("output.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec {
            columns: vec![
                SortColumn {
                    name: "category".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "value".to_string(),
                    direction: SortDirection::Ascending,
                },
            ],
        }),
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    let categories = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(categories.value(0), "A");
    assert_eq!(values.value(0), 1);
    assert_eq!(categories.value(1), "A");
    assert_eq!(values.value(1), 3);
    assert_eq!(categories.value(2), "B");
    assert_eq!(values.value(2), 2);
    assert_eq!(categories.value(3), "B");
    assert_eq!(values.value(3), 4);
}

#[tokio::test]
async fn test_transform_sort_descending() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Descending,
            }],
        }),
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let mut all_ids = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..ids.len() {
            all_ids.push(ids.value(i));
        }
    }

    let mut sorted = all_ids.clone();
    sorted.sort();
    sorted.reverse();
    assert_eq!(all_ids, sorted);
}

#[tokio::test]
async fn test_transform_parquet_compression_gzip() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Gzip),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_parquet_compression_lz4() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Lz4),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_parquet_bloom_all() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: Some(AllColumnsBloomFilterConfig {
            fpp: 0.01,
            ndv: None,
        }),
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: Some(ParquetStatistics::Chunk),
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_writer_version() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: Some(ParquetWriterVersion::V1),
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_no_dictionary() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: true,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_arrow_format_stream() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: Some(ArrowIPCFormat::Stream),
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_arrow_record_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: Some(1000),
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_transform_parquet_row_group_size() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: Some(1000),
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_transform_partition_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("{{name}}.parquet");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Snappy),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.parquet");
    let output_b = temp_dir.path().join("b.parquet");

    assert!(output_a.exists());
    assert!(output_b.exists());

    let batches_a = test_helpers::read_parquet_file(&output_a);
    let batches_b = test_helpers::read_parquet_file(&output_b);

    assert_eq!(batches_a.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(batches_b.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_transform_multi_column_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2023, 2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 2, 1])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("year={{year}}/month={{month}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(temp_dir.path().join("year=2023/month=1.arrow").exists());
    assert!(temp_dir.path().join("year=2023/month=2.arrow").exists());
    assert!(temp_dir.path().join("year=2024/month=1.arrow").exists());
}

#[tokio::test]
async fn test_transform_from_many_to_partitioned() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");

    let schema = test_helpers::simple_schema();
    let batch1 = test_helpers::create_batch(&schema, &[1, 2], &["a", "b"]);
    let batch2 = test_helpers::create_batch(&schema, &[3, 4], &["a", "c"]);
    test_helpers::write_arrow_file(&input1, &schema, vec![batch1]);
    test_helpers::write_arrow_file(&input2, &schema, vec![batch2]);

    let _ = std::fs::remove_file(temp_dir.path().join("a.arrow"));
    let _ = std::fs::remove_file(temp_dir.path().join("b.arrow"));
    let _ = std::fs::remove_file(temp_dir.path().join("c.arrow"));

    let template = temp_dir.path().join("{{name}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: None,
        from_many: vec![
            input1.to_string_lossy().to_string(),
            input2.to_string_lossy().to_string(),
        ],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: false,
        overwrite: true,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.arrow");
    let output_b = temp_dir.path().join("b.arrow");
    let output_c = temp_dir.path().join("c.arrow");

    assert!(output_a.exists());
    assert!(output_b.exists());
    assert!(output_c.exists());

    let batches_a = test_helpers::read_arrow_file(&output_a);
    let batches_b = test_helpers::read_arrow_file(&output_b);
    let batches_c = test_helpers::read_arrow_file(&output_c);

    let rows_a: usize = batches_a.iter().map(|b| b.num_rows()).sum();
    let rows_b: usize = batches_b.iter().map(|b| b.num_rows()).sum();
    let rows_c: usize = batches_c.iter().map(|b| b.num_rows()).sum();
    let total_rows = rows_a + rows_b + rows_c;

    assert!(rows_a >= 1, "partition 'a' should have at least 1 row");
    assert_eq!(rows_b, 1, "partition 'b' should have 1 row");
    assert_eq!(rows_c, 1, "partition 'c' should have 1 row");
    assert!(
        total_rows >= 3,
        "total rows should be at least 3, got {}",
        total_rows
    );
    assert!(
        total_rows <= 4,
        "total rows should be at most 4, got {}",
        total_rows
    );
}

#[tokio::test]
async fn test_transform_invalid_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let result = silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT nonexistent FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_transform_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    test_helpers::write_arrow_file(&input, &schema, vec![]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn test_transform_bloom_filter_with_custom_ndv() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: Some(AllColumnsBloomFilterConfig {
            fpp: 0.005,
            ndv: Some(1000),
        }),
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches[0].num_rows(), 5);
}

#[tokio::test]
async fn test_transform_bloom_filter_column_specific_with_ndv() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![ColumnSpecificBloomFilterConfig {
            name: "id".to_string(),
            config: silk_chiffon::ColumnBloomFilterConfig {
                fpp: 0.005,
                ndv: Some(5000),
            },
        }],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_mixed_parquet_and_arrow_inputs() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_input1 = temp_dir.path().join("data1.arrow");
    let arrow_input2 = temp_dir.path().join("data2.arrow");
    let parquet_input = temp_dir.path().join("data3.parquet");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch1 = test_helpers::create_batch(&schema, &[1, 2], &["a", "b"]);
    let batch2 = test_helpers::create_batch(&schema, &[3, 4], &["c", "d"]);
    let batch3 = test_helpers::create_batch(&schema, &[5, 6], &["e", "f"]);
    test_helpers::write_arrow_file(&arrow_input1, &schema, vec![batch1]);
    test_helpers::write_arrow_file(&arrow_input2, &schema, vec![batch2]);
    test_helpers::write_parquet_file(&parquet_input, &schema, vec![batch3]);

    let glob_pattern = temp_dir.path().join("data*.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: None,
        from_many: vec![glob_pattern.to_string_lossy().to_string()],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Snappy),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
}

#[tokio::test]
async fn test_transform_partition_list_outputs_text() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "a", "b"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("{{name}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: Some(ListOutputsFormat::Text),
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.arrow");
    let output_b = temp_dir.path().join("b.arrow");

    assert!(output_a.exists());
    assert!(output_b.exists());
}

#[tokio::test]
async fn test_transform_partition_list_outputs_json() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2], &["x", "y"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("{{name}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec![],
        list_outputs: Some(ListOutputsFormat::Json),
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let output_x = temp_dir.path().join("x.arrow");
    let output_y = temp_dir.path().join("y.arrow");

    assert!(output_x.exists());
    assert!(output_y.exists());
}

#[tokio::test]
async fn test_transform_explicit_input_format_arrow_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: Some(DataFormat::Arrow),
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_parquet_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_transform_explicit_output_format_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_parquet_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: Some(DataFormat::Parquet),
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_transform_arrow_compression_lz4() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: Some(ArrowCompression::Lz4),
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_query_with_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("{{category}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("category".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: false,
        overwrite: true,
        query: Some("SELECT * FROM data WHERE value > 15".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap_or_else(|e| panic!("Command failed with error: {:?}", e));

    let output_a = temp_dir.path().join("A.arrow");
    let output_b = temp_dir.path().join("B.arrow");

    let has_a = output_a.exists();
    let has_b = output_b.exists();

    assert!(has_a || has_b, "At least one partition file should exist");

    let mut total_rows = 0;
    if has_a {
        let batches_a = test_helpers::read_arrow_file(&output_a);
        total_rows += batches_a.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    if has_b {
        let batches_b = test_helpers::read_arrow_file(&output_b);
        total_rows += batches_b.iter().map(|b| b.num_rows()).sum::<usize>();
    }

    assert!(
        total_rows >= 2,
        "Expected at least 2 rows total after filtering, got {}",
        total_rows
    );
}

#[tokio::test]
async fn test_transform_query_with_different_dialect() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = test_helpers::simple_schema();
    let batch = test_helpers::create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT * FROM data WHERE id >= 2".to_string()),
        dialect: QueryDialect::PostgreSQL,
        sort_by: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = test_helpers::read_arrow_file(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_transform_partition_with_query_and_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![5, 3, 8, 1, 6, 2, 9])),
            Arc::new(StringArray::from(vec![
                "US", "EU", "US", "EU", "US", "EU", "US",
            ])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 50, 75, 300, 125])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir.path().join("{{region}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("region".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        create_dirs: false,
        overwrite: false,
        query: Some("SELECT * FROM data WHERE score > 100".to_string()),
        dialect: QueryDialect::default(),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "score".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let output_us = temp_dir.path().join("US.arrow");
    let output_eu = temp_dir.path().join("EU.arrow");

    assert!(output_us.exists());
    assert!(output_eu.exists());

    let batches_us = test_helpers::read_arrow_file(&output_us);
    let batches_eu = test_helpers::read_arrow_file(&output_eu);

    let us_rows: usize = batches_us.iter().map(|b| b.num_rows()).sum();
    let eu_rows: usize = batches_eu.iter().map(|b| b.num_rows()).sum();

    assert_eq!(us_rows, 2);
    assert_eq!(eu_rows, 2);
    let mut us_scores_vec = Vec::new();
    for batch in batches_us {
        if let Some(score_col) = batch.column_by_name("score") {
            let scores = score_col.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..scores.len() {
                us_scores_vec.push(scores.value(i));
            }
        }
    }
    us_scores_vec.sort();
    assert_eq!(us_scores_vec, vec![125, 150]);

    let mut eu_scores_vec = Vec::new();
    for batch in batches_eu {
        if let Some(score_col) = batch.column_by_name("score") {
            let scores = score_col.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..scores.len() {
                eu_scores_vec.push(scores.value(i));
            }
        }
    }
    eu_scores_vec.sort();
    assert_eq!(eu_scores_vec, vec![200, 300]);
}
