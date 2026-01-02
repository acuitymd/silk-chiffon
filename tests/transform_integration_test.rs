use anyhow::Result;
use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use camino::Utf8PathBuf;
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
use std::path::Path;
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Snappy),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        parquet_compression: Some(ParquetCompression::Zstd),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT * FROM data WHERE id > 1".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
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
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: Some(ArrowCompression::Zstd),
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
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
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
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
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: true,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT id FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT COUNT(*) as count FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
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
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
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
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
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
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Gzip),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Lz4),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
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
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: Some(ParquetStatistics::Chunk),
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: Some(ParquetWriterVersion::V1),
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: true,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
async fn test_transform_parquet_column_no_dictionary() {
    // dictionary globally enabled (default), but disabled for specific column
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec!["id".to_string()],
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
async fn test_transform_parquet_column_dictionary() {
    // dictionary globally disabled, but enabled for specific column
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: true,
        parquet_column_dictionary: vec!["name".to_string()],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: Some(ArrowIPCFormat::Stream),
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: Some(1000),
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: Some(1000),
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Snappy),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: true,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT nonexistent FROM data".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
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
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
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
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Snappy),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: Some(DataFormat::Arrow),
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: Some(DataFormat::Parquet),
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: Some(ArrowCompression::Lz4),
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: false,
        overwrite: true,
        query: Some("SELECT * FROM data WHERE value > 15".to_string()),
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: Some("SELECT * FROM data WHERE id >= 2".to_string()),
        dialect: QueryDialect::PostgreSQL,
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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
        list_outputs_file: None,
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
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: None,
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
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

/// round-trip test: arrow -> parquet -> arrow, verify data is identical
#[tokio::test]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
async fn test_parquet_roundtrip_data_fidelity() {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int64Array,
        LargeStringArray, StringViewArray, TimestampMicrosecondArray, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, TimeUnit};

    let temp_dir = TempDir::new().unwrap();
    let input_arrow = temp_dir.path().join("input.arrow");
    let intermediate_parquet = temp_dir.path().join("intermediate.parquet");
    let output_arrow = temp_dir.path().join("output.arrow");

    // create a schema with various data types to test round-trip fidelity
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("int8_col", DataType::Int8, false),
        Field::new("int16_col", DataType::Int16, false),
        Field::new("int64_col", DataType::Int64, false),
        Field::new("uint32_col", DataType::UInt32, false),
        Field::new("float32_col", DataType::Float32, false),
        Field::new("float64_col", DataType::Float64, false),
        Field::new("bool_col", DataType::Boolean, false),
        Field::new("string_col", DataType::Utf8, false),
        Field::new("nullable_int", DataType::Int32, true),
        Field::new("nullable_string", DataType::Utf8, true),
        Field::new(
            "timestamp_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    // generate a larger dataset (10k rows) split into multiple batches
    let num_rows: i32 = 10_000;
    let input_batch_size: i32 = 1_000; // 10 input batches
    let parquet_row_group_size: usize = 2_000; // 5 row groups
    let output_batch_size: usize = 1_500; // ~7 output batches

    // helper to create a batch for a range of rows
    let make_batch = |start: i32, end: i32| -> RecordBatch {
        let ids: Vec<i32> = (start..end).collect();
        let int8_vals: Vec<i8> = (start..end).map(|i| (i % 128) as i8).collect();
        let int16_vals: Vec<i16> = (start..end).map(|i| (i % 32768) as i16).collect();
        let int64_vals: Vec<i64> = (start..end).map(|i| i64::from(i) * 1_000_000).collect();
        let uint32_vals: Vec<u32> = (start..end).map(|i| i as u32 * 2).collect();
        let float32_vals: Vec<f32> = (start..end).map(|i| i as f32 * 0.5).collect();
        let float64_vals: Vec<f64> = (start..end).map(|i| f64::from(i) * 1.5).collect();
        let bool_vals: Vec<bool> = (start..end).map(|i| i % 2 == 0).collect();
        let string_vals: Vec<String> = (start..end).map(|i| format!("row_{i}")).collect();
        let nullable_int_vals: Vec<Option<i32>> = (start..end)
            .map(|i| if i % 3 == 0 { None } else { Some(i) })
            .collect();
        let nullable_string_vals: Vec<Option<String>> = (start..end)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(format!("nullable_{i}"))
                }
            })
            .collect();
        let timestamp_vals: Vec<i64> = (start..end).map(|i| i64::from(i) * 1_000_000).collect();

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int8Array::from(int8_vals)),
                Arc::new(Int16Array::from(int16_vals)),
                Arc::new(Int64Array::from(int64_vals)),
                Arc::new(UInt32Array::from(uint32_vals)),
                Arc::new(Float32Array::from(float32_vals)),
                Arc::new(Float64Array::from(float64_vals)),
                Arc::new(BooleanArray::from(bool_vals)),
                Arc::new(StringArray::from(
                    string_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(nullable_int_vals)),
                Arc::new(StringArray::from(
                    nullable_string_vals
                        .iter()
                        .map(|s| s.as_deref())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(timestamp_vals)),
            ],
        )
        .unwrap()
    };

    // create multiple input batches
    let input_batches_to_write: Vec<RecordBatch> = (0..num_rows)
        .step_by(input_batch_size as usize)
        .map(|start| make_batch(start, (start + input_batch_size).min(num_rows)))
        .collect();

    assert_eq!(
        input_batches_to_write.len(),
        10,
        "should have 10 input batches"
    );

    // write the input arrow file with multiple batches
    test_helpers::write_arrow_file(&input_arrow, &schema, input_batches_to_write);

    // step 1: convert arrow to parquet with multiple row groups
    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input_arrow.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(intermediate_parquet.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: Some(ParquetCompression::Zstd),
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: Some(parquet_row_group_size),
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(intermediate_parquet.exists());

    // verify parquet has multiple row groups
    let parquet_file = std::fs::File::open(&intermediate_parquet).unwrap();
    let parquet_reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(parquet_file)
            .unwrap();
    let num_row_groups = parquet_reader.metadata().num_row_groups();
    assert!(
        num_row_groups >= 5,
        "should have at least 5 row groups, got {num_row_groups}"
    );

    // step 2: convert parquet back to arrow with specified batch size
    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(intermediate_parquet.to_string_lossy().to_string()),
        from_many: vec![],
        to: Some(output_arrow.to_string_lossy().to_string()),
        to_many: None,
        by: None,
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: Some(DataFormat::Parquet),
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: Some(output_batch_size),
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    assert!(output_arrow.exists());

    // step 3: read both files and compare directly
    let input_batches = test_helpers::read_arrow_file(&input_arrow);
    let output_batches = test_helpers::read_arrow_file(&output_arrow);

    // verify we have multiple batches in both files
    assert!(
        input_batches.len() >= 10,
        "input should have at least 10 batches, got {}",
        input_batches.len()
    );
    assert!(
        output_batches.len() >= 6,
        "output should have at least 6 batches, got {}",
        output_batches.len()
    );

    // verify row counts match
    let input_rows: usize = input_batches.iter().map(|b| b.num_rows()).sum();
    let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(input_rows, output_rows, "row counts should match");
    assert_eq!(input_rows, num_rows as usize);

    // helper to extract string values handling different arrow string types
    fn extract_strings(batches: &[RecordBatch], col_name: &str) -> Vec<Option<String>> {
        let mut result = Vec::new();
        for batch in batches {
            let col = batch.column_by_name(col_name).unwrap();
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        result.push(Some(arr.value(i).to_string()));
                    }
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        result.push(Some(arr.value(i).to_string()));
                    }
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        result.push(Some(arr.value(i).to_string()));
                    }
                }
            } else {
                panic!("{} unexpected type: {:?}", col_name, col.data_type());
            }
        }
        result
    }

    // extract values from input file
    let mut input_ids = Vec::new();
    let mut input_int8s = Vec::new();
    let mut input_int16s = Vec::new();
    let mut input_int64s = Vec::new();
    let mut input_uint32s = Vec::new();
    let mut input_float32s = Vec::new();
    let mut input_float64s = Vec::new();
    let mut input_bools = Vec::new();
    let mut input_nullable_ints = Vec::new();
    let mut input_timestamps = Vec::new();

    for batch in &input_batches {
        let col = batch.column_by_name("id").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        input_ids.extend(arr.iter());

        let col = batch.column_by_name("int8_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
        input_int8s.extend(arr.iter());

        let col = batch.column_by_name("int16_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
        input_int16s.extend(arr.iter());

        let col = batch.column_by_name("int64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        input_int64s.extend(arr.iter());

        let col = batch.column_by_name("uint32_col").unwrap();
        let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
        input_uint32s.extend(arr.iter());

        let col = batch.column_by_name("float32_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
        input_float32s.extend(arr.iter());

        let col = batch.column_by_name("float64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        input_float64s.extend(arr.iter());

        let col = batch.column_by_name("bool_col").unwrap();
        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        input_bools.extend(arr.iter());

        let col = batch.column_by_name("nullable_int").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        input_nullable_ints.extend(arr.iter());

        let col = batch.column_by_name("timestamp_col").unwrap();
        let arr = col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        input_timestamps.extend(arr.iter());
    }

    let input_strings = extract_strings(&input_batches, "string_col");
    let input_nullable_strings = extract_strings(&input_batches, "nullable_string");

    // extract values from output file
    let mut output_ids = Vec::new();
    let mut output_int8s = Vec::new();
    let mut output_int16s = Vec::new();
    let mut output_int64s = Vec::new();
    let mut output_uint32s = Vec::new();
    let mut output_float32s = Vec::new();
    let mut output_float64s = Vec::new();
    let mut output_bools = Vec::new();
    let mut output_nullable_ints = Vec::new();
    let mut output_timestamps = Vec::new();

    for batch in &output_batches {
        let col = batch.column_by_name("id").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        output_ids.extend(arr.iter());

        let col = batch.column_by_name("int8_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
        output_int8s.extend(arr.iter());

        let col = batch.column_by_name("int16_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
        output_int16s.extend(arr.iter());

        let col = batch.column_by_name("int64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        output_int64s.extend(arr.iter());

        let col = batch.column_by_name("uint32_col").unwrap();
        let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
        output_uint32s.extend(arr.iter());

        let col = batch.column_by_name("float32_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
        output_float32s.extend(arr.iter());

        let col = batch.column_by_name("float64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        output_float64s.extend(arr.iter());

        let col = batch.column_by_name("bool_col").unwrap();
        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        output_bools.extend(arr.iter());

        let col = batch.column_by_name("nullable_int").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        output_nullable_ints.extend(arr.iter());

        let col = batch.column_by_name("timestamp_col").unwrap();
        let arr = col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        output_timestamps.extend(arr.iter());
    }

    let output_strings = extract_strings(&output_batches, "string_col");
    let output_nullable_strings = extract_strings(&output_batches, "nullable_string");

    // compare input vs output directly
    assert_eq!(input_ids, output_ids, "id values should match");
    assert_eq!(input_int8s, output_int8s, "int8 values should match");
    assert_eq!(input_int16s, output_int16s, "int16 values should match");
    assert_eq!(input_int64s, output_int64s, "int64 values should match");
    assert_eq!(input_uint32s, output_uint32s, "uint32 values should match");
    assert_eq!(
        input_float32s, output_float32s,
        "float32 values should match"
    );
    assert_eq!(
        input_float64s, output_float64s,
        "float64 values should match"
    );
    assert_eq!(input_bools, output_bools, "bool values should match");
    assert_eq!(input_strings, output_strings, "string values should match");
    assert_eq!(
        input_nullable_ints, output_nullable_ints,
        "nullable int values should match"
    );
    assert_eq!(
        input_nullable_strings, output_nullable_strings,
        "nullable string values should match"
    );
    assert_eq!(
        input_timestamps, output_timestamps,
        "timestamp values should match"
    );
}

/// One-off helper to verify all rows in a batch have the expected partition values.
/// This is entirely specific to the test data and not generalized at all.
fn verify_int32_partition_values(
    batches: &[RecordBatch],
    expected_year: i32,
    expected_month: i32,
    file_path: &str,
) {
    for batch in batches {
        let year_col = batch.column_by_name("year").unwrap();
        let month_col = batch.column_by_name("month").unwrap();
        let years = year_col.as_any().downcast_ref::<Int32Array>().unwrap();
        let months = month_col.as_any().downcast_ref::<Int32Array>().unwrap();

        for i in 0..batch.num_rows() {
            assert_eq!(
                years.value(i),
                expected_year,
                "Row {} in {} has wrong year: expected {}, got {}",
                i,
                file_path,
                expected_year,
                years.value(i)
            );
            assert_eq!(
                months.value(i),
                expected_month,
                "Row {} in {} has wrong month: expected {}, got {}",
                i,
                file_path,
                expected_month,
                months.value(i)
            );
        }
    }
}

fn verify_string_partition_values(
    batches: &[RecordBatch],
    expected_region: &str,
    expected_year: i32,
    file_path: &str,
) {
    for batch in batches {
        let region_col = batch.column_by_name("region").unwrap();
        let year_col = batch.column_by_name("year").unwrap();
        let regions = region_col.as_any().downcast_ref::<StringArray>().unwrap();
        let years = year_col.as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(
                regions.value(i),
                expected_region,
                "Row {} in {} has wrong region: expected {}, got {}",
                i,
                file_path,
                expected_region,
                regions.value(i)
            );
            assert_eq!(
                years.value(i),
                expected_year,
                "Row {} in {} has wrong year: expected {}, got {}",
                i,
                file_path,
                expected_year,
                years.value(i)
            );
        }
    }
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn test_multi_column_partition_verifies_data_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    // IMPORTANT:
    // We are creating UNSORTED data here. Why, you might ask?
    // Because the partitioner depends on the data being sorted
    // in order to function correctly and so we are testing that
    // transform correctly sorts the data BEFORE partitioning it.

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // again, data is intentionally not sorted by (year, month)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            // year: mixed order
            Arc::new(Int32Array::from(vec![
                2024, 2023, 2024, 2023, 2024, 2023, 2024, 2023,
            ])),
            // month: mixed order
            Arc::new(Int32Array::from(vec![1, 2, 2, 1, 1, 2, 2, 1])),
            // id: unique per row for verification
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
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
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Arrow), // be explicit to ensure we are testing the correct format
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let file_2023_1 = temp_dir.path().join("year=2023/month=1.arrow");
    let file_2023_2 = temp_dir.path().join("year=2023/month=2.arrow");
    let file_2024_1 = temp_dir.path().join("year=2024/month=1.arrow");
    let file_2024_2 = temp_dir.path().join("year=2024/month=2.arrow");

    assert!(file_2023_1.exists(), "2023/1 partition file should exist");
    assert!(file_2023_2.exists(), "2023/2 partition file should exist");
    assert!(file_2024_1.exists(), "2024/1 partition file should exist");
    assert!(file_2024_2.exists(), "2024/2 partition file should exist");

    let batches_2023_1 = test_helpers::read_arrow_file(&file_2023_1);
    let batches_2023_2 = test_helpers::read_arrow_file(&file_2023_2);
    let batches_2024_1 = test_helpers::read_arrow_file(&file_2024_1);
    let batches_2024_2 = test_helpers::read_arrow_file(&file_2024_2);

    verify_int32_partition_values(&batches_2023_1, 2023, 1, "2023/1");
    verify_int32_partition_values(&batches_2023_2, 2023, 2, "2023/2");
    verify_int32_partition_values(&batches_2024_1, 2024, 1, "2024/1");
    verify_int32_partition_values(&batches_2024_2, 2024, 2, "2024/2");

    // ids 4 and 8
    assert_eq!(count_rows(&batches_2023_1), 2, "2023/1 should have 2 rows");
    // ids 2 and 6
    assert_eq!(count_rows(&batches_2023_2), 2, "2023/2 should have 2 rows");
    // ids 1 and 5
    assert_eq!(count_rows(&batches_2024_1), 2, "2024/1 should have 2 rows");
    // ids 3 and 7
    assert_eq!(count_rows(&batches_2024_2), 2, "2024/2 should have 2 rows");

    assert_eq!(
        count_rows(&batches_2023_1)
            + count_rows(&batches_2023_2)
            + count_rows(&batches_2024_1)
            + count_rows(&batches_2024_2),
        8,
        "total rows should match input"
    );
}

#[tokio::test]
async fn test_multi_column_partition_verifies_data_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // unsorted data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![
                2024, 2023, 2024, 2023, 2024, 2023, 2024, 2023,
            ])),
            Arc::new(Int32Array::from(vec![1, 2, 2, 1, 1, 2, 2, 1])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}.parquet");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let file_2023_1 = temp_dir.path().join("year=2023/month=1.parquet");
    let file_2023_2 = temp_dir.path().join("year=2023/month=2.parquet");
    let file_2024_1 = temp_dir.path().join("year=2024/month=1.parquet");
    let file_2024_2 = temp_dir.path().join("year=2024/month=2.parquet");

    assert!(file_2023_1.exists());
    assert!(file_2023_2.exists());
    assert!(file_2024_1.exists());
    assert!(file_2024_2.exists());

    let batches_2023_1 = test_helpers::read_parquet_file(&file_2023_1);
    let batches_2023_2 = test_helpers::read_parquet_file(&file_2023_2);
    let batches_2024_1 = test_helpers::read_parquet_file(&file_2024_1);
    let batches_2024_2 = test_helpers::read_parquet_file(&file_2024_2);

    verify_int32_partition_values(&batches_2023_1, 2023, 1, "2023/1");
    verify_int32_partition_values(&batches_2023_2, 2023, 2, "2023/2");
    verify_int32_partition_values(&batches_2024_1, 2024, 1, "2024/1");
    verify_int32_partition_values(&batches_2024_2, 2024, 2, "2024/2");

    assert_eq!(count_rows(&batches_2023_1), 2);
    assert_eq!(count_rows(&batches_2023_2), 2);
    assert_eq!(count_rows(&batches_2024_1), 2);
    assert_eq!(count_rows(&batches_2024_2), 2);
}

#[tokio::test]
async fn test_multi_column_partition_three_columns_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("day", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // unsorted by (year, month, day)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2024, 2023, 2024, 2023])),
            Arc::new(Int32Array::from(vec![1, 1, 1, 1])),
            Arc::new(Int32Array::from(vec![15, 10, 10, 15])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}/day={{day}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month,day".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let file_2023_1_10 = temp_dir.path().join("year=2023/month=1/day=10.arrow");
    let file_2023_1_15 = temp_dir.path().join("year=2023/month=1/day=15.arrow");
    let file_2024_1_10 = temp_dir.path().join("year=2024/month=1/day=10.arrow");
    let file_2024_1_15 = temp_dir.path().join("year=2024/month=1/day=15.arrow");

    assert!(file_2023_1_10.exists(), "2023/1/10 should exist");
    assert!(file_2023_1_15.exists(), "2023/1/15 should exist");
    assert!(file_2024_1_10.exists(), "2024/1/10 should exist");
    assert!(file_2024_1_15.exists(), "2024/1/15 should exist");

    let batches = test_helpers::read_arrow_file(&file_2023_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = test_helpers::read_arrow_file(&file_2023_1_15);
    assert_eq!(count_rows(&batches), 1);
    let batches = test_helpers::read_arrow_file(&file_2024_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = test_helpers::read_arrow_file(&file_2024_1_15);
    assert_eq!(count_rows(&batches), 1);
}

#[tokio::test]
async fn test_multi_column_partition_three_columns_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("day", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2024, 2023, 2024, 2023])),
            Arc::new(Int32Array::from(vec![1, 1, 1, 1])),
            Arc::new(Int32Array::from(vec![15, 10, 10, 15])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}/day={{day}}.parquet");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month,day".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let file_2023_1_10 = temp_dir.path().join("year=2023/month=1/day=10.parquet");
    let file_2023_1_15 = temp_dir.path().join("year=2023/month=1/day=15.parquet");
    let file_2024_1_10 = temp_dir.path().join("year=2024/month=1/day=10.parquet");
    let file_2024_1_15 = temp_dir.path().join("year=2024/month=1/day=15.parquet");

    assert!(file_2023_1_10.exists(), "2023/1/10 should exist");
    assert!(file_2023_1_15.exists(), "2023/1/15 should exist");
    assert!(file_2024_1_10.exists(), "2024/1/10 should exist");
    assert!(file_2024_1_15.exists(), "2024/1/15 should exist");

    let batches = test_helpers::read_parquet_file(&file_2023_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = test_helpers::read_parquet_file(&file_2023_1_15);
    assert_eq!(count_rows(&batches), 1);
    let batches = test_helpers::read_parquet_file(&file_2024_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = test_helpers::read_parquet_file(&file_2024_1_15);
    assert_eq!(count_rows(&batches), 1);
}

#[tokio::test]
async fn test_multi_column_partition_mixed_types() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "us-west", "eu-west", "us-west", "eu-west", "us-west", "eu-west",
            ])),
            Arc::new(Int32Array::from(vec![2024, 2023, 2023, 2024, 2024, 2023])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir
        .path()
        .join("region={{region}}/year={{year}}.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("region,year".to_string()),
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let file_eu_2023 = temp_dir.path().join("region=eu-west/year=2023.arrow");
    let file_eu_2024 = temp_dir.path().join("region=eu-west/year=2024.arrow");
    let file_us_2023 = temp_dir.path().join("region=us-west/year=2023.arrow");
    let file_us_2024 = temp_dir.path().join("region=us-west/year=2024.arrow");

    assert!(file_eu_2023.exists(), "eu-west/2023 should exist");
    assert!(file_eu_2024.exists(), "eu-west/2024 should exist");
    assert!(file_us_2023.exists(), "us-west/2023 should exist");
    assert!(file_us_2024.exists(), "us-west/2024 should exist");

    // ids 2, 6
    assert_eq!(count_rows(&test_helpers::read_arrow_file(&file_eu_2023)), 2);
    // id 4
    assert_eq!(count_rows(&test_helpers::read_arrow_file(&file_eu_2024)), 1);
    // id 3
    assert_eq!(count_rows(&test_helpers::read_arrow_file(&file_us_2023)), 1);
    // ids 1, 5
    assert_eq!(count_rows(&test_helpers::read_arrow_file(&file_us_2024)), 2);

    let file_eu_2023_batches = test_helpers::read_arrow_file(&file_eu_2023);
    let file_eu_2024_batches = test_helpers::read_arrow_file(&file_eu_2024);
    let file_us_2023_batches = test_helpers::read_arrow_file(&file_us_2023);
    let file_us_2024_batches = test_helpers::read_arrow_file(&file_us_2024);

    verify_string_partition_values(&file_eu_2023_batches, "eu-west", 2023, "eu-west/2023");
    verify_string_partition_values(&file_eu_2024_batches, "eu-west", 2024, "eu-west/2024");
    verify_string_partition_values(&file_us_2023_batches, "us-west", 2023, "us-west/2023");
    verify_string_partition_values(&file_us_2024_batches, "us-west", 2024, "us-west/2024");
}

#[tokio::test]
async fn test_multi_column_partition_parquet_with_exclude() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}.parquet");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        exclude_columns: vec!["year".to_string(), "month".to_string()],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let file_2023 = temp_dir.path().join("year=2023/month=1.parquet");
    let file_2024 = temp_dir.path().join("year=2024/month=1.parquet");

    assert!(file_2023.exists());
    assert!(file_2024.exists());

    // verify partition columns are excluded from file
    let batches = test_helpers::read_parquet_file(&file_2023);
    assert_eq!(batches[0].num_columns(), 1, "only one column should remain");
    assert_eq!(
        batches[0].schema().field(0).name(),
        "id",
        "only 'id' column should remain"
    );
}

#[tokio::test]
async fn test_multi_column_partition_arrow_with_exclude() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![100, 200])),
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
        exclude_columns: vec!["year".to_string(), "month".to_string()],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let file_2023 = temp_dir.path().join("year=2023/month=1.arrow");
    let file_2024 = temp_dir.path().join("year=2024/month=1.arrow");

    assert!(file_2023.exists());
    assert!(file_2024.exists());

    let batches = test_helpers::read_arrow_file(&file_2023);
    assert_eq!(batches[0].num_columns(), 1, "only one column should remain");
    assert_eq!(
        batches[0].schema().field(0).name(),
        "id",
        "only 'id' column should remain"
    );
}

#[tokio::test]
async fn test_multi_column_partition_verifies_output_paths_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let list_output = temp_dir.path().join("outputs.json");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // unsorted data to ensure sorting happens
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2024, 2023, 2024, 2023])),
            Arc::new(Int32Array::from(vec![12, 6, 6, 12])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir
        .path()
        .join("data/year={{year}}/month={{month}}/data.arrow");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        exclude_columns: vec![],
        list_outputs: Some(ListOutputsFormat::Json),
        list_outputs_file: Some(Utf8PathBuf::from_path_buf(list_output.clone()).unwrap()),
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Arrow),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let outputs_json = std::fs::read_to_string(&list_output).unwrap();
    let outputs: serde_json::Value = serde_json::from_str(&outputs_json).unwrap();
    let files = outputs.as_array().unwrap();
    assert_eq!(files.len(), 4, "should have 4 partition files");

    for file in files {
        let path = file["path"].as_str().unwrap();
        let partition_values = file["partition_values"].as_array().unwrap();

        assert_eq!(partition_values.len(), 2);
        assert_eq!(partition_values[0]["column"], "year");
        assert_eq!(partition_values[1]["column"], "month");

        let year = partition_values[0]["value"].as_i64().unwrap();
        let month = partition_values[1]["value"].as_i64().unwrap();

        assert!(
            path.contains(&format!("year={}", year)),
            "path '{}' should contain year={}",
            path,
            year
        );
        assert!(
            path.contains(&format!("month={}", month)),
            "path '{}' should contain month={}",
            path,
            month
        );

        assert!(
            std::path::Path::new(path).exists(),
            "file should exist: {}",
            path
        );

        let batches = test_helpers::read_arrow_file(Path::new(path));
        let expected_year = i32::try_from(year).unwrap();
        let expected_month = i32::try_from(month).unwrap();
        for batch in &batches {
            let years = batch
                .column_by_name("year")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let months = batch
                .column_by_name("month")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                assert_eq!(
                    years.value(i),
                    expected_year,
                    "file {} row {} should have year={}",
                    path,
                    i,
                    year
                );
                assert_eq!(
                    months.value(i),
                    expected_month,
                    "file {} row {} should have month={}",
                    path,
                    i,
                    month
                );
            }
        }
    }
}

#[tokio::test]
async fn test_multi_column_partition_verifies_output_paths_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let list_output = temp_dir.path().join("outputs.json");

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "us-east", "eu-west", "us-east", "eu-west",
            ])),
            Arc::new(Int32Array::from(vec![2024, 2024, 2023, 2023])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    test_helpers::write_arrow_file(&input, &schema, vec![batch]);

    let template = temp_dir
        .path()
        .join("output/region={{region}}/year={{year}}.parquet");

    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        from_many: vec![],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("region,year".to_string()),
        exclude_columns: vec![],
        list_outputs: Some(ListOutputsFormat::Json),
        list_outputs_file: Some(Utf8PathBuf::from_path_buf(list_output.clone()).unwrap()),
        create_dirs: true,
        overwrite: false,
        query: None,
        dialect: QueryDialect::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    })
    .await
    .unwrap();

    let outputs_json = std::fs::read_to_string(&list_output).unwrap();
    let outputs: serde_json::Value = serde_json::from_str(&outputs_json).unwrap();
    let files = outputs.as_array().unwrap();
    assert_eq!(files.len(), 4, "should have 4 partition files");

    for file in files {
        let path = file["path"].as_str().unwrap();
        let partition_values = file["partition_values"].as_array().unwrap();

        assert_eq!(partition_values.len(), 2);
        assert_eq!(partition_values[0]["column"], "region");
        assert_eq!(partition_values[1]["column"], "year");

        let region = partition_values[0]["value"].as_str().unwrap();
        let year = partition_values[1]["value"].as_i64().unwrap();

        assert!(
            path.contains(&format!("region={}", region)),
            "path '{}' should contain region={}",
            path,
            region
        );
        assert!(
            path.contains(&format!("year={}", year)),
            "path '{}' should contain year={}",
            path,
            year
        );

        assert!(
            std::path::Path::new(path).exists(),
            "file should exist: {}",
            path
        );

        let batches = test_helpers::read_parquet_file(Path::new(path));
        let expected_year = i32::try_from(year).unwrap();
        for batch in &batches {
            let regions = batch
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let years = batch
                .column_by_name("year")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                assert_eq!(
                    regions.value(i),
                    region,
                    "file {} row {} should have region={}",
                    path,
                    i,
                    region
                );
                assert_eq!(
                    years.value(i),
                    expected_year,
                    "file {} row {} should have year={}",
                    path,
                    i,
                    year
                );
            }
        }
    }

    // verify all 4 expected paths exist with correct partition values
    let expected_paths = [
        ("output/region=eu-west/year=2023.parquet", "eu-west", 2023),
        ("output/region=eu-west/year=2024.parquet", "eu-west", 2024),
        ("output/region=us-east/year=2023.parquet", "us-east", 2023),
        ("output/region=us-east/year=2024.parquet", "us-east", 2024),
    ];

    for (rel_path, expected_region, expected_year) in expected_paths {
        let full_path = temp_dir.path().join(rel_path);
        assert!(
            full_path.exists(),
            "partition file {} should exist",
            rel_path
        );

        // verify file contents match the path
        let batches = test_helpers::read_parquet_file(&full_path);
        for batch in &batches {
            let regions = batch
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let years = batch
                .column_by_name("year")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                assert_eq!(
                    regions.value(i),
                    expected_region,
                    "file {} row {} should have region={}",
                    rel_path,
                    i,
                    expected_region
                );
                assert_eq!(
                    years.value(i),
                    expected_year,
                    "file {} row {} should have year={}",
                    rel_path,
                    i,
                    expected_year
                );
            }
        }
    }
}
