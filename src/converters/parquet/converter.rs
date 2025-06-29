use anyhow::{Result, anyhow};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

use crate::{
    BloomFilterConfig, ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortSpec,
    converters::arrow::ArrowConverter,
    utils::arrow_io::{ArrowIPCFormat, ArrowIPCReader},
};

use super::{ndv_calculator::NdvCalculator, writer_builder::ParquetWritePropertiesBuilder};

pub struct ParquetConverter {
    input_path: String,
    output_path: PathBuf,
    sort_spec: SortSpec,
    record_batch_size: usize,
    compression: ParquetCompression,
    bloom_filters: BloomFilterConfig,
    statistics: ParquetStatistics,
    parquet_row_group_size: usize,
    no_dictionary: bool,
    writer_version: ParquetWriterVersion,
    write_sorted_metadata: bool,
}

impl ParquetConverter {
    pub fn new(input_path: String, output_path: PathBuf) -> Self {
        Self {
            input_path,
            output_path,
            sort_spec: SortSpec::default(),
            record_batch_size: 122_880,
            compression: ParquetCompression::None,
            bloom_filters: BloomFilterConfig::default(),
            statistics: ParquetStatistics::Page,
            parquet_row_group_size: 1_048_576,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
            write_sorted_metadata: false,
        }
    }

    pub fn with_sort_spec(mut self, sort_spec: SortSpec) -> Self {
        self.sort_spec = sort_spec;
        self
    }

    pub fn with_record_batch_size(mut self, size: usize) -> Self {
        self.record_batch_size = size;
        self
    }

    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_bloom_filters(mut self, bloom_filters: BloomFilterConfig) -> Self {
        self.bloom_filters = bloom_filters;
        self
    }

    pub fn with_statistics(mut self, statistics: ParquetStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn with_parquet_row_group_size(mut self, size: usize) -> Self {
        self.parquet_row_group_size = size;
        self
    }

    pub fn with_no_dictionary(mut self, no_dictionary: bool) -> Self {
        self.no_dictionary = no_dictionary;
        self
    }

    pub fn with_writer_version(mut self, version: ParquetWriterVersion) -> Self {
        self.writer_version = version;
        self
    }

    pub fn with_write_sorted_metadata(mut self, write_sorted_metadata: bool) -> Self {
        self.write_sorted_metadata = write_sorted_metadata;
        self
    }

    pub async fn convert(&self) -> Result<()> {
        let (arrow_file_path, _temp_variable_to_keep_file_on_disk_until_end_of_function) =
            self.prepare_arrow_file().await?;

        let ndv_map = NdvCalculator::new(self.bloom_filters.clone(), self.parquet_row_group_size)
            .calculate(&arrow_file_path)
            .await?;

        let writer_properties = ParquetWritePropertiesBuilder::new(
            self.compression,
            self.statistics,
            self.writer_version,
            self.parquet_row_group_size,
            self.no_dictionary,
            self.bloom_filters.clone(),
            self.sort_spec.clone(),
        )
        .build(&arrow_file_path, &ndv_map)?;

        self.write_parquet(&arrow_file_path, writer_properties)
            .await?;

        Ok(())
    }

    async fn prepare_arrow_file(&self) -> Result<(PathBuf, Option<NamedTempFile>)> {
        if self.needs_intermediate_arrow_file() {
            let temp_file = NamedTempFile::new()?;
            let temp_path = temp_file.path().with_extension("arrow").to_path_buf();

            let mut arrow_converter = ArrowConverter::new(&self.input_path, &temp_path);

            if self.sort_spec.is_configured() {
                arrow_converter = arrow_converter.with_sorting(self.sort_spec.clone());
            }

            arrow_converter = arrow_converter.with_record_batch_size(self.record_batch_size);

            arrow_converter.convert().await?;

            Ok((temp_path, Some(temp_file)))
        } else {
            Ok((PathBuf::from(&self.input_path), None))
        }
    }

    fn needs_intermediate_arrow_file(&self) -> bool {
        ArrowIPCReader::is_stream_format(&self.input_path)
            || self.sort_spec.is_configured()
            || NdvCalculator::new(self.bloom_filters.clone(), self.parquet_row_group_size)
                .needs_calculation()
    }

    async fn write_parquet(
        &self,
        input_path: &Path,
        writer_properties: WriterProperties,
    ) -> Result<()> {
        let reader = ArrowIPCReader::from_path(
            input_path
                .to_str()
                .ok_or_else(|| anyhow!("Invalid path: {:?}", input_path))?,
        )?;
        let schema = reader
            .schema()
            .map_err(|e| anyhow!("Schema not found: {}", e))?;

        let file = File::create(&self.output_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(writer_properties))
            .map_err(|e| anyhow!("Failed to create Parquet writer: {}", e))?;

        match reader.format() {
            ArrowIPCFormat::File => {
                let file_reader = reader.file_reader()?;
                for batch in file_reader {
                    writer.write(&batch?)?;
                }
            }
            ArrowIPCFormat::Stream => {
                let stream_reader = reader.stream_reader()?;
                for batch in stream_reader {
                    writer.write(&batch?)?;
                }
            }
        }

        writer.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AllColumnsBloomFilterConfig, BloomFilterConfig, ColumnBloomFilterConfig,
        ColumnSpecificBloomFilterConfig, ParquetCompression, ParquetStatistics,
        ParquetWriterVersion, SortColumn, SortDirection, SortSpec,
        utils::test_helpers::{file_helpers, test_data, verify},
    };
    use arrow::array::{Array, Int32Array};
    use parquet::file::reader::FileReader;
    use tempfile::tempdir;

    use crate::converters::parquet::ParquetConverter;

    #[tokio::test]
    async fn test_converter_basic_file_to_parquet() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3, 4, 5];
        let test_names = vec!["Alice", "Bob", "Charlie", "David", "Eve"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        );
        converter.convert().await.unwrap();

        assert!(output_path.exists());
        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
    }

    #[tokio::test]
    async fn test_converter_basic_stream_to_parquet() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        );
        converter.convert().await.unwrap();

        assert!(output_path.exists());
        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
    }

    #[tokio::test]
    async fn test_converter_with_compression() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        for compression in [
            ParquetCompression::Zstd,
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Lz4,
        ] {
            let output = temp_dir
                .path()
                .join(format!("output_{:?}.parquet", compression));
            let converter =
                ParquetConverter::new(input_path.to_str().unwrap().to_string(), output.clone())
                    .with_compression(compression);
            converter.convert().await.unwrap();
            assert!(output.exists());
        }
    }

    #[tokio::test]
    async fn test_converter_with_sorting() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![5, 2, 4, 1, 3];
        let test_names = vec!["Eve", "Bob", "David", "Alice", "Charlie"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(sort_spec);
        converter.convert().await.unwrap();

        let file = std::fs::File::open(&output_path).unwrap();
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
        assert_eq!(ids.value(3), 4);
        assert_eq!(ids.value(4), 5);
    }

    #[tokio::test]
    async fn test_converter_with_statistics() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        for stats in [
            ParquetStatistics::None,
            ParquetStatistics::Chunk,
            ParquetStatistics::Page,
        ] {
            let output = temp_dir.path().join(format!("output_{:?}.parquet", stats));
            let converter =
                ParquetConverter::new(input_path.to_str().unwrap().to_string(), output.clone())
                    .with_statistics(stats);
            converter.convert().await.unwrap();
            assert!(output.exists());
        }
    }

    #[tokio::test]
    async fn test_converter_with_writer_version() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        for version in [ParquetWriterVersion::V1, ParquetWriterVersion::V2] {
            let output = temp_dir
                .path()
                .join(format!("output_{:?}.parquet", version));
            let converter =
                ParquetConverter::new(input_path.to_str().unwrap().to_string(), output.clone())
                    .with_writer_version(version);
            converter.convert().await.unwrap();
            assert!(output.exists());
        }
    }

    #[tokio::test]
    async fn test_converter_with_dictionary_encoding() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_no_dictionary(true);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_with_row_group_size() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        let mut batches = Vec::new();
        for i in 0..10 {
            let test_ids = vec![i * 3 + 1, i * 3 + 2, i * 3 + 3];
            let test_names = vec!["A", "B", "C"];
            batches.push(test_data::create_batch_with_ids_and_names(
                &schema,
                &test_ids,
                &test_names,
            ));
        }
        file_helpers::write_arrow_file(&input_path, &schema, batches).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_parquet_row_group_size(10);
        converter.convert().await.unwrap();

        let file = std::fs::File::open(&output_path).unwrap();
        let parquet_reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        let metadata = parquet_reader.metadata();
        assert!(metadata.num_row_groups() > 1);
    }

    #[tokio::test]
    async fn test_converter_empty_file() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        file_helpers::write_arrow_stream(&input_path, &schema, vec![]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        );
        converter.convert().await.unwrap();

        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_converter_invalid_input_path() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let converter = ParquetConverter::new("/nonexistent/file.arrow".to_string(), output_path);
        let result = converter.convert().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_converter_corrupted_file() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("corrupted.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        file_helpers::write_invalid_file(&input_path).unwrap();

        let converter =
            ParquetConverter::new(input_path.to_str().unwrap().to_string(), output_path);
        let result = converter.convert().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_converter_with_bloom_filter_all_columns() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3, 1, 2];
        let test_names = vec!["A", "B", "C", "A", "B"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: 0.001 });

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_bloom_filters(bloom_config);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_with_bloom_filter_specific_columns() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3, 1, 2];
        let test_names = vec!["A", "B", "C", "A", "B"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let bloom_config = BloomFilterConfig::Columns(vec![
            ColumnSpecificBloomFilterConfig {
                name: "id".to_string(),
                config: ColumnBloomFilterConfig { fpp: Some(0.01) },
            },
            ColumnSpecificBloomFilterConfig {
                name: "name".to_string(),
                config: ColumnBloomFilterConfig { fpp: None },
            },
        ]);

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_bloom_filters(bloom_config);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_bloom_filter_invalid_column() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "nonexistent_column".to_string(),
            config: ColumnBloomFilterConfig { fpp: Some(0.01) },
        }]);

        let converter =
            ParquetConverter::new(input_path.to_str().unwrap().to_string(), output_path)
                .with_bloom_filters(bloom_config);
        let result = converter.convert().await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not found in schema")
        );
    }

    #[tokio::test]
    async fn test_converter_with_sorted_metadata() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![5, 2, 4, 1, 3];
        let test_names = vec!["Eve", "Bob", "David", "Alice", "Charlie"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(sort_spec)
        .with_write_sorted_metadata(true);
        converter.convert().await.unwrap();

        // TODO: Once parquet-rs exposes sort metadata reading, verify it here
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_multi_column_sort_metadata() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_groups = vec![1, 2, 1, 2, 1];
        let test_values = vec![30, 20, 10, 40, 20];

        let schema = test_data::multi_column_for_sorting_schema();
        let batch =
            test_data::create_multi_column_for_sorting_batch(&schema, &test_groups, &test_values);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![
                SortColumn {
                    name: "group".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "value".to_string(),
                    direction: SortDirection::Descending,
                },
            ],
        };

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(sort_spec)
        .with_write_sorted_metadata(true);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_sort_invalid_column() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "nonexistent_column".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let converter =
            ParquetConverter::new(input_path.to_str().unwrap().to_string(), output_path)
                .with_sort_spec(sort_spec)
                .with_write_sorted_metadata(true);
        let result = converter.convert().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_converter_full_configuration() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![5, 2, 4, 1, 3, 5, 2];
        let test_names = vec!["Eve", "Bob", "David", "Alice", "Charlie", "Eve", "Bob"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "id".to_string(),
            config: ColumnBloomFilterConfig { fpp: Some(0.001) },
        }]);

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(sort_spec)
        .with_compression(ParquetCompression::Zstd)
        .with_bloom_filters(bloom_config)
        .with_statistics(ParquetStatistics::Page)
        .with_parquet_row_group_size(100_000)
        .with_no_dictionary(false)
        .with_writer_version(ParquetWriterVersion::V2)
        .with_write_sorted_metadata(true)
        .with_record_batch_size(1000);

        converter.convert().await.unwrap();

        assert!(output_path.exists());
        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 2);
        assert_eq!(ids.value(3), 3);
        assert_eq!(ids.value(4), 4);
        assert_eq!(ids.value(5), 5);
        assert_eq!(ids.value(6), 5);
    }
}
