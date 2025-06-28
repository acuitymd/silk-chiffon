use anyhow::{Result, anyhow};
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use datafusion::{
    functions_aggregate::expr_fn::count_distinct,
    prelude::{SessionContext, col},
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::{
        EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
    },
    format::SortingColumn,
    schema::types::ColumnPath,
};
use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

use crate::{
    BloomFilterConfig, ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortDirection,
    SortSpec,
    converters::arrow::ArrowConverter,
    utils::arrow_io::{ArrowIPCFormat, ArrowIPCReader},
};

pub struct ParquetConverter {
    input_path: String,
    output_path: PathBuf,
    sort_spec: Option<SortSpec>,
    record_batch_size: usize, // Size of Arrow record batches for processing
    compression: ParquetCompression,
    bloom_filters: Option<BloomFilterConfig>,
    statistics: ParquetStatistics,
    parquet_row_group_size: usize, // Rows per row group in output Parquet file
    no_dictionary: bool,
    writer_version: ParquetWriterVersion,
    write_sorted_metadata: bool,
}

impl ParquetConverter {
    pub fn new(input_path: String, output_path: PathBuf) -> Self {
        Self {
            input_path,
            output_path,
            sort_spec: None,
            record_batch_size: 122_880,
            compression: ParquetCompression::None,
            bloom_filters: None,
            statistics: ParquetStatistics::Page,
            parquet_row_group_size: 1_048_576,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
            write_sorted_metadata: false,
        }
    }

    pub fn with_sort_spec(mut self, sort_spec: Option<SortSpec>) -> Self {
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

    pub fn with_bloom_filters(mut self, bloom_filters: Option<BloomFilterConfig>) -> Self {
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
        let (arrow_file_path, _temp_keep_alive) = if self.input_is_arrow_stream()
            || self.sort_spec.is_some()
            || self.needs_ndv_calculation()
        {
            // create a temp file to house the sorted/converted arrow file
            // which will be deleted when the value is dropped at the end
            // of the function
            let temp_file = NamedTempFile::new()?;
            let temp_path = temp_file.path().to_path_buf();

            let mut arrow_converter = ArrowConverter::new(&self.input_path, &temp_path);

            if let Some(ref sort_spec) = self.sort_spec {
                arrow_converter = arrow_converter.with_sorting(sort_spec.clone());
            }

            arrow_converter = arrow_converter.with_record_batch_size(self.record_batch_size);

            arrow_converter.convert().await?;

            (temp_path, Some(temp_file))
        } else {
            (PathBuf::from(&self.input_path), None)
        };

        let ndv_map = if self.needs_ndv_calculation() {
            self.calculate_ndv(&arrow_file_path).await?
        } else {
            HashMap::new()
        };

        let writer_properties = self.create_writer_properties(&arrow_file_path, &ndv_map)?;

        self.write_parquet(&arrow_file_path, writer_properties)
            .await?;

        Ok(())
    }

    fn input_is_arrow_stream(&self) -> bool {
        ArrowIPCReader::from_path(&self.input_path)
            .is_ok_and(|r| r.format() == ArrowIPCFormat::Stream)
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

    fn needs_ndv_calculation(&self) -> bool {
        self.bloom_filters.is_some()
    }

    async fn calculate_ndv(&self, arrow_file_path: &Path) -> Result<HashMap<String, u64>> {
        let ctx = SessionContext::new();

        let arrow_reader = ArrowIPCReader::from_path(
            arrow_file_path
                .to_str()
                .ok_or_else(|| anyhow!("Invalid path: {:?}", arrow_file_path))?,
        )?;
        let schema = arrow_reader.schema()?;

        let file_reader = arrow_reader.file_reader()?;
        let provider = datafusion::datasource::MemTable::try_new(
            file_reader.schema(),
            vec![file_reader.collect::<Result<Vec<_>, _>>()?],
        )?;

        ctx.register_table("arrow_table", std::sync::Arc::new(provider))?;
        let df = ctx.table("arrow_table").await?;

        let columns = if let Some(bloom_filters) = &self.bloom_filters {
            if bloom_filters.all_columns.is_some() {
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect()
            } else {
                bloom_filters
                    .specific_columns
                    .iter()
                    .filter(|col| col.size_config.ndv.is_none())
                    .map(|col| col.name.clone())
                    .collect()
            }
        } else {
            vec![]
        };

        self.validate_column_names(&columns, &schema)?;

        if columns.is_empty() {
            return Ok(HashMap::new());
        }

        let agg_exprs: Vec<_> = columns
            .iter()
            .map(|col_name| count_distinct(col(col_name)).alias(col_name))
            .collect();

        let result_df = df.aggregate(vec![], agg_exprs)?;
        let batches = result_df.collect().await?;

        self.extract_ndv_results(batches, &columns)
    }

    fn extract_ndv_results(
        &self,
        batches: Vec<RecordBatch>,
        column_names: &[String],
    ) -> Result<HashMap<String, u64>> {
        if batches.is_empty() {
            return Ok(HashMap::new());
        }

        if batches.len() != 1 {
            return Err(anyhow!(
                "Expected exactly one result batch, got {}",
                batches.len()
            ));
        }

        let batch = &batches[0];
        let mut results = HashMap::new();

        for column_name in column_names {
            let array = batch
                .column_by_name(column_name)
                .ok_or_else(|| anyhow!("NDV result column '{}' not found", column_name))?;

            if array.len() != 1 {
                return Err(anyhow!(
                    "Expected single NDV result row, got {}",
                    array.len()
                ));
            }

            let count_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| anyhow!("NDV result is not Int64 type"))?;

            let ndv_value = count_array.value(0);
            results.insert(column_name.clone(), ndv_value as u64);
        }

        Ok(results)
    }

    fn validate_column_names(&self, columns: &[String], schema: &Schema) -> Result<()> {
        let invalid_columns: Vec<_> = columns
            .iter()
            .filter(|col_name| schema.field_with_name(col_name).is_err())
            .collect();

        if !invalid_columns.is_empty() {
            let available_columns: Vec<_> = schema.fields().iter().map(|f| f.name()).collect();
            return Err(anyhow!(
                "Column(s) {:?} not found in schema. Available columns: {:?}",
                invalid_columns,
                available_columns
            ));
        }

        Ok(())
    }

    fn create_writer_properties(
        &self,
        input_path: &Path,
        ndv_map: &HashMap<String, u64>,
    ) -> Result<WriterProperties> {
        let schema = ArrowIPCReader::from_path(input_path)?.schema()?;

        let builder = WriterProperties::builder()
            .set_max_row_group_size(self.parquet_row_group_size)
            .set_compression(match self.compression {
                ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
                ParquetCompression::Snappy => Compression::SNAPPY,
                ParquetCompression::Gzip => Compression::GZIP(GzipLevel::default()),
                ParquetCompression::Lz4 => Compression::LZ4_RAW,
                ParquetCompression::None => Compression::UNCOMPRESSED,
            })
            .set_writer_version(match self.writer_version {
                ParquetWriterVersion::V1 => WriterVersion::PARQUET_1_0,
                ParquetWriterVersion::V2 => WriterVersion::PARQUET_2_0,
            })
            .set_statistics_enabled(match self.statistics {
                ParquetStatistics::None => EnabledStatistics::None,
                ParquetStatistics::Chunk => EnabledStatistics::Chunk,
                ParquetStatistics::Page => EnabledStatistics::Page,
            })
            .set_dictionary_enabled(!self.no_dictionary);

        let builder = if self.bloom_filters.is_some() {
            self.apply_bloom_filters(builder, ndv_map)?
        } else {
            builder
        };

        let builder = if self.write_sorted_metadata && self.sort_spec.is_some() {
            self.apply_sort_metadata(builder, &schema)?
        } else {
            builder
        };

        Ok(builder.build())
    }

    fn apply_bloom_filters(
        &self,
        mut builder: WriterPropertiesBuilder,
        ndv_map: &HashMap<String, u64>,
    ) -> Result<WriterPropertiesBuilder> {
        if let Some(bloom_all) = &self
            .bloom_filters
            .as_ref()
            .and_then(|b| b.all_columns.as_ref())
        {
            let fpp = bloom_all.fpp.unwrap_or(0.01);
            builder = builder
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_fpp(fpp);

            for (col_name, &ndv) in ndv_map {
                let col_path = ColumnPath::from(col_name.as_str());
                builder = builder.set_column_bloom_filter_ndv(col_path, ndv);
            }
        }

        for bloom_col in &self.bloom_filters.as_ref().unwrap().specific_columns {
            let col_path = ColumnPath::from(bloom_col.name.as_str());
            let fpp = bloom_col.size_config.fpp.unwrap_or(0.01);

            let ndv = bloom_col
                .size_config
                .ndv
                .or_else(|| ndv_map.get(&bloom_col.name).copied())
                .ok_or_else(|| anyhow!("NDV not available for column {}", bloom_col.name))?;

            builder = builder
                .set_column_bloom_filter_enabled(col_path.clone(), true)
                .set_column_bloom_filter_fpp(col_path.clone(), fpp)
                .set_column_bloom_filter_ndv(col_path, ndv);
        }

        Ok(builder)
    }

    fn apply_sort_metadata(
        &self,
        builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
    ) -> Result<WriterPropertiesBuilder> {
        if self.sort_spec.is_none() {
            return Ok(builder);
        }

        let mut sorting_columns = Vec::new();

        for sort_col in &self.sort_spec.as_ref().unwrap().columns {
            let column_idx = schema
                .index_of(&sort_col.name)
                .map_err(|_| anyhow!("Sort column '{}' not found in schema", sort_col.name))?;

            let descending = sort_col.direction == SortDirection::Descending;

            sorting_columns.push(SortingColumn {
                column_idx: column_idx as i32,
                descending,
                nulls_first: descending,
            });
        }

        Ok(builder.set_sorting_columns(Some(sorting_columns)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AllColumnsBloomFilterSizeConfig, ColumnBloomFilterConfig, ColumnBloomFilterSizeConfig,
        SortColumn,
        utils::test_helpers::{file_helpers, test_data, verify},
    };
    use arrow::array::{Array, Int32Array};
    use parquet::file::reader::FileReader;
    use tempfile::tempdir;

    mod parquet_converter_tests {
        use super::*;

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
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
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
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
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
            .with_sort_spec(Some(sort_spec));
            converter.convert().await.unwrap();

            let file = std::fs::File::open(&output_path).unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
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
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
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

            let converter =
                ParquetConverter::new("/nonexistent/file.arrow".to_string(), output_path);
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
    }

    mod bloom_filter_tests {
        use super::*;

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

            let bloom_config = BloomFilterConfig {
                all_columns: Some(AllColumnsBloomFilterSizeConfig { fpp: Some(0.001) }),
                specific_columns: vec![],
            };

            let converter = ParquetConverter::new(
                input_path.to_str().unwrap().to_string(),
                output_path.clone(),
            )
            .with_bloom_filters(Some(bloom_config));
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

            let bloom_config = BloomFilterConfig {
                all_columns: None,
                specific_columns: vec![
                    ColumnBloomFilterConfig {
                        name: "id".to_string(),
                        size_config: ColumnBloomFilterSizeConfig {
                            fpp: Some(0.01),
                            ndv: None,
                        },
                    },
                    ColumnBloomFilterConfig {
                        name: "name".to_string(),
                        size_config: ColumnBloomFilterSizeConfig {
                            fpp: None,
                            ndv: Some(3),
                        },
                    },
                ],
            };

            let converter = ParquetConverter::new(
                input_path.to_str().unwrap().to_string(),
                output_path.clone(),
            )
            .with_bloom_filters(Some(bloom_config));
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

            let bloom_config = BloomFilterConfig {
                all_columns: None,
                specific_columns: vec![ColumnBloomFilterConfig {
                    name: "nonexistent_column".to_string(),
                    size_config: ColumnBloomFilterSizeConfig {
                        fpp: Some(0.01),
                        ndv: None,
                    },
                }],
            };

            let converter =
                ParquetConverter::new(input_path.to_str().unwrap().to_string(), output_path)
                    .with_bloom_filters(Some(bloom_config));
            let result = converter.convert().await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("not found in schema")
            );
        }
    }

    mod sorting_metadata_tests {
        use super::*;

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
            .with_sort_spec(Some(sort_spec))
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
            let batch = test_data::create_multi_column_for_sorting_batch(
                &schema,
                &test_groups,
                &test_values,
            );
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
            .with_sort_spec(Some(sort_spec))
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
                    .with_sort_spec(Some(sort_spec))
                    .with_write_sorted_metadata(true);
            let result = converter.convert().await;

            assert!(result.is_err());
        }
    }

    mod integration_tests {
        use super::*;

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

            let bloom_config = BloomFilterConfig {
                all_columns: None,
                specific_columns: vec![ColumnBloomFilterConfig {
                    name: "id".to_string(),
                    size_config: ColumnBloomFilterSizeConfig {
                        fpp: Some(0.001),
                        ndv: None,
                    },
                }],
            };

            let converter = ParquetConverter::new(
                input_path.to_str().unwrap().to_string(),
                output_path.clone(),
            )
            .with_sort_spec(Some(sort_spec))
            .with_compression(ParquetCompression::Zstd)
            .with_bloom_filters(Some(bloom_config))
            .with_statistics(ParquetStatistics::Page)
            .with_parquet_row_group_size(100_000)
            .with_no_dictionary(false)
            .with_writer_version(ParquetWriterVersion::V2)
            .with_write_sorted_metadata(true)
            .with_record_batch_size(1000);

            converter.convert().await.unwrap();

            assert!(output_path.exists());
            let file = std::fs::File::open(&output_path).unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
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
}
