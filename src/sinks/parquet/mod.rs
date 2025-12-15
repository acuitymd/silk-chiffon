mod writer;

pub use writer::ParquetWriter;

use futures::stream::StreamExt;
use parquet::{
    file::{
        metadata::SortingColumn,
        properties::{WriterProperties, WriterPropertiesBuilder},
    },
    schema::types::ColumnPath,
};
use std::{collections::HashMap, path::PathBuf, sync::Mutex};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;

use crate::{
    BloomFilterConfig, ColumnEncodingConfig, ParquetCompression, ParquetEncoding,
    ParquetStatistics, ParquetWriterVersion, SortDirection, SortSpec,
    sinks::data_sink::{DataSink, SinkResult},
};

pub struct ParquetSinkOptions {
    record_batch_size: usize,
    max_row_group_size: usize,
    max_parallelism: Option<usize>,
    sort_spec: SortSpec,
    compression: ParquetCompression,
    bloom_filters: BloomFilterConfig,
    statistics: ParquetStatistics,
    no_dictionary: bool,
    writer_version: ParquetWriterVersion,
    encoding: Option<ParquetEncoding>,
    column_encodings: Vec<ColumnEncodingConfig>,
    ndv_map: HashMap<String, u64>,
    metadata: HashMap<String, Option<String>>,
}

impl Default for ParquetSinkOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetSinkOptions {
    pub fn new() -> Self {
        Self {
            record_batch_size: 122_880,
            max_row_group_size: 1_048_576,
            max_parallelism: None,
            sort_spec: SortSpec::default(),
            compression: ParquetCompression::default(),
            bloom_filters: BloomFilterConfig::default(),
            statistics: ParquetStatistics::default(),
            no_dictionary: false,
            writer_version: ParquetWriterVersion::default(),
            encoding: None,
            column_encodings: Vec::new(),
            ndv_map: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.record_batch_size = record_batch_size;
        self
    }

    pub fn with_max_row_group_size(mut self, max_row_group_size: usize) -> Self {
        self.max_row_group_size = max_row_group_size;
        self
    }

    pub fn with_max_parallelism(mut self, max_parallelism: Option<usize>) -> Self {
        self.max_parallelism = max_parallelism;
        self
    }

    pub fn with_sort_spec(mut self, sort_spec: SortSpec) -> Self {
        self.sort_spec = sort_spec;
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

    pub fn with_no_dictionary(mut self, no_dictionary: bool) -> Self {
        self.no_dictionary = no_dictionary;
        self
    }

    pub fn with_writer_version(mut self, writer_version: ParquetWriterVersion) -> Self {
        self.writer_version = writer_version;
        self
    }

    pub fn with_encoding(mut self, encoding: Option<ParquetEncoding>) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn with_column_encodings(mut self, column_encodings: Vec<ColumnEncodingConfig>) -> Self {
        self.column_encodings = column_encodings;
        self
    }

    pub fn with_metadata_value(mut self, key: String, value: Option<String>) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, Option<String>>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_ndv_map(mut self, ndv_map: HashMap<String, u64>) -> Self {
        self.ndv_map = ndv_map;
        self
    }
}

pub struct ParquetSinkInner {
    path: PathBuf,
    writer: Option<ParquetWriter>,
}

pub struct ParquetSink {
    inner: Mutex<ParquetSinkInner>,
}

impl ParquetSink {
    pub fn create(path: PathBuf, schema: &SchemaRef, options: &ParquetSinkOptions) -> Result<Self> {
        let mut writer_builder = WriterProperties::builder()
            .set_max_row_group_size(options.max_row_group_size)
            .set_compression(options.compression.into())
            .set_writer_version(options.writer_version.into())
            .set_statistics_enabled(options.statistics.into())
            .set_dictionary_enabled(!options.no_dictionary);

        writer_builder =
            Self::apply_encodings(writer_builder, &options.encoding, &options.column_encodings);

        writer_builder = Self::apply_bloom_filters(
            writer_builder,
            schema,
            &options.bloom_filters,
            &options.ndv_map,
        )?;

        writer_builder = Self::apply_sort_metadata(&options.sort_spec, writer_builder, schema)?;

        let props = writer_builder.build();

        let writer = ParquetWriter::try_new(
            &path,
            schema,
            props,
            options.max_row_group_size,
            options.max_parallelism,
        )?;

        let inner = ParquetSinkInner {
            path,
            writer: Some(writer),
        };

        let sink = Self {
            inner: Mutex::new(inner),
        };

        Ok(sink)
    }

    fn apply_encodings(
        mut builder: WriterPropertiesBuilder,
        default_encoding: &Option<ParquetEncoding>,
        column_encodings: &[ColumnEncodingConfig],
    ) -> WriterPropertiesBuilder {
        if let Some(encoding) = default_encoding {
            builder = builder.set_encoding((*encoding).into());
        }

        for col_encoding in column_encodings {
            let col_path = ColumnPath::from(col_encoding.name.as_str());
            builder = builder.set_column_encoding(col_path, col_encoding.encoding.into());
        }

        builder
    }

    fn apply_bloom_filters(
        mut builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
        bloom_filters: &BloomFilterConfig,
        ndv_map: &HashMap<String, u64>,
    ) -> Result<WriterPropertiesBuilder> {
        match &bloom_filters {
            BloomFilterConfig::None => Ok(builder),
            BloomFilterConfig::All(bloom_all) => {
                let fpp = bloom_all.fpp;
                builder = builder
                    .set_bloom_filter_enabled(true)
                    .set_bloom_filter_fpp(fpp);

                if let Some(user_ndv) = bloom_all.ndv {
                    for field in schema.fields() {
                        let col_path = ColumnPath::from(field.name().as_str());
                        builder = builder.set_column_bloom_filter_ndv(col_path, user_ndv);
                    }
                } else {
                    for (col_name, &ndv) in ndv_map {
                        let col_path = ColumnPath::from(col_name.as_str());
                        builder = builder.set_column_bloom_filter_ndv(col_path, ndv);
                    }
                }
                Ok(builder)
            }
            BloomFilterConfig::Columns(columns) => {
                for bloom_col in columns {
                    let col_path = ColumnPath::from(bloom_col.name.as_str());
                    let fpp = bloom_col.config.fpp;

                    builder = builder
                        .set_column_bloom_filter_enabled(col_path.clone(), true)
                        .set_column_bloom_filter_fpp(col_path.clone(), fpp);

                    let ndv = bloom_col
                        .config
                        .ndv
                        .or_else(|| ndv_map.get(&bloom_col.name).copied());
                    if let Some(ndv) = ndv {
                        builder = builder.set_column_bloom_filter_ndv(col_path, ndv);
                    }
                }
                Ok(builder)
            }
        }
    }

    fn apply_sort_metadata(
        sort_spec: &SortSpec,
        builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
    ) -> Result<WriterPropertiesBuilder> {
        if sort_spec.is_empty() {
            return Ok(builder);
        }

        let mut sorting_columns = Vec::new();

        for sort_col in &sort_spec.columns {
            let column_idx = schema
                .index_of(&sort_col.name)
                .map_err(|_| anyhow!("Sort column '{}' not found in schema", sort_col.name))?;

            let descending = sort_col.direction == SortDirection::Descending;

            sorting_columns.push(SortingColumn {
                column_idx: i32::try_from(column_idx)
                    .map_err(|_| anyhow!("Column index out of range"))?,
                descending,
                nulls_first: descending,
            });
        }

        Ok(builder.set_sorting_columns(Some(sorting_columns)))
    }
}

#[async_trait]
impl DataSink for ParquetSink {
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<SinkResult> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.write_batch(batch).await?;
        }

        self.finish().await
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow!("Failed to lock inner: {}", e))?;

        let writer = inner
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("Writer already closed"))?;
        writer.write(&batch)?;

        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow!("Failed to lock inner: {}", e))?;

        let writer = inner
            .writer
            .take()
            .ok_or_else(|| anyhow!("Writer already closed"))?;
        let rows_written = writer.close()?;

        Ok(SinkResult {
            files_written: vec![inner.path.clone()],
            rows_written,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        AllColumnsBloomFilterConfig, ColumnBloomFilterConfig, ColumnSpecificBloomFilterConfig,
        SortColumn, SortDirection,
        sources::data_source::DataSource,
        utils::parquet_inspection::read_entire_parquet_file,
        utils::test_helpers::{file_helpers, test_data, verify},
    };

    use tempfile::tempdir;

    mod parquet_sink_tests {
        use super::*;

        #[tokio::test]
        async fn test_sink_writes_single_batch_with_single_row_group() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &ParquetSinkOptions::new())
                    .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);
            assert_eq!(result.files_written.len(), 1);
            assert_eq!(result.files_written[0], output_path);

            let batches = verify::read_parquet_file(&output_path).unwrap();
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(file.row_groups.len(), 1);
        }

        #[tokio::test]
        async fn test_sink_writes_multiple_batches_with_single_row_group() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[3, 4], &["c", "d"]);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &ParquetSinkOptions::new())
                    .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 4);

            let batches = verify::read_parquet_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 4);

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(file.row_groups.len(), 1);
        }

        #[tokio::test]
        async fn test_sink_multiple_row_groups() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[3, 4], &["c", "d"]);
            let batch3 = test_data::create_batch_with_ids_and_names(&schema, &[5], &["e"]);

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_max_row_group_size(3),
            )
            .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            sink.write_batch(batch3).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 5);

            let batches = verify::read_parquet_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(file.row_groups.len(), 2);
        }

        #[tokio::test]
        async fn test_sink_with_compression() {
            for compression in [
                ParquetCompression::Zstd,
                ParquetCompression::Snappy,
                ParquetCompression::Gzip,
                ParquetCompression::Lz4,
            ] {
                let temp_dir = tempdir().unwrap();
                let output_path = temp_dir.path().join("output.parquet");

                let schema = test_data::simple_schema();
                // need enough data with enough repetition to benefit from compression
                let batch = test_data::create_batch_with_ids_and_names(
                    &schema,
                    &[
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                    ],
                    &[
                        "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj",
                        "kkk", "lll", "mmm", "nnn", "ooo", "ppp", "qqq", "rrr", "sss", "ttt",
                        "uuu", "vvv", "www", "xxx", "yyy", "zzz", "AAA", "BBB", "CCC", "DDD",
                        "EEE", "FFF", "GGG", "HHH", "III", "JJJ", "KKK", "LLL", "MMM", "NNN",
                        "OOO", "PPP", "QQQ", "RRR", "SSS", "TTT", "UUU", "VVV", "WWW", "XXX",
                        "YYY", "ZZZ", "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh",
                        "iii", "jjj", "kkk", "lll", "mmm", "nnn", "ooo", "ppp", "qqq", "rrr",
                        "sss", "ttt", "uuu", "vvv", "www", "xxx", "yyy", "zzz", "AAA", "BBB",
                        "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III", "JJJ", "KKK", "LLL",
                        "MMM", "NNN", "OOO", "PPP", "QQQ", "RRR", "SSS", "TTT", "UUU", "VVV",
                    ],
                );

                let mut compressed_sink = ParquetSink::create(
                    output_path.clone(),
                    &schema,
                    &ParquetSinkOptions::new().with_compression(compression),
                )
                .unwrap();

                compressed_sink.write_batch(batch.clone()).await.unwrap();
                compressed_sink.finish().await.unwrap();

                let file = read_entire_parquet_file(&output_path).unwrap();
                assert_eq!(file.row_groups.len(), 1);
                assert!(file.total_compressed_size_bytes < file.total_uncompressed_size_bytes);
            }
        }

        #[tokio::test]
        async fn test_sink_empty_batches() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &ParquetSinkOptions::new())
                    .unwrap();

            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 0);
            assert!(output_path.exists());

            let batches = verify::read_parquet_file(&output_path).unwrap();
            assert_eq!(batches.len(), 0);
        }

        #[tokio::test]
        async fn test_sink_write_stream() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch1 =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[4, 5], &["d", "e"]);

            file_helpers::write_arrow_file(&input_path, &schema, vec![batch1, batch2]).unwrap();

            let source = crate::sources::arrow::ArrowDataSource::new(
                input_path.to_str().unwrap().to_string(),
            );
            let stream = source.as_stream().await.unwrap();

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &ParquetSinkOptions::new())
                    .unwrap();

            let result = sink.write_stream(stream).await.unwrap();

            assert_eq!(result.rows_written, 5);

            let batches = verify::read_parquet_file(&output_path).unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
        }

        #[tokio::test]
        async fn test_sink_with_sort_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[3, 1, 2], &["c", "a", "b"]);

            let sort_spec = SortSpec {
                columns: vec![SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_sort_spec(sort_spec),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].sorting_columns.is_some());
        }

        #[tokio::test]
        async fn test_sink_without_sort_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[3, 1, 2], &["c", "a", "b"]);

            let sort_spec = SortSpec { columns: vec![] };

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_sort_spec(sort_spec),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].sorting_columns.is_none());
        }

        #[tokio::test]
        async fn test_sink_without_bloom_filters() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterConfig::None;

            let options = ParquetSinkOptions::new().with_bloom_filters(bloom_filters);

            let mut sink = ParquetSink::create(output_path.clone(), &schema, &options).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(!file.has_any_bloom_filters);
            assert!(!file.row_groups[0].columns[0].has_bloom_filter);
            assert!(!file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test]
        async fn test_sink_with_bloom_filters() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterConfig::All(AllColumnsBloomFilterConfig {
                fpp: 0.01,
                ndv: None,
            });

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 3);
            ndv_map.insert("name".to_string(), 3);

            let options = ParquetSinkOptions::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink = ParquetSink::create(output_path.clone(), &schema, &options).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test]
        async fn test_sink_with_bloom_filters_specific_columns() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
                name: "id".to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: 0.05,
                    ndv: None,
                },
            }]);

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 3);

            let options = ParquetSinkOptions::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink = ParquetSink::create(output_path.clone(), &schema, &options).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(!file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test]
        async fn test_sink_without_dictionary_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(
                &schema,
                &[1, 2, 3, 4, 5],
                &["a", "b", "c", "d", "e"],
            );

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_no_dictionary(true),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(!file.has_any_dictionary);
        }

        #[tokio::test]
        async fn test_sink_with_dictionary_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(
                &schema,
                &[1, 2, 3, 4, 5],
                &["a", "b", "c", "d", "e"],
            );

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_no_dictionary(false),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_dictionary);
        }

        #[tokio::test]
        async fn test_sink_with_statistics_options() {
            for statistics in [
                ParquetStatistics::None,
                ParquetStatistics::Chunk,
                ParquetStatistics::Page,
            ] {
                let temp_dir = tempdir().unwrap();
                let output_path = temp_dir.path().join("output.parquet");

                let schema = test_data::simple_schema();
                let batch = test_data::create_batch_with_ids_and_names(
                    &schema,
                    &[1, 2, 3],
                    &["a", "b", "c"],
                );

                let mut sink = ParquetSink::create(
                    output_path.clone(),
                    &schema,
                    &ParquetSinkOptions::new().with_statistics(statistics),
                )
                .unwrap();

                sink.write_batch(batch).await.unwrap();
                sink.finish().await.unwrap();

                let file = read_entire_parquet_file(&output_path).unwrap();

                match statistics {
                    ParquetStatistics::None => {
                        assert!(
                            file.row_groups[0].columns[0].statistics.is_none(),
                            "statistics should be none for None"
                        );
                    }
                    ParquetStatistics::Chunk => {
                        assert!(
                            file.row_groups[0].columns[0].statistics.is_some(),
                            "statistics should be some for Chunk"
                        );
                    }
                    ParquetStatistics::Page => {
                        assert!(
                            file.row_groups[0].columns[0].statistics.is_some(),
                            "statistics should be some for Page"
                        );
                    }
                }
            }
        }

        #[tokio::test]
        async fn test_sink_with_writer_versions() {
            for version in [ParquetWriterVersion::V1, ParquetWriterVersion::V2] {
                let temp_dir = tempdir().unwrap();
                let output_path = temp_dir.path().join("output.parquet");

                let schema = test_data::simple_schema();
                let batch = test_data::create_batch_with_ids_and_names(
                    &schema,
                    &[1, 2, 3],
                    &["a", "b", "c"],
                );

                let mut sink = ParquetSink::create(
                    output_path.clone(),
                    &schema,
                    &ParquetSinkOptions::new().with_writer_version(version),
                )
                .unwrap();

                sink.write_batch(batch).await.unwrap();
                sink.finish().await.unwrap();

                let file = read_entire_parquet_file(&output_path).unwrap();
                assert_eq!(file.metadata.version(), Into::<i32>::into(version));
            }
        }

        #[tokio::test]
        async fn test_sink_sort_metadata_invalid_column() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();

            let sort_spec = SortSpec {
                columns: vec![SortColumn {
                    name: "nonexistent_column".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let result = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_sort_spec(sort_spec),
            );

            assert!(result.is_err());
            let err = result.err().unwrap();
            assert!(err.to_string().contains("nonexistent_column"));
        }

        #[tokio::test]
        async fn test_sink_bloom_filter_without_ndv() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
                name: "id".to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: 0.05,
                    ndv: None,
                },
            }]);

            let options = ParquetSinkOptions::new().with_bloom_filters(bloom_filters);

            let mut sink = ParquetSink::create(output_path.clone(), &schema, &options).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test]
        async fn test_sink_bloom_filter_all_columns_with_user_ndv() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterConfig::All(AllColumnsBloomFilterConfig {
                fpp: 0.01,
                ndv: Some(5000),
            });

            let options = ParquetSinkOptions::new().with_bloom_filters(bloom_filters);

            let mut sink = ParquetSink::create(output_path.clone(), &schema, &options).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test]
        async fn test_sink_bloom_filter_column_with_user_ndv() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
                name: "id".to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: 0.01,
                    ndv: Some(10000),
                },
            }]);

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 500);

            let options = ParquetSinkOptions::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink = ParquetSink::create(output_path.clone(), &schema, &options).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test]
        async fn test_sink_multi_column_sort_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[3, 1, 2], &["c", "a", "b"]);

            let sort_spec = SortSpec {
                columns: vec![
                    SortColumn {
                        name: "name".to_string(),
                        direction: SortDirection::Ascending,
                    },
                    SortColumn {
                        name: "id".to_string(),
                        direction: SortDirection::Descending,
                    },
                ],
            };

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_sort_spec(sort_spec),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(
                file.row_groups[0].sorting_columns.as_ref().unwrap().len(),
                2
            );
        }
    }

    mod options_builder_tests {
        use super::*;

        #[test]
        fn test_default_options() {
            let options = ParquetSinkOptions::default();
            assert_eq!(options.record_batch_size, 122_880);
            assert_eq!(options.max_row_group_size, 1_048_576);
            assert!(matches!(options.compression, ParquetCompression::None));
            assert!(matches!(options.bloom_filters, BloomFilterConfig::None));
            assert!(matches!(options.statistics, ParquetStatistics::Page));
            assert!(!options.no_dictionary);
            assert!(matches!(options.writer_version, ParquetWriterVersion::V2));
            assert!(options.metadata.is_empty());
        }

        #[test]
        fn test_builder_pattern() {
            let mut metadata = HashMap::new();
            metadata.insert("test".to_string(), Some("value".to_string()));

            let sort_spec = SortSpec {
                columns: vec![SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let bloom_filters = BloomFilterConfig::All(AllColumnsBloomFilterConfig {
                fpp: 0.01,
                ndv: None,
            });

            let options = ParquetSinkOptions::new()
                .with_record_batch_size(1000)
                .with_max_row_group_size(5000)
                .with_sort_spec(sort_spec.clone())
                .with_compression(ParquetCompression::Zstd)
                .with_bloom_filters(bloom_filters)
                .with_statistics(ParquetStatistics::None)
                .with_no_dictionary(true)
                .with_writer_version(ParquetWriterVersion::V1)
                .with_metadata(metadata.clone());

            assert_eq!(options.record_batch_size, 1000);
            assert_eq!(options.max_row_group_size, 5000);
            assert_eq!(options.sort_spec.columns.len(), 1);
            assert!(matches!(options.compression, ParquetCompression::Zstd));
            assert!(matches!(options.bloom_filters, BloomFilterConfig::All(_)));
            assert!(matches!(options.statistics, ParquetStatistics::None));
            assert!(options.no_dictionary);
            assert!(matches!(options.writer_version, ParquetWriterVersion::V1));
            assert_eq!(options.metadata, metadata);
        }

        #[test]
        fn test_metadata_value() {
            let options = ParquetSinkOptions::new()
                .with_metadata_value("key1".to_string(), Some("val1".to_string()))
                .with_metadata_value("key2".to_string(), Some("val2".to_string()));

            assert_eq!(options.metadata.len(), 2);
            assert_eq!(
                options.metadata.get("key1").unwrap(),
                &Some("val1".to_string())
            );
            assert_eq!(
                options.metadata.get("key2").unwrap(),
                &Some("val2".to_string())
            );
        }

        #[test]
        fn test_metadata() {
            let mut metadata = HashMap::new();
            metadata.insert("key1".to_string(), Some("val1".to_string()));
            metadata.insert("key2".to_string(), Some("val2".to_string()));

            let options = ParquetSinkOptions::new().with_metadata(metadata.clone());

            assert_eq!(options.metadata, metadata);
        }

        #[test]
        fn test_encoding_options() {
            use crate::{ColumnEncodingConfig, ParquetEncoding};

            let options = ParquetSinkOptions::new()
                .with_encoding(Some(ParquetEncoding::DeltaBinaryPacked))
                .with_column_encodings(vec![ColumnEncodingConfig {
                    name: "name".to_string(),
                    encoding: ParquetEncoding::DeltaByteArray,
                }]);

            assert_eq!(options.encoding, Some(ParquetEncoding::DeltaBinaryPacked));
            assert_eq!(options.column_encodings.len(), 1);
            assert_eq!(options.column_encodings[0].name, "name");
            assert_eq!(
                options.column_encodings[0].encoding,
                ParquetEncoding::DeltaByteArray
            );
        }
    }

    mod encoding_tests {
        use super::*;
        use crate::{ColumnEncodingConfig, ParquetEncoding};

        #[tokio::test]
        async fn test_sink_with_default_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_encoding(Some(ParquetEncoding::Plain)),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);
            assert!(output_path.exists());

            let batches = verify::read_parquet_file(&output_path).unwrap();
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }

        #[tokio::test]
        async fn test_sink_with_column_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_column_encodings(vec![ColumnEncodingConfig {
                    name: "id".to_string(),
                    encoding: ParquetEncoding::DeltaBinaryPacked,
                }]),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);
            assert!(output_path.exists());

            let batches = verify::read_parquet_file(&output_path).unwrap();
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }

        #[tokio::test]
        async fn test_sink_with_multiple_column_encodings() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_column_encodings(vec![
                    ColumnEncodingConfig {
                        name: "id".to_string(),
                        encoding: ParquetEncoding::DeltaBinaryPacked,
                    },
                    ColumnEncodingConfig {
                        name: "name".to_string(),
                        encoding: ParquetEncoding::DeltaByteArray,
                    },
                ]),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);
            assert!(output_path.exists());

            let batches = verify::read_parquet_file(&output_path).unwrap();
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }
    }
}
