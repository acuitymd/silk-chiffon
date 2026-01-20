mod parallel_writer;
pub mod pools;
mod sequential_writer;
mod writer_trait;

pub use parallel_writer::{ParallelParquetWriter, ParallelWriterConfig};
pub use pools::ParquetPools;
pub use sequential_writer::{SequentialParquetWriter, SequentialWriterConfig};
pub use writer_trait::ParquetWriter;

/// Default buffer size for parquet file I/O.
pub const DEFAULT_BUFFER_SIZE: usize = 32 * 1024 * 1024; // 32MiB
pub const DEFAULT_MAX_ROW_GROUP_SIZE: usize = 1_048_576;
pub const DEFAULT_WRITE_BATCH_SIZE: usize = 8192;
pub const DEFAULT_DATA_PAGE_SIZE_LIMIT: usize = 100 * 1024 * 1024; // 100MiB (DuckDB MAX_UNCOMPRESSED_PAGE_SIZE)
pub const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT: usize = 1024 * 1024 * 1024; // 1GiB (DuckDB MAX_UNCOMPRESSED_DICT_PAGE_SIZE)

use parquet::{
    file::{
        metadata::SortingColumn,
        properties::{WriterProperties, WriterPropertiesBuilder},
    },
    schema::types::ColumnPath,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

use anyhow::{Result, anyhow};
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, SchemaRef},
};
use async_trait::async_trait;

use crate::{
    BloomFilterConfig, ColumnEncodingConfig, ParquetCompression, ParquetEncoder, ParquetEncoding,
    ParquetStatistics, ParquetWriterVersion, SortDirection, SortSpec,
    sinks::data_sink::{DataSink, SinkResult},
};

pub struct ParquetSinkOptions {
    pub encoding_batch_size: usize,
    pub max_row_group_size: usize,
    pub max_row_group_concurrency: usize,
    pub buffer_size: usize,
    pub batch_channel_size: usize,
    pub encoded_channel_size: usize,
    pub sort_spec: SortSpec,
    pub compression: ParquetCompression,
    pub bloom_filters: BloomFilterConfig,
    pub statistics: ParquetStatistics,
    pub no_dictionary: bool,
    pub column_dictionary: Vec<String>,
    pub column_no_dictionary: Vec<String>,
    pub writer_version: ParquetWriterVersion,
    pub encoding: Option<ParquetEncoding>,
    pub column_encodings: Vec<ColumnEncodingConfig>,
    pub ndv_map: HashMap<String, u64>,
    pub metadata: HashMap<String, Option<String>>,
    pub encoder: ParquetEncoder,
    pub data_page_size_limit: usize,
    pub data_page_row_count_limit: usize,
    pub dictionary_page_size_limit: usize,
    pub write_batch_size: usize,
    pub offset_index_enabled: bool,
    pub skip_arrow_metadata: bool,
}

impl Default for ParquetSinkOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetSinkOptions {
    pub fn new() -> Self {
        Self {
            encoding_batch_size: 122_880,
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            max_row_group_concurrency: 4,
            buffer_size: DEFAULT_BUFFER_SIZE,
            batch_channel_size: 16,
            encoded_channel_size: 4,
            sort_spec: SortSpec::default(),
            compression: ParquetCompression::default(),
            bloom_filters: BloomFilterConfig::default(),
            statistics: ParquetStatistics::default(),
            no_dictionary: false,
            column_dictionary: Vec::new(),
            column_no_dictionary: Vec::new(),
            writer_version: ParquetWriterVersion::default(),
            encoding: None,
            column_encodings: Vec::new(),
            ndv_map: HashMap::new(),
            metadata: HashMap::new(),
            encoder: ParquetEncoder::default(),
            data_page_size_limit: DEFAULT_DATA_PAGE_SIZE_LIMIT,
            data_page_row_count_limit: usize::MAX,
            dictionary_page_size_limit: DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            offset_index_enabled: false,
            skip_arrow_metadata: true,
        }
    }

    pub fn with_encoding_batch_size(mut self, encoding_batch_size: usize) -> Self {
        self.encoding_batch_size = encoding_batch_size;
        self
    }

    pub fn with_max_row_group_size(mut self, max_row_group_size: usize) -> Self {
        self.max_row_group_size = max_row_group_size;
        self
    }

    pub fn with_max_row_group_concurrency(mut self, max_row_group_concurrency: usize) -> Self {
        self.max_row_group_concurrency = max_row_group_concurrency;
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn with_batch_channel_size(mut self, batch_channel_size: usize) -> Self {
        self.batch_channel_size = batch_channel_size;
        self
    }

    pub fn with_encoded_channel_size(mut self, encoded_channel_size: usize) -> Self {
        self.encoded_channel_size = encoded_channel_size;
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

    pub fn with_column_dictionary(mut self, column_dictionary: Vec<String>) -> Self {
        self.column_dictionary = column_dictionary;
        self
    }

    pub fn with_column_no_dictionary(mut self, column_no_dictionary: Vec<String>) -> Self {
        self.column_no_dictionary = column_no_dictionary;
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

    pub fn with_encoder(mut self, encoder: ParquetEncoder) -> Self {
        self.encoder = encoder;
        self
    }

    pub fn with_data_page_size_limit(mut self, limit: usize) -> Self {
        self.data_page_size_limit = limit;
        self
    }

    pub fn with_data_page_row_count_limit(mut self, limit: usize) -> Self {
        self.data_page_row_count_limit = limit;
        self
    }

    pub fn with_dictionary_page_size_limit(mut self, limit: usize) -> Self {
        self.dictionary_page_size_limit = limit;
        self
    }

    pub fn with_write_batch_size(mut self, size: usize) -> Self {
        self.write_batch_size = size;
        self
    }

    pub fn with_offset_index_enabled(mut self, enabled: bool) -> Self {
        self.offset_index_enabled = enabled;
        self
    }

    pub fn with_skip_arrow_metadata(mut self, skip: bool) -> Self {
        self.skip_arrow_metadata = skip;
        self
    }
}

struct ParquetSinkInner {
    path: PathBuf,
    writer: Option<Box<dyn ParquetWriter>>,
}

pub struct ParquetSink {
    inner: Mutex<ParquetSinkInner>,
}

impl ParquetSink {
    pub fn create(
        path: PathBuf,
        schema: &SchemaRef,
        options: &ParquetSinkOptions,
        pools: Arc<ParquetPools>,
    ) -> Result<Self> {
        let mut writer_builder = WriterProperties::builder()
            .set_created_by(format!("silk-chiffon v{}", env!("CARGO_PKG_VERSION")))
            .set_max_row_group_size(options.max_row_group_size)
            .set_compression(options.compression.into())
            .set_writer_version(options.writer_version.into())
            .set_statistics_enabled(options.statistics.into())
            .set_dictionary_enabled(!options.no_dictionary)
            .set_data_page_size_limit(options.data_page_size_limit)
            .set_data_page_row_count_limit(options.data_page_row_count_limit)
            .set_dictionary_page_size_limit(options.dictionary_page_size_limit)
            .set_write_batch_size(options.write_batch_size)
            .set_offset_index_disabled(!options.offset_index_enabled);

        writer_builder = Self::apply_column_dictionary(
            writer_builder,
            &options.column_dictionary,
            &options.column_no_dictionary,
        );

        Self::validate_column_encodings(schema, &options.column_encodings)?;

        writer_builder = Self::apply_encodings(
            writer_builder,
            schema,
            options.writer_version,
            &options.encoding,
            &options.column_encodings,
        );

        writer_builder = Self::apply_bloom_filters(
            writer_builder,
            schema,
            &options.bloom_filters,
            &options.ndv_map,
        );

        writer_builder = Self::apply_sort_metadata(&options.sort_spec, writer_builder, schema)?;

        let props = writer_builder.build();

        let writer: Box<dyn ParquetWriter> = match options.encoder {
            ParquetEncoder::Sequential => {
                let config = SequentialWriterConfig {
                    max_row_group_size: options.max_row_group_size,
                    buffer_size: options.buffer_size,
                    skip_arrow_metadata: options.skip_arrow_metadata,
                };
                Box::new(SequentialParquetWriter::new(&path, schema, props, config)?)
            }
            ParquetEncoder::Parallel => {
                let config = ParallelWriterConfig {
                    max_row_group_size: options.max_row_group_size,
                    max_row_group_concurrency: options.max_row_group_concurrency,
                    buffer_size: options.buffer_size,
                    encoding_batch_size: options.encoding_batch_size,
                    batch_channel_size: options.batch_channel_size,
                    encoded_channel_size: options.encoded_channel_size,
                    skip_arrow_metadata: options.skip_arrow_metadata,
                };
                Box::new(ParallelParquetWriter::new(
                    &path, schema, props, pools, config,
                ))
            }
        };

        Ok(Self {
            inner: Mutex::new(ParquetSinkInner {
                path,
                writer: Some(writer),
            }),
        })
    }

    fn apply_column_dictionary(
        mut builder: WriterPropertiesBuilder,
        column_dictionary: &[String],
        column_no_dictionary: &[String],
    ) -> WriterPropertiesBuilder {
        for col_name in column_dictionary {
            let col_path = ColumnPath::from(col_name.as_str());
            builder = builder.set_column_dictionary_enabled(col_path, true);
        }

        for col_name in column_no_dictionary {
            let col_path = ColumnPath::from(col_name.as_str());
            builder = builder.set_column_dictionary_enabled(col_path, false);
        }

        builder
    }

    fn validate_column_encodings(
        schema: &SchemaRef,
        column_encodings: &[ColumnEncodingConfig],
    ) -> Result<()> {
        for col_encoding in column_encodings {
            let data_type =
                Self::resolve_column_type(schema, &col_encoding.name).ok_or_else(|| {
                    anyhow!(
                        "column '{}' specified in --parquet-column-encoding not found in schema",
                        col_encoding.name
                    )
                })?;

            if let Some(error) = col_encoding.encoding.validate_for_type(&data_type) {
                return Err(anyhow!(
                    "invalid encoding for column '{}': {}",
                    col_encoding.name,
                    error
                ));
            }
        }
        Ok(())
    }

    /// Resolve a column path (potentially nested like "outer.inner") to its data type.
    fn resolve_column_type(schema: &SchemaRef, column_path: &str) -> Option<DataType> {
        let parts: Vec<&str> = column_path.split('.').collect();
        let mut current_type: Option<&DataType> = None;

        for (i, part) in parts.iter().enumerate() {
            if i == 0 {
                // first part: look up in schema
                let field = schema.field_with_name(part).ok()?;
                current_type = Some(field.data_type());
            } else {
                // subsequent parts: navigate into struct
                match current_type? {
                    DataType::Struct(fields) => {
                        let field = fields.iter().find(|f| f.name() == part)?;
                        current_type = Some(field.data_type());
                    }
                    _ => return None, // not a struct, can't navigate further
                }
            }
        }

        current_type.cloned()
    }

    fn apply_encodings(
        mut builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
        writer_version: ParquetWriterVersion,
        default_encoding: &Option<ParquetEncoding>,
        column_encodings: &[ColumnEncodingConfig],
    ) -> WriterPropertiesBuilder {
        if let Some(encoding) = default_encoding {
            builder = builder.set_encoding((*encoding).into());
        }

        for field in schema.fields() {
            let col_path = ColumnPath::from(field.name().as_str());
            match field.data_type() {
                DataType::Boolean => {
                    builder = builder.set_column_encoding(col_path, ParquetEncoding::Plain.into());
                }
                DataType::Float32 | DataType::Float64
                    if writer_version == ParquetWriterVersion::V2 =>
                {
                    builder = builder
                        .set_column_encoding(col_path, ParquetEncoding::ByteStreamSplit.into());
                }
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                    if writer_version == ParquetWriterVersion::V2 =>
                {
                    builder = builder
                        .set_column_encoding(col_path, ParquetEncoding::DeltaBinaryPacked.into());
                }
                _ => {}
            }
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
    ) -> WriterPropertiesBuilder {
        let disabled_cols: std::collections::HashSet<&str> = bloom_filters
            .column_disabled()
            .iter()
            .map(|s| s.as_str())
            .collect();

        if let Some(all_config) = bloom_filters.all_enabled() {
            // enable per-column instead of globally - parquet-rs set_column_bloom_filter_enabled(false)
            // doesn't override a global enable, and set_bloom_filter_fpp implicitly enables globally
            for field in schema.fields() {
                let col_name = field.name().as_str();
                if disabled_cols.contains(col_name) {
                    continue;
                }
                let col_path = ColumnPath::from(col_name);
                builder = builder
                    .set_column_bloom_filter_enabled(col_path.clone(), true)
                    .set_column_bloom_filter_fpp(col_path.clone(), all_config.fpp);
                if let Some(ndv) = all_config.ndv.or_else(|| ndv_map.get(col_name).copied()) {
                    builder = builder.set_column_bloom_filter_ndv(col_path, ndv);
                }
            }
        }

        for bloom_col in bloom_filters.column_enabled() {
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

        builder
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
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut inner = self.inner.lock().await;

        let writer = inner
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("Writer already closed"))?;

        writer.write(batch).await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        let mut inner = self.inner.lock().await;

        let writer = inner
            .writer
            .take()
            .ok_or_else(|| anyhow!("Writer already closed"))?;

        let rows_written = writer.close().await?;

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

    fn test_pools() -> Arc<ParquetPools> {
        Arc::new(ParquetPools::new(2, 1).unwrap())
    }

    mod parquet_sink_tests {
        use super::*;

        #[tokio::test]
        async fn test_sink_writes_single_batch_with_single_row_group() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new(),
                test_pools(),
            )
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

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new(),
                test_pools(),
            )
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
                test_pools(),
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
                    test_pools(),
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

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new(),
                test_pools(),
            )
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

            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new(),
                test_pools(),
            )
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
                test_pools(),
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
                test_pools(),
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

            let bloom_filters = BloomFilterConfig::default();

            let options = ParquetSinkOptions::new().with_bloom_filters(bloom_filters);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();

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

            let bloom_filters = BloomFilterConfig::builder()
                .all_enabled(AllColumnsBloomFilterConfig {
                    fpp: 0.01,
                    ndv: None,
                })
                .build()
                .unwrap();

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 3);
            ndv_map.insert("name".to_string(), 3);

            let options = ParquetSinkOptions::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();

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

            let bloom_filters = BloomFilterConfig::builder()
                .enable_column(ColumnSpecificBloomFilterConfig {
                    name: "id".to_string(),
                    config: ColumnBloomFilterConfig {
                        fpp: 0.05,
                        ndv: None,
                    },
                })
                .build()
                .unwrap();

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 3);

            let options = ParquetSinkOptions::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(!file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test]
        async fn test_sink_with_bloom_all_enabled_but_specific_columns_disabled() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterConfig::builder()
                .all_enabled(AllColumnsBloomFilterConfig {
                    fpp: 0.01,
                    ndv: None,
                })
                .disable_column("id")
                .build()
                .unwrap();

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 3);
            ndv_map.insert("name".to_string(), 3);

            let options = ParquetSinkOptions::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(
                !file.row_groups[0].columns[0].has_bloom_filter,
                "id column should NOT have bloom filter"
            );
            assert!(
                file.row_groups[0].columns[1].has_bloom_filter,
                "name column should have bloom filter"
            );
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
                test_pools(),
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
                test_pools(),
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
                    test_pools(),
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
                    test_pools(),
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
                test_pools(),
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

            let bloom_filters = BloomFilterConfig::builder()
                .enable_column(ColumnSpecificBloomFilterConfig {
                    name: "id".to_string(),
                    config: ColumnBloomFilterConfig {
                        fpp: 0.05,
                        ndv: None,
                    },
                })
                .build()
                .unwrap();

            let options = ParquetSinkOptions::new().with_bloom_filters(bloom_filters);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();

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

            let bloom_filters = BloomFilterConfig::builder()
                .all_enabled(AllColumnsBloomFilterConfig {
                    fpp: 0.01,
                    ndv: Some(5000),
                })
                .build()
                .unwrap();

            let options = ParquetSinkOptions::new().with_bloom_filters(bloom_filters);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();

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

            let bloom_filters = BloomFilterConfig::builder()
                .enable_column(ColumnSpecificBloomFilterConfig {
                    name: "id".to_string(),
                    config: ColumnBloomFilterConfig {
                        fpp: 0.01,
                        ndv: Some(10000),
                    },
                })
                .build()
                .unwrap();

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 500);

            let options = ParquetSinkOptions::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();

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
                test_pools(),
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
            assert_eq!(options.encoding_batch_size, 122_880);
            assert_eq!(options.max_row_group_size, DEFAULT_MAX_ROW_GROUP_SIZE);
            assert!(matches!(options.compression, ParquetCompression::None));
            assert!(!options.bloom_filters.is_configured());
            assert!(matches!(options.statistics, ParquetStatistics::Chunk));
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

            let bloom_filters = BloomFilterConfig::builder()
                .all_enabled(AllColumnsBloomFilterConfig {
                    fpp: 0.01,
                    ndv: None,
                })
                .build()
                .unwrap();

            let options = ParquetSinkOptions::new()
                .with_encoding_batch_size(1000)
                .with_max_row_group_size(5000)
                .with_sort_spec(sort_spec.clone())
                .with_compression(ParquetCompression::Zstd)
                .with_bloom_filters(bloom_filters)
                .with_statistics(ParquetStatistics::None)
                .with_no_dictionary(true)
                .with_writer_version(ParquetWriterVersion::V1)
                .with_metadata(metadata.clone());

            assert_eq!(options.encoding_batch_size, 1000);
            assert_eq!(options.max_row_group_size, 5000);
            assert_eq!(options.sort_spec.columns.len(), 1);
            assert!(matches!(options.compression, ParquetCompression::Zstd));
            assert!(options.bloom_filters.is_configured());
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
        use std::sync::Arc;

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
                test_pools(),
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
                test_pools(),
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
                test_pools(),
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
        async fn test_sink_with_nested_column_encoding() {
            use arrow::array::{Int32Array, StructArray};
            use arrow::datatypes::Field;

            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            // create schema with nested struct: { outer: { inner: i32 } }
            let inner_field = Field::new("inner", arrow::datatypes::DataType::Int32, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field.clone()].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            // create batch with nested data
            let inner_array = Int32Array::from(vec![1, 2, 3]);
            let outer_array =
                StructArray::from(vec![(Arc::new(inner_field), Arc::new(inner_array) as _)]);
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(outer_array)]).unwrap();

            // use nested column path for encoding
            let mut sink = ParquetSink::create(
                output_path.clone(),
                &schema,
                &ParquetSinkOptions::new().with_column_encodings(vec![ColumnEncodingConfig {
                    name: "outer.inner".to_string(),
                    encoding: ParquetEncoding::DeltaBinaryPacked,
                }]),
                test_pools(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);
            assert!(output_path.exists());
        }

        #[test]
        fn test_resolve_column_type_top_level() {
            let schema = test_data::simple_schema();
            let data_type = ParquetSink::resolve_column_type(&schema, "id");
            assert_eq!(data_type, Some(arrow::datatypes::DataType::Int32));
        }

        #[test]
        fn test_resolve_column_type_nested() {
            use arrow::datatypes::Field;

            let inner_field = Field::new("inner", arrow::datatypes::DataType::Int64, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            let data_type = ParquetSink::resolve_column_type(&schema, "outer.inner");
            assert_eq!(data_type, Some(arrow::datatypes::DataType::Int64));
        }

        #[test]
        fn test_resolve_column_type_not_found() {
            let schema = test_data::simple_schema();
            let data_type = ParquetSink::resolve_column_type(&schema, "nonexistent");
            assert_eq!(data_type, None);
        }

        #[test]
        fn test_resolve_column_type_nested_not_found() {
            use arrow::datatypes::Field;

            let inner_field = Field::new("inner", arrow::datatypes::DataType::Int64, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            let data_type = ParquetSink::resolve_column_type(&schema, "outer.nonexistent");
            assert_eq!(data_type, None);
        }
    }

    mod default_encoding_tests {
        use std::path::Path;

        use super::*;
        use arrow::array::{BooleanArray, Float32Array, Float64Array, Int64Array};
        use arrow::datatypes::Field;
        use parquet::basic::Encoding;

        fn numeric_schema() -> SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                Field::new("int_col", DataType::Int64, false),
                Field::new("float_col", DataType::Float32, false),
                Field::new("double_col", DataType::Float64, false),
                Field::new("bool_col", DataType::Boolean, false),
                Field::new("string_col", DataType::Utf8, false),
            ]))
        }

        fn create_numeric_batch(schema: &SchemaRef) -> RecordBatch {
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                    Arc::new(BooleanArray::from(vec![true, false, true])),
                    Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap()
        }

        fn get_column_encodings(path: &Path) -> HashMap<String, Vec<Encoding>> {
            let file = read_entire_parquet_file(path).unwrap();
            file.row_groups[0]
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.encodings.clone()))
                .collect()
        }

        #[tokio::test]
        async fn test_v2_applies_byte_stream_split_to_floats() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = ParquetSinkOptions::new()
                .with_writer_version(ParquetWriterVersion::V2)
                .with_no_dictionary(true); // disable dict to see the fallback encoding

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["float_col"].contains(&Encoding::BYTE_STREAM_SPLIT),
                "float_col should use BYTE_STREAM_SPLIT, got {:?}",
                encodings["float_col"]
            );
            assert!(
                encodings["double_col"].contains(&Encoding::BYTE_STREAM_SPLIT),
                "double_col should use BYTE_STREAM_SPLIT, got {:?}",
                encodings["double_col"]
            );
        }

        #[tokio::test]
        async fn test_v2_applies_delta_binary_packed_to_integers() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = ParquetSinkOptions::new()
                .with_writer_version(ParquetWriterVersion::V2)
                .with_no_dictionary(true);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["int_col"].contains(&Encoding::DELTA_BINARY_PACKED),
                "int_col should use DELTA_BINARY_PACKED, got {:?}",
                encodings["int_col"]
            );
        }

        #[tokio::test]
        async fn test_booleans_always_use_plain() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = ParquetSinkOptions::new().with_writer_version(ParquetWriterVersion::V2);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            // booleans should use PLAIN for data encoding
            // (RLE may still appear for definition/repetition levels)
            assert!(
                encodings["bool_col"].contains(&Encoding::PLAIN),
                "bool_col should use PLAIN, got {:?}",
                encodings["bool_col"]
            );
        }

        #[tokio::test]
        async fn test_v1_uses_plain_for_floats_and_integers() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = ParquetSinkOptions::new()
                .with_writer_version(ParquetWriterVersion::V1)
                .with_no_dictionary(true);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);

            assert!(
                !encodings["float_col"].contains(&Encoding::BYTE_STREAM_SPLIT),
                "V1 float_col should NOT use BYTE_STREAM_SPLIT"
            );
            assert!(
                encodings["float_col"].contains(&Encoding::PLAIN),
                "V1 float_col should use PLAIN"
            );
            assert!(
                !encodings["int_col"].contains(&Encoding::DELTA_BINARY_PACKED),
                "V1 int_col should NOT use DELTA_BINARY_PACKED"
            );
            assert!(
                encodings["int_col"].contains(&Encoding::PLAIN),
                "V1 int_col should use PLAIN"
            );
        }

        #[tokio::test]
        async fn test_user_encoding_overrides_default() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = ParquetSinkOptions::new()
                .with_writer_version(ParquetWriterVersion::V2)
                .with_no_dictionary(true)
                .with_column_encodings(vec![ColumnEncodingConfig {
                    name: "float_col".to_string(),
                    encoding: ParquetEncoding::Plain,
                }]);

            let mut sink =
                ParquetSink::create(output_path.clone(), &schema, &options, test_pools()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["float_col"].contains(&Encoding::PLAIN),
                "float_col should use PLAIN (user override)"
            );
            assert!(
                !encodings["float_col"].contains(&Encoding::BYTE_STREAM_SPLIT),
                "float_col should NOT use BYTE_STREAM_SPLIT when overridden"
            );
            assert!(
                encodings["double_col"].contains(&Encoding::BYTE_STREAM_SPLIT),
                "double_col should use BYTE_STREAM_SPLIT"
            );
        }
    }
}
