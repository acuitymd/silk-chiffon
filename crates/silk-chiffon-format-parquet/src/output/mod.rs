//! Command-scoped Parquet output binding and per-object sinks.
//!
//! One binding shares independent encoding and writing runtimes across its
//! sinks. Each sink owns its bounded pipeline and storage upload together so
//! finish, abort, and drop cannot detach codec work from upload cleanup.

mod analysis_provider;
mod pipeline;
mod runtime;

use pipeline::PipelineConfig;
use runtime::OutputRuntimes;

/// Default bytes buffered between encoding and the object upload.
const DEFAULT_BUFFER_SIZE: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_ROW_GROUP_SIZE: usize = 1_048_576;
const DEFAULT_WRITE_BATCH_SIZE: usize = 8192;
/// DuckDB's maximum uncompressed page size.
const DEFAULT_DATA_PAGE_SIZE_LIMIT: usize = 100 * 1024 * 1024;
/// DuckDB's maximum uncompressed dictionary page size.
const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT: usize = 1024 * 1024 * 1024;
const PARQUET_CREATED_BY: &str = "silk-chiffon";

use parquet::{
    basic::Compression as CompressionCodec,
    file::{
        metadata::{KeyValue, SortingColumn},
        properties::{WriterProperties, WriterPropertiesBuilder},
    },
    schema::types::ColumnPath,
};
use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use apply_if::ApplyIf;
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, SchemaRef},
};
use async_trait::async_trait;
use silk_chiffon_storage::{ObjectUploadTask, PreparedOutputTarget};
use tokio::sync::mpsc;

use crate::{
    BloomFilterPolicy, ColumnEncoding, Compression, DictionaryMode, DictionaryPolicy, Encoding,
    Statistics, WriterVersion,
};
use silk_chiffon_core::{
    DataSink, NullPlacement, OpenSinkMode, SinkBinding, SinkBindingConfig, SinkCompletion,
    SortColumn, SortDirection, validate_batch_schema,
};

/// Options for configuring Parquet file output.
///
/// Column paths use dot notation with prefix matching. For example, "user" matches
/// all columns under the user struct (user.name, user.address.city, etc.).
#[derive(Clone)]
pub struct OutputConfig {
    /// Rows per row group.
    pub max_row_group_size: usize,
    pub max_row_group_concurrency: Option<usize>,
    /// I/O buffer size in bytes.
    pub buffer_size: usize,
    pub ingestion_queue_size: Option<usize>,
    pub encoding_queue_size: Option<usize>,
    pub writing_queue_size: Option<usize>,
    pub output_ordering: Vec<SortColumn>,
    pub compression: CompressionCodec,
    pub bloom_filters: BloomFilterPolicy,
    pub statistics: Statistics,
    /// Disable dictionary encoding globally.
    pub no_dictionary: bool,
    /// Columns to always attempt dictionary encoding (dot-separated paths).
    pub column_dictionary_always: Vec<String>,
    /// Columns to analyze for dictionary encoding (dot-separated paths).
    /// Analysis runs per-row-group; dictionary disabled if cardinality > 20%.
    pub column_dictionary_analyze: Vec<String>,
    /// Columns to disable dictionary encoding for (dot-separated paths).
    pub column_no_dictionary: Vec<String>,
    pub writer_version: WriterVersion,
    pub encoding: Option<Encoding>,
    pub column_encodings: Vec<ColumnEncoding>,
    /// Pre-computed NDV per column for bloom filter sizing.
    /// Keys are column names, or dot-separated paths for nested columns.
    pub ndv_map: HashMap<String, u64>,
    pub metadata: HashMap<String, Option<String>>,
    /// Max data page size in bytes.
    pub data_page_size_limit: usize,
    /// Max rows per data page.
    pub data_page_row_count_limit: usize,
    /// Max dictionary page size in bytes.
    pub dictionary_page_size_limit: usize,
    /// Rows per write batch (internal encoding granularity).
    pub write_batch_size: usize,
    pub offset_index_enabled: bool,
    pub skip_arrow_metadata: bool,
    pub page_header_statistics: bool,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputConfig {
    pub fn new() -> Self {
        Self {
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            max_row_group_concurrency: None,
            buffer_size: DEFAULT_BUFFER_SIZE,
            ingestion_queue_size: None,
            encoding_queue_size: None,
            writing_queue_size: None,
            output_ordering: Vec::new(),
            compression: CompressionCodec::UNCOMPRESSED,
            bloom_filters: BloomFilterPolicy::default(),
            statistics: Statistics::default(),
            no_dictionary: false,
            column_dictionary_always: Vec::new(),
            column_dictionary_analyze: Vec::new(),
            column_no_dictionary: Vec::new(),
            writer_version: WriterVersion::default(),
            encoding: None,
            column_encodings: Vec::new(),
            ndv_map: HashMap::new(),
            metadata: HashMap::new(),
            data_page_size_limit: DEFAULT_DATA_PAGE_SIZE_LIMIT,
            data_page_row_count_limit: usize::MAX,
            dictionary_page_size_limit: DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            offset_index_enabled: false,
            skip_arrow_metadata: true,
            page_header_statistics: false,
        }
    }

    pub fn with_max_row_group_size(mut self, max_row_group_size: usize) -> Self {
        self.max_row_group_size = max_row_group_size;
        self
    }

    pub fn with_max_row_group_concurrency(mut self, max_row_group_concurrency: usize) -> Self {
        self.max_row_group_concurrency = Some(max_row_group_concurrency);
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn with_ingestion_queue_size(mut self, ingestion_queue_size: usize) -> Self {
        self.ingestion_queue_size = Some(ingestion_queue_size);
        self
    }

    pub fn with_encoding_queue_size(mut self, encoding_queue_size: usize) -> Self {
        self.encoding_queue_size = Some(encoding_queue_size);
        self
    }

    pub fn with_writing_queue_size(mut self, writing_queue_size: usize) -> Self {
        self.writing_queue_size = Some(writing_queue_size);
        self
    }

    pub fn with_output_ordering(mut self, output_ordering: Vec<SortColumn>) -> Self {
        self.output_ordering = output_ordering;
        self
    }

    pub fn with_compression(mut self, compression: CompressionCodec) -> Self {
        self.compression = compression;
        self
    }

    /// Resolve a `Compression` enum + optional level into the low-level
    /// `Compression` type and set it on this options struct.
    fn with_compression_and_level(
        self,
        compression: Compression,
        level: Option<i32>,
    ) -> anyhow::Result<Self> {
        Ok(self.with_compression(compression.to_compression(level)?))
    }

    pub fn with_bloom_filters(mut self, bloom_filters: BloomFilterPolicy) -> Self {
        self.bloom_filters = bloom_filters;
        self
    }

    pub fn with_statistics(mut self, statistics: Statistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn with_no_dictionary(mut self, no_dictionary: bool) -> Self {
        self.no_dictionary = no_dictionary;
        self
    }

    #[cfg(test)]
    fn with_column_dictionary_always(mut self, columns: Vec<String>) -> Self {
        self.column_dictionary_always = columns;
        self
    }

    #[cfg(test)]
    fn with_column_dictionary_analyze(mut self, columns: Vec<String>) -> Self {
        self.column_dictionary_analyze = columns;
        self
    }

    pub fn with_column_no_dictionary(mut self, column_no_dictionary: Vec<String>) -> Self {
        self.column_no_dictionary = column_no_dictionary;
        self
    }

    /// Apply dictionary configuration from CLI-style `DictionaryPolicy` entries,
    /// splitting them into always/analyze lists internally.
    pub fn with_dictionary_configs(mut self, configs: &[DictionaryPolicy]) -> Self {
        let mut always = Vec::new();
        let mut analyze = Vec::new();
        for config in configs {
            match config.mode {
                DictionaryMode::Always => always.push(config.name.clone()),
                DictionaryMode::Analyze => analyze.push(config.name.clone()),
            }
        }
        self.column_dictionary_always = always;
        self.column_dictionary_analyze = analyze;
        self
    }

    pub fn with_writer_version(mut self, writer_version: WriterVersion) -> Self {
        self.writer_version = writer_version;
        self
    }

    pub fn with_encoding(mut self, encoding: Option<Encoding>) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn with_column_encodings(mut self, column_encodings: Vec<ColumnEncoding>) -> Self {
        self.column_encodings = column_encodings;
        self
    }

    #[cfg(test)]
    fn with_metadata_value(mut self, key: String, value: Option<String>) -> Self {
        self.metadata.insert(key, value);
        self
    }

    #[cfg(test)]
    fn with_metadata(mut self, metadata: HashMap<String, Option<String>>) -> Self {
        self.metadata = metadata;
        self
    }

    #[cfg(test)]
    fn with_ndv_map(mut self, ndv_map: HashMap<String, u64>) -> Self {
        self.ndv_map = ndv_map;
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

    pub fn with_page_header_statistics(mut self, enabled: bool) -> Self {
        self.page_header_statistics = enabled;
        self
    }
}

pub struct Sink {
    schema: SchemaRef,
    ingestion_sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<ObjectUploadTask<u64>>,
}

fn column_path_from_dot_notation(name: &str) -> ColumnPath {
    if name.contains('.') {
        ColumnPath::from(name.split('.').map(String::from).collect::<Vec<_>>())
    } else {
        ColumnPath::from(name)
    }
}

const DEFAULT_INGESTION: usize = 1;
const DEFAULT_ENCODING: usize = 4;
const DEFAULT_WRITING: usize = 4;
const DEFAULT_CONCURRENCY: usize = 4;

fn resolve_queue_sizes(options: &OutputConfig) -> (usize, usize, usize, usize) {
    (
        options.ingestion_queue_size.unwrap_or(DEFAULT_INGESTION),
        options.encoding_queue_size.unwrap_or(DEFAULT_ENCODING),
        options.writing_queue_size.unwrap_or(DEFAULT_WRITING),
        options
            .max_row_group_concurrency
            .unwrap_or(DEFAULT_CONCURRENCY),
    )
}

impl Sink {
    fn create(
        target: PreparedOutputTarget,
        schema: &SchemaRef,
        options: &OutputConfig,
        runtimes: Arc<OutputRuntimes>,
    ) -> Result<Self> {
        let mut writer_builder = WriterProperties::builder()
            .set_created_by(PARQUET_CREATED_BY.to_string())
            .set_max_row_group_row_count(Some(options.max_row_group_size))
            .set_compression(options.compression)
            .set_writer_version(options.writer_version.into())
            .set_statistics_enabled(options.statistics.into())
            .set_dictionary_enabled(!options.no_dictionary)
            .set_data_page_size_limit(options.data_page_size_limit)
            .set_data_page_row_count_limit(options.data_page_row_count_limit)
            .set_dictionary_page_size_limit(options.dictionary_page_size_limit)
            .set_write_batch_size(options.write_batch_size)
            .set_offset_index_disabled(!options.offset_index_enabled)
            .set_write_page_header_statistics(options.page_header_statistics)
            .set_coerce_types(true);

        let all_dictionary_columns: Vec<_> = options
            .column_dictionary_always
            .iter()
            .chain(options.column_dictionary_analyze.iter())
            .cloned()
            .collect();
        Self::validate_column_dictionary(
            schema,
            &all_dictionary_columns,
            &options.column_no_dictionary,
        )?;

        Self::validate_column_encodings(schema, &options.column_encodings)?;

        writer_builder = Self::apply_encodings(
            writer_builder,
            schema,
            options.writer_version,
            &options.encoding,
            &options.column_encodings,
        );

        writer_builder =
            Self::apply_sort_metadata(&options.output_ordering, writer_builder, schema)?;

        if !options.metadata.is_empty() {
            let key_values: Vec<KeyValue> = options
                .metadata
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect();
            writer_builder = writer_builder.set_key_value_metadata(Some(key_values));
        }

        let props = writer_builder.build();

        let (
            ingestion_queue_size,
            encoding_queue_size,
            writing_queue_size,
            max_row_group_concurrency,
        ) = resolve_queue_sizes(options);

        let config = PipelineConfig {
            max_row_group_size: options.max_row_group_size,
            max_row_group_concurrency,
            buffer_size: options.buffer_size,
            ingestion_queue_size,
            encoding_queue_size,
            writing_queue_size,
            skip_arrow_metadata: options.skip_arrow_metadata,
            dictionary_page_size_limit: options.dictionary_page_size_limit,
            dictionary_enabled_default: !options.no_dictionary,
            user_disabled_dictionary: options.column_no_dictionary.clone(),
            user_enabled_dictionary_always: options.column_dictionary_always.clone(),
            user_enabled_dictionary_analyze: options.column_dictionary_analyze.clone(),
            bloom_filter_config: if options.bloom_filters.is_configured() {
                Some(options.bloom_filters.clone())
            } else {
                None
            },
            ndv_map: options.ndv_map.clone(),
        };
        let (ingestion_sender, task) =
            pipeline::start_pipeline(target, schema, props, runtimes, config);

        Ok(Self {
            schema: Arc::clone(schema),
            ingestion_sender: Some(ingestion_sender),
            task: Some(task),
        })
    }

    fn validate_column_dictionary(
        schema: &SchemaRef,
        column_dictionary: &[String],
        column_no_dictionary: &[String],
    ) -> Result<()> {
        let dict_set: std::collections::HashSet<_> = column_dictionary.iter().collect();
        let no_dict_set: std::collections::HashSet<_> = column_no_dictionary.iter().collect();
        let conflicts: Vec<_> = dict_set.intersection(&no_dict_set).collect();
        if !conflicts.is_empty() {
            anyhow::bail!(
                "column(s) {} specified in both --parquet-dictionary-column and --parquet-dictionary-column-off",
                conflicts
                    .iter()
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        for col_name in column_dictionary {
            if Self::resolve_column_type(schema, col_name).is_none() {
                anyhow::bail!(
                    "column '{}' specified in --parquet-dictionary-column not found in schema",
                    col_name
                );
            }
        }

        for col_name in column_no_dictionary {
            if Self::resolve_column_type(schema, col_name).is_none() {
                anyhow::bail!(
                    "column '{}' specified in --parquet-dictionary-column-off not found in schema",
                    col_name
                );
            }
        }

        Ok(())
    }

    fn validate_column_encodings(
        schema: &SchemaRef,
        column_encodings: &[ColumnEncoding],
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

    fn resolve_column_type(schema: &SchemaRef, column_path: &str) -> Option<DataType> {
        let parts: Vec<&str> = column_path.split('.').collect();
        let mut current_type: Option<&DataType> = None;

        for (i, part) in parts.iter().enumerate() {
            if i == 0 {
                let field = schema.field_with_name(part).ok()?;
                current_type = Some(field.data_type());
            } else {
                match current_type? {
                    DataType::Struct(fields) => {
                        let field = fields.iter().find(|f| f.name() == part)?;
                        current_type = Some(field.data_type());
                    }
                    // for lists, maps, etc. - the path is valid if it starts with a valid column
                    _ => return current_type.cloned(),
                }
            }
        }

        current_type.cloned()
    }

    fn apply_encodings(
        mut builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
        writer_version: WriterVersion,
        default_encoding: &Option<Encoding>,
        column_encodings: &[ColumnEncoding],
    ) -> WriterPropertiesBuilder {
        if let Some(encoding) = default_encoding {
            builder = builder.set_encoding((*encoding).into());
        }

        // only apply automatic type-based encodings for V2
        // V1 uses PLAIN for maximum compatibility (matches DuckDB behavior)
        if writer_version == WriterVersion::V2 {
            for field in schema.fields() {
                let col_path = ColumnPath::from(field.name().as_str());
                let encoding = Self::resolve_v2_encoding_overridess(field.data_type());
                if let Some(encoding) = encoding {
                    builder = builder.set_column_encoding(col_path, encoding.into());
                }
            }
        }

        for col_encoding in column_encodings {
            let col_path = column_path_from_dot_notation(&col_encoding.name);
            builder = builder.set_column_encoding(col_path, col_encoding.encoding.into());
        }

        builder
    }

    fn resolve_v2_encoding_overridess(data_type: &DataType) -> Option<Encoding> {
        match data_type {
            DataType::Boolean => Some(Encoding::Plain),
            DataType::Float32 | DataType::Float64 => Some(Encoding::ByteStreamSplit),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(Encoding::DeltaBinaryPacked),
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView => Some(Encoding::DeltaLengthByteArray),
            _ => None,
        }
    }

    fn apply_sort_metadata(
        output_ordering: &[SortColumn],
        builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
    ) -> Result<WriterPropertiesBuilder> {
        if output_ordering.is_empty() {
            return Ok(builder);
        }

        let mut sorting_columns = Vec::new();

        for sort_col in output_ordering {
            let column_idx = schema
                .index_of(sort_col.name())
                .map_err(|_| anyhow!("Sort column '{}' not found in schema", sort_col.name()))?;

            let descending = sort_col.direction() == SortDirection::Descending;

            sorting_columns.push(SortingColumn {
                column_idx: i32::try_from(column_idx)
                    .map_err(|_| anyhow!("Column index out of range"))?,
                descending,
                nulls_first: sort_col.null_placement() == NullPlacement::First,
            });
        }

        Ok(builder.set_sorting_columns(Some(sorting_columns)))
    }
}

#[async_trait]
impl DataSink for Sink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        validate_batch_schema(&self.schema, batch.schema_ref())?;
        let sender = self
            .ingestion_sender
            .as_ref()
            .ok_or_else(|| anyhow!("sink already finished"))?;
        let cancellation = self
            .task
            .as_ref()
            .ok_or_else(|| anyhow!("sink already finished"))?
            .cancellation();
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                Err(anyhow!("batch send failed: Parquet pipeline closed"))
            }
            result = sender.send(batch) => {
                result.map_err(|_| anyhow!("batch send failed: Parquet pipeline closed"))
            }
        }
    }

    async fn finish(mut self: Box<Self>) -> Result<SinkCompletion> {
        self.ingestion_sender.take();
        let task = self
            .task
            .take()
            .ok_or_else(|| anyhow!("sink already finished"))?;
        let (rows_written, url) = task.finish().await?;

        Ok(SinkCompletion::new(url, [], rows_written))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(task) = &self.task {
            task.cancellation().cancel();
        }
        self.ingestion_sender.take();
        match self.task.take() {
            Some(task) => task.abort().await,
            None => Ok(()),
        }
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.cancellation().cancel();
        }
        self.ingestion_sender.take();
    }
}

pub(crate) async fn bind(
    context: &SinkBindingConfig,
    args: &crate::TransformArgs,
) -> Result<Box<dyn SinkBinding>> {
    let options = options(context, args)?;
    let runtimes = Arc::new(OutputRuntimes::try_new(
        args.parquet_column_encoding_threads
            .unwrap_or(context.thread_budget().get()),
        args.parquet_writing_threads.unwrap_or(1),
    )?);
    Ok(Box::new(OutputBinding { options, runtimes }))
}

fn options(context: &SinkBindingConfig, args: &crate::TransformArgs) -> Result<OutputConfig> {
    for disabled in &args.parquet_bloom_column_off {
        if args
            .parquet_bloom_column
            .iter()
            .any(|column| &column.name == disabled)
        {
            anyhow::bail!(
                "column '{disabled}' specified in both --parquet-bloom-column-off and --parquet-bloom-column"
            );
        }
    }
    for disabled in &args.parquet_dictionary_column_off {
        if args
            .parquet_dictionary_column
            .iter()
            .any(|column| &column.name == disabled)
        {
            anyhow::bail!(
                "column '{disabled}' specified in both --parquet-dictionary-column-off and --parquet-dictionary-column"
            );
        }
    }

    let all_enabled = if args.parquet_bloom_all_off {
        None
    } else {
        args.parquet_bloom_all
            .clone()
            .or(Some(crate::DefaultBloomFilterPolicy {
                fpp: crate::DEFAULT_BLOOM_FILTER_FPP,
                ndv: None,
            }))
    };
    let bloom_filters = crate::BloomFilterPolicy::try_new(
        all_enabled,
        args.parquet_bloom_column.clone(),
        args.parquet_bloom_column_off.clone(),
    )?;
    let output_ordering = if args.parquet_sorted_metadata {
        context.output_ordering().to_vec()
    } else {
        Vec::new()
    };

    let mut options = OutputConfig::new()
        .with_compression_and_level(args.parquet_compression, args.parquet_compression_level)?
        .with_statistics(args.parquet_statistics)
        .with_writer_version(args.parquet_writer_version)
        .with_ingestion_queue_size(args.parquet_ingestion_queue_size)
        .with_encoding_queue_size(args.parquet_encoding_queue_size)
        .with_writing_queue_size(args.parquet_writing_queue_size)
        .with_no_dictionary(args.parquet_dictionary_all_off)
        .with_dictionary_configs(&args.parquet_dictionary_column)
        .with_column_no_dictionary(args.parquet_dictionary_column_off.clone())
        .with_encoding(args.parquet_encoding)
        .with_column_encodings(args.parquet_column_encoding.clone())
        .with_bloom_filters(bloom_filters)
        .with_offset_index_enabled(args.parquet_offset_index)
        .with_skip_arrow_metadata(!args.parquet_arrow_metadata)
        .with_page_header_statistics(args.parquet_page_header_statistics)
        .apply_if_some(args.parquet_buffer_size, OutputConfig::with_buffer_size)
        .apply_if_some(
            args.parquet_row_group_size,
            OutputConfig::with_max_row_group_size,
        )
        .apply_if_some(
            args.parquet_row_group_concurrency,
            OutputConfig::with_max_row_group_concurrency,
        )
        .apply_if_some(
            args.parquet_data_page_size,
            OutputConfig::with_data_page_size_limit,
        )
        .apply_if_some(
            args.parquet_data_page_row_limit,
            OutputConfig::with_data_page_row_count_limit,
        )
        .apply_if_some(
            args.parquet_dictionary_page_size,
            OutputConfig::with_dictionary_page_size_limit,
        )
        .apply_if_some(
            args.parquet_write_batch_size,
            OutputConfig::with_write_batch_size,
        )
        .with_output_ordering(output_ordering);

    if context.open_sink_mode() == OpenSinkMode::Multiple {
        options = options
            .with_ingestion_queue_size(1)
            .with_encoding_queue_size(1)
            .with_writing_queue_size(1)
            .with_max_row_group_concurrency(1);
    }
    Ok(options)
}

struct OutputBinding {
    options: OutputConfig,
    runtimes: Arc<OutputRuntimes>,
}

#[async_trait]
impl SinkBinding for OutputBinding {
    async fn open_sink(
        &self,
        target: PreparedOutputTarget,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(Sink::create(
            target,
            &schema,
            &self.options,
            Arc::clone(&self.runtimes),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        BloomFilterSettings, ColumnBloomFilterPolicy, DefaultBloomFilterPolicy,
        inspection::Inspector,
    };
    use silk_chiffon_core::{NullPlacement, SortColumn, SortDirection};
    use silk_chiffon_test_support::{
        TestExtract, TestFile, batch as test_data,
        parquet::read_entire_file as read_entire_parquet_file, prepared_local_output_target,
        verify,
    };

    use camino::Utf8Path;
    use tempfile::tempdir;

    fn test_runtimes() -> Arc<OutputRuntimes> {
        Arc::new(OutputRuntimes::try_new(2, 1).unwrap())
    }

    fn sort_column(name: &str, direction: SortDirection) -> SortColumn {
        let null_placement = match direction {
            SortDirection::Ascending => NullPlacement::Last,
            SortDirection::Descending => NullPlacement::First,
        };
        SortColumn::new(name, direction, null_placement)
    }

    fn bloom_all(fpp: f64, ndv: Option<u64>) -> BloomFilterPolicy {
        BloomFilterPolicy::try_new(
            Some(DefaultBloomFilterPolicy { fpp, ndv }),
            Vec::new(),
            Vec::new(),
        )
        .unwrap()
    }

    fn bloom_column(name: &str, fpp: f64, ndv: Option<u64>) -> BloomFilterPolicy {
        BloomFilterPolicy::try_new(
            None,
            vec![ColumnBloomFilterPolicy {
                name: name.to_owned(),
                config: BloomFilterSettings { fpp, ndv },
            }],
            Vec::new(),
        )
        .unwrap()
    }

    fn inspect(path: &std::path::Path) -> Inspector {
        Inspector::open(Utf8Path::from_path(path).unwrap()).unwrap()
    }

    fn assert_has_dictionary(inspector: &Inspector, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups()[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            col.has_dictionary,
            "column '{}' should have dictionary, encodings: {:?}",
            col_name, col.encodings
        );
    }

    fn assert_no_dictionary(inspector: &Inspector, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups()[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            !col.has_dictionary,
            "column '{}' should NOT have dictionary, encodings: {:?}",
            col_name, col.encodings
        );
    }

    fn assert_has_bloom_filter(inspector: &Inspector, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups()[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            col.has_bloom_filter,
            "column '{}' should have bloom filter",
            col_name
        );
    }

    #[allow(dead_code)]
    fn assert_no_bloom_filter(inspector: &Inspector, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups()[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            !col.has_bloom_filter,
            "column '{}' should NOT have bloom filter",
            col_name
        );
    }

    #[allow(dead_code)]
    fn assert_has_encoding(inspector: &Inspector, col_name: &str, encoding: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups()[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            col.encodings.iter().any(|e| e.contains(encoding)),
            "column '{}' should have {} encoding, got: {:?}",
            col_name,
            encoding,
            col.encodings
        );
    }

    #[allow(dead_code)]
    fn assert_no_encoding(inspector: &Inspector, col_name: &str, encoding: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups()[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            !col.encodings.iter().any(|e| e.contains(encoding)),
            "column '{}' should NOT have {} encoding, got: {:?}",
            col_name,
            encoding,
            col.encodings
        );
    }

    mod sink_tests {
        use std::fs::File;

        use super::*;

        use arrow::array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
        use arrow::array::{Array, StringArray};
        use arrow::datatypes::Field;
        use parquet::arrow::ArrowWriter;
        use parquet::file::reader::FileReader;
        use rand::{RngExt, SeedableRng, rngs::SmallRng};

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_writes_single_batch_with_single_row_group() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new(),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 3);
            assert_eq!(result.durable_locations().len(), 1);
            assert_eq!(
                result.durable_locations()[0],
                url::Url::from_file_path(&output_path).unwrap()
            );

            let batches = TestFile::read_parquet(&output_path);
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(file.row_groups.len(), 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_default_file_metadata_is_stable() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new(),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = parquet::file::reader::SerializedFileReader::try_from(
                std::fs::File::open(&output_path).unwrap(),
            )
            .unwrap();
            let file_metadata = file.metadata().file_metadata();

            assert_eq!(file_metadata.created_by(), Some(PARQUET_CREATED_BY));
            assert!(file_metadata.key_value_metadata().is_none());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_writes_multiple_batches_with_single_row_group() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[3, 4], &["c", "d"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new(),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 4);

            let batches = TestFile::read_parquet(&output_path);
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 4);

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(file.row_groups.len(), 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_multiple_row_groups() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[3, 4], &["c", "d"]);
            let batch3 = test_data::create_batch_with_ids_and_names(&schema, &[5], &["e"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_max_row_group_size(3),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            sink.write_batch(batch3).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 5);

            let batches = TestFile::read_parquet(&output_path);
            assert_eq!(batches.len(), 1);

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(file.row_groups.len(), 2);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn row_group_limit_splits_batches_at_every_boundary() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("row-groups.parquet");
            let schema = test_data::simple_schema();
            let batches = [
                test_data::create_batch_with_ids_and_names(&schema, &[], &[]),
                test_data::create_batch_with_ids_and_names(&schema, &[0, 1], &["a", "b"]),
                test_data::create_batch_with_ids_and_names(
                    &schema,
                    &[2, 3, 4, 5, 6, 7, 8],
                    &["c", "d", "e", "f", "g", "h", "i"],
                ),
                test_data::create_batch_with_ids_and_names(&schema, &[9], &["j"]),
            ];
            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new()
                    .with_max_row_group_size(3)
                    .with_max_row_group_concurrency(2)
                    .with_ingestion_queue_size(1)
                    .with_encoding_queue_size(1)
                    .with_writing_queue_size(1),
                test_runtimes(),
            )
            .unwrap();

            for batch in batches {
                sink.write_batch(batch).await.unwrap();
            }
            let completion = Box::new(sink).finish().await.unwrap();

            assert_eq!(completion.rows_written(), 10);
            let file = read_entire_parquet_file(&output_path).unwrap();
            assert_eq!(
                file.row_groups
                    .iter()
                    .map(|row_group| row_group.num_rows)
                    .collect::<Vec<_>>(),
                [3, 3, 3, 1]
            );
            let batches = TestFile::read_parquet(&output_path);
            verify::assert_id_name_batch_data_matches(
                &batches[0],
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn seeded_batch_shapes_preserve_rows_under_maximum_concurrency() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("seeded-row-groups.parquet");
            let schema = test_data::simple_schema();
            let mut rng = SmallRng::seed_from_u64(0x5eed_cafe);
            let mut expected_ids = Vec::new();
            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new()
                    .with_max_row_group_size(17)
                    .with_max_row_group_concurrency(4)
                    .with_ingestion_queue_size(1)
                    .with_encoding_queue_size(1)
                    .with_writing_queue_size(1),
                test_runtimes(),
            )
            .unwrap();

            for _ in 0..64 {
                let batch_size = rng.random_range(0..=40);
                let ids = (i32::try_from(expected_ids.len()).unwrap()..)
                    .take(batch_size)
                    .collect::<Vec<_>>();
                let names = ids
                    .iter()
                    .map(|id| format!("value-{id}"))
                    .collect::<Vec<_>>();
                let name_refs = names.iter().map(String::as_str).collect::<Vec<_>>();
                sink.write_batch(test_data::create_batch_with_ids_and_names(
                    &schema, &ids, &name_refs,
                ))
                .await
                .unwrap();
                expected_ids.extend(ids);
            }
            let completion = Box::new(sink).finish().await.unwrap();

            assert_eq!(completion.rows_written(), expected_ids.len() as u64);
            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(
                file.row_groups
                    .iter()
                    .all(|row_group| row_group.num_rows <= 17)
            );
            assert!(
                file.row_groups
                    .iter()
                    .take(file.row_groups.len().saturating_sub(1))
                    .all(|row_group| row_group.num_rows == 17)
            );
            assert_eq!(
                TestExtract::i32_all(&TestFile::read_parquet(&output_path), "id"),
                expected_ids
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_compression() {
            for compression in [
                CompressionCodec::ZSTD(Default::default()),
                CompressionCodec::SNAPPY,
                CompressionCodec::GZIP(Default::default()),
                CompressionCodec::BROTLI(Default::default()),
                CompressionCodec::LZ4_RAW,
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

                let mut compressed_sink = Sink::create(
                    prepared_local_output_target(&output_path),
                    &schema,
                    &OutputConfig::new().with_compression(compression),
                    test_runtimes(),
                )
                .unwrap();

                compressed_sink.write_batch(batch.clone()).await.unwrap();
                Box::new(compressed_sink).finish().await.unwrap();

                let file = read_entire_parquet_file(&output_path).unwrap();
                assert_eq!(file.row_groups.len(), 1);
                assert!(file.total_compressed_size_bytes < file.total_uncompressed_size_bytes);
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_empty_batches() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();

            let sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new(),
                test_runtimes(),
            )
            .unwrap();

            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 0);
            assert!(output_path.exists());

            let batches = TestFile::read_parquet(&output_path);
            assert_eq!(batches.len(), 0);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_write_stream() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch1 =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[4, 5], &["d", "e"]);

            let ctx = datafusion::prelude::SessionContext::new();
            let provider = Arc::new(
                datafusion::datasource::MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![batch1, batch2]],
                )
                .unwrap(),
            );
            let stream = ctx
                .read_table(provider)
                .unwrap()
                .execute_stream()
                .await
                .unwrap();

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new(),
                test_runtimes(),
            )
            .unwrap();

            sink.write_stream(stream).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();
            assert_eq!(result.rows_written(), 5);

            let batches = TestFile::read_parquet(&output_path);
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_sort_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[3, 1, 2], &["c", "a", "b"]);

            let output_ordering = vec![sort_column("id", SortDirection::Ascending)];

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_output_ordering(output_ordering),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].sorting_columns.is_some());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_without_sort_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[3, 1, 2], &["c", "a", "b"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new(),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].sorting_columns.is_none());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_without_bloom_filters() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = BloomFilterPolicy::default();

            let options = OutputConfig::new().with_bloom_filters(bloom_filters);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(!file.has_any_bloom_filters);
            assert!(!file.row_groups[0].columns[0].has_bloom_filter);
            assert!(!file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_bloom_filters() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            // Without an explicit NDV, bloom filters follow the dictionary decision.
            // specify NDV to force unconditional bloom filter enablement
            let bloom_filters = bloom_all(0.01, Some(3));

            let options = OutputConfig::new().with_bloom_filters(bloom_filters);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_bloom_filters_specific_columns() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            // specify NDV to force unconditional bloom filter enablement for id column only
            let bloom_filters = bloom_column("id", 0.05, Some(3));

            let options = OutputConfig::new().with_bloom_filters(bloom_filters);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(!file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_bloom_all_enabled_but_specific_columns_disabled() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            // specify NDV to force unconditional bloom filter enablement, but disable id
            let bloom_filters = BloomFilterPolicy::try_new(
                Some(DefaultBloomFilterPolicy {
                    fpp: 0.01,
                    ndv: Some(3),
                }),
                Vec::new(),
                vec!["id".to_owned()],
            )
            .unwrap();

            let options = OutputConfig::new().with_bloom_filters(bloom_filters);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

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

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_without_dictionary_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(
                &schema,
                &[1, 2, 3, 4, 5],
                &["a", "b", "c", "d", "e"],
            );

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_no_dictionary(true),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(!file.has_any_dictionary);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_dictionary_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            // Repeated values keep the dictionary below the cardinality threshold.
            // 2 unique values in 100 rows = 2% cardinality, well below the 20% threshold
            let ids: Vec<i32> = (0..100).map(|i| i % 2).collect();
            let names: Vec<&str> = (0..100)
                .map(|i| if i % 2 == 0 { "a" } else { "b" })
                .collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_no_dictionary(false),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_dictionary);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_statistics_options() {
            for statistics in [Statistics::None, Statistics::Chunk, Statistics::Page] {
                let temp_dir = tempdir().unwrap();
                let output_path = temp_dir.path().join("output.parquet");

                let schema = test_data::simple_schema();
                let batch = test_data::create_batch_with_ids_and_names(
                    &schema,
                    &[1, 2, 3],
                    &["a", "b", "c"],
                );

                let mut sink = Sink::create(
                    prepared_local_output_target(&output_path),
                    &schema,
                    &OutputConfig::new().with_statistics(statistics),
                    test_runtimes(),
                )
                .unwrap();

                sink.write_batch(batch).await.unwrap();
                Box::new(sink).finish().await.unwrap();

                let file = read_entire_parquet_file(&output_path).unwrap();

                match statistics {
                    Statistics::None => {
                        assert!(
                            file.row_groups[0].columns[0].statistics.is_none(),
                            "statistics should be none for None"
                        );
                    }
                    Statistics::Chunk => {
                        assert!(
                            file.row_groups[0].columns[0].statistics.is_some(),
                            "statistics should be some for Chunk"
                        );
                    }
                    Statistics::Page => {
                        assert!(
                            file.row_groups[0].columns[0].statistics.is_some(),
                            "statistics should be some for Page"
                        );
                    }
                }
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_writer_versions() {
            for version in [WriterVersion::V1, WriterVersion::V2] {
                let temp_dir = tempdir().unwrap();
                let output_path = temp_dir.path().join("output.parquet");

                let schema = test_data::simple_schema();
                let batch = test_data::create_batch_with_ids_and_names(
                    &schema,
                    &[1, 2, 3],
                    &["a", "b", "c"],
                );

                let mut sink = Sink::create(
                    prepared_local_output_target(&output_path),
                    &schema,
                    &OutputConfig::new().with_writer_version(version),
                    test_runtimes(),
                )
                .unwrap();

                sink.write_batch(batch).await.unwrap();
                Box::new(sink).finish().await.unwrap();

                let file = read_entire_parquet_file(&output_path).unwrap();
                assert_eq!(file.metadata.version(), Into::<i32>::into(version));
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_sort_metadata_invalid_column() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();

            let output_ordering = vec![sort_column("nonexistent_column", SortDirection::Ascending)];

            let result = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_output_ordering(output_ordering),
                test_runtimes(),
            );

            assert!(result.is_err());
            let err = result.err().unwrap();
            assert!(err.to_string().contains("nonexistent_column"));
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_bloom_filter_without_ndv() {
            // tests bloom filter enablement without explicit NDV
            // Without an explicit NDV, bloom filters require the dictionary to be kept.
            // so we use low-cardinality data to ensure dictionary stays enabled
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            // 2 unique values in 100 rows = low cardinality, dictionary stays enabled
            let ids: Vec<i32> = (0..100).map(|i| i % 2).collect();
            let names: Vec<&str> = (0..100)
                .map(|i| if i % 2 == 0 { "a" } else { "b" })
                .collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let bloom_filters = bloom_column("id", 0.05, None);

            let options = OutputConfig::new().with_bloom_filters(bloom_filters);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_bloom_filter_all_columns_with_user_ndv() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = bloom_all(0.01, Some(5000));

            let options = OutputConfig::new().with_bloom_filters(bloom_filters);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(file.row_groups[0].columns[1].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_bloom_filter_column_with_user_ndv() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = bloom_column("id", 0.01, Some(10000));

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 500);

            let options = OutputConfig::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_ndv_map_as_sole_ndv_source() {
            // test that ndv_map provides NDV when bloom filter config has no explicit NDV
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            // bloom filter WITHOUT explicit NDV - should use ndv_map
            let bloom_filters = bloom_column("id", 0.01, None);

            // provide NDV via ndv_map instead
            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 1000);

            let options = OutputConfig::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            // bloom filter should be enabled using ndv_map value
            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_multi_column_sort_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[3, 1, 2], &["c", "a", "b"]);

            let output_ordering = vec![
                SortColumn::new("name", SortDirection::Ascending, NullPlacement::First),
                SortColumn::new("id", SortDirection::Descending, NullPlacement::Last),
            ];

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_output_ordering(output_ordering),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            let sorting = file.row_groups[0].sorting_columns.as_ref().unwrap();
            assert_eq!(sorting.len(), 2);
            assert!(sorting[0].nulls_first);
            assert!(!sorting[1].nulls_first);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_simple_pipeline_dictionary_always() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let ids: Vec<i32> = (0..1000).collect();
            let names: Vec<&str> = (0..1000)
                .map(|i| if i % 2 == 0 { "a" } else { "b" })
                .collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let options = OutputConfig::new().with_column_dictionary_always(vec!["id".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].columns[0].has_dictionary);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_simple_pipeline_bloom_with_explicit_ndv() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = bloom_column("id", 0.01, Some(100));

            let options = OutputConfig::new()
                .with_bloom_filters(bloom_filters)
                .with_no_dictionary(true);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
            assert!(!file.row_groups[0].columns[0].has_dictionary);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_simple_pipeline_ndv_map_skips_analysis() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let bloom_filters = bloom_column("id", 0.01, None);

            let mut ndv_map = HashMap::new();
            ndv_map.insert("id".to_string(), 100);

            let options = OutputConfig::new()
                .with_bloom_filters(bloom_filters)
                .with_ndv_map(ndv_map)
                .with_no_dictionary(true);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.has_any_bloom_filters);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_analysis_pipeline_dictionary_analyze_low_cardinality() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let ids: Vec<i32> = (0..1000).map(|i| i % 5).collect();
            let names: Vec<&str> = (0..1000).map(|_| "test").collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let options =
                OutputConfig::new().with_column_dictionary_analyze(vec!["id".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].columns[0].has_dictionary);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_analysis_pipeline_dictionary_analyze_high_cardinality() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let ids: Vec<i32> = (0..1000).collect();
            let names: Vec<&str> = (0..1000).map(|_| "test").collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let options =
                OutputConfig::new().with_column_dictionary_analyze(vec!["id".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(!file.row_groups[0].columns[0].has_dictionary);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_analysis_pipeline_bloom_needs_analysis() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let ids: Vec<i32> = (0..100).map(|i| i % 5).collect();
            let names: Vec<&str> = (0..100).map(|_| "test").collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let bloom_filters = bloom_column("id", 0.01, None);

            let options = OutputConfig::new()
                .with_bloom_filters(bloom_filters)
                .with_column_dictionary_analyze(vec!["id".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].columns[0].has_dictionary);
            assert!(file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_analysis_pipeline_high_cardinality_disables_dict_and_bloom() {
            // bloom filters only enabled when dictionary encoding is used (matches DuckDB)
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let ids: Vec<i32> = (0..1000).collect();
            let names: Vec<&str> = (0..1000).map(|_| "test").collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let bloom_filters = bloom_column("id", 0.01, None);

            let options = OutputConfig::new()
                .with_bloom_filters(bloom_filters)
                .with_column_dictionary_analyze(vec!["id".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            // high cardinality (100% distinct) disables dictionary
            assert!(!file.row_groups[0].columns[0].has_dictionary);
            // bloom filter also disabled when dictionary is disabled (matches DuckDB)
            assert!(!file.row_groups[0].columns[0].has_bloom_filter);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_mixed_simple_and_analysis_columns() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let ids: Vec<i32> = (0..100).collect();
            let names: Vec<&str> = (0..100)
                .map(|i| if i % 2 == 0 { "a" } else { "b" })
                .collect();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);

            let options = OutputConfig::new()
                .with_column_dictionary_always(vec!["id".to_string()])
                .with_column_dictionary_analyze(vec!["name".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            assert!(file.row_groups[0].columns[0].has_dictionary);
            assert!(file.row_groups[0].columns[1].has_dictionary);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_nested_column_dictionary_always_mode() {
            // nested columns can't be analyzed with DataFusion's approx_distinct
            // so we test that explicit "Always" mode works for nested columns
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            // build nested list: List<List<Int32>>
            let inner_builder = ListBuilder::new(Int32Builder::new());
            let mut builder = ListBuilder::new(inner_builder);
            for i in 0i32..1000 {
                builder.values().values().append_value(i % 5);
                builder.values().append(true);
                builder.append(true);
            }
            let list_of_list_array = builder.finish();

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "col",
                list_of_list_array.data_type().clone(),
                true,
            )]));

            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list_of_list_array)])
                    .unwrap();

            // use "Always" mode since nested types can't be analyzed
            let options =
                OutputConfig::new().with_column_dictionary_always(vec!["col".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();
            let leaf = file.row_groups[0]
                .columns
                .iter()
                .find(|c| c.name == "col.list.element.list.element")
                .expect("should find leaf column col.list.element.list.element");
            assert!(
                leaf.has_dictionary,
                "nested column leaf should use dictionary with Always mode"
            );
        }

        #[test]
        fn test_map_parquet_direct_write() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("map_direct.parquet");

            let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            for i in 0usize..10 {
                builder.keys().append_value(["a", "b", "c"][i % 3]);
                builder.values().append_value((i % 5) as i32);
                builder.append(true).unwrap();
            }
            let map_array = builder.finish();

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Arc::new(
                arrow::datatypes::Field::new("col", map_array.data_type().clone(), true),
            )]));
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(map_array)]).unwrap();

            let file = File::create(&output_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            assert!(output_path.exists());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_custom_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut metadata = HashMap::new();
            metadata.insert("custom_key".to_string(), Some("custom_value".to_string()));
            metadata.insert("another_key".to_string(), None);

            let options = OutputConfig::new().with_metadata(metadata);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = parquet::file::reader::SerializedFileReader::try_from(
                std::fs::File::open(&output_path).unwrap(),
            )
            .unwrap();
            let file_metadata = file.metadata().file_metadata();
            let kv_metadata = file_metadata.key_value_metadata().unwrap();

            let custom_kv = kv_metadata
                .iter()
                .find(|kv| kv.key == "custom_key")
                .unwrap();
            assert_eq!(custom_kv.value, Some("custom_value".to_string()));

            let another_kv = kv_metadata
                .iter()
                .find(|kv| kv.key == "another_key")
                .unwrap();
            assert_eq!(another_kv.value, None);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_analysis_resets_between_row_groups() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            // simple schema: one Utf8 column
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "col",
                DataType::Utf8,
                false,
            )]));

            // configure with small row group size to force multiple row groups
            // and dictionary_analyze for our column
            let options = OutputConfig::new()
                .with_max_row_group_size(100)
                .with_column_dictionary_analyze(vec!["col".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();

            // batch 1: LOW cardinality (5 distinct values in 100 rows = 5%)
            let low_cardinality_values: Vec<&str> =
                (0..100).map(|i| ["a", "b", "c", "d", "e"][i % 5]).collect();
            let batch1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(low_cardinality_values))],
            )
            .unwrap();

            // batch 2: HIGH cardinality (100 distinct values in 100 rows = 100%)
            let high_cardinality_values: Vec<String> =
                (0..100).map(|i| format!("val_{i}")).collect();
            let batch2 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(
                    high_cardinality_values
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>(),
                ))],
            )
            .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            let _result = Box::new(sink).finish().await.unwrap();

            let file = read_entire_parquet_file(&output_path).unwrap();

            // verify 2 row groups were written
            assert_eq!(file.row_groups.len(), 2, "expected 2 row groups");

            // row group 0 (low cardinality): should HAVE dictionary
            assert!(
                file.row_groups[0].columns[0].has_dictionary,
                "row group 0 should have dictionary (low cardinality)"
            );

            // row group 1 (high cardinality): should NOT have dictionary
            assert!(
                !file.row_groups[1].columns[0].has_dictionary,
                "row group 1 should NOT have dictionary (high cardinality)"
            );
        }
    }

    mod output_config_tests {
        use super::*;

        #[test]
        fn test_default_options() {
            let options = OutputConfig::default();
            assert_eq!(options.max_row_group_size, DEFAULT_MAX_ROW_GROUP_SIZE);
            assert!(matches!(
                options.compression,
                CompressionCodec::UNCOMPRESSED
            ));
            assert!(!options.bloom_filters.is_configured());
            assert!(matches!(options.statistics, Statistics::Chunk));
            assert!(!options.no_dictionary);
            assert!(matches!(options.writer_version, WriterVersion::V2));
            assert!(options.metadata.is_empty());
        }

        #[test]
        fn test_config_composition() {
            let mut metadata = HashMap::new();
            metadata.insert("test".to_string(), Some("value".to_string()));

            let output_ordering = vec![sort_column("id", SortDirection::Ascending)];

            let bloom_filters = bloom_all(0.01, None);

            let options = OutputConfig::new()
                .with_max_row_group_size(5000)
                .with_output_ordering(output_ordering.clone())
                .with_compression(CompressionCodec::ZSTD(Default::default()))
                .with_bloom_filters(bloom_filters)
                .with_statistics(Statistics::None)
                .with_no_dictionary(true)
                .with_writer_version(WriterVersion::V1)
                .with_metadata(metadata.clone());

            assert_eq!(options.max_row_group_size, 5000);
            assert_eq!(options.output_ordering.len(), 1);
            assert!(matches!(options.compression, CompressionCodec::ZSTD(_)));
            assert!(options.bloom_filters.is_configured());
            assert!(matches!(options.statistics, Statistics::None));
            assert!(options.no_dictionary);
            assert!(matches!(options.writer_version, WriterVersion::V1));
            assert_eq!(options.metadata, metadata);
        }

        #[test]
        fn test_metadata_value() {
            let options = OutputConfig::new()
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

            let options = OutputConfig::new().with_metadata(metadata.clone());

            assert_eq!(options.metadata, metadata);
        }

        #[test]
        fn test_encoding_options() {
            let options = OutputConfig::new()
                .with_encoding(Some(Encoding::DeltaBinaryPacked))
                .with_column_encodings(vec![ColumnEncoding {
                    name: "name".to_string(),
                    encoding: Encoding::DeltaByteArray,
                }]);

            assert_eq!(options.encoding, Some(Encoding::DeltaBinaryPacked));
            assert_eq!(options.column_encodings.len(), 1);
            assert_eq!(options.column_encodings[0].name, "name");
            assert_eq!(
                options.column_encodings[0].encoding,
                Encoding::DeltaByteArray
            );
        }

        #[test]
        fn default_queue_sizes_are_explicit() {
            let options = OutputConfig::new();
            let (ing, enc, wrt, conc) = resolve_queue_sizes(&options);
            assert_eq!(ing, 1);
            assert_eq!(enc, 4);
            assert_eq!(wrt, 4);
            assert_eq!(conc, 4);
        }
    }

    mod encoding_tests {
        use arrow::{
            array::{Int32Array, StructArray},
            datatypes::Field,
        };

        use super::*;
        use std::sync::Arc;

        use crate::{ColumnEncoding, Encoding};

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_default_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_encoding(Some(Encoding::Plain)),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 3);
            assert!(output_path.exists());

            let batches = TestFile::read_parquet(&output_path);
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_column_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_column_encodings(vec![ColumnEncoding {
                    name: "id".to_string(),
                    encoding: Encoding::DeltaBinaryPacked,
                }]),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 3);
            assert!(output_path.exists());

            let batches = TestFile::read_parquet(&output_path);
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_multiple_column_encodings() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_column_encodings(vec![
                    ColumnEncoding {
                        name: "id".to_string(),
                        encoding: Encoding::DeltaBinaryPacked,
                    },
                    ColumnEncoding {
                        name: "name".to_string(),
                        encoding: Encoding::DeltaByteArray,
                    },
                ]),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 3);
            assert!(output_path.exists());

            let batches = TestFile::read_parquet(&output_path);
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_sink_with_nested_column_encoding() {
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
            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &OutputConfig::new().with_column_encodings(vec![ColumnEncoding {
                    name: "outer.inner".to_string(),
                    encoding: Encoding::DeltaBinaryPacked,
                }]),
                test_runtimes(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 3);
            assert!(output_path.exists());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_nested_column_dictionary_dot_notation() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let inner_field = Field::new("inner", arrow::datatypes::DataType::Utf8, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field.clone()].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            let inner_array = arrow::array::StringArray::from(vec!["a", "b", "c"]);
            let outer_array =
                StructArray::from(vec![(Arc::new(inner_field), Arc::new(inner_array) as _)]);
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(outer_array)]).unwrap();

            let options =
                OutputConfig::new().with_column_dictionary_always(vec!["outer.inner".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let inspector = inspect(&output_path);
            assert_has_dictionary(&inspector, "outer.inner");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_nested_column_bloom_filter_dot_notation() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let inner_field = Field::new("inner", arrow::datatypes::DataType::Utf8, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field.clone()].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            let inner_array = arrow::array::StringArray::from(vec!["a", "b", "c"]);
            let outer_array =
                StructArray::from(vec![(Arc::new(inner_field), Arc::new(inner_array) as _)]);
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(outer_array)]).unwrap();

            let bloom_config = bloom_column("outer.inner", 0.01, Some(100));

            let options = OutputConfig::new().with_bloom_filters(bloom_config);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let inspector = inspect(&output_path);
            assert_has_bloom_filter(&inspector, "outer.inner");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_nested_column_no_dictionary_dot_notation() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let inner_field = Field::new("inner", arrow::datatypes::DataType::Utf8, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field.clone()].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            let inner_array = arrow::array::StringArray::from(vec!["a", "b", "c"]);
            let outer_array =
                StructArray::from(vec![(Arc::new(inner_field), Arc::new(inner_array) as _)]);
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(outer_array)]).unwrap();

            let options =
                OutputConfig::new().with_column_no_dictionary(vec!["outer.inner".to_string()]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let inspector = inspect(&output_path);
            assert_no_dictionary(&inspector, "outer.inner");
        }

        #[test]
        fn test_resolve_column_type_top_level() {
            let schema = test_data::simple_schema();
            let data_type = Sink::resolve_column_type(&schema, "id");
            assert_eq!(data_type, Some(arrow::datatypes::DataType::Int32));
        }

        #[test]
        fn test_resolve_column_type_nested() {
            let inner_field = Field::new("inner", arrow::datatypes::DataType::Int64, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            let data_type = Sink::resolve_column_type(&schema, "outer.inner");
            assert_eq!(data_type, Some(arrow::datatypes::DataType::Int64));
        }

        #[test]
        fn test_resolve_column_type_not_found() {
            let schema = test_data::simple_schema();
            let data_type = Sink::resolve_column_type(&schema, "nonexistent");
            assert_eq!(data_type, None);
        }

        #[test]
        fn test_resolve_column_type_nested_not_found() {
            let inner_field = Field::new("inner", arrow::datatypes::DataType::Int64, false);
            let outer_field = Field::new(
                "outer",
                arrow::datatypes::DataType::Struct(vec![inner_field].into()),
                false,
            );
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![outer_field]));

            let data_type = Sink::resolve_column_type(&schema, "outer.nonexistent");
            assert_eq!(data_type, None);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_dictionary_column_conflict_rejected() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");
            let schema = test_data::simple_schema();

            let options = OutputConfig::new()
                .with_column_dictionary_always(vec!["name".to_string()])
                .with_column_no_dictionary(vec!["name".to_string()]);

            let result = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            );
            match result {
                Ok(_) => panic!("should reject conflicting dictionary settings"),
                Err(err) => {
                    let msg = err.to_string();
                    assert!(msg.contains("'name'"));
                    assert!(msg.contains("--parquet-dictionary-column"));
                    assert!(msg.contains("--parquet-dictionary-column-off"));
                }
            }
        }
    }

    mod default_encoding_tests {
        use std::path::Path;

        use super::*;
        use arrow::array::{BooleanArray, Float32Array, Float64Array, Int64Array};
        use arrow::datatypes::Field;
        use parquet::basic::Encoding as ParquetEncoding;

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

        fn get_column_encodings(path: &Path) -> HashMap<String, Vec<ParquetEncoding>> {
            let file = read_entire_parquet_file(path).unwrap();
            file.row_groups[0]
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.encodings.clone()))
                .collect()
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_v2_applies_byte_stream_split_to_floats() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V2)
                .with_no_dictionary(true); // disable dict to see the fallback encoding

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["float_col"].contains(&ParquetEncoding::BYTE_STREAM_SPLIT),
                "float_col should use BYTE_STREAM_SPLIT, got {:?}",
                encodings["float_col"]
            );
            assert!(
                encodings["double_col"].contains(&ParquetEncoding::BYTE_STREAM_SPLIT),
                "double_col should use BYTE_STREAM_SPLIT, got {:?}",
                encodings["double_col"]
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_v2_applies_delta_binary_packed_to_integers() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V2)
                .with_no_dictionary(true);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["int_col"].contains(&ParquetEncoding::DELTA_BINARY_PACKED),
                "int_col should use DELTA_BINARY_PACKED, got {:?}",
                encodings["int_col"]
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_booleans_always_use_plain() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            // disable dictionary to isolate encoding behavior from dictionary decisions
            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V2)
                .with_no_dictionary(true);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            // booleans should use PLAIN for data encoding
            // (RLE may still appear for definition/repetition levels)
            assert!(
                encodings["bool_col"].contains(&ParquetEncoding::PLAIN),
                "bool_col should use PLAIN, got {:?}",
                encodings["bool_col"]
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_uses_optimal_encodings() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V2)
                .with_no_dictionary(true);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);

            assert!(
                encodings["float_col"].contains(&ParquetEncoding::BYTE_STREAM_SPLIT),
                "float_col should use BYTE_STREAM_SPLIT"
            );
            assert!(
                encodings["int_col"].contains(&ParquetEncoding::DELTA_BINARY_PACKED),
                "int_col should use DELTA_BINARY_PACKED"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_v1_uses_plain_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V1)
                .with_no_dictionary(true);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);

            assert!(
                !encodings["float_col"].contains(&ParquetEncoding::BYTE_STREAM_SPLIT),
                "V1 float_col should NOT use BYTE_STREAM_SPLIT"
            );
            assert!(
                !encodings["int_col"].contains(&ParquetEncoding::DELTA_BINARY_PACKED),
                "V1 int_col should NOT use DELTA_BINARY_PACKED"
            );
            assert!(
                encodings["float_col"].contains(&ParquetEncoding::PLAIN),
                "V1 float_col should use PLAIN"
            );
            assert!(
                encodings["int_col"].contains(&ParquetEncoding::PLAIN),
                "V1 int_col should use PLAIN"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_user_encoding_overrides_default() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = numeric_schema();
            let batch = create_numeric_batch(&schema);

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V2)
                .with_no_dictionary(true)
                .with_column_encodings(vec![ColumnEncoding {
                    name: "float_col".to_string(),
                    encoding: Encoding::Plain,
                }]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["float_col"].contains(&ParquetEncoding::PLAIN),
                "float_col should use PLAIN (user override)"
            );
            assert!(
                !encodings["float_col"].contains(&ParquetEncoding::BYTE_STREAM_SPLIT),
                "float_col should NOT use BYTE_STREAM_SPLIT when overridden"
            );
            assert!(
                encodings["double_col"].contains(&ParquetEncoding::BYTE_STREAM_SPLIT),
                "double_col should use BYTE_STREAM_SPLIT"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_v1_writer_with_delta_binary_packed_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "int_col",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
            )
            .unwrap();

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V1)
                .with_no_dictionary(true)
                .with_column_encodings(vec![ColumnEncoding {
                    name: "int_col".to_string(),
                    encoding: Encoding::DeltaBinaryPacked,
                }]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["int_col"].contains(&ParquetEncoding::DELTA_BINARY_PACKED),
                "int_col should use DELTA_BINARY_PACKED with V1 writer"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_v1_writer_with_byte_stream_split_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "float_col",
                DataType::Float64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]))],
            )
            .unwrap();

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V1)
                .with_no_dictionary(true)
                .with_column_encodings(vec![ColumnEncoding {
                    name: "float_col".to_string(),
                    encoding: Encoding::ByteStreamSplit,
                }]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["float_col"].contains(&ParquetEncoding::BYTE_STREAM_SPLIT),
                "float_col should use BYTE_STREAM_SPLIT with V1 writer"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_v1_writer_with_delta_length_byte_array_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "str_col",
                DataType::Utf8,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arrow::array::StringArray::from(vec![
                    "hello", "world", "test", "data", "here",
                ]))],
            )
            .unwrap();

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V1)
                .with_no_dictionary(true)
                .with_column_encodings(vec![ColumnEncoding {
                    name: "str_col".to_string(),
                    encoding: Encoding::DeltaLengthByteArray,
                }]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["str_col"].contains(&ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY),
                "str_col should use DELTA_LENGTH_BYTE_ARRAY with V1 writer"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_v1_writer_with_delta_byte_array_encoding() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.parquet");

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "str_col",
                DataType::Utf8,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arrow::array::StringArray::from(vec![
                    "apple",
                    "application",
                    "apply",
                    "append",
                    "apps",
                ]))],
            )
            .unwrap();

            let options = OutputConfig::new()
                .with_writer_version(WriterVersion::V1)
                .with_no_dictionary(true)
                .with_column_encodings(vec![ColumnEncoding {
                    name: "str_col".to_string(),
                    encoding: Encoding::DeltaByteArray,
                }]);

            let mut sink = Sink::create(
                prepared_local_output_target(&output_path),
                &schema,
                &options,
                test_runtimes(),
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let encodings = get_column_encodings(&output_path);
            assert!(
                encodings["str_col"].contains(&ParquetEncoding::DELTA_BYTE_ARRAY),
                "str_col should use DELTA_BYTE_ARRAY with V1 writer"
            );
        }
    }
}
