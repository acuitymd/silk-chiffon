use std::{ffi::OsString, sync::Arc};

use anyhow::{Context, Result, anyhow};
use apply_if::ApplyIf;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use camino::Utf8PathBuf;
use clap::{
    Args, Command as ClapCommand, CommandFactory, FromArgMatches, builder::PossibleValuesParser,
};
use datafusion::prelude::SessionContext;
use silk_chiffon_core::{
    DataSink, DataSource, FormatDefinition, FormatFuture, FormatMatch, FormatRegistry,
    InspectionDefinition, InspectionMode, InspectionOutput, OutputOrderingColumn, SinkBinding,
    SinkBindingConfig, TransformDefinition,
};
use silk_chiffon_storage::{StorageHandle, StorageRegistry};

#[cfg(feature = "local")]
use silk_chiffon_storage::local;

use crate::{
    AllColumnsBloomFilterConfig, ArrowArgs, BloomFilterConfig, Cli, Command as RuntimeCommand,
    DEFAULT_BLOOM_FILTER_FPP, DetectArgs, DetectCommand, InspectArrowArgs, InspectCommand,
    InspectParquetArgs, InspectVortexArgs, InspectionArgs, OutputFormat, ParquetArgs, SortColumn,
    SortSpec, TransformArgs, TransformCommand, VortexArgs,
    inspection::{
        arrow::ArrowInspector, inspectable::Inspectable, parquet::ParquetInspector,
        vortex::VortexInspector,
    },
    sinks::{
        arrow::{ArrowSink, ArrowSinkOptions},
        parquet::{ParquetRuntimes, ParquetSink, ParquetSinkOptions},
        vortex::{VortexSink, VortexSinkOptions},
    },
    sources::{arrow::ArrowDataSource, parquet::ParquetDataSource, vortex::VortexDataSource},
};

/// Builds the executable's set of available data formats.
pub fn format_registry() -> FormatRegistry {
    FormatRegistry::builder()
        .register(arrow_format())
        .register(parquet_format())
        .register(vortex_format())
        .build()
        .expect("built-in format registrations must not conflict")
}

/// Builds the executable's feature-selected storage backends.
pub fn storage_registry() -> StorageRegistry {
    let builder = StorageRegistry::builder();
    #[cfg(feature = "local")]
    let builder = builder.register(local::backend().expect("built-in local backend must be valid"));
    builder
        .build()
        .expect("built-in storage backends must not conflict")
}

pub(crate) struct CliDefinition {
    formats: FormatRegistry,
    storage: StorageRegistry,
}

impl CliDefinition {
    pub(crate) fn new() -> Self {
        Self {
            formats: format_registry(),
            storage: storage_registry(),
        }
    }

    pub(crate) fn command(&self, command: ClapCommand) -> ClapCommand {
        command.mut_subcommands(|command| match command.get_name() {
            "transform" => self.augment_transform_command(command),
            "inspect" => self.augment_inspect_command(command),
            "detect" => self.storage.augment_args(command),
            _ => command,
        })
    }

    fn augment_transform_command(&self, command: ClapCommand) -> ClapCommand {
        let possible_formats = self
            .formats
            .formats()
            .map(|format| format.name())
            .collect::<Vec<_>>();
        let command = command.mut_args(|argument| match argument.get_id().as_str() {
            "input_format" | "output_format" => {
                argument.value_parser(PossibleValuesParser::new(possible_formats.clone()))
            }
            _ => argument,
        });
        self.formats
            .augment_transform_args(self.storage.augment_args(command))
    }

    fn augment_inspect_command(&self, mut command: ClapCommand) -> ClapCommand {
        for format in self
            .formats
            .formats()
            .filter(|format| format.has_inspector())
        {
            let format_command = ClapCommand::new(format.name())
                .about(format!(
                    "Inspect {} file metadata and structure",
                    format.name()
                ))
                .visible_aliases(format.aliases().iter().copied());
            let format_command = InspectionArgs::augment_args(format_command);
            let format_command = format.augment_inspection_args(format_command);
            command = command.subcommand(self.storage.augment_args(format_command));
        }
        command
    }

    fn bind(self, matches: &clap::ArgMatches) -> Result<Cli, clap::Error> {
        let (name, matches) = matches.subcommand().ok_or_else(|| {
            clap::Error::raw(
                clap::error::ErrorKind::MissingSubcommand,
                "a command is required",
            )
        })?;
        let command = match name {
            "transform" => {
                let args = TransformArgs::from_arg_matches(matches)?;
                let formats = self.formats.bind_transform(matches)?;
                let storage = self.storage.create_session(matches).map_err(clap_error)?;
                RuntimeCommand::Transform(TransformCommand::from_parsed(args, formats, storage))
            }
            "detect" => {
                let args = DetectArgs::from_arg_matches(matches)?;
                let storage = self.storage.create_session(matches).map_err(clap_error)?;
                RuntimeCommand::Detect(DetectCommand::from_parsed(args, storage, self.formats))
            }
            "inspect" => {
                RuntimeCommand::Inspect(parse_inspect(matches, &self.formats, &self.storage)?)
            }
            "completions" => RuntimeCommand::Completions {
                shell: *matches
                    .get_one("shell")
                    .expect("Clap requires the completion shell"),
            },
            _ => unreachable!("Clap accepted an unknown command"),
        };
        Ok(Cli { command })
    }
}

pub(crate) fn try_parse_from<I, T>(arguments: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let definition = CliDefinition::new();
    let matches = definition
        .command(crate::CliSchema::command())
        .try_get_matches_from(arguments)?;
    definition.bind(&matches)
}

fn parse_inspect(
    matches: &clap::ArgMatches,
    formats: &FormatRegistry,
    storage_registry: &StorageRegistry,
) -> Result<InspectCommand, clap::Error> {
    let (name, matches) = matches.subcommand().ok_or_else(|| {
        clap::Error::raw(
            clap::error::ErrorKind::MissingSubcommand,
            "an inspect command is required",
        )
    })?;
    let storage = storage_registry
        .create_session(matches)
        .map_err(clap_error)?;
    let args = InspectionArgs::from_arg_matches(matches)?;
    let inspection = bind_inspection(formats, name, matches)?;
    Ok(InspectCommand::from_parsed(
        args.file,
        inspection_mode(args.format),
        inspection,
        storage,
    ))
}

fn inspection_mode(format: OutputFormat) -> InspectionMode {
    if format.resolves_to_json() {
        InspectionMode::Json
    } else {
        InspectionMode::Text
    }
}

fn bind_inspection(
    formats: &FormatRegistry,
    format: &str,
    matches: &clap::ArgMatches,
) -> Result<silk_chiffon_core::InspectionBinding, clap::Error> {
    formats
        .get(format)
        .expect("the CLI contains only registered formats")
        .bind_inspection(matches)
}

fn clap_error(error: impl std::fmt::Display) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::ValueValidation, error.to_string())
}

fn arrow_format() -> FormatDefinition {
    FormatDefinition::builder("arrow")
        .extensions(["arrow", "arrows"])
        .detector(detect_arrow)
        .detection_priority(1)
        .transform(
            TransformDefinition::with_args::<ArrowArgs>()
                .source(create_arrow_source)
                .sink(bind_arrow_sink)
                .build(),
        )
        .inspection(InspectionDefinition::with_args::<InspectArrowArgs>(
            inspect_arrow,
        ))
        .build()
}

fn parquet_format() -> FormatDefinition {
    FormatDefinition::builder("parquet")
        .extensions(["parquet"])
        .detector(detect_parquet)
        .detection_priority(0)
        .transform(
            TransformDefinition::with_args::<ParquetArgs>()
                .source(create_parquet_source)
                .sink(bind_parquet_sink)
                .build(),
        )
        .inspection(InspectionDefinition::with_args::<InspectParquetArgs>(
            inspect_parquet,
        ))
        .build()
}

fn vortex_format() -> FormatDefinition {
    FormatDefinition::builder("vortex")
        .extensions(["vortex"])
        .detector(detect_vortex)
        .detection_priority(2)
        .transform(
            TransformDefinition::with_args::<VortexArgs>()
                .source(create_vortex_source)
                .sink(bind_vortex_sink)
                .build(),
        )
        .inspection(InspectionDefinition::with_args::<InspectVortexArgs>(
            inspect_vortex,
        ))
        .build()
}

fn detect_arrow(handle: &StorageHandle) -> FormatFuture<'_, Option<FormatMatch>> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        Ok(ArrowInspector::detect_variant(&path)
            .ok()
            .map(|variant| FormatMatch::with_variant(variant.to_string())))
    })
}

fn detect_parquet(handle: &StorageHandle) -> FormatFuture<'_, Option<FormatMatch>> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        Ok(ParquetInspector::is_format(&path)?.then(FormatMatch::new))
    })
}

fn detect_vortex(handle: &StorageHandle) -> FormatFuture<'_, Option<FormatMatch>> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        Ok(VortexInspector::is_format(&path)?.then(|| FormatMatch::with_variant("file")))
    })
}

fn create_arrow_source<'a>(
    handle: &'a StorageHandle,
    session: &'a SessionContext,
    _args: &'a ArrowArgs,
) -> FormatFuture<'a, Box<dyn DataSource>> {
    Box::pin(async move {
        Ok(Box::new(ArrowDataSource::new(
            local_path_string(handle)?,
            session.clone(),
        )) as Box<dyn DataSource>)
    })
}

fn create_parquet_source<'a>(
    handle: &'a StorageHandle,
    session: &'a SessionContext,
    _args: &'a ParquetArgs,
) -> FormatFuture<'a, Box<dyn DataSource>> {
    Box::pin(async move {
        Ok(Box::new(ParquetDataSource::new(
            local_path_string(handle)?,
            session.clone(),
        )) as Box<dyn DataSource>)
    })
}

fn create_vortex_source<'a>(
    handle: &'a StorageHandle,
    session: &'a SessionContext,
    _args: &'a VortexArgs,
) -> FormatFuture<'a, Box<dyn DataSource>> {
    Box::pin(async move {
        Ok(Box::new(VortexDataSource::new(
            local_path_string(handle)?,
            session.clone(),
        )) as Box<dyn DataSource>)
    })
}

fn bind_arrow_sink<'a>(
    _context: &'a SinkBindingConfig,
    args: &'a ArrowArgs,
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    let options = ArrowSinkOptions::new()
        .with_compression(args.arrow_compression)
        .with_format(args.arrow_format)
        .with_record_batch_size(args.arrow_record_batch_size)
        .with_queue_depth(args.arrow_writing_queue_size);
    Box::pin(async move { Ok(Box::new(ArrowSinkBinding { options }) as Box<dyn SinkBinding>) })
}

fn bind_parquet_sink<'a>(
    context: &'a SinkBindingConfig,
    args: &'a ParquetArgs,
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    Box::pin(async move {
        let options = parquet_options(context, args)?;
        let default_encoding_threads = context.thread_budget().get();
        let runtimes = Arc::new(ParquetRuntimes::try_new(
            args.parquet_column_encoding_threads
                .unwrap_or(default_encoding_threads),
            args.parquet_io_threads.unwrap_or(1),
        )?);
        Ok(Box::new(ParquetSinkBinding { options, runtimes }) as Box<dyn SinkBinding>)
    })
}

fn bind_vortex_sink<'a>(
    _context: &'a SinkBindingConfig,
    args: &'a VortexArgs,
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    let options = VortexSinkOptions::new().apply_if_some(
        args.vortex_record_batch_size,
        VortexSinkOptions::with_record_batch_size,
    );
    Box::pin(async move { Ok(Box::new(VortexSinkBinding { options }) as Box<dyn SinkBinding>) })
}

fn parquet_options(context: &SinkBindingConfig, args: &ParquetArgs) -> Result<ParquetSinkOptions> {
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
            .or(Some(AllColumnsBloomFilterConfig {
                fpp: DEFAULT_BLOOM_FILTER_FPP,
                ndv: None,
            }))
    };
    let bloom_filter = BloomFilterConfig::try_new(
        all_enabled,
        args.parquet_bloom_column.clone(),
        args.parquet_bloom_column_off.clone(),
    )?;
    let sort_spec =
        (args.parquet_sorted_metadata && !context.output_ordering().is_empty()).then(|| SortSpec {
            columns: context
                .output_ordering()
                .iter()
                .map(output_sort_column)
                .collect(),
        });

    let options = ParquetSinkOptions::new()
        .with_parquet_compression(args.parquet_compression, args.parquet_compression_level)?
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
        .with_bloom_filters(bloom_filter)
        .with_offset_index_enabled(args.parquet_offset_index)
        .with_skip_arrow_metadata(!args.parquet_arrow_metadata)
        .with_page_header_statistics(args.parquet_page_header_statistics)
        .apply_if_some(
            args.parquet_buffer_size,
            ParquetSinkOptions::with_buffer_size,
        )
        .apply_if_some(
            args.parquet_row_group_size,
            ParquetSinkOptions::with_max_row_group_size,
        )
        .apply_if_some(
            args.parquet_row_group_concurrency,
            ParquetSinkOptions::with_max_row_group_concurrency,
        )
        .apply_if_some(
            args.parquet_data_page_size,
            ParquetSinkOptions::with_data_page_size_limit,
        )
        .apply_if_some(
            args.parquet_data_page_row_limit,
            ParquetSinkOptions::with_data_page_row_count_limit,
        )
        .apply_if_some(
            args.parquet_dictionary_page_size,
            ParquetSinkOptions::with_dictionary_page_size_limit,
        )
        .apply_if_some(
            args.parquet_write_batch_size,
            ParquetSinkOptions::with_write_batch_size,
        )
        .apply_if_some(sort_spec, ParquetSinkOptions::with_sort_spec);
    Ok(options)
}

fn output_sort_column(column: &OutputOrderingColumn) -> SortColumn {
    SortColumn {
        name: column.name().to_owned(),
        direction: match column.direction() {
            silk_chiffon_core::SortDirection::Ascending => crate::SortDirection::Ascending,
            silk_chiffon_core::SortDirection::Descending => crate::SortDirection::Descending,
        },
    }
}

struct ArrowSinkBinding {
    options: ArrowSinkOptions,
}

#[async_trait]
impl SinkBinding for ArrowSinkBinding {
    async fn open_sink(
        &self,
        handle: StorageHandle,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(ArrowSink::create(
            handle.local_path()?,
            &schema,
            self.options.clone(),
        )?))
    }
}

struct ParquetSinkBinding {
    options: ParquetSinkOptions,
    runtimes: Arc<ParquetRuntimes>,
}

#[async_trait]
impl SinkBinding for ParquetSinkBinding {
    async fn open_sink(
        &self,
        handle: StorageHandle,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(ParquetSink::create(
            handle.local_path()?,
            &schema,
            &self.options,
            Arc::clone(&self.runtimes),
        )?))
    }
}

struct VortexSinkBinding {
    options: VortexSinkOptions,
}

#[async_trait]
impl SinkBinding for VortexSinkBinding {
    async fn open_sink(
        &self,
        handle: StorageHandle,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(VortexSink::create(
            handle.local_path()?,
            &schema,
            self.options,
        )?))
    }
}

fn inspect_arrow<'a>(
    handle: &'a StorageHandle,
    mode: InspectionMode,
    args: &'a InspectArrowArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        let inspector = ArrowInspector::open(&path, args.row_count || args.batches)
            .context("Failed to open Arrow file")?;
        if mode == InspectionMode::Json {
            return Ok(InspectionOutput::Json(inspector.to_json()));
        }
        let mut output = Vec::new();
        inspector.render_default(&mut output)?;
        if args.batches {
            inspector.render_batches(&mut output)?;
        }
        Ok(InspectionOutput::Text(String::from_utf8(output)?))
    })
}

fn inspect_parquet<'a>(
    handle: &'a StorageHandle,
    mode: InspectionMode,
    args: &'a InspectParquetArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        let inspector = ParquetInspector::open(&path).context("Failed to open Parquet file")?;
        let columns = args.pages.as_ref().and_then(|columns| {
            (!columns.is_empty()).then(|| columns.split(',').map(str::trim).collect::<Vec<_>>())
        });
        if mode == InspectionMode::Json {
            let value = if args.pages.is_some() {
                inspector.to_json_with_pages(columns.as_deref())
            } else {
                inspector.to_json()
            };
            return Ok(InspectionOutput::Json(value));
        }
        let mut output = Vec::new();
        inspector.render_with_row_group(&mut output, args.row_group)?;
        if args.pages.is_some() {
            inspector.render_pages(&mut output, args.row_group, columns.as_deref())?;
        }
        Ok(InspectionOutput::Text(String::from_utf8(output)?))
    })
}

fn inspect_vortex<'a>(
    handle: &'a StorageHandle,
    mode: InspectionMode,
    args: &'a InspectVortexArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        let inspector = VortexInspector::open_file(&path).context("Failed to open Vortex file")?;
        if mode == InspectionMode::Json {
            return Ok(InspectionOutput::Json(inspector.to_json()));
        }
        let mut output = Vec::new();
        inspector.render_default(&mut output)?;
        if args.schema {
            inspector.render_schema(&mut output)?;
        }
        if args.stats {
            inspector.render_stats(&mut output)?;
        }
        if args.layout {
            inspector.render_layout(&mut output)?;
        }
        Ok(InspectionOutput::Text(String::from_utf8(output)?))
    })
}

fn local_utf8_path(handle: &StorageHandle) -> Result<Utf8PathBuf> {
    Utf8PathBuf::from_path_buf(handle.local_path()?)
        .map_err(|path| anyhow!("Local path is not valid UTF-8: {}", path.display()))
}

fn local_path_string(handle: &StorageHandle) -> Result<String> {
    Ok(local_utf8_path(handle)?.into_string())
}

#[cfg(all(test, feature = "local-bare-paths"))]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use arrow::{array::RecordBatch, datatypes::SchemaRef};
    use async_trait::async_trait;
    use clap::CommandFactory;
    use datafusion::{
        catalog::{TableProvider, streaming::StreamingTable},
        error::DataFusionError,
        execution::TaskContext,
        physical_plan::{
            SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
        },
        prelude::SessionContext,
    };
    use futures::StreamExt;
    use silk_chiffon_core::{
        DataSource, FormatDefinition, FormatFuture, FormatRegistry, Replayability, SinkBinding,
        TransformDefinition,
    };

    use super::{CliDefinition, arrow_format, storage_registry};
    use crate::{
        CliSchema, Command,
        utils::test_data::{TestBatch, TestExtract, TestFile},
    };
    use silk_chiffon_storage::StorageHandle;

    static STREAM_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);
    static SINK_BINDINGS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct SinglePassPartition {
        batch: RecordBatch,
    }

    impl PartitionStream for SinglePassPartition {
        fn schema(&self) -> &SchemaRef {
            self.batch.schema_ref()
        }

        fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
            let stream = if STREAM_EXECUTIONS.fetch_add(1, Ordering::SeqCst) == 0 {
                futures::stream::iter([Ok(self.batch.clone())])
            } else {
                futures::stream::iter([Err(DataFusionError::Execution(
                    "single-pass source was consumed more than once".to_owned(),
                ))])
            };
            Box::pin(RecordBatchStreamAdapter::new(self.batch.schema(), stream))
        }
    }

    struct SinglePassSource;

    #[async_trait]
    impl DataSource for SinglePassSource {
        fn name(&self) -> &str {
            "single-pass-test"
        }

        fn replayability(&self) -> Replayability {
            Replayability::SinglePass
        }

        async fn schema(&self) -> Result<SchemaRef> {
            Ok(TestBatch::simple_schema())
        }

        async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
            let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
            let partition: Arc<dyn PartitionStream> = Arc::new(SinglePassPartition { batch });
            let table = StreamingTable::try_new(TestBatch::simple_schema(), vec![partition])?;
            Ok(Arc::new(table))
        }
    }

    fn single_pass_source<'a>(
        _: &'a StorageHandle,
        _: &'a SessionContext,
        _: &'a (),
    ) -> FormatFuture<'a, Box<dyn DataSource>> {
        Box::pin(async { Ok(Box::new(SinglePassSource) as Box<dyn DataSource>) })
    }

    fn single_pass_format() -> FormatDefinition {
        FormatDefinition::builder("single-pass-test")
            .extensions(["single-pass-test"])
            .transform(
                TransformDefinition::without_args()
                    .source(single_pass_source)
                    .build(),
            )
            .build()
    }

    #[derive(Debug)]
    struct InfinitePartition {
        batch: RecordBatch,
    }

    impl PartitionStream for InfinitePartition {
        fn schema(&self) -> &SchemaRef {
            self.batch.schema_ref()
        }

        fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
            let stream =
                futures::stream::iter([Ok(self.batch.clone())]).chain(futures::stream::pending::<
                    Result<RecordBatch, DataFusionError>,
                >());
            Box::pin(RecordBatchStreamAdapter::new(self.batch.schema(), stream))
        }
    }

    struct InfiniteSource;

    #[async_trait]
    impl DataSource for InfiniteSource {
        fn name(&self) -> &str {
            "infinite-test"
        }

        fn replayability(&self) -> Replayability {
            Replayability::SinglePass
        }

        async fn schema(&self) -> Result<SchemaRef> {
            Ok(TestBatch::simple_schema())
        }

        async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
            let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
            let partition: Arc<dyn PartitionStream> = Arc::new(InfinitePartition { batch });
            let table = StreamingTable::try_new(TestBatch::simple_schema(), vec![partition])?
                .with_infinite_table(true);
            Ok(Arc::new(table))
        }
    }

    fn infinite_source<'a>(
        _: &'a StorageHandle,
        _: &'a SessionContext,
        _: &'a (),
    ) -> FormatFuture<'a, Box<dyn DataSource>> {
        Box::pin(async { Ok(Box::new(InfiniteSource) as Box<dyn DataSource>) })
    }

    fn infinite_format() -> FormatDefinition {
        FormatDefinition::builder("infinite-test")
            .extensions(["infinite-test"])
            .transform(
                TransformDefinition::without_args()
                    .source(infinite_source)
                    .build(),
            )
            .build()
    }

    fn count_sink_binding<'a>(
        _: &'a silk_chiffon_core::SinkBindingConfig,
        _: &'a (),
    ) -> FormatFuture<'a, Box<dyn SinkBinding>> {
        SINK_BINDINGS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(Box::new(super::ArrowSinkBinding {
                options: crate::sinks::arrow::ArrowSinkOptions::new(),
            }) as Box<dyn SinkBinding>)
        })
    }

    fn counted_sink_format() -> FormatDefinition {
        FormatDefinition::builder("counted-sink-test")
            .extensions(["counted-sink-test"])
            .transform(
                TransformDefinition::without_args()
                    .sink(count_sink_binding)
                    .build(),
            )
            .build()
    }

    fn test_cli(definition: CliDefinition, arguments: &[&str]) -> crate::Cli {
        let matches = definition
            .command(CliSchema::command())
            .try_get_matches_from(arguments)
            .unwrap();
        definition.bind(&matches).unwrap()
    }

    #[tokio::test]
    async fn sort_skips_row_size_measurement_for_single_pass_source() {
        STREAM_EXECUTIONS.store(0, Ordering::SeqCst);
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.single-pass-test");
        let output = directory.path().join("output.arrow");
        std::fs::write(&input, b"test source input").unwrap();

        let formats = FormatRegistry::builder()
            .register(single_pass_format())
            .register(arrow_format())
            .build()
            .unwrap();
        let definition = CliDefinition {
            formats,
            storage: storage_registry(),
        };
        let matches = definition
            .command(CliSchema::command())
            .try_get_matches_from([
                "silk-chiffon",
                "transform",
                "--from",
                input.to_str().unwrap(),
                "--input-format",
                "single-pass-test",
                "--to",
                output.to_str().unwrap(),
                "--output-format",
                "arrow",
                "--sort-by",
                "id",
            ])
            .unwrap();
        let cli = definition.bind(&matches).unwrap();
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let batches = TestFile::read_arrow(&output);
        assert_eq!(TestExtract::i32_all(&batches, "id"), [1, 2, 3]);
        assert_eq!(STREAM_EXECUTIONS.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unbounded_plan_is_rejected_before_sink_binding() {
        SINK_BINDINGS.store(0, Ordering::SeqCst);
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.infinite-test");
        let output = directory.path().join("output.counted-sink-test");
        std::fs::write(&input, b"test source input").unwrap();

        let definition = CliDefinition {
            formats: FormatRegistry::builder()
                .register(infinite_format())
                .register(counted_sink_format())
                .build()
                .unwrap(),
            storage: storage_registry(),
        };
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from",
                input.to_str().unwrap(),
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();
        assert!(error.to_string().contains("require a bounded input plan"));
        assert_eq!(SINK_BINDINGS.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bounded_query_can_replace_an_unbounded_source_plan() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.infinite-test");
        let output = directory.path().join("output.arrow");
        std::fs::write(&input, b"test source input").unwrap();

        let definition = CliDefinition {
            formats: FormatRegistry::builder()
                .register(infinite_format())
                .register(arrow_format())
                .build()
                .unwrap(),
            storage: storage_registry(),
        };
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from",
                input.to_str().unwrap(),
                "--to",
                output.to_str().unwrap(),
                "--query",
                "SELECT CAST(1 AS INT) AS id, 'a' AS name",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let batches = TestFile::read_arrow(&output);
        assert_eq!(TestExtract::i32_all(&batches, "id"), [1]);
    }
}
