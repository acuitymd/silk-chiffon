use std::{ffi::OsString, sync::Arc};

use anyhow::{Context, Result, anyhow};
use apply_if::ApplyIf;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use camino::Utf8PathBuf;
use clap::{Command, CommandFactory, FromArgMatches, builder::PossibleValuesParser};
use silk_chiffon_core::{
    DataSink, DataSinkFactory, DataSource, FormatFuture, FormatInspection, FormatRegistration,
    FormatRegistry, FormatTransform, Identification, InspectionOutput, OutputSortColumn,
    SinkFactoryContext,
};
use silk_chiffon_storage::{StorageHandle, StorageRegistry};

#[cfg(feature = "local")]
use silk_chiffon_storage::local;

use crate::{
    AllColumnsBloomFilterConfig, ArrowArgs, BloomFilterConfig, Cli, Commands,
    DEFAULT_BLOOM_FILTER_FPP, InspectArrowArgs, InspectCommand, InspectIdentifyArgs,
    InspectParquetArgs, InspectSubcommand, InspectVortexArgs, ParquetArgs, SortColumn, SortSpec,
    TransformBaseArgs, TransformCommand, VortexArgs,
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

pub fn format_registry() -> FormatRegistry {
    FormatRegistry::builder()
        .register(arrow_registration())
        .register(parquet_registration())
        .register(vortex_registration())
        .build()
        .expect("built-in format registrations must not conflict")
}

pub fn storage_registry() -> StorageRegistry {
    let builder = StorageRegistry::builder();
    #[cfg(feature = "local")]
    let builder = builder.register(local::backend().expect("built-in local backend must be valid"));
    builder
        .build()
        .expect("built-in storage backends must not conflict")
}

struct ExecutableRegistries {
    formats: FormatRegistry,
    storage: StorageRegistry,
}

impl ExecutableRegistries {
    fn new() -> Self {
        Self {
            formats: format_registry(),
            storage: storage_registry(),
        }
    }

    fn assembled_command(&self, command: Command) -> Command {
        command.mut_subcommands(|command| match command.get_name() {
            "transform" => self.assemble_transform_command(command),
            "inspect" => self.assemble_inspect_command(command),
            _ => command,
        })
    }

    fn assemble_transform_command(&self, command: Command) -> Command {
        let possible_formats = self
            .formats
            .registrations()
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

    fn assemble_inspect_command(&self, command: Command) -> Command {
        command.mut_subcommands(|command| {
            let Some(format) = self.formats.get(command.get_name()) else {
                return self.storage.augment_args(command);
            };
            format.augment_inspection_args(self.storage.augment_args(command))
        })
    }

    fn runtime_cli(self, matches: &clap::ArgMatches) -> Result<Cli, clap::Error> {
        let (name, matches) = matches.subcommand().ok_or_else(|| {
            clap::Error::raw(
                clap::error::ErrorKind::MissingSubcommand,
                "a command is required",
            )
        })?;
        let command = match name {
            "transform" => {
                let args = TransformBaseArgs::from_arg_matches(matches)?;
                let formats = self.formats.bind_transform_args(matches)?;
                let storage = self.storage.create_session(matches).map_err(clap_error)?;
                Commands::Transform(TransformCommand::from_parsed(args, formats, storage))
            }
            "inspect" => Commands::Inspect(parse_inspect(matches, self.formats, &self.storage)?),
            "completions" => Commands::Completions {
                shell: *matches
                    .get_one("shell")
                    .expect("Clap requires the completion shell"),
            },
            _ => unreachable!("Clap accepted an unknown command"),
        };
        Ok(Cli { command })
    }
}

pub(crate) fn assembled_command(command: Command) -> Command {
    ExecutableRegistries::new().assembled_command(command)
}

pub(crate) fn try_parse_from<I, T>(arguments: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let registries = ExecutableRegistries::new();
    let matches = registries
        .assembled_command(crate::CliSchema::command())
        .try_get_matches_from(arguments)?;
    registries.runtime_cli(&matches)
}

pub(crate) fn default_transform_command() -> Result<TransformCommand, clap::Error> {
    let registries = ExecutableRegistries::new();
    let matches = registries
        .assembled_command(crate::CliSchema::command())
        .try_get_matches_from([
            "silk-chiffon",
            "transform",
            "--from",
            "input.arrow",
            "--to",
            "output.arrow",
        ])?;
    let Cli {
        command: Commands::Transform(mut command),
    } = registries.runtime_cli(&matches)?
    else {
        unreachable!("the default command is a transform")
    };
    command.from = None;
    command.to = None;
    Ok(command)
}

fn parse_inspect(
    matches: &clap::ArgMatches,
    formats: FormatRegistry,
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
    let (command, inspection) = match name {
        "identify" => (
            InspectSubcommand::Identify(InspectIdentifyArgs::from_arg_matches(matches)?),
            None,
        ),
        "parquet" => (
            InspectSubcommand::Parquet(InspectParquetArgs::from_arg_matches(matches)?),
            Some(bind_inspection(&formats, "parquet", matches)?),
        ),
        "arrow" => (
            InspectSubcommand::Arrow(InspectArrowArgs::from_arg_matches(matches)?),
            Some(bind_inspection(&formats, "arrow", matches)?),
        ),
        "vortex" => (
            InspectSubcommand::Vortex(InspectVortexArgs::from_arg_matches(matches)?),
            Some(bind_inspection(&formats, "vortex", matches)?),
        ),
        _ => unreachable!("Clap accepted an unknown inspect command"),
    };
    Ok(InspectCommand::from_parsed(
        command, inspection, storage, formats,
    ))
}

fn bind_inspection(
    formats: &FormatRegistry,
    format: &str,
    matches: &clap::ArgMatches,
) -> Result<silk_chiffon_core::ConfiguredInspection, clap::Error> {
    formats
        .get(format)
        .expect("the CLI contains only registered formats")
        .bind_inspection_args(matches)
}

fn clap_error(error: impl std::fmt::Display) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::ValueValidation, error.to_string())
}

fn arrow_registration() -> FormatRegistration {
    FormatRegistration::builder("arrow")
        .extensions(["arrow", "arrows"])
        .identifier(identify_arrow)
        .identifier_priority(1)
        .transform(
            FormatTransform::with_args::<ArrowArgs>()
                .source(arrow_source)
                .sink(arrow_sink_factory)
                .build(),
        )
        .inspection(FormatInspection::with_args::<InspectArrowArgs>(
            inspect_arrow,
        ))
        .build()
}

fn parquet_registration() -> FormatRegistration {
    FormatRegistration::builder("parquet")
        .extensions(["parquet"])
        .identifier(identify_parquet)
        .identifier_priority(0)
        .transform(
            FormatTransform::with_args::<ParquetArgs>()
                .source(parquet_source)
                .sink(parquet_sink_factory)
                .build(),
        )
        .inspection(FormatInspection::with_args::<InspectParquetArgs>(
            inspect_parquet,
        ))
        .build()
}

fn vortex_registration() -> FormatRegistration {
    FormatRegistration::builder("vortex")
        .extensions(["vortex"])
        .identifier(identify_vortex)
        .identifier_priority(2)
        .transform(
            FormatTransform::with_args::<VortexArgs>()
                .source(vortex_source)
                .sink(vortex_sink_factory)
                .build(),
        )
        .inspection(FormatInspection::with_args::<InspectVortexArgs>(
            inspect_vortex,
        ))
        .build()
}

fn identify_arrow(handle: &StorageHandle) -> FormatFuture<'_, Option<Identification>> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        Ok(ArrowInspector::detect_variant(&path)
            .ok()
            .map(|variant| Identification::with_variant(variant.to_string())))
    })
}

fn identify_parquet(handle: &StorageHandle) -> FormatFuture<'_, Option<Identification>> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        Ok(ParquetInspector::is_format(&path)?.then(Identification::new))
    })
}

fn identify_vortex(handle: &StorageHandle) -> FormatFuture<'_, Option<Identification>> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        Ok(VortexInspector::is_format(&path)?.then(|| Identification::with_variant("file")))
    })
}

fn arrow_source<'a>(
    handle: &'a StorageHandle,
    _args: &'a ArrowArgs,
) -> FormatFuture<'a, Box<dyn DataSource>> {
    Box::pin(async move {
        Ok(Box::new(ArrowDataSource::new(local_path_string(handle)?)) as Box<dyn DataSource>)
    })
}

fn parquet_source<'a>(
    handle: &'a StorageHandle,
    _args: &'a ParquetArgs,
) -> FormatFuture<'a, Box<dyn DataSource>> {
    Box::pin(async move {
        Ok(Box::new(ParquetDataSource::new(local_path_string(handle)?)) as Box<dyn DataSource>)
    })
}

fn vortex_source<'a>(
    handle: &'a StorageHandle,
    _args: &'a VortexArgs,
) -> FormatFuture<'a, Box<dyn DataSource>> {
    Box::pin(async move {
        Ok(Box::new(VortexDataSource::new(local_path_string(handle)?)) as Box<dyn DataSource>)
    })
}

fn arrow_sink_factory<'a>(
    _context: &'a SinkFactoryContext,
    args: &'a ArrowArgs,
) -> FormatFuture<'a, Box<dyn DataSinkFactory>> {
    let options = ArrowSinkOptions::new()
        .with_compression(args.arrow_compression)
        .with_format(args.arrow_format)
        .with_record_batch_size(args.arrow_record_batch_size)
        .with_queue_depth(args.arrow_writing_queue_size);
    Box::pin(async move { Ok(Box::new(ArrowFactory { options }) as Box<dyn DataSinkFactory>) })
}

fn parquet_sink_factory<'a>(
    context: &'a SinkFactoryContext,
    args: &'a ParquetArgs,
) -> FormatFuture<'a, Box<dyn DataSinkFactory>> {
    Box::pin(async move {
        let options = parquet_options(context, args)?;
        let thread_budget = context.thread_budget().get();
        let default_encoding_threads = if context.pipeline_sorts() {
            (thread_budget / 4).max(1)
        } else {
            (thread_budget * 3 / 4).max(1)
        };
        let runtimes = Arc::new(ParquetRuntimes::try_new(
            args.parquet_column_encoding_threads
                .unwrap_or(default_encoding_threads),
            args.parquet_io_threads.unwrap_or(1),
        )?);
        Ok(Box::new(ParquetFactory { options, runtimes }) as Box<dyn DataSinkFactory>)
    })
}

fn vortex_sink_factory<'a>(
    _context: &'a SinkFactoryContext,
    args: &'a VortexArgs,
) -> FormatFuture<'a, Box<dyn DataSinkFactory>> {
    let options = VortexSinkOptions::new().apply_if_some(
        args.vortex_record_batch_size,
        VortexSinkOptions::with_record_batch_size,
    );
    Box::pin(async move { Ok(Box::new(VortexFactory { options }) as Box<dyn DataSinkFactory>) })
}

fn parquet_options(context: &SinkFactoryContext, args: &ParquetArgs) -> Result<ParquetSinkOptions> {
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

fn output_sort_column(column: &OutputSortColumn) -> SortColumn {
    SortColumn {
        name: column.name().to_owned(),
        direction: match column.direction() {
            silk_chiffon_core::SortDirection::Ascending => crate::SortDirection::Ascending,
            silk_chiffon_core::SortDirection::Descending => crate::SortDirection::Descending,
        },
    }
}

struct ArrowFactory {
    options: ArrowSinkOptions,
}

#[async_trait]
impl DataSinkFactory for ArrowFactory {
    async fn create(&self, handle: StorageHandle, schema: SchemaRef) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(ArrowSink::create(
            handle.local_path()?,
            &schema,
            self.options.clone(),
        )?))
    }
}

struct ParquetFactory {
    options: ParquetSinkOptions,
    runtimes: Arc<ParquetRuntimes>,
}

#[async_trait]
impl DataSinkFactory for ParquetFactory {
    async fn create(&self, handle: StorageHandle, schema: SchemaRef) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(ParquetSink::create(
            handle.local_path()?,
            &schema,
            &self.options,
            Arc::clone(&self.runtimes),
        )?))
    }
}

struct VortexFactory {
    options: VortexSinkOptions,
}

#[async_trait]
impl DataSinkFactory for VortexFactory {
    async fn create(&self, handle: StorageHandle, schema: SchemaRef) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(VortexSink::create(
            handle.local_path()?,
            &schema,
            self.options,
        )?))
    }
}

fn inspect_arrow<'a>(
    handle: &'a StorageHandle,
    args: &'a InspectArrowArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        let inspector = ArrowInspector::open(&path, args.row_count || args.batches)
            .context("Failed to open Arrow file")?;
        if args.format.resolves_to_json() {
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
    args: &'a InspectParquetArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        let inspector = ParquetInspector::open(&path).context("Failed to open Parquet file")?;
        let columns = args.pages.as_ref().and_then(|columns| {
            (!columns.is_empty()).then(|| columns.split(',').map(str::trim).collect::<Vec<_>>())
        });
        if args.format.resolves_to_json() {
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
    args: &'a InspectVortexArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let path = local_utf8_path(handle)?;
        let inspector = VortexInspector::open_file(&path).context("Failed to open Vortex file")?;
        if args.format.resolves_to_json() {
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
    use silk_chiffon_core::{
        DataSource, FormatFuture, FormatRegistration, FormatRegistry, FormatTransform,
        Replayability,
    };

    use super::{ExecutableRegistries, arrow_registration, storage_registry};
    use crate::{
        CliSchema, Commands,
        utils::test_data::{TestBatch, TestExtract, TestFile},
    };
    use silk_chiffon_storage::StorageHandle;

    static STREAM_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

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

        async fn as_table_provider(
            &self,
            _: &mut SessionContext,
        ) -> Result<Arc<dyn TableProvider>> {
            let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
            let partition: Arc<dyn PartitionStream> = Arc::new(SinglePassPartition { batch });
            let table = StreamingTable::try_new(TestBatch::simple_schema(), vec![partition])?;
            Ok(Arc::new(table))
        }
    }

    fn single_pass_source<'a>(
        _: &'a StorageHandle,
        _: &'a (),
    ) -> FormatFuture<'a, Box<dyn DataSource>> {
        Box::pin(async { Ok(Box::new(SinglePassSource) as Box<dyn DataSource>) })
    }

    fn single_pass_registration() -> FormatRegistration {
        FormatRegistration::builder("single-pass-test")
            .extensions(["single-pass-test"])
            .transform(
                FormatTransform::without_args()
                    .source(single_pass_source)
                    .build(),
            )
            .build()
    }

    #[tokio::test]
    async fn sort_skips_row_size_measurement_for_single_pass_source() {
        STREAM_EXECUTIONS.store(0, Ordering::SeqCst);
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.single-pass-test");
        let output = directory.path().join("output.arrow");
        std::fs::write(&input, b"test source input").unwrap();

        let formats = FormatRegistry::builder()
            .register(single_pass_registration())
            .register(arrow_registration())
            .build()
            .unwrap();
        let registries = ExecutableRegistries {
            formats,
            storage: storage_registry(),
        };
        let matches = registries
            .assembled_command(CliSchema::command())
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
        let cli = registries.runtime_cli(&matches).unwrap();
        let Commands::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let batches = TestFile::read_arrow(&output);
        assert_eq!(TestExtract::i32_all(&batches, "id"), [1, 2, 3]);
        assert_eq!(STREAM_EXECUTIONS.load(Ordering::SeqCst), 1);
    }
}
