use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result;
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use clap::{Args, Command};
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    datasource::empty::EmptyTable,
    error::DataFusionError,
    execution::TaskContext,
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
    prelude::SessionContext,
};
use futures::StreamExt;
use silk_chiffon_core::{
    DataSink, DataSinkFactory, DataSource, DataSourceCapabilities, FormatCapability, FormatFuture,
    FormatInspection, FormatRegistration, FormatRegistry, FormatRegistryError, FormatRuntimeError,
    FormatTransform, Identification, InputAccess, InspectionOutput, OutputSortColumn, RowCount,
    SinkFactoryContext, SinkResult, SortDirection, StreamBoundedness,
};
use silk_chiffon_storage::{Location, ResolvedLocation, StorageResolver};

#[derive(Args, Clone, Debug, Eq, PartialEq)]
struct TestFormatArgs {
    /// Selects a test-format worker count.
    ///
    /// This value configures the registered source and sink factories.
    #[arg(long, default_value_t = 4)]
    test_format_workers: usize,
}

#[derive(Args, Clone, Debug, Eq, PartialEq)]
struct TestInspectionArgs {
    /// Includes details in test-format inspection output.
    #[arg(long)]
    test_format_details: bool,
}

#[derive(Args, Clone, Debug)]
struct OtherFormatArgs {
    #[arg(long)]
    other_format_option: bool,
}

#[derive(Args, Clone, Debug)]
struct ConflictingIdArgs {
    #[arg(long)]
    test_format_workers: Option<usize>,
}

#[derive(Args, Clone, Debug)]
struct FirstLongArgs {
    #[arg(long = "shared-long")]
    first_long: bool,
}

#[derive(Args, Clone, Debug)]
struct SecondLongArgs {
    #[arg(long = "shared-long")]
    second_long: bool,
}

#[derive(Args, Clone, Debug)]
struct FirstShortArgs {
    #[arg(long = "first-short", short = 'z')]
    first_short: bool,
}

#[derive(Args, Clone, Debug)]
struct SecondShortArgs {
    #[arg(long = "second-short", short = 'z')]
    second_short: bool,
}

struct TestSource {
    name: String,
    schema: SchemaRef,
}

#[async_trait]
impl DataSource for TestSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> DataSourceCapabilities {
        DataSourceCapabilities::new(StreamBoundedness::Finite, InputAccess::RandomAccess)
    }

    async fn schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.schema))
    }

    async fn row_count(&self) -> Result<RowCount> {
        Ok(RowCount::Exact(0))
    }

    async fn as_table_provider(&self, _: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(EmptyTable::new(Arc::clone(&self.schema))))
    }
}

#[derive(Debug)]
struct TestPartitionStream {
    schema: SchemaRef,
    boundedness: StreamBoundedness,
}

impl PartitionStream for TestPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        match self.boundedness {
            StreamBoundedness::Finite => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::empty::<Result<RecordBatch, DataFusionError>>(),
            )),
            StreamBoundedness::Infinite => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::pending::<Result<RecordBatch, DataFusionError>>(),
            )),
        }
    }
}

struct DirectStreamSource {
    schema: SchemaRef,
    boundedness: StreamBoundedness,
}

#[async_trait]
impl DataSource for DirectStreamSource {
    fn name(&self) -> &str {
        "direct-stream"
    }

    fn capabilities(&self) -> DataSourceCapabilities {
        DataSourceCapabilities::new(self.boundedness, InputAccess::Sequential)
    }

    async fn schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.schema))
    }

    async fn as_table_provider(&self, _: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        let partition = Arc::new(TestPartitionStream {
            schema: Arc::clone(&self.schema),
            boundedness: self.boundedness,
        });
        let table = StreamingTable::try_new(Arc::clone(&self.schema), vec![partition])?
            .with_infinite_table(self.boundedness == StreamBoundedness::Infinite);
        Ok(Arc::new(table))
    }
}

struct TestSink {
    output: url::Url,
    created: Arc<AtomicUsize>,
    workers: usize,
    thread_budget: NonZeroUsize,
    sorting: bool,
    output_ordering: Arc<[OutputSortColumn]>,
}

struct TestSinkFactory {
    created: Arc<AtomicUsize>,
    workers: usize,
    thread_budget: NonZeroUsize,
    sorting: bool,
    output_ordering: Arc<[OutputSortColumn]>,
}

#[async_trait]
impl DataSinkFactory for TestSinkFactory {
    async fn create(&self, location: ResolvedLocation, _: SchemaRef) -> Result<Box<dyn DataSink>> {
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestSink {
            output: location.url,
            created: Arc::clone(&self.created),
            workers: self.workers,
            thread_budget: self.thread_budget,
            sorting: self.sorting,
            output_ordering: Arc::clone(&self.output_ordering),
        }))
    }
}

#[async_trait]
impl DataSink for TestSink {
    async fn write_batch(&mut self, _: RecordBatch) -> Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        let ordering_score = self
            .output_ordering
            .iter()
            .map(|column| {
                column.name().len()
                    + match column.direction() {
                        SortDirection::Ascending => 1,
                        SortDirection::Descending => 2,
                    }
            })
            .sum::<usize>();
        Ok(SinkResult {
            files_written: vec![self.output.clone()],
            rows_written: (self.workers
                + self.thread_budget.get()
                + usize::from(self.sorting)
                + self.created.load(Ordering::SeqCst)
                + ordering_score) as u64,
        })
    }
}

fn identifier(location: &ResolvedLocation) -> FormatFuture<'_, Option<Identification>> {
    Box::pin(async move {
        Ok((location.path.extension() == Some("test"))
            .then(|| Identification::with_variant("test-stream")))
    })
}

fn source<'a>(
    location: &'a ResolvedLocation,
    settings: &'a TestFormatArgs,
) -> FormatFuture<'a, Box<dyn DataSource>> {
    Box::pin(async move {
        Ok(Box::new(TestSource {
            name: format!("{}:{}", location.url, settings.test_format_workers),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
        }) as Box<dyn DataSource>)
    })
}

fn sink<'a>(
    context: &'a SinkFactoryContext,
    settings: &'a TestFormatArgs,
) -> FormatFuture<'a, Box<dyn DataSinkFactory>> {
    Box::pin(async move {
        Ok(Box::new(TestSinkFactory {
            created: Arc::new(AtomicUsize::new(0)),
            workers: settings.test_format_workers,
            thread_budget: context.thread_budget(),
            sorting: context.pipeline_sorts(),
            output_ordering: Arc::from(context.output_ordering()),
        }) as Box<dyn DataSinkFactory>)
    })
}

fn inspector<'a>(
    location: &'a ResolvedLocation,
    settings: &'a TestInspectionArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        Ok(InspectionOutput::Text(format!(
            "{} details={}",
            location.url, settings.test_format_details
        )))
    })
}

fn registration(name: &'static str) -> FormatRegistration {
    let transform = FormatTransform::with_args::<TestFormatArgs>()
        .source(source)
        .sink(sink)
        .build();
    let inspection = FormatInspection::with_args::<TestInspectionArgs>(inspector);

    FormatRegistration::builder(name)
        .aliases(["t"])
        .extensions(["test"])
        .identifier(identifier)
        .identifier_priority(7)
        .transform(transform)
        .inspection(inspection)
        .build()
}

fn parse_transform(registry: &FormatRegistry, arguments: &[&str]) -> usize {
    let command = registry.augment_transform_args(Command::new("test"));
    let matches = command.try_get_matches_from(arguments).unwrap();
    let settings = registry.parse_transform_cli(&matches).unwrap();
    let registration = registry.get("test").unwrap();
    let location = resolved_location("input.test");
    let source =
        futures::executor::block_on(registration.create_source(&location, &settings)).unwrap();
    source.name().rsplit_once(':').unwrap().1.parse().unwrap()
}

fn resolved_location(path: &str) -> ResolvedLocation {
    let location = Location::parse(path, std::env::current_dir().unwrap()).unwrap();
    StorageResolver::new().resolve(&location).unwrap()
}

#[test]
fn registration_keeps_capabilities_independently_optional() {
    let empty = FormatRegistration::builder("empty").build();
    assert!(!empty.has_identifier());
    assert!(!empty.has_source());
    assert!(!empty.has_sink());
    assert!(!empty.has_inspector());

    let source_only = FormatRegistration::builder("source-only")
        .transform(
            FormatTransform::with_args::<TestFormatArgs>()
                .source(source)
                .build(),
        )
        .build();
    assert!(!source_only.has_identifier());
    assert!(source_only.has_source());
    assert!(!source_only.has_sink());
    assert!(!source_only.has_inspector());
}

#[test]
fn registered_format_contributes_help_and_parses_ordinary_clap_args() {
    let registry = FormatRegistry::builder()
        .register(registration("test"))
        .build()
        .unwrap();

    let help = registry
        .augment_transform_args(Command::new("test"))
        .render_long_help()
        .to_string();
    assert!(help.contains("--test-format-workers"));
    assert!(help.contains("Selects a test-format worker count."));
    assert!(help.contains("This value configures the registered source and sink factories."));

    assert_eq!(
        parse_transform(&registry, &["test", "--test-format-workers", "9"]),
        9
    );
}

#[test]
fn inspection_args_are_scoped_to_the_typed_inspector_callback() {
    let registration = registration("test");
    let command = registration.augment_inspection_args(Command::new("inspect-test"));
    let matches = command
        .try_get_matches_from(["inspect-test", "--test-format-details"])
        .unwrap();
    let settings = registration.parse_inspection_cli(&matches).unwrap();
    let location = resolved_location("input.test");

    let output = futures::executor::block_on(registration.inspect(&location, &settings)).unwrap();

    assert_eq!(
        output,
        InspectionOutput::Text(format!("{} details=true", location.url))
    );
}

#[test]
fn names_aliases_and_extensions_are_case_insensitive() {
    let registry = FormatRegistry::builder()
        .register(registration("test"))
        .build()
        .unwrap();

    assert_eq!(registry.get("TEST").unwrap().name(), "test");
    assert_eq!(registry.get("T").unwrap().name(), "test");
    assert_eq!(registry.by_extension("TEST").unwrap().name(), "test");
    assert_eq!(registry.by_extension(".test").unwrap().name(), "test");
}

#[test]
fn unregistered_format_is_unavailable() {
    let registry = FormatRegistry::builder()
        .register(registration("test"))
        .build()
        .unwrap();

    assert!(registry.get("missing").is_none());
    assert!(registry.by_extension("missing").is_none());
}

#[test]
fn duplicate_names_aliases_and_extensions_are_rejected() {
    let duplicate_name = FormatRegistry::builder()
        .register(registration("test"))
        .register(registration("TEST"))
        .build();
    assert!(matches!(
        duplicate_name,
        Err(FormatRegistryError::DuplicateName(name)) if name == "test"
    ));

    let duplicate_alias = FormatRegistry::builder()
        .register(registration("test"))
        .register(FormatRegistration::builder("other").aliases(["T"]).build())
        .build();
    assert!(matches!(
        duplicate_alias,
        Err(FormatRegistryError::DuplicateAlias(alias)) if alias == "t"
    ));

    let duplicate_extension = FormatRegistry::builder()
        .register(registration("test"))
        .register(
            FormatRegistration::builder("other")
                .extensions([".TEST"])
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_extension,
        Err(FormatRegistryError::DuplicateExtension(extension)) if extension == "test"
    ));
}

#[test]
fn duplicate_transform_argument_ids_long_names_and_short_names_are_rejected() {
    let duplicate_id = FormatRegistry::builder()
        .register(registration("test"))
        .register(
            FormatRegistration::builder("other")
                .transform(FormatTransform::with_args::<ConflictingIdArgs>().build())
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_id,
        Err(FormatRegistryError::DuplicateCliArgument(argument))
            if argument == "test_format_workers"
    ));

    let duplicate_long = FormatRegistry::builder()
        .register(
            FormatRegistration::builder("first")
                .transform(FormatTransform::with_args::<FirstLongArgs>().build())
                .build(),
        )
        .register(
            FormatRegistration::builder("second")
                .transform(FormatTransform::with_args::<SecondLongArgs>().build())
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_long,
        Err(FormatRegistryError::DuplicateCliArgument(argument)) if argument == "second_long"
    ));

    let duplicate_short = FormatRegistry::builder()
        .register(
            FormatRegistration::builder("first")
                .transform(FormatTransform::with_args::<FirstShortArgs>().build())
                .build(),
        )
        .register(
            FormatRegistration::builder("second")
                .transform(FormatTransform::with_args::<SecondShortArgs>().build())
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_short,
        Err(FormatRegistryError::DuplicateCliArgument(argument)) if argument == "second_short"
    ));
}

#[test]
fn identifier_iteration_uses_priority_then_registration_order() {
    let registry = FormatRegistry::builder()
        .register(
            FormatRegistration::builder("late")
                .identifier(identifier)
                .identifier_priority(10)
                .build(),
        )
        .register(
            FormatRegistration::builder("first")
                .identifier(identifier)
                .identifier_priority(1)
                .build(),
        )
        .register(
            FormatRegistration::builder("second")
                .identifier(identifier)
                .identifier_priority(1)
                .build(),
        )
        .register(FormatRegistration::builder("none").build())
        .build()
        .unwrap();

    assert_eq!(
        registry
            .identifiers()
            .map(FormatRegistration::name)
            .collect::<Vec<_>>(),
        ["first", "second", "late"]
    );
}

#[test]
fn explicit_async_capability_outputs_preserve_typed_settings_and_context() {
    let registry = FormatRegistry::builder()
        .register(registration("test"))
        .build()
        .unwrap();
    let matches = registry
        .augment_transform_args(Command::new("test"))
        .try_get_matches_from(["test", "--test-format-workers", "6"])
        .unwrap();
    let settings = registry.parse_transform_cli(&matches).unwrap();
    let registration = registry.get("test").unwrap();
    let location = resolved_location("input.test");

    let identified = futures::executor::block_on(registration.identify(&location))
        .unwrap()
        .unwrap();
    assert_eq!(identified.format(), "test");
    assert_eq!(identified.variant(), Some("test-stream"));

    let source =
        futures::executor::block_on(registration.create_source(&location, &settings)).unwrap();
    assert_eq!(source.name(), format!("{}:6", location.url));
    assert!(
        futures::executor::block_on(source.schema())
            .unwrap()
            .fields()
            .is_empty()
    );
    assert_eq!(
        futures::executor::block_on(source.row_count()).unwrap(),
        RowCount::Exact(0)
    );
    let mut session = SessionContext::new();
    let mut stream = futures::executor::block_on(source.as_stream(&mut session)).unwrap();
    assert!(
        futures::executor::block_on(stream.next()).is_none(),
        "the default stream should execute the source's table provider"
    );

    let context = SinkFactoryContext::new(NonZeroUsize::new(2).unwrap(), false, vec![]);
    let factory =
        futures::executor::block_on(registration.create_sink_factory(&context, &settings)).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::empty());
    let mut sink = futures::executor::block_on(factory.create(location.clone(), schema)).unwrap();
    let result = futures::executor::block_on(sink.finish()).unwrap();
    assert_eq!(
        result.files_written.as_slice(),
        std::slice::from_ref(&location.url)
    );
    assert_eq!(result.rows_written, 9);
}

#[test]
fn direct_stream_sources_do_not_require_storage_locations() {
    let source = DirectStreamSource {
        schema: Arc::new(arrow::datatypes::Schema::empty()),
        boundedness: StreamBoundedness::Infinite,
    };

    let capabilities = source.capabilities();
    assert_eq!(capabilities.boundedness(), StreamBoundedness::Infinite);
    assert_eq!(capabilities.input_access(), InputAccess::Sequential);
    assert_eq!(
        futures::executor::block_on(source.row_count()).unwrap(),
        RowCount::Unknown
    );

    let mut session = SessionContext::new();
    let provider = futures::executor::block_on(source.as_table_provider(&mut session)).unwrap();
    assert_eq!(provider.schema(), source.schema);
    let state = session.state();
    let plan = futures::executor::block_on(provider.scan(&state, None, &[], None)).unwrap();
    assert!(plan.properties().boundedness.is_unbounded());
}

#[test]
fn finite_sequential_sources_can_sort_without_a_row_count() {
    let source = DirectStreamSource {
        schema: Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        boundedness: StreamBoundedness::Finite,
    };

    let capabilities = source.capabilities();
    assert_eq!(capabilities.boundedness(), StreamBoundedness::Finite);
    assert_eq!(capabilities.input_access(), InputAccess::Sequential);
    assert_eq!(
        futures::executor::block_on(source.row_count()).unwrap(),
        RowCount::Unknown
    );

    let mut session = SessionContext::new();
    let provider = futures::executor::block_on(source.as_table_provider(&mut session)).unwrap();
    let data_frame = session
        .read_table(provider)
        .unwrap()
        .sort(vec![datafusion::prelude::col("value").sort(true, true)])
        .unwrap();
    let plan = futures::executor::block_on(data_frame.create_physical_plan()).unwrap();
    assert!(!plan.properties().boundedness.is_unbounded());
}

#[test]
fn one_typed_sink_factory_shares_state_across_output_sinks() {
    let registry = FormatRegistry::builder()
        .register(registration("test"))
        .build()
        .unwrap();
    let matches = registry
        .augment_transform_args(Command::new("test"))
        .try_get_matches_from(["test", "--test-format-workers", "6"])
        .unwrap();
    let settings = registry.parse_transform_cli(&matches).unwrap();
    let registration = registry.get("test").unwrap();
    let context = SinkFactoryContext::new(
        NonZeroUsize::new(3).unwrap(),
        true,
        vec![
            OutputSortColumn::new("customer_id", SortDirection::Ascending),
            OutputSortColumn::new("event_time", SortDirection::Descending),
        ],
    );
    let factory =
        futures::executor::block_on(registration.create_sink_factory(&context, &settings)).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::empty());
    let first_location = resolved_location("first.test");
    let second_location = resolved_location("second.test");

    let mut first =
        futures::executor::block_on(factory.create(first_location.clone(), Arc::clone(&schema)))
            .unwrap();
    let mut second =
        futures::executor::block_on(factory.create(second_location.clone(), schema)).unwrap();
    let first_result = futures::executor::block_on(first.finish()).unwrap();
    let second_result = futures::executor::block_on(second.finish()).unwrap();

    assert_eq!(first_result.files_written, vec![first_location.url]);
    assert_eq!(second_result.files_written, vec![second_location.url]);
    let expected_rows = 6 + 3 + 1 + 2 + ("customer_id".len() + 1) + ("event_time".len() + 2);
    assert_eq!(first_result.rows_written, expected_rows as u64);
    assert_eq!(second_result.rows_written, expected_rows as u64);
}

#[test]
fn runtime_invocation_reports_settings_mismatches_without_panicking() {
    let registry = FormatRegistry::builder()
        .register(registration("test"))
        .build()
        .unwrap();
    let matches = registry
        .augment_transform_args(Command::new("test"))
        .try_get_matches_from(["test"])
        .unwrap();
    let settings = registry.parse_transform_cli(&matches).unwrap();
    let mismatched = FormatRegistration::builder("test")
        .transform(
            FormatTransform::with_args::<OtherFormatArgs>()
                .source(|_, _| {
                    Box::pin(async {
                        unreachable!("the typed callback must not run with mismatched settings")
                    })
                })
                .build(),
        )
        .build();
    let location = resolved_location("input.test");

    let error = futures::executor::block_on(mismatched.create_source(&location, &settings))
        .err()
        .unwrap();
    assert!(matches!(
        error,
        FormatRuntimeError::SettingsTypeMismatch { format } if format == "test"
    ));
}

#[test]
fn inspection_invocation_reports_settings_mismatches_without_panicking() {
    let registration = registration("test");
    let matches = registration
        .augment_inspection_args(Command::new("inspect-test"))
        .try_get_matches_from(["inspect-test"])
        .unwrap();
    let settings = registration.parse_inspection_cli(&matches).unwrap();
    let mismatched = FormatRegistration::builder("test")
        .inspection(FormatInspection::with_args::<OtherFormatArgs>(
            |_, settings| {
                let _ = settings.other_format_option;
                Box::pin(async {
                    unreachable!("the typed callback must not run with mismatched settings")
                })
            },
        ))
        .build();
    let location = resolved_location("input.test");

    let error = futures::executor::block_on(mismatched.inspect(&location, &settings))
        .err()
        .unwrap();
    assert!(matches!(
        error,
        FormatRuntimeError::SettingsTypeMismatch { format } if format == "test"
    ));
}

#[test]
fn unavailable_capabilities_return_structured_errors() {
    let registry = FormatRegistry::builder()
        .register(FormatRegistration::builder("empty").build())
        .build()
        .unwrap();
    let matches = registry
        .augment_transform_args(Command::new("test"))
        .try_get_matches_from(["test"])
        .unwrap();
    let settings = registry.parse_transform_cli(&matches).unwrap();
    let registration = registry.get("empty").unwrap();
    let location = resolved_location("input.test");

    let error = futures::executor::block_on(registration.create_source(&location, &settings))
        .err()
        .unwrap();
    assert!(matches!(
        error,
        FormatRuntimeError::CapabilityUnavailable {
            format: "empty",
            capability: FormatCapability::Source,
        }
    ));
}

#[test]
fn type_erasure_is_private_to_the_registration_adapter() {
    let library = include_str!("../src/lib.rs");
    let registration = include_str!("../src/registration.rs");
    let source_contract = include_str!("../src/data_source.rs");
    let sink_contract = include_str!("../src/data_sink.rs");
    let inspection_contract = include_str!("../src/inspection.rs");
    let public_sources = [
        library,
        registration,
        source_contract,
        sink_contract,
        inspection_contract,
    ]
    .join("\n");

    for forbidden in [
        "std::any::Any",
        "dyn Any",
        "get_any",
        "CapabilityResult",
        "Box<dyn Any",
    ] {
        assert!(
            !public_sources.contains(forbidden),
            "public contract contains {forbidden}"
        );
    }

    let adapter = include_str!("../src/registration/erased.rs");
    assert!(adapter.contains("std::any::Any"));
    assert!(adapter.contains("Box<dyn Any + Send + Sync>"));
}

#[test]
fn data_source_uses_one_context_aware_table_provider_contract() {
    let source_contract = include_str!("../src/data_source.rs");

    assert!(!source_contract.contains("supports_table_provider"));
    assert!(!source_contract.contains("SessionContext::new"));
}
