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
    datatypes::{Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use clap::{Args, Command};
use datafusion::{catalog::TableProvider, datasource::empty::EmptyTable, prelude::SessionContext};
use silk_chiffon_core::{
    DataSink, DetectedFormat, FormatDefinition, FormatFuture, FormatOperation,
    FormatOperationError, FormatRegistry, FormatRegistryError, InputDetection, InputLeaf,
    InputVariant, InspectionDefinition, InspectionMode, InspectionOutput, OpenSinkMode,
    OutputOrderingColumn, SinkBinding, SinkBindingConfig, SinkCompletion, SortDirection,
    TransformDefinition,
};
use silk_chiffon_storage::{InputObject, LocationInput, StorageHandle, local};

#[derive(Args)]
struct TestArgs {
    /// Number embedded in the test input and output paths.
    #[arg(long, default_value_t = 4)]
    test_workers: usize,
}

#[derive(Args)]
struct InspectionArgs {
    #[arg(long)]
    test_details: bool,
}

#[derive(Args)]
struct SharedArgs {
    #[arg(long = "shared")]
    value: bool,
}

struct TestSinkBinding {
    opened: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
    workers: usize,
    thread_budget: NonZeroUsize,
    output_ordering: Arc<[OutputOrderingColumn]>,
}

struct TestSink {
    output: url::Url,
    opened: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
    workers: usize,
    thread_budget: NonZeroUsize,
    output_ordering: Arc<[OutputOrderingColumn]>,
}

#[async_trait]
impl SinkBinding for TestSinkBinding {
    async fn open_sink(&self, handle: StorageHandle, _: SchemaRef) -> Result<Box<dyn DataSink>> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestSink {
            output: handle.url().clone(),
            opened: Arc::clone(&self.opened),
            aborted: Arc::clone(&self.aborted),
            workers: self.workers,
            thread_budget: self.thread_budget,
            output_ordering: Arc::clone(&self.output_ordering),
        }))
    }
}

#[async_trait]
impl DataSink for TestSink {
    async fn write_batch(&mut self, _: RecordBatch) -> Result<()> {
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<SinkCompletion> {
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
        Ok(SinkCompletion::new(
            self.output.clone(),
            [],
            (self.workers
                + self.thread_budget.get()
                + self.opened.load(Ordering::SeqCst)
                + ordering_score) as u64,
        ))
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.aborted.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn detect_test(object: &InputObject) -> FormatFuture<'_, InputDetection> {
    Box::pin(async move {
        Ok(
            if object.handle().object_path().extension() == Some("test") {
                InputDetection::Match(InputVariant::named("test-stream"))
            } else {
                InputDetection::Mismatch
            },
        )
    })
}

fn detect_any(_: &InputObject) -> FormatFuture<'_, InputDetection> {
    Box::pin(async { Ok(InputDetection::Match(InputVariant::new())) })
}

fn create_unit_provider<'a>(
    _: &'a InputLeaf,
    _: &'a SessionContext,
    _: &'a (),
) -> FormatFuture<'a, Arc<dyn TableProvider>> {
    Box::pin(async {
        Ok(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))) as Arc<dyn TableProvider>)
    })
}

fn create_provider<'a>(
    leaf: &'a InputLeaf,
    session: &'a SessionContext,
    settings: &'a TestArgs,
) -> FormatFuture<'a, Arc<dyn TableProvider>> {
    Box::pin(async move {
        let partitions = session.state().config_options().execution.target_partitions;
        let variant = leaf.variant().name().unwrap_or("none");
        let schema = Arc::new(Schema::new(vec![Field::new(
            format!(
                "workers-{}-partitions-{partitions}-variant-{variant}",
                settings.test_workers
            ),
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        Ok(Arc::new(EmptyTable::new(schema)) as Arc<dyn TableProvider>)
    })
}

fn bind_sink<'a>(
    config: &'a SinkBindingConfig,
    settings: &'a TestArgs,
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    Box::pin(async move {
        Ok(Box::new(TestSinkBinding {
            opened: Arc::new(AtomicUsize::new(0)),
            aborted: Arc::new(AtomicUsize::new(0)),
            workers: settings.test_workers,
            thread_budget: config.thread_budget(),
            output_ordering: Arc::from(config.output_ordering()),
        }) as Box<dyn SinkBinding>)
    })
}

fn inspect<'a>(
    handle: &'a StorageHandle,
    mode: InspectionMode,
    settings: &'a InspectionArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        Ok(match mode {
            InspectionMode::Text => InspectionOutput::Text(format!(
                "{} details={}",
                handle.url(),
                settings.test_details
            )),
            InspectionMode::Json => InspectionOutput::Json(serde_json::json!({
                "details": settings.test_details,
            })),
        })
    })
}

fn test_format(name: &'static str) -> FormatDefinition {
    FormatDefinition::builder(name)
        .aliases(["t"])
        .extensions(["test"])
        .detector(detect_test)
        .detection_priority(7)
        .transform(
            TransformDefinition::with_args::<TestArgs>()
                .input_provider(create_provider)
                .sink(bind_sink)
                .build(),
        )
        .inspection(InspectionDefinition::with_args::<InspectionArgs>(inspect))
        .build()
}

fn local_object(extension: &str) -> InputObject {
    let file = tempfile::Builder::new()
        .suffix(extension)
        .tempfile()
        .unwrap();
    let location = LocationInput::parse(file.path().to_str().unwrap()).unwrap();
    futures::executor::block_on(local::session().unwrap().lookup_input(&location)).unwrap()
}

fn local_handle(path: &str) -> StorageHandle {
    let location = LocationInput::parse(path).unwrap();
    local::session().unwrap().input_handle(&location).unwrap()
}

fn local_leaf(session: &SessionContext, extension: &str, variant: InputVariant) -> InputLeaf {
    InputLeaf::try_new(session, &[local_object(extension)], variant).unwrap()
}

fn bind_test_transform(
    registry: &FormatRegistry,
    arguments: &[&str],
) -> silk_chiffon_core::TransformBindings {
    let matches = registry
        .augment_transform_args(Command::new("test"))
        .try_get_matches_from(arguments)
        .unwrap();
    registry.bind_transform(&matches).unwrap()
}

#[test]
fn definitions_keep_capabilities_independently_optional() {
    let empty = FormatDefinition::builder("empty").build();
    assert!(!empty.has_detector());
    assert!(!empty.has_input_provider());
    assert!(!empty.has_sink());
    assert!(!empty.has_inspector());

    let input_only = FormatDefinition::builder("input-only")
        .transform(
            TransformDefinition::with_args::<TestArgs>()
                .input_provider(create_provider)
                .build(),
        )
        .build();
    assert!(input_only.has_input_provider());
    assert!(!input_only.has_sink());
}

#[test]
fn transform_arguments_remain_bound_to_typed_functions() {
    let registry = FormatRegistry::builder()
        .register(test_format("test"))
        .build()
        .unwrap();
    let help = registry
        .augment_transform_args(Command::new("test"))
        .render_long_help()
        .to_string();
    assert!(help.contains("--test-workers"));

    let bindings = bind_test_transform(&registry, &["test", "--test-workers", "9"]);
    let session = SessionContext::new();
    let leaf = local_leaf(&session, ".test", InputVariant::named("test-stream"));
    let provider = futures::executor::block_on(
        bindings
            .get("test")
            .unwrap()
            .create_input_provider(&leaf, &session),
    )
    .unwrap();
    assert!(
        provider
            .schema()
            .field(0)
            .name()
            .starts_with("workers-9-partitions-")
    );
    assert!(
        provider
            .schema()
            .field(0)
            .name()
            .ends_with("variant-test-stream")
    );
}

#[test]
fn inspection_arguments_and_mode_reach_the_inspector() {
    let format = test_format("test");
    let matches = format
        .augment_inspection_args(Command::new("inspect"))
        .try_get_matches_from(["inspect", "--test-details"])
        .unwrap();
    let binding = format.bind_inspection(&matches).unwrap();
    let output = futures::executor::block_on(
        binding.inspect(local_object(".test").handle(), InspectionMode::Json),
    )
    .unwrap();
    assert_eq!(
        output,
        InspectionOutput::Json(serde_json::json!({ "details": true }))
    );
}

#[test]
fn names_aliases_and_extensions_are_case_insensitive() {
    let registry = FormatRegistry::builder()
        .register(test_format("test"))
        .build()
        .unwrap();
    assert_eq!(registry.get("TEST").unwrap().name(), "test");
    assert_eq!(registry.get("T").unwrap().name(), "test");
    assert_eq!(registry.by_extension(".TEST").unwrap().name(), "test");

    let bindings = bind_test_transform(&registry, &["test"]);
    assert_eq!(bindings.get("T").unwrap().format(), "test");
    assert_eq!(bindings.by_extension("TEST").unwrap().format(), "test");
}

#[test]
fn duplicate_claims_report_every_format() {
    let names = FormatRegistry::builder()
        .register(FormatDefinition::builder("dup").build())
        .register(FormatDefinition::builder("two").aliases(["DUP"]).build())
        .register(FormatDefinition::builder("three").aliases(["dup"]).build())
        .build();
    assert!(matches!(
        names,
        Err(FormatRegistryError::DuplicateName { name, formats })
            if name == "dup" && formats == ["dup", "two", "three"]
    ));

    let extensions = FormatRegistry::builder()
        .register(
            FormatDefinition::builder("one")
                .extensions(["same"])
                .build(),
        )
        .register(
            FormatDefinition::builder("two")
                .extensions([".SAME"])
                .build(),
        )
        .register(
            FormatDefinition::builder("three")
                .extensions(["same"])
                .build(),
        )
        .build();
    assert!(matches!(
        extensions,
        Err(FormatRegistryError::DuplicateExtension { extension, formats })
            if extension == "same" && formats == ["one", "two", "three"]
    ));

    let arguments = FormatRegistry::builder()
        .register(
            FormatDefinition::builder("one")
                .transform(TransformDefinition::with_args::<SharedArgs>().build())
                .build(),
        )
        .register(
            FormatDefinition::builder("two")
                .transform(TransformDefinition::with_args::<SharedArgs>().build())
                .build(),
        )
        .register(
            FormatDefinition::builder("three")
                .transform(TransformDefinition::with_args::<SharedArgs>().build())
                .build(),
        )
        .build();
    assert!(matches!(
        arguments,
        Err(FormatRegistryError::DuplicateCliArgument { argument, formats })
            if argument == "value" && formats == ["one", "two", "three"]
    ));
}

fn detected_name(detected: Option<DetectedFormat>) -> Option<&'static str> {
    detected.map(|detected| detected.format())
}

#[test]
fn detection_uses_priority_then_registration_order() {
    let registry = FormatRegistry::builder()
        .register(
            FormatDefinition::builder("late")
                .detector(detect_test)
                .detection_priority(10)
                .build(),
        )
        .register(
            FormatDefinition::builder("first")
                .detector(detect_test)
                .detection_priority(1)
                .build(),
        )
        .register(
            FormatDefinition::builder("second")
                .detector(detect_test)
                .detection_priority(1)
                .build(),
        )
        .build()
        .unwrap();
    let detected = futures::executor::block_on(registry.detect(&local_object(".test"))).unwrap();
    assert_eq!(detected_name(detected), Some("first"));
}

#[test]
fn detection_tries_the_case_insensitive_extension_owner_first() {
    let transform = || {
        TransformDefinition::without_args()
            .input_provider(create_unit_provider)
            .build()
    };
    let registry = FormatRegistry::builder()
        .register(
            FormatDefinition::builder("priority")
                .detector(detect_any)
                .detection_priority(0)
                .transform(transform())
                .build(),
        )
        .register(
            FormatDefinition::builder("preferred")
                .extensions(["preferred"])
                .detector(detect_any)
                .detection_priority(10)
                .transform(transform())
                .build(),
        )
        .build()
        .unwrap();
    let object = local_object(".PREFERRED");

    let detected = futures::executor::block_on(registry.detect(&object)).unwrap();
    assert_eq!(detected_name(detected), Some("preferred"));

    let bindings = bind_test_transform(&registry, &["test"]);
    let detected = futures::executor::block_on(bindings.detect(&object)).unwrap();
    assert_eq!(
        detected.map(|(format, _)| format.format()),
        Some("preferred")
    );
}

#[test]
fn transform_detection_skips_formats_without_input_providers() {
    let registry = FormatRegistry::builder()
        .register(
            FormatDefinition::builder("sink-only")
                .extensions(["test"])
                .detector(detect_any)
                .detection_priority(0)
                .transform(
                    TransformDefinition::with_args::<TestArgs>()
                        .sink(bind_sink)
                        .build(),
                )
                .build(),
        )
        .register(
            FormatDefinition::builder("input")
                .detector(detect_any)
                .detection_priority(10)
                .transform(
                    TransformDefinition::without_args()
                        .input_provider(create_unit_provider)
                        .build(),
                )
                .build(),
        )
        .build()
        .unwrap();
    let bindings = bind_test_transform(&registry, &["test"]);

    let detected = futures::executor::block_on(bindings.detect(&local_object(".test"))).unwrap();

    assert_eq!(detected.map(|(format, _)| format.format()), Some("input"));
}

#[test]
fn one_sink_binding_shares_state_across_opened_sinks() {
    let registry = FormatRegistry::builder()
        .register(test_format("test"))
        .build()
        .unwrap();
    let bindings = bind_test_transform(&registry, &["test", "--test-workers", "6"]);
    let transform = bindings.get("test").unwrap();
    let config = SinkBindingConfig::new(
        NonZeroUsize::new(3).unwrap(),
        OpenSinkMode::Multiple,
        vec![OutputOrderingColumn::new(
            "event_time",
            SortDirection::Descending,
        )],
    );
    let binding = futures::executor::block_on(transform.bind_sink(&config)).unwrap();
    let schema = Arc::new(Schema::empty());
    let first_handle = local_handle("first.test");
    let second_handle = local_handle("second.test");
    let first =
        futures::executor::block_on(binding.open_sink(first_handle.clone(), Arc::clone(&schema)))
            .unwrap();
    let second =
        futures::executor::block_on(binding.open_sink(second_handle.clone(), schema)).unwrap();
    let first_result = futures::executor::block_on(first.finish()).unwrap();
    let second_result = futures::executor::block_on(second.finish()).unwrap();
    assert_eq!(
        first_result.durable_locations(),
        [first_handle.url().clone()]
    );
    assert_eq!(
        second_result.durable_locations(),
        [second_handle.url().clone()]
    );
    let expected_rows = 6 + 3 + 2 + "event_time".len() + 2;
    assert_eq!(first_result.rows_written(), expected_rows as u64);
    assert_eq!(second_result.rows_written(), expected_rows as u64);
}

#[test]
fn an_open_sink_can_be_consumed_by_abort() {
    let registry = FormatRegistry::builder()
        .register(test_format("test"))
        .build()
        .unwrap();
    let bindings = bind_test_transform(&registry, &["test"]);
    let transform = bindings.get("test").unwrap();
    let config = SinkBindingConfig::new(
        NonZeroUsize::new(1).unwrap(),
        OpenSinkMode::OneAtATime,
        Vec::new(),
    );
    let binding = futures::executor::block_on(transform.bind_sink(&config)).unwrap();
    let sink = futures::executor::block_on(
        binding.open_sink(local_handle("aborted.test"), Arc::new(Schema::empty())),
    )
    .unwrap();

    futures::executor::block_on(sink.abort()).unwrap();
}

#[test]
fn unavailable_capabilities_return_structured_errors() {
    let registry = FormatRegistry::builder()
        .register(
            FormatDefinition::builder("empty")
                .transform(TransformDefinition::without_args().build())
                .build(),
        )
        .build()
        .unwrap();
    let bindings = bind_test_transform(&registry, &["test"]);
    let session = SessionContext::new();
    let leaf = local_leaf(&session, ".empty", InputVariant::new());
    let error = futures::executor::block_on(
        bindings
            .get("empty")
            .unwrap()
            .create_input_provider(&leaf, &session),
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        FormatOperationError::Unsupported {
            format: "empty",
            operation: FormatOperation::InputProviderCreation,
        }
    ));
}
