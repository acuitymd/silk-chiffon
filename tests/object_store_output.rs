use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{Int32Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use clap::Command;
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    error::DataFusionError,
    execution::TaskContext,
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchReceiverStreamBuilder,
        streaming::PartitionStream,
    },
};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use silk_chiffon::sinks::data_sink::DataSink;
use silk_chiffon_core::{FormatRegistry, InputSources, OpenSinkMode, Pipeline, SinkBindingConfig};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, OutputPreparation, StorageAccess, StorageBackend, StorageHandle,
    StorageRegistry, StorageSession,
};
use silk_chiffon_test_support::controlled_upload::{
    ControlledUploadStore, controlled_upload_lock, controlled_upload_storage,
    controlled_upload_storage_with, controlled_upload_store,
};

type TrackingStore = ControlledUploadStore;

const SOURCE_BATCH_LIMIT: usize = 1_000_000;

async fn open_registered_arrow_sink(
    handle: StorageHandle,
    schema: arrow::datatypes::SchemaRef,
    arguments: &[&str],
) -> Box<dyn DataSink> {
    let registry = FormatRegistry::builder()
        .register(silk_chiffon_format_arrow::definition())
        .build()
        .unwrap();
    let matches = registry
        .augment_transform_args(Command::new("test"))
        .try_get_matches_from(std::iter::once("test").chain(arguments.iter().copied()))
        .unwrap();
    let bindings = registry.bind_transform(&matches).unwrap();
    let binding = bindings.get("arrow").unwrap();
    let sink_binding = binding
        .bind_sink(&SinkBindingConfig::new(
            NonZeroUsize::new(1).unwrap(),
            OpenSinkMode::OneAtATime,
            Vec::new(),
        ))
        .await
        .unwrap();
    sink_binding.open_sink(handle, schema).await.unwrap()
}

async fn open_registered_parquet_sink(
    handle: StorageHandle,
    schema: arrow::datatypes::SchemaRef,
    arguments: &[&str],
) -> Box<dyn DataSink> {
    let registry = FormatRegistry::builder()
        .register(silk_chiffon_format_parquet::definition())
        .build()
        .unwrap();
    let matches = registry
        .augment_transform_args(Command::new("test").arg(clap::Arg::new("sort_by").long("sort-by")))
        .try_get_matches_from(std::iter::once("test").chain(arguments.iter().copied()))
        .unwrap();
    let bindings = registry.bind_transform(&matches).unwrap();
    let sink_binding = bindings
        .get("parquet")
        .unwrap()
        .bind_sink(&SinkBindingConfig::new(
            NonZeroUsize::new(2).unwrap(),
            OpenSinkMode::OneAtATime,
            Vec::new(),
        ))
        .await
        .unwrap();
    sink_binding.open_sink(handle, schema).await.unwrap()
}

async fn open_registered_vortex_sink(
    handle: StorageHandle,
    schema: arrow::datatypes::SchemaRef,
    arguments: &[&str],
) -> Box<dyn DataSink> {
    let registry = FormatRegistry::builder()
        .register(silk_chiffon_format_vortex::definition())
        .build()
        .unwrap();
    let matches = registry
        .augment_transform_args(Command::new("test"))
        .try_get_matches_from(std::iter::once("test").chain(arguments.iter().copied()))
        .unwrap();
    let bindings = registry.bind_transform(&matches).unwrap();
    let sink_binding = bindings
        .get("vortex")
        .unwrap()
        .bind_sink(&SinkBindingConfig::new(
            NonZeroUsize::new(1).unwrap(),
            OpenSinkMode::OneAtATime,
            Vec::new(),
        ))
        .await
        .unwrap();
    sink_binding.open_sink(handle, schema).await.unwrap()
}

#[derive(Clone, Debug)]
enum SourceTaskExit {
    Endless,
    CompleteAfter(usize),
    FailAfter {
        batches: usize,
        release: Arc<tokio::sync::Barrier>,
    },
}

#[derive(Debug)]
struct SourceTaskState {
    started: AtomicBool,
    stopped: AtomicBool,
    cancelled: AtomicBool,
    batches_sent: AtomicUsize,
    state_changed: tokio::sync::Notify,
}

impl SourceTaskState {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            batches_sent: AtomicUsize::new(0),
            state_changed: tokio::sync::Notify::new(),
        }
    }

    async fn wait_until_started(&self) {
        loop {
            let state_changed = self.state_changed.notified();
            if self.started.load(Ordering::SeqCst) {
                return;
            }
            state_changed.await;
        }
    }

    async fn wait_until_stopped(&self) {
        loop {
            let state_changed = self.state_changed.notified();
            if self.stopped.load(Ordering::SeqCst) {
                return;
            }
            state_changed.await;
        }
    }
}

struct SourceTaskLifetime {
    state: Arc<SourceTaskState>,
    completed: bool,
}

impl Drop for SourceTaskLifetime {
    fn drop(&mut self) {
        self.state
            .cancelled
            .store(!self.completed, Ordering::SeqCst);
        self.state.stopped.store(true, Ordering::SeqCst);
        self.state.state_changed.notify_waiters();
    }
}

#[derive(Debug)]
struct StructuredServicePartition {
    batch: RecordBatch,
    state: Arc<SourceTaskState>,
    exit: SourceTaskExit,
}

impl PartitionStream for StructuredServicePartition {
    fn schema(&self) -> &arrow::datatypes::SchemaRef {
        self.batch.schema_ref()
    }

    fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut stream = RecordBatchReceiverStreamBuilder::new(self.batch.schema(), 1);
        let sender = stream.tx();
        let batch = self.batch.clone();
        let state = Arc::clone(&self.state);
        let exit = self.exit.clone();
        stream.spawn(async move {
            let mut lifetime = SourceTaskLifetime {
                state: Arc::clone(&state),
                completed: false,
            };
            state.started.store(true, Ordering::SeqCst);
            state.state_changed.notify_waiters();
            let batches = match &exit {
                SourceTaskExit::Endless => SOURCE_BATCH_LIMIT,
                SourceTaskExit::CompleteAfter(batches)
                | SourceTaskExit::FailAfter { batches, .. } => *batches,
            };
            for _ in 0..batches {
                if sender.send(Ok(batch.clone())).await.is_err() {
                    return Ok(());
                }
                state.batches_sent.fetch_add(1, Ordering::SeqCst);
            }
            if let SourceTaskExit::FailAfter { release, .. } = exit {
                release.wait().await;
                if sender
                    .send(Err(DataFusionError::Execution(
                        "controlled source failure".to_owned(),
                    )))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            lifetime.completed = true;
            Ok(())
        });
        stream.build()
    }
}

fn memory_store(
    _store_url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(InMemory::new()))
}

fn storage() -> StorageSession {
    let backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["memory"])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(memory_store)
        .build()
        .unwrap();
    let registry = StorageRegistry::builder()
        .register(backend)
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("output-test"));
    let matches = command
        .try_get_matches_from([
            "output-test",
            "--object-store-upload-part-size",
            "64",
            "--object-store-max-in-flight-parts",
            "2",
        ])
        .unwrap();
    registry.create_session(&matches).unwrap()
}

async fn prepared_handle(storage: &StorageSession, target: &str) -> StorageHandle {
    storage
        .prepare_output_target(
            &LocationInput::parse(target).unwrap(),
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap()
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

fn source_provider(
    batch: &RecordBatch,
    state: Arc<SourceTaskState>,
    exit: SourceTaskExit,
) -> Arc<dyn TableProvider> {
    let partition = Arc::new(StructuredServicePartition {
        batch: batch.clone(),
        state,
        exit,
    });
    Arc::new(StreamingTable::try_new(batch.schema(), vec![partition]).unwrap())
}

async fn source_execution(
    batch: &RecordBatch,
    sources: Vec<(Arc<SourceTaskState>, SourceTaskExit)>,
) -> SendableRecordBatchStream {
    let providers = sources
        .into_iter()
        .map(|(state, exit)| source_provider(batch, state, exit))
        .collect();
    let mut pipeline = Pipeline::new().with_target_partitions(Some(1));
    let session = pipeline.create_session_context().unwrap();
    pipeline = pipeline.with_inputs(InputSources::try_new(&session, providers).unwrap());
    pipeline
        .prepare(session)
        .await
        .unwrap()
        .begin_execution()
        .unwrap()
        .into_sendable_stream()
}

async fn assert_durable(completion: silk_chiffon_core::SinkCompletion, handle: &StorageHandle) {
    assert_eq!(completion.rows_written(), 3);
    assert_eq!(completion.durable_locations(), [handle.url().clone()]);
    assert!(
        handle
            .object_store()
            .head(handle.object_path())
            .await
            .unwrap()
            .size
            > 0
    );
}

async fn drive_to_active_part(
    sink: &mut dyn DataSink,
    handle: &StorageHandle,
    store: &TrackingStore,
) {
    let active_before = store.active_parts();
    for _ in 0..64 {
        if store.active_parts() > active_before {
            break;
        }

        let write = sink.write_batch(batch());
        tokio::pin!(write);
        tokio::select! {
            result = &mut write => result.unwrap(),
            result = tokio::time::timeout(Duration::from_secs(5), async {
                while store.active_parts() == active_before {
                    tokio::task::yield_now().await;
                }
            }) => {
                result.unwrap_or_else(|_| {
                    panic!(
                        "format did not start its multipart upload for {}",
                        handle.url()
                    )
                });
                break;
            }
        }
    }
    tokio::time::timeout(
        Duration::from_secs(5),
        store.wait_for_more_active_parts(active_before),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "format did not start its multipart upload for {}",
            handle.url()
        )
    });
}

async fn wait_for_multipart_cleanup(store: &TrackingStore, active_before: usize) {
    tokio::time::timeout(
        Duration::from_secs(5),
        store.wait_for_active_parts(active_before),
    )
    .await
    .expect("multipart part remained active after cleanup");
}

async fn wait_for_resource_release<T>(resource: &std::sync::Weak<T>, message: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while resource.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{message}"));
}

async fn wait_for_source_stop(state: &SourceTaskState) {
    tokio::time::timeout(Duration::from_secs(5), state.wait_until_stopped())
        .await
        .expect("DataFusion source task did not stop when its execution stream was dropped");
}

async fn wait_for_sources_started(states: &[Arc<SourceTaskState>]) {
    tokio::time::timeout(Duration::from_secs(5), async {
        for state in states {
            state.wait_until_started().await;
        }
    })
    .await
    .expect("DataFusion did not start every source task");
}

async fn wait_for_sources_stopped(states: &[Arc<SourceTaskState>]) {
    for state in states {
        wait_for_source_stop(state).await;
    }
}

async fn assert_abort_cleans_multipart(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    store: &TrackingStore,
) {
    let starts_before = store.multipart_starts();
    let aborts_before = store.aborts();
    let active_before = store.active_parts();
    drive_to_active_part(sink.as_mut(), handle, store).await;

    tokio::time::timeout(Duration::from_secs(5), sink.abort())
        .await
        .unwrap_or_else(|_| panic!("format abort timed out for {}", handle.url()))
        .unwrap();

    assert_eq!(
        store.aborts() - aborts_before,
        store.multipart_starts() - starts_before
    );
    wait_for_multipart_cleanup(store, active_before).await;
    assert_eq!(store.active_parts(), active_before);
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

async fn assert_abort_reports_cleanup_failure(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    store: &TrackingStore,
) {
    let active_before = store.active_parts();
    drive_to_active_part(sink.as_mut(), handle, store).await;
    store.fail_next_abort();

    let error = tokio::time::timeout(Duration::from_secs(5), sink.abort())
        .await
        .unwrap_or_else(|_| panic!("format abort timed out for {}", handle.url()))
        .unwrap_err();

    assert!(
        format!("{error:#}").contains("controlled abort failure"),
        "{error:#}"
    );
    wait_for_multipart_cleanup(store, active_before).await;
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

async fn assert_drop_cleans_multipart(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    store: &TrackingStore,
) {
    let aborts_before = store.aborts();
    let active_before = store.active_parts();
    drive_to_active_part(sink.as_mut(), handle, store).await;
    drop(sink);

    tokio::time::timeout(Duration::from_secs(5), async {
        while store.aborts() == aborts_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("format drop did not abort its multipart upload");
    wait_for_multipart_cleanup(store, active_before).await;
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

async fn assert_cancelled_finish_cleans_multipart(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    store: &TrackingStore,
) {
    let aborts_before = store.aborts();
    let active_before = store.active_parts();
    drive_to_active_part(sink.as_mut(), handle, store).await;

    let mut finish = tokio::spawn(sink.finish());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut finish)
            .await
            .is_err(),
        "format finish was not blocked for {}",
        handle.url()
    );
    finish.abort();
    assert!(finish.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(5), async {
        while store.aborts() == aborts_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling format finish did not abort its multipart upload");
    wait_for_multipart_cleanup(store, active_before).await;
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

async fn assert_finish_failure_cleans_multipart(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    store: &TrackingStore,
) {
    let aborts_before = store.aborts();
    sink.write_batch(batch()).await.unwrap();
    store.fail_next_complete();

    let error = tokio::time::timeout(Duration::from_secs(5), sink.finish())
        .await
        .unwrap_or_else(|_| panic!("format finish timed out for {}", handle.url()))
        .unwrap_err();

    assert!(
        error.to_string().contains("controlled complete failure"),
        "{error:#}"
    );
    assert_eq!(store.aborts(), aborts_before + 1);
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

async fn assert_controlled_write_failure(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    expected: &str,
) {
    let error = match sink.write_batch(batch()).await {
        Ok(()) => sink.finish().await.unwrap_err(),
        Err(error) => {
            let _ = sink.abort().await;
            error
        }
    };

    assert!(format!("{error:#}").contains(expected), "{error:#}");
    assert!(matches!(
        handle.object_store().head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn arrow_sink_writes_a_memory_object() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/output.arrow").await;
    let batch = batch();
    let mut sink = open_registered_arrow_sink(handle.clone(), batch.schema(), &[]).await;

    sink.write_batch(batch).await.unwrap();
    let completion = sink.finish().await.unwrap();
    assert_durable(completion, &handle).await;
}

#[tokio::test]
async fn arrow_stream_sink_writes_a_readable_memory_object() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/output.arrows").await;
    let batch = batch();
    let expected_rows = batch.num_rows();
    let mut sink = open_registered_arrow_sink(
        handle.clone(),
        batch.schema(),
        &["--arrow-format", "stream"],
    )
    .await;

    sink.write_batch(batch).await.unwrap();
    let completion = sink.finish().await.unwrap();
    assert_eq!(
        completion.rows_written(),
        u64::try_from(expected_rows).unwrap()
    );
    let bytes = handle
        .object_store()
        .get(handle.object_path())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let batches = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected_rows
    );
}

#[tokio::test]
async fn parquet_sink_writes_a_memory_object() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/output.parquet").await;
    let batch = batch();
    let mut sink = open_registered_parquet_sink(handle.clone(), batch.schema(), &[]).await;

    sink.write_batch(batch).await.unwrap();
    let completion = sink.finish().await.unwrap();
    assert_durable(completion, &handle).await;
}

async fn assert_schema_mismatch_is_rejected_before_encoding(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
) {
    let actual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let mismatched = RecordBatch::try_new(
        actual_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let error = sink.write_batch(mismatched).await.unwrap_err();

    assert!(
        format!("{error:#}").contains("schema"),
        "unexpected error: {error:#}"
    );
    sink.abort().await.unwrap();
    assert!(matches!(
        handle.object_store().head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn every_format_rejects_schema_mismatch_before_encoding() {
    let storage = storage();
    let schema = batch().schema();

    let arrow_handle = prepared_handle(&storage, "memory://bucket/schema-mismatch.arrow").await;
    let arrow = open_registered_arrow_sink(arrow_handle.clone(), Arc::clone(&schema), &[]).await;
    assert_schema_mismatch_is_rejected_before_encoding(arrow, &arrow_handle).await;

    let parquet_handle = prepared_handle(&storage, "memory://bucket/schema-mismatch.parquet").await;
    let parquet =
        open_registered_parquet_sink(parquet_handle.clone(), Arc::clone(&schema), &[]).await;
    assert_schema_mismatch_is_rejected_before_encoding(parquet, &parquet_handle).await;

    let vortex_handle = prepared_handle(&storage, "memory://bucket/schema-mismatch.vortex").await;
    let vortex = open_registered_vortex_sink(vortex_handle.clone(), schema, &[]).await;
    assert_schema_mismatch_is_rejected_before_encoding(vortex, &vortex_handle).await;
}

#[tokio::test]
async fn every_format_accepts_metadata_only_schema_differences() {
    let storage = storage();
    let expected_schema = batch().schema();
    let actual_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "field-source".to_owned(),
                "batch".to_owned(),
            )])),
            Field::new("name", DataType::Utf8, false),
        ],
        HashMap::from([("schema-source".to_owned(), "batch".to_owned())]),
    ));
    let metadata_batch = RecordBatch::try_new(
        actual_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let arrow_handle = prepared_handle(&storage, "memory://bucket/metadata.arrow").await;
    let mut arrow =
        open_registered_arrow_sink(arrow_handle.clone(), Arc::clone(&expected_schema), &[]).await;
    arrow.write_batch(metadata_batch.clone()).await.unwrap();
    assert_durable(arrow.finish().await.unwrap(), &arrow_handle).await;

    let parquet_handle = prepared_handle(&storage, "memory://bucket/metadata.parquet").await;
    let mut parquet =
        open_registered_parquet_sink(parquet_handle.clone(), Arc::clone(&expected_schema), &[])
            .await;
    parquet.write_batch(metadata_batch.clone()).await.unwrap();
    assert_durable(parquet.finish().await.unwrap(), &parquet_handle).await;

    let vortex_handle = prepared_handle(&storage, "memory://bucket/metadata.vortex").await;
    let mut vortex = open_registered_vortex_sink(vortex_handle.clone(), expected_schema, &[]).await;
    vortex.write_batch(metadata_batch).await.unwrap();
    assert_durable(vortex.finish().await.unwrap(), &vortex_handle).await;
}

#[tokio::test]
async fn vortex_sink_writes_a_memory_object() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/output.vortex").await;
    let batch = batch();
    let mut sink = open_registered_vortex_sink(handle.clone(), batch.schema(), &[]).await;

    sink.write_batch(batch).await.unwrap();
    let completion = sink.finish().await.unwrap();
    assert_durable(completion, &handle).await;
}

#[tokio::test]
async fn every_format_reports_single_put_failures_without_durable_outputs() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage_with(1024 * 1024, 2);
    let store = controlled_upload_store();
    let multipart_starts = store.multipart_starts();

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/put-error.arrow").await;
    let arrow = open_registered_arrow_sink(arrow_handle.clone(), batch().schema(), &[]).await;
    store.fail_next_put();
    assert_controlled_write_failure(arrow, &arrow_handle, "controlled put failure").await;

    let parquet_handle = prepared_handle(&storage, "tracking://bucket/put-error.parquet").await;
    let parquet = open_registered_parquet_sink(parquet_handle.clone(), batch().schema(), &[]).await;
    store.fail_next_put();
    assert_controlled_write_failure(parquet, &parquet_handle, "controlled put failure").await;

    let vortex_handle = prepared_handle(&storage, "tracking://bucket/put-error.vortex").await;
    let vortex = open_registered_vortex_sink(vortex_handle.clone(), batch().schema(), &[]).await;
    store.fail_next_put();
    assert_controlled_write_failure(vortex, &vortex_handle, "controlled put failure").await;

    assert_eq!(store.multipart_starts(), multipart_starts);
}

#[tokio::test]
async fn every_format_reports_multipart_start_failures_without_durable_outputs() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/start-error.arrow").await;
    let arrow = open_registered_arrow_sink(
        arrow_handle.clone(),
        batch().schema(),
        &["--arrow-record-batch-size", "1"],
    )
    .await;
    store.fail_next_multipart_start();
    assert_controlled_write_failure(arrow, &arrow_handle, "controlled multipart-start failure")
        .await;

    let parquet_handle = prepared_handle(&storage, "tracking://bucket/start-error.parquet").await;
    let parquet = open_registered_parquet_sink(
        parquet_handle.clone(),
        batch().schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
        ],
    )
    .await;
    store.fail_next_multipart_start();
    assert_controlled_write_failure(
        parquet,
        &parquet_handle,
        "controlled multipart-start failure",
    )
    .await;

    let vortex_handle = prepared_handle(&storage, "tracking://bucket/start-error.vortex").await;
    let vortex = open_registered_vortex_sink(
        vortex_handle.clone(),
        batch().schema(),
        &["--vortex-record-batch-size", "1"],
    )
    .await;
    store.fail_next_multipart_start();
    assert_controlled_write_failure(vortex, &vortex_handle, "controlled multipart-start failure")
        .await;
}

#[tokio::test]
async fn parquet_late_part_failure_cancels_the_entire_pipeline() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let handle = prepared_handle(&storage, "tracking://bucket/later-part-error.parquet").await;
    let sink = open_registered_parquet_sink(
        handle.clone(),
        batch().schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
            "--parquet-ingestion-queue-size",
            "1",
            "--parquet-encoding-queue-size",
            "1",
            "--parquet-writing-queue-size",
            "1",
            "--parquet-row-group-concurrency",
            "2",
        ],
    )
    .await;
    store.fail_part_after(4);

    assert_controlled_write_failure(sink, &handle, "controlled part failure").await;
    assert_eq!(store.active_parts(), 0);
}

async fn assert_sink_failure_cancels_every_datafusion_source_task(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    store: &Arc<TrackingStore>,
    expected_write_error: &str,
) {
    let batch = batch();
    let source_states = vec![
        Arc::new(SourceTaskState::new()),
        Arc::new(SourceTaskState::new()),
    ];
    let stream = source_execution(
        &batch,
        source_states
            .iter()
            .map(|state| (Arc::clone(state), SourceTaskExit::Endless))
            .collect(),
    )
    .await;
    let blocked_parts = store.block_parts();
    let write_error = {
        let write = sink.write_stream(stream);
        tokio::pin!(write);

        tokio::select! {
            () = wait_for_sources_started(&source_states) => {}
            result = &mut write => panic!("sink stopped before every source started: {result:?}"),
        }
        store.fail_next_part();
        drop(blocked_parts);
        tokio::time::timeout(Duration::from_secs(5), &mut write)
            .await
            .expect("sink failure did not stop stream consumption")
            .unwrap_err()
    };
    assert!(
        write_error.to_string().contains(expected_write_error),
        "{write_error:#}"
    );
    wait_for_sources_stopped(&source_states).await;
    for state in &source_states {
        assert!(state.started.load(Ordering::SeqCst));
        assert!(state.cancelled.load(Ordering::SeqCst));
        assert!(state.batches_sent.load(Ordering::SeqCst) < SOURCE_BATCH_LIMIT);
    }

    let cleanup_error = tokio::time::timeout(Duration::from_secs(5), sink.abort())
        .await
        .expect("sink cleanup remained blocked after source cancellation")
        .unwrap_err();
    assert!(
        format!("{cleanup_error:#}").contains("controlled part failure"),
        "{cleanup_error:#}"
    );
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_sink_failure_cancels_every_datafusion_source_task() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let handle = prepared_handle(&storage, "tracking://bucket/source-cancellation.arrow").await;
    let sink = open_registered_arrow_sink(
        handle.clone(),
        batch().schema(),
        &[
            "--arrow-record-batch-size",
            "1",
            "--arrow-writing-queue-size",
            "1",
        ],
    )
    .await;

    assert_sink_failure_cancels_every_datafusion_source_task(
        sink,
        &handle,
        &store,
        "writer task died",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_sink_failure_cancels_every_datafusion_source_task() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let handle = prepared_handle(&storage, "tracking://bucket/source-cancellation.parquet").await;
    let sink = open_registered_parquet_sink(
        handle.clone(),
        batch().schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
            "--parquet-ingestion-queue-size",
            "1",
            "--parquet-encoding-queue-size",
            "1",
            "--parquet-writing-queue-size",
            "1",
            "--parquet-row-group-concurrency",
            "1",
            "--parquet-dictionary-column",
            "name:analyze",
        ],
    )
    .await;

    assert_sink_failure_cancels_every_datafusion_source_task(
        sink,
        &handle,
        &store,
        "Parquet pipeline closed",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_failure_cancels_its_active_sibling() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/source-failure.arrow").await;
    let batch = batch();
    let failing_state = Arc::new(SourceTaskState::new());
    let sibling_state = Arc::new(SourceTaskState::new());
    let source_states = vec![Arc::clone(&failing_state), Arc::clone(&sibling_state)];
    let release_failure = Arc::new(tokio::sync::Barrier::new(2));
    let stream = source_execution(
        &batch,
        vec![
            (
                Arc::clone(&failing_state),
                SourceTaskExit::FailAfter {
                    batches: 1,
                    release: Arc::clone(&release_failure),
                },
            ),
            (Arc::clone(&sibling_state), SourceTaskExit::Endless),
        ],
    )
    .await;
    let mut sink = open_registered_arrow_sink(
        handle.clone(),
        batch.schema(),
        &[
            "--arrow-record-batch-size",
            "1",
            "--arrow-writing-queue-size",
            "1",
        ],
    )
    .await;
    let write_error = {
        let write = sink.write_stream(stream);
        tokio::pin!(write);

        tokio::select! {
            () = wait_for_sources_started(&source_states) => {}
            result = &mut write => panic!("source failed before its sibling started: {result:?}"),
        }
        release_failure.wait().await;
        tokio::time::timeout(Duration::from_secs(5), &mut write)
            .await
            .expect("source failure did not stop stream consumption")
            .unwrap_err()
    };

    assert!(
        format!("{write_error:#}").contains("controlled source failure"),
        "{write_error:#}"
    );
    wait_for_sources_stopped(&source_states).await;
    assert!(!failing_state.cancelled.load(Ordering::SeqCst));
    assert!(sibling_state.cancelled.load(Ordering::SeqCst));
    sink.abort().await.unwrap();
    assert!(matches!(
        handle.object_store().head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_pipeline_stream_cancels_every_active_source() {
    let batch = batch();
    let source_states = vec![
        Arc::new(SourceTaskState::new()),
        Arc::new(SourceTaskState::new()),
    ];
    let mut stream = source_execution(
        &batch,
        source_states
            .iter()
            .map(|state| (Arc::clone(state), SourceTaskExit::Endless))
            .collect(),
    )
    .await;

    tokio::time::timeout(Duration::from_secs(5), async {
        while source_states
            .iter()
            .any(|state| !state.started.load(Ordering::SeqCst))
        {
            stream
                .next()
                .await
                .expect("endless input stopped before every source started")
                .unwrap();
        }
    })
    .await
    .expect("DataFusion did not activate every source while polling");
    drop(stream);

    wait_for_sources_stopped(&source_states).await;
    for state in &source_states {
        assert!(state.cancelled.load(Ordering::SeqCst));
        assert!(state.batches_sent.load(Ordering::SeqCst) < SOURCE_BATCH_LIMIT);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_a_finite_source_records_normal_completion() {
    let batch = batch();
    let source_state = Arc::new(SourceTaskState::new());
    let mut stream = source_execution(
        &batch,
        vec![(Arc::clone(&source_state), SourceTaskExit::CompleteAfter(3))],
    )
    .await;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }

    wait_for_source_stop(&source_state).await;
    assert_eq!(rows, 9);
    assert_eq!(source_state.batches_sent.load(Ordering::SeqCst), 3);
    assert!(!source_state.cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn arrow_abort_cancels_a_backpressured_multipart_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let _blocked = store.block_parts();

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/output.arrow").await;
    let arrow = open_registered_arrow_sink(
        arrow_handle.clone(),
        batch.schema(),
        &["--arrow-record-batch-size", "1"],
    )
    .await;
    assert_abort_cleans_multipart(arrow, &arrow_handle, &store).await;
}

#[tokio::test]
async fn parquet_abort_cancels_a_backpressured_multipart_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let _blocked = store.block_parts();

    let parquet_handle = prepared_handle(&storage, "tracking://bucket/output.parquet").await;
    let parquet = open_registered_parquet_sink(
        parquet_handle.clone(),
        batch.schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
            "--parquet-dictionary-column",
            "name:analyze",
        ],
    )
    .await;
    assert_abort_cleans_multipart(parquet, &parquet_handle, &store).await;
}

#[tokio::test]
async fn vortex_abort_cancels_a_backpressured_multipart_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let _blocked = store.block_parts();

    let vortex_handle = prepared_handle(&storage, "tracking://bucket/output.vortex").await;
    let vortex = open_registered_vortex_sink(
        vortex_handle.clone(),
        batch.schema(),
        &["--vortex-record-batch-size", "1"],
    )
    .await;
    assert_abort_cleans_multipart(vortex, &vortex_handle, &store).await;
}

#[tokio::test]
async fn arrow_drop_fallback_cancels_a_backpressured_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let _blocked = store.block_parts();

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/drop.arrow").await;
    let arrow = open_registered_arrow_sink(
        arrow_handle.clone(),
        batch.schema(),
        &["--arrow-record-batch-size", "1"],
    )
    .await;
    assert_drop_cleans_multipart(arrow, &arrow_handle, &store).await;
}

#[tokio::test]
async fn parquet_drop_fallback_cancels_a_backpressured_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let _blocked = store.block_parts();
    let parquet_handle = prepared_handle(&storage, "tracking://bucket/drop.parquet").await;
    let parquet = open_registered_parquet_sink(
        parquet_handle.clone(),
        batch.schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
            "--parquet-dictionary-column",
            "name:analyze",
        ],
    )
    .await;
    assert_drop_cleans_multipart(parquet, &parquet_handle, &store).await;
}

#[tokio::test]
async fn vortex_drop_fallback_cancels_a_backpressured_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let _blocked = store.block_parts();
    let vortex_handle = prepared_handle(&storage, "tracking://bucket/drop.vortex").await;
    let vortex = open_registered_vortex_sink(
        vortex_handle.clone(),
        batch.schema(),
        &["--vortex-record-batch-size", "1"],
    )
    .await;
    assert_drop_cleans_multipart(vortex, &vortex_handle, &store).await;
}

#[tokio::test]
async fn arrow_cancelled_finish_cleans_a_backpressured_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let schema = batch().schema();
    let schema_released = Arc::downgrade(&schema);
    let _blocked = store.block_parts();
    let handle = prepared_handle(&storage, "tracking://bucket/cancel-finish.arrow").await;
    let sink = open_registered_arrow_sink(
        handle.clone(),
        Arc::clone(&schema),
        &["--arrow-record-batch-size", "1"],
    )
    .await;
    drop(schema);

    assert_cancelled_finish_cleans_multipart(sink, &handle, &store).await;
    wait_for_resource_release(&schema_released, "cancelled Arrow finish retained its task").await;
}

#[tokio::test]
async fn parquet_cancelled_finish_cleans_pipeline_and_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let schema = batch().schema();
    let schema_released = Arc::downgrade(&schema);
    let _blocked = store.block_parts();
    let handle = prepared_handle(&storage, "tracking://bucket/cancel-finish.parquet").await;
    let sink = open_registered_parquet_sink(
        handle.clone(),
        Arc::clone(&schema),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
            "--parquet-ingestion-queue-size",
            "1",
            "--parquet-encoding-queue-size",
            "1",
            "--parquet-writing-queue-size",
            "1",
            "--parquet-dictionary-column",
            "name:analyze",
        ],
    )
    .await;
    drop(schema);

    assert_cancelled_finish_cleans_multipart(sink, &handle, &store).await;
    wait_for_resource_release(
        &schema_released,
        "cancelled Parquet finish retained its task tree",
    )
    .await;
}

#[tokio::test]
async fn vortex_cancelled_finish_cleans_a_backpressured_upload() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let schema = batch().schema();
    let schema_released = Arc::downgrade(&schema);
    let _blocked = store.block_parts();
    let handle = prepared_handle(&storage, "tracking://bucket/cancel-finish.vortex").await;
    let sink = open_registered_vortex_sink(
        handle.clone(),
        Arc::clone(&schema),
        &["--vortex-record-batch-size", "1"],
    )
    .await;
    drop(schema);

    assert_cancelled_finish_cleans_multipart(sink, &handle, &store).await;
    wait_for_resource_release(
        &schema_released,
        "cancelled Vortex finish retained its task",
    )
    .await;
}

#[tokio::test]
async fn format_aborts_report_multipart_cleanup_failures() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let _blocked = store.block_parts();

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/abort-error.arrow").await;
    let arrow = open_registered_arrow_sink(
        arrow_handle.clone(),
        batch.schema(),
        &["--arrow-record-batch-size", "1"],
    )
    .await;
    assert_abort_reports_cleanup_failure(arrow, &arrow_handle, &store).await;

    let parquet_handle = prepared_handle(&storage, "tracking://bucket/abort-error.parquet").await;
    let parquet = open_registered_parquet_sink(
        parquet_handle.clone(),
        batch.schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
        ],
    )
    .await;
    assert_abort_reports_cleanup_failure(parquet, &parquet_handle, &store).await;

    let vortex_handle = prepared_handle(&storage, "tracking://bucket/abort-error.vortex").await;
    let vortex = open_registered_vortex_sink(
        vortex_handle.clone(),
        batch.schema(),
        &["--vortex-record-batch-size", "1"],
    )
    .await;
    assert_abort_reports_cleanup_failure(vortex, &vortex_handle, &store).await;
}

#[tokio::test]
async fn parquet_upload_failure_cancels_all_pipeline_channels() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let handle = prepared_handle(&storage, "tracking://bucket/part-error.parquet").await;
    let mut sink = open_registered_parquet_sink(
        handle.clone(),
        batch().schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
            "--parquet-ingestion-queue-size",
            "1",
            "--parquet-encoding-queue-size",
            "1",
            "--parquet-writing-queue-size",
            "1",
            "--parquet-row-group-concurrency",
            "1",
            "--parquet-dictionary-column",
            "name:analyze",
        ],
    )
    .await;
    store.fail_next_part();

    let write_error = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Err(error) = sink.write_batch(batch()).await {
                break error;
            }
        }
    })
    .await
    .expect("Parquet ingestion remained blocked after the upload failed");
    assert!(
        write_error.to_string().contains("Parquet pipeline closed"),
        "{write_error:#}"
    );

    let abort_error = tokio::time::timeout(Duration::from_secs(5), sink.abort())
        .await
        .expect("Parquet cleanup remained blocked after the upload failed")
        .unwrap_err();
    assert!(
        format!("{abort_error:#}").contains("controlled part failure"),
        "{abort_error:#}"
    );
    assert_eq!(store.active_parts(), 0);
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn format_finish_failures_abort_multipart_uploads() {
    let _lock = controlled_upload_lock().await;
    let storage = controlled_upload_storage();
    let store = controlled_upload_store();
    let batch = batch();
    let failed_arrow_handle =
        prepared_handle(&storage, "tracking://bucket/failed-output.arrow").await;
    let failed_arrow = open_registered_arrow_sink(
        failed_arrow_handle.clone(),
        batch.schema(),
        &["--arrow-record-batch-size", "1"],
    )
    .await;
    assert_finish_failure_cleans_multipart(failed_arrow, &failed_arrow_handle, &store).await;

    let failed_parquet_handle =
        prepared_handle(&storage, "tracking://bucket/failed-output.parquet").await;
    let failed_parquet = open_registered_parquet_sink(
        failed_parquet_handle.clone(),
        batch.schema(),
        &[
            "--parquet-row-group-size",
            "1",
            "--parquet-buffer-size",
            "1B",
        ],
    )
    .await;
    assert_finish_failure_cleans_multipart(failed_parquet, &failed_parquet_handle, &store).await;

    let failed_vortex_handle =
        prepared_handle(&storage, "tracking://bucket/failed-output.vortex").await;
    let failed_vortex = open_registered_vortex_sink(
        failed_vortex_handle.clone(),
        batch.schema(),
        &["--vortex-record-batch-size", "1"],
    )
    .await;
    assert_finish_failure_cleans_multipart(failed_vortex, &failed_vortex_handle, &store).await;
}
