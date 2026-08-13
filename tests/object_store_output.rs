use std::{
    fmt, io,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{Int32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
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
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path as ObjectPath,
};
use silk_chiffon::sinks::{
    arrow::{ArrowSink, ArrowSinkOptions},
    data_sink::DataSink,
    parquet::{ParquetRuntimes, ParquetSink, ParquetSinkOptions},
    vortex::{VortexSink, VortexSinkOptions},
};
use silk_chiffon_core::{InputSources, Pipeline};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, OutputPreparation, StorageAccess, StorageBackend, StorageHandle,
    StorageRegistry, StorageSession,
};

static TRACKING_STORE: OnceLock<Arc<TrackingStore>> = OnceLock::new();
static TRACKING_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SOURCE_BATCH_LIMIT: usize = 1_000_000;

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

#[derive(Debug)]
struct TrackingStore {
    inner: InMemory,
    multipart_starts: AtomicUsize,
    active_parts: AtomicUsize,
    aborts: AtomicUsize,
    block_parts: AtomicBool,
    fail_next_abort: AtomicBool,
    fail_next_complete: AtomicBool,
    fail_next_part: AtomicBool,
    part_started: tokio::sync::Notify,
}

impl TrackingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            multipart_starts: AtomicUsize::new(0),
            active_parts: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            block_parts: AtomicBool::new(false),
            fail_next_abort: AtomicBool::new(false),
            fail_next_complete: AtomicBool::new(false),
            fail_next_part: AtomicBool::new(false),
            part_started: tokio::sync::Notify::new(),
        }
    }
}

impl fmt::Display for TrackingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TrackingStore")
    }
}

#[async_trait]
impl ObjectStore for TrackingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.multipart_starts.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TrackingMultipart {
            inner: self.inner.put_multipart_opts(location, options).await?,
            store: tracking_store(),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct TrackingMultipart {
    inner: Box<dyn MultipartUpload>,
    store: Arc<TrackingStore>,
}

struct ActivePart(Arc<TrackingStore>);

impl Drop for ActivePart {
    fn drop(&mut self) {
        self.0.active_parts.fetch_sub(1, Ordering::SeqCst);
    }
}

struct BlockParts(Arc<TrackingStore>);

impl BlockParts {
    fn new(store: Arc<TrackingStore>) -> Self {
        store.block_parts.store(true, Ordering::SeqCst);
        Self(store)
    }
}

impl Drop for BlockParts {
    fn drop(&mut self) {
        self.0.block_parts.store(false, Ordering::SeqCst);
        self.0.part_started.notify_waiters();
    }
}

#[async_trait]
impl MultipartUpload for TrackingMultipart {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        let part = self.inner.put_part(payload);
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            store.active_parts.fetch_add(1, Ordering::SeqCst);
            store.part_started.notify_waiters();
            let _active = ActivePart(Arc::clone(&store));
            while store.block_parts.load(Ordering::SeqCst) {
                store.part_started.notified().await;
            }
            if store.fail_next_part.swap(false, Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "tracking",
                    source: Box::new(io::Error::other("controlled part failure")),
                });
            }
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.store.fail_next_complete.swap(false, Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "tracking",
                source: Box::new(io::Error::other("controlled complete failure")),
            });
        }
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.store.aborts.fetch_add(1, Ordering::SeqCst);
        self.inner.abort().await?;
        if self.store.fail_next_abort.swap(false, Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "tracking",
                source: Box::new(io::Error::other("controlled abort failure")),
            });
        }
        Ok(())
    }
}

fn tracking_store() -> Arc<TrackingStore> {
    Arc::clone(TRACKING_STORE.get_or_init(|| Arc::new(TrackingStore::new())))
}

fn memory_store(
    _store_url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(InMemory::new()))
}

fn tracked_store(
    _store_url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(tracking_store())
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

fn tracking_storage() -> StorageSession {
    let backend = StorageBackend::without_args()
        .name("tracking")
        .schemes(["tracking"])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(tracked_store)
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
            "1",
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
    let active_before = store.active_parts.load(Ordering::SeqCst);
    for _ in 0..64 {
        if store.active_parts.load(Ordering::SeqCst) > active_before {
            break;
        }

        let write = sink.write_batch(batch());
        tokio::pin!(write);
        tokio::select! {
            result = &mut write => result.unwrap(),
            result = tokio::time::timeout(Duration::from_secs(5), async {
                while store.active_parts.load(Ordering::SeqCst) == active_before {
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
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.active_parts.load(Ordering::SeqCst) == active_before {
            store.part_started.notified().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "format did not start its multipart upload for {}",
            handle.url()
        )
    });
}

async fn wait_for_multipart_cleanup(store: &TrackingStore, active_before: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.active_parts.load(Ordering::SeqCst) != active_before {
            tokio::task::yield_now().await;
        }
    })
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
    let starts_before = store.multipart_starts.load(Ordering::SeqCst);
    let aborts_before = store.aborts.load(Ordering::SeqCst);
    let active_before = store.active_parts.load(Ordering::SeqCst);
    drive_to_active_part(sink.as_mut(), handle, store).await;

    tokio::time::timeout(Duration::from_secs(5), sink.abort())
        .await
        .unwrap_or_else(|_| panic!("format abort timed out for {}", handle.url()))
        .unwrap();

    assert_eq!(
        store.aborts.load(Ordering::SeqCst) - aborts_before,
        store.multipart_starts.load(Ordering::SeqCst) - starts_before
    );
    wait_for_multipart_cleanup(store, active_before).await;
    assert_eq!(store.active_parts.load(Ordering::SeqCst), active_before);
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
    let active_before = store.active_parts.load(Ordering::SeqCst);
    drive_to_active_part(sink.as_mut(), handle, store).await;
    store.fail_next_abort.store(true, Ordering::SeqCst);

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
    let aborts_before = store.aborts.load(Ordering::SeqCst);
    let active_before = store.active_parts.load(Ordering::SeqCst);
    drive_to_active_part(sink.as_mut(), handle, store).await;
    drop(sink);

    tokio::time::timeout(Duration::from_secs(5), async {
        while store.aborts.load(Ordering::SeqCst) == aborts_before {
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
    let aborts_before = store.aborts.load(Ordering::SeqCst);
    let active_before = store.active_parts.load(Ordering::SeqCst);
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
        while store.aborts.load(Ordering::SeqCst) == aborts_before {
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
    let aborts_before = store.aborts.load(Ordering::SeqCst);
    sink.write_batch(batch()).await.unwrap();
    store.fail_next_complete.store(true, Ordering::SeqCst);

    let error = tokio::time::timeout(Duration::from_secs(5), sink.finish())
        .await
        .unwrap_or_else(|_| panic!("format finish timed out for {}", handle.url()))
        .unwrap_err();

    assert!(
        error.to_string().contains("controlled complete failure"),
        "{error:#}"
    );
    assert_eq!(store.aborts.load(Ordering::SeqCst), aborts_before + 1);
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn arrow_sink_writes_a_memory_object() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/output.arrow").await;
    let batch = batch();
    let mut sink =
        ArrowSink::create(handle.clone(), &batch.schema(), ArrowSinkOptions::new()).unwrap();

    sink.write_batch(batch).await.unwrap();
    let completion = Box::new(sink).finish().await.unwrap();
    assert_durable(completion, &handle).await;
}

#[tokio::test]
async fn parquet_sink_writes_a_memory_object() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/output.parquet").await;
    let batch = batch();
    let mut sink = ParquetSink::create(
        handle.clone(),
        &batch.schema(),
        &ParquetSinkOptions::new(),
        Arc::new(ParquetRuntimes::try_new(2, 1).unwrap()),
    )
    .unwrap();

    sink.write_batch(batch).await.unwrap();
    let completion = Box::new(sink).finish().await.unwrap();
    assert_durable(completion, &handle).await;
}

#[tokio::test]
async fn vortex_sink_writes_a_memory_object() {
    let storage = storage();
    let handle = prepared_handle(&storage, "memory://bucket/output.vortex").await;
    let batch = batch();
    let mut sink =
        VortexSink::create(handle.clone(), &batch.schema(), VortexSinkOptions::new()).unwrap();

    sink.write_batch(batch).await.unwrap();
    let completion = Box::new(sink).finish().await.unwrap();
    assert_durable(completion, &handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_failure_cancels_every_datafusion_source_task() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let handle = prepared_handle(&storage, "tracking://bucket/source-cancellation.arrow").await;
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
    let mut sink = ArrowSink::create(
        handle.clone(),
        &batch.schema(),
        ArrowSinkOptions::new()
            .with_record_batch_size(1)
            .with_queue_depth(1),
    )
    .unwrap();
    let blocked_parts = BlockParts::new(Arc::clone(&store));
    let write_error = {
        let write = sink.write_stream(stream);
        tokio::pin!(write);

        tokio::select! {
            () = wait_for_sources_started(&source_states) => {}
            result = &mut write => panic!("sink stopped before every source started: {result:?}"),
        }
        store.fail_next_part.store(true, Ordering::SeqCst);
        drop(blocked_parts);
        tokio::time::timeout(Duration::from_secs(5), &mut write)
            .await
            .expect("sink failure did not stop stream consumption")
            .unwrap_err()
    };
    assert!(write_error.to_string().contains("writer task died"));
    wait_for_sources_stopped(&source_states).await;
    for state in &source_states {
        assert!(state.started.load(Ordering::SeqCst));
        assert!(state.cancelled.load(Ordering::SeqCst));
        assert!(state.batches_sent.load(Ordering::SeqCst) < SOURCE_BATCH_LIMIT);
    }

    let cleanup_error = tokio::time::timeout(Duration::from_secs(5), Box::new(sink).abort())
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
    let mut sink = ArrowSink::create(
        handle.clone(),
        &batch.schema(),
        ArrowSinkOptions::new()
            .with_record_batch_size(1)
            .with_queue_depth(1),
    )
    .unwrap();
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
    Box::new(sink).abort().await.unwrap();
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
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let _blocked = BlockParts::new(Arc::clone(&store));

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/output.arrow").await;
    let arrow = ArrowSink::create(
        arrow_handle.clone(),
        &batch.schema(),
        ArrowSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_abort_cleans_multipart(Box::new(arrow), &arrow_handle, &store).await;
}

#[tokio::test]
async fn parquet_abort_cancels_a_backpressured_multipart_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let _blocked = BlockParts::new(Arc::clone(&store));

    let parquet_handle = prepared_handle(&storage, "tracking://bucket/output.parquet").await;
    let runtimes = Arc::new(ParquetRuntimes::try_new(2, 1).unwrap());
    let runtimes_released = Arc::downgrade(&runtimes);
    let parquet = ParquetSink::create(
        parquet_handle.clone(),
        &batch.schema(),
        &ParquetSinkOptions::new()
            .with_max_row_group_size(1)
            .with_buffer_size(1)
            .with_column_dictionary_analyze(vec!["name".to_string()]),
        runtimes,
    )
    .unwrap();
    assert_abort_cleans_multipart(Box::new(parquet), &parquet_handle, &store).await;
    assert!(runtimes_released.upgrade().is_none());
}

#[tokio::test]
async fn vortex_abort_cancels_a_backpressured_multipart_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let _blocked = BlockParts::new(Arc::clone(&store));

    let vortex_handle = prepared_handle(&storage, "tracking://bucket/output.vortex").await;
    let vortex = VortexSink::create(
        vortex_handle.clone(),
        &batch.schema(),
        VortexSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_abort_cleans_multipart(Box::new(vortex), &vortex_handle, &store).await;
}

#[tokio::test]
async fn arrow_drop_fallback_cancels_a_backpressured_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let _blocked = BlockParts::new(Arc::clone(&store));

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/drop.arrow").await;
    let arrow = ArrowSink::create(
        arrow_handle.clone(),
        &batch.schema(),
        ArrowSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_drop_cleans_multipart(Box::new(arrow), &arrow_handle, &store).await;
}

#[tokio::test]
async fn parquet_drop_fallback_cancels_a_backpressured_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let _blocked = BlockParts::new(Arc::clone(&store));
    let parquet_handle = prepared_handle(&storage, "tracking://bucket/drop.parquet").await;
    let runtimes = Arc::new(ParquetRuntimes::try_new(2, 1).unwrap());
    let runtimes_released = Arc::downgrade(&runtimes);
    let parquet = ParquetSink::create(
        parquet_handle.clone(),
        &batch.schema(),
        &ParquetSinkOptions::new()
            .with_max_row_group_size(1)
            .with_buffer_size(1)
            .with_column_dictionary_analyze(vec!["name".to_string()]),
        runtimes,
    )
    .unwrap();
    assert_drop_cleans_multipart(Box::new(parquet), &parquet_handle, &store).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtimes_released.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Parquet tasks retained their dedicated runtimes after drop");
}

#[tokio::test]
async fn vortex_drop_fallback_cancels_a_backpressured_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let _blocked = BlockParts::new(Arc::clone(&store));
    let vortex_handle = prepared_handle(&storage, "tracking://bucket/drop.vortex").await;
    let vortex = VortexSink::create(
        vortex_handle.clone(),
        &batch.schema(),
        VortexSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_drop_cleans_multipart(Box::new(vortex), &vortex_handle, &store).await;
}

#[tokio::test]
async fn arrow_cancelled_finish_cleans_a_backpressured_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let schema = batch().schema();
    let schema_released = Arc::downgrade(&schema);
    let _blocked = BlockParts::new(Arc::clone(&store));
    let handle = prepared_handle(&storage, "tracking://bucket/cancel-finish.arrow").await;
    let sink = ArrowSink::create(
        handle.clone(),
        &schema,
        ArrowSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    drop(schema);

    assert_cancelled_finish_cleans_multipart(Box::new(sink), &handle, &store).await;
    wait_for_resource_release(&schema_released, "cancelled Arrow finish retained its task").await;
}

#[tokio::test]
async fn parquet_cancelled_finish_cleans_pipeline_and_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let schema = batch().schema();
    let schema_released = Arc::downgrade(&schema);
    let _blocked = BlockParts::new(Arc::clone(&store));
    let handle = prepared_handle(&storage, "tracking://bucket/cancel-finish.parquet").await;
    let runtimes = Arc::new(ParquetRuntimes::try_new(2, 1).unwrap());
    let runtimes_released = Arc::downgrade(&runtimes);
    let sink = ParquetSink::create(
        handle.clone(),
        &schema,
        &ParquetSinkOptions::new()
            .with_max_row_group_size(1)
            .with_buffer_size(1)
            .with_ingestion_queue_size(1)
            .with_encoding_queue_size(1)
            .with_writing_queue_size(1)
            .with_column_dictionary_analyze(vec!["name".to_string()]),
        runtimes,
    )
    .unwrap();
    drop(schema);

    assert_cancelled_finish_cleans_multipart(Box::new(sink), &handle, &store).await;
    wait_for_resource_release(
        &schema_released,
        "cancelled Parquet finish retained its task tree",
    )
    .await;
    wait_for_resource_release(
        &runtimes_released,
        "cancelled Parquet finish retained its dedicated runtimes",
    )
    .await;
}

#[tokio::test]
async fn vortex_cancelled_finish_cleans_a_backpressured_upload() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let schema = batch().schema();
    let schema_released = Arc::downgrade(&schema);
    let _blocked = BlockParts::new(Arc::clone(&store));
    let handle = prepared_handle(&storage, "tracking://bucket/cancel-finish.vortex").await;
    let sink = VortexSink::create(
        handle.clone(),
        &schema,
        VortexSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    drop(schema);

    assert_cancelled_finish_cleans_multipart(Box::new(sink), &handle, &store).await;
    wait_for_resource_release(
        &schema_released,
        "cancelled Vortex finish retained its task",
    )
    .await;
}

#[tokio::test]
async fn format_aborts_report_multipart_cleanup_failures() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let _blocked = BlockParts::new(Arc::clone(&store));

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/abort-error.arrow").await;
    let arrow = ArrowSink::create(
        arrow_handle.clone(),
        &batch.schema(),
        ArrowSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_abort_reports_cleanup_failure(Box::new(arrow), &arrow_handle, &store).await;

    let parquet_handle = prepared_handle(&storage, "tracking://bucket/abort-error.parquet").await;
    let parquet = ParquetSink::create(
        parquet_handle.clone(),
        &batch.schema(),
        &ParquetSinkOptions::new()
            .with_max_row_group_size(1)
            .with_buffer_size(1),
        Arc::new(ParquetRuntimes::try_new(2, 1).unwrap()),
    )
    .unwrap();
    assert_abort_reports_cleanup_failure(Box::new(parquet), &parquet_handle, &store).await;

    let vortex_handle = prepared_handle(&storage, "tracking://bucket/abort-error.vortex").await;
    let vortex = VortexSink::create(
        vortex_handle.clone(),
        &batch.schema(),
        VortexSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_abort_reports_cleanup_failure(Box::new(vortex), &vortex_handle, &store).await;
}

#[tokio::test]
async fn parquet_upload_failure_cancels_all_pipeline_channels() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let handle = prepared_handle(&storage, "tracking://bucket/part-error.parquet").await;
    let runtimes = Arc::new(ParquetRuntimes::try_new(1, 1).unwrap());
    let runtimes_released = Arc::downgrade(&runtimes);
    let mut sink = ParquetSink::create(
        handle.clone(),
        &batch().schema(),
        &ParquetSinkOptions::new()
            .with_max_row_group_size(1)
            .with_buffer_size(1)
            .with_ingestion_queue_size(1)
            .with_encoding_queue_size(1)
            .with_writing_queue_size(1)
            .with_max_row_group_concurrency(1)
            .with_column_dictionary_analyze(vec!["name".to_string()]),
        runtimes,
    )
    .unwrap();
    store.fail_next_part.store(true, Ordering::SeqCst);

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
        write_error.to_string().contains("writer pipeline closed"),
        "{write_error:#}"
    );

    let abort_error = tokio::time::timeout(Duration::from_secs(5), Box::new(sink).abort())
        .await
        .expect("Parquet cleanup remained blocked after the upload failed")
        .unwrap_err();
    assert!(
        format!("{abort_error:#}").contains("controlled part failure"),
        "{abort_error:#}"
    );
    assert_eq!(store.active_parts.load(Ordering::SeqCst), 0);
    assert!(runtimes_released.upgrade().is_none());
    assert!(matches!(
        store.head(handle.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn format_finish_failures_abort_multipart_uploads() {
    let _lock = TRACKING_TEST_LOCK.lock().await;
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();
    let failed_arrow_handle =
        prepared_handle(&storage, "tracking://bucket/failed-output.arrow").await;
    let failed_arrow = ArrowSink::create(
        failed_arrow_handle.clone(),
        &batch.schema(),
        ArrowSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_finish_failure_cleans_multipart(Box::new(failed_arrow), &failed_arrow_handle, &store)
        .await;

    let failed_parquet_handle =
        prepared_handle(&storage, "tracking://bucket/failed-output.parquet").await;
    let failed_parquet = ParquetSink::create(
        failed_parquet_handle.clone(),
        &batch.schema(),
        &ParquetSinkOptions::new()
            .with_max_row_group_size(1)
            .with_buffer_size(1),
        Arc::new(ParquetRuntimes::try_new(2, 1).unwrap()),
    )
    .unwrap();
    assert_finish_failure_cleans_multipart(
        Box::new(failed_parquet),
        &failed_parquet_handle,
        &store,
    )
    .await;

    let failed_vortex_handle =
        prepared_handle(&storage, "tracking://bucket/failed-output.vortex").await;
    let failed_vortex = VortexSink::create(
        failed_vortex_handle.clone(),
        &batch.schema(),
        VortexSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_finish_failure_cleans_multipart(Box::new(failed_vortex), &failed_vortex_handle, &store)
        .await;
}
