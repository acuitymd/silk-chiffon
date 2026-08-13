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
use futures::stream::BoxStream;
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
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, OutputPreparation, StorageAccess, StorageBackend, StorageHandle,
    StorageRegistry, StorageSession,
};

static TRACKING_STORE: OnceLock<Arc<TrackingStore>> = OnceLock::new();

#[derive(Debug)]
struct TrackingStore {
    inner: InMemory,
    multipart_starts: AtomicUsize,
    aborts: AtomicUsize,
    fail_next_complete: AtomicBool,
}

impl TrackingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            multipart_starts: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            fail_next_complete: AtomicBool::new(false),
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

#[async_trait]
impl MultipartUpload for TrackingMultipart {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        self.inner.put_part(payload)
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
        self.inner.abort().await
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

async fn assert_abort_cleans_multipart(
    mut sink: Box<dyn DataSink>,
    handle: &StorageHandle,
    store: &TrackingStore,
    expect_multipart: bool,
) {
    let starts_before = store.multipart_starts.load(Ordering::SeqCst);
    let aborts_before = store.aborts.load(Ordering::SeqCst);
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(5), sink.write_batch(batch()))
            .await
            .unwrap_or_else(|_| panic!("format write timed out for {}", handle.url()))
            .unwrap();
    }
    if expect_multipart {
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.multipart_starts.load(Ordering::SeqCst) == starts_before {
                tokio::task::yield_now().await;
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

    tokio::time::timeout(Duration::from_secs(5), sink.abort())
        .await
        .unwrap_or_else(|_| panic!("format abort timed out for {}", handle.url()))
        .unwrap();

    assert_eq!(
        store.aborts.load(Ordering::SeqCst) - aborts_before,
        store.multipart_starts.load(Ordering::SeqCst) - starts_before
    );
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

#[tokio::test]
async fn format_terminal_paths_clean_fake_store_multipart_uploads() {
    let storage = tracking_storage();
    let store = tracking_store();
    let batch = batch();

    let arrow_handle = prepared_handle(&storage, "tracking://bucket/output.arrow").await;
    let arrow = ArrowSink::create(
        arrow_handle.clone(),
        &batch.schema(),
        ArrowSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_abort_cleans_multipart(Box::new(arrow), &arrow_handle, &store, true).await;

    let parquet_handle = prepared_handle(&storage, "tracking://bucket/output.parquet").await;
    let parquet = ParquetSink::create(
        parquet_handle.clone(),
        &batch.schema(),
        &ParquetSinkOptions::new()
            .with_max_row_group_size(1)
            .with_buffer_size(1),
        Arc::new(ParquetRuntimes::try_new(2, 1).unwrap()),
    )
    .unwrap();
    assert_abort_cleans_multipart(Box::new(parquet), &parquet_handle, &store, false).await;

    let vortex_handle = prepared_handle(&storage, "tracking://bucket/output.vortex").await;
    let vortex = VortexSink::create(
        vortex_handle.clone(),
        &batch.schema(),
        VortexSinkOptions::new().with_record_batch_size(1),
    )
    .unwrap();
    assert_abort_cleans_multipart(Box::new(vortex), &vortex_handle, &store, true).await;

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
