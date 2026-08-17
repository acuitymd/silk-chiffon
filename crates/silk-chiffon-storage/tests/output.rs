use std::{
    collections::HashMap,
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use clap::Command;
use futures::{SinkExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path as ObjectPath,
};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, ObjectUpload, ObjectUploadTask, OutputPreparation,
    StorageAccess, StorageBackend, StorageHandle, StorageRegistry,
};

static MEMORY_STORE: OnceLock<Arc<InMemory>> = OnceLock::new();
static CONTROLLED_STORES: OnceLock<Mutex<HashMap<String, Arc<ControlledStore>>>> = OnceLock::new();
static NEXT_TARGET: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct UploadControl {
    active_parts: AtomicUsize,
    maximum_active_parts: AtomicUsize,
    parts_created: AtomicUsize,
    puts: AtomicUsize,
    multipart_starts: AtomicUsize,
    completes: AtomicUsize,
    aborts: AtomicUsize,
    block_multipart_start: AtomicBool,
    block_parts: AtomicBool,
    fail_next_put: AtomicBool,
    fail_next_multipart_start: AtomicBool,
    fail_next_part: AtomicBool,
    fail_next_complete: AtomicBool,
    fail_next_abort: AtomicBool,
    part_started: tokio::sync::Notify,
    multipart_start_release: tokio::sync::Semaphore,
    part_release: tokio::sync::Semaphore,
}

impl UploadControl {
    fn new() -> Self {
        Self {
            active_parts: AtomicUsize::new(0),
            maximum_active_parts: AtomicUsize::new(0),
            parts_created: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            multipart_starts: AtomicUsize::new(0),
            completes: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            block_multipart_start: AtomicBool::new(false),
            block_parts: AtomicBool::new(false),
            fail_next_put: AtomicBool::new(false),
            fail_next_multipart_start: AtomicBool::new(false),
            fail_next_part: AtomicBool::new(false),
            fail_next_complete: AtomicBool::new(false),
            fail_next_abort: AtomicBool::new(false),
            part_started: tokio::sync::Notify::new(),
            multipart_start_release: tokio::sync::Semaphore::new(0),
            part_release: tokio::sync::Semaphore::new(0),
        }
    }
}

#[derive(Debug)]
struct ControlledStore {
    inner: InMemory,
    control: Arc<UploadControl>,
}

impl fmt::Display for ControlledStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlledStore")
    }
}

#[async_trait]
impl ObjectStore for ControlledStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.control.puts.fetch_add(1, Ordering::SeqCst);
        if self.control.fail_next_put.swap(false, Ordering::SeqCst) {
            return Err(controlled_error("controlled put failure"));
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.control.multipart_starts.fetch_add(1, Ordering::SeqCst);
        if self
            .control
            .fail_next_multipart_start
            .swap(false, Ordering::SeqCst)
        {
            return Err(controlled_error("controlled multipart start failure"));
        }
        let inner = self.inner.put_multipart_opts(location, options).await?;
        if self.control.block_multipart_start.load(Ordering::SeqCst) {
            self.control
                .multipart_start_release
                .acquire()
                .await
                .unwrap()
                .forget();
        }
        Ok(Box::new(ControlledMultipart {
            inner,
            control: Arc::clone(&self.control),
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
struct ControlledMultipart {
    inner: Box<dyn MultipartUpload>,
    control: Arc<UploadControl>,
}

struct ActivePart(Arc<UploadControl>);

impl Drop for ActivePart {
    fn drop(&mut self) {
        self.0.active_parts.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl MultipartUpload for ControlledMultipart {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        self.control.parts_created.fetch_add(1, Ordering::SeqCst);
        let part = self.inner.put_part(payload);
        let control = Arc::clone(&self.control);
        Box::pin(async move {
            let active = control.active_parts.fetch_add(1, Ordering::SeqCst) + 1;
            control
                .maximum_active_parts
                .fetch_max(active, Ordering::SeqCst);
            control.part_started.notify_waiters();
            let _active = ActivePart(Arc::clone(&control));
            if control.block_parts.load(Ordering::SeqCst) {
                control.part_release.acquire().await.unwrap().forget();
            }
            if control.fail_next_part.swap(false, Ordering::SeqCst) {
                return Err(controlled_error("controlled part failure"));
            }
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.control.completes.fetch_add(1, Ordering::SeqCst);
        if self
            .control
            .fail_next_complete
            .swap(false, Ordering::SeqCst)
        {
            return Err(controlled_error("controlled complete failure"));
        }
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.control.aborts.fetch_add(1, Ordering::SeqCst);
        self.inner.abort().await?;
        if self.control.fail_next_abort.swap(false, Ordering::SeqCst) {
            return Err(controlled_error("controlled abort failure"));
        }
        Ok(())
    }
}

fn controlled_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "controlled",
        source: Box::new(io::Error::other(message)),
    }
}

fn memory_store() -> Arc<InMemory> {
    Arc::clone(MEMORY_STORE.get_or_init(|| Arc::new(InMemory::new())))
}

fn create_memory_store(
    _store_url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(memory_store())
}

fn controlled_store(store_url: &url::Url) -> Arc<ControlledStore> {
    let stores = CONTROLLED_STORES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut stores = stores.lock().unwrap();
    Arc::clone(stores.entry(store_url.to_string()).or_insert_with(|| {
        Arc::new(ControlledStore {
            inner: InMemory::new(),
            control: Arc::new(UploadControl::new()),
        })
    }))
}

fn create_controlled_store(
    store_url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(controlled_store(store_url))
}

fn prepare_memory_target<'a>(
    _handle: &'a StorageHandle,
    _preparation: &'a OutputPreparation,
    _settings: &'a (),
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async { Ok(()) })
}

fn reject_output_target<'a>(
    _handle: &'a StorageHandle,
    _preparation: &'a OutputPreparation,
    _settings: &'a (),
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async { anyhow::bail!("controlled preparation failure") })
}

fn registry() -> StorageRegistry {
    StorageRegistry::builder()
        .register(
            StorageBackend::without_args()
                .name("memory")
                .schemes(["memory"])
                .access(StorageAccess::ReadWrite)
                .allow_any_location()
                .object_store_creator(create_memory_store)
                .prepare_output_target(prepare_memory_target)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn controlled_registry() -> StorageRegistry {
    StorageRegistry::builder()
        .register(
            StorageBackend::without_args()
                .name("controlled")
                .schemes(["controlled"])
                .access(StorageAccess::ReadWrite)
                .allow_any_location()
                .object_store_creator(create_controlled_store)
                .prepare_output_target(prepare_memory_target)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn rejecting_registry() -> StorageRegistry {
    StorageRegistry::builder()
        .register(
            StorageBackend::without_args()
                .name("rejecting")
                .schemes(["rejecting"])
                .access(StorageAccess::ReadWrite)
                .allow_any_location()
                .object_store_creator(create_memory_store)
                .prepare_output_target(reject_output_target)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn unique_location(label: &str) -> LocationInput {
    let ordinal = NEXT_TARGET.fetch_add(1, Ordering::Relaxed);
    LocationInput::parse(format!("memory://bucket/{label}-{ordinal}.bin")).unwrap()
}

fn session(arguments: &[&str]) -> silk_chiffon_storage::StorageSession {
    let registry = registry();
    let command = registry.augment_args(Command::new("output-test"));
    let matches = command.try_get_matches_from(arguments).unwrap();
    registry.create_session(&matches).unwrap()
}

fn controlled_session(arguments: &[&str]) -> silk_chiffon_storage::StorageSession {
    let registry = controlled_registry();
    let command = registry.augment_args(Command::new("output-test"));
    let matches = command.try_get_matches_from(arguments).unwrap();
    registry.create_session(&matches).unwrap()
}

async fn controlled_upload(
    storage: &silk_chiffon_storage::StorageSession,
    root: &str,
    label: &str,
) -> (ObjectUpload, Arc<ControlledStore>, ObjectPath) {
    let location = LocationInput::parse(format!("controlled://{root}/{label}.bin")).unwrap();
    let handle = storage
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    let store = controlled_store(handle.store_url());
    let path = handle.object_path().clone();
    (ObjectUpload::new(handle), store, path)
}

#[tokio::test]
async fn upload_task_finishes_the_producer_before_completing_the_object() {
    let storage = controlled_session(&["output-test"]);
    let root = unique_controlled_root("upload-task-finish");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    let mut writer = upload.writer().unwrap();
    let task = ObjectUploadTask::spawn("test producer", upload, move |_| {
        tokio::spawn(async move {
            writer.send(Bytes::from_static(b"complete")).await.unwrap();
            Ok(7_u64)
        })
    });

    let (value, _) = task.finish().await.unwrap();

    assert_eq!(value, 7);
    assert_eq!(
        store.inner.get(&path).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

#[tokio::test]
async fn upload_task_cancels_its_producer_and_aborts_the_upload() {
    let storage = controlled_session(&["output-test", "--object-store-upload-part-size", "8"]);
    let root = unique_controlled_root("upload-task-abort");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    store.control.block_parts.store(true, Ordering::SeqCst);
    let control = Arc::clone(&store.control);
    let mut writer = upload.writer().unwrap();
    let task = ObjectUploadTask::spawn("test producer", upload, move |cancellation| {
        tokio::spawn(async move {
            writer.send(Bytes::from_static(b"12345678")).await.unwrap();
            cancellation.cancelled().await;
            Err::<(), _>(anyhow::anyhow!("producer observed cancellation"))
        })
    });

    wait_for_active_parts(&control, 1).await;
    task.abort().await.unwrap();

    assert!(store.inner.head(&path).await.is_err());
    assert_eq!(store.control.aborts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn upload_task_preserves_producer_and_cleanup_failures() {
    let storage = controlled_session(&["output-test", "--object-store-upload-part-size", "8"]);
    let root = unique_controlled_root("upload-task-producer-error");
    let (mut upload, store, _path) = controlled_upload(&storage, &root, "output").await;
    store.control.block_parts.store(true, Ordering::SeqCst);
    let control = Arc::clone(&store.control);
    let release = Arc::new(tokio::sync::Notify::new());
    let producer_release = Arc::clone(&release);
    let mut writer = upload.writer().unwrap();
    let task = ObjectUploadTask::spawn("test producer", upload, move |_| {
        tokio::spawn(async move {
            writer.send(Bytes::from_static(b"12345678")).await.unwrap();
            producer_release.notified().await;
            Err::<(), _>(anyhow::anyhow!("controlled producer failure"))
        })
    });

    wait_for_active_parts(&control, 1).await;
    control.fail_next_abort.store(true, Ordering::SeqCst);
    release.notify_one();
    let error = task.finish().await.unwrap_err();
    let message = format!("{error:#}");

    assert!(message.contains("controlled producer failure"), "{message}");
    assert!(message.contains("cleanup also failed"), "{message}");
    assert!(message.contains("controlled abort failure"), "{message}");
    assert_eq!(control.active_parts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn upload_task_aborts_the_upload_when_its_producer_panics() {
    let storage = controlled_session(&["output-test", "--object-store-upload-part-size", "8"]);
    let root = unique_controlled_root("upload-task-panic");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    store.control.block_parts.store(true, Ordering::SeqCst);
    let control = Arc::clone(&store.control);
    let release = Arc::new(tokio::sync::Notify::new());
    let producer_release = Arc::clone(&release);
    let mut writer = upload.writer().unwrap();
    let task = ObjectUploadTask::spawn("test producer", upload, move |_| {
        tokio::spawn(async move {
            writer.send(Bytes::from_static(b"12345678")).await.unwrap();
            producer_release.notified().await;
            panic!("controlled producer panic");
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        })
    });

    wait_for_active_parts(&control, 1).await;
    release.notify_one();
    let error = task.finish().await.unwrap_err();

    assert!(
        format!("{error:#}").contains("test producer task panicked"),
        "{error:#}"
    );
    assert_eq!(control.aborts.load(Ordering::SeqCst), 1);
    assert_eq!(control.active_parts.load(Ordering::SeqCst), 0);
    assert!(store.inner.head(&path).await.is_err());
}

#[tokio::test]
async fn dropping_upload_task_requests_producer_and_upload_cleanup() {
    let storage = controlled_session(&["output-test", "--object-store-upload-part-size", "8"]);
    let root = unique_controlled_root("upload-task-drop");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    store.control.block_parts.store(true, Ordering::SeqCst);
    let control = Arc::clone(&store.control);
    let producer_stopped = Arc::new(AtomicBool::new(false));
    let producer_state = Arc::clone(&producer_stopped);
    let mut writer = upload.writer().unwrap();
    let task = ObjectUploadTask::spawn("test producer", upload, move |cancellation| {
        tokio::spawn(async move {
            writer.send(Bytes::from_static(b"12345678")).await.unwrap();
            cancellation.cancelled().await;
            producer_state.store(true, Ordering::SeqCst);
            Ok(())
        })
    });

    wait_for_active_parts(&control, 1).await;
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !producer_stopped.load(Ordering::SeqCst)
            || control.aborts.load(Ordering::SeqCst) == 0
            || control.active_parts.load(Ordering::SeqCst) != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the owner did not settle producer and upload cleanup");

    assert!(store.inner.head(&path).await.is_err());
}

fn unique_controlled_root(label: &str) -> String {
    let ordinal = NEXT_TARGET.fetch_add(1, Ordering::Relaxed);
    format!("{label}-{ordinal}")
}

async fn wait_for_active_parts(control: &UploadControl, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while control.active_parts.load(Ordering::SeqCst) < expected {
            control.part_started.notified().await;
        }
    })
    .await
    .expect("multipart requests did not start");
}

async fn wait_for_multipart_starts(control: &UploadControl, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while control.multipart_starts.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("multipart upload did not start");
}

#[test]
fn upload_settings_have_approved_defaults_and_accept_smaller_values() {
    let default_session = session(&["output-test"]);
    assert_eq!(
        default_session.object_upload_settings().part_size().get(),
        10 * 1024 * 1024
    );
    assert_eq!(
        default_session
            .object_upload_settings()
            .max_in_flight_parts()
            .get(),
        8
    );

    let smaller = session(&[
        "output-test",
        "--object-store-upload-part-size",
        "16",
        "--object-store-max-in-flight-parts",
        "2",
    ]);
    assert_eq!(smaller.object_upload_settings().part_size().get(), 16);
    assert_eq!(
        smaller.object_upload_settings().max_in_flight_parts().get(),
        2
    );
}

#[tokio::test]
async fn prepared_targets_are_claimed_across_session_clones() {
    let storage = session(&["output-test"]);
    let clone = storage.clone();
    let location = unique_location("claim");

    storage
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    let error = clone
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("already claimed by this storage session")
    );
}

#[tokio::test]
async fn output_claims_use_store_and_object_identity_instead_of_the_raw_url() {
    let storage = session(&["output-test"]);
    let first = LocationInput::parse("memory://bucket/shared.bin?version=one").unwrap();
    let alias = LocationInput::parse("memory://bucket/shared.bin?version=two").unwrap();
    let other_root = LocationInput::parse("memory://other/shared.bin?version=two").unwrap();

    storage
        .prepare_output_target(
            &first,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    let error = storage
        .prepare_output_target(
            &alias,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already claimed"));
    storage
        .prepare_output_target(
            &other_root,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_backend_preparation_does_not_release_the_session_claim() {
    let registry = rejecting_registry();
    let command = registry.augment_args(Command::new("output-test"));
    let matches = command.try_get_matches_from(["output-test"]).unwrap();
    let storage = registry.create_session(&matches).unwrap();
    let location = LocationInput::parse("rejecting://bucket/output.bin").unwrap();

    let error = storage
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("controlled preparation failure"));

    let error = storage
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already claimed"));
}

#[tokio::test]
async fn external_existence_is_advisory_and_separate_from_session_claims() {
    let location = unique_location("existing");
    let first_session = session(&["output-test"]);
    let handle = first_session
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    handle
        .object_store()
        .put(handle.object_path(), Bytes::from_static(b"existing").into())
        .await
        .unwrap();

    let rejecting_session = session(&["output-test"]);
    let error = rejecting_session
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::RejectIfObserved, false),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("output target already exists"));

    let allowing_session = session(&["output-test"]);
    allowing_session
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn object_upload_completes_small_puts_and_aborts_unfinished_output() {
    let storage = session(&[
        "output-test",
        "--object-store-upload-part-size",
        "16",
        "--object-store-max-in-flight-parts",
        "2",
    ]);

    let completed_location = unique_location("complete");
    let completed_handle = storage
        .prepare_output_target(
            &completed_location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    let completed_path = completed_handle.object_path().clone();
    let completed_store = completed_handle.object_store();
    let completed_url = completed_handle.url().clone();
    let mut upload = ObjectUpload::new(completed_handle);
    upload.write(Bytes::from_static(b"small")).await.unwrap();
    let durable_url = upload.complete().await.unwrap();
    assert_eq!(durable_url, completed_url);
    assert_eq!(
        completed_store
            .get(&completed_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::from_static(b"small")
    );

    let multipart_location = unique_location("multipart-complete");
    let multipart_handle = storage
        .prepare_output_target(
            &multipart_location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    let multipart_path = multipart_handle.object_path().clone();
    let multipart_store = multipart_handle.object_store();
    let mut upload = ObjectUpload::new(multipart_handle);
    upload
        .write(Bytes::from_static(b"multipart payload"))
        .await
        .unwrap();
    upload.complete().await.unwrap();
    assert_eq!(
        multipart_store
            .get(&multipart_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::from_static(b"multipart payload")
    );

    let aborted_location = unique_location("abort");
    let aborted_handle = storage
        .prepare_output_target(
            &aborted_location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    let aborted_path = aborted_handle.object_path().clone();
    let aborted_store = aborted_handle.object_store();
    let mut upload = ObjectUpload::new(aborted_handle);
    upload
        .write(Bytes::from_static(b"multipart payload"))
        .await
        .unwrap();
    upload.abort().await.unwrap();
    assert!(matches!(
        aborted_store.head(&aborted_path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn object_upload_uses_single_put_below_threshold_and_multipart_at_threshold() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "2",
    ]);

    let empty_root = unique_controlled_root("empty");
    let (empty, empty_store, empty_path) = controlled_upload(&storage, &empty_root, "output").await;
    empty.complete().await.unwrap();
    assert_eq!(empty_store.control.puts.load(Ordering::SeqCst), 1);
    assert_eq!(
        empty_store.control.multipart_starts.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        empty_store
            .get(&empty_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::new()
    );

    let small_root = unique_controlled_root("small");
    let (mut small, small_store, small_path) =
        controlled_upload(&storage, &small_root, "output").await;
    small.write(Bytes::from_static(b"1234567")).await.unwrap();
    small.complete().await.unwrap();
    assert_eq!(small_store.control.puts.load(Ordering::SeqCst), 1);
    assert_eq!(
        small_store.control.multipart_starts.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        small_store
            .get(&small_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::from_static(b"1234567")
    );

    let threshold_root = unique_controlled_root("threshold");
    let (mut threshold, threshold_store, threshold_path) =
        controlled_upload(&storage, &threshold_root, "output").await;
    threshold
        .write(Bytes::from_static(b"12345678"))
        .await
        .unwrap();
    threshold.complete().await.unwrap();
    assert_eq!(threshold_store.control.puts.load(Ordering::SeqCst), 0);
    assert_eq!(
        threshold_store
            .control
            .multipart_starts
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(threshold_store.control.completes.load(Ordering::SeqCst), 1);
    assert_eq!(
        threshold_store
            .get(&threshold_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::from_static(b"12345678")
    );
}

#[tokio::test]
async fn object_upload_surfaces_fake_store_failures_and_cleanup_failures() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "2",
    ]);

    let put_root = unique_controlled_root("put-failure");
    let (mut put, put_store, put_path) = controlled_upload(&storage, &put_root, "output").await;
    put_store
        .control
        .fail_next_put
        .store(true, Ordering::SeqCst);
    put.write(Bytes::from_static(b"small")).await.unwrap();
    let error = put.complete().await.unwrap_err();
    assert!(error.to_string().contains("controlled put failure"));
    assert!(matches!(
        put_store.head(&put_path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let start_root = unique_controlled_root("start-failure");
    let (mut start, start_store, start_path) =
        controlled_upload(&storage, &start_root, "output").await;
    start_store
        .control
        .fail_next_multipart_start
        .store(true, Ordering::SeqCst);
    start.write(Bytes::from_static(b"12345678")).await.unwrap();
    let error = start.complete().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("controlled multipart start failure")
    );
    assert!(matches!(
        start_store.head(&start_path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let complete_root = unique_controlled_root("complete-failure");
    let (mut complete, complete_store, complete_path) =
        controlled_upload(&storage, &complete_root, "output").await;
    complete_store
        .control
        .fail_next_complete
        .store(true, Ordering::SeqCst);
    complete
        .write(Bytes::from_static(b"12345678"))
        .await
        .unwrap();
    let error = complete.complete().await.unwrap_err();
    assert!(error.to_string().contains("controlled complete failure"));
    assert_eq!(complete_store.control.aborts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        complete_store.head(&complete_path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let cleanup_root = unique_controlled_root("cleanup-failure");
    let (mut cleanup, cleanup_store, cleanup_path) =
        controlled_upload(&storage, &cleanup_root, "output").await;
    cleanup_store
        .control
        .fail_next_part
        .store(true, Ordering::SeqCst);
    cleanup_store
        .control
        .fail_next_abort
        .store(true, Ordering::SeqCst);
    cleanup
        .write(Bytes::from_static(b"12345678"))
        .await
        .unwrap();
    let error = cleanup.complete().await.unwrap_err();
    let message = error.to_string();
    assert!(message.contains("controlled part failure"), "{message}");
    assert!(message.contains("controlled abort failure"), "{message}");
    assert!(matches!(
        cleanup_store.head(&cleanup_path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let abort_root = unique_controlled_root("abort-failure");
    let (mut abort, abort_store, abort_path) =
        controlled_upload(&storage, &abort_root, "output").await;
    abort_store
        .control
        .fail_next_abort
        .store(true, Ordering::SeqCst);
    abort.write(Bytes::from_static(b"12345678")).await.unwrap();
    wait_for_multipart_starts(&abort_store.control, 1).await;
    let error = abort.abort().await.unwrap_err();
    assert!(error.to_string().contains("controlled abort failure"));
    assert!(matches!(
        abort_store.head(&abort_path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn object_upload_allows_only_one_byte_writer() {
    let storage = controlled_session(&["output-test"]);
    let root = unique_controlled_root("writer-owner");
    let (mut upload, _store, _path) = controlled_upload(&storage, &root, "output").await;

    let writer = upload.writer().unwrap();
    drop(writer);
    let error = upload.blocking_writer().unwrap_err();
    assert!(error.to_string().contains("already has a byte writer"));
    upload.abort().await.unwrap();
}

#[tokio::test]
async fn object_upload_bounds_shared_parts_backpressure_and_cleanup() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "3",
    ]);
    let root = unique_controlled_root("bounded");

    let (first, store, _) = controlled_upload(&storage, &root, "shared-first").await;
    let (second, second_store, _) = controlled_upload(&storage, &root, "shared-second").await;
    assert!(Arc::ptr_eq(&store, &second_store));
    let control = Arc::clone(&store.control);
    control.block_parts.store(true, Ordering::SeqCst);
    let first_write = tokio::spawn(async move {
        let mut upload = first;
        upload.write(Bytes::from(vec![1; 16])).await.unwrap();
        upload
    });
    let second_write = tokio::spawn(async move {
        let mut upload = second;
        upload.write(Bytes::from(vec![2; 16])).await.unwrap();
        upload
    });

    wait_for_active_parts(&control, 3).await;
    assert_eq!(control.maximum_active_parts.load(Ordering::SeqCst), 3);
    control.part_release.add_permits(4);
    first_write.await.unwrap().abort().await.unwrap();
    second_write.await.unwrap().abort().await.unwrap();
    assert_eq!(control.aborts.load(Ordering::SeqCst), 2);

    let (upload, _, _) = controlled_upload(&storage, &root, "backpressure").await;
    let mut write = tokio::spawn(async move {
        let mut upload = upload;
        upload.write(Bytes::from(vec![3; 48])).await.unwrap();
        upload
    });
    wait_for_active_parts(&control, 3).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut write)
            .await
            .is_err()
    );
    control.part_release.add_permits(6);
    write.await.unwrap().abort().await.unwrap();
    assert_eq!(control.aborts.load(Ordering::SeqCst), 3);

    let (mut failed, _, _) = controlled_upload(&storage, &root, "part-failure").await;
    control.fail_next_part.store(true, Ordering::SeqCst);
    failed.write(Bytes::from(vec![4; 8])).await.unwrap();
    wait_for_active_parts(&control, 1).await;
    control.part_release.add_permits(1);
    let error = failed.complete().await.unwrap_err();
    assert!(error.to_string().contains("controlled part failure"));
    assert_eq!(control.aborts.load(Ordering::SeqCst), 4);

    let (mut dropped, _, _) = controlled_upload(&storage, &root, "drop").await;
    dropped.write(Bytes::from(vec![5; 8])).await.unwrap();
    wait_for_active_parts(&control, 1).await;
    drop(dropped);
    tokio::time::timeout(Duration::from_secs(5), async {
        while control.aborts.load(Ordering::SeqCst) < 5 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop fallback did not abort the multipart upload");
}

#[tokio::test]
async fn shared_part_limit_gates_payload_creation_across_many_uploads() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "2",
    ]);
    let root = unique_controlled_root("bounded-payloads");
    let mut uploads = Vec::new();
    let mut control = None;
    for ordinal in 0_u8..8 {
        let (upload, store, _) =
            controlled_upload(&storage, &root, &format!("output-{ordinal}")).await;
        control.get_or_insert_with(|| Arc::clone(&store.control));
        uploads.push((ordinal, upload));
    }
    let control = control.unwrap();
    control.block_parts.store(true, Ordering::SeqCst);
    let mut writes = Vec::new();
    for (ordinal, upload) in uploads {
        writes.push(tokio::spawn(async move {
            let mut upload = upload;
            upload.write(Bytes::from(vec![ordinal; 32])).await?;
            Ok::<_, silk_chiffon_storage::ObjectUploadError>(upload)
        }));
    }

    wait_for_multipart_starts(&control, 8).await;
    wait_for_active_parts(&control, 2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(control.parts_created.load(Ordering::SeqCst), 2);

    for write in writes {
        write.abort();
        let _ = write.await;
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while control.aborts.load(Ordering::SeqCst) < 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping backpressured uploads did not abort them");
    assert_eq!(control.active_parts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn object_upload_abort_interrupts_a_backpressured_write() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "1",
    ]);
    let root = unique_controlled_root("abort-backpressure");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    let control = Arc::clone(&store.control);
    control.block_parts.store(true, Ordering::SeqCst);

    let mut writer = upload.writer().unwrap();
    let mut write = tokio::spawn(async move {
        for _ in 0..4 {
            writer.send(Bytes::from_static(b"12345678")).await?;
        }
        Ok::<_, futures::channel::mpsc::SendError>(())
    });
    wait_for_active_parts(&control, 1).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut write)
            .await
            .is_err()
    );

    tokio::time::timeout(Duration::from_secs(5), upload.abort())
        .await
        .expect("upload abort remained blocked behind a multipart part")
        .unwrap();
    let _write_result = tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .expect("byte writer remained blocked after upload abort")
        .unwrap();

    assert_eq!(control.active_parts.load(Ordering::SeqCst), 0);
    assert_eq!(control.aborts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn object_upload_abort_acquires_a_started_multipart_before_cleanup() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "1",
    ]);
    let root = unique_controlled_root("abort-multipart-start");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    let control = Arc::clone(&store.control);
    control.block_multipart_start.store(true, Ordering::SeqCst);

    let mut writer = upload.writer().unwrap();
    writer.send(Bytes::from_static(b"12345678")).await.unwrap();
    wait_for_multipart_starts(&control, 1).await;
    let mut abort = tokio::spawn(upload.abort());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut abort)
            .await
            .is_err()
    );

    control.multipart_start_release.add_permits(1);
    abort.await.unwrap().unwrap();
    drop(writer);

    assert_eq!(control.aborts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn object_upload_drop_cleans_up_after_multipart_initialization_finishes() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "1",
    ]);
    let root = unique_controlled_root("drop-multipart-start");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    let control = Arc::clone(&store.control);
    control.block_multipart_start.store(true, Ordering::SeqCst);

    let mut writer = upload.writer().unwrap();
    writer.send(Bytes::from_static(b"12345678")).await.unwrap();
    wait_for_multipart_starts(&control, 1).await;
    drop(upload);
    assert_eq!(control.aborts.load(Ordering::SeqCst), 0);

    control.multipart_start_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while control.aborts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop fallback did not clean up the initialized multipart upload");
    drop(writer);

    assert_eq!(control.aborts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn object_upload_complete_preserves_a_backpressured_write() {
    let storage = controlled_session(&[
        "output-test",
        "--object-store-upload-part-size",
        "8",
        "--object-store-max-in-flight-parts",
        "1",
    ]);
    let root = unique_controlled_root("complete-backpressure");
    let (mut upload, store, path) = controlled_upload(&storage, &root, "output").await;
    let control = Arc::clone(&store.control);
    control.block_parts.store(true, Ordering::SeqCst);

    let mut writer = upload.writer().unwrap();
    writer.send(Bytes::from(vec![1; 8])).await.unwrap();
    writer.send(Bytes::from(vec![2; 8])).await.unwrap();
    wait_for_active_parts(&control, 1).await;
    drop(writer);
    let mut complete = tokio::spawn(upload.complete());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut complete)
            .await
            .is_err()
    );

    control.part_release.add_permits(2);
    complete.await.unwrap().unwrap();
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();

    assert_eq!(bytes, [vec![1; 8], vec![2; 8]].concat());
    assert_eq!(control.active_parts.load(Ordering::SeqCst), 0);
    assert_eq!(control.completes.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "local-bare-paths")]
#[tokio::test]
async fn local_preparation_validates_or_creates_parent_directories() {
    let temporary = tempfile::tempdir().unwrap();
    let target = temporary.path().join("new/deep/output.arrow");
    let location = LocationInput::parse(target.to_str().unwrap()).unwrap();

    let rejected = silk_chiffon_storage::local::session()
        .unwrap()
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await;
    assert!(rejected.is_err());

    let handle = silk_chiffon_storage::local::session()
        .unwrap()
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, true),
        )
        .await
        .unwrap();
    assert!(target.parent().unwrap().is_dir());
    assert_eq!(handle.local_path().unwrap(), target);
}
