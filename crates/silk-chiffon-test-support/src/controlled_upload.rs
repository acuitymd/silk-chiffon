//! Controlled multipart storage for format lifecycle tests.

use std::{
    fmt, io,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use silk_chiffon_storage::{StorageAccess, StorageBackend, StorageRegistry, StorageSession};

static STORE: OnceLock<Arc<ControlledUploadStore>> = OnceLock::new();
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// In-memory store with deterministic multipart blocking and failure controls.
#[derive(Debug)]
pub struct ControlledUploadStore {
    inner: InMemory,
    multipart_starts: AtomicUsize,
    parts_started: AtomicUsize,
    active_parts: AtomicUsize,
    max_active_parts: AtomicUsize,
    aborts: AtomicUsize,
    block_parts: AtomicBool,
    fail_next_put: AtomicBool,
    fail_next_multipart_start: AtomicBool,
    fail_next_abort: AtomicBool,
    fail_next_complete: AtomicBool,
    fail_part: AtomicUsize,
    part_changed: tokio::sync::Notify,
}

impl ControlledUploadStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            multipart_starts: AtomicUsize::new(0),
            parts_started: AtomicUsize::new(0),
            active_parts: AtomicUsize::new(0),
            max_active_parts: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            block_parts: AtomicBool::new(false),
            fail_next_put: AtomicBool::new(false),
            fail_next_multipart_start: AtomicBool::new(false),
            fail_next_abort: AtomicBool::new(false),
            fail_next_complete: AtomicBool::new(false),
            fail_part: AtomicUsize::new(0),
            part_changed: tokio::sync::Notify::new(),
        }
    }

    /// Returns the number of multipart sessions created by the store.
    pub fn multipart_starts(&self) -> usize {
        self.multipart_starts.load(Ordering::SeqCst)
    }

    /// Returns the number of part requests started by the store.
    pub fn parts_started(&self) -> usize {
        self.parts_started.load(Ordering::SeqCst)
    }

    /// Returns the number of part requests that have not yet settled.
    pub fn active_parts(&self) -> usize {
        self.active_parts.load(Ordering::SeqCst)
    }

    /// Returns the highest observed number of concurrent part requests.
    pub fn max_active_parts(&self) -> usize {
        self.max_active_parts.load(Ordering::SeqCst)
    }

    /// Returns the number of multipart abort requests.
    pub fn aborts(&self) -> usize {
        self.aborts.load(Ordering::SeqCst)
    }

    /// Fails the next part request to start.
    pub fn fail_next_part(&self) {
        self.fail_part_after(0);
    }

    /// Fails a later part after `successful_parts` more requests start.
    pub fn fail_part_after(&self, successful_parts: usize) {
        self.fail_part.store(
            self.parts_started() + successful_parts + 1,
            Ordering::SeqCst,
        );
    }

    /// Fails the next single-object put request.
    pub fn fail_next_put(&self) {
        self.fail_next_put.store(true, Ordering::SeqCst);
    }

    /// Fails the next multipart-start request.
    pub fn fail_next_multipart_start(&self) {
        self.fail_next_multipart_start.store(true, Ordering::SeqCst);
    }

    /// Fails the next multipart completion request.
    pub fn fail_next_complete(&self) {
        self.fail_next_complete.store(true, Ordering::SeqCst);
    }

    /// Fails the next multipart abort after delegating cleanup to the inner store.
    pub fn fail_next_abort(&self) {
        self.fail_next_abort.store(true, Ordering::SeqCst);
    }

    /// Blocks new and active part requests until the returned guard is dropped.
    pub fn block_parts(self: &Arc<Self>) -> BlockParts {
        self.block_parts.store(true, Ordering::SeqCst);
        BlockParts(Arc::clone(self))
    }

    /// Waits until the number of active parts exceeds `previous`.
    pub async fn wait_for_more_active_parts(&self, previous: usize) {
        loop {
            let changed = self.part_changed.notified();
            if self.active_parts() > previous {
                return;
            }
            changed.await;
        }
    }

    /// Waits until the number of active parts returns to `expected`.
    pub async fn wait_for_active_parts(&self, expected: usize) {
        loop {
            let changed = self.part_changed.notified();
            if self.active_parts() == expected {
                return;
            }
            changed.await;
        }
    }

    fn reset_controls(&self) {
        self.block_parts.store(false, Ordering::SeqCst);
        self.fail_next_put.store(false, Ordering::SeqCst);
        self.fail_next_multipart_start
            .store(false, Ordering::SeqCst);
        self.fail_next_abort.store(false, Ordering::SeqCst);
        self.fail_next_complete.store(false, Ordering::SeqCst);
        self.fail_part.store(0, Ordering::SeqCst);
        self.max_active_parts
            .store(self.active_parts(), Ordering::SeqCst);
        self.part_changed.notify_waiters();
    }
}

impl fmt::Display for ControlledUploadStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlledUploadStore")
    }
}

/// Guard that releases part requests when dropped.
pub struct BlockParts(Arc<ControlledUploadStore>);

impl Drop for BlockParts {
    fn drop(&mut self) {
        self.0.block_parts.store(false, Ordering::SeqCst);
        self.0.part_changed.notify_waiters();
    }
}

struct ActivePart(Arc<ControlledUploadStore>);

impl Drop for ActivePart {
    fn drop(&mut self) {
        self.0.active_parts.fetch_sub(1, Ordering::SeqCst);
        self.0.part_changed.notify_waiters();
    }
}

#[derive(Debug)]
struct ControlledMultipart {
    inner: Box<dyn MultipartUpload>,
    store: Arc<ControlledUploadStore>,
}

#[async_trait]
impl MultipartUpload for ControlledMultipart {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        let part = self.inner.put_part(payload);
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            let part_number = store.parts_started.fetch_add(1, Ordering::SeqCst) + 1;
            let active = store.active_parts.fetch_add(1, Ordering::SeqCst) + 1;
            store.max_active_parts.fetch_max(active, Ordering::SeqCst);
            store.part_changed.notify_waiters();
            let _active = ActivePart(Arc::clone(&store));
            while store.block_parts.load(Ordering::SeqCst) {
                store.part_changed.notified().await;
            }
            if store
                .fail_part
                .compare_exchange(part_number, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Err(controlled_error("controlled part failure"));
            }
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.store.fail_next_complete.swap(false, Ordering::SeqCst) {
            return Err(controlled_error("controlled complete failure"));
        }
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.store.aborts.fetch_add(1, Ordering::SeqCst);
        self.inner.abort().await?;
        if self.store.fail_next_abort.swap(false, Ordering::SeqCst) {
            return Err(controlled_error("controlled abort failure"));
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for ControlledUploadStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_next_put.swap(false, Ordering::SeqCst) {
            return Err(controlled_error("controlled put failure"));
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.multipart_starts.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_multipart_start.swap(false, Ordering::SeqCst) {
            return Err(controlled_error("controlled multipart-start failure"));
        }
        Ok(Box::new(ControlledMultipart {
            inner: self.inner.put_multipart_opts(location, options).await?,
            store: controlled_upload_store(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Returns the process-wide controlled store used by test storage sessions.
pub fn controlled_upload_store() -> Arc<ControlledUploadStore> {
    Arc::clone(STORE.get_or_init(|| Arc::new(ControlledUploadStore::new())))
}

/// Serializes tests that mutate the process-wide controlled store.
pub async fn controlled_upload_lock() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().await;
    controlled_upload_store().reset_controls();
    guard
}

/// Creates a storage session that routes `tracking:` URLs to the shared store.
pub fn controlled_upload_storage() -> StorageSession {
    controlled_upload_storage_with(1, 2)
}

/// Creates a controlled session with explicit upload limits.
pub fn controlled_upload_storage_with(
    part_size: usize,
    max_in_flight_parts: usize,
) -> StorageSession {
    fn create_store(
        _: &url::Url,
        _: &(),
        _: Option<&silk_chiffon_storage::RetryConfig>,
    ) -> anyhow::Result<Arc<dyn ObjectStore>> {
        Ok(controlled_upload_store())
    }

    let backend = StorageBackend::without_args()
        .name("tracking")
        .schemes(["tracking"])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(create_store)
        .build()
        .unwrap();
    let registry = StorageRegistry::builder()
        .register(backend)
        .build()
        .unwrap();
    let command = registry.augment_args(clap::Command::new("controlled-upload-test"));
    let part_size = part_size.to_string();
    let max_in_flight_parts = max_in_flight_parts.to_string();
    let matches = command
        .try_get_matches_from([
            "controlled-upload-test",
            "--object-store-upload-part-size",
            part_size.as_str(),
            "--object-store-max-in-flight-parts",
            max_in_flight_parts.as_str(),
        ])
        .unwrap();
    registry.create_session(&matches).unwrap()
}

fn controlled_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "controlled-upload",
        source: Box::new(io::Error::other(message)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use object_store::ObjectStoreExt;
    use silk_chiffon_storage::{ExistingOutput, LocationInput, ObjectUpload, OutputPreparation};

    use super::*;

    static OBJECT_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    async fn upload(storage: &StorageSession, name: &str) -> ObjectUpload {
        let ordinal = OBJECT_SEQUENCE.fetch_add(1, Ordering::SeqCst);
        let location = LocationInput::parse(format!("tracking://bucket/{name}-{ordinal}")).unwrap();
        let handle = storage
            .prepare_output_target(
                &location,
                &OutputPreparation::new(ExistingOutput::Allow, false),
            )
            .await
            .unwrap();
        ObjectUpload::new(handle)
    }

    #[tokio::test]
    async fn later_part_failures_are_deterministic_and_cleanup_the_object() {
        let _guard = controlled_upload_lock().await;
        let storage = controlled_upload_storage();
        let store = controlled_upload_store();
        let aborts = store.aborts();
        let mut upload = upload(&storage, "later-part").await;
        store.fail_part_after(2);

        let _ = upload.write(Bytes::from_static(b"abcdef")).await;
        let error = upload.complete().await.unwrap_err();

        assert!(format!("{error:#}").contains("controlled part failure"));
        assert!(store.parts_started() >= 3);
        assert_eq!(store.active_parts(), 0);
        assert_eq!(store.aborts(), aborts + 1);
    }

    #[tokio::test]
    async fn lock_resets_unused_failure_controls_between_tests() {
        {
            let _guard = controlled_upload_lock().await;
            controlled_upload_store().fail_next_put();
        }
        let _guard = controlled_upload_lock().await;
        let storage = controlled_upload_storage_with(1024, 1);
        let mut upload = upload(&storage, "reset-controls").await;
        upload.write(Bytes::from_static(b"ok")).await.unwrap();
        let target = upload.complete().await.unwrap();
        let path = Path::from_url_path(target.path()).unwrap();

        assert_eq!(
            controlled_upload_store()
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "ok"
        );
    }
}
