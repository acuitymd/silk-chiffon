//! Instrumented object store for bounded-read contract tests.

use std::{
    fmt, io,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};

/// In-memory store that records bounded reads and can fail them deterministically.
///
/// Format tests use this probe to enforce the shared rule that inspection and
/// detection read only the byte ranges they need. It also observes concurrent
/// reads without imposing a format-specific inspection API.
#[derive(Debug)]
pub struct ReadProbeStore {
    inner: InMemory,
    ranges: Mutex<Vec<GetRange>>,
    head_request_count: AtomicUsize,
    fail_reads: AtomicBool,
    active_reads: AtomicUsize,
    max_active_reads: AtomicUsize,
}

impl ReadProbeStore {
    /// Creates an empty probe store.
    pub fn new() -> Self {
        Self {
            inner: InMemory::new(),
            ranges: Mutex::new(Vec::new()),
            head_request_count: AtomicUsize::new(0),
            fail_reads: AtomicBool::new(false),
            active_reads: AtomicUsize::new(0),
            max_active_reads: AtomicUsize::new(0),
        }
    }

    /// Clears observations and disables injected failures without deleting objects.
    pub fn reset_observation(&self) {
        self.ranges.lock().unwrap().clear();
        self.head_request_count.store(0, Ordering::SeqCst);
        self.fail_reads.store(false, Ordering::SeqCst);
        self.active_reads.store(0, Ordering::SeqCst);
        self.max_active_reads.store(0, Ordering::SeqCst);
    }

    /// Returns every non-HEAD range requested since the last reset.
    pub fn ranges(&self) -> Vec<GetRange> {
        self.ranges.lock().unwrap().clone()
    }

    /// Returns the number of metadata-only requests since the last reset.
    pub fn head_request_count(&self) -> usize {
        self.head_request_count.load(Ordering::SeqCst)
    }

    /// Makes subsequent non-HEAD reads fail until observations are reset.
    pub fn set_fail_reads(&self, fail: bool) {
        self.fail_reads.store(fail, Ordering::SeqCst);
    }

    /// Returns the largest number of overlapping `get_opts` calls observed.
    pub fn max_active_reads(&self) -> usize {
        self.max_active_reads.load(Ordering::SeqCst)
    }

    fn observe_active_read(&self) -> ActiveRead<'_> {
        let active = self.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active_reads.fetch_max(active, Ordering::SeqCst);
        ActiveRead(self)
    }
}

impl Default for ReadProbeStore {
    fn default() -> Self {
        Self::new()
    }
}

struct ActiveRead<'a>(&'a ReadProbeStore);

impl Drop for ActiveRead<'_> {
    fn drop(&mut self) {
        self.0.active_reads.fetch_sub(1, Ordering::SeqCst);
    }
}

impl fmt::Display for ReadProbeStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReadProbeStore")
    }
}

#[async_trait]
impl ObjectStore for ReadProbeStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let _active = self.observe_active_read();
        if options.head {
            self.head_request_count.fetch_add(1, Ordering::SeqCst);
        } else {
            if self.fail_reads.load(Ordering::SeqCst) {
                return Err(probe_error("controlled object-store read failure"));
            }
            let range = options
                .range
                .clone()
                .ok_or_else(|| probe_error("test subject attempted an unbounded object read"))?;
            self.ranges.lock().unwrap().push(range);
            tokio::task::yield_now().await;
        }
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

fn probe_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "read-probe",
        source: Box::new(io::Error::other(message)),
    }
}

#[cfg(test)]
mod tests {
    use object_store::{GetRange, ObjectStoreExt};

    use super::*;

    #[tokio::test]
    async fn records_bounded_reads_and_rejects_unbounded_ones() {
        let store = ReadProbeStore::new();
        let path = Path::from("input");
        store.put(&path, "abcdef".into()).await.unwrap();

        assert_eq!(store.get_range(&path, 1..4).await.unwrap(), "bcd");
        assert_eq!(store.ranges(), [GetRange::Bounded(1..4)]);
        assert_eq!(store.head_request_count(), 0);

        store.head(&path).await.unwrap();
        assert_eq!(store.head_request_count(), 1);

        let error = store.get(&path).await.unwrap_err();
        assert!(error.to_string().contains("unbounded object read"));
    }

    #[tokio::test]
    async fn injected_failures_do_not_replace_stored_objects() {
        let store = ReadProbeStore::new();
        let path = Path::from("input");
        store.put(&path, "abcdef".into()).await.unwrap();
        store.set_fail_reads(true);

        let error = store.get_range(&path, 0..1).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("controlled object-store read failure")
        );

        store.reset_observation();
        assert_eq!(store.get_range(&path, 0..1).await.unwrap(), "a");
    }
}
