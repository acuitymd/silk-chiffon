//! Temporary uploads and conditional output commit.
//!
//! An output moves through `Open`, `Uploaded`, `Committed`, or `Aborted`. Bytes
//! are written to a reserved temporary object while open. Upload completion
//! makes only that private object visible. Commit atomically publishes the
//! destination according to the overwrite policy, then removes the temporary
//! object.

use std::{
    fmt,
    ops::Range,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, RenameTargetMode, Result as StoreResult, UploadPart, buffered::BufWriter,
    path::Path,
};
use tokio::{io::AsyncWriteExt, sync::Notify, task::JoinHandle};
use uuid::Uuid;

use super::{
    StorageConfig,
    gcs::commit_gcs_013,
    location::{ObjectLocation, StoreKind},
};

const GCS_TEMPORARY_PREFIX: &str = "__silk_chiffon_tmp__";
const GCS_MAX_OBJECT_NAME_BYTES: usize = 1024;
const LOCAL_TEMPORARY_MARKER: &str = ".silk-chiffon-tmp-";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputPolicy {
    overwrite: bool,
    create_dirs: bool,
}

impl OutputPolicy {
    #[must_use]
    pub const fn new(overwrite: bool, create_dirs: bool) -> Self {
        Self {
            overwrite,
            create_dirs,
        }
    }

    #[must_use]
    pub const fn overwrite(self) -> bool {
        self.overwrite
    }

    #[must_use]
    pub const fn create_dirs(self) -> bool {
        self.create_dirs
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectOutputState {
    Open,
    Uploaded,
    Committed,
    Aborted,
}

/// A byte-oriented output that publishes its destination only after commit.
pub struct ObjectOutput {
    destination: ObjectLocation,
    temporary: Path,
    policy: OutputPolicy,
    part_size: usize,
    writer: Option<BufWriter>,
    finalizer: Option<JoinHandle<std::io::Result<()>>>,
    cleanup: Arc<MultipartCleanup>,
    state: ObjectOutputState,
    temporary_present: bool,
}

impl ObjectOutput {
    pub async fn open(
        destination: ObjectLocation,
        config: StorageConfig,
        policy: OutputPolicy,
    ) -> Result<Self> {
        if config.upload_part_size == 0 {
            bail!("object-store upload part size must be greater than zero");
        }
        if config.upload_concurrency == 0 {
            bail!("object-store upload concurrency must be greater than zero");
        }
        prepare_destination(&destination, policy).await?;

        let temporary = temporary_path(&destination);
        let cleanup = Arc::new(MultipartCleanup::default());
        let store: Arc<dyn ObjectStore> = Arc::new(UploadCleanupStore {
            inner: Arc::clone(destination.store().object_store()),
            cleanup: Arc::clone(&cleanup),
        });
        let writer = BufWriter::with_capacity(store, temporary.clone(), config.upload_part_size)
            .with_max_concurrency(config.upload_concurrency);

        Ok(Self {
            destination,
            temporary,
            policy,
            part_size: config.upload_part_size,
            writer: Some(writer),
            finalizer: None,
            cleanup,
            state: ObjectOutputState::Open,
            temporary_present: false,
        })
    }

    #[must_use]
    pub(crate) const fn part_size(&self) -> usize {
        self.part_size
    }

    #[must_use]
    pub(crate) fn destination_display(&self) -> &str {
        self.destination.display()
    }

    #[must_use]
    pub(crate) fn destination(&self) -> &ObjectLocation {
        &self.destination
    }

    pub async fn write(&mut self, mut bytes: Bytes) -> Result<()> {
        if self.state != ObjectOutputState::Open {
            bail!("cannot write output while it is {:?}", self.state);
        }

        while !bytes.is_empty() {
            let chunk = bytes.split_to(bytes.len().min(self.part_size));
            let Some(writer) = self.writer.as_mut() else {
                bail!(
                    "output '{}' is being finalized; finish commit or abort it before writing",
                    self.destination.display()
                );
            };
            let result = writer.put(chunk).await;
            if let Err(error) = result {
                let primary = anyhow!(error).context(format!(
                    "could not upload temporary output for '{}'",
                    self.destination.display()
                ));
                return Err(self.fail_open_output(primary).await);
            }
        }
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<String> {
        match self.state {
            ObjectOutputState::Open => self.finish_upload().await?,
            ObjectOutputState::Uploaded => {}
            ObjectOutputState::Committed => bail!(
                "output '{}' has already been committed",
                self.destination.display()
            ),
            ObjectOutputState::Aborted => {
                bail!("output '{}' has been aborted", self.destination.display())
            }
        }

        let intended_mode = if self.policy.overwrite {
            CopyMode::Overwrite
        } else {
            CopyMode::Create
        };
        let store = self.destination.store().object_store();
        let commit_result = match self.destination.store().kind() {
            StoreKind::Local => {
                let target_mode = match intended_mode {
                    CopyMode::Create => RenameTargetMode::Create,
                    CopyMode::Overwrite => RenameTargetMode::Overwrite,
                };
                store
                    .rename_opts(
                        &self.temporary,
                        self.destination.path(),
                        RenameOptions::new().with_target_mode(target_mode),
                    )
                    .await
            }
            StoreKind::Gcs { .. } => {
                commit_gcs_013(
                    store.as_ref(),
                    &self.temporary,
                    self.destination.path(),
                    intended_mode,
                )
                .await
            }
        };

        if let Err(error) = commit_result {
            let primary = anyhow!(error).context(format!(
                "could not commit output '{}'",
                self.destination.display()
            ));
            let cleanup = self
                .delete_temporary()
                .await
                .err()
                .map(|error| format!("{error:#}"));
            self.state = ObjectOutputState::Aborted;
            return Err(with_cleanup(primary, cleanup));
        }

        self.state = ObjectOutputState::Committed;
        if matches!(self.destination.store().kind(), StoreKind::Local) {
            self.temporary_present = false;
        } else if let Err(error) = self.delete_temporary().await {
            return Err(error.context(format!(
                "output '{}' was committed, but its temporary object could not be removed",
                self.destination.display()
            )));
        }

        Ok(self.destination.display().to_string())
    }

    pub async fn abort(&mut self) -> Result<()> {
        match self.state {
            ObjectOutputState::Open => {
                if self.finalizer.is_some() {
                    self.finish_upload().await?;
                    let result = self.delete_temporary().await;
                    self.state = ObjectOutputState::Aborted;
                    return result;
                }

                let result = match self.writer.as_mut() {
                    Some(writer) => writer.abort().await,
                    None => {
                        return self.cleanup_canceled_finalization().await;
                    }
                };
                self.writer.take();
                let tracked_cleanup = self.cleanup.wait().await;
                self.state = ObjectOutputState::Aborted;

                match result {
                    Ok(()) => tracked_cleanup.map_err(anyhow::Error::msg),
                    Err(error) => Err(with_cleanup(anyhow!(error), tracked_cleanup.err())),
                }
            }
            ObjectOutputState::Uploaded => {
                let result = self.delete_temporary().await;
                self.state = ObjectOutputState::Aborted;
                result
            }
            ObjectOutputState::Committed | ObjectOutputState::Aborted => {
                if self.temporary_present {
                    self.delete_temporary().await
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn finish_upload(&mut self) -> Result<()> {
        if self.finalizer.is_none() {
            let Some(mut writer) = self.writer.take() else {
                bail!(
                    "output '{}' has no writer or active finalizer",
                    self.destination.display()
                );
            };
            self.temporary_present = true;
            // The store operation must finish before cancellation cleanup deletes its object.
            self.finalizer = Some(tokio::spawn(async move { writer.shutdown().await }));
        }

        let Some(finalizer) = self.finalizer.as_mut() else {
            bail!(
                "output '{}' has no active finalizer",
                self.destination.display()
            );
        };
        let result = finalizer.await;
        self.finalizer.take();
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let primary = anyhow!(error).context(format!(
                    "temporary output finalizer failed for '{}'",
                    self.destination.display()
                ));
                return Err(self.cleanup_failed_finalization(primary).await);
            }
        };

        if let Err(error) = result {
            let primary = anyhow!(error).context(format!(
                "could not finish temporary output for '{}'",
                self.destination.display()
            ));
            return Err(self.cleanup_failed_finalization(primary).await);
        }

        self.state = ObjectOutputState::Uploaded;
        if let Err(error) = self.cleanup.wait().await {
            let primary = anyhow!(error).context(format!(
                "temporary output cleanup failed for '{}'",
                self.destination.display()
            ));
            return Err(self.cleanup_failed_finalization(primary).await);
        }
        Ok(())
    }

    async fn fail_open_output(&mut self, primary: anyhow::Error) -> anyhow::Error {
        let abort_result = match self.writer.as_mut() {
            Some(writer) => writer.abort().await,
            None => Ok(()),
        };
        self.writer.take();
        let tracked_cleanup = self.cleanup.wait().await.err();
        self.state = ObjectOutputState::Aborted;

        let cleanup = abort_result
            .err()
            .map(|error| error.to_string())
            .or(tracked_cleanup);
        with_cleanup(primary, cleanup)
    }

    async fn cleanup_failed_finalization(&mut self, primary: anyhow::Error) -> anyhow::Error {
        let multipart_cleanup = self.cleanup.wait().await.err();
        let object_cleanup = self
            .delete_temporary()
            .await
            .err()
            .map(|error| format!("{error:#}"));
        let cleanup = [multipart_cleanup, object_cleanup]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("; ");
        self.state = ObjectOutputState::Aborted;
        with_cleanup(primary, (!cleanup.is_empty()).then_some(cleanup))
    }

    async fn cleanup_canceled_finalization(&mut self) -> Result<()> {
        let multipart_cleanup = self.cleanup.wait().await.err();
        let object_cleanup = if self.temporary_present {
            self.delete_temporary()
                .await
                .err()
                .map(|error| format!("{error:#}"))
        } else {
            None
        };
        self.state = ObjectOutputState::Aborted;

        let cleanup = [multipart_cleanup, object_cleanup]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("; ");
        if cleanup.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "could not clean canceled output '{}': {cleanup}",
                self.destination.display()
            ))
        }
    }

    async fn delete_temporary(&mut self) -> Result<()> {
        match self
            .destination
            .store()
            .object_store()
            .delete(&self.temporary)
            .await
        {
            Ok(()) => {
                self.temporary_present = false;
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.temporary_present = false;
                Ok(())
            }
            Err(error) => Err(anyhow!(error).context(format!(
                "could not remove temporary output for '{}'",
                self.destination.display()
            ))),
        }
    }

    #[cfg(test)]
    fn temporary(&self) -> &Path {
        &self.temporary
    }
}

impl fmt::Debug for ObjectOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectOutput")
            .field("destination", &self.destination)
            .field("temporary", &self.temporary)
            .field("policy", &self.policy)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl Drop for ObjectOutput {
    fn drop(&mut self) {
        self.writer.take();
        if !self.temporary_present {
            return;
        }

        let store = Arc::clone(self.destination.store().object_store());
        let temporary = self.temporary.clone();
        let finalizer = self.finalizer.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(finalizer) = finalizer {
                    let _ = finalizer.await;
                }
                let _ = store.delete(&temporary).await;
            });
        }
    }
}

async fn prepare_destination(destination: &ObjectLocation, policy: OutputPolicy) -> Result<()> {
    if !policy.overwrite {
        match destination
            .store()
            .object_store()
            .head(destination.path())
            .await
        {
            Ok(_) => bail!(
                "output '{}' already exists; use --overwrite to replace it",
                destination.display()
            ),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(anyhow!(error).context(format!(
                    "could not check output '{}'",
                    destination.display()
                )));
            }
        }
    }

    if matches!(destination.store().kind(), StoreKind::Local) {
        let path = local_filesystem_path(destination);
        let parent = path
            .parent()
            .context("local output has no parent directory")?;
        if policy.create_dirs {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "could not create parent directory for '{}'",
                    destination.display()
                )
            })?;
        } else if !parent.is_dir() {
            bail!(
                "parent directory for output '{}' does not exist; use --create-dirs to create it",
                destination.display()
            );
        }
    }

    Ok(())
}

fn local_filesystem_path(location: &ObjectLocation) -> PathBuf {
    PathBuf::from(std::path::MAIN_SEPARATOR.to_string()).join(location.path().as_ref())
}

fn temporary_path(destination: &ObjectLocation) -> Path {
    let id = Uuid::new_v4();
    match destination.store().kind() {
        StoreKind::Local => destination
            .path()
            .parent()
            .unwrap_or(Path::ROOT)
            .join(format!("{LOCAL_TEMPORARY_MARKER}{id}")),
        StoreKind::Gcs { .. } => {
            let suffix = format!("-{id}");
            let maximum_destination_bytes =
                GCS_MAX_OBJECT_NAME_BYTES - GCS_TEMPORARY_PREFIX.len() - 1 - suffix.len();
            let destination = truncate_utf8(destination.path().as_ref(), maximum_destination_bytes);
            Path::parse(format!("{GCS_TEMPORARY_PREFIX}/{destination}{suffix}"))
                .expect("destination-derived GCS temporary path must remain valid")
        }
    }
}

fn truncate_utf8(value: &str, maximum_bytes: usize) -> &str {
    let mut end = value.len().min(maximum_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn with_cleanup(primary: anyhow::Error, cleanup: Option<String>) -> anyhow::Error {
    match cleanup {
        Some(cleanup) => anyhow::Error::new(PrimaryWithCleanup { primary, cleanup }),
        None => primary,
    }
}

#[derive(Debug)]
struct PrimaryWithCleanup {
    primary: anyhow::Error,
    cleanup: String,
}

impl fmt::Display for PrimaryWithCleanup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:#}; cleanup also failed: {}",
            self.primary, self.cleanup
        )
    }
}

impl std::error::Error for PrimaryWithCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

#[derive(Debug, Default)]
struct MultipartCleanup {
    state: Mutex<MultipartCleanupState>,
    changed: Notify,
}

#[derive(Debug, Default)]
enum MultipartCleanupState {
    #[default]
    NotStarted,
    Pending,
    Finished(std::result::Result<(), String>),
}

impl MultipartCleanup {
    fn start(&self) {
        *self.state.lock().expect("multipart cleanup lock") = MultipartCleanupState::Pending;
    }

    fn finish(&self, result: &StoreResult<()>) {
        let result = match result {
            Ok(()) => Ok(()),
            Err(error) => Err(error.to_string()),
        };
        *self.state.lock().expect("multipart cleanup lock") =
            MultipartCleanupState::Finished(result);
        self.changed.notify_waiters();
    }

    async fn wait(&self) -> std::result::Result<(), String> {
        loop {
            let changed = self.changed.notified();
            match &*self.state.lock().expect("multipart cleanup lock") {
                MultipartCleanupState::NotStarted => return Ok(()),
                MultipartCleanupState::Pending => {}
                MultipartCleanupState::Finished(result) => return result.clone(),
            }
            changed.await;
        }
    }
}

#[derive(Debug)]
struct UploadCleanupStore {
    inner: Arc<dyn ObjectStore>,
    cleanup: Arc<MultipartCleanup>,
}

impl fmt::Display for UploadCleanupStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait]
impl ObjectStore for UploadCleanupStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        let upload = self.inner.put_multipart_opts(location, options).await?;
        self.cleanup.start();
        Ok(Box::new(GuardedMultipart {
            inner: Some(upload),
            cleanup: Arc::clone(&self.cleanup),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct GuardedMultipart {
    inner: Option<Box<dyn MultipartUpload>>,
    cleanup: Arc<MultipartCleanup>,
}

impl GuardedMultipart {
    fn finish_tracking(&mut self, result: &StoreResult<()>) {
        self.cleanup.finish(result);
        self.inner.take();
    }
}

#[async_trait]
impl MultipartUpload for GuardedMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner
            .as_mut()
            .expect("multipart upload is active")
            .put_part(data)
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        let result = self
            .inner
            .as_mut()
            .expect("multipart upload is active")
            .complete()
            .await;
        match result {
            Ok(result) => {
                self.finish_tracking(&Ok(()));
                Ok(result)
            }
            Err(primary) => {
                let cleanup = self
                    .inner
                    .as_mut()
                    .expect("multipart upload is active")
                    .abort()
                    .await;
                self.finish_tracking(&cleanup);
                match cleanup {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(object_store::Error::Generic {
                        store: "multipart upload",
                        source: Box::new(StorePrimaryWithCleanup { primary, cleanup }),
                    }),
                }
            }
        }
    }

    async fn abort(&mut self) -> StoreResult<()> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        let result = inner.abort().await;
        self.finish_tracking(&result);
        result
    }
}

impl Drop for GuardedMultipart {
    fn drop(&mut self) {
        let Some(mut upload) = self.inner.take() else {
            return;
        };
        let cleanup = Arc::clone(&self.cleanup);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let result = upload.abort().await;
                cleanup.finish(&result);
            });
        } else {
            let error = object_store::Error::Generic {
                store: "multipart upload",
                source: "no Tokio runtime was available for cancellation cleanup".into(),
            };
            cleanup.finish(&Err(error));
        }
    }
}

#[derive(Debug)]
struct StorePrimaryWithCleanup {
    primary: object_store::Error,
    cleanup: object_store::Error,
}

impl fmt::Display for StorePrimaryWithCleanup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}; cleanup also failed: {}", self.primary, self.cleanup)
    }
}

impl std::error::Error for StorePrimaryWithCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.primary)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::{Error as StoreError, PutPayload, memory::InMemory, path::Path};

    use crate::storage::{StorageContext, StoreHandle};

    #[derive(Debug, Default)]
    struct RequestStats {
        puts: AtomicUsize,
        multiparts: AtomicUsize,
        parts: AtomicUsize,
        active_parts: AtomicUsize,
        maximum_parts: AtomicUsize,
        aborts: AtomicUsize,
        deletes: AtomicUsize,
        heads: AtomicUsize,
        copy_modes: Mutex<Vec<CopyMode>>,
        rename_modes: Mutex<Vec<RenameTargetMode>>,
    }

    #[derive(Debug, Clone)]
    struct TestStore {
        inner: InMemory,
        stats: Arc<RequestStats>,
        emulate_gcs_013: bool,
        part_delay: Duration,
        put_response_delay: Duration,
        copy_delay: Duration,
        fail_part: Arc<AtomicBool>,
        fail_abort: Arc<AtomicBool>,
        fail_copy: Arc<AtomicBool>,
        fail_delete: Arc<AtomicBool>,
    }

    impl TestStore {
        fn new(emulate_gcs_013: bool) -> Self {
            Self {
                inner: InMemory::new(),
                stats: Arc::new(RequestStats::default()),
                emulate_gcs_013,
                part_delay: Duration::ZERO,
                put_response_delay: Duration::ZERO,
                copy_delay: Duration::ZERO,
                fail_part: Arc::new(AtomicBool::new(false)),
                fail_abort: Arc::new(AtomicBool::new(false)),
                fail_copy: Arc::new(AtomicBool::new(false)),
                fail_delete: Arc::new(AtomicBool::new(false)),
            }
        }

        fn with_part_delay(mut self, delay: Duration) -> Self {
            self.part_delay = delay;
            self
        }

        fn with_copy_delay(mut self, delay: Duration) -> Self {
            self.copy_delay = delay;
            self
        }

        fn with_put_response_delay(mut self, delay: Duration) -> Self {
            self.put_response_delay = delay;
            self
        }

        async fn bytes(&self, path: &str) -> Option<Bytes> {
            self.inner
                .get(&Path::from(path))
                .await
                .ok()?
                .bytes()
                .await
                .ok()
        }
    }

    impl fmt::Display for TestStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("TestStore")
        }
    }

    #[async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            self.stats.puts.fetch_add(1, Ordering::SeqCst);
            let result = self.inner.put_opts(location, payload, options).await;
            tokio::time::sleep(self.put_response_delay).await;
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.stats.multiparts.fetch_add(1, Ordering::SeqCst);
            let inner = self.inner.put_multipart_opts(location, options).await?;
            Ok(Box::new(TestMultipart {
                inner,
                stats: Arc::clone(&self.stats),
                part_delay: self.part_delay,
                fail_part: Arc::clone(&self.fail_part),
                fail_abort: Arc::clone(&self.fail_abort),
            }))
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
            if options.head {
                self.stats.heads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> StoreResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            let inner = self.inner.clone();
            let stats = Arc::clone(&self.stats);
            let fail_delete = Arc::clone(&self.fail_delete);
            locations
                .then(move |location| {
                    let inner = inner.clone();
                    let stats = Arc::clone(&stats);
                    let fail_delete = Arc::clone(&fail_delete);
                    async move {
                        let location = location?;
                        stats.deletes.fetch_add(1, Ordering::SeqCst);
                        if fail_delete.load(Ordering::SeqCst) {
                            return Err(test_error("injected delete failure"));
                        }
                        inner.delete(&location).await?;
                        Ok(location)
                    }
                })
                .boxed()
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
            self.stats.copy_modes.lock().unwrap().push(options.mode);
            tokio::time::sleep(self.copy_delay).await;
            if self.fail_copy.load(Ordering::SeqCst) {
                return Err(test_error("injected commit failure"));
            }
            let applied_mode = if self.emulate_gcs_013 {
                match options.mode {
                    CopyMode::Create => CopyMode::Overwrite,
                    CopyMode::Overwrite => CopyMode::Create,
                }
            } else {
                options.mode
            };
            self.inner
                .copy_opts(from, to, CopyOptions::new().with_mode(applied_mode))
                .await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> StoreResult<()> {
            self.stats
                .rename_modes
                .lock()
                .unwrap()
                .push(options.target_mode);
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    struct TestMultipart {
        inner: Box<dyn MultipartUpload>,
        stats: Arc<RequestStats>,
        part_delay: Duration,
        fail_part: Arc<AtomicBool>,
        fail_abort: Arc<AtomicBool>,
    }

    struct ActivePart(Arc<RequestStats>);

    impl Drop for ActivePart {
        fn drop(&mut self) {
            self.0.active_parts.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl MultipartUpload for TestMultipart {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let future = self.inner.put_part(data);
            let stats = Arc::clone(&self.stats);
            let delay = self.part_delay;
            let fail_part = Arc::clone(&self.fail_part);
            Box::pin(async move {
                stats.parts.fetch_add(1, Ordering::SeqCst);
                let active = stats.active_parts.fetch_add(1, Ordering::SeqCst) + 1;
                stats.maximum_parts.fetch_max(active, Ordering::SeqCst);
                let _active = ActivePart(Arc::clone(&stats));
                tokio::time::sleep(delay).await;
                if fail_part.load(Ordering::SeqCst) {
                    return Err(test_error("injected part failure"));
                }
                future.await
            })
        }

        async fn complete(&mut self) -> StoreResult<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> StoreResult<()> {
            self.stats.aborts.fetch_add(1, Ordering::SeqCst);
            if self.fail_abort.load(Ordering::SeqCst) {
                return Err(test_error("injected abort failure"));
            }
            self.inner.abort().await
        }
    }

    fn test_error(message: &'static str) -> StoreError {
        StoreError::Generic {
            store: "test",
            source: message.into(),
        }
    }

    fn config(part_size: usize, upload_concurrency: usize) -> StorageConfig {
        StorageConfig {
            max_requests: 64,
            upload_part_size: part_size,
            upload_concurrency,
        }
    }

    fn test_location(store: Arc<TestStore>, kind: StoreKind, path: &str) -> ObjectLocation {
        let store_url = match &kind {
            StoreKind::Local => ObjectStoreUrl::local_filesystem(),
            StoreKind::Gcs { bucket } => ObjectStoreUrl::parse(format!("gs://{bucket}")).unwrap(),
        };
        let handle = Arc::new(StoreHandle::new(kind, store_url, store));
        ObjectLocation::new(
            path.to_string(),
            format!("gs://bucket/{path}"),
            Path::from(path),
            handle,
        )
    }

    async fn gcs_output(
        store: Arc<TestStore>,
        path: &str,
        config: StorageConfig,
        policy: OutputPolicy,
    ) -> Result<ObjectOutput> {
        ObjectOutput::open(
            test_location(
                store,
                StoreKind::Gcs {
                    bucket: "bucket".to_string(),
                },
                path,
            ),
            config,
            policy,
        )
        .await
    }

    #[tokio::test]
    async fn small_output_uses_one_put_and_stays_private_until_commit() {
        let store = Arc::new(TestStore::new(true));
        let mut output = gcs_output(
            Arc::clone(&store),
            "small.bin",
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        output.write(Bytes::from_static(b"small")).await.unwrap();

        assert!(store.bytes("small.bin").await.is_none());

        let temporary = output.temporary().clone();
        output.commit().await.unwrap();
        assert_eq!(store.stats.puts.load(Ordering::SeqCst), 1);
        assert_eq!(store.stats.multiparts.load(Ordering::SeqCst), 0);
        assert_eq!(store.bytes("small.bin").await.unwrap(), "small");
        assert!(store.inner.head(&temporary).await.is_err());
    }

    #[tokio::test]
    async fn large_output_uses_multipart_with_configured_concurrency() {
        let store = Arc::new(TestStore::new(true).with_part_delay(Duration::from_millis(20)));
        let mut output = gcs_output(
            Arc::clone(&store),
            "large.bin",
            config(4, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        output
            .write(Bytes::from_static(b"abcdefghijkl"))
            .await
            .unwrap();

        assert!(store.bytes("large.bin").await.is_none());

        output.commit().await.unwrap();
        assert_eq!(store.stats.multiparts.load(Ordering::SeqCst), 1);
        assert_eq!(store.stats.parts.load(Ordering::SeqCst), 3);
        assert_eq!(store.stats.maximum_parts.load(Ordering::SeqCst), 2);
        assert_eq!(store.bytes("large.bin").await.unwrap(), "abcdefghijkl");
    }

    #[tokio::test]
    async fn create_only_preflight_rejects_existing_destination() {
        let store = Arc::new(TestStore::new(true));
        store
            .inner
            .put(&Path::from("existing.bin"), PutPayload::from_static(b"old"))
            .await
            .unwrap();

        let error = gcs_output(
            Arc::clone(&store),
            "existing.bin",
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("already exists"));
        assert_eq!(store.stats.heads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn create_only_commit_wins_or_rejects_a_race() {
        let store = Arc::new(TestStore::new(true));
        let mut output = gcs_output(
            Arc::clone(&store),
            "race.bin",
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        output.write(Bytes::from_static(b"new")).await.unwrap();
        store
            .inner
            .put(&Path::from("race.bin"), PutPayload::from_static(b"racer"))
            .await
            .unwrap();

        let error = output.commit().await.unwrap_err();

        assert!(error.to_string().contains("could not commit"));
        assert_eq!(store.bytes("race.bin").await.unwrap(), "racer");
        assert_eq!(
            *store.stats.copy_modes.lock().unwrap(),
            vec![CopyMode::Overwrite]
        );
        assert!(store.inner.head(output.temporary()).await.is_err());
    }

    #[tokio::test]
    async fn overwrite_replaces_the_destination() {
        let store = Arc::new(TestStore::new(true));
        store
            .inner
            .put(&Path::from("replace.bin"), PutPayload::from_static(b"old"))
            .await
            .unwrap();
        let mut output = gcs_output(
            Arc::clone(&store),
            "replace.bin",
            config(8, 2),
            OutputPolicy::new(true, false),
        )
        .await
        .unwrap();
        output.write(Bytes::from_static(b"new")).await.unwrap();

        output.commit().await.unwrap();

        assert_eq!(store.bytes("replace.bin").await.unwrap(), "new");
        assert_eq!(
            *store.stats.copy_modes.lock().unwrap(),
            vec![CopyMode::Create]
        );
    }

    #[tokio::test]
    async fn commit_failure_deletes_the_completed_temporary_object() {
        let store = Arc::new(TestStore::new(true));
        store.fail_copy.store(true, Ordering::SeqCst);
        let mut output = gcs_output(
            Arc::clone(&store),
            "failure.bin",
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        let temporary = output.temporary().clone();
        output.write(Bytes::from_static(b"bytes")).await.unwrap();

        let error = output.commit().await.unwrap_err();

        assert!(format!("{error:#}").contains("injected commit failure"));
        assert!(store.inner.head(&temporary).await.is_err());
        assert_eq!(output.state, ObjectOutputState::Aborted);
    }

    #[tokio::test]
    async fn commit_failure_retains_primary_error_when_cleanup_also_fails() {
        let store = Arc::new(TestStore::new(true));
        store.fail_copy.store(true, Ordering::SeqCst);
        store.fail_delete.store(true, Ordering::SeqCst);
        let mut output = gcs_output(
            Arc::clone(&store),
            "two-failures.bin",
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        let temporary = output.temporary().clone();
        output.write(Bytes::from_static(b"bytes")).await.unwrap();

        let error = output.commit().await.unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("injected commit failure"));
        assert!(message.contains("cleanup also failed"));
        assert!(message.contains("injected delete failure"));
        assert!(store.inner.head(&temporary).await.is_ok());

        store.fail_delete.store(false, Ordering::SeqCst);
        output.abort().await.unwrap();
        output.abort().await.unwrap();
        assert!(store.inner.head(&temporary).await.is_err());
    }

    #[tokio::test]
    async fn multipart_part_failure_aborts_and_preserves_cleanup_error() {
        let store = Arc::new(TestStore::new(true));
        store.fail_part.store(true, Ordering::SeqCst);
        store.fail_abort.store(true, Ordering::SeqCst);
        let mut output = gcs_output(
            Arc::clone(&store),
            "failure.bin",
            config(4, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();

        let error = output
            .write(Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("injected part failure"));
        assert!(message.contains("cleanup also failed"));
        assert!(message.contains("injected abort failure"));
        assert_eq!(store.stats.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(output.state, ObjectOutputState::Aborted);
    }

    #[tokio::test]
    async fn abort_is_idempotent() {
        let store = Arc::new(TestStore::new(true));
        let mut output = gcs_output(
            Arc::clone(&store),
            "abort.bin",
            config(4, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        output.write(Bytes::from_static(b"abcd")).await.unwrap();

        output.abort().await.unwrap();
        output.abort().await.unwrap();

        assert_eq!(store.stats.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(output.state, ObjectOutputState::Aborted);
    }

    #[tokio::test]
    async fn cancellation_schedules_multipart_abort() {
        let store = Arc::new(TestStore::new(true).with_part_delay(Duration::from_secs(30)));
        let output = gcs_output(
            Arc::clone(&store),
            "cancel.bin",
            config(4, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        let task = tokio::spawn(async move {
            let mut output = output;
            output.write(Bytes::from_static(b"abcd")).await.unwrap();
            futures::future::pending::<()>().await;
        });
        while store.stats.parts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        task.abort();
        let _ = task.await;
        for _ in 0..100 {
            if store.stats.aborts.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert_eq!(store.stats.aborts.load(Ordering::SeqCst), 1);
        assert!(store.bytes("cancel.bin").await.is_none());
    }

    #[tokio::test]
    async fn cancellation_during_commit_deletes_the_completed_temporary_object() {
        let store = Arc::new(TestStore::new(true).with_copy_delay(Duration::from_secs(30)));
        let mut output = gcs_output(
            Arc::clone(&store),
            "cancel-commit.bin",
            config(8, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        let temporary = output.temporary().clone();
        output.write(Bytes::from_static(b"small")).await.unwrap();
        let task = tokio::spawn(async move { output.commit().await });
        while store.stats.copy_modes.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }

        task.abort();
        let _ = task.await;
        for _ in 0..100 {
            if store.inner.head(&temporary).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert!(store.inner.head(&temporary).await.is_err());
        assert!(store.bytes("cancel-commit.bin").await.is_none());
    }

    #[tokio::test]
    async fn cancellation_during_single_put_deletes_a_visible_temporary_object() {
        let store =
            Arc::new(TestStore::new(true).with_put_response_delay(Duration::from_millis(50)));
        let mut output = gcs_output(
            Arc::clone(&store),
            "cancel-put.bin",
            config(8, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        let temporary = output.temporary().clone();
        output.write(Bytes::from_static(b"small")).await.unwrap();
        let task = tokio::spawn(async move { output.commit().await });
        while store.inner.head(&temporary).await.is_err() {
            tokio::task::yield_now().await;
        }

        task.abort();
        let _ = task.await;
        for _ in 0..100 {
            if store.inner.head(&temporary).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert!(store.inner.head(&temporary).await.is_err());
        assert!(store.bytes("cancel-put.bin").await.is_none());
    }

    #[tokio::test]
    async fn retained_owner_can_abort_a_canceled_single_put_finalizer() {
        let store =
            Arc::new(TestStore::new(true).with_put_response_delay(Duration::from_millis(50)));
        let mut output = gcs_output(
            Arc::clone(&store),
            "retained-put.bin",
            config(8, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        let temporary = output.temporary().clone();
        output.write(Bytes::from_static(b"small")).await.unwrap();
        let mut commit = Box::pin(output.commit());

        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut commit)
                .await
                .is_err()
        );
        drop(commit);
        assert!(store.inner.head(&temporary).await.is_ok());
        assert!(output.write(Bytes::from_static(b"more")).await.is_err());

        output.abort().await.unwrap();
        output.abort().await.unwrap();

        assert_eq!(output.state, ObjectOutputState::Aborted);
        assert!(store.inner.head(&temporary).await.is_err());
        assert!(store.bytes("retained-put.bin").await.is_none());
        assert!(output.commit().await.is_err());
    }

    #[tokio::test]
    async fn retained_owner_can_abort_a_canceled_multipart_finalizer() {
        let store = Arc::new(TestStore::new(true).with_part_delay(Duration::from_millis(50)));
        let mut output = gcs_output(
            Arc::clone(&store),
            "retained-multipart.bin",
            config(4, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        let temporary = output.temporary().clone();
        output.write(Bytes::from_static(b"abcd")).await.unwrap();
        let mut commit = Box::pin(output.commit());

        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut commit)
                .await
                .is_err()
        );
        drop(commit);
        assert!(output.write(Bytes::from_static(b"more")).await.is_err());

        output.abort().await.unwrap();
        output.abort().await.unwrap();

        assert_eq!(output.state, ObjectOutputState::Aborted);
        assert!(store.inner.head(&temporary).await.is_err());
        assert!(store.bytes("retained-multipart.bin").await.is_none());
        assert!(output.commit().await.is_err());
    }

    #[tokio::test]
    async fn retained_owner_can_resume_a_canceled_commit_finalizer() {
        let store =
            Arc::new(TestStore::new(true).with_put_response_delay(Duration::from_millis(30)));
        let mut output = gcs_output(
            Arc::clone(&store),
            "resumed-put.bin",
            config(8, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        output.write(Bytes::from_static(b"small")).await.unwrap();
        let mut commit = Box::pin(output.commit());

        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut commit)
                .await
                .is_err()
        );
        drop(commit);

        output.commit().await.unwrap();

        assert_eq!(output.state, ObjectOutputState::Committed);
        assert_eq!(store.bytes("resumed-put.bin").await.unwrap(), "small");
    }

    #[tokio::test]
    async fn local_parent_policy_creates_or_rejects_missing_directories() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("missing/deep/output.bin");
        let context = StorageContext::new(config(8, 2)).unwrap();
        let display = destination.to_string_lossy();

        let error = context
            .create_output(&display, OutputPolicy::new(false, false))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("--create-dirs"));

        let mut output = context
            .create_output(&display, OutputPolicy::new(false, true))
            .await
            .unwrap();
        output.write(Bytes::from_static(b"local")).await.unwrap();
        output.commit().await.unwrap();

        assert_eq!(std::fs::read(destination).unwrap(), b"local");
    }

    #[tokio::test]
    async fn gcs_prefixes_need_no_directory_creation() {
        let store = Arc::new(TestStore::new(true));
        let mut output = gcs_output(
            Arc::clone(&store),
            "missing/deep/output.bin",
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();
        output.write(Bytes::from_static(b"remote")).await.unwrap();
        output.commit().await.unwrap();

        assert_eq!(
            store.bytes("missing/deep/output.bin").await.unwrap(),
            "remote"
        );
    }

    #[tokio::test]
    async fn temporary_names_use_a_destination_owned_reserved_prefix() {
        let store = Arc::new(TestStore::new(true));
        let output = gcs_output(
            store,
            "output.bin",
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();

        assert!(
            output
                .temporary()
                .as_ref()
                .starts_with(&format!("{GCS_TEMPORARY_PREFIX}/output.bin-"))
        );
    }

    #[tokio::test]
    async fn temporary_names_fit_the_gcs_object_name_limit() {
        let store = Arc::new(TestStore::new(true));
        let destination = format!("owned/{}/output.bin", "é".repeat(500));
        let output = gcs_output(
            store,
            &destination,
            config(8, 2),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap();

        let temporary = output.temporary().as_ref();
        assert!(temporary.len() <= GCS_MAX_OBJECT_NAME_BYTES);
        assert!(temporary.starts_with(&format!("{GCS_TEMPORARY_PREFIX}/owned/")));
        let id = &temporary[temporary.len() - 36..];
        Uuid::parse_str(id).unwrap();
    }

    #[tokio::test]
    async fn temporary_name_truncation_cannot_create_a_relative_or_empty_segment() {
        let fixed = GCS_MAX_OBJECT_NAME_BYTES
            - GCS_TEMPORARY_PREFIX.len()
            - 1
            - format!("-{}", Uuid::nil()).len();
        for destination in [
            format!("{}/tail", "a".repeat(fixed - 1)),
            format!("{}/.tail", "a".repeat(fixed - 2)),
            format!("{}/..tail", "a".repeat(fixed - 3)),
        ] {
            let store = Arc::new(TestStore::new(true));
            let output = gcs_output(
                store,
                &destination,
                config(8, 2),
                OutputPolicy::new(false, false),
            )
            .await
            .unwrap();
            assert!(output.temporary().as_ref().len() <= GCS_MAX_OBJECT_NAME_BYTES);
        }
    }

    #[tokio::test]
    async fn rejects_zero_writer_settings() {
        let store = Arc::new(TestStore::new(true));
        let error = gcs_output(
            Arc::clone(&store),
            "output.bin",
            config(0, 1),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("part size"));

        let error = gcs_output(
            store,
            "output.bin",
            config(1, 0),
            OutputPolicy::new(false, false),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("concurrency"));
    }

    #[test]
    fn output_policy_records_directory_and_overwrite_rules() {
        let policy = OutputPolicy::new(true, false);

        assert!(policy.overwrite());
        assert!(!policy.create_dirs());
    }
}
