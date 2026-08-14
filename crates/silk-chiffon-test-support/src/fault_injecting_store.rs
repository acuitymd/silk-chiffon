//! Deterministic failures around any object-store implementation.

use std::{
    collections::{BTreeSet, HashMap},
    fmt, io,
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, path::Path,
};

/// An independently faultable call in the [`ObjectStore`] contract.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObjectStoreOperation {
    /// A single-object put.
    Put,
    /// Creation of a multipart upload.
    MultipartStart,
    /// One part of an established multipart upload.
    MultipartPart,
    /// Completion of an established multipart upload.
    MultipartComplete,
    /// Abortion of an established multipart upload.
    MultipartAbort,
    /// A metadata, object, or range get expressed through `get_opts`.
    Get,
    /// A multi-range get expressed through `get_ranges`.
    GetRanges,
    /// A bulk or single-object delete expressed through `delete_stream`.
    Delete,
    /// A recursive listing.
    List,
    /// A recursive listing after an exclusive offset.
    ListWithOffset,
    /// A delimiter-based listing.
    ListWithDelimiter,
    /// A copy.
    Copy,
    /// A rename.
    Rename,
}

impl fmt::Display for ObjectStoreOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Put => "put",
            Self::MultipartStart => "multipart start",
            Self::MultipartPart => "multipart part",
            Self::MultipartComplete => "multipart complete",
            Self::MultipartAbort => "multipart abort",
            Self::Get => "get",
            Self::GetRanges => "get ranges",
            Self::Delete => "delete",
            Self::List => "list",
            Self::ListWithOffset => "list with offset",
            Self::ListWithDelimiter => "list with delimiter",
            Self::Copy => "copy",
            Self::Rename => "rename",
        })
    }
}

#[derive(Debug, Default)]
struct OperationState {
    calls: usize,
    failing_calls: BTreeSet<usize>,
}

#[derive(Debug, Default)]
struct FaultState {
    operations: Mutex<HashMap<ObjectStoreOperation, OperationState>>,
}

impl FaultState {
    fn fail_after(&self, operation: ObjectStoreOperation, successful_calls: usize) {
        let mut operations = self.operations.lock().expect("fault state poisoned");
        let state = operations.entry(operation).or_default();
        let mut failing_call = state
            .calls
            .checked_add(successful_calls)
            .and_then(|call| call.checked_add(1))
            .expect("fault call number overflowed");
        while !state.failing_calls.insert(failing_call) {
            failing_call = failing_call
                .checked_add(1)
                .expect("fault call number overflowed");
        }
    }

    fn check(&self, operation: ObjectStoreOperation) -> object_store::Result<()> {
        let call = {
            let mut operations = self.operations.lock().expect("fault state poisoned");
            let state = operations.entry(operation).or_default();
            state.calls += 1;
            let call = state.calls;
            if !state.failing_calls.remove(&call) {
                return Ok(());
            }
            call
        };
        Err(injected_error(operation, call))
    }

    fn calls(&self, operation: ObjectStoreOperation) -> usize {
        self.operations
            .lock()
            .expect("fault state poisoned")
            .get(&operation)
            .map_or(0, |state| state.calls)
    }

    fn reset(&self) {
        self.operations
            .lock()
            .expect("fault state poisoned")
            .clear();
    }
}

/// Wraps an object store with deterministic, operation-scoped failures.
///
/// Faults are consumed before the wrapped operation begins. This makes it
/// possible to assert that rejected mutations had no hidden side effects.
/// Clones share call counts and scheduled faults.
#[derive(Clone, Debug)]
pub struct FaultInjectingStore {
    inner: Arc<dyn ObjectStore>,
    faults: Arc<FaultState>,
}

impl FaultInjectingStore {
    /// Wraps `inner` with a new independent fault schedule.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            faults: Arc::new(FaultState::default()),
        }
    }

    /// Fails the next call to `operation` once.
    pub fn fail_next(&self, operation: ObjectStoreOperation) {
        self.fail_after(operation, 0);
    }

    /// Fails once after `successful_calls` more calls to `operation`.
    pub fn fail_after(&self, operation: ObjectStoreOperation, successful_calls: usize) {
        self.faults.fail_after(operation, successful_calls);
    }

    /// Returns calls that reached the wrapper for `operation`.
    pub fn calls(&self, operation: ObjectStoreOperation) -> usize {
        self.faults.calls(operation)
    }

    /// Clears all call counts and unused faults.
    pub fn reset(&self) {
        self.faults.reset();
    }
}

impl fmt::Display for FaultInjectingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "FaultInjectingStore({})", self.inner)
    }
}

#[derive(Debug)]
struct FaultInjectingUpload {
    inner: Box<dyn MultipartUpload>,
    faults: Arc<FaultState>,
}

#[async_trait]
impl MultipartUpload for FaultInjectingUpload {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        if let Err(error) = self.faults.check(ObjectStoreOperation::MultipartPart) {
            return Box::pin(async { Err(error) });
        }
        self.inner.put_part(payload)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.faults.check(ObjectStoreOperation::MultipartComplete)?;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.faults.check(ObjectStoreOperation::MultipartAbort)?;
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for FaultInjectingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.faults.check(ObjectStoreOperation::Put)?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.faults.check(ObjectStoreOperation::MultipartStart)?;
        Ok(Box::new(FaultInjectingUpload {
            inner: self.inner.put_multipart_opts(location, options).await?,
            faults: Arc::clone(&self.faults),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.faults.check(ObjectStoreOperation::Get)?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.faults.check(ObjectStoreOperation::GetRanges)?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        if let Err(error) = self.faults.check(ObjectStoreOperation::Delete) {
            return stream::once(async { Err(error) }).boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(error) = self.faults.check(ObjectStoreOperation::List) {
            return stream::once(async { Err(error) }).boxed();
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(error) = self.faults.check(ObjectStoreOperation::ListWithOffset) {
            return stream::once(async { Err(error) }).boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.faults.check(ObjectStoreOperation::ListWithDelimiter)?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.faults.check(ObjectStoreOperation::Copy)?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.faults.check(ObjectStoreOperation::Rename)?;
        self.inner.rename_opts(from, to, options).await
    }
}

fn injected_error(operation: ObjectStoreOperation, call: usize) -> object_store::Error {
    object_store::Error::Generic {
        store: "fault-injecting",
        source: Box::new(io::Error::other(format!(
            "injected {operation} failure on call {call}"
        ))),
    }
}
