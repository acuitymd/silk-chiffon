//! Exact input objects resolved for one command invocation.

use object_store::ObjectMeta;

use crate::StorageHandle;

/// An exact input handle and the metadata observed while resolving it.
///
/// The metadata is not a snapshot or reservation. Callers require the object to remain stable for
/// the command's lifetime.
#[derive(Clone, Debug)]
pub struct InputObject {
    handle: StorageHandle,
    metadata: ObjectMeta,
}

impl InputObject {
    pub(crate) fn new(handle: StorageHandle, metadata: ObjectMeta) -> Self {
        Self { handle, metadata }
    }

    /// Returns the storage handle for this exact object.
    pub fn handle(&self) -> &StorageHandle {
        &self.handle
    }

    /// Returns the metadata observed while resolving this object.
    pub fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}
