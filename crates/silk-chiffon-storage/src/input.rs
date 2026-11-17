//! Exact input objects resolved for one command invocation.

use object_store::ObjectMeta;

use crate::InputHandle;

/// An exact input handle and the metadata observed while resolving it.
///
/// The metadata is not a snapshot or reservation. Callers require the object to remain stable for
/// the command's lifetime.
#[derive(Clone, Debug)]
pub struct InputObject {
    input_handle: InputHandle,
    metadata: ObjectMeta,
}

impl InputObject {
    pub(crate) fn new(input_handle: InputHandle, metadata: ObjectMeta) -> Self {
        Self {
            input_handle,
            metadata,
        }
    }

    /// Returns the input handle for this exact object.
    pub fn input_handle(&self) -> &InputHandle {
        &self.input_handle
    }

    /// Returns the metadata observed while resolving this object.
    pub fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}
