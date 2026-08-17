//! Directional access to one canonical storage location.

use std::{fmt, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    path::Path as ObjectPath,
};
use url::Url;

use crate::{StorageError, upload::ObjectUploadContext};

/// Shared storage state before its access direction is exposed publicly.
#[derive(Clone)]
pub(crate) struct StorageHandle {
    url: Url,
    object_store: Arc<dyn ObjectStore>,
    read_only_store: Arc<dyn ObjectStore>,
    object_path: ObjectPath,
    store_url: Url,
    pub(crate) object_upload_context: Arc<ObjectUploadContext>,
}

impl StorageHandle {
    pub(crate) fn new(
        url: Url,
        object_store: Arc<dyn ObjectStore>,
        read_only_store: Arc<dyn ObjectStore>,
        object_path: ObjectPath,
        store_url: Url,
        object_upload_context: Arc<ObjectUploadContext>,
    ) -> Self {
        Self {
            url,
            object_store,
            read_only_store,
            object_path,
            store_url,
            object_upload_context,
        }
    }

    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn object_path(&self) -> &ObjectPath {
        &self.object_path
    }

    pub(crate) fn store_url(&self) -> &Url {
        &self.store_url
    }

    pub(crate) fn local_path(&self) -> Result<PathBuf, StorageError> {
        if self.url.scheme() != "file" {
            return Err(StorageError::InvalidFilePath(PathBuf::from(
                self.url.as_str(),
            )));
        }
        self.url
            .to_file_path()
            .map_err(|()| StorageError::InvalidFilePath(PathBuf::from(self.url.as_str())))
    }
}

macro_rules! directional_accessors {
    ($type:ty) => {
        impl $type {
            /// Returns the canonical URL for this exact location, including its query.
            pub fn url(&self) -> &Url {
                self.handle.url()
            }

            /// Returns the path used for operations against this location's object store.
            pub fn object_path(&self) -> &ObjectPath {
                self.handle.object_path()
            }

            /// Returns the scheme-and-authority root used as this session's store cache key.
            pub fn store_url(&self) -> &Url {
                self.handle.store_url()
            }
        }
    };
}

macro_rules! local_path_accessor {
    ($type:ty) => {
        impl $type {
            /// Converts a `file:` URL into a filesystem path.
            ///
            /// # Errors
            ///
            /// Returns [`StorageError::InvalidFilePath`] when the URL is not a representable
            /// local file URL.
            pub fn local_path(&self) -> Result<PathBuf, StorageError> {
                self.handle.local_path()
            }
        }
    };
}

/// One exact location selected for input access.
///
/// The exposed object store permits reads and listing but rejects mutation. This keeps an input
/// handle from being reused as an output capability while still satisfying DataFusion's
/// object-store interface.
#[derive(Clone)]
pub struct InputHandle {
    handle: StorageHandle,
    read_only_store: Arc<dyn ObjectStore>,
}

impl InputHandle {
    pub(crate) fn new(handle: StorageHandle) -> Self {
        let read_only_store = Arc::clone(&handle.read_only_store);
        Self {
            handle,
            read_only_store,
        }
    }

    /// Returns shared ownership of the read-only object-store view.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.read_only_store)
    }

    pub(crate) fn inner_object_store(&self) -> Arc<dyn ObjectStore> {
        self.handle.object_store()
    }
}

pub(crate) fn read_only_store(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(ReadOnlyObjectStore { inner })
}

directional_accessors!(InputHandle);
local_path_accessor!(InputHandle);

impl fmt::Debug for InputHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InputHandle")
            .field("url", &self.url())
            .field("object_path", &self.object_path())
            .field("store_url", &self.store_url())
            .finish_non_exhaustive()
    }
}

/// One selected and claimed output passed to backend preparation code.
///
/// Only a storage session can construct this value. A successful backend preparation converts it
/// into [`PreparedOutputTarget`] before application or format code can open a writer.
pub struct OutputTarget {
    handle: StorageHandle,
}

impl OutputTarget {
    pub(crate) fn new(handle: StorageHandle) -> Self {
        Self { handle }
    }

    /// Returns shared ownership of the writable object-store client.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.handle.object_store()
    }
}

directional_accessors!(OutputTarget);
local_path_accessor!(OutputTarget);

impl fmt::Debug for OutputTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutputTarget")
            .field("url", &self.url())
            .field("object_path", &self.object_path())
            .field("store_url", &self.store_url())
            .finish_non_exhaustive()
    }
}

/// One output target whose claim, existence policy, and backend preparation have succeeded.
#[derive(Clone)]
pub struct PreparedOutputTarget {
    handle: StorageHandle,
}

impl PreparedOutputTarget {
    pub(crate) fn new(target: OutputTarget) -> Self {
        Self {
            handle: target.handle,
        }
    }

    /// Returns shared ownership of the writable object-store client.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.handle.object_store()
    }

    pub(crate) fn into_handle(self) -> StorageHandle {
        self.handle
    }
}

directional_accessors!(PreparedOutputTarget);

impl fmt::Debug for PreparedOutputTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedOutputTarget")
            .field("url", &self.url())
            .field("object_path", &self.object_path())
            .field("store_url", &self.store_url())
            .field(
                "object_upload_settings",
                &self.handle.object_upload_context.settings,
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ReadOnlyObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl fmt::Display for ReadOnlyObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "read-only {}", self.inner)
    }
}

impl ReadOnlyObjectStore {
    fn unsupported<T>(&self, operation: &str) -> StoreResult<T> {
        Err(object_store::Error::NotImplemented {
            operation: operation.to_owned(),
            implementer: "SilkInputHandle".to_owned(),
        })
    }
}

#[async_trait]
impl ObjectStore for ReadOnlyObjectStore {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.unsupported("put_opts")
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.unsupported("put_multipart_opts")
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[std::ops::Range<u64>],
    ) -> StoreResult<Vec<bytes::Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, StoreResult<ObjectPath>>,
    ) -> BoxStream<'static, StoreResult<ObjectPath>> {
        Box::pin(futures::stream::once(async {
            Err(object_store::Error::NotImplemented {
                operation: "delete_stream".to_owned(),
                implementer: "SilkInputHandle".to_owned(),
            })
        }))
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> StoreResult<()> {
        self.unsupported("copy_opts")
    }
}
