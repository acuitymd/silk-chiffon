//! A selected object store paired with one canonical location and object path.
//!
//! [`StorageHandle`] is the value returned after a session chooses a backend and invokes its typed
//! callbacks. It keeps the exact location URL, the backend's object path, the shared object-store
//! client, and the root URL used for session caching and DataFusion registration.

use std::{path::PathBuf, sync::Arc};

use object_store::{ObjectStore, path::Path as ObjectPath};
use url::Url;

use crate::{StorageError, upload::ObjectUploadContext};

/// One exact location paired with the client and object path needed to access it.
///
/// The fields are private so callers cannot accidentally combine a URL, path, and client from
/// different handle requests.
#[derive(Clone)]
pub struct StorageHandle {
    url: Url,
    object_store: Arc<dyn ObjectStore>,
    object_path: ObjectPath,
    store_url: Url,
    pub(crate) object_upload_context: Arc<ObjectUploadContext>,
}

impl std::fmt::Debug for StorageHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageHandle")
            .field("url", &self.url)
            .field("object_store", &self.object_store)
            .field("object_path", &self.object_path)
            .field("store_url", &self.store_url)
            .field(
                "object_upload_settings",
                &self.object_upload_context.settings,
            )
            .finish()
    }
}

impl StorageHandle {
    pub(crate) fn new(
        url: Url,
        object_store: Arc<dyn ObjectStore>,
        object_path: ObjectPath,
        store_url: Url,
        object_upload_context: Arc<ObjectUploadContext>,
    ) -> Self {
        Self {
            url,
            object_store,
            object_path,
            store_url,
            object_upload_context,
        }
    }

    /// Returns the canonical URL for the exact location, including its query.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns shared ownership of the object-store client selected for this handle.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    /// Returns the path used for operations against [`Self::object_store`].
    pub fn object_path(&self) -> &ObjectPath {
        &self.object_path
    }

    /// Returns the root URL used for session caching and external object-store registration.
    ///
    /// The URL retains the scheme, host, and port. Its path is `/`, and it has no query or
    /// fragment.
    pub fn store_url(&self) -> &Url {
        &self.store_url
    }

    /// Converts a `file:` handle URL into a filesystem path.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidFilePath`] when the URL is not a representable local file
    /// URL.
    pub fn local_path(&self) -> Result<PathBuf, StorageError> {
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
