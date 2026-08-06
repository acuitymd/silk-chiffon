//! Per-command backend settings, routing, retry configuration, and object-store caching.
//!
//! A [`StorageSession`] belongs to one parsed command invocation. The registry has already fixed
//! backend membership and routes; session creation adds each backend's parsed settings and a fresh
//! object-store cache. Cloning a session shares that command-scoped state.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use object_store::{ObjectStore, RetryConfig};
use thiserror::Error;
use url::Url;

use crate::{
    LocationInput, RetryConfigurationError, StorageBackendBuildError, StorageDirection,
    StorageError, StorageHandle, StorageRegistryError, backend::BackendBinding,
    registry::RoutingIndex,
};

/// Storage state bound to one command invocation.
///
/// One session owns one parsed settings value per backend and one object-store cache. Its clones
/// share both through the same internal [`Arc`]. A separate call to
/// [`StorageRegistry::create_session`](crate::StorageRegistry::create_session) creates independent
/// session state with a fresh cache.
#[derive(Clone)]
pub struct StorageSession {
    state: Arc<SessionState>,
}

impl fmt::Debug for StorageSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageSession")
            .field("backends", &self.state.backends.len())
            .field("retry", &self.state.retry)
            .field(
                "cached_object_stores",
                &self
                    .state
                    .object_store_cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish()
    }
}

struct SessionState {
    backends: Box<[Box<dyn BackendBinding>]>,
    routing: Arc<RoutingIndex>,
    retry: Option<RetryConfig>,
    object_store_cache: Mutex<HashMap<Url, Arc<dyn ObjectStore>>>,
}

impl StorageSession {
    pub(crate) fn new(
        backends: Box<[Box<dyn BackendBinding>]>,
        routing: Arc<RoutingIndex>,
        retry: Option<RetryConfig>,
    ) -> Self {
        Self {
            state: Arc::new(SessionState {
                backends,
                routing,
                retry,
                object_store_cache: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Returns the validated shared retry configuration for this command invocation.
    ///
    /// This is `None` when no registered backend requested shared retries.
    pub fn retry_configuration(&self) -> Option<&RetryConfig> {
        self.state.retry.as_ref()
    }

    /// Creates a handle for reading without checking whether the object exists.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when no backend owns the route, the selected backend rejects input,
    /// a bare mapper returns a scheme not owned by that backend, or a backend callback fails.
    pub fn input_handle(&self, input: &LocationInput) -> Result<StorageHandle, StorageError> {
        self.create_handle(input, StorageDirection::Input)
    }

    /// Creates a handle for writing without checking existence or overwrite policy.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when no backend owns the route, the selected backend rejects
    /// output, a bare mapper returns a scheme not owned by that backend, or a callback fails.
    pub fn output_handle(&self, input: &LocationInput) -> Result<StorageHandle, StorageError> {
        self.create_handle(input, StorageDirection::Output)
    }

    fn create_handle(
        &self,
        input: &LocationInput,
        direction: StorageDirection,
    ) -> Result<StorageHandle, StorageError> {
        let backend_index = match input {
            LocationInput::Url(location) => self
                .state
                .routing
                .backend_index_by_scheme
                .get(location.url().scheme())
                .copied()
                .ok_or_else(|| {
                    StorageError::UnsupportedScheme(location.url().scheme().to_owned())
                })?,
            LocationInput::Bare(bare_location) => self
                .state
                .routing
                .bare_location_backend_index
                .ok_or_else(|| StorageError::UnsupportedBareLocation(bare_location.clone()))?,
        };
        let backend = &self.state.backends[backend_index];
        if !backend.supports(direction) {
            return Err(StorageError::DirectionUnsupported {
                backend: backend.name(),
                direction,
            });
        }

        let location = match input {
            LocationInput::Url(location) => location.clone(),
            LocationInput::Bare(bare_location) => backend
                .map_bare_location(bare_location)
                .expect("the indexed bare-location backend must have a mapper")
                .map_err(|source| StorageError::BareLocationMapping {
                    backend: backend.name(),
                    bare_location: bare_location.clone(),
                    source,
                })?,
        };
        let scheme = location.url().scheme();
        if self
            .state
            .routing
            .backend_index_by_scheme
            .get(scheme)
            .copied()
            != Some(backend_index)
        {
            return Err(StorageError::BareLocationSchemeMismatch {
                backend: backend.name(),
                scheme: scheme.to_owned(),
            });
        }

        let object_path = backend.map_object_path(&location).map_err(|source| {
            StorageError::ObjectPathMapping {
                backend: backend.name(),
                location: location.url().clone(),
                source,
            }
        })?;
        let store_url = store_url(location.url());

        let mut object_store_cache = self
            .state
            .object_store_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let object_store = match object_store_cache.entry(store_url.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => Arc::clone(entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let retry = if backend.uses_shared_retries() {
                    self.state.retry.as_ref()
                } else {
                    None
                };
                // The lock spans construction so concurrent requests cannot create duplicate
                // clients for the same store URL.
                let object_store =
                    backend
                        .create_object_store(&store_url, retry)
                        .map_err(|source| StorageError::ObjectStoreCreation {
                            backend: backend.name(),
                            store_url: store_url.clone(),
                            source,
                        })?;
                Arc::clone(entry.insert(object_store))
            }
        };

        Ok(StorageHandle::new(
            location.url().clone(),
            object_store,
            object_path,
            store_url,
        ))
    }
}

/// Errors that can occur while composing or creating a storage session.
#[derive(Debug, Error)]
pub enum StorageSessionCreationError {
    #[error(transparent)]
    Backend(#[from] StorageBackendBuildError),
    #[error(transparent)]
    Registry(#[from] StorageRegistryError),
    #[error(transparent)]
    Arguments(#[from] clap::Error),
    #[error(transparent)]
    Retry(#[from] RetryConfigurationError),
}

fn store_url(url: &Url) -> Url {
    let mut store_url = url.clone();
    store_url.set_path("/");
    store_url.set_query(None);
    store_url.set_fragment(None);
    store_url
}
