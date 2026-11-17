//! Per-command backend settings, routing, retry configuration, and object-store caching.
//!
//! A [`StorageSession`] belongs to one parsed command invocation. The registry has already fixed
//! backend membership and routes; session creation adds each backend's parsed settings and a fresh
//! object-store cache. Cloning a session shares that command-scoped state.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use futures::TryStreamExt;
use glob::MatchOptions;
use object_store::{ObjectStore, ObjectStoreExt, RetryConfig, path::Path as ObjectPath};
use parking_lot::Mutex;
use thiserror::Error;
use url::{Position, Url};

use crate::{
    ExistingOutput, InputHandle, InputObject, Location, LocationInput, LocationPattern,
    OutputPreparation, OutputTarget, PreparedOutputTarget, RetryConfigurationError,
    StorageBackendBuildError, StorageDirection, StorageError, StorageRegistryError,
    backend::BackendBinding,
    handle::StorageHandle,
    pattern::PatternInput,
    registry::RoutingIndex,
    upload::{ObjectUploadContext, ObjectUploadSettings},
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
                "object_upload_settings",
                &self.state.object_upload_context.settings,
            )
            .field(
                "cached_object_stores",
                &self.state.object_store_cache.lock().len(),
            )
            .finish()
    }
}

struct SessionState {
    backends: Box<[Box<dyn BackendBinding>]>,
    routing: Arc<RoutingIndex>,
    retry: Option<RetryConfig>,
    object_store_cache: Mutex<HashMap<Url, CachedObjectStore>>,
    object_upload_context: Arc<ObjectUploadContext>,
    claimed_output_targets: Mutex<HashSet<OutputTargetIdentity>>,
}

#[derive(Clone)]
struct CachedObjectStore {
    writable: Arc<dyn ObjectStore>,
    read_only: Arc<dyn ObjectStore>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OutputTargetIdentity {
    store_url: Url,
    object_path: ObjectPath,
}

impl StorageSession {
    pub(crate) fn new(
        backends: Box<[Box<dyn BackendBinding>]>,
        routing: Arc<RoutingIndex>,
        retry: Option<RetryConfig>,
        object_upload_settings: ObjectUploadSettings,
    ) -> Self {
        let object_upload_context = Arc::new(ObjectUploadContext::new(object_upload_settings));
        Self {
            state: Arc::new(SessionState {
                backends,
                routing,
                retry,
                object_store_cache: Mutex::new(HashMap::new()),
                object_upload_context,
                claimed_output_targets: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Creates a handle for reading without checking whether the object exists.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when no backend owns the route, the selected backend rejects input,
    /// a bare mapper returns a scheme not owned by that backend, or a backend callback fails.
    pub fn input_handle(&self, input: &LocationInput) -> Result<InputHandle, StorageError> {
        self.create_handle(input, StorageDirection::Input)
            .map(InputHandle::new)
    }

    /// Resolves an exact input and records the object's current metadata.
    ///
    /// The returned metadata is an observation, not a snapshot or reservation. The object must
    /// remain stable for the command's lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when handle creation or the metadata request fails.
    pub async fn lookup_input(&self, input: &LocationInput) -> Result<InputObject, StorageError> {
        let handle = self.input_handle(input)?;
        let metadata = handle.object_store().head(handle.object_path()).await?;
        Ok(InputObject::new(handle, metadata))
    }

    /// Resolves, claims, checks, and performs backend preparation for one output target.
    ///
    /// The claim is retained for the session lifetime even when later preparation fails. External
    /// existence checks are advisory and do not reserve the object against another process.
    pub async fn prepare_output_target(
        &self,
        input: &LocationInput,
        preparation: &OutputPreparation,
    ) -> Result<PreparedOutputTarget, StorageError> {
        let target = OutputTarget::new(self.create_handle(input, StorageDirection::Output)?);
        let identity = OutputTargetIdentity {
            store_url: target.store_url().clone(),
            object_path: target.object_path().clone(),
        };
        if !self.state.claimed_output_targets.lock().insert(identity) {
            return Err(StorageError::OutputTargetAlreadyClaimed {
                target: target.url().clone(),
            });
        }

        if preparation.existing_output() == ExistingOutput::RejectIfObserved {
            match target.object_store().head(target.object_path()).await {
                Ok(_) => {
                    return Err(StorageError::OutputTargetAlreadyExists {
                        target: target.url().clone(),
                    });
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(source) => return Err(source.into()),
            }
        }

        let backend_index = self.backend_index_for_url(target.url())?;
        let backend = &self.state.backends[backend_index];
        backend
            .prepare_output_target(&target, preparation)
            .await
            .map_err(|source| StorageError::OutputTargetPreparation {
                backend: backend.name(),
                target: target.url().clone(),
                source,
            })?;
        Ok(PreparedOutputTarget::new(target))
    }

    /// Expands one exact location or object-path glob into zero or more input objects.
    ///
    /// Exact patterns perform one metadata request without listing. Active globs list from their
    /// longest complete literal-segment prefix and match complete canonical object paths. The
    /// returned order is unspecified; callers own no-match policy, ordering, and deduplication.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when routing, backend validation, metadata, or listing fails.
    pub async fn expand_input_pattern(
        &self,
        pattern: &LocationPattern,
    ) -> Result<Vec<InputObject>, StorageError> {
        match &pattern.input {
            PatternInput::Exact(input) => self.expand_exact_pattern(input).await,
            PatternInput::Bare { source, .. } => {
                let backend_index = self
                    .state
                    .routing
                    .bare_location_backend_index
                    .ok_or_else(|| StorageError::UnsupportedBarePattern(source.clone()))?;
                let backend = &self.state.backends[backend_index];
                if !backend.supports(StorageDirection::Input) {
                    return Err(StorageError::DirectionUnsupported {
                        backend: backend.name(),
                        direction: StorageDirection::Input,
                    });
                }
                let mapped = backend
                    .map_bare_pattern(source)
                    .ok_or_else(|| StorageError::UnsupportedBarePattern(source.clone()))?
                    .map_err(|source_error| StorageError::BarePatternMapping {
                        backend: backend.name(),
                        bare_pattern: source.clone(),
                        source: source_error,
                    })?;
                self.expand_mapped_pattern(&mapped, backend_index).await
            }
            PatternInput::Url { .. } => self.expand_url_pattern(pattern, None).await,
        }
    }

    async fn expand_mapped_pattern(
        &self,
        pattern: &LocationPattern,
        backend_index: usize,
    ) -> Result<Vec<InputObject>, StorageError> {
        match &pattern.input {
            PatternInput::Exact(LocationInput::Url(location)) => {
                self.require_pattern_backend(location, backend_index)?;
                let handle = InputHandle::new(self.create_handle_for_location(
                    location,
                    backend_index,
                    StorageDirection::Input,
                )?);
                self.head_pattern(handle, location.url().as_str()).await
            }
            PatternInput::Url { location, .. } => {
                self.require_pattern_backend(location, backend_index)?;
                self.expand_url_pattern(pattern, Some(backend_index)).await
            }
            PatternInput::Exact(LocationInput::Bare(_)) | PatternInput::Bare { .. } => {
                Err(StorageError::BarePatternSchemeMismatch {
                    backend: self.state.backends[backend_index].name(),
                    scheme: "bare".to_owned(),
                })
            }
        }
    }

    async fn expand_exact_pattern(
        &self,
        input: &LocationInput,
    ) -> Result<Vec<InputObject>, StorageError> {
        let source = match input {
            LocationInput::Url(location) => location.url().as_str(),
            LocationInput::Bare(source) => source,
        };
        let handle = self.input_handle(input)?;
        self.head_pattern(handle, source).await
    }

    async fn head_pattern(
        &self,
        handle: InputHandle,
        pattern: &str,
    ) -> Result<Vec<InputObject>, StorageError> {
        match handle.object_store().head(handle.object_path()).await {
            Ok(metadata) => Ok(vec![InputObject::new(handle, metadata)]),
            Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(source) => Err(StorageError::PatternMetadata {
                pattern: pattern.to_owned(),
                source,
            }),
        }
    }

    async fn expand_url_pattern(
        &self,
        pattern: &LocationPattern,
        expected_backend_index: Option<usize>,
    ) -> Result<Vec<InputObject>, StorageError> {
        let PatternInput::Url {
            source,
            location,
            matcher,
            literal_prefix,
        } = &pattern.input
        else {
            unreachable!("caller passes an explicit active pattern");
        };
        let backend_index = match expected_backend_index {
            Some(index) => index,
            None => self.backend_index_for_location(location)?,
        };
        let pattern_handle = InputHandle::new(self.create_handle_for_location(
            location,
            backend_index,
            StorageDirection::Input,
        )?);
        let listing_prefix = if literal_prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::parse(literal_prefix).map_err(|source| {
                StorageError::InvalidObjectPath {
                    location: location.url().clone(),
                    source: Box::new(source),
                }
            })?)
        };
        let options = MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: false,
        };
        let mut listed = pattern_handle.object_store().list(listing_prefix.as_ref());
        let mut handles = Vec::new();
        while let Some(metadata) =
            listed
                .try_next()
                .await
                .map_err(|source_error| StorageError::PatternListing {
                    pattern: source.clone(),
                    source: source_error,
                })?
        {
            if !matcher.matches_with(metadata.location.as_ref(), options) {
                continue;
            }
            let url = matched_url(location.url(), &metadata.location)?;
            let matched_location = Location::parse_url(url.as_str())?;
            let backend = &self.state.backends[backend_index];
            backend
                .validate_location(&matched_location)
                .map_err(|source| StorageError::LocationValidation {
                    backend: backend.name(),
                    location: url.clone(),
                    source,
                })?;
            let handle = InputHandle::new(StorageHandle::new(
                url,
                pattern_handle.inner_object_store(),
                pattern_handle.object_store(),
                metadata.location.clone(),
                pattern_handle.store_url().clone(),
                Arc::clone(&self.state.object_upload_context),
            ));
            handles.push(InputObject::new(handle, metadata));
        }
        Ok(handles)
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

        self.create_handle_for_location(&location, backend_index, direction)
    }

    fn backend_index_for_location(&self, location: &Location) -> Result<usize, StorageError> {
        self.backend_index_for_url(location.url())
    }

    fn backend_index_for_url(&self, url: &Url) -> Result<usize, StorageError> {
        self.state
            .routing
            .backend_index_by_scheme
            .get(url.scheme())
            .copied()
            .ok_or_else(|| StorageError::UnsupportedScheme(url.scheme().to_owned()))
    }

    fn require_pattern_backend(
        &self,
        location: &Location,
        backend_index: usize,
    ) -> Result<(), StorageError> {
        if self
            .state
            .routing
            .backend_index_by_scheme
            .get(location.url().scheme())
            .copied()
            != Some(backend_index)
        {
            return Err(StorageError::BarePatternSchemeMismatch {
                backend: self.state.backends[backend_index].name(),
                scheme: location.url().scheme().to_owned(),
            });
        }
        Ok(())
    }

    fn create_handle_for_location(
        &self,
        location: &Location,
        backend_index: usize,
        direction: StorageDirection,
    ) -> Result<StorageHandle, StorageError> {
        let backend = &self.state.backends[backend_index];
        if !backend.supports(direction) {
            return Err(StorageError::DirectionUnsupported {
                backend: backend.name(),
                direction,
            });
        }
        backend
            .validate_location(location)
            .map_err(|source| StorageError::LocationValidation {
                backend: backend.name(),
                location: location.url().clone(),
                source,
            })?;
        let object_path = ObjectPath::from_url_path(location.url().path()).map_err(|source| {
            StorageError::InvalidObjectPath {
                location: location.url().clone(),
                source: Box::new(source),
            }
        })?;
        let store_url = store_url(location.url());

        let mut object_store_cache = self.state.object_store_cache.lock();
        let object_store = match object_store_cache.entry(store_url.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let retry = if backend.uses_shared_retries() {
                    self.state.retry.as_ref()
                } else {
                    None
                };
                // The lock spans construction so concurrent requests cannot create duplicate
                // clients for the same store URL.
                let writable =
                    backend
                        .create_object_store(&store_url, retry)
                        .map_err(|source| StorageError::ObjectStoreCreation {
                            backend: backend.name(),
                            store_url: store_url.clone(),
                            source,
                        })?;
                let object_store = CachedObjectStore {
                    read_only: crate::handle::read_only_store(Arc::clone(&writable)),
                    writable,
                };
                entry.insert(object_store).clone()
            }
        };

        Ok(StorageHandle::new(
            location.url().clone(),
            object_store.writable,
            object_store.read_only,
            object_path,
            store_url,
            Arc::clone(&self.state.object_upload_context),
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

fn matched_url(base: &Url, object_path: &ObjectPath) -> Result<Url, StorageError> {
    let mut exact = base.clone();
    let query = exact.query().map(str::to_owned);
    exact.set_query(None);
    exact.set_path("/");
    {
        let mut segments = exact
            .path_segments_mut()
            .expect("routed storage URLs are hierarchical");
        segments.clear();
        segments.extend(object_path.parts().map(|part| part.as_ref().to_owned()));
    }
    let mut source = exact[..Position::BeforePath].to_owned();
    source.push_str(
        &exact[Position::BeforePath..Position::AfterPath]
            .replace('*', "%2A")
            .replace('[', "%5B")
            .replace(']', "%5D"),
    );
    if let Some(query) = query {
        source.push('?');
        source.push_str(&query);
    }
    Location::parse_url(&source).map(|location| location.url().clone())
}
