//! Validation, routing indexes, and host-owned Clap command composition.
//!
//! A [`StorageRegistry`] is definition-time configuration. It contains available backend
//! definitions but no settings parsed for one command invocation. Building a registry validates
//! backend names, URL schemes, CLI keys, and the bare-location route before constructing indexes
//! that a later [`StorageSession`] can reuse.

use std::{collections::HashMap, fmt, sync::Arc};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use thiserror::Error;

use crate::{
    ObjectUploadArgs, RetryArgs, StorageBackend, StorageSession, StorageSessionCreationError,
    backend::{CliArgumentKey, argument_keys},
};

/// Collects backend definitions before validating their names, schemes, CLI keys, and bare route.
#[derive(Debug, Default)]
pub struct StorageRegistryBuilder {
    backends: Vec<StorageBackend>,
}

impl StorageRegistryBuilder {
    /// Adds one available backend in registry and CLI composition order.
    pub fn register(mut self, backend: StorageBackend) -> Self {
        self.backends.push(backend);
        self
    }

    /// Validates the complete backend set and builds its routing indexes.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRegistryError`] for the first conflicting name, URL scheme, CLI key, or
    /// bare-location claim under the registry's deterministic validation order.
    pub fn build(self) -> Result<StorageRegistry, StorageRegistryError> {
        StorageRegistry::from_backends(self.backends)
    }
}

/// A validated and indexed collection of available storage backend definitions.
///
/// Registry construction fixes membership, order, and route ownership. The same registry can
/// augment a host command and create independent sessions from different [`ArgMatches`].
pub struct StorageRegistry {
    backends: Box<[StorageBackend]>,
    routing: Arc<RoutingIndex>,
    uses_shared_retries: bool,
}

impl fmt::Debug for StorageRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRegistry")
            .field("backends", &self.backends)
            .field("uses_shared_retries", &self.uses_shared_retries)
            .finish_non_exhaustive()
    }
}

impl StorageRegistry {
    /// Starts an empty registry builder.
    pub fn builder() -> StorageRegistryBuilder {
        StorageRegistryBuilder::default()
    }

    fn from_backends(backends: Vec<StorageBackend>) -> Result<Self, StorageRegistryError> {
        validate_backend_names(&backends)?;
        validate_schemes(&backends)?;

        let uses_shared_retries = backends.iter().any(StorageBackend::uses_shared_retries);
        validate_cli_arguments(&backends, uses_shared_retries)?;
        validate_bare_location_claim(&backends)?;

        let mut backend_index_by_scheme = HashMap::new();
        let mut bare_location_backend_index = None;
        for (backend_index, backend) in backends.iter().enumerate() {
            for &scheme in backend.schemes() {
                backend_index_by_scheme.insert(scheme, backend_index);
            }
            if backend.claims_bare_locations() {
                bare_location_backend_index = Some(backend_index);
            }
        }

        Ok(Self {
            backends: backends.into_boxed_slice(),
            routing: Arc::new(RoutingIndex {
                backend_index_by_scheme,
                bare_location_backend_index,
            }),
            uses_shared_retries,
        })
    }

    /// Returns all available backends in registration order.
    pub fn backends(&self) -> &[StorageBackend] {
        &self.backends
    }

    /// Finds the backend that claims an exact canonical URL scheme.
    ///
    /// Lookup does not normalize input. Backend construction requires lowercase schemes, so an
    /// uppercase spelling returns `None`.
    pub fn by_scheme(&self, scheme: &str) -> Option<&StorageBackend> {
        self.routing
            .backend_index_by_scheme
            .get(scheme)
            .map(|&backend_index| &self.backends[backend_index])
    }

    /// Returns the backend that claims schemeless input, when one is registered.
    pub fn bare_location_backend(&self) -> Option<&StorageBackend> {
        self.routing
            .bare_location_backend_index
            .map(|backend_index| &self.backends[backend_index])
    }

    /// Adds shared retry arguments and each backend's arguments to a host-owned Clap command.
    ///
    /// The host must parse the returned command and pass its [`ArgMatches`] to
    /// [`Self::create_session`]. Registry validation covers storage contributors, not arguments
    /// that were already present on `command`.
    pub fn augment_args(&self, mut command: Command) -> Command {
        let host_about = command.get_about().cloned();
        let host_long_about = command.get_long_about().cloned();
        command = ObjectUploadArgs::augment_args(command);
        if self.uses_shared_retries {
            command = RetryArgs::augment_args(command);
        }
        for backend in &self.backends {
            command = backend.augment_args(command);
        }
        if let Some(about) = host_about {
            command = command.about(about);
        }
        if let Some(long_about) = host_long_about {
            command = command.long_about(long_about);
        }
        command
    }

    /// Creates one command-scoped storage session from host-parsed arguments.
    ///
    /// Each call parses one settings value per backend and one shared retry configuration when any
    /// backend requested it. The returned session starts with an empty object-store cache.
    ///
    /// # Errors
    ///
    /// Returns [`StorageSessionCreationError::Arguments`] when a backend cannot reconstruct its
    /// settings, or [`StorageSessionCreationError::Retry`] when shared retry values are invalid.
    pub fn create_session(
        &self,
        matches: &ArgMatches,
    ) -> Result<StorageSession, StorageSessionCreationError> {
        let object_upload_settings = ObjectUploadArgs::from_arg_matches(matches)?.into_settings();
        let retry = if self.uses_shared_retries {
            Some(RetryArgs::from_arg_matches(matches)?.into_retry_config()?)
        } else {
            None
        };

        let backends = self
            .backends
            .iter()
            .map(|backend| backend.bind(matches))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(StorageSession::new(
            backends.into_boxed_slice(),
            Arc::clone(&self.routing),
            retry,
            object_upload_settings,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct RoutingIndex {
    pub(crate) backend_index_by_scheme: HashMap<&'static str, usize>,
    pub(crate) bare_location_backend_index: Option<usize>,
}

/// Conflicts across otherwise valid storage backend definitions.
#[derive(Debug, Error)]
pub enum StorageRegistryError {
    #[error("storage backend name {name:?} is registered {occurrences} times")]
    DuplicateBackendName {
        name: &'static str,
        occurrences: usize,
    },
    #[error(
        "storage URL scheme {scheme:?} is claimed by multiple backends: {}",
        .backends.join(", ")
    )]
    DuplicateScheme {
        scheme: &'static str,
        backends: Box<[&'static str]>,
    },
    #[error(
        "multiple storage CLI contributors define {argument}: {}",
        .contributors.join(", ")
    )]
    DuplicateCliArgument {
        argument: String,
        contributors: Box<[&'static str]>,
    },
    #[error(
        "multiple storage backends claim bare locations: {}",
        .backends.join(", ")
    )]
    MultipleBareLocationBackends { backends: Box<[&'static str]> },
}

fn validate_backend_names(backends: &[StorageBackend]) -> Result<(), StorageRegistryError> {
    let mut counts = HashMap::new();
    let mut order = Vec::new();
    for backend in backends {
        if !counts.contains_key(backend.name()) {
            order.push(backend.name());
        }
        *counts.entry(backend.name()).or_insert(0) += 1;
    }

    for name in order {
        let occurrences = counts[&name];
        if occurrences > 1 {
            return Err(StorageRegistryError::DuplicateBackendName { name, occurrences });
        }
    }
    Ok(())
}

fn validate_schemes(backends: &[StorageBackend]) -> Result<(), StorageRegistryError> {
    let mut claims = HashMap::<_, Vec<_>>::new();
    let mut order = Vec::new();
    for backend in backends {
        for &scheme in backend.schemes() {
            if !claims.contains_key(scheme) {
                order.push(scheme);
            }
            claims.entry(scheme).or_default().push(backend.name());
        }
    }

    for scheme in order {
        let claimants = &claims[scheme];
        if claimants.len() > 1 {
            return Err(StorageRegistryError::DuplicateScheme {
                scheme,
                backends: claimants.clone().into_boxed_slice(),
            });
        }
    }
    Ok(())
}

fn validate_cli_arguments(
    backends: &[StorageBackend],
    uses_shared_retries: bool,
) -> Result<(), StorageRegistryError> {
    let mut claims = HashMap::<CliArgumentKey, Vec<&'static str>>::new();
    let mut order = Vec::new();

    let upload_keys = argument_keys(
        "shared object uploads",
        <ObjectUploadArgs as Args>::augment_args,
    );
    add_cli_claims(
        &mut claims,
        &mut order,
        "shared object uploads",
        &upload_keys,
    );

    let retry_keys = uses_shared_retries
        .then(|| argument_keys("shared storage retries", <RetryArgs as Args>::augment_args));
    if let Some(keys) = &retry_keys {
        add_cli_claims(&mut claims, &mut order, "shared storage retries", keys);
    }
    for backend in backends {
        add_cli_claims(
            &mut claims,
            &mut order,
            backend.name(),
            backend.argument_keys(),
        );
    }

    for key in order {
        let contributors = &claims[&key];
        if contributors.len() > 1 {
            return Err(StorageRegistryError::DuplicateCliArgument {
                argument: key.to_string(),
                contributors: contributors.clone().into_boxed_slice(),
            });
        }
    }
    Ok(())
}

fn add_cli_claims(
    claims: &mut HashMap<CliArgumentKey, Vec<&'static str>>,
    order: &mut Vec<CliArgumentKey>,
    contributor: &'static str,
    keys: &[CliArgumentKey],
) {
    for key in keys {
        if !claims.contains_key(key) {
            order.push(key.clone());
        }
        claims.entry(key.clone()).or_default().push(contributor);
    }
}

fn validate_bare_location_claim(backends: &[StorageBackend]) -> Result<(), StorageRegistryError> {
    let claimants = backends
        .iter()
        .filter(|backend| backend.claims_bare_locations())
        .map(StorageBackend::name)
        .collect::<Vec<_>>();
    if claimants.len() > 1 {
        return Err(StorageRegistryError::MultipleBareLocationBackends {
            backends: claimants.into_boxed_slice(),
        });
    }
    Ok(())
}
