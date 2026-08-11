//! Built-in storage backend for canonical `file:///` locations.
//!
//! The `local` Cargo feature exposes [`backend`] and [`session`] for explicit `file:` URLs. The
//! separate `local-bare-paths` feature also makes this backend interpret schemeless input as a
//! filesystem path.

#[cfg(feature = "local")]
use std::sync::Arc;

#[cfg(feature = "local")]
use clap::Command;
#[cfg(feature = "local")]
use object_store::{ObjectStore, local::LocalFileSystem};

#[cfg(feature = "local-bare-paths")]
use crate::{Location, LocationPattern};
#[cfg(feature = "local")]
use crate::{
    StorageAccess, StorageBackend, StorageBackendBuildError, StorageRegistry, StorageSession,
    StorageSessionCreationError,
};

/// Builds the built-in local backend definition for canonical `file:///` locations.
///
/// With `local-bare-paths`, the same definition also claims schemeless input and maps relative
/// paths against the process working directory.
///
/// # Errors
///
/// Returns [`StorageBackendBuildError`] if the built-in definition violates backend invariants.
#[cfg(feature = "local")]
pub fn backend() -> Result<StorageBackend, StorageBackendBuildError> {
    let builder = StorageBackend::without_args()
        .name("local")
        .schemes(["file"])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(create_object_store);

    #[cfg(feature = "local-bare-paths")]
    let builder = builder
        .bare_location_mapper(map_bare_location)
        .bare_pattern_mapper(map_bare_pattern);

    builder.build()
}

/// Creates a storage session containing only the built-in local backend.
///
/// This shortcut uses default host arguments. Applications that compose multiple backends should
/// build a [`StorageRegistry`] and pass their own parsed matches to
/// [`StorageRegistry::create_session`].
///
/// # Errors
///
/// Returns [`StorageSessionCreationError`] if the backend, registry, or default session arguments
/// cannot be created.
#[cfg(feature = "local")]
pub fn session() -> Result<StorageSession, StorageSessionCreationError> {
    let registry = StorageRegistry::builder().register(backend()?).build()?;
    let command_name = "fake-convenience-command-that-is-never-used";
    let command = registry.augment_args(Command::new(command_name));
    let matches = command.try_get_matches_from([command_name])?;
    registry.create_session(&matches)
}

#[cfg(feature = "local")]
fn create_object_store(
    _store_url: &url::Url,
    _settings: &(),
    _retry: Option<&crate::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new()))
}

#[cfg(feature = "local-bare-paths")]
fn map_bare_location(input: &str, _settings: &()) -> anyhow::Result<Location> {
    let path = std::path::Path::new(input);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(Location::from_file_path(absolute)?)
}

#[cfg(feature = "local-bare-paths")]
fn map_bare_pattern(input: &str, _settings: &()) -> anyhow::Result<LocationPattern> {
    let path = std::path::Path::new(input);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(LocationPattern::from_file_path_pattern(&absolute, input)?)
}
