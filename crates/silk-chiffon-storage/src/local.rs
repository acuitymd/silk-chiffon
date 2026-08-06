//! Built-in storage provider for canonical `file:///` locations.
//!
//! The `local` Cargo feature enables explicit `file:` locations. The `local-bare-paths` feature
//! additionally assigns schemeless input to this provider.

#[cfg(feature = "local")]
use crate::StorageProviderRegistration;

/// Registers the built-in owner of canonical `file:///` locations.
///
/// Locations resolve through `object_store::local::LocalFileSystem`. When `local-bare-paths` is
/// enabled, the provider also interprets schemeless input as a filesystem path relative to the
/// process working directory.
#[cfg(feature = "local")]
pub fn registration() -> StorageProviderRegistration {
    let builder = StorageProviderRegistration::without_args(
        "local",
        "file",
        crate::StorageAccess::ReadWrite,
        resolve,
    );

    #[cfg(feature = "local-bare-paths")]
    let builder = builder.bare_locations(map_bare_location);

    builder.build()
}

/// Defers store construction so the command-scoped cache creates at most one filesystem client.
#[cfg(feature = "local")]
fn resolve(
    location: &crate::Location,
    _settings: &(),
    _retry: Option<&crate::RetryConfig>,
) -> anyhow::Result<crate::ProviderResolution> {
    use std::sync::Arc;

    use object_store::{ObjectStore, local::LocalFileSystem, path::Path as ObjectPath};

    let path = ObjectPath::from_url_path(location.url().path())?;
    Ok(crate::ProviderResolution::from_factory(path, || {
        Ok(Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>)
    }))
}

#[cfg(feature = "local-bare-paths")]
fn map_bare_location(input: &str, _settings: &()) -> anyhow::Result<crate::Location> {
    let path = std::path::Path::new(input);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(crate::Location::from_file_path(absolute)?)
}
