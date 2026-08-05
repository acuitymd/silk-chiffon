//! Built-in storage provider for canonical `file:///` locations.
//!
//! The registration exists with or without the `local` Cargo feature so the `file` scheme has a
//! stable owner across feature sets.

use crate::StorageProviderRegistration;

/// Registers the built-in owner of canonical `file:///` locations.
///
/// With the `local` feature, locations resolve through `object_store::local::LocalFileSystem`.
/// Without it, resolution returns [`crate::StorageError::ProviderDisabled`] with instructions to
/// rebuild `silk-chiffon-storage` with the feature.
pub fn registration() -> StorageProviderRegistration {
    #[cfg(feature = "local")]
    {
        StorageProviderRegistration::without_args("local")
            .schemes(["file"])
            .enabled(crate::StorageAccess::ReadWrite, resolve)
    }

    #[cfg(not(feature = "local"))]
    {
        StorageProviderRegistration::without_args("local")
            .schemes(["file"])
            .disabled("rebuild silk-chiffon-storage with the local feature")
    }
}

#[cfg(feature = "local")]
/// Defers store construction so the command-scoped cache creates at most one filesystem client.
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
