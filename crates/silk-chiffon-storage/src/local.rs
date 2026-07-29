use crate::StorageProviderRegistration;

/// Registers canonical `file:///` locations for input and output.
///
/// Without the `local` Cargo feature, the registration retains the scheme and returns guidance when resolution is attempted.
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
