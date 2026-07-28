use crate::StorageProviderRegistration;

/// Registers canonical `file:///` locations for input and output.
///
/// Without the `local` Cargo feature, the registration retains the scheme and returns guidance when resolution is attempted.
pub fn registration() -> StorageProviderRegistration {
    #[cfg(feature = "local")]
    {
        StorageProviderRegistration::without_args("local")
            .schemes(["file"])
            .input(resolve)
            .output(resolve)
            .build()
    }

    #[cfg(not(feature = "local"))]
    {
        StorageProviderRegistration::without_args("local")
            .schemes(["file"])
            .feature_disabled_diagnostic("rebuild silk-chiffon-storage with the local feature")
            .build()
    }
}

#[cfg(feature = "local")]
fn resolve(
    location: &crate::Location,
    _settings: &(),
    _retry: Option<&crate::RetryConfiguration>,
) -> Result<crate::ProviderResolution, crate::StorageError> {
    use std::sync::Arc;

    use object_store::{ObjectStore, local::LocalFileSystem, path::Path as ObjectPath};

    let path = ObjectPath::from_url_path(location.url().path())?;
    let mut store_url = location.url().clone();
    store_url.set_path("/");
    Ok(crate::ProviderResolution::from_factory(
        store_url,
        path,
        || Ok(Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>),
    ))
}
