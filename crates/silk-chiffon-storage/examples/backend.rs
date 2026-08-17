use std::sync::Arc;

use object_store::{ObjectStore, RetryConfig, memory::InMemory};
use silk_chiffon_storage::{Location, StorageAccess, StorageBackend, StorageBackendBuildError};
use url::Url;

fn validate_location(_location: &Location, _state: &()) -> anyhow::Result<()> {
    Ok(())
}

fn create_store(
    _store_url: &Url,
    _state: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(InMemory::new()))
}

fn backend() -> Result<StorageBackend, StorageBackendBuildError> {
    StorageBackend::without_args()
        .name("example-memory")
        .schemes(["example-memory"])
        .access(StorageAccess::ReadWrite)
        .location_validator(validate_location)
        .object_store_creator(create_store)
        .build()
}

fn main() {
    let backend = backend().expect("the example backend is valid");
    assert_eq!(backend.name(), "example-memory");
    assert_eq!(backend.schemes(), ["example-memory"]);
}
