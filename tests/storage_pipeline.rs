use std::sync::Arc;

use silk_chiffon::pipeline::Pipeline;
use silk_chiffon_storage::{Location, StorageResolver};
use tempfile::TempDir;

#[test]
fn pipeline_registers_the_resolved_store_with_datafusion() {
    let working_directory = TempDir::new().unwrap();
    let location = Location::parse("data.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::new().resolve(&location).unwrap();
    let expected_store = Arc::clone(&resolved.store);
    let expected_url = resolved.url.clone();

    let mut pipeline = Pipeline::new().with_storage_location(resolved);
    let context = pipeline.build_session_context().unwrap();
    let registered_store = context
        .runtime_env()
        .object_store_registry
        .get_store(&expected_url)
        .unwrap();

    assert!(Arc::ptr_eq(&expected_store, &registered_store));
}
