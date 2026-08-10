use std::sync::Arc;

use silk_chiffon::pipeline::Pipeline;
use silk_chiffon_storage::{LocationInput, local};
use tempfile::TempDir;

#[test]
fn pipeline_registers_the_handle_store_with_datafusion() {
    let working_directory = TempDir::new().unwrap();
    let path = working_directory.path().join("data.parquet");
    let location = LocationInput::parse(path.to_str().unwrap()).unwrap();
    let handle = local::session().unwrap().input_handle(&location).unwrap();
    let expected_store = handle.object_store();
    let expected_url = handle.url().clone();

    let mut pipeline = Pipeline::new().with_storage_handle(handle);
    let context = pipeline.create_session_context().unwrap();
    let registered_store = context
        .runtime_env()
        .object_store_registry
        .get_store(&expected_url)
        .unwrap();

    assert!(Arc::ptr_eq(&expected_store, &registered_store));
}
