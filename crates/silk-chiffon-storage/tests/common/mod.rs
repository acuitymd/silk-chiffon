#[cfg(feature = "local")]
pub fn local_storage_session() -> silk_chiffon_storage::StorageSession {
    use clap::Command;
    use silk_chiffon_storage::{StorageRegistry, local};

    let registry = StorageRegistry::builder()
        .register(local::backend().unwrap())
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("storage-test"));
    let matches = command.try_get_matches_from(["storage-test"]).unwrap();
    registry.create_session(&matches).unwrap()
}
