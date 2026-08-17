/// Creates a command-scoped storage session containing the local backend.
pub fn local_storage_session() -> silk_chiffon_storage::StorageSession {
    use clap::Command;
    use silk_chiffon_storage::{StorageRegistry, local};

    let registry = StorageRegistry::builder()
        .register(local::backend().unwrap())
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("test-storage"));
    let matches = command.try_get_matches_from(["test-storage"]).unwrap();
    registry.create_session(&matches).unwrap()
}

/// Prepares one local output target for tests that exercise a storage-backed sink directly.
pub fn prepared_local_output_target(
    path: impl AsRef<std::path::Path>,
) -> silk_chiffon_storage::PreparedOutputTarget {
    use silk_chiffon_storage::{ExistingOutput, LocationInput, OutputPreparation};

    let path = path.as_ref().to_path_buf();
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir().unwrap().join(path)
    };
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let url = url::Url::from_file_path(&path).unwrap();
                local_storage_session()
                    .prepare_output_target(
                        &LocationInput::parse(url.as_str()).unwrap(),
                        &OutputPreparation::new(ExistingOutput::Allow, false),
                    )
                    .await
                    .unwrap()
            })
    })
    .join()
    .unwrap()
}
