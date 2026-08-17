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
                silk_chiffon_storage::local::session()
                    .unwrap()
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
