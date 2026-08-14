use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use bytes::Bytes;
use clap::Command;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, LocationPattern, OutputPreparation, StorageAccess,
    StorageBackend, StorageError, StorageRegistry, StorageSession,
};

static STORES: OnceLock<Mutex<HashMap<String, Arc<InMemory>>>> = OnceLock::new();

fn fake_store(
    store_url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = Arc::clone(
        STORES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(store_url.to_string())
            .or_insert_with(|| Arc::new(InMemory::new())),
    );
    Ok(store)
}

fn fake_cloud_session(scheme: &'static str) -> StorageSession {
    let backend = StorageBackend::without_args()
        .name("fake-cloud")
        .schemes([scheme])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(fake_store)
        .build()
        .unwrap();
    let registry = StorageRegistry::builder()
        .register(backend)
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("fake-cloud-test"));
    let matches = command.try_get_matches_from(["fake-cloud-test"]).unwrap();
    registry.create_session(&matches).unwrap()
}

async fn exercise_cloud_location_contract(scheme: &'static str) {
    let storage = fake_cloud_session(scheme);
    let first_url = format!("{scheme}://bucket/data/one.arrow");
    let second_url = format!("{scheme}://bucket/data/r%C3%A9sum%C3%A9.arrow");
    for (url, contents) in [
        (&first_url, Bytes::from_static(b"first")),
        (&second_url, Bytes::from_static(b"second")),
    ] {
        let handle = storage
            .input_handle(&LocationInput::parse(url).unwrap())
            .unwrap();
        handle
            .object_store()
            .put(handle.object_path(), contents.into())
            .await
            .unwrap();
    }

    let exact = storage
        .lookup_input(&LocationInput::parse(&first_url).unwrap())
        .await
        .unwrap();
    assert_eq!(exact.metadata().size, 5);
    assert_eq!(
        exact
            .handle()
            .object_store()
            .get_range(exact.handle().object_path(), 1..4)
            .await
            .unwrap(),
        Bytes::from_static(b"irs")
    );

    let pattern = LocationPattern::parse(format!("{scheme}://bucket/data/*.arrow")).unwrap();
    let mut matches = storage
        .expand_input_pattern(&pattern)
        .await
        .unwrap()
        .into_iter()
        .map(|input| input.handle().url().to_string())
        .collect::<Vec<_>>();
    matches.sort();
    assert_eq!(matches, [first_url.clone(), second_url.clone()]);

    let existing_check = fake_cloud_session(scheme);
    assert!(matches!(
        existing_check
            .prepare_output_target(
                &LocationInput::parse(&first_url).unwrap(),
                &OutputPreparation::new(ExistingOutput::RejectIfObserved, false),
            )
            .await,
        Err(StorageError::OutputTargetAlreadyExists { .. })
    ));

    let output_url = format!("{scheme}://bucket/output/new.parquet");
    let output = storage
        .prepare_output_target(
            &LocationInput::parse(&output_url).unwrap(),
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    assert!(matches!(
        storage
            .prepare_output_target(
                &LocationInput::parse(&output_url).unwrap(),
                &OutputPreparation::new(ExistingOutput::Allow, false),
            )
            .await,
        Err(StorageError::OutputTargetAlreadyClaimed { .. })
    ));

    let mut multipart = output
        .object_store()
        .put_multipart(output.object_path())
        .await
        .unwrap();
    multipart
        .put_part(Bytes::from_static(b"unfinished").into())
        .await
        .unwrap();
    multipart.abort().await.unwrap();
    assert!(matches!(
        output.object_store().head(output.object_path()).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let listed = output
        .object_store()
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
}

#[tokio::test]
async fn gcs_and_s3_syntax_share_the_storage_neutral_location_contracts() {
    exercise_cloud_location_contract("gs").await;
    exercise_cloud_location_contract("s3").await;
}
