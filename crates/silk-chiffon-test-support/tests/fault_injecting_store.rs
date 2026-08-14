use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use clap::Command;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, LocationPattern, ObjectUpload, OutputPreparation, StorageAccess,
    StorageBackend, StorageError, StorageRegistry,
};
use silk_chiffon_test_support::{FaultInjectingStore, ObjectStoreOperation};

fn assert_injected(error: &object_store::Error, operation: &str) {
    assert!(
        error.to_string().contains(operation),
        "expected an injected {operation} failure, got {error}"
    );
}

#[tokio::test]
async fn every_object_store_operation_can_fail_without_hiding_later_success() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = FaultInjectingStore::new(inner);
    let source = Path::from("source");
    let target = Path::from("target");
    store
        .put(&source, Bytes::from_static(b"source").into())
        .await
        .unwrap();

    store.fail_next(ObjectStoreOperation::Put);
    assert_injected(
        &store
            .put(&target, Bytes::from_static(b"target").into())
            .await
            .unwrap_err(),
        "put",
    );
    assert!(matches!(
        store.head(&target).await,
        Err(object_store::Error::NotFound { .. })
    ));
    store
        .put(&target, Bytes::from_static(b"target").into())
        .await
        .unwrap();

    store.fail_next(ObjectStoreOperation::Get);
    assert_injected(&store.get(&source).await.unwrap_err(), "get");
    store.get(&source).await.unwrap();

    store.fail_next(ObjectStoreOperation::GetRanges);
    assert_injected(
        &store.get_ranges(&source, &[0..2, 2..4]).await.unwrap_err(),
        "get ranges",
    );
    assert_eq!(
        store.get_ranges(&source, &[0..2, 2..4]).await.unwrap(),
        [Bytes::from_static(b"so"), Bytes::from_static(b"ur")]
    );

    store.fail_next(ObjectStoreOperation::List);
    assert_injected(
        &store.list(None).try_collect::<Vec<_>>().await.unwrap_err(),
        "list",
    );
    assert_eq!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        2
    );

    store.fail_next(ObjectStoreOperation::ListWithOffset);
    assert_injected(
        &store
            .list_with_offset(None, &source)
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err(),
        "list with offset",
    );
    store
        .list_with_offset(None, &source)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    store.fail_next(ObjectStoreOperation::ListWithDelimiter);
    assert_injected(
        &store.list_with_delimiter(None).await.unwrap_err(),
        "list with delimiter",
    );
    store.list_with_delimiter(None).await.unwrap();

    let copied = Path::from("copied");
    store.fail_next(ObjectStoreOperation::Copy);
    assert_injected(&store.copy(&source, &copied).await.unwrap_err(), "copy");
    assert!(matches!(
        store.head(&copied).await,
        Err(object_store::Error::NotFound { .. })
    ));
    store.copy(&source, &copied).await.unwrap();

    let renamed = Path::from("renamed");
    store.fail_next(ObjectStoreOperation::Rename);
    assert_injected(
        &store.rename(&copied, &renamed).await.unwrap_err(),
        "rename",
    );
    assert!(store.head(&copied).await.is_ok());
    assert!(matches!(
        store.head(&renamed).await,
        Err(object_store::Error::NotFound { .. })
    ));
    store.rename(&copied, &renamed).await.unwrap();

    store.fail_next(ObjectStoreOperation::Delete);
    let locations = stream::iter([Ok(renamed.clone())]).boxed();
    assert_injected(
        &store
            .delete_stream(locations)
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err(),
        "delete",
    );
    assert!(store.head(&renamed).await.is_ok());
    let locations = stream::iter([Ok(renamed)]).boxed();
    store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
}

#[tokio::test]
async fn every_multipart_stage_can_fail_and_failures_can_be_delayed() {
    let store = FaultInjectingStore::new(Arc::new(InMemory::new()));
    let path = Path::from("multipart");

    store.fail_next(ObjectStoreOperation::MultipartStart);
    assert_injected(
        &store.put_multipart(&path).await.unwrap_err(),
        "multipart start",
    );
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let mut upload = store.put_multipart(&path).await.unwrap();
    store.fail_next(ObjectStoreOperation::MultipartPart);
    assert_injected(
        &upload
            .put_part(Bytes::from_static(b"part").into())
            .await
            .unwrap_err(),
        "multipart part",
    );
    upload
        .put_part(Bytes::from_static(b"part").into())
        .await
        .unwrap();
    store.fail_next(ObjectStoreOperation::MultipartComplete);
    assert_injected(&upload.complete().await.unwrap_err(), "multipart complete");
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let mut upload = store.put_multipart(&path).await.unwrap();
    store.fail_next(ObjectStoreOperation::MultipartAbort);
    assert_injected(&upload.abort().await.unwrap_err(), "multipart abort");

    let previous_gets = store.calls(ObjectStoreOperation::Get);
    store.fail_after(ObjectStoreOperation::Get, 2);
    assert!(store.get(&path).await.is_err());
    assert!(store.get(&path).await.is_err());
    assert_injected(&store.get(&path).await.unwrap_err(), "get");
    assert_eq!(store.calls(ObjectStoreOperation::Get), previous_gets + 3);
}

#[tokio::test]
async fn repeated_fault_requests_schedule_distinct_calls() {
    let store = FaultInjectingStore::new(Arc::new(InMemory::new()));
    let path = Path::from("missing");
    store.fail_next(ObjectStoreOperation::Get);
    store.fail_next(ObjectStoreOperation::Get);

    assert_injected(&store.get(&path).await.unwrap_err(), "get");
    assert_injected(&store.get(&path).await.unwrap_err(), "get");
    let delegated = store.get(&path).await.unwrap_err();
    assert!(!delegated.to_string().contains("injected"));
}

static SESSION_STORE: OnceLock<Arc<FaultInjectingStore>> = OnceLock::new();

fn session_store_creator(
    _store_url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = Arc::clone(
        SESSION_STORE.get_or_init(|| Arc::new(FaultInjectingStore::new(Arc::new(InMemory::new())))),
    );
    Ok(store)
}

#[tokio::test]
async fn injected_faults_cross_the_storage_backend_and_session_boundary() {
    let backend = StorageBackend::without_args()
        .name("faulty-cloud")
        .schemes(["faulty"])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(session_store_creator)
        .build()
        .unwrap();
    let registry = StorageRegistry::builder()
        .register(backend)
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("fault-injection-test"));
    let matches = command
        .try_get_matches_from(["fault-injection-test"])
        .unwrap();
    let storage = registry.create_session(&matches).unwrap();
    let input = LocationInput::parse("faulty://bucket/input.arrow").unwrap();
    let handle = storage.input_handle(&input).unwrap();
    let store = SESSION_STORE.get().unwrap();
    handle
        .object_store()
        .put(handle.object_path(), Bytes::from_static(b"input").into())
        .await
        .unwrap();

    store.fail_next(ObjectStoreOperation::Get);
    assert!(matches!(
        storage.lookup_input(&input).await,
        Err(StorageError::ObjectStore(_))
    ));

    store.fail_next(ObjectStoreOperation::List);
    let pattern = LocationPattern::parse("faulty://bucket/*.arrow").unwrap();
    assert!(matches!(
        storage.expand_input_pattern(&pattern).await,
        Err(StorageError::PatternListing { .. })
    ));

    let output = storage
        .prepare_output_target(
            &LocationInput::parse("faulty://bucket/output.parquet").unwrap(),
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await
        .unwrap();
    let mut upload = ObjectUpload::new(output);
    upload.write(Bytes::from_static(b"output")).await.unwrap();
    store.fail_next(ObjectStoreOperation::Put);
    assert!(upload.complete().await.is_err());
}
