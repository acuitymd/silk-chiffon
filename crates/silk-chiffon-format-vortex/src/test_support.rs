use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Result;
use arrow::{
    array::{Int32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use bytes::Bytes;
use clap::Command;
use object_store::{ObjectStore, ObjectStoreExt};
use silk_chiffon_storage::{
    InputObject, LocationInput, StorageAccess, StorageBackend, StorageRegistry, StorageSession,
};
use silk_chiffon_test_support::ReadProbeStore;

static STORE: OnceLock<Arc<ReadProbeStore>> = OnceLock::new();
static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static OBJECT_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn store() -> Arc<ReadProbeStore> {
    Arc::clone(STORE.get_or_init(|| Arc::new(ReadProbeStore::new())))
}

pub(crate) async fn guard() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn create_store(
    _: &url::Url,
    _: &(),
    _: Option<&silk_chiffon_storage::RetryConfig>,
) -> Result<Arc<dyn ObjectStore>> {
    Ok(store())
}

fn session() -> StorageSession {
    let registry = StorageRegistry::builder()
        .register(
            StorageBackend::without_args()
                .name("memory")
                .schemes(["memory"])
                .access(StorageAccess::ReadOnly)
                .allow_any_location()
                .object_store_creator(create_store)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let matches = registry
        .augment_args(Command::new("test"))
        .try_get_matches_from(["test"])
        .unwrap();
    registry.create_session(&matches).unwrap()
}

pub(crate) async fn object_with(bytes: impl Into<Bytes>) -> InputObject {
    let session = session();
    let sequence = OBJECT_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let location =
        LocationInput::parse(format!("memory://bucket/vortex-{sequence}.vortex")).unwrap();
    let handle = session.input_handle(&location).unwrap();
    handle
        .object_store()
        .put(handle.object_path(), bytes.into().into())
        .await
        .unwrap();
    session.lookup_input(&location).await.unwrap()
}

pub(crate) fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

pub(crate) fn simple_batch() -> RecordBatch {
    RecordBatch::try_new(
        simple_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

pub(crate) async fn vortex_bytes(batches: Vec<RecordBatch>) -> Bytes {
    Bytes::from(
        silk_chiffon_test_support::vortex::write_batches(&simple_schema(), batches)
            .await
            .unwrap(),
    )
}

pub(crate) async fn vortex_object() -> InputObject {
    object_with(vortex_bytes(vec![simple_batch()]).await).await
}
