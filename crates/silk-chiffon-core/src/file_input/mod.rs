//! DataFusion object-store views for exact input files.

mod provider;
mod store;

use datafusion::{
    datasource::listing::PartitionedFile, execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};
use silk_chiffon_storage::InputObject;
use url::Url;

use crate::FormatInputVariant;
use store::register_input_store;

/// The canonical input URL attached to a DataFusion file descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalInputUrl {
    url: Url,
}

impl CanonicalInputUrl {
    /// Returns the exact input URL, including its query.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

/// Exact files prepared by the host as one homogeneous format group.
///
/// Construction enforces the format-independent group invariants: at least
/// one object, one storage root, deterministic representative selection, and
/// one scoped DataFusion store registration. Format implementations can then
/// focus on schema, statistics, and decoding.
#[derive(Debug)]
pub struct FileInputGroup {
    object_store_url: ObjectStoreUrl,
    files: Vec<PartitionedFile>,
    representative_index: usize,
    variant: FormatInputVariant,
}

impl FileInputGroup {
    /// Prepares one group from objects already grouped by format and variant.
    pub(crate) fn try_new(
        session: &SessionContext,
        objects: &[InputObject],
        variant: FormatInputVariant,
    ) -> anyhow::Result<Self> {
        let representative_index = objects
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| {
                left.metadata()
                    .size
                    .cmp(&right.metadata().size)
                    .then_with(|| {
                        right
                            .input_handle()
                            .url()
                            .as_str()
                            .cmp(left.input_handle().url().as_str())
                    })
            })
            .map(|(index, _)| index)
            .ok_or_else(|| anyhow::anyhow!("cannot build an empty file-input group"))?;
        let (object_store_url, files) = register_input_store(session, objects)?;
        Ok(Self {
            object_store_url,
            files,
            representative_index,
            variant,
        })
    }

    /// Returns the scoped store registered for this group.
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    /// Returns the exact DataFusion file descriptors in operand order.
    pub fn files(&self) -> &[PartitionedFile] {
        &self.files
    }

    /// Returns the largest file, choosing the smallest canonical URL on a size tie.
    pub fn representative(&self) -> &PartitionedFile {
        &self.files[self.representative_index]
    }

    /// Returns the format-specific container variant selected before grouping.
    pub fn variant(&self) -> &FormatInputVariant {
        &self.variant
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use clap::Command;
    use futures::{StreamExt, TryStreamExt};
    use object_store::{
        CopyOptions, GetOptions, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
        Result as StoreResult, memory::InMemory, path::Path as ObjectPath,
    };
    use silk_chiffon_storage::{
        ExistingOutput, LocationInput, OutputPreparation, StorageAccess, StorageBackend,
        StorageRegistry, StorageSession,
    };

    use super::store::{INTERNAL_PREFIX, InputStoreView, encode, scoped_path};
    use super::*;

    fn memory_storage() -> StorageSession {
        fn create_store(
            _store_url: &Url,
            _settings: &(),
            _retry: Option<&object_store::RetryConfig>,
        ) -> anyhow::Result<Arc<dyn ObjectStore>> {
            Ok(Arc::new(InMemory::new()))
        }

        let backend = StorageBackend::without_args()
            .name("memory")
            .schemes(["mem"])
            .access(StorageAccess::ReadWrite)
            .allow_any_location()
            .object_store_creator(create_store)
            .build()
            .unwrap();
        let registry = StorageRegistry::builder()
            .register(backend)
            .build()
            .unwrap();
        let matches = registry
            .augment_args(Command::new("input-store-test"))
            .try_get_matches_from(["input-store-test"])
            .unwrap();
        registry.create_session(&matches).unwrap()
    }

    async fn put_input(storage: &StorageSession, url: &str, bytes: &'static [u8]) -> InputObject {
        let input = LocationInput::parse(url).unwrap();
        let target = storage
            .prepare_output_target(
                &input,
                &OutputPreparation::new(ExistingOutput::Allow, false),
            )
            .await
            .unwrap();
        target
            .object_store()
            .put(target.object_path(), Bytes::from_static(bytes).into())
            .await
            .unwrap();
        storage.lookup_input(&input).await.unwrap()
    }

    fn operation_error<T>(result: StoreResult<T>) -> String {
        match result {
            Ok(_) => panic!("a scoped input view operation unexpectedly succeeded"),
            Err(error) => error.to_string(),
        }
    }

    fn view() -> InputStoreView {
        InputStoreView {
            inner: Arc::new(InMemory::new()),
            store_root: Url::parse("s3://bucket/").unwrap(),
        }
    }

    #[test]
    fn scoped_paths_round_trip_without_a_lookup_map() {
        let canonical = Url::parse("s3://bucket/data/one.arrow?versionId=one").unwrap();
        let inner: ObjectPath = "data/one.arrow".into();
        let decoded = view()
            .decode_path(&scoped_path(&canonical, &inner))
            .unwrap();

        assert_eq!(decoded.canonical_url, canonical);
        assert_eq!(decoded.inner_path, inner);
    }

    #[test]
    fn read_errors_use_the_canonical_input_identity() {
        futures::executor::block_on(async {
            let canonical = Url::parse("s3://bucket/missing.arrow?versionId=one").unwrap();
            let location = scoped_path(&canonical, &"missing.arrow".into());
            let error = view().get_range(&location, 0..1).await.unwrap_err();

            assert!(error.to_string().contains(canonical.as_str()));
            assert!(!error.to_string().contains(INTERNAL_PREFIX));
        });
    }

    #[test]
    fn scoped_reads_preserve_the_external_path_and_support_ranges() {
        futures::executor::block_on(async {
            let inner = Arc::new(InMemory::new());
            let inner_path: ObjectPath = "data/one.arrow".into();
            inner
                .put(&inner_path, Bytes::from_static(b"abcdef").into())
                .await
                .unwrap();
            let view = InputStoreView {
                inner,
                store_root: Url::parse("s3://bucket/").unwrap(),
            };
            let canonical = Url::parse("s3://bucket/data/one.arrow?versionId=one").unwrap();
            let location = scoped_path(&canonical, &inner_path);

            let result = view
                .get_opts(&location, GetOptions::default())
                .await
                .unwrap();
            assert_eq!(result.meta.location, location);
            assert_eq!(result.bytes().await.unwrap(), Bytes::from_static(b"abcdef"));
            assert_eq!(
                view.get_ranges(&location, &[0..2, 4..6]).await.unwrap(),
                [Bytes::from_static(b"ab"), Bytes::from_static(b"ef")]
            );
            assert_eq!(view.to_string(), "Silk input view for s3://bucket/");
        });
    }

    #[test]
    fn multi_range_errors_use_the_canonical_input_identity() {
        futures::executor::block_on(async {
            let canonical = Url::parse("s3://bucket/missing.arrow?versionId=one").unwrap();
            let location = scoped_path(&canonical, &"missing.arrow".into());
            let error = view()
                .get_ranges(&location, &[0..1, 2..3])
                .await
                .unwrap_err();

            assert!(error.to_string().contains(canonical.as_str()));
            assert!(!error.to_string().contains(INTERNAL_PREFIX));
        });
    }

    #[test]
    fn a_view_rejects_paths_from_another_root() {
        futures::executor::block_on(async {
            let canonical = Url::parse("s3://other/one.arrow").unwrap();
            let location = scoped_path(&canonical, &"one.arrow".into());

            assert!(
                view()
                    .get_range(&location, 0..1)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("another root")
            );
        });
    }

    #[test]
    fn malformed_scoped_paths_never_reach_the_inner_store() {
        for path in [
            "outside/00/00",
            "__silk_input/00",
            "__silk_input/0/00",
            "__silk_input/zz/00",
        ] {
            let error = match view().decode_path(&ObjectPath::from(path)) {
                Ok(_) => panic!("a malformed scoped path unexpectedly decoded"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("invalid internal input path"));
        }
    }

    #[test]
    fn the_scoped_store_is_read_only_and_does_not_list() {
        futures::executor::block_on(async {
            let view = view();
            let path = ObjectPath::from("object");

            assert!(
                operation_error(
                    view.put_opts(&path, Bytes::new().into(), PutOptions::default())
                        .await
                )
                .contains("put_opts")
            );
            assert!(
                operation_error(
                    view.put_multipart_opts(&path, PutMultipartOptions::default())
                        .await
                )
                .contains("put_multipart_opts")
            );
            assert!(operation_error(view.list(None).try_next().await).contains("list"));
            assert!(
                operation_error(view.list_with_delimiter(None).await)
                    .contains("list_with_delimiter")
            );
            assert!(
                operation_error(
                    view.copy_opts(&path, &ObjectPath::from("copy"), CopyOptions::default())
                        .await
                )
                .contains("copy_opts")
            );
            assert!(
                operation_error(
                    view.delete_stream(futures::stream::iter([Ok(path)]).boxed())
                        .try_next()
                        .await
                )
                .contains("delete_stream")
            );
        });
    }

    #[test]
    fn registrations_reuse_one_view_for_a_storage_root() {
        futures::executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let first_path = directory.path().join("first.arrow");
            let second_path = directory.path().join("second.arrow");
            std::fs::write(&first_path, b"first").unwrap();
            std::fs::write(&second_path, b"second").unwrap();
            let storage = silk_chiffon_test_support::local_storage_session();
            let first = storage
                .lookup_input(
                    &silk_chiffon_storage::LocationInput::parse(first_path.to_str().unwrap())
                        .unwrap(),
                )
                .await
                .unwrap();
            let second = storage
                .lookup_input(
                    &silk_chiffon_storage::LocationInput::parse(second_path.to_str().unwrap())
                        .unwrap(),
                )
                .await
                .unwrap();
            let session = SessionContext::new();

            let first_group = FileInputGroup::try_new(
                &session,
                &[first],
                FormatInputVariant::named("file", "file"),
            )
            .unwrap();
            let second_group = FileInputGroup::try_new(
                &session,
                &[second],
                FormatInputVariant::named("file", "file"),
            )
            .unwrap();

            assert_eq!(
                first_group.object_store_url(),
                second_group.object_store_url()
            );
            let store = session
                .runtime_env()
                .object_store(first_group.object_store_url())
                .unwrap();
            assert_eq!(
                store
                    .get_range(&first_group.files()[0].object_meta.location, 0..5)
                    .await
                    .unwrap(),
                bytes::Bytes::from_static(b"first")
            );
        });
    }

    #[test]
    fn registrations_keep_different_storage_roots_isolated() {
        let first =
            ObjectStoreUrl::parse(format!("silk-input://{}", encode(b"s3://first-bucket/")))
                .unwrap();
        let second =
            ObjectStoreUrl::parse(format!("silk-input://{}", encode(b"s3://second-bucket/")))
                .unwrap();

        assert_ne!(first, second);
        let first_url: &Url = first.as_ref();
        let second_url: &Url = second.as_ref();
        assert_ne!(first_url.host_str(), second_url.host_str());
    }

    #[test]
    fn a_group_cannot_span_storage_roots() {
        futures::executor::block_on(async {
            let storage = memory_storage();
            let first = put_input(&storage, "mem://first/object.arrow", b"first").await;
            let second = put_input(&storage, "mem://second/object.arrow", b"second").await;

            let error = FileInputGroup::try_new(
                &SessionContext::new(),
                &[first, second],
                FormatInputVariant::new(),
            )
            .expect_err("one group must not span storage roots");

            assert!(
                error
                    .to_string()
                    .contains("spans multiple object-store roots")
            );
        });
    }

    #[test]
    fn a_group_requires_at_least_one_file() {
        let error = FileInputGroup::try_new(&SessionContext::new(), &[], FormatInputVariant::new())
            .expect_err("an empty group must be rejected");

        assert!(error.to_string().contains("empty file-input group"));
    }

    #[test]
    fn a_group_selects_the_largest_file_as_its_representative() {
        futures::executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let smaller_path = directory.path().join("smaller.arrow");
            let larger_path = directory.path().join("larger.arrow");
            std::fs::write(&smaller_path, b"small").unwrap();
            std::fs::write(&larger_path, b"larger").unwrap();
            let storage = silk_chiffon_test_support::local_storage_session();
            let smaller = storage
                .lookup_input(
                    &silk_chiffon_storage::LocationInput::parse(smaller_path.to_str().unwrap())
                        .unwrap(),
                )
                .await
                .unwrap();
            let larger = storage
                .lookup_input(
                    &silk_chiffon_storage::LocationInput::parse(larger_path.to_str().unwrap())
                        .unwrap(),
                )
                .await
                .unwrap();
            let group = FileInputGroup::try_new(
                &SessionContext::new(),
                &[smaller, larger],
                FormatInputVariant::named("stream", "stream"),
            )
            .unwrap();

            assert!(
                group
                    .representative()
                    .extension::<CanonicalInputUrl>()
                    .unwrap()
                    .url()
                    .path()
                    .ends_with("larger.arrow")
            );
        });
    }

    #[test]
    fn representative_size_ties_use_the_smallest_canonical_url() {
        futures::executor::block_on(async {
            let storage = memory_storage();
            let later = put_input(&storage, "mem://bucket/z.arrow", b"same").await;
            let earlier = put_input(&storage, "mem://bucket/a.arrow", b"same").await;
            let group = FileInputGroup::try_new(
                &SessionContext::new(),
                &[later, earlier],
                FormatInputVariant::new(),
            )
            .unwrap();

            assert_eq!(
                group
                    .representative()
                    .extension::<CanonicalInputUrl>()
                    .unwrap()
                    .url()
                    .as_str(),
                "mem://bucket/a.arrow"
            );
        });
    }
}
