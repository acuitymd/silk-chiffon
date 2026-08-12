//! DataFusion object-store views for exact input files.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    datasource::listing::PartitionedFile, execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    path::Path as ObjectPath,
};
use silk_chiffon_storage::InputObject;
use url::Url;

use crate::InputVariant;

const INTERNAL_PREFIX: &str = "__silk_input";

/// The canonical input URL attached to a DataFusion file descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalInput {
    url: Url,
}

impl CanonicalInput {
    /// Returns the exact input URL, including its query.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

/// Exact files prepared by the host as one homogeneous format leaf.
///
/// Construction enforces the format-independent leaf invariants: at least
/// one object, one storage root, deterministic representative selection, and
/// one scoped DataFusion store registration. Format implementations can then
/// focus on schema, statistics, and decoding.
#[derive(Debug)]
pub struct InputLeaf {
    object_store_url: ObjectStoreUrl,
    files: Vec<PartitionedFile>,
    representative_index: usize,
    variant: InputVariant,
}

impl InputLeaf {
    /// Prepares one leaf from objects already grouped by format and variant.
    pub fn try_new(
        session: &SessionContext,
        objects: &[InputObject],
        variant: InputVariant,
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
                            .handle()
                            .url()
                            .as_str()
                            .cmp(left.handle().url().as_str())
                    })
            })
            .map(|(index, _)| index)
            .ok_or_else(|| anyhow::anyhow!("cannot build an empty file-input leaf"))?;
        let (object_store_url, files) = register_input_store(session, objects)?;
        Ok(Self {
            object_store_url,
            files,
            representative_index,
            variant,
        })
    }

    /// Returns the scoped store registered for this leaf.
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
    pub fn variant(&self) -> &InputVariant {
        &self.variant
    }
}

/// Registers one reversible DataFusion view for an input storage root.
///
/// Scoped paths encode both the canonical URL and the backend object path.
/// The view therefore needs no per-file lookup map, and registering another
/// leaf from the same root safely reuses the same DataFusion store URL.
fn register_input_store(
    session: &SessionContext,
    objects: &[InputObject],
) -> anyhow::Result<(ObjectStoreUrl, Vec<PartitionedFile>)> {
    let object = objects
        .first()
        .ok_or_else(|| anyhow::anyhow!("cannot register an empty input leaf"))?;
    let handle = object.handle();
    if objects
        .iter()
        .any(|object| object.handle().store_url() != handle.store_url())
    {
        anyhow::bail!("input leaf spans multiple object-store roots");
    }

    let namespace = encode(handle.store_url().as_str().as_bytes());
    let store_url = ObjectStoreUrl::parse(format!("silk-input://{namespace}@root"))?;
    let files = objects
        .iter()
        .map(|object| {
            let canonical = object.handle().url().clone();
            let mut metadata = object.metadata().clone();
            metadata.location = scoped_path(&canonical, object.handle().object_path());
            PartitionedFile::new_from_meta(metadata)
                .with_extension(CanonicalInput { url: canonical })
        })
        .collect();
    let view = Arc::new(InputStoreView {
        inner: handle.object_store(),
        store_root: handle.store_url().clone(),
    });
    session
        .runtime_env()
        .register_object_store(store_url.as_ref(), view);
    Ok((store_url, files))
}

fn scoped_path(canonical_url: &Url, inner_path: &ObjectPath) -> ObjectPath {
    ObjectPath::from(format!(
        "{INTERNAL_PREFIX}/{}/{}",
        encode(canonical_url.as_str().as_bytes()),
        encode(inner_path.as_ref().as_bytes())
    ))
}

fn encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

fn decode(encoded: &str) -> StoreResult<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return Err(invalid_path("an encoded component has odd length"));
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let digits = std::str::from_utf8(digits).map_err(invalid_path)?;
            u8::from_str_radix(digits, 16).map_err(invalid_path)
        })
        .collect()
}

fn invalid_path(source: impl fmt::Display) -> object_store::Error {
    object_store::Error::Generic {
        store: "SilkInputView",
        source: format!("invalid internal input path: {source}").into(),
    }
}

fn canonical_error(canonical_url: &Url, error: &object_store::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "SilkInputView",
        source: format!("input {canonical_url}: {error}").into(),
    }
}

#[derive(Debug)]
struct InputStoreView {
    inner: Arc<dyn ObjectStore>,
    store_root: Url,
}

impl fmt::Display for InputStoreView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Silk input view for {}", self.store_root)
    }
}

struct DecodedPath {
    canonical_url: Url,
    inner_path: ObjectPath,
}

impl InputStoreView {
    fn decode_path(&self, location: &ObjectPath) -> StoreResult<DecodedPath> {
        let encoded = location
            .as_ref()
            .strip_prefix(&format!("{INTERNAL_PREFIX}/"))
            .ok_or_else(|| invalid_path("the Silk input prefix is missing"))?;
        let (url, path) = encoded
            .split_once('/')
            .ok_or_else(|| invalid_path("the canonical URL or object path is missing"))?;
        let canonical_url = Url::parse(std::str::from_utf8(&decode(url)?).map_err(invalid_path)?)
            .map_err(invalid_path)?;
        let mut root = canonical_url.clone();
        root.set_path("/");
        root.set_query(None);
        root.set_fragment(None);
        if root != self.store_root {
            return Err(invalid_path("the canonical URL belongs to another root"));
        }
        let inner_path =
            ObjectPath::parse(std::str::from_utf8(&decode(path)?).map_err(invalid_path)?)
                .map_err(invalid_path)?;
        Ok(DecodedPath {
            canonical_url,
            inner_path,
        })
    }

    fn unsupported<T>(&self, operation: &str) -> StoreResult<T> {
        Err(object_store::Error::NotImplemented {
            operation: operation.to_owned(),
            implementer: "SilkInputView".to_owned(),
        })
    }
}

#[async_trait]
impl ObjectStore for InputStoreView {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.unsupported("put_opts")
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.unsupported("put_multipart_opts")
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> StoreResult<GetResult> {
        let decoded = self.decode_path(location)?;
        let result = self
            .inner
            .get_opts(&decoded.inner_path, options)
            .await
            .map_err(|error| canonical_error(&decoded.canonical_url, &error))?;
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let mut meta = result.meta.clone();
        let canonical_url = decoded.canonical_url.clone();
        let payload = GetResultPayload::Stream(
            result
                .into_stream()
                .map_err(move |error| canonical_error(&canonical_url, &error))
                .boxed(),
        );
        meta.location = location.clone();
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
        })
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[std::ops::Range<u64>],
    ) -> StoreResult<Vec<bytes::Bytes>> {
        let decoded = self.decode_path(location)?;
        self.inner
            .get_ranges(&decoded.inner_path, ranges)
            .await
            .map_err(|error| canonical_error(&decoded.canonical_url, &error))
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, StoreResult<ObjectPath>>,
    ) -> BoxStream<'static, StoreResult<ObjectPath>> {
        Box::pin(futures::stream::once(async {
            Err(object_store::Error::NotImplemented {
                operation: "delete_stream".to_owned(),
                implementer: "SilkInputView".to_owned(),
            })
        }))
    }

    fn list(&self, _prefix: Option<&ObjectPath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        Box::pin(futures::stream::once(async {
            Err(object_store::Error::NotImplemented {
                operation: "list".to_owned(),
                implementer: "SilkInputView".to_owned(),
            })
        }))
    }

    async fn list_with_delimiter(&self, _prefix: Option<&ObjectPath>) -> StoreResult<ListResult> {
        self.unsupported("list_with_delimiter")
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> StoreResult<()> {
        self.unsupported("copy_opts")
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;

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
    fn registrations_reuse_one_view_for_a_storage_root() {
        futures::executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let first_path = directory.path().join("first.arrow");
            let second_path = directory.path().join("second.arrow");
            std::fs::write(&first_path, b"first").unwrap();
            std::fs::write(&second_path, b"second").unwrap();
            let storage = silk_chiffon_storage::local::session().unwrap();
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

            let first_leaf =
                InputLeaf::try_new(&session, &[first], InputVariant::named("file")).unwrap();
            let second_leaf =
                InputLeaf::try_new(&session, &[second], InputVariant::named("file")).unwrap();

            assert_eq!(
                first_leaf.object_store_url(),
                second_leaf.object_store_url()
            );
            let store = session
                .runtime_env()
                .object_store(first_leaf.object_store_url())
                .unwrap();
            assert_eq!(
                store
                    .get_range(&first_leaf.files()[0].object_meta.location, 0..5)
                    .await
                    .unwrap(),
                bytes::Bytes::from_static(b"first")
            );
        });
    }

    #[test]
    fn a_leaf_requires_at_least_one_file() {
        let error = InputLeaf::try_new(&SessionContext::new(), &[], InputVariant::new())
            .expect_err("an empty leaf must be rejected");

        assert!(error.to_string().contains("empty file-input leaf"));
    }

    #[test]
    fn a_leaf_selects_the_largest_file_as_its_representative() {
        futures::executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let smaller_path = directory.path().join("smaller.arrow");
            let larger_path = directory.path().join("larger.arrow");
            std::fs::write(&smaller_path, b"small").unwrap();
            std::fs::write(&larger_path, b"larger").unwrap();
            let storage = silk_chiffon_storage::local::session().unwrap();
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
            let leaf = InputLeaf::try_new(
                &SessionContext::new(),
                &[smaller, larger],
                InputVariant::named("stream"),
            )
            .unwrap();

            assert!(
                leaf.representative()
                    .extension::<CanonicalInput>()
                    .unwrap()
                    .url()
                    .path()
                    .ends_with("larger.arrow")
            );
        });
    }
}
