//! Scoped DataFusion object-store registration for exact inputs.

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

use super::CanonicalInputUrl;

pub(super) const INTERNAL_PREFIX: &str = "__silk_input";

/// Registers one reversible DataFusion view for an input storage root.
///
/// Scoped paths encode both the canonical URL and the backend object path.
/// The view therefore needs no per-file lookup map, and registering another
/// group from the same root safely reuses the same DataFusion store URL.
pub(super) fn register_input_store(
    session: &SessionContext,
    objects: &[InputObject],
) -> anyhow::Result<(ObjectStoreUrl, Vec<PartitionedFile>)> {
    let object = objects
        .first()
        .ok_or_else(|| anyhow::anyhow!("cannot register an empty input group"))?;
    let handle = object.input_handle();
    if objects
        .iter()
        .any(|object| object.input_handle().store_url() != handle.store_url())
    {
        anyhow::bail!("input group spans multiple object-store roots");
    }

    let namespace = encode(handle.store_url().as_str().as_bytes());
    let store_url = ObjectStoreUrl::parse(format!("silk-input://{namespace}"))?;
    let files = objects
        .iter()
        .map(|object| {
            let canonical = object.input_handle().url().clone();
            let mut metadata = object.metadata().clone();
            metadata.location = scoped_path(&canonical, object.input_handle().object_path());
            PartitionedFile::new_from_meta(metadata)
                .with_extension(CanonicalInputUrl { url: canonical })
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

pub(super) fn scoped_path(canonical_url: &Url, inner_path: &ObjectPath) -> ObjectPath {
    ObjectPath::from(format!(
        "{INTERNAL_PREFIX}/{}/{}",
        encode(canonical_url.as_str().as_bytes()),
        encode(inner_path.as_ref().as_bytes())
    ))
}

pub(super) fn encode(bytes: &[u8]) -> String {
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
pub(super) struct InputStoreView {
    pub(super) inner: Arc<dyn ObjectStore>,
    pub(super) store_root: Url,
}

impl fmt::Display for InputStoreView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Silk input view for {}", self.store_root)
    }
}

pub(super) struct DecodedPath {
    pub(super) canonical_url: Url,
    pub(super) inner_path: ObjectPath,
}

impl InputStoreView {
    pub(super) fn decode_path(&self, location: &ObjectPath) -> StoreResult<DecodedPath> {
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
