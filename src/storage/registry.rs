//! Cached object-store resolution for parsed locations.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, limit::LimitStore};

use super::{
    StorageConfig,
    gcs::{GcsEnvironment, GcsStoreFactory, NativeGcsStoreFactory},
    location::{ObjectLocation, ParsedLocation, StoreHandle, StoreKind},
    output::{ObjectOutput, OutputPolicy},
};

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum StoreKey {
    Local,
    Gcs {
        bucket: String,
        environment: GcsEnvironment,
    },
}

impl fmt::Debug for StoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => f.write_str("Local"),
            Self::Gcs {
                bucket,
                environment,
            } => f
                .debug_struct("Gcs")
                .field("bucket", bucket)
                .field("environment", environment)
                .finish(),
        }
    }
}

pub struct StorageContext {
    config: StorageConfig,
    environment: GcsEnvironment,
    gcs_factory: Arc<dyn GcsStoreFactory>,
    stores: Mutex<HashMap<StoreKey, Arc<StoreHandle>>>,
}

impl StorageContext {
    pub fn new(config: StorageConfig) -> Result<Self> {
        Self::with_parts(
            config,
            GcsEnvironment::from_process()?,
            Arc::new(NativeGcsStoreFactory),
        )
    }

    pub(crate) fn with_parts(
        config: StorageConfig,
        environment: GcsEnvironment,
        gcs_factory: Arc<dyn GcsStoreFactory>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            environment,
            gcs_factory,
            stores: Mutex::new(HashMap::new()),
        })
    }

    pub fn resolve(&self, value: &str) -> Result<ObjectLocation> {
        self.resolve_parsed(ParsedLocation::parse(value)?)
    }

    pub(crate) fn resolve_pattern(&self, value: &str) -> Result<ObjectLocation> {
        self.resolve_parsed(ParsedLocation::parse_pattern(value)?)
    }

    pub async fn create_output(&self, value: &str, policy: OutputPolicy) -> Result<ObjectOutput> {
        ObjectOutput::open(self.resolve(value)?, self.config, policy).await
    }

    pub(crate) async fn create_output_at(
        &self,
        destination: ObjectLocation,
        policy: OutputPolicy,
    ) -> Result<ObjectOutput> {
        ObjectOutput::open(destination, self.config, policy).await
    }

    fn resolve_parsed(&self, parsed: ParsedLocation) -> Result<ObjectLocation> {
        match parsed {
            ParsedLocation::Local {
                original,
                display,
                path,
            } => {
                let store = self.store(StoreKey::Local)?;
                Ok(ObjectLocation::new(original, display, path, store))
            }
            ParsedLocation::Gcs {
                original,
                display,
                bucket,
                path,
            } => {
                let store = self.store(StoreKey::Gcs {
                    bucket,
                    environment: self.environment.clone(),
                })?;
                Ok(ObjectLocation::new(original, display, path, store))
            }
        }
    }

    fn store(&self, key: StoreKey) -> Result<Arc<StoreHandle>> {
        let mut stores = self
            .stores
            .lock()
            .map_err(|_| anyhow::anyhow!("object-store cache lock is poisoned"))?;
        if let Some(store) = stores.get(&key) {
            return Ok(Arc::clone(store));
        }

        let handle = match &key {
            StoreKey::Local => StoreHandle::new(
                StoreKind::Local,
                ObjectStoreUrl::local_filesystem(),
                Arc::new(LocalFileSystem::new()),
            ),
            StoreKey::Gcs {
                bucket,
                environment,
            } => {
                let store = self.gcs_factory.build(bucket, environment)?;
                StoreHandle::new(
                    StoreKind::Gcs {
                        bucket: bucket.clone(),
                    },
                    ObjectStoreUrl::parse(format!("gs://{bucket}"))?,
                    self.limited(store),
                )
            }
        };
        let handle = Arc::new(handle);
        stores.insert(key, Arc::clone(&handle));
        Ok(handle)
    }

    fn limited(&self, store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(LimitStore::new(store, self.config.max_requests))
    }

    #[cfg(test)]
    pub(crate) fn with_gcs_store(config: StorageConfig, store: Arc<dyn ObjectStore>) -> Self {
        Self::with_parts(
            config,
            GcsEnvironment::from_pairs([("GOOGLE_SKIP_SIGNATURE", "true")]).unwrap(),
            Arc::new(FixedGcsStoreFactory { store }),
        )
        .unwrap()
    }
}

#[cfg(test)]
#[derive(Debug)]
struct FixedGcsStoreFactory {
    store: Arc<dyn ObjectStore>,
}

#[cfg(test)]
impl GcsStoreFactory for FixedGcsStoreFactory {
    fn build(&self, _bucket: &str, _environment: &GcsEnvironment) -> Result<Arc<dyn ObjectStore>> {
        Ok(Arc::clone(&self.store))
    }
}

impl fmt::Debug for StorageContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageContext")
            .field("config", &self.config)
            .field("environment", &self.environment)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ops::Range,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
        Result as StoreResult, memory::InMemory, path::Path,
    };
    use tokio::sync::Barrier;

    #[derive(Debug, Default)]
    struct CountingFactory {
        builds: AtomicUsize,
    }

    impl GcsStoreFactory for CountingFactory {
        fn build(
            &self,
            _bucket: &str,
            _environment: &GcsEnvironment,
        ) -> Result<Arc<dyn ObjectStore>> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(InMemory::new()))
        }
    }

    #[derive(Debug)]
    struct ConcurrentStore {
        inner: InMemory,
        calls: AtomicUsize,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
        first_wave: Arc<Barrier>,
    }

    impl fmt::Display for ConcurrentStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("ConcurrentStore")
        }
    }

    struct ActiveRequest(Arc<AtomicUsize>);

    impl Drop for ActiveRequest {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ObjectStore for ConcurrentStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            let _active = ActiveRequest(Arc::clone(&self.active));
            if call < 2 {
                self.first_wave.wait().await;
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> StoreResult<Vec<bytes::Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> StoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    struct FixedFactory {
        store: Arc<dyn ObjectStore>,
    }

    impl GcsStoreFactory for FixedFactory {
        fn build(
            &self,
            _bucket: &str,
            _environment: &GcsEnvironment,
        ) -> Result<Arc<dyn ObjectStore>> {
            Ok(Arc::clone(&self.store))
        }
    }

    fn environment() -> GcsEnvironment {
        GcsEnvironment::from_pairs(std::iter::empty::<(&str, &str)>()).unwrap()
    }

    #[test]
    fn reuses_store_within_bucket_and_separates_buckets() {
        let factory = Arc::new(CountingFactory::default());
        let context = StorageContext::with_parts(
            StorageConfig::default(),
            environment(),
            Arc::clone(&factory) as _,
        )
        .unwrap();

        let first = context.resolve("gs://one/a.parquet").unwrap();
        let second = context.resolve("gs://one/b.parquet").unwrap();
        let third = context.resolve("gs://two/a.parquet").unwrap();

        assert!(Arc::ptr_eq(first.store(), second.store()));
        assert!(!Arc::ptr_eq(first.store(), third.store()));
        assert_eq!(factory.builds.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn local_resolution_does_not_initialize_gcs() {
        let factory = Arc::new(CountingFactory::default());
        let context = StorageContext::with_parts(
            StorageConfig::default(),
            environment(),
            Arc::clone(&factory) as _,
        )
        .unwrap();

        context.resolve("input.parquet").unwrap();

        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn store_urls_match_location_kind() {
        let context = StorageContext::with_parts(
            StorageConfig::default(),
            environment(),
            Arc::new(CountingFactory::default()),
        )
        .unwrap();

        let local = context.resolve("input.parquet").unwrap();
        let gcs = context.resolve("gs://bucket/input.parquet").unwrap();

        assert_eq!(local.store().store_url().as_str(), "file:///");
        assert_eq!(gcs.store().store_url().as_str(), "gs://bucket/");
    }

    #[test]
    fn cache_key_includes_endpoint_and_authentication() {
        let authenticated = StoreKey::Gcs {
            bucket: "bucket".to_string(),
            environment: GcsEnvironment::from_pairs([(
                "GOOGLE_BASE_URL",
                "https://storage.googleapis.com",
            )])
            .unwrap(),
        };
        let anonymous = StoreKey::Gcs {
            bucket: "bucket".to_string(),
            environment: GcsEnvironment::from_pairs([
                ("GOOGLE_BASE_URL", "http://127.0.0.1:4443"),
                ("GOOGLE_SKIP_SIGNATURE", "true"),
            ])
            .unwrap(),
        };

        assert_ne!(authenticated, anonymous);
    }

    #[tokio::test]
    async fn one_cached_store_shares_the_request_limit() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let first_wave = Arc::new(Barrier::new(3));
        let inner: Arc<dyn ObjectStore> = Arc::new(ConcurrentStore {
            inner: InMemory::new(),
            calls: AtomicUsize::new(0),
            active: Arc::clone(&active),
            maximum: Arc::clone(&maximum),
            first_wave: Arc::clone(&first_wave),
        });
        let context = StorageContext::with_parts(
            StorageConfig {
                max_requests: 2,
                ..StorageConfig::default()
            },
            environment(),
            Arc::new(FixedFactory { store: inner }),
        )
        .unwrap();
        let location = context.resolve("gs://bucket/missing.parquet").unwrap();

        let requests = (0..4)
            .map(|_| {
                let store = Arc::clone(location.store().object_store());
                tokio::spawn(async move { store.get(&Path::from("missing.parquet")).await })
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(2), first_wave.wait())
            .await
            .expect("two requests did not reach the limited store");
        for request in requests {
            let _ = request.await.unwrap();
        }

        assert_eq!(maximum.load(Ordering::SeqCst), 2);
    }
}
