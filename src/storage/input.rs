//! Exact input lookup and deterministic object-store glob expansion.
//!
//! Exact locations issue one metadata request. Glob patterns issue one recursive
//! listing beneath their literal prefix, then match full keys in memory. Results
//! from every store are sorted by display location and deduplicated by store and
//! object key.

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStoreExt, path::Path};

use super::{ObjectLocation, StorageContext, glob::ParsedInput};

#[derive(Clone, Debug)]
pub struct InputObject {
    location: ObjectLocation,
    meta: ObjectMeta,
}

impl InputObject {
    #[cfg(test)]
    pub(crate) fn new(location: ObjectLocation, meta: ObjectMeta) -> Self {
        Self { location, meta }
    }

    #[must_use]
    pub fn location(&self) -> &ObjectLocation {
        &self.location
    }

    #[must_use]
    pub fn metadata(&self) -> &ObjectMeta {
        &self.meta
    }

    #[must_use]
    pub fn extension(&self) -> Option<&str> {
        self.meta.location.extension()
    }
}

impl StorageContext {
    pub async fn resolve_input(&self, value: &str) -> Result<InputObject> {
        let location = self.resolve(value)?;
        head_input(location).await
    }

    pub async fn resolve_inputs(&self, values: &[String]) -> Result<Vec<InputObject>> {
        let mut inputs = Vec::new();
        for value in values {
            match ParsedInput::parse(self, value)? {
                ParsedInput::Exact(location) => inputs.push(head_input(location).await?),
                ParsedInput::Glob(pattern) => {
                    let prefix = (!pattern.list_prefix().is_root()).then(|| pattern.list_prefix());
                    let objects = pattern
                        .location()
                        .store()
                        .object_store()
                        .list(prefix)
                        .try_collect::<Vec<_>>()
                        .await
                        .with_context(|| {
                            format!(
                                "could not list objects for input pattern '{}'",
                                pattern.original()
                            )
                        })?;
                    for meta in objects {
                        if pattern.matches(&meta.location) {
                            inputs.push(InputObject {
                                location: pattern.concrete_location(meta.location.clone())?,
                                meta,
                            });
                        }
                    }
                }
            }
        }

        inputs.sort_by(|left, right| left.location.display().cmp(right.location.display()));
        let mut seen = HashSet::<(String, Path)>::new();
        inputs.retain(|input| {
            seen.insert((
                input.location.store().store_url().as_str().to_string(),
                input.meta.location.clone(),
            ))
        });

        if inputs.is_empty() {
            bail!("No input files found matching patterns: {values:?}");
        }
        Ok(inputs)
    }
}

async fn head_input(location: ObjectLocation) -> Result<InputObject> {
    let meta = location
        .store()
        .object_store()
        .head(location.path())
        .await
        .with_context(|| {
            format!(
                "could not read input metadata for '{}'",
                location.original()
            )
        })?;
    Ok(InputObject { location, meta })
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        ops::Range,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
        Result as StoreResult, memory::InMemory, path::Path,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::storage::{
        StorageConfig,
        gcs::{GcsEnvironment, GcsStoreFactory},
    };

    #[derive(Debug, Default)]
    struct RequestLog {
        heads: Mutex<Vec<Path>>,
        lists: Mutex<Vec<Option<Path>>>,
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        requests: Arc<RequestLog>,
    }

    impl fmt::Display for CountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
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
            if options.head {
                self.requests.heads.lock().unwrap().push(location.clone());
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> StoreResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.requests.lists.lock().unwrap().push(prefix.cloned());
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

    async fn fixture(keys: &[&str]) -> (StorageContext, Arc<RequestLog>) {
        let inner = InMemory::new();
        for key in keys {
            inner
                .put(
                    &Path::parse(*key).unwrap(),
                    PutPayload::from_static(b"test"),
                )
                .await
                .unwrap();
        }
        let requests = Arc::new(RequestLog::default());
        let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner,
            requests: Arc::clone(&requests),
        });
        let context = StorageContext::with_parts(
            StorageConfig::default(),
            GcsEnvironment::from_pairs([("GOOGLE_SKIP_SIGNATURE", "true")]).unwrap(),
            Arc::new(FixedFactory { store }),
        )
        .unwrap();
        (context, requests)
    }

    #[tokio::test]
    async fn exact_input_uses_head_without_listing() {
        let (storage, requests) = fixture(&["data/input.parquet"]).await;

        let input = storage
            .resolve_input("gs://bucket/data/input.parquet")
            .await
            .unwrap();

        assert_eq!(input.metadata().size, 4);
        assert_eq!(
            requests.heads.lock().unwrap().as_slice(),
            &[Path::from("data/input.parquet")]
        );
        assert!(requests.lists.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn glob_lists_once_at_its_literal_prefix() {
        let (storage, requests) = fixture(&[
            "data/year=2025/a.parquet",
            "data/year=2025/b.parquet",
            "data/year=2024/c.parquet",
        ])
        .await;

        let inputs = storage
            .resolve_inputs(&["gs://bucket/data/year=2025/*.parquet".to_string()])
            .await
            .unwrap();

        assert_eq!(inputs.len(), 2);
        assert_eq!(
            requests.lists.lock().unwrap().as_slice(),
            &[Some(Path::from("data/year=2025"))]
        );
        assert!(requests.heads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn results_are_sorted_and_deduplicated_across_patterns() {
        let (storage, requests) = fixture(&["data/b.parquet", "data/a.parquet"]).await;

        let inputs = storage
            .resolve_inputs(&[
                "gs://bucket/data/*.parquet".to_string(),
                "gs://bucket/data/a*.parquet".to_string(),
            ])
            .await
            .unwrap();
        let displays = inputs
            .iter()
            .map(|input| input.location().display())
            .collect::<Vec<_>>();

        assert_eq!(
            displays,
            ["gs://bucket/data/a.parquet", "gs://bucket/data/b.parquet"]
        );
        assert_eq!(requests.lists.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn matches_spaces_hashes_percent_signs_and_literal_glob_tokens() {
        let (storage, _) = fixture(&[
            "data/a file.parquet",
            "data/a#hash.parquet",
            "data/a%percent.parquet",
            "data/literal*.parquet",
            "data/literal?.parquet",
        ])
        .await;

        let inputs = storage
            .resolve_inputs(&[
                "gs://bucket/data/a*.parquet".to_string(),
                "gs://bucket/data/literal[*].parquet".to_string(),
                "gs://bucket/data/literal[?].parquet".to_string(),
            ])
            .await
            .unwrap();
        let paths = inputs
            .iter()
            .map(|input| input.metadata().location.as_ref())
            .collect::<Vec<_>>();

        assert_eq!(
            paths,
            [
                "data/a file.parquet",
                "data/a#hash.parquet",
                "data/a%percent.parquet",
                "data/literal*.parquet",
                "data/literal?.parquet",
            ]
        );
    }

    #[tokio::test]
    async fn missing_exact_input_names_the_location() {
        let (storage, _) = fixture(&[]).await;

        let error = storage
            .resolve_input("gs://bucket/data/missing.parquet")
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("gs://bucket/data/missing.parquet"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn empty_glob_names_the_original_patterns() {
        let (storage, _) = fixture(&[]).await;
        let patterns = vec![
            "gs://bucket/data/*.parquet".to_string(),
            "gs://bucket/other/*.arrow".to_string(),
        ];

        let error = storage
            .resolve_inputs(&patterns)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains(&patterns[0]) && error.contains(&patterns[1]),
            "{error}"
        );
    }

    #[tokio::test]
    async fn format_extension_uses_the_decoded_object_key() {
        let (storage, _) = fixture(&["data/input.parquet"]).await;

        let input = storage
            .resolve_input("gs://bucket/data/input%2Eparquet")
            .await
            .unwrap();

        assert_eq!(input.extension(), Some("parquet"));
    }

    #[tokio::test]
    async fn percent_escaped_glob_token_is_an_exact_key() {
        let (storage, requests) = fixture(&["data/literal*.parquet"]).await;

        let inputs = storage
            .resolve_inputs(&["gs://bucket/data/literal%2A.parquet".to_string()])
            .await
            .unwrap();

        assert_eq!(
            inputs[0].metadata().location.as_ref(),
            "data/literal*.parquet"
        );
        assert_eq!(requests.heads.lock().unwrap().len(), 1);
        assert!(requests.lists.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn generated_gcs_displays_round_trip_special_keys() {
        let (storage, _) = fixture(&["data/a # % * ?.parquet"]).await;

        let inputs = storage
            .resolve_inputs(&["gs://bucket/data/*.parquet".to_string()])
            .await
            .unwrap();
        let display = inputs[0].location().display();
        let reparsed = storage.resolve(display).unwrap();

        assert_eq!(reparsed.path(), &inputs[0].metadata().location);
        assert!(
            display.contains("%20") && display.contains("%25"),
            "{display}"
        );
    }

    #[tokio::test]
    async fn resolves_mixed_local_and_gcs_patterns() {
        let temp = TempDir::new().unwrap();
        std::fs::write(temp.path().join("b.parquet"), b"local").unwrap();
        let local_pattern = temp.path().join("*.parquet").to_string_lossy().into_owned();
        let (storage, _) = fixture(&["data/a.parquet"]).await;

        let inputs = storage
            .resolve_inputs(&["gs://bucket/data/*.parquet".to_string(), local_pattern])
            .await
            .unwrap();
        let displays = inputs
            .iter()
            .map(|input| input.location().display())
            .collect::<Vec<_>>();

        assert_eq!(inputs.len(), 2);
        assert!(displays[0] < displays[1]);
        assert!(inputs.iter().any(|input| matches!(
            input.location().store().kind(),
            crate::storage::StoreKind::Local
        )));
        assert!(inputs.iter().any(|input| matches!(
            input.location().store().kind(),
            crate::storage::StoreKind::Gcs { .. }
        )));
    }

    #[tokio::test]
    async fn relative_parent_glob_returns_a_usable_local_path() {
        let current = std::env::current_dir().unwrap();
        let sibling = tempfile::Builder::new()
            .prefix("silk-chiffon-parent-glob-")
            .tempdir_in(current.parent().unwrap())
            .unwrap();
        let input = sibling.path().join("input.parquet");
        std::fs::write(&input, b"local").unwrap();
        let pattern = format!(
            "../{}/*.parquet",
            sibling.path().file_name().unwrap().to_string_lossy()
        );
        let (storage, _) = fixture(&[]).await;

        let inputs = storage.resolve_inputs(&[pattern]).await.unwrap();

        assert_eq!(inputs[0].location().display(), input.to_string_lossy());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn parent_glob_after_symlink_follows_filesystem_resolution() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("root");
        let target = temp.path().join("target");
        let nested = target.join("nested");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        std::os::unix::fs::symlink(&nested, root.join("link")).unwrap();
        let input = target.join("input.parquet");
        std::fs::write(&input, b"local").unwrap();
        let pattern = root
            .join("link/../*.parquet")
            .to_string_lossy()
            .into_owned();
        let (storage, _) = fixture(&[]).await;

        let inputs = storage.resolve_inputs(&[pattern]).await.unwrap();

        assert_eq!(
            inputs[0].location().display(),
            std::fs::canonicalize(input).unwrap().to_string_lossy()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn wildcard_component_is_not_canonicalized_as_a_literal_directory() {
        let temp = TempDir::new().unwrap();
        for directory in ["*", "other"] {
            let directory = temp.path().join(directory);
            std::fs::create_dir(&directory).unwrap();
            std::fs::write(directory.join("input.parquet"), b"local").unwrap();
        }
        let pattern = temp
            .path()
            .join("*/input.parquet")
            .to_string_lossy()
            .into_owned();
        let (storage, _) = fixture(&[]).await;

        let inputs = storage.resolve_inputs(&[pattern]).await.unwrap();

        assert_eq!(inputs.len(), 2);
    }

    #[tokio::test]
    async fn rejects_parent_traversal_after_a_glob_component() {
        let temp = TempDir::new().unwrap();
        let pattern = temp
            .path()
            .join("*/../*.parquet")
            .to_string_lossy()
            .into_owned();
        let (storage, _) = fixture(&[]).await;

        let error = storage
            .resolve_inputs(&[pattern])
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("'..' after its first glob component"),
            "{error}"
        );
    }
}
