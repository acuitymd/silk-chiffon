use std::{
    fmt,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use clap::{Args, Command};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    memory::InMemory, path::Path as ObjectPath,
};
use silk_chiffon_storage::{
    Location, LocationInput, LocationPattern, ObjectStoreCreatorFn, StorageAccess, StorageBackend,
    StorageError, StorageRegistry, StorageSession,
};
use url::Url;

#[derive(Debug, Default)]
struct Observations {
    heads: usize,
    listing_prefixes: Vec<Option<String>>,
}

static OBSERVATIONS: Mutex<Observations> = Mutex::new(Observations {
    heads: 0,
    listing_prefixes: Vec::new(),
});

#[derive(Debug, Default)]
struct ObservedStore {
    inner: InMemory,
}

impl fmt::Display for ObservedStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("observed memory store")
    }
}

#[async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> StoreResult<GetResult> {
        if options.head {
            OBSERVATIONS.lock().unwrap().heads += 1;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<ObjectPath>>,
    ) -> BoxStream<'static, StoreResult<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        OBSERVATIONS
            .lock()
            .unwrap()
            .listing_prefixes
            .push(prefix.map(ToString::to_string));
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn validate_location(_location: &Location, _settings: &()) -> anyhow::Result<()> {
    Ok(())
}

fn create_object_store(
    _store_url: &Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(InMemory::new()))
}

fn create_observed_store(
    _store_url: &Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(ObservedStore::default()))
}

fn map_bare_location(input: &str, _settings: &()) -> anyhow::Result<Location> {
    Ok(Location::parse_url(format!("mem://bucket/{input}"))?)
}

fn map_bare_pattern(input: &str, _settings: &()) -> anyhow::Result<LocationPattern> {
    Ok(LocationPattern::parse_url(format!("mem://bucket/{input}"))?)
}

#[derive(Args)]
struct PatternArgs {
    #[arg(long)]
    pattern_prefix: String,
}

fn validate_typed_location(_location: &Location, _settings: &PatternArgs) -> anyhow::Result<()> {
    Ok(())
}

fn map_typed_bare_location(input: &str, settings: &PatternArgs) -> anyhow::Result<Location> {
    Ok(Location::parse_url(format!(
        "mem://bucket/{}/{input}",
        settings.pattern_prefix
    ))?)
}

fn map_typed_bare_pattern(input: &str, settings: &PatternArgs) -> anyhow::Result<LocationPattern> {
    Ok(LocationPattern::parse_url(format!(
        "mem://bucket/{}/{input}",
        settings.pattern_prefix
    ))?)
}

fn create_typed_store(
    _store_url: &Url,
    _settings: &PatternArgs,
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(InMemory::new()))
}

fn storage() -> StorageSession {
    storage_with_creator(create_object_store)
}

fn storage_with_creator(creator: ObjectStoreCreatorFn<()>) -> StorageSession {
    let backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .location_validator(validate_location)
        .bare_location_mapper(map_bare_location)
        .bare_pattern_mapper(map_bare_pattern)
        .object_store_creator(creator)
        .build()
        .unwrap();
    let registry = StorageRegistry::builder()
        .register(backend)
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("pattern-test"));
    let matches = command.try_get_matches_from(["pattern-test"]).unwrap();
    registry.create_session(&matches).unwrap()
}

async fn put(storage: &StorageSession, url: &str) {
    let input = LocationInput::parse(url).unwrap();
    let handle = storage.output_handle(&input).unwrap();
    handle
        .object_store()
        .put(handle.object_path(), Bytes::from_static(b"test").into())
        .await
        .unwrap();
}

#[tokio::test]
async fn expands_complete_object_paths_and_preserves_query_syntax() {
    let storage = storage();
    put(&storage, "mem://bucket/data/part-a.parquet").await;
    put(&storage, "mem://bucket/data/part-ab.parquet").await;

    let pattern =
        LocationPattern::parse("mem://bucket/data/part-?.parquet??versionId=one").unwrap();
    let matches = storage.expand_input_pattern(&pattern).await.unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(
        matches[0].url().as_str(),
        "mem://bucket/data/part-a.parquet?versionId=one"
    );
    assert_eq!(matches[0].object_path().as_ref(), "data/part-a.parquet");

    let exact = LocationInput::parse(matches[0].url().as_str()).unwrap();
    assert_eq!(
        storage.input_handle(&exact).unwrap().object_path(),
        matches[0].object_path()
    );
    let reusable_pattern = LocationPattern::parse(
        matches[0]
            .url()
            .as_str()
            .replace("?versionId", "??versionId"),
    )
    .unwrap();
    assert_eq!(
        storage
            .expand_input_pattern(&reusable_pattern)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn encoded_metacharacters_are_literal_and_matched_urls_are_pattern_safe() {
    let storage = storage();
    put(&storage, "mem://bucket/data/literal%2A.parquet").await;
    put(&storage, "mem://bucket/data/literal%2A%3F%5Bx%5D.parquet").await;

    let literal = LocationPattern::parse("mem://bucket/data/literal%2A.parquet").unwrap();
    let literal_matches = storage.expand_input_pattern(&literal).await.unwrap();
    assert_eq!(literal_matches.len(), 1);
    assert_eq!(
        literal_matches[0].object_path().as_ref(),
        "data/literal*.parquet"
    );

    let active = LocationPattern::parse("mem://bucket/data/*.parquet").unwrap();
    let active_matches = storage.expand_input_pattern(&active).await.unwrap();
    assert_eq!(active_matches.len(), 2);
    let metachar_match = active_matches
        .iter()
        .find(|handle| handle.object_path().as_ref().contains('?'))
        .unwrap();
    assert_eq!(
        metachar_match.url().as_str(),
        "mem://bucket/data/literal%2A%3F%5Bx%5D.parquet"
    );

    let reparsed = LocationPattern::parse(metachar_match.url().as_str()).unwrap();
    assert_eq!(
        storage.expand_input_pattern(&reparsed).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn encoded_unicode_and_percent_signs_preserve_object_path_identity() {
    let storage = storage();
    put(&storage, "mem://bucket/data/donn%C3%A9es/cent%25.arrow").await;
    let exact = storage
        .input_handle(
            &LocationInput::parse("mem://bucket/data/donn%C3%A9es/cent%25.arrow").unwrap(),
        )
        .unwrap();
    assert_eq!(exact.object_path().as_ref(), "data/données/cent%.arrow");

    let pattern = LocationPattern::parse("mem://bucket/data/donn%C3%A9es/*.arrow").unwrap();
    let matches = storage.expand_input_pattern(&pattern).await.unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(
        matches[0].url().as_str(),
        "mem://bucket/data/donn%C3%A9es/cent%25.arrow"
    );
    assert_eq!(
        matches[0].object_path().as_ref(),
        "data/données/cent%.arrow"
    );
}

#[tokio::test]
async fn adversarial_object_names_round_trip_as_exact_locations() {
    let storage = storage();
    for url in [
        "mem://bucket/data/%23%25%2A%3F%5B%5D%20donn%C3%A9es.arrow",
        "mem://bucket/data/%E2%98%83.arrow",
    ] {
        put(&storage, url).await;
    }

    let pattern = LocationPattern::parse("mem://bucket/data/*.arrow??token=a?b").unwrap();
    let matches = storage.expand_input_pattern(&pattern).await.unwrap();

    assert_eq!(matches.len(), 2);
    for handle in matches {
        assert_eq!(handle.url().query(), Some("token=a?b"));
        let exact = LocationInput::parse(handle.url().as_str()).unwrap();
        assert_eq!(
            storage.input_handle(&exact).unwrap().object_path(),
            handle.object_path()
        );
    }
}

#[tokio::test]
async fn encoded_separators_nul_and_empty_segments_never_reach_listing() {
    let storage = storage();

    for input in [
        "mem://bucket/data/%2F*.arrow",
        "mem://bucket/data/%00*.arrow",
        "mem://bucket/data//*.arrow",
    ] {
        let pattern = LocationPattern::parse(input).unwrap();
        let error = storage.expand_input_pattern(&pattern).await.unwrap_err();
        assert!(
            matches!(error, StorageError::InvalidObjectPath { .. }),
            "{input:?} should fail"
        );
    }
}

#[tokio::test]
async fn matched_urls_preserve_ipv6_authorities() {
    let storage = storage();
    put(&storage, "mem://[::1]/data/one.arrow").await;

    let pattern = LocationPattern::parse("mem://[::1]/data/*.arrow").unwrap();
    let matches = storage.expand_input_pattern(&pattern).await.unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].url().as_str(), "mem://[::1]/data/one.arrow");
}

#[tokio::test]
async fn exact_and_bare_patterns_may_expand_to_zero_or_more_handles() {
    let storage = storage();
    put(&storage, "mem://bucket/nested/one.arrow").await;

    let absent = LocationPattern::parse("mem://bucket/nested/absent.arrow").unwrap();
    assert!(
        storage
            .expand_input_pattern(&absent)
            .await
            .unwrap()
            .is_empty()
    );

    let bare = LocationPattern::parse("nested/*.arrow").unwrap();
    let matches = storage.expand_input_pattern(&bare).await.unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].url().as_str(), "mem://bucket/nested/one.arrow");
}

#[tokio::test]
async fn bare_exact_and_pattern_mapping_share_typed_backend_settings() {
    let backend = StorageBackend::with_args::<PatternArgs>()
        .name("typed-memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .location_validator(validate_typed_location)
        .bare_location_mapper(map_typed_bare_location)
        .bare_pattern_mapper(map_typed_bare_pattern)
        .object_store_creator(create_typed_store)
        .build()
        .unwrap();
    let registry = StorageRegistry::builder()
        .register(backend)
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("typed-pattern-test"));
    let matches = command
        .try_get_matches_from(["typed-pattern-test", "--pattern-prefix", "configured"])
        .unwrap();
    let storage = registry.create_session(&matches).unwrap();
    put(&storage, "mem://bucket/configured/one.arrow").await;

    let exact = storage
        .input_handle(&LocationInput::parse("one.arrow").unwrap())
        .unwrap();
    assert_eq!(exact.object_path().as_ref(), "configured/one.arrow");

    let pattern = LocationPattern::parse("*.arrow").unwrap();
    let matches = storage.expand_input_pattern(&pattern).await.unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].object_path().as_ref(), "configured/one.arrow");
}

#[tokio::test]
async fn matches_classes_recursive_segments_case_and_leading_dots() {
    let storage = storage();
    for url in [
        "mem://bucket/data/root.arrow",
        "mem://bucket/data/a/one.arrow",
        "mem://bucket/data/b/.hidden.arrow",
        "mem://bucket/data/deep/nested/two.arrow",
        "mem://bucket/data/a/UPPER.arrow",
    ] {
        put(&storage, url).await;
    }

    let class = LocationPattern::parse("mem://bucket/data/[ab]/*.arrow").unwrap();
    let class_matches = storage.expand_input_pattern(&class).await.unwrap();
    assert_eq!(class_matches.len(), 3);

    let recursive = LocationPattern::parse("mem://bucket/data/**/*.arrow").unwrap();
    let recursive_matches = storage.expand_input_pattern(&recursive).await.unwrap();
    assert_eq!(recursive_matches.len(), 5);

    let negative_class = LocationPattern::parse("mem://bucket/data/[!a]/*.arrow").unwrap();
    let negative_class_matches = storage.expand_input_pattern(&negative_class).await.unwrap();
    assert_eq!(negative_class_matches.len(), 1);
    assert_eq!(
        negative_class_matches[0].object_path().as_ref(),
        "data/b/.hidden.arrow"
    );

    let one_segment = LocationPattern::parse("mem://bucket/data/*.arrow").unwrap();
    let one_segment_matches = storage.expand_input_pattern(&one_segment).await.unwrap();
    assert_eq!(one_segment_matches.len(), 1);
    assert_eq!(
        one_segment_matches[0].object_path().as_ref(),
        "data/root.arrow"
    );

    let lowercase = LocationPattern::parse("mem://bucket/data/a/u*.arrow").unwrap();
    assert!(
        storage
            .expand_input_pattern(&lowercase)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn exact_patterns_bypass_listing_and_globs_use_complete_literal_prefixes() {
    let storage = storage_with_creator(create_observed_store);
    put(&storage, "mem://bucket/data/one.arrow").await;
    *OBSERVATIONS.lock().unwrap() = Observations::default();

    let exact = LocationPattern::parse("mem://bucket/data/one.arrow").unwrap();
    assert_eq!(storage.expand_input_pattern(&exact).await.unwrap().len(), 1);
    {
        let observations = OBSERVATIONS.lock().unwrap();
        assert_eq!(observations.heads, 1);
        assert!(observations.listing_prefixes.is_empty());
    }

    let active = LocationPattern::parse("mem://bucket/data/*.arrow").unwrap();
    assert_eq!(
        storage.expand_input_pattern(&active).await.unwrap().len(),
        1
    );
    let observations = OBSERVATIONS.lock().unwrap();
    assert_eq!(observations.heads, 1);
    assert_eq!(observations.listing_prefixes, [Some("data".to_owned())]);

    drop(observations);
    let no_literal_prefix = LocationPattern::parse("mem://bucket/*.arrow").unwrap();
    assert!(
        storage
            .expand_input_pattern(&no_literal_prefix)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        OBSERVATIONS.lock().unwrap().listing_prefixes,
        [Some("data".to_owned()), None]
    );
}

#[test]
fn rejects_glob_syntax_outside_the_url_path_and_malformed_patterns() {
    for input in [
        "m*em://bucket/object",
        "m?em://bucket/object",
        "mem://buck*et/object",
        "mem://buck?et/object",
    ] {
        assert!(
            LocationPattern::parse(input).is_err(),
            "{input:?} should fail"
        );
    }
    for input in [
        "mem://bucket/a/**b",
        "mem://bucket/a/b**",
        "mem://bucket/a/***",
        "mem://bucket/a/[abc",
        "mem://bucket/a/%GG*.arrow",
        "mem://user@bucket/a/*.arrow",
        "mem://bucket/a/*.arrow#fragment",
    ] {
        assert!(
            LocationPattern::parse(input).is_err(),
            "{input:?} should fail"
        );
    }
}
