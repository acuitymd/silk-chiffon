#![cfg(any(feature = "gcs", feature = "s3"))]

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use clap::Command;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, LocationPattern, ObjectUpload, OutputPreparation,
    StorageBackend, StorageError, StorageRegistry, StorageSession,
};

struct LiveConfig {
    scheme: &'static str,
    bucket: String,
    run_prefix: String,
}

impl LiveConfig {
    fn from_env(
        scheme: &'static str,
        bucket_variable: &'static str,
        prefix_variable: &'static str,
    ) -> Result<Self> {
        let bucket = std::env::var(bucket_variable)
            .with_context(|| format!("set {bucket_variable} to an explicit live-test bucket"))?;
        let prefix = std::env::var(prefix_variable).with_context(|| {
            format!("set {prefix_variable} to an explicit non-root live-test prefix")
        })?;
        validate_bucket(scheme, &bucket, bucket_variable)?;
        validate_prefix(&prefix, prefix_variable)?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let run_prefix = format!("{prefix}/{}-{nonce}", std::process::id());
        Ok(Self {
            scheme,
            bucket,
            run_prefix,
        })
    }

    fn url(&self, suffix: &str) -> String {
        format!(
            "{}://{}/{}/{}",
            self.scheme, self.bucket, self.run_prefix, suffix
        )
    }

    fn pattern(&self, suffix: &str) -> String {
        self.url(suffix)
    }
}

fn validate_bucket(scheme: &str, bucket: &str, variable: &str) -> Result<()> {
    ensure!(!bucket.trim().is_empty(), "{variable} must not be empty");
    ensure!(
        !bucket
            .bytes()
            .any(|byte| matches!(byte, b'/' | b'@' | b'?' | b'#' | b':')),
        "{variable} must be one URL host without user information, a port, query, fragment, or path"
    );
    let parsed = url::Url::parse(&format!("{scheme}://{bucket}/"))?;
    ensure!(
        parsed.host_str() == Some(bucket),
        "{variable} must be one canonical URL host"
    );
    ensure!(
        parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.port().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path() == "/",
        "{variable} must not alter URL authority or path structure"
    );
    Ok(())
}

fn validate_prefix(prefix: &str, variable: &str) -> Result<()> {
    ensure!(
        prefix == prefix.trim_matches('/'),
        "{variable} must not start or end with /"
    );
    ensure!(
        prefix.split('/').count() >= 2,
        "{variable} must contain at least two non-root path segments"
    );
    ensure!(
        prefix.split('/').all(|segment| !segment.is_empty()),
        "{variable} must not contain empty path segments"
    );
    ensure!(
        prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'/')),
        "{variable} may contain only ASCII letters, digits, -, _, and /"
    );
    Ok(())
}

#[test]
fn live_location_validation_rejects_authority_and_prefix_escape_inputs() {
    for bucket in [
        "bucket/escape",
        "user@bucket",
        "bucket:9000",
        "bucket?query",
        "bucket#fragment",
    ] {
        assert!(validate_bucket("s3", bucket, "BUCKET").is_err(), "{bucket}");
    }
    for prefix in ["one", "/one/two", "one/two/", "one//two", "one/../two"] {
        assert!(validate_prefix(prefix, "PREFIX").is_err(), "{prefix}");
    }
    validate_bucket("gs", "safe-bucket.example", "BUCKET").unwrap();
    validate_prefix("live-tests/unique", "PREFIX").unwrap();
}

fn session(backend: StorageBackend) -> Result<StorageSession> {
    let registry = StorageRegistry::builder().register(backend).build()?;
    let command = registry.augment_args(Command::new("cloud-live-test"));
    let matches = command.try_get_matches_from([
        "cloud-live-test",
        "--object-store-upload-part-size",
        "5MiB",
    ])?;
    Ok(registry.create_session(&matches)?)
}

async fn exercise_live_backend(
    config: &LiveConfig,
    backend: fn() -> Result<StorageBackend, silk_chiffon_storage::StorageBackendBuildError>,
) -> Result<()> {
    let storage = session(backend()?)?;
    let exact_url = config.url("exact.bin");
    let exact = storage
        .input_handle(&LocationInput::parse(&exact_url)?)
        .context("resolve exact live object")?;
    exact
        .object_store()
        .put(
            exact.object_path(),
            Bytes::from_static(b"0123456789").into(),
        )
        .await
        .context("write exact live object")?;

    let observed = storage
        .lookup_input(&LocationInput::parse(&exact_url)?)
        .await
        .context("observe exact live object")?;
    ensure!(
        observed.metadata().size == 10,
        "unexpected exact object size"
    );
    let range = observed
        .handle()
        .object_store()
        .get_range(observed.handle().object_path(), 2..6)
        .await
        .context("read exact live range")?;
    ensure!(
        range == Bytes::from_static(b"2345"),
        "unexpected range bytes"
    );

    for name in ["set/one.bin", "set/two.bin"] {
        let handle = storage.input_handle(&LocationInput::parse(config.url(name))?)?;
        handle
            .object_store()
            .put(handle.object_path(), Bytes::from_static(b"set").into())
            .await?;
    }
    let pattern = LocationPattern::parse(config.pattern("set/*.bin"))?;
    let matches = storage.expand_input_pattern(&pattern).await?;
    ensure!(matches.len() == 2, "expected two live pattern matches");

    let small_url = config.url("small-output.bin");
    let small = storage
        .prepare_output_target(
            &LocationInput::parse(&small_url)?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
    let mut small_upload = ObjectUpload::new(small);
    small_upload.write(Bytes::from_static(b"small")).await?;
    small_upload.complete().await?;

    let multipart_url = config.url("multipart-output.bin");
    let multipart = storage
        .prepare_output_target(
            &LocationInput::parse(&multipart_url)?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
    let mut multipart_upload = ObjectUpload::new(multipart);
    multipart_upload
        .write(Bytes::from(vec![0x5A; 5 * 1024 * 1024 + 1]))
        .await?;
    multipart_upload.complete().await?;

    let unfinished_url = config.url("unfinished-multipart.bin");
    let unfinished = storage.input_handle(&LocationInput::parse(&unfinished_url)?)?;
    let mut unfinished_upload = unfinished
        .object_store()
        .put_multipart(unfinished.object_path())
        .await?;
    unfinished_upload
        .put_part(Bytes::from(vec![0xA5; 5 * 1024 * 1024]).into())
        .await?;
    unfinished_upload.abort().await?;
    let unfinished_lookup = storage
        .lookup_input(&LocationInput::parse(&unfinished_url)?)
        .await;
    ensure!(
        matches!(
            unfinished_lookup,
            Err(StorageError::ObjectStore(
                object_store::Error::NotFound { .. }
            ))
        ),
        "aborted multipart upload unexpectedly created its final object"
    );

    let observation_session = session(backend()?)?;
    let existing_output = observation_session
        .prepare_output_target(
            &LocationInput::parse(&small_url)?,
            &OutputPreparation::new(ExistingOutput::RejectIfObserved, false),
        )
        .await;
    ensure!(
        matches!(
            existing_output,
            Err(StorageError::OutputTargetAlreadyExists { .. })
        ),
        "fresh session did not observe the existing output"
    );

    let claim_url = config.url("same-session-claim.bin");
    storage
        .prepare_output_target(
            &LocationInput::parse(&claim_url)?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
    let duplicate_claim = storage
        .prepare_output_target(
            &LocationInput::parse(&claim_url)?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await;
    ensure!(
        matches!(
            duplicate_claim,
            Err(StorageError::OutputTargetAlreadyClaimed { .. })
        ),
        "same storage session accepted a duplicate output claim"
    );
    Ok(())
}

async fn cleanup_prefix(
    config: &LiveConfig,
    backend: fn() -> Result<StorageBackend, silk_chiffon_storage::StorageBackendBuildError>,
) -> Result<Vec<String>> {
    let storage = session(backend()?)?;
    let root = storage.input_handle(&LocationInput::parse(config.url("cleanup-root"))?)?;
    let prefix = object_store::path::Path::parse(&config.run_prefix)?;
    let objects = root
        .object_store()
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await?;
    let mut deletion_errors = Vec::new();
    for object in objects {
        if let Err(error) = root.object_store().delete(&object.location).await {
            deletion_errors.push(format!("{}: {error}", object.location));
        }
    }
    let leftovers = root
        .object_store()
        .list(Some(&prefix))
        .map_ok(|object| object.location.to_string())
        .try_collect::<Vec<_>>()
        .await?;
    ensure!(
        deletion_errors.is_empty(),
        "live cleanup failures under {}: {}",
        config.run_prefix,
        deletion_errors.join(", ")
    );
    Ok(leftovers)
}

async fn run_with_cleanup(
    config: LiveConfig,
    backend: fn() -> Result<StorageBackend, silk_chiffon_storage::StorageBackendBuildError>,
) {
    let exercise = exercise_live_backend(&config, backend).await;
    let cleanup = cleanup_prefix(&config, backend).await;
    match (exercise, cleanup) {
        (Ok(()), Ok(leftovers)) if leftovers.is_empty() => {}
        (exercise, cleanup) => panic!(
            "live cloud test failed under prefix {}: exercise={exercise:?}; cleanup={cleanup:?}",
            config.run_prefix
        ),
    }
}

#[cfg(feature = "gcs")]
#[tokio::test]
#[ignore = "requires explicit GCS bucket, prefix, and upstream credentials"]
async fn live_gcs_exact_patterns_ranges_outputs_multipart_claims_and_cleanup() {
    let config = LiveConfig::from_env(
        "gs",
        "SILK_CHIFFON_LIVE_GCS_BUCKET",
        "SILK_CHIFFON_LIVE_GCS_PREFIX",
    )
    .unwrap();
    run_with_cleanup(config, silk_chiffon_storage::gcs::backend).await;
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires explicit S3 bucket, prefix, and upstream credentials"]
async fn live_s3_exact_patterns_ranges_outputs_multipart_claims_and_cleanup() {
    let config = LiveConfig::from_env(
        "s3",
        "SILK_CHIFFON_LIVE_S3_BUCKET",
        "SILK_CHIFFON_LIVE_S3_PREFIX",
    )
    .unwrap();
    run_with_cleanup(config, silk_chiffon_storage::s3::backend).await;
}
