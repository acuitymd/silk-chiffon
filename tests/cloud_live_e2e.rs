#![cfg(any(feature = "gcs", feature = "s3"))]

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use clap::Command as ClapCommand;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use silk_chiffon::{Cli, Command};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, OutputPreparation, StorageRegistry, StorageSession,
};
use silk_chiffon_test_support::{TestBatch, TestFile};

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
        Ok(Self {
            scheme,
            bucket,
            run_prefix: format!("{prefix}/root-e2e-{}-{nonce}", std::process::id()),
        })
    }

    fn url(&self, suffix: &str) -> String {
        format!(
            "{}://{}/{}/{}",
            self.scheme, self.bucket, self.run_prefix, suffix
        )
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

fn session() -> Result<StorageSession> {
    let builder = StorageRegistry::builder();
    #[cfg(feature = "gcs")]
    let builder = builder.register(silk_chiffon_storage::gcs::backend()?);
    #[cfg(feature = "s3")]
    let builder = builder.register(silk_chiffon_storage::s3::backend()?);
    let registry = builder.build()?;
    let command = registry.augment_args(ClapCommand::new("cloud-live-e2e"));
    let matches = command.try_get_matches_from(["cloud-live-e2e"])?;
    Ok(registry.create_session(&matches)?)
}

async fn run_cli(arguments: Vec<String>) -> Result<()> {
    let cli = Cli::try_parse_from(arguments)?;
    if matches!(cli.command, Command::Completions { .. }) {
        bail!("live E2E test did not request completions");
    }
    cli.command.execute().await
}

async fn exercise_live_cli(config: &LiveConfig) -> Result<()> {
    let storage = session()?;
    let temp_dir = tempfile::tempdir()?;
    let seed_path = temp_dir.path().join("seed.parquet");
    TestFile::write_parquet_batch(
        &seed_path,
        &TestBatch::simple_with(&[1, 2, 3], &["one", "two", "three"]),
    );
    let input_url = config.url("input.parquet");
    let input = storage
        .prepare_output_target(
            &LocationInput::parse(&input_url)?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
    input
        .object_store()
        .put(
            input.object_path(),
            Bytes::from(std::fs::read(seed_path)?).into(),
        )
        .await
        .context("seed formatted cloud input")?;

    run_cli(vec![
        "silk-chiffon".into(),
        "detect".into(),
        input_url.clone(),
        "--format".into(),
        "json".into(),
    ])
    .await
    .context("detect cloud input through the composed CLI")?;
    run_cli(vec![
        "silk-chiffon".into(),
        "inspect".into(),
        "parquet".into(),
        input_url.clone(),
        "--format".into(),
        "json".into(),
    ])
    .await
    .context("inspect cloud input through the composed CLI")?;

    let output_url = config.url("output.parquet");
    run_cli(vec![
        "silk-chiffon".into(),
        "transform".into(),
        "--from".into(),
        input_url,
        "--to".into(),
        output_url.clone(),
        "--query".into(),
        "SELECT * FROM data WHERE id >= 2".into(),
        "--target-partitions".into(),
        "1".into(),
    ])
    .await
    .context("transform between cloud objects through the composed CLI")?;
    run_cli(vec![
        "silk-chiffon".into(),
        "detect".into(),
        output_url.clone(),
        "--format".into(),
        "json".into(),
    ])
    .await
    .context("detect transformed cloud output through the composed CLI")?;
    run_cli(vec![
        "silk-chiffon".into(),
        "inspect".into(),
        "parquet".into(),
        output_url.clone(),
        "--format".into(),
        "json".into(),
    ])
    .await
    .context("inspect transformed cloud output through the composed CLI")?;

    let output = storage
        .lookup_input(&LocationInput::parse(&output_url)?)
        .await
        .context("resolve transformed cloud output")?;
    let output_bytes = output
        .input_handle()
        .object_store()
        .get(output.input_handle().object_path())
        .await?
        .bytes()
        .await?;
    let downloaded_path = temp_dir.path().join("downloaded.parquet");
    std::fs::write(&downloaded_path, output_bytes)?;
    let row_count = TestFile::read_parquet(&downloaded_path)
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum::<usize>();
    ensure!(
        row_count == 2,
        "transformed cloud output had {row_count} rows"
    );
    Ok(())
}

async fn cleanup_prefix(config: &LiveConfig) -> Result<Vec<String>> {
    let storage = session()?;
    let root = storage
        .prepare_output_target(
            &LocationInput::parse(config.url("cleanup-root"))?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
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

async fn run_with_cleanup(config: LiveConfig) {
    let exercise = exercise_live_cli(&config).await;
    let cleanup = cleanup_prefix(&config).await;
    match (exercise, cleanup) {
        (Ok(()), Ok(leftovers)) if leftovers.is_empty() => {}
        (exercise, cleanup) => panic!(
            "live cloud CLI test failed under prefix {}: exercise={exercise:?}; cleanup={cleanup:?}",
            config.run_prefix
        ),
    }
}

#[cfg(feature = "gcs")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires explicit GCS bucket, prefix, and upstream credentials"]
async fn live_gcs_composed_cli_detects_inspects_transforms_verifies_and_cleans_up() {
    let config = LiveConfig::from_env(
        "gs",
        "SILK_CHIFFON_LIVE_GCS_BUCKET",
        "SILK_CHIFFON_LIVE_GCS_PREFIX",
    )
    .unwrap();
    run_with_cleanup(config).await;
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires explicit S3 bucket, prefix, and upstream credentials"]
async fn live_s3_composed_cli_detects_inspects_transforms_verifies_and_cleans_up() {
    let config = LiveConfig::from_env(
        "s3",
        "SILK_CHIFFON_LIVE_S3_BUCKET",
        "SILK_CHIFFON_LIVE_S3_PREFIX",
    )
    .unwrap();
    run_with_cleanup(config).await;
}
