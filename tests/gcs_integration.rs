//! Opt-in command scenarios against Google Cloud Storage.

use std::{
    collections::BTreeSet,
    future::Future,
    io::{Read, Seek},
    panic::{AssertUnwindSafe, resume_unwind},
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::array::{Int32Array, RecordBatch, StringArray};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use object_store::{ObjectStoreExt, PutPayload, path::Path};
use serde::Serialize;
use silk_chiffon::{
    StorageConfig,
    storage::{ObjectOutput, OutputPolicy, StorageContext},
    utils::test_data::{TestBatch, TestFile},
};
use tempfile::TempDir;
use uuid::Uuid;

const RESERVED_TEMP_PREFIX: &str = "__silk_chiffon_tmp__";

#[derive(Clone, Copy)]
struct PerformanceConfig {
    rows: usize,
    target_partitions: usize,
    request_limit: usize,
    upload_part_size: usize,
    upload_concurrency: usize,
    partition_count: usize,
}

impl PerformanceConfig {
    fn from_environment() -> Result<Self> {
        let config = Self {
            rows: positive_env("SILK_CHIFFON_GCS_PERF_ROWS", 250_000)?,
            target_partitions: positive_env("SILK_CHIFFON_GCS_PERF_TARGET_PARTITIONS", 8)?,
            request_limit: positive_env("SILK_CHIFFON_GCS_PERF_MAX_REQUESTS", 64)?,
            upload_part_size: positive_env(
                "SILK_CHIFFON_GCS_PERF_UPLOAD_PART_SIZE",
                10 * 1024 * 1024,
            )?,
            upload_concurrency: positive_env("SILK_CHIFFON_GCS_PERF_UPLOAD_CONCURRENCY", 8)?,
            partition_count: positive_env("SILK_CHIFFON_GCS_PERF_PARTITIONS", 64)?,
        };
        ensure!(
            config.partition_count <= config.rows,
            "SILK_CHIFFON_GCS_PERF_PARTITIONS cannot exceed SILK_CHIFFON_GCS_PERF_ROWS"
        );
        Ok(config)
    }

    fn storage_args(self) -> Vec<String> {
        args([
            "--object-store-max-requests".to_string(),
            self.request_limit.to_string(),
            "--object-store-upload-part-size".to_string(),
            self.upload_part_size.to_string(),
            "--object-store-upload-concurrency".to_string(),
            self.upload_concurrency.to_string(),
        ])
    }
}

fn positive_env(name: &str, default: usize) -> Result<usize> {
    let Some(value) = std::env::var(name).ok().filter(|value| !value.is_empty()) else {
        return Ok(default);
    };
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(parsed > 0, "{name} must be a positive integer");
    Ok(parsed)
}

fn performance_binary() -> Result<PathBuf> {
    let path = std::env::var_os("SILK_CHIFFON_GCS_PERF_BINARY").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/release/silk-chiffon"),
        PathBuf::from,
    );
    ensure!(
        path.is_file(),
        "build the release binary or set SILK_CHIFFON_GCS_PERF_BINARY before running GCS performance work"
    );
    Ok(path)
}

#[derive(Debug, Serialize)]
struct PerformanceRecord {
    profile_kind: &'static str,
    workload: &'static str,
    input_size_bytes: u64,
    output_size_bytes: Option<u64>,
    rows: usize,
    selected_columns: Vec<&'static str>,
    target_partitions: usize,
    output_partitions: Option<usize>,
    request_limit: usize,
    upload_part_size_bytes: usize,
    upload_concurrency: usize,
    elapsed_ms: u128,
    peak_rss_bytes: u64,
    transferred_bytes: Option<u64>,
    request_count: Option<u64>,
    request_concurrency: Option<u64>,
    unavailable_provider_metrics: &'static str,
}

struct PerformanceWorkload {
    name: &'static str,
    input_size_bytes: u64,
    rows: usize,
    selected_columns: Vec<&'static str>,
    target_partitions: usize,
    output_partitions: Option<usize>,
}

#[derive(Clone, Copy)]
struct PerformanceMeasurement {
    output_size_bytes: Option<u64>,
    elapsed: Duration,
    peak_rss_bytes: u64,
}

impl PerformanceRecord {
    fn new(
        workload: PerformanceWorkload,
        config: PerformanceConfig,
        measurement: PerformanceMeasurement,
    ) -> Self {
        Self {
            profile_kind: "real_gcs_external_process",
            workload: workload.name,
            input_size_bytes: workload.input_size_bytes,
            output_size_bytes: measurement.output_size_bytes,
            rows: workload.rows,
            selected_columns: workload.selected_columns,
            target_partitions: workload.target_partitions,
            output_partitions: workload.output_partitions,
            request_limit: config.request_limit,
            upload_part_size_bytes: config.upload_part_size,
            upload_concurrency: config.upload_concurrency,
            elapsed_ms: measurement.elapsed.as_millis(),
            peak_rss_bytes: measurement.peak_rss_bytes,
            transferred_bytes: None,
            request_count: None,
            request_concurrency: None,
            unavailable_provider_metrics: "the CLI process does not expose provider byte or request counters",
        }
    }

    fn emit(&self) -> Result<()> {
        println!("{}", serde_json::to_string(self)?);
        Ok(())
    }
}

async fn cleanup_both<D, T>(data_cleanup: D, temporary_cleanup: T) -> Result<()>
where
    D: Future<Output = Result<()>>,
    T: Future<Output = Result<()>>,
{
    let data_result = data_cleanup.await;
    let temporary_result = temporary_cleanup.await;
    match (data_result, temporary_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(data), Ok(())) => Err(data.context("UUID data-prefix cleanup failed")),
        (Ok(()), Err(temporary)) => {
            Err(temporary.context("reserved temporary-prefix cleanup failed"))
        }
        (Err(data), Err(temporary)) => Err(anyhow!(
            "UUID data-prefix cleanup failed: {data:#}; reserved temporary-prefix cleanup also failed: {temporary:#}"
        )),
    }
}

async fn delete_all<F, Fut>(paths: Vec<Path>, mut delete: F) -> Result<()>
where
    F: FnMut(Path) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut failures = Vec::new();
    for path in paths {
        if let Err(error) = delete(path).await {
            failures.push(format!("{error:#}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!(
            "{} object deletion(s) failed: {}",
            failures.len(),
            failures.join("; ")
        )
    }
}

async fn delete_listed<S, F, Fut>(mut listed: S, prefix: &str, delete: F) -> Result<()>
where
    S: futures::Stream<Item = Result<Path>> + Unpin,
    F: FnMut(Path) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut paths = Vec::new();
    let listing_result = loop {
        match listed.next().await {
            Some(Ok(path)) if path.as_ref().starts_with(prefix) => paths.push(path),
            Some(Ok(path)) => {
                break Err(anyhow!(
                    "refusing to delete listed object outside the UUID-owned prefix: {path}"
                ));
            }
            Some(Err(error)) => break Err(error.context("failed while listing cleanup prefix")),
            None => break Ok(()),
        }
    };
    let deletion_result = delete_all(paths, delete).await;

    match (listing_result, deletion_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(listing), Ok(())) => Err(listing),
        (Ok(()), Err(deletion)) => Err(deletion),
        (Err(listing), Err(deletion)) => Err(anyhow!(
            "{listing:#}; deletion of previously listed objects also failed: {deletion:#}"
        )),
    }
}

fn redact_bucket(bucket: &str, value: impl AsRef<str>) -> String {
    value.as_ref().replace(bucket, "[bucket]")
}

fn combine_scenario_and_cleanup(
    bucket: &str,
    scenario: Result<()>,
    cleanup: Result<()>,
) -> Result<()> {
    let result = match (scenario, cleanup) {
        (Ok(()), Ok(())) => return Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup.context("GCS test-prefix cleanup failed")),
        (Err(primary), Err(cleanup)) => Err(anyhow!(
            "{primary:#}; GCS test-prefix cleanup also failed: {cleanup:#}"
        )),
    };
    result.map_err(|error| anyhow!(redact_bucket(bucket, format!("{error:#}"))))
}

struct GcsHarness {
    bucket: String,
    prefix: String,
    storage: Arc<StorageContext>,
    local: TempDir,
}

impl GcsHarness {
    fn from_environment() -> Result<Option<Self>> {
        let Some(bucket) = std::env::var("SILK_CHIFFON_GCS_TEST_BUCKET")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!("skipped: set SILK_CHIFFON_GCS_TEST_BUCKET to run live GCS scenarios");
            return Ok(None);
        };
        let base = std::env::var("SILK_CHIFFON_GCS_TEST_PREFIX")
            .ok()
            .filter(|value| !value.trim_matches('/').is_empty())
            .unwrap_or_else(|| "silk-chiffon-tests".to_string());
        ensure!(
            base.trim_matches('/').len() <= 256,
            "SILK_CHIFFON_GCS_TEST_PREFIX must be at most 256 UTF-8 bytes"
        );
        let prefix = format!("{}/{}/", base.trim_matches('/'), Uuid::new_v4());

        Ok(Some(Self {
            bucket,
            prefix,
            storage: Arc::new(StorageContext::new(StorageConfig::default())?),
            local: TempDir::new()?,
        }))
    }

    fn uri(&self, key: &str) -> String {
        format!("gs://{}/{}{}", self.bucket, self.prefix, key)
    }

    fn temporary_prefix(&self) -> String {
        format!("{RESERVED_TEMP_PREFIX}/{}", self.prefix)
    }

    fn local_path(&self, name: &str) -> String {
        self.local.path().join(name).to_string_lossy().into_owned()
    }

    async fn put(&self, key: &str, bytes: impl Into<PutPayload>) -> Result<()> {
        let location = self.storage.resolve(&self.uri(key))?;
        location
            .store()
            .object_store()
            .put(location.path(), bytes.into())
            .await?;
        Ok(())
    }

    async fn bytes(&self, key: &str) -> Result<Bytes> {
        let location = self.storage.resolve(&self.uri(key))?;
        Ok(location
            .store()
            .object_store()
            .get(location.path())
            .await?
            .bytes()
            .await?)
    }

    async fn object_size(&self, key: &str) -> Result<u64> {
        let location = self.storage.resolve(&self.uri(key))?;
        Ok(location
            .store()
            .object_store()
            .head(location.path())
            .await?
            .size)
    }

    async fn list(&self, prefix: &str) -> Result<BTreeSet<String>> {
        let location = self.storage.resolve(&self.uri("list-anchor"))?;
        let mut objects = location
            .store()
            .object_store()
            .list(Some(&Path::parse(prefix)?));
        let mut paths = BTreeSet::new();
        while let Some(object) = objects.next().await {
            paths.insert(object?.location.to_string());
        }
        Ok(paths)
    }

    async fn prefix_stats(&self, prefix: &str) -> Result<(usize, u64)> {
        let location = self.storage.resolve(&self.uri("stats-anchor"))?;
        let mut objects = location
            .store()
            .object_store()
            .list(Some(&Path::parse(prefix)?));
        let mut count = 0;
        let mut bytes = 0;
        while let Some(object) = objects.next().await {
            let object = object?;
            ensure!(
                object.location.as_ref().starts_with(prefix),
                "object listing escaped the requested performance prefix"
            );
            count += 1;
            bytes += object.size;
        }
        Ok((count, bytes))
    }

    async fn reserved_temporary_objects(&self) -> Result<BTreeSet<String>> {
        self.list(&self.temporary_prefix()).await
    }

    async fn cleanup(&self) -> Result<()> {
        cleanup_both(
            self.delete_prefix(&self.prefix),
            self.delete_prefix(&self.temporary_prefix()),
        )
        .await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        ensure!(self.prefix.ends_with('/'));
        ensure!(
            prefix == self.prefix || prefix == self.temporary_prefix(),
            "refusing to clean outside the UUID-owned test prefix"
        );
        let location = self.storage.resolve(&self.uri("cleanup-anchor"))?;
        let store = Arc::clone(location.store().object_store());
        let objects = store
            .list(Some(&Path::parse(prefix)?))
            .map(|object| object.map(|object| object.location).map_err(Into::into));
        delete_listed(objects, prefix, |path| {
            let store = Arc::clone(&store);
            async move {
                store
                    .delete(&path)
                    .await
                    .with_context(|| format!("failed to delete {path}"))
            }
        })
        .await
    }

    fn redact(&self, value: impl AsRef<str>) -> String {
        redact_bucket(&self.bucket, value)
    }

    fn command(&self, args: &[String]) -> Result<Output> {
        Command::new(assert_cmd::cargo::cargo_bin!("silk-chiffon"))
            .args(args)
            .output()
            .context("failed to run silk-chiffon")
    }

    fn command_ok(&self, args: &[String]) -> Result<Output> {
        let output = self.command(args)?;
        if !output.status.success() {
            bail!(
                "silk-chiffon exited with {}: {}",
                output.status,
                self.redact(String::from_utf8_lossy(&output.stderr))
            );
        }
        Ok(output)
    }

    fn command_fails(&self, args: &[String]) -> Result<String> {
        let output = self.command(args)?;
        ensure!(
            !output.status.success(),
            "silk-chiffon unexpectedly succeeded"
        );
        Ok(self.redact(String::from_utf8_lossy(&output.stderr)))
    }

    fn profile_command(
        &self,
        config: PerformanceConfig,
        command_args: &[String],
    ) -> Result<(Duration, u64)> {
        let mut args = config.storage_args();
        args.extend_from_slice(command_args);
        let started = Instant::now();
        let mut stderr_file = tempfile::tempfile()?;
        let mut child = Command::new(performance_binary()?)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_file.try_clone()?))
            .spawn()
            .context("failed to run silk-chiffon performance workload")?;
        let pid = sysinfo::Pid::from_u32(child.id());
        let mut system = sysinfo::System::new();
        let mut peak_rss_bytes = 0;
        let status = loop {
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
            if let Some(process) = system.process(pid) {
                peak_rss_bytes = peak_rss_bytes.max(process.memory());
            }
            if let Some(status) = child.try_wait()? {
                break status;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let mut stderr = String::new();
        stderr_file.rewind()?;
        stderr_file.read_to_string(&mut stderr)?;
        if !status.success() {
            bail!("silk-chiffon exited with {status}: {}", self.redact(stderr));
        }
        ensure!(
            peak_rss_bytes > 0,
            "could not sample release-process RSS for performance workload"
        );
        Ok((started.elapsed(), peak_rss_bytes))
    }
}

fn args(values: impl IntoIterator<Item = impl Into<String>>) -> Vec<String> {
    values.into_iter().map(Into::into).collect()
}

fn write_local_fixtures(harness: &GcsHarness) -> Result<(String, String, String)> {
    let arrow = harness.local_path("input.arrow");
    TestFile::write_arrow_batch(
        std::path::Path::new(&arrow),
        &TestBatch::builder()
            .column_i32("id", &[1, 2, 3, 4])
            .column_string("name", &["a", "b", "a", "c"])
            .build(),
    );

    let nested = harness.local_path("nested.parquet");
    let nested_batch = TestBatch::builder()
        .column_i32("id", &[1, 2, 3])
        .column_struct("person", |column| {
            column
                .field_string("name", &["alice", "bob", "cara"])
                .field_i32("age", &[31, 29, 37])
        })
        .column_struct("company", |column| {
            column.field_string("name", &["acme", "beta", "acme"])
        })
        .build();
    TestFile::write_parquet_batch(std::path::Path::new(&nested), &nested_batch);

    let large = harness.local_path("large.arrow");
    let strings = (0..45_000)
        .map(|index| format!("{index:08x}-{}", "x".repeat(184)))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from_iter_values(0..45_000)) as _),
        ("payload", Arc::new(StringArray::from(strings)) as _),
    ])?;
    TestFile::write_arrow_batch(std::path::Path::new(&large), &batch);
    ensure!(std::fs::metadata(&large)?.len() > 5 * 1024 * 1024);

    Ok((arrow, nested, large))
}

fn write_performance_fixture(harness: &GcsHarness, config: PerformanceConfig) -> Result<String> {
    ensure!(
        config.rows <= usize::try_from(i32::MAX)?,
        "SILK_CHIFFON_GCS_PERF_ROWS exceeds the i32 fixture limit"
    );
    ensure!(
        config.partition_count <= usize::try_from(i32::MAX)?,
        "SILK_CHIFFON_GCS_PERF_PARTITIONS exceeds the i32 fixture limit"
    );
    let path = harness.local_path("performance.arrow");
    let rows = config.rows;
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(
                (0..rows).map(|value| i32::try_from(value).unwrap()),
            )) as _,
        ),
        (
            "category",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|index| format!("category-{}", index % 97)),
            )) as _,
        ),
        (
            "partition",
            Arc::new(Int32Array::from_iter_values((0..rows).map(|index| {
                i32::try_from(index % config.partition_count).unwrap()
            }))) as _,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(deterministic_payload),
            )) as _,
        ),
    ])?;
    TestFile::write_arrow_batch(std::path::Path::new(&path), &batch);
    ensure!(
        std::fs::metadata(&path)?.len() > u64::try_from(config.upload_part_size)?,
        "performance fixture must exceed one configured upload part"
    );
    Ok(path)
}

fn deterministic_payload(index: usize) -> String {
    let mut state = u64::try_from(index).unwrap() ^ 0xa076_1d64_78bd_642f;
    (0..128)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + u8::try_from(state % 26).unwrap())
        })
        .collect()
}

async fn profile_workload(
    harness: &GcsHarness,
    config: PerformanceConfig,
    workload: PerformanceWorkload,
    command: Vec<String>,
) -> Result<()> {
    let (elapsed, peak_rss_bytes) = harness.profile_command(config, &command)?;
    PerformanceRecord::new(
        workload,
        config,
        PerformanceMeasurement {
            output_size_bytes: None,
            elapsed,
            peak_rss_bytes,
        },
    )
    .emit()
}

async fn profile_remote_upload(
    harness: &GcsHarness,
    config: PerformanceConfig,
    workload: &'static str,
    input_size_bytes: u64,
    output_key: &str,
    command: Vec<String>,
) -> Result<()> {
    let (elapsed, peak_rss_bytes) = harness.profile_command(config, &command)?;
    let output_size_bytes = harness.object_size(output_key).await?;
    ensure!(
        output_size_bytes > u64::try_from(config.upload_part_size)?,
        "{workload} output did not exceed one configured upload part"
    );
    PerformanceRecord::new(
        PerformanceWorkload {
            name: workload,
            input_size_bytes,
            rows: config.rows,
            selected_columns: vec!["id", "category", "partition", "payload"],
            target_partitions: config.target_partitions,
            output_partitions: Some(1),
        },
        config,
        PerformanceMeasurement {
            output_size_bytes: Some(output_size_bytes),
            elapsed,
            peak_rss_bytes,
        },
    )
    .emit()
}

async fn scenario_exact_glob_and_transfer_directions(
    harness: &GcsHarness,
    arrow: &str,
) -> Result<()> {
    let remote_arrow = harness.uri("directions/exact.arrow");
    harness.command_ok(&args(["transform", "--from", arrow, "--to", &remote_arrow]))?;

    let local_parquet = harness.local_path("download.parquet");
    harness.command_ok(&args([
        "transform",
        "--from",
        &remote_arrow,
        "--to",
        &local_parquet,
    ]))?;
    ensure!(std::fs::metadata(local_parquet)?.len() > 0);

    let arrow_bytes = std::fs::read(arrow)?;
    harness
        .put("glob/part-a.arrow", PutPayload::from(arrow_bytes.clone()))
        .await?;
    harness
        .put("glob/part-b.arrow", PutPayload::from(arrow_bytes))
        .await?;
    let glob_output = harness.local_path("glob.parquet");
    harness.command_ok(&args([
        "transform",
        "--from-many",
        &harness.uri("glob/*.arrow"),
        "--to",
        &glob_output,
    ]))?;
    ensure!(std::fs::metadata(glob_output)?.len() > 0);

    harness.command_ok(&args([
        "transform",
        "--from",
        &remote_arrow,
        "--to",
        &harness.uri("directions/copied.vortex"),
    ]))?;
    Ok(())
}

async fn scenario_formats_and_multipart(
    harness: &GcsHarness,
    arrow: &str,
    large: &str,
) -> Result<()> {
    for (key, options) in [
        ("formats/file.arrow", vec![]),
        ("formats/stream.arrow", vec!["--arrow-format", "stream"]),
        ("formats/data.parquet", vec![]),
        ("formats/data.vortex", vec![]),
    ] {
        let mut command = args(["transform", "--from", arrow, "--to", &harness.uri(key)]);
        command.extend(options.into_iter().map(str::to_string));
        harness.command_ok(&command)?;

        let downloaded = harness.local_path(&format!("download-{}", key.replace('/', "-")));
        harness.command_ok(&args([
            "transform",
            "--from",
            &harness.uri(key),
            "--to",
            &downloaded,
        ]))?;
        ensure!(std::fs::metadata(downloaded)?.len() > 0);
    }

    harness.command_ok(&args([
        "--object-store-upload-part-size",
        "5MiB",
        "--object-store-upload-concurrency",
        "2",
        "transform",
        "--from",
        large,
        "--to",
        &harness.uri("multipart/large.arrow"),
    ]))?;
    ensure!(harness.bytes("multipart/large.arrow").await?.len() > 5 * 1024 * 1024);
    Ok(())
}

async fn scenario_partitions_manifest_and_output_policy(
    harness: &GcsHarness,
    arrow: &str,
) -> Result<()> {
    let template = harness.uri("partitioned/{{name}}/data.parquet");
    let manifest = harness.uri("partitioned/outputs.json");
    harness.command_ok(&args([
        "transform",
        "--from",
        arrow,
        "--to-many",
        &template,
        "--by",
        "name",
        "--list-outputs",
        "json",
        "--list-outputs-file",
        &manifest,
    ]))?;
    let listed: Vec<String> =
        serde_json::from_slice(&harness.bytes("partitioned/outputs.json").await?)?;
    ensure!(listed.len() == 3, "expected three partition outputs");
    ensure!(listed.iter().all(|value| value.starts_with("gs://")));

    let destination = harness.uri("policy/data.parquet");
    harness.command_ok(&args(["transform", "--from", arrow, "--to", &destination]))?;
    let create_error =
        harness.command_fails(&args(["transform", "--from", arrow, "--to", &destination]))?;
    ensure!(create_error.contains("already exists"));
    harness.command_ok(&args([
        "transform",
        "--from",
        arrow,
        "--to",
        &destination,
        "--overwrite",
    ]))?;
    Ok(())
}

async fn scenario_inspect(harness: &GcsHarness, nested: &str) -> Result<()> {
    harness
        .put(
            "inspect/nested.parquet",
            PutPayload::from(std::fs::read(nested)?),
        )
        .await?;

    for key in [
        "formats/file.arrow",
        "formats/stream.arrow",
        "formats/data.parquet",
        "formats/data.vortex",
    ] {
        harness.command_ok(&args([
            "inspect",
            "identify",
            &harness.uri(key),
            "--format",
            "json",
        ]))?;
    }

    for command in [
        args([
            "inspect",
            "arrow",
            &harness.uri("formats/file.arrow"),
            "--format",
            "text",
        ]),
        args([
            "inspect",
            "arrow",
            &harness.uri("formats/file.arrow"),
            "--format",
            "json",
            "--batches",
            "--row-count",
        ]),
        args([
            "inspect",
            "arrow",
            &harness.uri("formats/stream.arrow"),
            "--format",
            "text",
        ]),
        args([
            "inspect",
            "arrow",
            &harness.uri("formats/stream.arrow"),
            "--format",
            "json",
            "--batches",
            "--row-count",
        ]),
        args([
            "inspect",
            "parquet",
            &harness.uri("inspect/nested.parquet"),
            "--format",
            "text",
            "--row-group",
            "0",
        ]),
        args([
            "inspect",
            "parquet",
            &harness.uri("inspect/nested.parquet"),
            "--format",
            "json",
        ]),
        args([
            "inspect",
            "parquet",
            &harness.uri("inspect/nested.parquet"),
            "--format",
            "text",
            "--pages",
        ]),
        args([
            "inspect",
            "parquet",
            &harness.uri("inspect/nested.parquet"),
            "--format",
            "text",
            "--pages=age",
        ]),
        args([
            "inspect",
            "parquet",
            &harness.uri("inspect/nested.parquet"),
            "--format",
            "text",
            "--pages=person.name",
        ]),
        args([
            "inspect",
            "vortex",
            &harness.uri("formats/data.vortex"),
            "--format",
            "text",
        ]),
        args([
            "inspect",
            "vortex",
            &harness.uri("formats/data.vortex"),
            "--format",
            "json",
        ]),
        args([
            "inspect",
            "vortex",
            &harness.uri("formats/data.vortex"),
            "--format",
            "text",
            "--schema",
            "--stats",
            "--layout",
        ]),
    ] {
        harness.command_ok(&command)?;
    }

    let ambiguous = harness.command_fails(&args([
        "inspect",
        "parquet",
        &harness.uri("inspect/nested.parquet"),
        "--format",
        "text",
        "--pages=name",
    ]))?;
    ensure!(ambiguous.contains("ambiguous"));
    ensure!(ambiguous.contains("person.name"));
    ensure!(ambiguous.contains("company.name"));

    let missing = harness.command_fails(&args([
        "inspect",
        "identify",
        &harness.uri("inspect/missing.parquet"),
    ]))?;
    ensure!(missing.contains("gs://"));
    for secret_marker in ["private_key", "access_token", "X-Goog-Signature", "ya29."] {
        ensure!(!missing.contains(secret_marker));
    }
    Ok(())
}

async fn scenario_injected_failure_cleanup(harness: &GcsHarness) -> Result<()> {
    let destination = harness.uri("failure/aborted.bin");
    let mut output = ObjectOutput::open(
        harness.storage.resolve(&destination)?,
        StorageConfig {
            upload_part_size: 5 * 1024 * 1024,
            upload_concurrency: 2,
            ..StorageConfig::default()
        },
        OutputPolicy::new(false, false),
    )
    .await?;
    output
        .write(Bytes::from(vec![0x5a; 6 * 1024 * 1024]))
        .await?;
    harness
        .put("failure/aborted.bin", PutPayload::from_static(b"racer"))
        .await?;
    ensure!(output.commit().await.is_err());
    ensure!(harness.bytes("failure/aborted.bin").await? == "racer");
    ensure!(harness.reserved_temporary_objects().await?.is_empty());
    Ok(())
}

async fn run_performance_scenarios(harness: &GcsHarness, config: PerformanceConfig) -> Result<()> {
    let local_arrow = write_performance_fixture(harness, config)?;
    let local_size = std::fs::metadata(&local_arrow)?.len();
    let target_partitions = config.target_partitions.to_string();
    let uploads = [
        (
            "upload_arrow_file",
            "performance/input.file.arrow",
            Vec::new(),
        ),
        (
            "upload_arrow_stream",
            "performance/input.stream.arrow",
            vec!["--arrow-format", "stream"],
        ),
        ("upload_parquet", "performance/input.parquet", Vec::new()),
        ("upload_vortex", "performance/input.vortex", Vec::new()),
    ];
    for (workload, key, options) in uploads {
        let mut command = args([
            "transform",
            "--from",
            &local_arrow,
            "--to",
            &harness.uri(key),
            "--target-partitions",
            &target_partitions,
        ]);
        command.extend(options.into_iter().map(str::to_string));
        profile_remote_upload(harness, config, workload, local_size, key, command).await?;
    }

    let parquet_size = harness.object_size("performance/input.parquet").await?;
    profile_workload(
        harness,
        config,
        PerformanceWorkload {
            name: "parquet_projection_filter",
            input_size_bytes: parquet_size,
            rows: config.rows,
            selected_columns: vec!["id", "payload"],
            target_partitions: config.target_partitions,
            output_partitions: Some(1),
        },
        args([
            "transform",
            "--from",
            &harness.uri("performance/input.parquet"),
            "--to",
            &harness.local_path("read-parquet.arrow"),
            "--query",
            "SELECT id, payload FROM data WHERE id % 2 = 0",
            "--target-partitions",
            &target_partitions,
        ]),
    )
    .await?;

    for (workload, key, output, columns) in [
        (
            "arrow_file_read",
            "performance/input.file.arrow",
            "read-arrow-file.parquet",
            vec!["id", "category"],
        ),
        (
            "arrow_stream_read",
            "performance/input.stream.arrow",
            "read-arrow-stream.parquet",
            vec!["id", "category"],
        ),
        (
            "vortex_projection",
            "performance/input.vortex",
            "read-vortex.parquet",
            vec!["id", "payload"],
        ),
    ] {
        profile_workload(
            harness,
            config,
            PerformanceWorkload {
                name: workload,
                input_size_bytes: harness.object_size(key).await?,
                rows: config.rows,
                selected_columns: columns.clone(),
                target_partitions: config.target_partitions,
                output_partitions: Some(1),
            },
            args([
                "transform",
                "--from",
                &harness.uri(key),
                "--to",
                &harness.local_path(output),
                "--query",
                &format!("SELECT {} FROM data", columns.join(", ")),
                "--target-partitions",
                &target_partitions,
            ]),
        )
        .await?;
    }

    let arrow_bytes = std::fs::read(&local_arrow)?;
    let mut parallel_size = 0;
    let mut parallel_command = args(["transform"]);
    for index in 0..4 {
        let key = format!("performance/parallel/input-{index}.arrow");
        harness
            .put(&key, PutPayload::from(arrow_bytes.clone()))
            .await?;
        parallel_size += harness.object_size(&key).await?;
        parallel_command.extend(args(["--from-many", &harness.uri(&key)]));
    }
    parallel_command.extend(args([
        "--to",
        &harness.local_path("read-parallel.parquet"),
        "--target-partitions",
        &target_partitions,
    ]));
    profile_workload(
        harness,
        config,
        PerformanceWorkload {
            name: "parallel_input_files",
            input_size_bytes: parallel_size,
            rows: config
                .rows
                .checked_mul(4)
                .context("parallel-input row count overflowed usize")?,
            selected_columns: vec!["id", "category", "partition", "payload"],
            target_partitions: config.target_partitions,
            output_partitions: Some(1),
        },
        parallel_command,
    )
    .await?;

    let partition_command = args([
        "transform",
        "--from",
        &local_arrow,
        "--to-many",
        &harness.uri("performance/partitioned/{{partition}}/data.parquet"),
        "--by",
        "partition",
        "--partition-strategy",
        "nosort-multi",
        "--target-partitions",
        &target_partitions,
    ]);
    let (elapsed, peak_rss_bytes) = harness.profile_command(config, &partition_command)?;
    let partition_prefix = format!("{}performance/partitioned/", harness.prefix);
    let (partition_count, partition_bytes) = harness.prefix_stats(&partition_prefix).await?;
    ensure!(
        partition_count == config.partition_count,
        "partition workload wrote {partition_count} outputs instead of {}",
        config.partition_count
    );
    PerformanceRecord::new(
        PerformanceWorkload {
            name: "partitioned_many_open_sinks",
            input_size_bytes: local_size,
            rows: config.rows,
            selected_columns: vec!["id", "category", "partition", "payload"],
            target_partitions: config.target_partitions,
            output_partitions: Some(partition_count),
        },
        config,
        PerformanceMeasurement {
            output_size_bytes: Some(partition_bytes),
            elapsed,
            peak_rss_bytes,
        },
    )
    .emit()?;

    for (workload, options, columns) in [
        (
            "parquet_inspect_footer_text",
            vec!["--format", "text"],
            Vec::new(),
        ),
        (
            "parquet_inspect_default_json",
            vec!["--format", "json"],
            vec!["id", "category", "partition", "payload"],
        ),
        (
            "parquet_inspect_all_pages",
            vec!["--format", "text", "--pages"],
            vec!["id", "category", "partition", "payload"],
        ),
        (
            "parquet_inspect_selected_pages",
            vec!["--format", "text", "--pages=payload"],
            vec!["payload"],
        ),
    ] {
        let mut command = args([
            "inspect",
            "parquet",
            &harness.uri("performance/input.parquet"),
        ]);
        command.extend(options.into_iter().map(str::to_string));
        profile_workload(
            harness,
            config,
            PerformanceWorkload {
                name: workload,
                input_size_bytes: parquet_size,
                rows: config.rows,
                selected_columns: columns,
                target_partitions: 1,
                output_partitions: None,
            },
            command,
        )
        .await?;
    }

    ensure!(
        harness.reserved_temporary_objects().await?.is_empty(),
        "performance scenarios left a reserved temporary object"
    );
    Ok(())
}

async fn run_scenarios(harness: &GcsHarness) -> Result<()> {
    ensure!(harness.reserved_temporary_objects().await?.is_empty());
    let (arrow, nested, large) = write_local_fixtures(harness)?;
    scenario_exact_glob_and_transfer_directions(harness, &arrow).await?;
    scenario_formats_and_multipart(harness, &arrow, &large).await?;
    scenario_partitions_manifest_and_output_policy(harness, &arrow).await?;
    scenario_inspect(harness, &nested).await?;
    scenario_injected_failure_cleanup(harness).await?;
    ensure!(
        harness.reserved_temporary_objects().await?.is_empty(),
        "live scenarios left a reserved temporary object"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires SILK_CHIFFON_GCS_TEST_BUCKET and Google ADC"]
async fn gcs_command_scenarios() -> Result<()> {
    let Some(harness) = GcsHarness::from_environment()? else {
        return Ok(());
    };

    let result = AssertUnwindSafe(run_scenarios(&harness))
        .catch_unwind()
        .await;
    let cleanup = harness.cleanup().await;
    match result {
        Ok(scenario) => combine_scenario_and_cleanup(&harness.bucket, scenario, cleanup),
        Err(panic) => {
            if let Err(cleanup) = cleanup {
                eprintln!(
                    "GCS test-prefix cleanup failed after panic: {}",
                    harness.redact(format!("{cleanup:#}"))
                );
            }
            resume_unwind(panic)
        }
    }
}

#[tokio::test]
#[ignore = "requires explicit GCS performance authorization and Google ADC"]
async fn gcs_object_store_performance() -> Result<()> {
    if std::env::var("SILK_CHIFFON_GCS_PERF").as_deref() != Ok("1") {
        eprintln!("skipped: set SILK_CHIFFON_GCS_PERF=1 to run chargeable GCS performance work");
        return Ok(());
    }
    let config = PerformanceConfig::from_environment()?;
    let Some(harness) = GcsHarness::from_environment()? else {
        return Ok(());
    };

    let result = AssertUnwindSafe(run_performance_scenarios(&harness, config))
        .catch_unwind()
        .await;
    let cleanup = harness.cleanup().await;
    match result {
        Ok(scenario) => combine_scenario_and_cleanup(&harness.bucket, scenario, cleanup),
        Err(panic) => {
            if let Err(cleanup) = cleanup {
                eprintln!(
                    "GCS performance-prefix cleanup failed after panic: {}",
                    harness.redact(format!("{cleanup:#}"))
                );
            }
            resume_unwind(panic)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn cleanup_attempts_both_prefixes_and_aggregates_errors() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let data_attempts = Arc::clone(&attempts);
        let temporary_attempts = Arc::clone(&attempts);

        let result = cleanup_both(
            async move {
                data_attempts.lock().unwrap().push("data");
                bail!("data cleanup failed")
            },
            async move {
                temporary_attempts.lock().unwrap().push("temporary");
                bail!("temporary cleanup failed")
            },
        )
        .await;

        assert_eq!(*attempts.lock().unwrap(), ["data", "temporary"]);
        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("data cleanup failed"));
        assert!(error.contains("temporary cleanup failed"));
    }

    #[tokio::test]
    async fn delete_all_continues_after_individual_failures() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let paths = ["owned/a", "owned/b", "owned/c"]
            .into_iter()
            .map(Path::from)
            .collect();

        let result = delete_all(paths, {
            let attempts = Arc::clone(&attempts);
            move |path| {
                attempts.lock().unwrap().push(path.to_string());
                async move {
                    if path.as_ref() == "owned/b" {
                        bail!("injected delete failure for {path}")
                    }
                    Ok(())
                }
            }
        })
        .await;

        assert_eq!(*attempts.lock().unwrap(), ["owned/a", "owned/b", "owned/c"]);
        assert!(
            format!("{:#}", result.unwrap_err()).contains("injected delete failure for owned/b")
        );
    }

    #[tokio::test]
    async fn listing_error_preserves_discovered_paths_and_delete_failures() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let listed = futures::stream::iter(vec![
            Ok(Path::from("owned/a")),
            Err(anyhow!("injected listing failure")),
            Ok(Path::from("owned/not-yielded")),
        ]);

        let result = delete_listed(listed, "owned/", {
            let attempts = Arc::clone(&attempts);
            move |path| {
                attempts.lock().unwrap().push(path.to_string());
                async move { bail!("injected delete failure for {path}") }
            }
        })
        .await;

        assert_eq!(*attempts.lock().unwrap(), ["owned/a"]);
        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("injected listing failure"));
        assert!(error.contains("injected delete failure for owned/a"));
    }

    #[test]
    fn combined_scenario_and_cleanup_errors_are_redacted() {
        let error = combine_scenario_and_cleanup(
            "private-bucket",
            Err(anyhow!("primary failure for gs://private-bucket/data")),
            Err(anyhow!("cleanup failure for private-bucket")),
        )
        .unwrap_err();
        let message = format!("{error:#}");

        assert!(!message.contains("private-bucket"));
        assert!(message.contains("primary failure"));
        assert!(message.contains("cleanup failure"));
    }
}
