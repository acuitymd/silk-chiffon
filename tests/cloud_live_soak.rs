#![cfg(all(feature = "bigquery", feature = "gcs", feature = "s3"))]

use std::{
    collections::HashSet,
    fs::{self, File},
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use arrow::{
    array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray},
    datatypes::{DataType, Field, Schema},
    ipc::reader::StreamReader as ArrowStreamReader,
};
use clap::Command as ClapCommand;
use datafusion::{datasource::MemTable, prelude::SessionContext};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use rand::{RngExt, SeedableRng, rngs::SmallRng};
use silk_chiffon::{Cli, Command};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, OutputPreparation, StorageRegistry, StorageSession,
};
use silk_chiffon_test_support::TestFile;

const FORMATS: [SoakFormat; 4] = [
    SoakFormat::ArrowFile,
    SoakFormat::ArrowStream,
    SoakFormat::Parquet,
    SoakFormat::Vortex,
];
const LAYOUTS: [OutputLayout; 4] = [
    OutputLayout::Direct,
    OutputLayout::SortSingle,
    OutputLayout::NosortMulti,
    OutputLayout::NosortEvict,
];
const TARGETS: [OutputTarget; 3] = [OutputTarget::Local, OutputTarget::Gcs, OutputTarget::S3];
const PARTITIONED_LAYOUTS: [OutputLayout; 3] = [
    OutputLayout::SortSingle,
    OutputLayout::NosortMulti,
    OutputLayout::NosortEvict,
];
const CLOUD_TARGETS: [OutputTarget; 2] = [OutputTarget::Gcs, OutputTarget::S3];
const PARTITION_COUNTS: [usize; 4] = [1, 2, 4, 8];
const PARTITION_MODULI: [i64; 3] = [3, 7, 17];
const LOCAL_ROW_COUNTS: [usize; 3] = [1, 17, 257];
const MAX_EVICT_BQS_ROWS: usize = 1_000;
const EVICT_LOCAL_ROWS: usize = 17;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SoakFormat {
    ArrowFile,
    ArrowStream,
    Parquet,
    Vortex,
}

impl SoakFormat {
    fn name(self) -> &'static str {
        match self {
            Self::ArrowFile | Self::ArrowStream => "arrow",
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::ArrowFile | Self::ArrowStream => "arrow",
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
        }
    }

    fn append_output_args(self, arguments: &mut Vec<String>) {
        arguments.extend(["--output-format".into(), self.name().into()]);
        if matches!(self, Self::ArrowFile | Self::ArrowStream) {
            arguments.extend([
                "--arrow-format".into(),
                if self == Self::ArrowFile {
                    "file".into()
                } else {
                    "stream".into()
                },
            ]);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum OutputLayout {
    Direct,
    SortSingle,
    NosortMulti,
    NosortEvict,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum OutputTarget {
    Local,
    Gcs,
    S3,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BqsPredicate {
    NonNull,
    Prefix,
    Middle,
}

impl BqsPredicate {
    fn selected_range(self, total_rows: usize) -> (usize, usize) {
        match self {
            Self::NonNull => (1, total_rows),
            Self::Prefix => (1, (total_rows / 2).max(1)),
            Self::Middle => {
                let lower = (total_rows / 5).saturating_add(1).min(total_rows);
                let upper = (total_rows.saturating_mul(4) / 5).max(lower);
                (lower, upper)
            }
        }
    }

    fn expression(self, total_rows: usize) -> String {
        let (lower, upper) = self.selected_range(total_rows);
        match self {
            Self::NonNull => "id IS NOT NULL".to_owned(),
            Self::Prefix => format!("id < 0 OR id <= {upper}"),
            Self::Middle => format!("id < 0 OR id BETWEEN {lower} AND {upper}"),
        }
    }
}

const BQS_PREDICATES: [BqsPredicate; 3] = [
    BqsPredicate::NonNull,
    BqsPredicate::Prefix,
    BqsPredicate::Middle,
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct Scenario {
    local_format: SoakFormat,
    output_format: SoakFormat,
    output_layout: OutputLayout,
    output_target: OutputTarget,
    target_partitions: usize,
    bqs_stream_count: usize,
    partition_modulus: i64,
    local_rows: usize,
    bqs_predicate: BqsPredicate,
    project_name_first: bool,
    sort_direct_output: bool,
}

impl Scenario {
    fn for_case(seed: u64, index: u64, max_stream_count: usize) -> Self {
        let mandatory_cases = CLOUD_TARGETS.len() * FORMATS.len() * PARTITIONED_LAYOUTS.len();
        if let Ok(index) = usize::try_from(index)
            && index < mandatory_cases
        {
            let output_layout = PARTITIONED_LAYOUTS[index / (CLOUD_TARGETS.len() * FORMATS.len())];
            return Self {
                local_format: FORMATS[(index + 1) % FORMATS.len()],
                output_format: FORMATS[(index / CLOUD_TARGETS.len()) % FORMATS.len()],
                output_layout,
                output_target: CLOUD_TARGETS[index % CLOUD_TARGETS.len()],
                target_partitions: PARTITION_COUNTS[index % PARTITION_COUNTS.len()],
                bqs_stream_count: 1 + index % max_stream_count,
                partition_modulus: PARTITION_MODULI[index % PARTITION_MODULI.len()],
                local_rows: if output_layout == OutputLayout::NosortEvict {
                    EVICT_LOCAL_ROWS
                } else {
                    LOCAL_ROW_COUNTS[index % LOCAL_ROW_COUNTS.len()]
                },
                bqs_predicate: BQS_PREDICATES[index % BQS_PREDICATES.len()],
                project_name_first: index % 2 == 1,
                sort_direct_output: false,
            };
        }
        Self::generate(seed, index, max_stream_count)
    }

    fn generate(seed: u64, index: u64, max_stream_count: usize) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let output_layout = LAYOUTS[rng.random_range(0..LAYOUTS.len())];
        Self {
            local_format: FORMATS[rng.random_range(0..FORMATS.len())],
            output_format: FORMATS[rng.random_range(0..FORMATS.len())],
            output_layout,
            output_target: TARGETS[rng.random_range(0..TARGETS.len())],
            target_partitions: PARTITION_COUNTS[rng.random_range(0..PARTITION_COUNTS.len())],
            bqs_stream_count: rng.random_range(1..=max_stream_count),
            partition_modulus: PARTITION_MODULI[rng.random_range(0..PARTITION_MODULI.len())],
            local_rows: if output_layout == OutputLayout::NosortEvict {
                EVICT_LOCAL_ROWS
            } else {
                LOCAL_ROW_COUNTS[rng.random_range(0..LOCAL_ROW_COUNTS.len())]
            },
            bqs_predicate: BQS_PREDICATES[rng.random_range(0..BQS_PREDICATES.len())],
            project_name_first: rng.random_bool(0.5),
            sort_direct_output: rng.random_bool(0.5),
        }
    }

    fn selected_bqs_range(&self, total_rows: usize) -> (usize, usize) {
        let (lower, upper) = self.bqs_predicate.selected_range(total_rows);
        if self.output_layout == OutputLayout::NosortEvict {
            (
                lower,
                upper.min(lower.saturating_add(MAX_EVICT_BQS_ROWS - 1)),
            )
        } else {
            (lower, upper)
        }
    }

    fn predicate_expression(&self, total_rows: usize) -> String {
        if self.output_layout == OutputLayout::NosortEvict {
            let (lower, upper) = self.selected_bqs_range(total_rows);
            format!("id < 0 OR id BETWEEN {lower} AND {upper}")
        } else {
            self.bqs_predicate.expression(total_rows)
        }
    }

    fn explicit_bqs_restriction(&self, total_rows: usize) -> Option<String> {
        (self.output_layout == OutputLayout::NosortEvict).then(|| {
            let (lower, upper) = self.selected_bqs_range(total_rows);
            format!("`id` BETWEEN {lower} AND {upper}")
        })
    }
}

#[derive(Debug)]
struct CloudConfig {
    scheme: &'static str,
    bucket: String,
    run_prefix: String,
}

impl CloudConfig {
    fn from_env(
        scheme: &'static str,
        bucket_variable: &'static str,
        prefix_variable: &'static str,
        nonce: u128,
    ) -> Result<Self> {
        let bucket = required(bucket_variable)?;
        let prefix = required(prefix_variable)?;
        validate_bucket(scheme, &bucket, bucket_variable)?;
        validate_prefix(&prefix, prefix_variable)?;
        Ok(Self {
            scheme,
            bucket,
            run_prefix: format!("{prefix}/soak-{}-{nonce}", std::process::id()),
        })
    }

    fn url(&self, suffix: &str) -> String {
        format!(
            "{}://{}/{}/{}",
            self.scheme, self.bucket, self.run_prefix, suffix
        )
    }
}

#[derive(Debug)]
struct BigQueryConfig {
    reference: String,
    session_project: String,
    quota_project: Option<String>,
    expected_rows: usize,
    max_stream_count: usize,
}

impl BigQueryConfig {
    fn from_env() -> Result<Self> {
        ensure!(
            required("SILK_CHIFFON_BQS_LIVE_ACKNOWLEDGE_COST")? == "1",
            "SILK_CHIFFON_BQS_LIVE_ACKNOWLEDGE_COST must equal 1"
        );
        let session_project = required("SILK_CHIFFON_BQS_LIVE_SESSION_PROJECT")?;
        let table_project = required("SILK_CHIFFON_BQS_LIVE_TABLE_PROJECT")?;
        let dataset = required("SILK_CHIFFON_BQS_LIVE_DATASET")?;
        let table = required("SILK_CHIFFON_BQS_LIVE_TABLE")?;
        let location = required("SILK_CHIFFON_BQS_LIVE_EXPECTED_LOCATION")?;
        let expected_rows = parse_positive::<usize>(
            "SILK_CHIFFON_BQS_LIVE_EXPECTED_ROWS",
            &required("SILK_CHIFFON_BQS_LIVE_EXPECTED_ROWS")?,
        )?;
        let max_stream_count = std::env::var("SILK_CHIFFON_BQS_LIVE_MAX_STREAM_COUNT")
            .map_or_else(
                |_| Ok(8),
                |value| parse_positive("SILK_CHIFFON_BQS_LIVE_MAX_STREAM_COUNT", &value),
            )?;
        Ok(Self {
            reference: format!(
                "bqs:///projects/{table_project}/datasets/{dataset}/tables/{table}?location={location}"
            ),
            session_project,
            quota_project: std::env::var("SILK_CHIFFON_BQS_LIVE_QUOTA_PROJECT").ok(),
            expected_rows,
            max_stream_count,
        })
    }

    fn append_args(&self, arguments: &mut Vec<String>, stream_count: usize) {
        arguments.extend([
            "--bqs-session-project".into(),
            self.session_project.clone(),
            "--bqs-max-stream-count".into(),
            stream_count.to_string(),
        ]);
        if let Some(project) = &self.quota_project {
            arguments.extend(["--bqs-quota-project".into(), project.clone()]);
        }
    }
}

#[derive(Debug)]
struct SoakConfig {
    seed: u64,
    duration: Duration,
    max_cases: Option<u64>,
    replay_case: Option<u64>,
    gcs: CloudConfig,
    s3: CloudConfig,
    bigquery: BigQueryConfig,
}

impl SoakConfig {
    fn from_env() -> Result<Self> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let nonce_bytes = nonce.to_le_bytes();
        let default_seed = u64::from_le_bytes(nonce_bytes[..8].try_into().unwrap())
            ^ u64::from_le_bytes(nonce_bytes[8..].try_into().unwrap());
        let seed = std::env::var("SILK_CHIFFON_LIVE_SOAK_SEED").map_or_else(
            |_| Ok(default_seed),
            |value| parse_u64("SILK_CHIFFON_LIVE_SOAK_SEED", &value),
        )?;
        let duration = humantime::parse_duration(
            &std::env::var("SILK_CHIFFON_LIVE_SOAK_DURATION").unwrap_or_else(|_| "5m".to_owned()),
        )
        .context("SILK_CHIFFON_LIVE_SOAK_DURATION must be a positive duration")?;
        ensure!(!duration.is_zero(), "the soak duration must be positive");
        let max_cases = optional_positive("SILK_CHIFFON_LIVE_SOAK_MAX_CASES")?;
        let replay_case = std::env::var("SILK_CHIFFON_LIVE_SOAK_CASE")
            .ok()
            .map(|value| parse_u64("SILK_CHIFFON_LIVE_SOAK_CASE", &value))
            .transpose()?;
        Ok(Self {
            seed,
            duration,
            max_cases,
            replay_case,
            gcs: CloudConfig::from_env(
                "gs",
                "SILK_CHIFFON_LIVE_GCS_BUCKET",
                "SILK_CHIFFON_LIVE_GCS_PREFIX",
                nonce,
            )?,
            s3: CloudConfig::from_env(
                "s3",
                "SILK_CHIFFON_LIVE_S3_BUCKET",
                "SILK_CHIFFON_LIVE_S3_PREFIX",
                nonce,
            )?,
            bigquery: BigQueryConfig::from_env()?,
        })
    }
}

enum VerificationInput {
    Exact(String),
    Pattern(String),
}

struct OutputSelection {
    arguments: Vec<String>,
    verification_input: VerificationInput,
}

fn output_selection(root: &str, format: SoakFormat, layout: OutputLayout) -> OutputSelection {
    let extension = format.extension();
    match layout {
        OutputLayout::Direct => {
            let target = format!("{root}/data.{extension}");
            OutputSelection {
                arguments: vec!["--to".into(), target.clone()],
                verification_input: VerificationInput::Exact(target),
            }
        }
        OutputLayout::SortSingle | OutputLayout::NosortMulti => {
            let strategy = if layout == OutputLayout::SortSingle {
                "sort-single"
            } else {
                "nosort-multi"
            };
            OutputSelection {
                arguments: vec![
                    "--to-many".into(),
                    format!("{root}/part-{{{{partition_key}}}}/data.{extension}"),
                    "--by".into(),
                    "partition_key".into(),
                    "--partition-strategy".into(),
                    strategy.into(),
                ],
                verification_input: VerificationInput::Pattern(format!("{root}/**/*.{extension}")),
            }
        }
        OutputLayout::NosortEvict => OutputSelection {
            arguments: vec![
                "--to-many".into(),
                format!("{root}/part-{{{{partition_key}}}}/data-{{{{file_number}}}}.{extension}"),
                "--by".into(),
                "partition_key".into(),
                "--partition-strategy".into(),
                "nosort-evict".into(),
                "--max-open-partitions".into(),
                "2".into(),
            ],
            verification_input: VerificationInput::Pattern(format!("{root}/**/*.{extension}")),
        },
    }
}

async fn write_local_input(
    path: &Path,
    format: SoakFormat,
    case_index: u64,
    row_count: usize,
) -> Result<()> {
    let first_id = first_local_id(case_index)?;
    let ids = (0..row_count)
        .map(|offset| first_id - i64::try_from(offset).unwrap())
        .collect::<Vec<_>>();
    let names = (0..row_count)
        .map(|offset| format!("local-{case_index}-{offset}"))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from_iter_values(
                names.iter().map(String::as_str),
            )),
        ],
    )?;
    match format {
        SoakFormat::ArrowFile => TestFile::write_arrow_batch(path, &batch),
        SoakFormat::ArrowStream => TestFile::write_arrow_stream(path, &[batch]),
        SoakFormat::Parquet => TestFile::write_parquet_batch(path, &batch),
        SoakFormat::Vortex => {
            let bytes =
                silk_chiffon_test_support::vortex::encode_batches(&schema, vec![batch]).await?;
            fs::write(path, bytes)?;
        }
    }
    Ok(())
}

fn first_local_id(case_index: u64) -> Result<i64> {
    let ordinal = case_index
        .checked_add(1)
        .context("local fixture case index overflow")?;
    i64::try_from(ordinal)?
        .checked_mul(1_000_000)
        .and_then(i64::checked_neg)
        .context("local fixture ID range overflow")
}

async fn run_cli(arguments: Vec<String>) -> Result<()> {
    let cli = Cli::try_parse_from(arguments)?;
    if matches!(cli.command, Command::Completions { .. }) {
        bail!("live soak did not request completions");
    }
    cli.command.execute().await
}

async fn run_scenario(config: &SoakConfig, scenario: &Scenario, case_index: u64) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let input_path = temp
        .path()
        .join(format!("local.{}", scenario.local_format.extension()));
    write_local_input(
        &input_path,
        scenario.local_format,
        case_index,
        scenario.local_rows,
    )
    .await?;
    let case_prefix = format!("seed-{:016x}/case-{case_index:08}", config.seed);
    let output_root = match scenario.output_target {
        OutputTarget::Local => temp.path().join("output").to_string_lossy().into_owned(),
        OutputTarget::Gcs => config.gcs.url(&format!("{case_prefix}/output")),
        OutputTarget::S3 => config.s3.url(&format!("{case_prefix}/output")),
    };
    let selection = output_selection(&output_root, scenario.output_format, scenario.output_layout);
    let mut arguments = vec![
        "silk-chiffon".into(),
        "transform".into(),
        "--from".into(),
        config.bigquery.reference.clone(),
        "--from".into(),
        input_path.to_string_lossy().into_owned(),
        "--query".into(),
        scenario_query(scenario, config.bigquery.expected_rows),
        "--target-partitions".into(),
        scenario.target_partitions.to_string(),
    ];
    arguments.extend(selection.arguments);
    scenario.output_format.append_output_args(&mut arguments);
    config
        .bigquery
        .append_args(&mut arguments, scenario.bqs_stream_count);
    if let Some(restriction) = scenario.explicit_bqs_restriction(config.bigquery.expected_rows) {
        arguments.extend(["--bqs-row-restriction".into(), restriction]);
    }
    if scenario.output_layout == OutputLayout::Direct && scenario.sort_direct_output {
        arguments.extend(["--sort-by".into(), "id:desc".into()]);
    }
    run_cli(arguments)
        .await
        .with_context(|| format!("transform failed for {scenario:?}"))?;

    assert_outputs(config, scenario, &case_prefix, &output_root).await?;
    let oracle_path = temp.path().join("oracle.arrow");
    let mut verification = vec!["silk-chiffon".into(), "transform".into()];
    match selection.verification_input {
        VerificationInput::Exact(reference) => {
            verification.extend(["--from".into(), reference]);
        }
        VerificationInput::Pattern(pattern) => {
            verification.extend(["--from-pattern".into(), pattern]);
        }
    }
    verification.extend([
        "--to".into(),
        oracle_path.to_string_lossy().into_owned(),
        "--output-format".into(),
        "arrow".into(),
        "--arrow-format".into(),
        "stream".into(),
        "--query".into(),
        "SELECT id, name FROM data".into(),
        "--target-partitions".into(),
        "1".into(),
    ]);
    run_cli(verification)
        .await
        .with_context(|| format!("read-back verification failed for {scenario:?}"))?;

    let expected_bqs_range = scenario.selected_bqs_range(config.bigquery.expected_rows);
    verify_oracle(
        &oracle_path,
        expected_bqs_range,
        case_index,
        scenario.local_rows,
    )
    .with_context(|| format!("content oracle failed for {scenario:?}"))
}

fn scenario_query(scenario: &Scenario, total_bqs_rows: usize) -> String {
    let projected_columns = if scenario.project_name_first {
        "name, id"
    } else {
        "id, name"
    };
    let predicate = scenario.predicate_expression(total_bqs_rows);
    let partition_key = if scenario.output_layout == OutputLayout::NosortEvict {
        format!(
            "CASE WHEN id > 0 THEN 0 ELSE ABS(id) % {} END",
            scenario.partition_modulus
        )
    } else {
        format!("id % {}", scenario.partition_modulus)
    };
    format!(
        "SELECT {projected_columns}, {partition_key} AS partition_key FROM data WHERE {predicate}",
    )
}

fn verify_oracle(
    path: &Path,
    expected_bqs_range: (usize, usize),
    case_index: u64,
    expected_local_rows: usize,
) -> Result<()> {
    let (first_bqs_id, last_bqs_id) = expected_bqs_range;
    ensure!(
        first_bqs_id > 0 && first_bqs_id <= last_bqs_id,
        "invalid expected BQS ID range {first_bqs_id}..={last_bqs_id}"
    );
    let expected_bqs_rows = last_bqs_id - first_bqs_id + 1;
    let mut bqs_seen = vec![0_u64; expected_bqs_rows.div_ceil(64)];
    let mut local_seen = vec![false; expected_local_rows];
    let mut bqs_count = 0_usize;
    let mut local_count = 0_usize;
    let first_local_id = first_local_id(case_index)?;
    let reader = ArrowStreamReader::try_new(File::open(path)?, None)?;
    for batch in reader {
        let batch = batch?;
        let id_index = batch.schema().index_of("id")?;
        let name_index = batch.schema().index_of("name")?;
        let ids = batch
            .column(id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("oracle id column is not Int64")?;
        let names = batch.column(name_index);
        for row in 0..batch.num_rows() {
            ensure!(!ids.is_null(row), "row {row} has a null id");
            ensure!(!names.is_null(row), "row {row} has a null name");
            let id = ids.value(row);
            let name = string_value(names.as_ref(), row)?;
            if id > 0 {
                let id = usize::try_from(id)?;
                ensure!(
                    (first_bqs_id..=last_bqs_id).contains(&id),
                    "unexpected BQS id {id}"
                );
                let index = id - first_bqs_id;
                let word = index / 64;
                let mask = 1_u64 << (index % 64);
                ensure!(bqs_seen[word] & mask == 0, "duplicate BQS id {id}");
                bqs_seen[word] |= mask;
                bqs_count += 1;
                ensure!(name == format!("row-{id}"), "wrong name for BQS id {id}");
            } else {
                let offset = usize::try_from(first_local_id - id)
                    .with_context(|| format!("unexpected local id {id}"))?;
                ensure!(offset < expected_local_rows, "unexpected local id {id}");
                ensure!(!local_seen[offset], "duplicate local id {id}");
                local_seen[offset] = true;
                local_count += 1;
                ensure!(
                    name == format!("local-{case_index}-{offset}"),
                    "wrong name for local id {id}"
                );
            }
        }
    }
    ensure!(
        bqs_count == expected_bqs_rows,
        "expected {expected_bqs_rows} BQS rows, observed {bqs_count}"
    );
    ensure!(
        local_count == expected_local_rows,
        "expected {expected_local_rows} local rows, observed {local_count}"
    );
    Ok(())
}

fn string_value(array: &dyn Array, row: usize) -> Result<&str> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        Ok(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        Ok(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        Ok(array.value(row))
    } else {
        bail!(
            "oracle name column has unsupported type {}",
            array.data_type()
        )
    }
}

async fn assert_outputs(
    config: &SoakConfig,
    scenario: &Scenario,
    case_prefix: &str,
    output_root: &str,
) -> Result<()> {
    let paths = match scenario.output_target {
        OutputTarget::Local => local_files(Path::new(output_root))?,
        OutputTarget::Gcs => cloud_files(&config.gcs, case_prefix).await?,
        OutputTarget::S3 => cloud_files(&config.s3, case_prefix).await?,
    };
    ensure!(!paths.is_empty(), "scenario produced no output files");
    if scenario.output_layout == OutputLayout::Direct {
        ensure!(paths.len() == 1, "direct output produced multiple files");
    } else {
        for partition in 0..scenario.partition_modulus {
            let marker = format!("part-{partition}/");
            ensure!(
                paths.iter().any(|path| path.contains(&marker)),
                "partition {partition} produced no output: {paths:?}"
            );
        }
    }
    let extension = format!(".{}", scenario.output_format.extension());
    ensure!(
        paths.iter().all(|path| path.ends_with(&extension)),
        "output extension mismatch: {paths:?}"
    );
    Ok(())
}

fn local_files(root: &Path) -> Result<Vec<String>> {
    fn visit(path: &Path, files: &mut Vec<String>) -> Result<()> {
        if path.is_file() {
            ensure!(fs::metadata(path)?.len() > 0, "empty output at {path:?}");
            files.push(path.to_string_lossy().into_owned());
        } else if path.exists() {
            for entry in fs::read_dir(path)? {
                visit(&entry?.path(), files)?;
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, &mut files)?;
    Ok(files)
}

async fn cloud_files(config: &CloudConfig, case_prefix: &str) -> Result<Vec<String>> {
    let storage = storage_session()?;
    let root = storage
        .prepare_output_target(
            &LocationInput::parse(config.url(&format!("{case_prefix}/list-root")))?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
    let prefix =
        object_store::path::Path::parse(format!("{}/{case_prefix}/output", config.run_prefix))?;
    let objects = root
        .object_store()
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await?;
    for object in &objects {
        ensure!(object.size > 0, "empty cloud output at {}", object.location);
    }
    Ok(objects
        .into_iter()
        .map(|object| object.location.to_string())
        .collect())
}

async fn cleanup_under(config: &CloudConfig, suffix: Option<&str>) -> Result<Vec<String>> {
    let storage = storage_session()?;
    let target_suffix = suffix.unwrap_or("cleanup-root");
    let root = storage
        .prepare_output_target(
            &LocationInput::parse(config.url(&format!("{target_suffix}/cleanup-root")))?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
    let prefix = object_store::path::Path::parse(match suffix {
        Some(suffix) => format!("{}/{suffix}", config.run_prefix),
        None => config.run_prefix.clone(),
    })?;
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
        "cleanup failures under {prefix}: {}",
        deletion_errors.join(", ")
    );
    Ok(leftovers)
}

fn storage_session() -> Result<StorageSession> {
    let registry = StorageRegistry::builder()
        .register(silk_chiffon_storage::gcs::backend()?)
        .register(silk_chiffon_storage::s3::backend()?)
        .build()?;
    let command = registry.augment_args(ClapCommand::new("cloud-live-soak"));
    let matches = command.try_get_matches_from(["cloud-live-soak"])?;
    Ok(registry.create_session(&matches)?)
}

async fn run_case_with_cleanup(
    config: &SoakConfig,
    scenario: &Scenario,
    case_index: u64,
) -> Result<()> {
    let case_prefix = format!("seed-{:016x}/case-{case_index:08}", config.seed);
    let exercise = run_scenario(config, scenario, case_index).await;
    let cleanup = match scenario.output_target {
        OutputTarget::Local => Ok(Vec::new()),
        OutputTarget::Gcs => cleanup_under(&config.gcs, Some(&case_prefix)).await,
        OutputTarget::S3 => cleanup_under(&config.s3, Some(&case_prefix)).await,
    };
    match (exercise, cleanup) {
        (Ok(()), Ok(leftovers)) if leftovers.is_empty() => Ok(()),
        (exercise, cleanup) => bail!(
            "case failed and/or leaked cloud objects: exercise={exercise:?}; cleanup={cleanup:?}"
        ),
    }
}

async fn run_soak(config: &SoakConfig) -> Result<()> {
    eprintln!(
        "cloud soak seed={} duration={:?} max_cases={:?} replay_case={:?}",
        config.seed, config.duration, config.max_cases, config.replay_case
    );
    let started = Instant::now();
    let mut completed = 0_u64;
    let mut case_index = config.replay_case.unwrap_or(0);
    let exercise: Result<()> = async {
        loop {
            if config.replay_case.is_none() {
                if completed > 0 && started.elapsed() >= config.duration {
                    break;
                }
                if config.max_cases.is_some_and(|maximum| completed >= maximum) {
                    break;
                }
            }
            let scenario = Scenario::for_case(
                config.seed,
                case_index,
                config.bigquery.max_stream_count,
            );
            eprintln!(
                "cloud soak case={case_index} seed={} scenario={scenario:?}",
                config.seed
            );
            run_case_with_cleanup(config, &scenario, case_index)
                .await
                .with_context(|| {
                    format!(
                        "replay with SILK_CHIFFON_LIVE_SOAK_SEED={} SILK_CHIFFON_LIVE_SOAK_CASE={case_index}",
                        config.seed
                    )
                })?;
            completed += 1;
            if config.replay_case.is_some() {
                break;
            }
            case_index += 1;
        }
        Ok(())
    }
    .await;
    let gcs_cleanup = cleanup_under(&config.gcs, None).await;
    let s3_cleanup = cleanup_under(&config.s3, None).await;
    match (exercise, gcs_cleanup, s3_cleanup) {
        (Ok(()), Ok(gcs), Ok(s3)) if gcs.is_empty() && s3.is_empty() => {
            eprintln!(
                "cloud soak completed {completed} cases in {:?} with no leaked objects",
                started.elapsed()
            );
            Ok(())
        }
        (exercise, gcs, s3) => bail!(
            "cloud soak failed or leaked objects: exercise={exercise:?}; gcs_cleanup={gcs:?}; s3_cleanup={s3:?}"
        ),
    }
}

fn required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("set {name} for the live cloud soak"))
}

fn parse_positive<T>(name: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr + PartialEq + Default,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let parsed = value
        .parse::<T>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(parsed != T::default(), "{name} must be positive");
    Ok(parsed)
}

fn parse_u64(name: &str, value: &str) -> Result<u64> {
    let parsed = if let Some(hex) = value.strip_prefix("0x") {
        u64::from_str_radix(hex, 16)
    } else {
        value.parse()
    }
    .with_context(|| format!("{name} must be a decimal integer or 0x-prefixed hexadecimal"))?;
    Ok(parsed)
}

fn optional_positive(name: &str) -> Result<Option<u64>> {
    std::env::var(name)
        .ok()
        .map(|value| parse_positive(name, &value))
        .transpose()
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
fn generated_scenarios_are_replayable_and_cover_every_dimension() {
    let seed = 0x7265_706c_6179_u64;
    let mut local_formats = HashSet::new();
    let mut output_formats = HashSet::new();
    let mut layouts = HashSet::new();
    let mut targets = HashSet::new();
    let mut partitions = HashSet::new();
    let mut moduli = HashSet::new();
    let mut local_rows = HashSet::new();
    let mut predicates = HashSet::new();
    let mut local_output_pairs = HashSet::new();
    let mut output_layout_pairs = HashSet::new();
    let mut layout_target_pairs = HashSet::new();
    let mut saw_sorted = false;
    let mut saw_unsorted = false;
    for index in 0..4_096 {
        let scenario = Scenario::generate(seed, index, 8);
        assert_eq!(scenario, Scenario::generate(seed, index, 8));
        local_formats.insert(scenario.local_format);
        output_formats.insert(scenario.output_format);
        layouts.insert(scenario.output_layout);
        targets.insert(scenario.output_target);
        partitions.insert(scenario.target_partitions);
        moduli.insert(scenario.partition_modulus);
        local_rows.insert(scenario.local_rows);
        predicates.insert(scenario.bqs_predicate);
        local_output_pairs.insert((scenario.local_format, scenario.output_format));
        output_layout_pairs.insert((scenario.output_format, scenario.output_layout));
        layout_target_pairs.insert((scenario.output_layout, scenario.output_target));
        saw_sorted |= scenario.sort_direct_output;
        saw_unsorted |= !scenario.sort_direct_output;
    }
    assert_eq!(local_formats, HashSet::from(FORMATS));
    assert_eq!(output_formats, HashSet::from(FORMATS));
    assert_eq!(layouts, HashSet::from(LAYOUTS));
    assert_eq!(targets, HashSet::from(TARGETS));
    assert_eq!(partitions, HashSet::from(PARTITION_COUNTS));
    assert_eq!(moduli, HashSet::from(PARTITION_MODULI));
    assert_eq!(local_rows, HashSet::from(LOCAL_ROW_COUNTS));
    assert_eq!(predicates, HashSet::from(BQS_PREDICATES));
    assert_eq!(local_output_pairs.len(), FORMATS.len() * FORMATS.len());
    assert_eq!(output_layout_pairs.len(), FORMATS.len() * LAYOUTS.len());
    assert_eq!(layout_target_pairs.len(), LAYOUTS.len() * TARGETS.len());
    assert!(saw_sorted && saw_unsorted);
}

#[test]
fn mandatory_prelude_partitions_every_format_to_both_object_stores() {
    let scenarios = (0..24)
        .map(|index| Scenario::for_case(1, index, 8))
        .collect::<Vec<_>>();
    assert!(
        scenarios
            .iter()
            .all(|scenario| scenario.output_target != OutputTarget::Local)
    );
    assert!(
        scenarios
            .iter()
            .all(|scenario| scenario.output_layout != OutputLayout::Direct)
    );
    assert_eq!(
        scenarios
            .iter()
            .map(|scenario| (
                scenario.output_target,
                scenario.output_format,
                scenario.output_layout,
            ))
            .collect::<HashSet<_>>()
            .len(),
        CLOUD_TARGETS.len() * FORMATS.len() * PARTITIONED_LAYOUTS.len()
    );
    assert_eq!(
        scenarios
            .iter()
            .map(|scenario| scenario.bqs_predicate)
            .collect::<HashSet<_>>(),
        HashSet::from(BQS_PREDICATES)
    );
    assert!(scenarios.iter().any(|scenario| scenario.project_name_first));
    assert!(
        scenarios
            .iter()
            .any(|scenario| !scenario.project_name_first)
    );
    for scenario in scenarios
        .iter()
        .filter(|scenario| scenario.output_layout == OutputLayout::NosortEvict)
    {
        let (lower, upper) = scenario.selected_bqs_range(5_000_000);
        assert_eq!(scenario.local_rows, EVICT_LOCAL_ROWS);
        assert_eq!(upper - lower + 1, MAX_EVICT_BQS_ROWS);
        assert_eq!(
            scenario.predicate_expression(5_000_000),
            format!("id < 0 OR id BETWEEN {lower} AND {upper}")
        );
        assert_eq!(
            scenario.explicit_bqs_restriction(5_000_000),
            Some(format!("`id` BETWEEN {lower} AND {upper}"))
        );
        assert!(scenario_query(scenario, 5_000_000).contains("CASE WHEN id > 0 THEN 0"));
    }
}

#[test]
fn output_templates_encode_each_partition_lifecycle() {
    let direct = output_selection("root", SoakFormat::ArrowStream, OutputLayout::Direct);
    assert_eq!(direct.arguments, ["--to", "root/data.arrow"]);
    let sorted = output_selection("root", SoakFormat::Parquet, OutputLayout::SortSingle);
    assert!(sorted.arguments.iter().any(|value| value == "sort-single"));
    assert!(
        !sorted
            .arguments
            .iter()
            .any(|value| value.contains("file_number"))
    );
    let multi = output_selection("root", SoakFormat::Vortex, OutputLayout::NosortMulti);
    assert!(multi.arguments.iter().any(|value| value == "nosort-multi"));
    assert!(
        !multi
            .arguments
            .iter()
            .any(|value| value.contains("file_number"))
    );
    let evict = output_selection("root", SoakFormat::ArrowFile, OutputLayout::NosortEvict);
    assert!(evict.arguments.iter().any(|value| value == "nosort-evict"));
    assert!(
        evict
            .arguments
            .iter()
            .any(|value| value.contains("file_number"))
    );
}

fn write_oracle_rows(path: &Path, rows: &[(i64, &str)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8View, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringViewArray::from_iter_values(
                rows.iter().map(|(_, name)| *name),
            )),
        ],
    )
    .unwrap();
    TestFile::write_arrow_stream(path, &[batch]);
}

#[test]
fn content_oracle_checks_every_unordered_row_and_rejects_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let valid = temp.path().join("valid.arrow");
    write_oracle_rows(
        &valid,
        &[
            (3, "row-3"),
            (-1_000_001, "local-0-1"),
            (1, "row-1"),
            (-1_000_000, "local-0-0"),
            (2, "row-2"),
        ],
    );
    verify_oracle(&valid, (1, 3), 0, 2).unwrap();

    let duplicate = temp.path().join("duplicate.arrow");
    write_oracle_rows(&duplicate, &[(1, "row-1"), (1, "row-1")]);
    assert!(
        verify_oracle(&duplicate, (1, 2), 0, 0)
            .unwrap_err()
            .to_string()
            .contains("duplicate BQS id 1")
    );

    let corrupted = temp.path().join("corrupted.arrow");
    write_oracle_rows(&corrupted, &[(1, "wrong")]);
    assert!(
        verify_oracle(&corrupted, (1, 1), 0, 0)
            .unwrap_err()
            .to_string()
            .contains("wrong name for BQS id 1")
    );
}

#[tokio::test]
async fn every_generated_pushdown_query_plans_against_the_fixture_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let provider = MemTable::try_new(
        Arc::clone(&schema),
        vec![vec![RecordBatch::new_empty(schema)]],
    )
    .unwrap();
    let session = SessionContext::new();
    session.register_table("data", Arc::new(provider)).unwrap();
    for index in 0..24 {
        let scenario = Scenario::for_case(0x0071_7565_7279, index, 8);
        let frame = session
            .sql(&scenario_query(&scenario, 5_000_000))
            .await
            .unwrap();
        assert_eq!(frame.schema().field_names().len(), 3);
        assert!(frame.schema().has_column_with_unqualified_name("id"));
        assert!(frame.schema().has_column_with_unqualified_name("name"));
        assert!(
            frame
                .schema()
                .has_column_with_unqualified_name("partition_key")
        );
        assert!(!frame.schema().has_column_with_unqualified_name("payload"));
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires explicit GCS, S3, and BigQuery fixtures and may run for hours"]
async fn live_seeded_mixed_input_cross_provider_soak() {
    let config = SoakConfig::from_env().unwrap();
    run_soak(&config).await.unwrap();
}
