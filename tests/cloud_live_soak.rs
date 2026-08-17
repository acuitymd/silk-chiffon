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
use bytes::Bytes;
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
const INPUT_OBJECT_STORES: [OutputTarget; 3] =
    [OutputTarget::Local, OutputTarget::Gcs, OutputTarget::S3];
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

const DEFAULT_INPUT_SERVICES: [InputService; 2] = [InputService::BigQuery, InputService::Local];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SoakFormat {
    ArrowFile,
    ArrowStream,
    Parquet,
    Vortex,
}

impl SoakFormat {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "arrow-file" => Ok(Self::ArrowFile),
            "arrow-stream" => Ok(Self::ArrowStream),
            "parquet" => Ok(Self::Parquet),
            "vortex" => Ok(Self::Vortex),
            _ => bail!(
                "unknown soak format {value:?}; choose arrow-file, arrow-stream, parquet, or vortex"
            ),
        }
    }

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

impl OutputTarget {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "local" => Ok(Self::Local),
            "gcs" => Ok(Self::Gcs),
            "s3" => Ok(Self::S3),
            _ => bail!("unknown soak object store {value:?}; choose local, gcs, or s3"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum InputService {
    BigQuery,
    Local,
}

impl InputService {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "bqs" => Ok(Self::BigQuery),
            "local" => Ok(Self::Local),
            _ => bail!("unknown soak input service {value:?}; choose bqs or local"),
        }
    }
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
    input_format: SoakFormat,
    input_object_store: OutputTarget,
    output_format: SoakFormat,
    output_layout: OutputLayout,
    output_object_store: OutputTarget,
    target_partitions: usize,
    bqs_stream_count: usize,
    partition_modulus: i64,
    local_rows: usize,
    bqs_predicate: BqsPredicate,
    project_name_first: bool,
    sort_direct_output: bool,
}

impl Scenario {
    fn for_case(
        seed: u64,
        index: u64,
        max_stream_count: usize,
        input_formats: &[SoakFormat],
        output_formats: &[SoakFormat],
        input_object_stores: &[OutputTarget],
        output_object_stores: &[OutputTarget],
    ) -> Self {
        let mandatory_cases =
            output_object_stores.len() * output_formats.len() * PARTITIONED_LAYOUTS.len();
        if let Ok(index) = usize::try_from(index)
            && index < mandatory_cases
        {
            let output_layout =
                PARTITIONED_LAYOUTS[index / (output_object_stores.len() * output_formats.len())];
            return Self {
                input_format: input_formats[(index + 1) % input_formats.len()],
                input_object_store: input_object_stores
                    [(index / output_object_stores.len()) % input_object_stores.len()],
                output_format: output_formats
                    [(index / output_object_stores.len()) % output_formats.len()],
                output_layout,
                output_object_store: output_object_stores[index % output_object_stores.len()],
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
        Self::generate(
            seed,
            index,
            max_stream_count,
            input_formats,
            output_formats,
            input_object_stores,
            output_object_stores,
        )
    }

    fn generate(
        seed: u64,
        index: u64,
        max_stream_count: usize,
        input_formats: &[SoakFormat],
        output_formats: &[SoakFormat],
        input_object_stores: &[OutputTarget],
        output_object_stores: &[OutputTarget],
    ) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let output_layout = LAYOUTS[rng.random_range(0..LAYOUTS.len())];
        Self {
            input_format: input_formats[rng.random_range(0..input_formats.len())],
            input_object_store: input_object_stores[rng.random_range(0..input_object_stores.len())],
            output_format: output_formats[rng.random_range(0..output_formats.len())],
            output_layout,
            output_object_store: output_object_stores
                [rng.random_range(0..output_object_stores.len())],
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
    input_formats: Vec<SoakFormat>,
    output_formats: Vec<SoakFormat>,
    input_object_stores: Vec<OutputTarget>,
    output_object_stores: Vec<OutputTarget>,
    input_services: Vec<InputService>,
    gcs: Option<CloudConfig>,
    s3: Option<CloudConfig>,
    bigquery: Option<BigQueryConfig>,
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
        let input_formats = parse_selection(
            "SILK_CHIFFON_LIVE_SOAK_INPUT_FORMATS",
            FORMATS,
            SoakFormat::parse,
        )?;
        let output_formats = parse_selection(
            "SILK_CHIFFON_LIVE_SOAK_OUTPUT_FORMATS",
            FORMATS,
            SoakFormat::parse,
        )?;
        let input_object_stores = parse_selection(
            "SILK_CHIFFON_LIVE_SOAK_INPUT_OBJECT_STORES",
            INPUT_OBJECT_STORES,
            OutputTarget::parse,
        )?;
        let output_object_stores = parse_selection(
            "SILK_CHIFFON_LIVE_SOAK_OUTPUT_OBJECT_STORES",
            TARGETS,
            OutputTarget::parse,
        )?;
        let input_services = parse_selection(
            "SILK_CHIFFON_LIVE_SOAK_INPUT_SERVICES",
            DEFAULT_INPUT_SERVICES,
            InputService::parse,
        )?;
        let local_input_selected = input_services.contains(&InputService::Local);
        Ok(Self {
            seed,
            duration,
            max_cases,
            replay_case,
            input_formats,
            output_formats,
            input_object_stores: input_object_stores.clone(),
            output_object_stores: output_object_stores.clone(),
            input_services: input_services.clone(),
            gcs: (output_object_stores.contains(&OutputTarget::Gcs)
                || (local_input_selected && input_object_stores.contains(&OutputTarget::Gcs)))
            .then(|| {
                CloudConfig::from_env(
                    "gs",
                    "SILK_CHIFFON_LIVE_GCS_BUCKET",
                    "SILK_CHIFFON_LIVE_GCS_PREFIX",
                    nonce,
                )
            })
            .transpose()?,
            s3: (output_object_stores.contains(&OutputTarget::S3)
                || (local_input_selected && input_object_stores.contains(&OutputTarget::S3)))
            .then(|| {
                CloudConfig::from_env(
                    "s3",
                    "SILK_CHIFFON_LIVE_S3_BUCKET",
                    "SILK_CHIFFON_LIVE_S3_PREFIX",
                    nonce,
                )
            })
            .transpose()?,
            bigquery: input_services
                .contains(&InputService::BigQuery)
                .then(BigQueryConfig::from_env)
                .transpose()?,
        })
    }

    fn cloud_config(&self, target: OutputTarget) -> Result<&CloudConfig> {
        match target {
            OutputTarget::Local => bail!("local output has no cloud configuration"),
            OutputTarget::Gcs => self
                .gcs
                .as_ref()
                .context("GCS was not selected for this soak"),
            OutputTarget::S3 => self
                .s3
                .as_ref()
                .context("S3 was not selected for this soak"),
        }
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

async fn upload_input(
    config: &SoakConfig,
    target: OutputTarget,
    path: &Path,
    case_prefix: &str,
) -> Result<String> {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .context("input fixture has no UTF-8 extension")?;
    let url = config
        .cloud_config(target)?
        .url(&format!("{case_prefix}/input.{extension}"));
    let storage = storage_session(config)?;
    let output = storage
        .prepare_output_target(
            &LocationInput::parse(&url)?,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;
    let bytes = Bytes::from(fs::read(path)?);
    output
        .object_store()
        .put(output.object_path(), bytes.into())
        .await?;
    Ok(url)
}

async fn run_scenario(config: &SoakConfig, scenario: &Scenario, case_index: u64) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let local_input = if config.input_services.contains(&InputService::Local) {
        let input_path = temp
            .path()
            .join(format!("local.{}", scenario.input_format.extension()));
        write_local_input(
            &input_path,
            scenario.input_format,
            case_index,
            scenario.local_rows,
        )
        .await?;
        Some(input_path)
    } else {
        None
    };
    let case_prefix = format!("seed-{:016x}/case-{case_index:08}", config.seed);
    let output_root = match scenario.output_object_store {
        OutputTarget::Local => temp.path().join("output").to_string_lossy().into_owned(),
        target => config
            .cloud_config(target)?
            .url(&format!("{case_prefix}/output")),
    };
    let selection = output_selection(&output_root, scenario.output_format, scenario.output_layout);
    let mut arguments = vec!["silk-chiffon".into(), "transform".into()];
    if config.input_services.contains(&InputService::BigQuery) {
        let bigquery = config.bigquery.as_ref().context("BQS was not selected")?;
        arguments.extend(["--from".into(), bigquery.reference.clone()]);
    }
    if let Some(input_path) = local_input {
        let input_reference = match scenario.input_object_store {
            OutputTarget::Local => input_path.to_string_lossy().into_owned(),
            target => upload_input(config, target, &input_path, &case_prefix).await?,
        };
        arguments.extend(["--from".into(), input_reference]);
    }
    ensure!(
        arguments.len() > 2,
        "at least one soak input must be selected"
    );
    let total_bqs_rows = config
        .bigquery
        .as_ref()
        .map_or(0, |bigquery| bigquery.expected_rows);
    arguments.extend([
        "--query".into(),
        scenario_query(scenario, total_bqs_rows, &config.input_services),
        "--target-partitions".into(),
        scenario.target_partitions.to_string(),
    ]);
    arguments.extend(selection.arguments);
    scenario.output_format.append_output_args(&mut arguments);
    if let Some(bigquery) = &config.bigquery {
        bigquery.append_args(&mut arguments, scenario.bqs_stream_count);
        if let Some(restriction) = scenario.explicit_bqs_restriction(bigquery.expected_rows) {
            arguments.extend(["--bqs-row-restriction".into(), restriction]);
        }
    }
    if scenario.output_layout == OutputLayout::Direct && scenario.sort_direct_output {
        arguments.extend(["--sort-by".into(), "id:desc".into()]);
    }
    run_cli(arguments)
        .await
        .with_context(|| format!("transform failed for {scenario:?}"))?;

    assert_outputs(config, scenario, case_index, &case_prefix, &output_root).await?;
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

    let expected_bqs_range = config
        .bigquery
        .as_ref()
        .map(|bigquery| scenario.selected_bqs_range(bigquery.expected_rows));
    verify_oracle(
        &oracle_path,
        expected_bqs_range,
        case_index,
        if config.input_services.contains(&InputService::Local) {
            scenario.local_rows
        } else {
            0
        },
    )
    .with_context(|| format!("content oracle failed for {scenario:?}"))
}

fn scenario_query(
    scenario: &Scenario,
    total_bqs_rows: usize,
    input_services: &[InputService],
) -> String {
    let projected_columns = if scenario.project_name_first {
        "name, id"
    } else {
        "id, name"
    };
    let predicate = match (
        input_services.contains(&InputService::BigQuery),
        input_services.contains(&InputService::Local),
    ) {
        (true, _) => scenario.predicate_expression(total_bqs_rows),
        (false, true) => "id < 0".to_owned(),
        (false, false) => unreachable!("the soak requires one input"),
    };
    let partition_key = if scenario.output_layout == OutputLayout::NosortEvict {
        format!(
            "CASE WHEN id > 0 THEN 0 ELSE ABS(id) % {} END",
            scenario.partition_modulus
        )
    } else {
        format!(
            "CASE WHEN id > 0 THEN id % {0} ELSE ABS(id) % {0} END",
            scenario.partition_modulus
        )
    };
    format!(
        "SELECT {projected_columns}, {partition_key} AS partition_key FROM data WHERE {predicate}",
    )
}

fn verify_oracle(
    path: &Path,
    expected_bqs_range: Option<(usize, usize)>,
    case_index: u64,
    expected_local_rows: usize,
) -> Result<()> {
    let (first_bqs_id, last_bqs_id, expected_bqs_rows) = expected_bqs_range
        .map_or((0, 0, 0), |(first, last)| {
            (first, last, last.saturating_sub(first).saturating_add(1))
        });
    if expected_bqs_range.is_some() {
        ensure!(
            first_bqs_id > 0 && first_bqs_id <= last_bqs_id,
            "invalid expected BQS ID range {first_bqs_id}..={last_bqs_id}"
        );
    }
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
    case_index: u64,
    case_prefix: &str,
    output_root: &str,
) -> Result<()> {
    let paths = match scenario.output_object_store {
        OutputTarget::Local => local_files(Path::new(output_root))?,
        target => cloud_files(config, config.cloud_config(target)?, case_prefix).await?,
    };
    ensure!(!paths.is_empty(), "scenario produced no output files");
    if scenario.output_layout == OutputLayout::Direct {
        ensure!(paths.len() == 1, "direct output produced multiple files");
    } else {
        let partitions = paths
            .iter()
            .map(|path| partition_id(path))
            .collect::<Result<HashSet<_>>>()?;
        let expected = expected_partition_ids(
            scenario,
            case_index,
            &config.input_services,
            config
                .bigquery
                .as_ref()
                .map(|bigquery| bigquery.expected_rows),
        )?;
        ensure!(
            partitions == expected,
            "expected partition set {expected:?}, observed {partitions:?}: {paths:?}"
        );
    }
    let extension = format!(".{}", scenario.output_format.extension());
    ensure!(
        paths.iter().all(|path| path.ends_with(&extension)),
        "output extension mismatch: {paths:?}"
    );
    Ok(())
}

fn partition_id(path: &str) -> Result<usize> {
    let value = path
        .split('/')
        .rev()
        .find_map(|segment| segment.strip_prefix("part-"))
        .with_context(|| format!("partitioned output has no partition path: {path}"))?;
    value
        .parse()
        .with_context(|| format!("partitioned output has an invalid partition path: {path}"))
}

fn expected_partition_ids(
    scenario: &Scenario,
    case_index: u64,
    input_services: &[InputService],
    total_bqs_rows: Option<usize>,
) -> Result<HashSet<usize>> {
    let modulus =
        usize::try_from(scenario.partition_modulus).context("partition modulus must fit usize")?;
    ensure!(modulus > 0, "partition modulus must be positive");
    let mut expected = HashSet::new();
    if input_services.contains(&InputService::BigQuery) {
        let total_bqs_rows = total_bqs_rows.context("BQS row count was not configured")?;
        let (lower, upper) = scenario.selected_bqs_range(total_bqs_rows);
        if scenario.output_layout == OutputLayout::NosortEvict {
            expected.insert(0);
        } else if upper.saturating_sub(lower).saturating_add(1) >= modulus {
            expected.extend(0..modulus);
        } else {
            for id in lower..=upper {
                expected.insert(id % modulus);
            }
        }
    }
    if input_services.contains(&InputService::Local) {
        let first_id = first_local_id(case_index)?;
        for offset in 0..scenario.local_rows {
            let id = first_id
                .checked_sub(i64::try_from(offset)?)
                .context("local fixture ID range overflow")?;
            expected.insert(usize::try_from(id.unsigned_abs())? % modulus);
        }
    }
    Ok(expected)
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

async fn cloud_files(
    soak_config: &SoakConfig,
    config: &CloudConfig,
    case_prefix: &str,
) -> Result<Vec<String>> {
    let storage = storage_session(soak_config)?;
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

async fn cleanup_under(
    soak_config: &SoakConfig,
    config: &CloudConfig,
    suffix: Option<&str>,
) -> Result<Vec<String>> {
    let storage = storage_session(soak_config)?;
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

fn storage_session(config: &SoakConfig) -> Result<StorageSession> {
    let mut registry = StorageRegistry::builder();
    let needs_gcs = config.output_object_stores.contains(&OutputTarget::Gcs)
        || (config.input_services.contains(&InputService::Local)
            && config.input_object_stores.contains(&OutputTarget::Gcs));
    let needs_s3 = config.output_object_stores.contains(&OutputTarget::S3)
        || (config.input_services.contains(&InputService::Local)
            && config.input_object_stores.contains(&OutputTarget::S3));
    if needs_gcs {
        registry = registry.register(silk_chiffon_storage::gcs::backend()?);
    }
    if needs_s3 {
        registry = registry.register(silk_chiffon_storage::s3::backend()?);
    }
    let registry = registry.build()?;
    let command = registry.augment_args(ClapCommand::new("cloud-live-soak"));
    let matches = command.try_get_matches_from(["cloud-live-soak"])?;
    Ok(registry.create_session(&matches)?)
}

async fn run_case(config: &SoakConfig, scenario: &Scenario, case_index: u64) -> Result<()> {
    run_scenario(config, scenario, case_index).await
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
                config
                    .bigquery
                    .as_ref()
                    .map_or(1, |bigquery| bigquery.max_stream_count),
                &config.input_formats,
                &config.output_formats,
                &config.input_object_stores,
                &config.output_object_stores,
            );
            eprintln!(
                "cloud soak case={case_index} seed={} scenario={scenario:?}",
                config.seed
            );
            run_case(config, &scenario, case_index)
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
    let mut leftovers = Vec::new();
    let mut cleanup_errors = Vec::new();
    for target in [OutputTarget::Gcs, OutputTarget::S3] {
        if config.output_object_stores.contains(&target)
            || (config.input_services.contains(&InputService::Local)
                && config.input_object_stores.contains(&target))
        {
            match config.cloud_config(target) {
                Ok(cloud) => match cleanup_under(config, cloud, None).await {
                    Ok(target_leftovers) => leftovers.extend(target_leftovers),
                    Err(error) => cleanup_errors.push(format!("{target:?}: {error}")),
                },
                Err(error) => cleanup_errors.push(format!("{target:?}: {error}")),
            }
        }
    }
    if exercise.is_ok() && leftovers.is_empty() && cleanup_errors.is_empty() {
        eprintln!(
            "cloud soak completed {completed} cases in {:?} with no leaked objects",
            started.elapsed()
        );
        Ok(())
    } else {
        bail!(
            "cloud soak failed or leaked objects: exercise={exercise:?}; leftovers={leftovers:?}; cleanup_errors={cleanup_errors:?}"
        )
    }
}

fn required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("set {name} for the live cloud soak"))
}

fn parse_selection<T, const N: usize>(
    name: &str,
    defaults: [T; N],
    parse: impl Fn(&str) -> Result<T>,
) -> Result<Vec<T>>
where
    T: Copy + Eq,
{
    let Some(value) = std::env::var_os(name) else {
        return Ok(defaults.into());
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{name} must be valid UTF-8"))?;
    parse_selection_value(name, &value, parse)
}

fn parse_selection_value<T>(
    name: &str,
    value: &str,
    parse: impl Fn(&str) -> Result<T>,
) -> Result<Vec<T>>
where
    T: Copy + Eq,
{
    let mut selected = Vec::new();
    for item in value.split(',').map(str::trim) {
        ensure!(!item.is_empty(), "{name} must not contain empty values");
        let parsed = parse(item).with_context(|| format!("invalid value in {name}"))?;
        ensure!(
            !selected.contains(&parsed),
            "{name} contains duplicate value {item:?}"
        );
        selected.push(parsed);
    }
    ensure!(
        !selected.is_empty(),
        "{name} must select at least one value"
    );
    Ok(selected)
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
    let mut input_object_stores = HashSet::new();
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
        let scenario = Scenario::generate(
            seed,
            index,
            8,
            &FORMATS,
            &FORMATS,
            &INPUT_OBJECT_STORES,
            &TARGETS,
        );
        assert_eq!(
            scenario,
            Scenario::generate(
                seed,
                index,
                8,
                &FORMATS,
                &FORMATS,
                &INPUT_OBJECT_STORES,
                &TARGETS,
            )
        );
        local_formats.insert(scenario.input_format);
        input_object_stores.insert(scenario.input_object_store);
        output_formats.insert(scenario.output_format);
        layouts.insert(scenario.output_layout);
        targets.insert(scenario.output_object_store);
        partitions.insert(scenario.target_partitions);
        moduli.insert(scenario.partition_modulus);
        local_rows.insert(scenario.local_rows);
        predicates.insert(scenario.bqs_predicate);
        local_output_pairs.insert((scenario.input_format, scenario.output_format));
        output_layout_pairs.insert((scenario.output_format, scenario.output_layout));
        layout_target_pairs.insert((scenario.output_layout, scenario.output_object_store));
        saw_sorted |= scenario.sort_direct_output;
        saw_unsorted |= !scenario.sort_direct_output;
    }
    assert_eq!(local_formats, HashSet::from(FORMATS));
    assert_eq!(input_object_stores, HashSet::from(INPUT_OBJECT_STORES));
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
fn soak_selection_parses_independent_format_store_and_input_sets() {
    assert_eq!(
        parse_selection_value("formats", "parquet, arrow-stream", SoakFormat::parse).unwrap(),
        vec![SoakFormat::Parquet, SoakFormat::ArrowStream]
    );
    assert_eq!(
        parse_selection_value("stores", "gcs", OutputTarget::parse).unwrap(),
        vec![OutputTarget::Gcs]
    );
    assert_eq!(
        parse_selection_value("input services", "local", InputService::parse).unwrap(),
        vec![InputService::Local]
    );
    assert!(parse_selection_value("stores", "gcs,gcs", OutputTarget::parse).is_err());
    assert!(parse_selection_value("input services", "", InputService::parse).is_err());
}

#[test]
fn selected_dimensions_shape_the_deterministic_prelude() {
    let formats = [SoakFormat::Parquet];
    let targets = [OutputTarget::Gcs];
    let scenarios = (0..3)
        .map(|index| {
            Scenario::for_case(
                1,
                index,
                8,
                &formats,
                &formats,
                &[OutputTarget::Gcs],
                &targets,
            )
        })
        .collect::<Vec<_>>();
    assert!(scenarios.iter().all(|scenario| {
        scenario.input_format == SoakFormat::Parquet
            && scenario.output_format == SoakFormat::Parquet
            && scenario.input_object_store == OutputTarget::Gcs
            && scenario.output_object_store == OutputTarget::Gcs
    }));
    assert_eq!(
        scenarios
            .iter()
            .map(|scenario| scenario.output_layout)
            .collect::<HashSet<_>>(),
        HashSet::from(PARTITIONED_LAYOUTS)
    );
}

#[test]
fn mandatory_prelude_partitions_every_format_to_both_object_stores() {
    let scenarios = (0..24)
        .map(|index| {
            Scenario::for_case(
                1,
                index,
                8,
                &FORMATS,
                &FORMATS,
                &INPUT_OBJECT_STORES,
                &CLOUD_TARGETS,
            )
        })
        .collect::<Vec<_>>();
    assert!(
        scenarios
            .iter()
            .all(|scenario| scenario.output_object_store != OutputTarget::Local)
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
                scenario.output_object_store,
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
            .map(|scenario| (scenario.input_object_store, scenario.output_object_store,))
            .collect::<HashSet<_>>()
            .len(),
        INPUT_OBJECT_STORES.len() * CLOUD_TARGETS.len()
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
        assert!(
            scenario_query(scenario, 5_000_000, &DEFAULT_INPUT_SERVICES)
                .contains("CASE WHEN id > 0 THEN 0")
        );
    }
}

#[test]
fn bqs_only_eviction_keeps_the_sql_row_bound() {
    let scenario = (0..24)
        .map(|index| {
            Scenario::for_case(
                1,
                index,
                8,
                &FORMATS,
                &FORMATS,
                &INPUT_OBJECT_STORES,
                &CLOUD_TARGETS,
            )
        })
        .find(|scenario| scenario.output_layout == OutputLayout::NosortEvict)
        .expect("mandatory prelude includes eviction");
    let (lower, upper) = scenario.selected_bqs_range(5_000_000);
    let query = scenario_query(&scenario, 5_000_000, &[InputService::BigQuery]);
    assert!(query.contains(&format!("id BETWEEN {lower} AND {upper}")));
}

#[test]
fn expected_partitions_follow_the_selected_sources() {
    let mut scenario = Scenario::for_case(
        1,
        0,
        8,
        &FORMATS,
        &FORMATS,
        &INPUT_OBJECT_STORES,
        &CLOUD_TARGETS,
    );
    scenario.output_layout = OutputLayout::NosortMulti;
    scenario.partition_modulus = 7;
    scenario.local_rows = 1;
    scenario.bqs_predicate = BqsPredicate::Prefix;

    assert_eq!(
        expected_partition_ids(&scenario, 0, &[InputService::BigQuery], Some(6)).unwrap(),
        HashSet::from([1, 2, 3])
    );
    assert_eq!(
        expected_partition_ids(&scenario, 3, &[InputService::Local], None).unwrap(),
        HashSet::from([4])
    );
    assert_eq!(
        expected_partition_ids(
            &scenario,
            3,
            &[InputService::BigQuery, InputService::Local],
            Some(6),
        )
        .unwrap(),
        HashSet::from([1, 2, 3, 4])
    );
    assert!(expected_partition_ids(&scenario, 0, &[InputService::BigQuery], None).is_err());

    scenario.output_layout = OutputLayout::NosortEvict;
    assert_eq!(
        expected_partition_ids(&scenario, 0, &[InputService::BigQuery], Some(6)).unwrap(),
        HashSet::from([0])
    );

    assert_eq!(
        partition_id("gs://part-9/root/part-2/data.parquet").unwrap(),
        2
    );
    assert!(partition_id("gs://bucket/root/data.parquet").is_err());
    assert!(partition_id("gs://bucket/root/part-invalid/data.parquet").is_err());
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
    verify_oracle(&valid, Some((1, 3)), 0, 2).unwrap();

    let duplicate = temp.path().join("duplicate.arrow");
    write_oracle_rows(&duplicate, &[(1, "row-1"), (1, "row-1")]);
    assert!(
        verify_oracle(&duplicate, Some((1, 2)), 0, 0)
            .unwrap_err()
            .to_string()
            .contains("duplicate BQS id 1")
    );

    let corrupted = temp.path().join("corrupted.arrow");
    write_oracle_rows(&corrupted, &[(1, "wrong")]);
    assert!(
        verify_oracle(&corrupted, Some((1, 1)), 0, 0)
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
        let scenario = Scenario::for_case(
            0x0071_7565_7279,
            index,
            8,
            &FORMATS,
            &FORMATS,
            &INPUT_OBJECT_STORES,
            &CLOUD_TARGETS,
        );
        let frame = session
            .sql(&scenario_query(
                &scenario,
                5_000_000,
                &DEFAULT_INPUT_SERVICES,
            ))
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
