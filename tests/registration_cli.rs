use clap::CommandFactory;
use silk_chiffon::{Cli, Command};
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_core::{InspectionOutput, PresentationMode};
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_storage::LocationInput;
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_test_support::{TestBatch, TestFile};

#[test]
fn composed_cli_binds_registered_transform_arguments() {
    let cli = Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from",
        "input.arrow",
        "--to",
        "output.parquet",
        "--arrow-record-batch-size",
        "4096",
        "--parquet-row-group-size",
        "8192",
        "--vortex-record-batch-size",
        "2048",
    ])
    .unwrap();

    let Command::Transform(_command) = cli.command else {
        panic!("expected transform command");
    };
}

#[test]
fn composed_cli_rejects_an_unregistered_format() {
    let error = Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from",
        "input.arrow",
        "--to",
        "output.arrow",
        "--input-format",
        "unknown",
    ])
    .unwrap_err();
    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
    let message = error.to_string();
    assert!(message.contains("arrow"));
    assert!(message.contains("parquet"));
    assert!(message.contains("vortex"));
}

#[test]
fn transform_cli_combines_repeatable_exact_references_and_patterns() {
    Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from",
        "one.arrow",
        "--from",
        "two.arrow",
        "--to",
        "output.arrow",
    ])
    .unwrap();

    Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from-pattern",
        "one-*.arrow",
        "--from-pattern",
        "two-*.arrow",
        "--to",
        "output.arrow",
    ])
    .unwrap();

    Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from",
        "one.arrow",
        "--from-pattern",
        "two-*.arrow",
        "--to",
        "output.arrow",
    ])
    .unwrap();
}

#[test]
fn allow_unmatched_patterns_requires_a_pattern_operand() {
    let error = Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from",
        "one.arrow",
        "--allow-unmatched-patterns",
        "--to",
        "output.arrow",
    ])
    .unwrap_err();
    assert_eq!(
        error.kind(),
        clap::error::ErrorKind::MissingRequiredArgument
    );
}

#[test]
fn transform_cli_rejects_removed_input_flag() {
    let error = Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from-many",
        "*.arrow",
        "--to",
        "output.arrow",
    ])
    .unwrap_err();
    assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
}

#[test]
fn runtime_command_resolves_the_worker_thread_policy() {
    let cli = Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from",
        "input.arrow",
        "--to",
        "output.arrow",
        "--thread-budget",
        "2",
    ])
    .unwrap();

    assert_eq!(cli.command.runtime_worker_threads(), 2);
}

#[test]
fn registered_arguments_are_present_in_help_and_completions() {
    let mut command = Cli::command();
    let help = command
        .find_subcommand_mut("transform")
        .unwrap()
        .render_long_help()
        .to_string();
    assert!(help.contains("--arrow-record-batch-size"));
    assert!(help.contains("--parquet-row-group-size"));
    assert!(help.contains("--parquet-writing-threads"));
    assert!(!help.contains("--parquet-io-threads"));
    assert!(help.contains("--vortex-record-batch-size"));
    assert!(help.contains("possible values: arrow, parquet, vortex"));
    #[cfg(feature = "gcs")]
    for option in ["--gcs-endpoint", "--gcs-anonymous", "--gcs-request-timeout"] {
        assert!(help.contains(option));
    }
    #[cfg(not(feature = "gcs"))]
    assert!(!help.contains("--gcs-"));
    #[cfg(feature = "s3")]
    for option in [
        "--s3-region",
        "--s3-endpoint",
        "--s3-addressing-style",
        "--s3-anonymous",
        "--s3-request-timeout",
    ] {
        assert!(help.contains(option));
    }
    #[cfg(not(feature = "s3"))]
    assert!(!help.contains("--s3-"));
    #[cfg(feature = "bigquery")]
    for option in [
        "--bqs-session-project",
        "--bqs-quota-project",
        "--bqs-endpoint",
        "--bqs-universe-domain",
        "--bqs-row-restriction",
        "--bqs-max-stream-count",
        "--bqs-max-response-bytes",
        "--bqs-arrow-wire-compression",
        "--bqs-response-compression",
        "--bqs-picos-timestamp-precision",
        "--bqs-read-idle-timeout",
        "--bqs-read-retry-window",
        "--bqs-read-retry-initial-backoff",
        "--bqs-read-retry-max-backoff",
    ] {
        assert!(help.contains(option));
    }
    #[cfg(not(feature = "bigquery"))]
    assert!(!help.contains("--bqs-"));

    let mut completions = Vec::new();
    clap_complete::generate(
        clap_complete::Shell::Bash,
        &mut Cli::command(),
        "silk-chiffon",
        &mut completions,
    );
    let completions = String::from_utf8(completions).unwrap();
    assert!(completions.contains("--arrow-record-batch-size"));
    assert!(completions.contains("--parquet-row-group-size"));
    assert!(completions.contains("--parquet-writing-threads"));
    assert!(!completions.contains("--parquet-io-threads"));
    assert!(completions.contains("--vortex-record-batch-size"));
    #[cfg(feature = "gcs")]
    assert!(completions.contains("--gcs-endpoint"));
    #[cfg(not(feature = "gcs"))]
    assert!(!completions.contains("--gcs-"));
    #[cfg(feature = "s3")]
    assert!(completions.contains("--s3-endpoint"));
    #[cfg(not(feature = "s3"))]
    assert!(!completions.contains("--s3-"));
    #[cfg(feature = "bigquery")]
    assert!(completions.contains("--bqs-session-project"));
    #[cfg(not(feature = "bigquery"))]
    assert!(!completions.contains("--bqs-"));
}

#[cfg(feature = "local-bare-paths")]
#[tokio::test(flavor = "multi_thread")]
async fn registered_transform_uses_bound_format_and_storage_settings() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output_one = temp_dir.path().join("one.parquet");
    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&input, &batch);

    let cli = Cli::try_parse_from([
        "silk-chiffon",
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output_one.to_str().unwrap(),
        "--parquet-row-group-size",
        "2",
    ])
    .unwrap();
    let Command::Transform(command) = cli.command else {
        panic!("expected transform command");
    };
    Command::Transform(command).execute().await.unwrap();
    assert!(output_one.exists());
    assert_eq!(
        TestFile::read_parquet(&output_one)
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );

    let storage = silk_chiffon_storage::local::session().unwrap();
    let input_object = storage
        .lookup_input(&LocationInput::parse(input.to_str().unwrap()).unwrap())
        .await
        .unwrap();
    assert!(input_object.metadata().size > 0);
}

#[cfg(feature = "local-bare-paths")]
#[tokio::test]
async fn composed_inspection_invokes_the_bound_registration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input = temp_dir.path().join("input.parquet");
    TestFile::write_parquet_batch(
        &input,
        &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]),
    );
    let cli = Cli::try_parse_from([
        "silk-chiffon",
        "inspect",
        "parquet",
        input.to_str().unwrap(),
        "--format",
        "json",
    ])
    .unwrap();
    let Command::Inspect(command) = cli.command else {
        panic!("expected inspect command");
    };
    let object = command
        .storage()
        .lookup_input(&LocationInput::parse(input.to_str().unwrap()).unwrap())
        .await
        .unwrap();
    let output = command
        .inspection()
        .inspect(&object, PresentationMode::Json)
        .await
        .unwrap();
    let InspectionOutput::Json(output) = output else {
        panic!("expected JSON inspection output");
    };
    assert_eq!(output["format"], "parquet");
    assert_eq!(output["rows"], 3);
}
