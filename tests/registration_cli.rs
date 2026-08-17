use clap::CommandFactory;
use silk_chiffon::{Cli, Command, registration};
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_core::{InspectionMode, InspectionOutput};
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_storage::LocationInput;
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_test_support::{TestBatch, TestFile};

#[test]
fn executable_registers_formats_and_the_available_storage() {
    let formats = registration::format_registry();
    assert_eq!(
        formats
            .formats()
            .map(|format| format.name())
            .collect::<Vec<_>>(),
        ["arrow", "parquet", "vortex"]
    );
    assert!(formats.formats().all(|format| {
        format.has_detector()
            && format.has_input_provider()
            && format.has_sink()
            && format.has_inspector()
    }));

    let storage = registration::storage_registry();
    #[cfg(feature = "local")]
    assert_eq!(
        storage
            .backends()
            .iter()
            .map(|backend| backend.name())
            .collect::<Vec<_>>(),
        ["local"]
    );
    #[cfg(not(feature = "local"))]
    assert!(storage.backends().is_empty());

    #[cfg(feature = "local-bare-paths")]
    assert_eq!(
        storage
            .bare_location_backend()
            .map(|backend| backend.name()),
        Some("local")
    );
    #[cfg(not(feature = "local-bare-paths"))]
    assert!(storage.bare_location_backend().is_none());
}

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
    silk_chiffon::commands::transform::run(command)
        .await
        .unwrap();
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
    let detected = registration::format_registry()
        .detect(&input_object)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detected.format(), "parquet");
    assert_eq!(detected.variant(), None);
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
        .inspect(&object, InspectionMode::Json)
        .await
        .unwrap();
    let InspectionOutput::Json(output) = output else {
        panic!("expected JSON inspection output");
    };
    assert_eq!(output["format"], "parquet");
    assert_eq!(output["rows"], 3);
}
