#[cfg(feature = "local-bare-paths")]
use std::num::NonZeroUsize;

use clap::CommandFactory;
#[cfg(feature = "local-bare-paths")]
use silk_chiffon::utils::test_data::{TestBatch, TestFile};
use silk_chiffon::{Cli, Commands, registration};
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_core::{
    DataSourceCapabilities, InputAccess, InspectionOutput, RowCount, SinkFactoryContext,
    StreamBoundedness,
};
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_storage::LocationInput;

#[test]
fn executable_registers_formats_and_the_available_storage() {
    let formats = registration::format_registry();
    assert_eq!(
        formats
            .registrations()
            .map(|format| format.name())
            .collect::<Vec<_>>(),
        ["arrow", "parquet", "vortex"]
    );
    assert!(formats.registrations().all(|format| {
        format.has_identifier()
            && format.has_source()
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

    let Commands::Transform(command) = cli.command else {
        panic!("expected transform command");
    };
    assert_eq!(command.formats().formats().count(), 3);
    #[cfg(feature = "local-bare-paths")]
    assert!(
        command
            .storage()
            .input_handle(&LocationInput::parse("input.arrow").unwrap())
            .is_ok()
    );
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
fn registered_arguments_are_present_in_help_and_completions() {
    let mut command = Cli::command();
    let help = command
        .find_subcommand_mut("transform")
        .unwrap()
        .render_long_help()
        .to_string();
    assert!(help.contains("--arrow-record-batch-size"));
    assert!(help.contains("--parquet-row-group-size"));
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
    assert!(completions.contains("--vortex-record-batch-size"));
}

#[test]
fn format_dispatch_stays_behind_registered_capabilities() {
    let command_types = include_str!("../src/lib.rs");
    let transform = include_str!("../src/commands/transform.rs");
    let registration = include_str!("../src/registration.rs");

    assert!(!command_types.contains("enum DataFormat"));
    assert!(!transform.contains("DataFormat::"));
    assert!(!transform.contains("fn detect_format"));
    assert_eq!(transform.matches(".create_sink_factory(").count(), 1);
    assert!(!registration.contains("LazyLock"));
    assert!(!registration.contains("static FORMATS"));
    for source in [transform, registration] {
        assert!(!source.contains("downcast_ref"));
        assert!(!source.contains("dyn Any"));
    }
}

#[cfg(feature = "local-bare-paths")]
#[tokio::test(flavor = "multi_thread")]
async fn registered_capabilities_use_command_storage_and_explicit_outputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output_one = temp_dir.path().join("one.parquet");
    let output_two = temp_dir.path().join("two.parquet");
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
    let Commands::Transform(command) = cli.command else {
        panic!("expected transform command");
    };
    let input_handle = command
        .storage()
        .input_handle(&LocationInput::parse(input.to_str().unwrap()).unwrap())
        .unwrap();
    let parquet = command.formats().get("parquet").unwrap();
    let source = parquet.create_source(&input_handle).await.unwrap();
    assert_eq!(
        source.capabilities(),
        DataSourceCapabilities::new(StreamBoundedness::Finite, InputAccess::RandomAccess)
    );
    assert_eq!(source.row_count().await.unwrap(), RowCount::Exact(3));
    let schema = source.schema().await.unwrap();

    let context = SinkFactoryContext::new(NonZeroUsize::new(2).unwrap(), false, Vec::new());
    let sink_factory = parquet.create_sink_factory(&context).await.unwrap();
    for output in [&output_one, &output_two] {
        let handle = command
            .storage()
            .output_handle(&LocationInput::parse(output.to_str().unwrap()).unwrap())
            .unwrap();
        let mut sink = sink_factory
            .create(handle, std::sync::Arc::clone(&schema))
            .await
            .unwrap();
        sink.write_batch(batch.clone()).await.unwrap();
        let result = sink.finish().await.unwrap();
        assert_eq!(result.rows_written, 3);
        assert_eq!(result.files_written.len(), 1);
    }
    assert!(output_one.exists());
    assert!(output_two.exists());

    let identified = registration::format_registry()
        .get("parquet")
        .unwrap()
        .identify(&input_handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identified.format(), "parquet");
    assert_eq!(identified.variant(), None);
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
    let Commands::Inspect(command) = cli.command else {
        panic!("expected inspect command");
    };
    let handle = command
        .storage()
        .input_handle(&LocationInput::parse(input.to_str().unwrap()).unwrap())
        .unwrap();
    let output = command
        .inspection()
        .unwrap()
        .inspect(&handle)
        .await
        .unwrap();
    let InspectionOutput::Json(output) = output else {
        panic!("expected JSON inspection output");
    };
    assert_eq!(output["format"], "parquet");
    assert_eq!(output["rows"], 3);
}
