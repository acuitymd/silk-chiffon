use arrow::array::{Array, Int32Array, Int64Array, NullArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use camino::Utf8PathBuf;
use silk_chiffon::{
    Cli, Command, ListOutputsFormat, PartitionStrategy, PoolReserveSpec, SortColumn, SortDirection,
    SortSpec,
};
use silk_chiffon_core::QueryDialect;
use silk_chiffon_test_support::{TestBatch, TestExtract, TestFile, prepared_local_output};
use std::ffi::OsString;
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Default)]
struct TestTransformCommand {
    from: Option<String>,
    exact_references: Vec<String>,
    patterns: Vec<String>,
    allow_unmatched_patterns: bool,
    input_format: Option<String>,
    output_format: Option<String>,
    to: Option<String>,
    to_many: Option<String>,
    dialect: QueryDialect,
    exclude_columns: Vec<String>,
    query: Option<String>,
    sort_by: Option<SortSpec>,
    non_spillable_reserve: Option<PoolReserveSpec>,
    memory_pool_top_consumers: usize,
    preserve_input_order: bool,
    target_partitions: Option<usize>,
    by: Option<String>,
    partition_strategy: PartitionStrategy,
    max_open_partitions: Option<usize>,
    list_outputs: Option<ListOutputsFormat>,
    list_outputs_file: Option<Utf8PathBuf>,
    create_dirs: bool,
    overwrite: bool,
    format_args: Vec<OsString>,
}

fn transform_defaults_with<I, T>(format_args: I) -> TestTransformCommand
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    TestTransformCommand {
        memory_pool_top_consumers: 10,
        create_dirs: true,
        format_args: format_args.into_iter().map(Into::into).collect(),
        ..TestTransformCommand::default()
    }
}

fn transform_defaults() -> TestTransformCommand {
    transform_defaults_with(std::iter::empty::<OsString>())
}

async fn run_transform(command: TestTransformCommand) -> anyhow::Result<()> {
    use clap::ValueEnum;

    let mut arguments = vec![OsString::from("silk-chiffon"), OsString::from("transform")];
    macro_rules! push_value {
        ($flag:expr, $value:expr $(,)?) => {{
            arguments.push(OsString::from($flag));
            arguments.push(OsString::from($value));
        }};
    }
    if let Some(reference) = command.from {
        push_value!("--from", reference);
    }
    for reference in command.exact_references {
        push_value!("--from", reference);
    }
    for pattern in command.patterns {
        push_value!("--from-pattern", pattern);
    }
    if command.allow_unmatched_patterns {
        arguments.push(OsString::from("--allow-unmatched-patterns"));
    }
    if let Some(format) = command.input_format {
        push_value!("--input-format", format);
    }
    if let Some(format) = command.output_format {
        push_value!("--output-format", format);
    }
    if let Some(target) = command.to {
        push_value!("--to", target);
    }
    if let Some(template) = command.to_many {
        push_value!("--to-many", template);
    }
    push_value!(
        "--dialect",
        command
            .dialect
            .to_possible_value()
            .expect("every dialect has a Clap value")
            .get_name(),
    );
    for column in command.exclude_columns {
        push_value!("--exclude-columns", column);
    }
    if let Some(query) = command.query {
        push_value!("--query", query);
    }
    if let Some(sort) = command.sort_by {
        push_value!("--sort-by", sort.to_string());
    }
    if let Some(reserve) = command.non_spillable_reserve {
        let reserve = match reserve {
            PoolReserveSpec::Percent(percent) => format!("{percent}%"),
            PoolReserveSpec::Fixed(bytes) => format!("{bytes}B"),
        };
        push_value!("--non-spillable-reserve", reserve);
    }
    if command.memory_pool_top_consumers != 10 {
        push_value!(
            "--memory-pool-top-consumers",
            command.memory_pool_top_consumers.to_string(),
        );
    }
    if command.preserve_input_order {
        arguments.push(OsString::from("--preserve-input-order"));
    }
    if let Some(partitions) = command.target_partitions {
        push_value!("--target-partitions", partitions.to_string());
    }
    if let Some(fields) = command.by {
        push_value!("--by", fields);
    }
    if command.partition_strategy != PartitionStrategy::default() {
        push_value!(
            "--partition-strategy",
            command.partition_strategy.to_string()
        );
    }
    if let Some(max_open) = command.max_open_partitions {
        push_value!("--max-open-partitions", max_open.to_string());
    }
    if let Some(format) = command.list_outputs {
        push_value!("--list-outputs", format.to_string());
    }
    if let Some(path) = command.list_outputs_file {
        push_value!("--list-outputs-file", path.into_os_string());
    }
    if command.create_dirs {
        arguments.push(OsString::from("--create-dirs"));
    }
    if command.overwrite {
        arguments.push(OsString::from("--overwrite"));
    }
    arguments.extend(command.format_args);

    let Cli {
        command: Command::Transform(command),
    } = Cli::try_parse_from(arguments)?
    else {
        unreachable!()
    };
    silk_chiffon::commands::transform::run(command).await
}

mod test_helpers {
    use parquet::file::reader::FileReader;
    use silk_chiffon_test_support::parquet::{ParquetContents, read_entire_file};
    use std::path::Path;

    pub fn get_parquet_row_group_metadata(
        path: &Path,
        idx: usize,
    ) -> parquet::file::metadata::RowGroupMetaData {
        let file = std::fs::File::open(path).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        reader.metadata().row_group(idx).clone()
    }

    pub fn inspect(path: &Path) -> ParquetContents {
        read_entire_file(path).unwrap()
    }

    pub fn assert_has_dictionary(inspector: &ParquetContents, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            col.has_dictionary,
            "expected {} to have dictionary",
            col_name
        );
    }

    pub fn assert_no_dictionary(inspector: &ParquetContents, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            !col.has_dictionary,
            "expected {} to NOT have dictionary",
            col_name
        );
    }

    pub fn assert_has_bloom_filter(inspector: &ParquetContents, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            col.has_bloom_filter,
            "expected {} to have bloom filter",
            col_name
        );
    }

    pub fn assert_no_bloom_filter(inspector: &ParquetContents, col_name: &str) {
        let col = inspector.column(col_name).unwrap_or_else(|| {
            let available: Vec<_> = inspector.row_groups[0]
                .columns
                .iter()
                .map(|c| &c.name)
                .collect();
            panic!(
                "column '{}' not found. available: {:?}",
                col_name, available
            )
        });
        assert!(
            !col.has_bloom_filter,
            "expected {} to NOT have bloom filter",
            col_name
        );
    }
}

#[tokio::test]
async fn test_transform_arrow_to_arrow_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_arrow_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-compression", "snappy"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_parquet_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("arrow".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_parquet_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults_with(["--parquet-compression", "zstd"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_repeatable_from_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    run_transform(TestTransformCommand {
        from: None,
        exact_references: vec![
            input1.to_string_lossy().to_string(),
            input2.to_string_lossy().to_string(),
        ],
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
}

#[tokio::test]
async fn test_transform_from_pattern_with_glob() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("file1.arrow");
    let input2 = temp_dir.path().join("file2.arrow");
    let input3 = temp_dir.path().join("other.parquet");
    let output = temp_dir.path().join("output.arrow");

    let batch1 = TestBatch::simple_with(&[1], &["a"]);
    let batch2 = TestBatch::simple_with(&[2], &["b"]);
    let batch3 = TestBatch::simple_with(&[3], &["c"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);
    TestFile::write_parquet_batch(&input3, &batch3);

    let glob_pattern = temp_dir.path().join("file*.arrow");

    run_transform(TestTransformCommand {
        from: None,
        patterns: vec![glob_pattern.to_string_lossy().to_string()],
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_transform_to_many_partitioned() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{name}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        create_dirs: false,
        ..transform_defaults()
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.arrow");
    let output_b = temp_dir.path().join("b.arrow");

    assert!(output_a.exists());
    assert!(output_b.exists());

    let batches_a = TestFile::read_arrow_auto(&output_a);
    let batches_b = TestFile::read_arrow_auto(&output_b);

    assert_eq!(batches_a.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(batches_b.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_transform_to_many_rejects_unselected_template_field() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let batch = TestBatch::simple_with(&[1], &["a"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{missing}}.arrow");
    let result = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        ..transform_defaults()
    })
    .await;

    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("template field \"missing\" is not selected by --by")
    );
}

#[tokio::test]
async fn test_transform_to_many_rejects_malformed_template() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let batch = TestBatch::simple_with(&[1], &["a"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{name.arrow");
    let result = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        ..transform_defaults()
    })
    .await;

    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid file output template")
    );
}

#[tokio::test]
async fn test_nosort_evict_requires_direct_file_number_interpolation() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    TestFile::write_arrow_batch(&input, &TestBatch::simple_with(&[1], &["a"]));

    let result = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(
            temp_dir
                .path()
                .join("{{name}}.arrow")
                .to_string_lossy()
                .to_string(),
        ),
        by: Some("name".to_string()),
        partition_strategy: PartitionStrategy::NosortEvict,
        ..transform_defaults()
    })
    .await;

    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("must directly interpolate { file_number }")
    );
}

#[tokio::test]
async fn test_partition_targets_are_not_rewritten_after_a_session_collision() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    TestFile::write_arrow_batch(&input, &TestBatch::simple_with(&[1, 2], &["a", "b"]));

    let result = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(
            temp_dir
                .path()
                .join("same.arrow")
                .to_string_lossy()
                .to_string(),
        ),
        by: Some("name".to_string()),
        partition_strategy: PartitionStrategy::NosortMulti,
        overwrite: true,
        ..transform_defaults()
    })
    .await;

    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("already claimed by this storage session")
    );
    assert!(!temp_dir.path().join("same_1.arrow").exists());
}

#[tokio::test]
async fn test_dynamic_partition_extension_requires_explicit_format() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    TestFile::write_arrow_batch(&input, &TestBatch::simple_with(&[1], &["a"]));
    let template = temp_dir.path().join("{{name}}.{{ 'arrow' }}");

    let result = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        ..transform_defaults()
    })
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Could not detect format")
    );

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        output_format: Some("arrow".to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();
    assert!(temp_dir.path().join("a.arrow").exists());
}

#[tokio::test]
async fn test_transform_with_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        query: Some("SELECT * FROM data WHERE id > 1".to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_transform_with_sorting() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        sort_by: Some(SortSpec {
            columns: vec![silk_chiffon::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[tokio::test]
async fn test_transform_with_arrow_compression() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults_with(["--arrow-compression", "zstd"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_with_parquet_bloom_filters() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    );
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-bloom-column", "id"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
}

#[tokio::test]
async fn test_transform_with_sorted_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        sort_by: Some(SortSpec {
            columns: vec![silk_chiffon::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-sorted-metadata"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let rg_metadata = test_helpers::get_parquet_row_group_metadata(&output, 0);
    assert!(rg_metadata.sorting_columns().is_some());
    let sorting_cols = rg_metadata.sorting_columns().unwrap();
    assert_eq!(sorting_cols.len(), 1);
    assert_eq!(sorting_cols[0].column_idx, 0);
}

#[tokio::test]
async fn test_transform_partition_with_create_dirs() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let batch = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("nested/{{name}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(temp_dir.path().join("nested").exists());
    assert!(temp_dir.path().join("nested/a.arrow").exists());
    assert!(temp_dir.path().join("nested/b.arrow").exists());
}

#[tokio::test]
async fn test_transform_partition_with_overwrite() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let existing = temp_dir.path().join("a.arrow");

    let batch = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);
    TestFile::write_arrow_batch(&existing, &batch);

    let template = temp_dir.path().join("{{name}}.arrow");

    let result = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        create_dirs: false,
        ..transform_defaults()
    })
    .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        create_dirs: false,
        overwrite: true,
        ..transform_defaults()
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_transform_from_pattern_empty_glob() {
    let temp_dir = TempDir::new().unwrap();
    let output = temp_dir.path().join("output.arrow");

    let glob_pattern = temp_dir.path().join("nonexistent*.arrow");

    let result = run_transform(TestTransformCommand {
        from: None,
        patterns: vec![glob_pattern.to_string_lossy().to_string()],
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults()
    })
    .await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("matched no locations")
    );
}

#[tokio::test]
async fn test_transform_combines_exact_input_with_an_allowed_unmatched_pattern() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");
    TestFile::write_arrow_batch(&input, &TestBatch::simple_with(&[1, 2], &["a", "b"]));

    run_transform(TestTransformCommand {
        exact_references: vec![input.to_string_lossy().into_owned()],
        patterns: vec![
            temp_dir
                .path()
                .join("missing-*.arrow")
                .to_string_lossy()
                .into_owned(),
        ],
        allow_unmatched_patterns: true,
        to: Some(output.to_string_lossy().into_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
}

#[tokio::test]
async fn test_transform_rejects_an_unmatched_pattern_even_with_an_exact_input_by_default() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    TestFile::write_arrow_batch(&input, &TestBatch::simple_with(&[1, 2], &["a", "b"]));

    let result = run_transform(TestTransformCommand {
        exact_references: vec![input.to_string_lossy().into_owned()],
        patterns: vec![
            temp_dir
                .path()
                .join("missing-*.arrow")
                .to_string_lossy()
                .into_owned(),
        ],
        to: Some(
            temp_dir
                .path()
                .join("output.arrow")
                .to_string_lossy()
                .into_owned(),
        ),
        ..transform_defaults()
    })
    .await;

    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("matched no locations")
    );
}

#[tokio::test]
async fn test_transform_rejects_an_allowed_unmatched_pattern_without_another_source() {
    let temp_dir = TempDir::new().unwrap();
    let result = run_transform(TestTransformCommand {
        patterns: vec![
            temp_dir
                .path()
                .join("missing-*.arrow")
                .to_string_lossy()
                .into_owned(),
        ],
        allow_unmatched_patterns: true,
        to: Some(
            temp_dir
                .path()
                .join("output.arrow")
                .to_string_lossy()
                .into_owned(),
        ),
        ..transform_defaults()
    })
    .await;

    assert!(result.unwrap_err().to_string().contains("no inputs"));
}

#[tokio::test]
async fn test_transform_partition_exclude_columns() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let batch = TestBatch::simple_with(&[1, 2], &["a", "a"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{name}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec!["name".to_string()],
        create_dirs: false,
        ..transform_defaults()
    })
    .await
    .unwrap();

    let output = temp_dir.path().join("a.arrow");
    assert!(output.exists());

    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "id");
}

#[tokio::test]
async fn test_transform_with_projection_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        query: Some("SELECT id FROM data".to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "id");
}

#[tokio::test]
async fn test_transform_with_aggregation_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        query: Some("SELECT COUNT(*) as count FROM data".to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches[0].num_rows(), 1);
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 3);
}

#[tokio::test]
async fn test_transform_query_and_sort_combined() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        query: Some("SELECT * FROM data WHERE id > 1".to_string()),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids.value(0), 2);
    assert_eq!(ids.value(1), 3);
}

#[tokio::test]
async fn test_transform_multi_column_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "B", "B"])),
            Arc::new(Int32Array::from(vec![3, 1, 2, 4])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let output = temp_dir.path().join("output.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        sort_by: Some(SortSpec {
            columns: vec![
                SortColumn {
                    name: "category".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "value".to_string(),
                    direction: SortDirection::Ascending,
                },
            ],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    let categories = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(categories.value(0), "A");
    assert_eq!(values.value(0), 1);
    assert_eq!(categories.value(1), "A");
    assert_eq!(values.value(1), 3);
    assert_eq!(categories.value(2), "B");
    assert_eq!(values.value(2), 2);
    assert_eq!(categories.value(3), "B");
    assert_eq!(values.value(3), 4);
}

#[tokio::test]
async fn test_transform_sort_descending() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Descending,
            }],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let mut all_ids = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..ids.len() {
            all_ids.push(ids.value(i));
        }
    }

    let mut sorted = all_ids.clone();
    sorted.sort();
    sorted.reverse();
    assert_eq!(all_ids, sorted);
}

#[tokio::test]
async fn test_transform_parquet_compression_gzip() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-compression", "gzip"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_parquet_compression_lz4() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-compression", "lz4"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_parquet_bloom_all() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-bloom-all"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-statistics", "chunk"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_writer_version() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-writer-version", "v1"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_dictionary_all_off() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-dictionary-all-off"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_dictionary_column_off() {
    // dictionary globally enabled (default), but disabled for specific column
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-dictionary-column-off", "id"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_parquet_dictionary_column() {
    // dictionary globally disabled, but enabled for specific column
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with([
            "--parquet-dictionary-all-off",
            "--parquet-dictionary-column",
            "name:always",
        ])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_transform_arrow_format_stream() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults_with(["--arrow-format", "stream"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_reads_arrow_stream_input_incrementally() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batches = [
        TestBatch::simple_with(&[1, 2], &["a", "b"]),
        TestBatch::simple_with(&[3], &["c"]),
    ];
    TestFile::write_arrow_stream(&input, &batches);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert_eq!(
        TestFile::read_parquet(&output)
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
}

#[tokio::test]
async fn arrow_stream_projection_and_limit_execute_in_the_custom_opener() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");
    TestFile::write_arrow_stream(
        &input,
        &[
            TestBatch::simple_with(&[1, 2], &["a", "b"]),
            TestBatch::simple_with(&[3, 4], &["c", "d"]),
        ],
    );

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().into_owned()),
        to: Some(output.to_string_lossy().into_owned()),
        query: Some("SELECT name FROM data ORDER BY id LIMIT 3".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(batches[0].schema().fields().len(), 1);
    assert_eq!(TestExtract::string_all(&batches, "name"), ["a", "b", "c"]);
}

#[tokio::test]
async fn arrow_stream_exact_statistics_count_every_zero_body_batch() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");
    let schema = Arc::new(Schema::new(vec![Field::new("empty", DataType::Null, true)]));
    let batches = [
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(NullArray::new(2))]).unwrap(),
        RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(3))]).unwrap(),
    ];
    TestFile::write_arrow_stream(&input, &batches);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().into_owned()),
        to: Some(output.to_string_lossy().into_owned()),
        query: Some("SELECT COUNT(*) AS row_count FROM data".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(TestExtract::i64(&batches[0], "row_count"), [5]);
}

#[tokio::test]
async fn multiple_arrow_streams_repartition_only_at_file_boundaries() {
    let temp_dir = TempDir::new().unwrap();
    let first = temp_dir.path().join("first.arrow");
    let second = temp_dir.path().join("second.arrow");
    let output = temp_dir.path().join("output.arrow");
    TestFile::write_arrow_stream(
        &first,
        &[
            TestBatch::simple_with(&[1, 2], &["a", "b"]),
            TestBatch::simple_with(&[3], &["c"]),
        ],
    );
    TestFile::write_arrow_stream(
        &second,
        &[
            TestBatch::simple_with(&[10], &["j"]),
            TestBatch::simple_with(&[11, 12], &["k", "l"]),
        ],
    );

    run_transform(TestTransformCommand {
        patterns: vec![format!("{}/*.arrow", temp_dir.path().display())],
        to: Some(output.to_string_lossy().into_owned()),
        target_partitions: Some(4),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let mut ids = TestExtract::i32_all(&TestFile::read_arrow(&output), "id");
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3, 10, 11, 12]);
}

#[tokio::test]
async fn one_pattern_groups_arrow_file_and_stream_variants_separately() {
    let temp_dir = TempDir::new().unwrap();
    let file = temp_dir.path().join("file.arrow");
    let stream = temp_dir.path().join("stream.arrow");
    let output = temp_dir.path().join("output.arrow");
    TestFile::write_arrow_batch(&file, &TestBatch::simple_with(&[1, 2], &["a", "b"]));
    TestFile::write_arrow_stream(&stream, &[TestBatch::simple_with(&[3, 4], &["c", "d"])]);

    run_transform(TestTransformCommand {
        patterns: vec![format!("{}/*.arrow", temp_dir.path().display())],
        to: Some(output.to_string_lossy().into_owned()),
        target_partitions: Some(3),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let mut ids = TestExtract::i32_all(&TestFile::read_arrow(&output), "id");
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3, 4]);
}

#[tokio::test]
async fn arrow_file_checks_each_nonrepresentative_schema_before_rows() {
    let temp_dir = TempDir::new().unwrap();
    let mismatched = temp_dir.path().join("a.arrow");
    let representative = temp_dir.path().join("z.arrow");
    let output = temp_dir.path().join("output.parquet");
    let mismatched_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();
    TestFile::write_arrow_batch(&mismatched, &mismatched_batch);
    let ids = (0..1_000).collect::<Vec<_>>();
    let names = (0..1_000)
        .map(|index| format!("representative-{index:04}-with-padding"))
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    TestFile::write_arrow_batch(&representative, &TestBatch::simple_with(&ids, &names));

    let error = run_transform(TestTransformCommand {
        patterns: vec![format!("{}/*.arrow", temp_dir.path().display())],
        to: Some(output.to_string_lossy().into_owned()),
        query: Some("SELECT * FROM data LIMIT 1".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("a.arrow"), "{message}");
    assert!(message.contains("schema mismatch"), "{message}");
    assert!(!message.contains("__silk_input"), "{message}");
}

#[tokio::test]
async fn arrow_stream_checks_each_file_schema_before_yielding_its_first_batch() {
    let temp_dir = TempDir::new().unwrap();
    let mismatched = temp_dir.path().join("a.arrow");
    let representative = temp_dir.path().join("z.arrow");
    let output = temp_dir.path().join("output.parquet");
    let mismatched_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();
    TestFile::write_arrow_stream(&mismatched, &[mismatched_batch]);
    let ids = (0..1_000).collect::<Vec<_>>();
    let names = (0..1_000)
        .map(|index| format!("representative-{index:04}-with-padding"))
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    let representative_batch = TestBatch::simple_with(&ids, &names);
    TestFile::write_arrow_stream(&representative, &[representative_batch]);

    let error = run_transform(TestTransformCommand {
        patterns: vec![format!("{}/*.arrow", temp_dir.path().display())],
        to: Some(output.to_string_lossy().to_string()),
        query: Some("SELECT * FROM data".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("a.arrow"), "{message}");
    assert!(message.contains("schema mismatch"), "{message}");
    assert!(!message.contains("__silk_input"), "{message}");
}

#[tokio::test]
async fn empty_vortex_files_still_participate_in_leaf_schema_validation() {
    use silk_chiffon::sinks::{
        data_sink::DataSink,
        vortex::{VortexSink, VortexSinkOptions},
    };

    let temp_dir = TempDir::new().unwrap();
    let mismatched = temp_dir.path().join("a.vortex");
    let representative = temp_dir.path().join("z.vortex");
    let output = temp_dir.path().join("output.arrow");
    let mismatched_schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int32,
        false,
    )]));
    let sink = VortexSink::create(
        prepared_local_output(&mismatched),
        &mismatched_schema,
        VortexSinkOptions::new(),
    )
    .unwrap();
    Box::new(sink).finish().await.unwrap();
    let representative_batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    let mut sink = VortexSink::create(
        prepared_local_output(&representative),
        &representative_batch.schema(),
        VortexSinkOptions::new(),
    )
    .unwrap();
    sink.write_batch(representative_batch).await.unwrap();
    Box::new(sink).finish().await.unwrap();

    let error = run_transform(TestTransformCommand {
        patterns: vec![format!("{}/*.vortex", temp_dir.path().display())],
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("a.vortex"), "{message}");
    assert!(message.contains("schema does not match"), "{message}");
    assert!(!message.contains("__silk_input"), "{message}");
    assert!(!output.exists());
}

#[tokio::test]
async fn test_transform_arrow_record_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults_with(["--arrow-record-batch-size", "1000"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_transform_parquet_row_group_size() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-row-group-size", "1000"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_transform_partition_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{name}}.parquet");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        create_dirs: false,
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-compression", "snappy"])
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.parquet");
    let output_b = temp_dir.path().join("b.parquet");

    assert!(output_a.exists());
    assert!(output_b.exists());

    let batches_a = TestFile::read_parquet(&output_a);
    let batches_b = TestFile::read_parquet(&output_b);

    assert_eq!(batches_a.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(batches_b.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_transform_low_cardinality_partition() {
    // test low-cardinality partitioning which doesn't require sorted input
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    // create unsorted data - rows are interleaved by partition value
    let schema = TestBatch::simple_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "b"])), // unsorted by name
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{name}}.parquet");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        partition_strategy: PartitionStrategy::NosortMulti,
        create_dirs: false,
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.parquet");
    let output_b = temp_dir.path().join("b.parquet");

    assert!(output_a.exists());
    assert!(output_b.exists());

    let batches_a = TestFile::read_parquet(&output_a);
    let batches_b = TestFile::read_parquet(&output_b);

    // each partition should have 2 rows
    assert_eq!(batches_a.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(batches_b.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // verify the data is correct (ids 1,3 for "a" and ids 2,4 for "b")
    let id_col_a = batches_a[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let id_col_b = batches_b[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let mut ids_a: Vec<i32> = (0..id_col_a.len()).map(|i| id_col_a.value(i)).collect();
    let mut ids_b: Vec<i32> = (0..id_col_b.len()).map(|i| id_col_b.value(i)).collect();
    ids_a.sort();
    ids_b.sort();

    assert_eq!(ids_a, vec![1, 3]);
    assert_eq!(ids_b, vec![2, 4]);
}

#[tokio::test]
async fn test_transform_multi_column_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2023, 2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 2, 1])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("year={{year}}/month={{month}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(temp_dir.path().join("year=2023/month=1.arrow").exists());
    assert!(temp_dir.path().join("year=2023/month=2.arrow").exists());
    assert!(temp_dir.path().join("year=2024/month=1.arrow").exists());
}

#[tokio::test]
async fn test_transform_repeatable_from_to_partitioned() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["a", "c"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    let _ = std::fs::remove_file(temp_dir.path().join("a.arrow"));
    let _ = std::fs::remove_file(temp_dir.path().join("b.arrow"));
    let _ = std::fs::remove_file(temp_dir.path().join("c.arrow"));

    let template = temp_dir.path().join("{{name}}.arrow");

    run_transform(TestTransformCommand {
        from: None,
        exact_references: vec![
            input1.to_string_lossy().to_string(),
            input2.to_string_lossy().to_string(),
        ],
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        create_dirs: false,
        overwrite: true,
        ..transform_defaults()
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.arrow");
    let output_b = temp_dir.path().join("b.arrow");
    let output_c = temp_dir.path().join("c.arrow");

    assert!(output_a.exists());
    assert!(output_b.exists());
    assert!(output_c.exists());

    let batches_a = TestFile::read_arrow_auto(&output_a);
    let batches_b = TestFile::read_arrow_auto(&output_b);
    let batches_c = TestFile::read_arrow_auto(&output_c);

    let rows_a: usize = batches_a.iter().map(|b| b.num_rows()).sum();
    let rows_b: usize = batches_b.iter().map(|b| b.num_rows()).sum();
    let rows_c: usize = batches_c.iter().map(|b| b.num_rows()).sum();
    let total_rows = rows_a + rows_b + rows_c;

    assert!(rows_a >= 1, "partition 'a' should have at least 1 row");
    assert_eq!(rows_b, 1, "partition 'b' should have 1 row");
    assert_eq!(rows_c, 1, "partition 'c' should have 1 row");
    assert!(
        total_rows >= 3,
        "total rows should be at least 3, got {}",
        total_rows
    );
    assert!(
        total_rows <= 4,
        "total rows should be at most 4, got {}",
        total_rows
    );
}

#[tokio::test]
async fn test_transform_invalid_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    let result = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        query: Some("SELECT nonexistent FROM data".to_string()),
        ..transform_defaults()
    })
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_transform_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = TestBatch::simple_schema();
    TestFile::write_arrow_empty(&input, &schema);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn test_transform_bloom_filter_with_custom_ndv() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-bloom-all", "fpp=0.005,ndv=1000"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches[0].num_rows(), 5);
}

#[tokio::test]
async fn test_transform_bloom_filter_column_specific_with_ndv() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-bloom-column", "id:fpp=0.005,ndv=5000"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_transform_mixed_parquet_and_arrow_inputs() {
    let temp_dir = TempDir::new().unwrap();
    let arrow_input1 = temp_dir.path().join("data1.arrow");
    let arrow_input2 = temp_dir.path().join("data2.arrow");
    let parquet_input = temp_dir.path().join("data3.parquet");
    let output = temp_dir.path().join("output.parquet");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
    let batch3 = TestBatch::simple_with(&[5, 6], &["e", "f"]);
    TestFile::write_arrow_batch(&arrow_input1, &batch1);
    TestFile::write_arrow_batch(&arrow_input2, &batch2);
    TestFile::write_parquet_batch(&parquet_input, &batch3);

    let glob_pattern = temp_dir.path().join("data*.arrow");

    run_transform(TestTransformCommand {
        from: None,
        patterns: vec![glob_pattern.to_string_lossy().to_string()],
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-compression", "snappy"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
}

#[tokio::test]
async fn different_input_leaves_union_reordered_and_missing_columns_by_name() {
    let temp_dir = TempDir::new().unwrap();
    let first = temp_dir.path().join("first.arrow");
    let second = temp_dir.path().join("second.arrow");
    let output = temp_dir.path().join("output.arrow");
    let first_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("left", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let second_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("right", DataType::Utf8, false),
            Field::new("id", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["z"])),
            Arc::new(Int32Array::from(vec![3])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&first, &first_batch);
    TestFile::write_arrow_batch(&second, &second_batch);

    run_transform(TestTransformCommand {
        exact_references: vec![
            first.to_string_lossy().into_owned(),
            second.to_string_lossy().into_owned(),
        ],
        to: Some(output.to_string_lossy().into_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let batches = TestFile::read_arrow(&output);
    let names = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(names, ["id", "left", "right"]);
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let ids = TestExtract::i32(batch, "id");
            let left = TestExtract::string_nullable(batch, "left");
            let right = TestExtract::string_nullable(batch, "right");
            ids.into_iter()
                .zip(left)
                .zip(right)
                .map(|((id, left), right)| (id, left, right))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        rows,
        [
            (1, Some("a".to_owned()), None),
            (2, Some("b".to_owned()), None),
            (3, None, Some("z".to_owned())),
        ]
    );
}

#[tokio::test]
async fn parquet_pattern_rejects_a_nonrepresentative_schema_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let expected = temp_dir.path().join("a.parquet");
    let mismatched = temp_dir.path().join("b.parquet");
    let output = temp_dir.path().join("output.arrow");
    TestFile::write_parquet_batch(
        &expected,
        &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]),
    );
    let mismatched_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![9]))],
    )
    .unwrap();
    TestFile::write_parquet_batch(&mismatched, &mismatched_batch);

    let error = run_transform(TestTransformCommand {
        patterns: vec![format!("{}/*.parquet", temp_dir.path().display())],
        to: Some(output.to_string_lossy().into_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("schema does not match"), "{message}");
    assert!(message.contains(".parquet"), "{message}");
    assert!(!message.contains("__silk_input"), "{message}");
    assert!(!output.exists());
}

#[tokio::test]
async fn test_transform_partition_list_outputs_text() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{name}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        list_outputs: Some(ListOutputsFormat::Text),
        create_dirs: false,
        ..transform_defaults()
    })
    .await
    .unwrap();

    let output_a = temp_dir.path().join("a.arrow");
    let output_b = temp_dir.path().join("b.arrow");

    assert!(output_a.exists());
    assert!(output_b.exists());
}

#[tokio::test]
async fn test_transform_partition_list_outputs_json() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let report = temp_dir.path().join("outputs.json");

    let batch = TestBatch::simple_with(&[1, 2], &["x", "y"]);
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{name}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("name".to_string()),
        exclude_columns: vec!["name".to_string()],
        list_outputs: Some(ListOutputsFormat::Json),
        list_outputs_file: Some(Utf8PathBuf::from_path_buf(report.clone()).unwrap()),
        create_dirs: false,
        ..transform_defaults()
    })
    .await
    .unwrap();

    let output_x = temp_dir.path().join("x.arrow");
    let output_y = temp_dir.path().join("y.arrow");

    assert!(output_x.exists());
    assert!(output_y.exists());
    assert_eq!(TestFile::read_arrow(&output_x)[0].num_columns(), 1);

    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(report).unwrap()).unwrap();
    let outputs = report.as_array().unwrap();
    assert_eq!(outputs.len(), 2);
    for output in outputs {
        assert_eq!(output["partition_fields"][0]["field"], "name");
        assert!(output["partition_fields"][0]["value"].is_string());
    }
}

#[tokio::test]
async fn test_transform_partition_failure_writes_completed_outputs_only() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let report = temp_dir.path().join("outputs.json");
    let target = temp_dir.path().join("shared.arrow");

    TestFile::write_arrow_batch(&input, &TestBatch::simple_with(&[1, 2], &["a", "b"]));

    let error = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().into_owned()),
        to_many: Some(target.to_string_lossy().into_owned()),
        by: Some("name".to_owned()),
        partition_strategy: PartitionStrategy::SortSingle,
        list_outputs: Some(ListOutputsFormat::Json),
        list_outputs_file: Some(Utf8PathBuf::from_path_buf(report.clone()).unwrap()),
        create_dirs: false,
        ..transform_defaults()
    })
    .await
    .unwrap_err();

    assert!(
        format!("{error:#}").contains("already claimed by this storage session"),
        "{error:#}"
    );
    let outputs: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(report).unwrap()).unwrap();
    let outputs = outputs.as_array().unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0]["partition_fields"][0]["value"], "a");
    let target_url = url::Url::from_file_path(&target).unwrap();
    assert_eq!(outputs[0]["durable_locations"][0], target_url.as_str());
}

#[tokio::test]
async fn test_transform_explicit_input_format_arrow_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        input_format: Some("arrow".to_owned()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn explicit_input_formats_use_the_selected_formats_detector() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");
    TestFile::write_arrow_batch(
        &input,
        &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]),
    );

    let error = run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        input_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap_err();

    assert!(error.to_string().contains("is not recognized as parquet"));
    assert!(!output.exists());
}

#[tokio::test]
async fn test_transform_explicit_output_format_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        input_format: Some("parquet".to_owned()),
        output_format: Some("arrow".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_transform_arrow_compression_lz4() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        ..transform_defaults_with(["--arrow-compression", "lz4"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let file_size = std::fs::metadata(&output).unwrap().len();
    assert!(file_size > 0);
}

#[tokio::test]
async fn test_transform_query_with_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{category}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("category".to_string()),
        create_dirs: false,
        overwrite: true,
        query: Some("SELECT * FROM data WHERE value > 15".to_string()),
        ..transform_defaults()
    })
    .await
    .unwrap_or_else(|e| panic!("Command failed with error: {:?}", e));

    let output_a = temp_dir.path().join("A.arrow");
    let output_b = temp_dir.path().join("B.arrow");

    let has_a = output_a.exists();
    let has_b = output_b.exists();

    assert!(has_a || has_b, "At least one partition file should exist");

    let mut total_rows = 0;
    if has_a {
        let batches_a = TestFile::read_arrow_auto(&output_a);
        total_rows += batches_a.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    if has_b {
        let batches_b = TestFile::read_arrow_auto(&output_b);
        total_rows += batches_b.iter().map(|b| b.num_rows()).sum::<usize>();
    }

    assert!(
        total_rows >= 2,
        "Expected at least 2 rows total after filtering, got {}",
        total_rows
    );
}

#[tokio::test]
async fn test_transform_query_with_different_dialect() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        query: Some("SELECT * FROM data WHERE id >= 2".to_string()),
        dialect: QueryDialect::PostgreSQL,
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_transform_partition_with_query_and_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![5, 3, 8, 1, 6, 2, 9])),
            Arc::new(StringArray::from(vec![
                "US", "EU", "US", "EU", "US", "EU", "US",
            ])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 50, 75, 300, 125])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("{{region}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("region".to_string()),
        create_dirs: false,
        query: Some("SELECT * FROM data WHERE score > 100".to_string()),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "score".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let output_us = temp_dir.path().join("US.arrow");
    let output_eu = temp_dir.path().join("EU.arrow");

    assert!(output_us.exists());
    assert!(output_eu.exists());

    let batches_us = TestFile::read_arrow_auto(&output_us);
    let batches_eu = TestFile::read_arrow_auto(&output_eu);

    let us_rows: usize = batches_us.iter().map(|b| b.num_rows()).sum();
    let eu_rows: usize = batches_eu.iter().map(|b| b.num_rows()).sum();

    assert_eq!(us_rows, 2);
    assert_eq!(eu_rows, 2);
    let mut us_scores_vec = Vec::new();
    for batch in batches_us {
        if let Some(score_col) = batch.column_by_name("score") {
            let scores = score_col.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..scores.len() {
                us_scores_vec.push(scores.value(i));
            }
        }
    }
    us_scores_vec.sort();
    assert_eq!(us_scores_vec, vec![125, 150]);

    let mut eu_scores_vec = Vec::new();
    for batch in batches_eu {
        if let Some(score_col) = batch.column_by_name("score") {
            let scores = score_col.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..scores.len() {
                eu_scores_vec.push(scores.value(i));
            }
        }
    }
    eu_scores_vec.sort();
    assert_eq!(eu_scores_vec, vec![200, 300]);
}

/// round-trip test: arrow -> parquet -> arrow, verify data is identical
#[tokio::test]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
async fn test_parquet_roundtrip_data_fidelity() {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int64Array,
        LargeStringArray, StringViewArray, TimestampMicrosecondArray, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, TimeUnit};

    let temp_dir = TempDir::new().unwrap();
    let input_arrow = temp_dir.path().join("input.arrow");
    let intermediate_parquet = temp_dir.path().join("intermediate.parquet");
    let output_arrow = temp_dir.path().join("output.arrow");

    // create a schema with various data types to test round-trip fidelity
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("int8_col", DataType::Int8, false),
        Field::new("int16_col", DataType::Int16, false),
        Field::new("int64_col", DataType::Int64, false),
        Field::new("uint32_col", DataType::UInt32, false),
        Field::new("float32_col", DataType::Float32, false),
        Field::new("float64_col", DataType::Float64, false),
        Field::new("bool_col", DataType::Boolean, false),
        Field::new("string_col", DataType::Utf8, false),
        Field::new("nullable_int", DataType::Int32, true),
        Field::new("nullable_string", DataType::Utf8, true),
        Field::new(
            "timestamp_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    // generate a larger dataset (10k rows) split into multiple batches
    let num_rows: i32 = 10_000;
    let input_batch_size: i32 = 1_000; // 10 input batches
    let parquet_row_group_size: usize = 2_000; // 5 row groups
    let output_batch_size: usize = 1_500; // ~7 output batches

    // helper to create a batch for a range of rows
    let make_batch = |start: i32, end: i32| -> RecordBatch {
        let ids: Vec<i32> = (start..end).collect();
        let int8_vals: Vec<i8> = (start..end).map(|i| (i % 128) as i8).collect();
        let int16_vals: Vec<i16> = (start..end).map(|i| (i % 32768) as i16).collect();
        let int64_vals: Vec<i64> = (start..end).map(|i| i64::from(i) * 1_000_000).collect();
        let uint32_vals: Vec<u32> = (start..end).map(|i| i as u32 * 2).collect();
        let float32_vals: Vec<f32> = (start..end).map(|i| i as f32 * 0.5).collect();
        let float64_vals: Vec<f64> = (start..end).map(|i| f64::from(i) * 1.5).collect();
        let bool_vals: Vec<bool> = (start..end).map(|i| i % 2 == 0).collect();
        let string_vals: Vec<String> = (start..end).map(|i| format!("row_{i}")).collect();
        let nullable_int_vals: Vec<Option<i32>> = (start..end)
            .map(|i| if i % 3 == 0 { None } else { Some(i) })
            .collect();
        let nullable_string_vals: Vec<Option<String>> = (start..end)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(format!("nullable_{i}"))
                }
            })
            .collect();
        let timestamp_vals: Vec<i64> = (start..end).map(|i| i64::from(i) * 1_000_000).collect();

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int8Array::from(int8_vals)),
                Arc::new(Int16Array::from(int16_vals)),
                Arc::new(Int64Array::from(int64_vals)),
                Arc::new(UInt32Array::from(uint32_vals)),
                Arc::new(Float32Array::from(float32_vals)),
                Arc::new(Float64Array::from(float64_vals)),
                Arc::new(BooleanArray::from(bool_vals)),
                Arc::new(StringArray::from(
                    string_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(nullable_int_vals)),
                Arc::new(StringArray::from(
                    nullable_string_vals
                        .iter()
                        .map(|s| s.as_deref())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(timestamp_vals)),
            ],
        )
        .unwrap()
    };

    // create multiple input batches
    let input_batches_to_write: Vec<RecordBatch> = (0..num_rows)
        .step_by(input_batch_size as usize)
        .map(|start| make_batch(start, (start + input_batch_size).min(num_rows)))
        .collect();

    assert_eq!(
        input_batches_to_write.len(),
        10,
        "should have 10 input batches"
    );

    // write the input arrow file with multiple batches
    TestFile::write_arrow(&input_arrow, &input_batches_to_write);

    // step 1: convert arrow to parquet with multiple row groups
    run_transform(TestTransformCommand {
        from: Some(input_arrow.to_string_lossy().to_string()),
        to: Some(intermediate_parquet.to_string_lossy().to_string()),
        preserve_input_order: true,
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(vec![
            "--parquet-compression".to_owned(),
            "zstd".to_owned(),
            "--parquet-row-group-size".to_owned(),
            parquet_row_group_size.to_string(),
        ])
    })
    .await
    .unwrap();

    assert!(intermediate_parquet.exists());

    // verify parquet has multiple row groups
    let parquet_file = std::fs::File::open(&intermediate_parquet).unwrap();
    let parquet_reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(parquet_file)
            .unwrap();
    let num_row_groups = parquet_reader.metadata().num_row_groups();
    assert!(
        num_row_groups >= 5,
        "should have at least 5 row groups, got {num_row_groups}"
    );

    // step 2: convert parquet back to arrow with specified batch size
    run_transform(TestTransformCommand {
        from: Some(intermediate_parquet.to_string_lossy().to_string()),
        to: Some(output_arrow.to_string_lossy().to_string()),
        preserve_input_order: true,
        input_format: Some("parquet".to_owned()),
        output_format: Some("arrow".to_owned()),
        ..transform_defaults_with(vec![
            "--arrow-record-batch-size".to_owned(),
            output_batch_size.to_string(),
        ])
    })
    .await
    .unwrap();

    assert!(output_arrow.exists());

    // step 3: read both files and compare directly
    let input_batches = TestFile::read_arrow_auto(&input_arrow);
    let output_batches = TestFile::read_arrow_auto(&output_arrow);

    // verify we have multiple batches in both files
    assert!(
        input_batches.len() >= 10,
        "input should have at least 10 batches, got {}",
        input_batches.len()
    );
    assert!(
        output_batches.len() >= 6,
        "output should have at least 6 batches, got {}",
        output_batches.len()
    );

    // verify row counts match
    let input_rows: usize = input_batches.iter().map(|b| b.num_rows()).sum();
    let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(input_rows, output_rows, "row counts should match");
    assert_eq!(input_rows, num_rows as usize);

    // helper to extract string values handling different arrow string types
    fn extract_strings(batches: &[RecordBatch], col_name: &str) -> Vec<Option<String>> {
        let mut result = Vec::new();
        for batch in batches {
            let col = batch.column_by_name(col_name).unwrap();
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        result.push(Some(arr.value(i).to_string()));
                    }
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        result.push(Some(arr.value(i).to_string()));
                    }
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        result.push(Some(arr.value(i).to_string()));
                    }
                }
            } else {
                panic!("{} unexpected type: {:?}", col_name, col.data_type());
            }
        }
        result
    }

    // extract values from input file
    let mut input_ids = Vec::new();
    let mut input_int8s = Vec::new();
    let mut input_int16s = Vec::new();
    let mut input_int64s = Vec::new();
    let mut input_uint32s = Vec::new();
    let mut input_float32s = Vec::new();
    let mut input_float64s = Vec::new();
    let mut input_bools = Vec::new();
    let mut input_nullable_ints = Vec::new();
    let mut input_timestamps = Vec::new();

    for batch in &input_batches {
        let col = batch.column_by_name("id").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        input_ids.extend(arr.iter());

        let col = batch.column_by_name("int8_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
        input_int8s.extend(arr.iter());

        let col = batch.column_by_name("int16_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
        input_int16s.extend(arr.iter());

        let col = batch.column_by_name("int64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        input_int64s.extend(arr.iter());

        let col = batch.column_by_name("uint32_col").unwrap();
        let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
        input_uint32s.extend(arr.iter());

        let col = batch.column_by_name("float32_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
        input_float32s.extend(arr.iter());

        let col = batch.column_by_name("float64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        input_float64s.extend(arr.iter());

        let col = batch.column_by_name("bool_col").unwrap();
        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        input_bools.extend(arr.iter());

        let col = batch.column_by_name("nullable_int").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        input_nullable_ints.extend(arr.iter());

        let col = batch.column_by_name("timestamp_col").unwrap();
        let arr = col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        input_timestamps.extend(arr.iter());
    }

    let input_strings = extract_strings(&input_batches, "string_col");
    let input_nullable_strings = extract_strings(&input_batches, "nullable_string");

    // extract values from output file
    let mut output_ids = Vec::new();
    let mut output_int8s = Vec::new();
    let mut output_int16s = Vec::new();
    let mut output_int64s = Vec::new();
    let mut output_uint32s = Vec::new();
    let mut output_float32s = Vec::new();
    let mut output_float64s = Vec::new();
    let mut output_bools = Vec::new();
    let mut output_nullable_ints = Vec::new();
    let mut output_timestamps = Vec::new();

    for batch in &output_batches {
        let col = batch.column_by_name("id").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        output_ids.extend(arr.iter());

        let col = batch.column_by_name("int8_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
        output_int8s.extend(arr.iter());

        let col = batch.column_by_name("int16_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
        output_int16s.extend(arr.iter());

        let col = batch.column_by_name("int64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        output_int64s.extend(arr.iter());

        let col = batch.column_by_name("uint32_col").unwrap();
        let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
        output_uint32s.extend(arr.iter());

        let col = batch.column_by_name("float32_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
        output_float32s.extend(arr.iter());

        let col = batch.column_by_name("float64_col").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        output_float64s.extend(arr.iter());

        let col = batch.column_by_name("bool_col").unwrap();
        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        output_bools.extend(arr.iter());

        let col = batch.column_by_name("nullable_int").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        output_nullable_ints.extend(arr.iter());

        let col = batch.column_by_name("timestamp_col").unwrap();
        let arr = col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        output_timestamps.extend(arr.iter());
    }

    let output_strings = extract_strings(&output_batches, "string_col");
    let output_nullable_strings = extract_strings(&output_batches, "nullable_string");

    // compare input vs output directly
    assert_eq!(input_ids, output_ids, "id values should match");
    assert_eq!(input_int8s, output_int8s, "int8 values should match");
    assert_eq!(input_int16s, output_int16s, "int16 values should match");
    assert_eq!(input_int64s, output_int64s, "int64 values should match");
    assert_eq!(input_uint32s, output_uint32s, "uint32 values should match");
    assert_eq!(
        input_float32s, output_float32s,
        "float32 values should match"
    );
    assert_eq!(
        input_float64s, output_float64s,
        "float64 values should match"
    );
    assert_eq!(input_bools, output_bools, "bool values should match");
    assert_eq!(input_strings, output_strings, "string values should match");
    assert_eq!(
        input_nullable_ints, output_nullable_ints,
        "nullable int values should match"
    );
    assert_eq!(
        input_nullable_strings, output_nullable_strings,
        "nullable string values should match"
    );
    assert_eq!(
        input_timestamps, output_timestamps,
        "timestamp values should match"
    );
}

/// One-off helper to verify all rows in a batch have the expected partition values.
/// This is entirely specific to the test data and not generalized at all.
fn verify_int32_partition_values(
    batches: &[RecordBatch],
    expected_year: i32,
    expected_month: i32,
    file_path: &str,
) {
    for batch in batches {
        let year_col = batch.column_by_name("year").unwrap();
        let month_col = batch.column_by_name("month").unwrap();
        let years = year_col.as_any().downcast_ref::<Int32Array>().unwrap();
        let months = month_col.as_any().downcast_ref::<Int32Array>().unwrap();

        for i in 0..batch.num_rows() {
            assert_eq!(
                years.value(i),
                expected_year,
                "Row {} in {} has wrong year: expected {}, got {}",
                i,
                file_path,
                expected_year,
                years.value(i)
            );
            assert_eq!(
                months.value(i),
                expected_month,
                "Row {} in {} has wrong month: expected {}, got {}",
                i,
                file_path,
                expected_month,
                months.value(i)
            );
        }
    }
}

fn verify_string_partition_values(
    batches: &[RecordBatch],
    expected_region: &str,
    expected_year: i32,
    file_path: &str,
) {
    for batch in batches {
        let region_col = batch.column_by_name("region").unwrap();
        let year_col = batch.column_by_name("year").unwrap();
        let regions = region_col.as_any().downcast_ref::<StringArray>().unwrap();
        let years = year_col.as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(
                regions.value(i),
                expected_region,
                "Row {} in {} has wrong region: expected {}, got {}",
                i,
                file_path,
                expected_region,
                regions.value(i)
            );
            assert_eq!(
                years.value(i),
                expected_year,
                "Row {} in {} has wrong year: expected {}, got {}",
                i,
                file_path,
                expected_year,
                years.value(i)
            );
        }
    }
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn test_multi_column_partition_verifies_data_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    // IMPORTANT:
    // We are creating UNSORTED data here. Why, you might ask?
    // Because the partitioner depends on the data being sorted
    // in order to function correctly and so we are testing that
    // transform correctly sorts the data BEFORE partitioning it.

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // again, data is intentionally not sorted by (year, month)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            // year: mixed order
            Arc::new(Int32Array::from(vec![
                2024, 2023, 2024, 2023, 2024, 2023, 2024, 2023,
            ])),
            // month: mixed order
            Arc::new(Int32Array::from(vec![1, 2, 2, 1, 1, 2, 2, 1])),
            // id: unique per row for verification
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("year={{year}}/month={{month}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        output_format: Some("arrow".to_owned()), // be explicit to ensure we are testing the correct format,
        ..transform_defaults()
    })
    .await
    .unwrap();

    let file_2023_1 = temp_dir.path().join("year=2023/month=1.arrow");
    let file_2023_2 = temp_dir.path().join("year=2023/month=2.arrow");
    let file_2024_1 = temp_dir.path().join("year=2024/month=1.arrow");
    let file_2024_2 = temp_dir.path().join("year=2024/month=2.arrow");

    assert!(file_2023_1.exists(), "2023/1 partition file should exist");
    assert!(file_2023_2.exists(), "2023/2 partition file should exist");
    assert!(file_2024_1.exists(), "2024/1 partition file should exist");
    assert!(file_2024_2.exists(), "2024/2 partition file should exist");

    let batches_2023_1 = TestFile::read_arrow_auto(&file_2023_1);
    let batches_2023_2 = TestFile::read_arrow_auto(&file_2023_2);
    let batches_2024_1 = TestFile::read_arrow_auto(&file_2024_1);
    let batches_2024_2 = TestFile::read_arrow_auto(&file_2024_2);

    verify_int32_partition_values(&batches_2023_1, 2023, 1, "2023/1");
    verify_int32_partition_values(&batches_2023_2, 2023, 2, "2023/2");
    verify_int32_partition_values(&batches_2024_1, 2024, 1, "2024/1");
    verify_int32_partition_values(&batches_2024_2, 2024, 2, "2024/2");

    // ids 4 and 8
    assert_eq!(count_rows(&batches_2023_1), 2, "2023/1 should have 2 rows");
    // ids 2 and 6
    assert_eq!(count_rows(&batches_2023_2), 2, "2023/2 should have 2 rows");
    // ids 1 and 5
    assert_eq!(count_rows(&batches_2024_1), 2, "2024/1 should have 2 rows");
    // ids 3 and 7
    assert_eq!(count_rows(&batches_2024_2), 2, "2024/2 should have 2 rows");

    assert_eq!(
        count_rows(&batches_2023_1)
            + count_rows(&batches_2023_2)
            + count_rows(&batches_2024_1)
            + count_rows(&batches_2024_2),
        8,
        "total rows should match input"
    );
}

#[tokio::test]
async fn test_multi_column_partition_verifies_data_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // unsorted data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![
                2024, 2023, 2024, 2023, 2024, 2023, 2024, 2023,
            ])),
            Arc::new(Int32Array::from(vec![1, 2, 2, 1, 1, 2, 2, 1])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}.parquet");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let file_2023_1 = temp_dir.path().join("year=2023/month=1.parquet");
    let file_2023_2 = temp_dir.path().join("year=2023/month=2.parquet");
    let file_2024_1 = temp_dir.path().join("year=2024/month=1.parquet");
    let file_2024_2 = temp_dir.path().join("year=2024/month=2.parquet");

    assert!(file_2023_1.exists());
    assert!(file_2023_2.exists());
    assert!(file_2024_1.exists());
    assert!(file_2024_2.exists());

    let batches_2023_1 = TestFile::read_parquet(&file_2023_1);
    let batches_2023_2 = TestFile::read_parquet(&file_2023_2);
    let batches_2024_1 = TestFile::read_parquet(&file_2024_1);
    let batches_2024_2 = TestFile::read_parquet(&file_2024_2);

    verify_int32_partition_values(&batches_2023_1, 2023, 1, "2023/1");
    verify_int32_partition_values(&batches_2023_2, 2023, 2, "2023/2");
    verify_int32_partition_values(&batches_2024_1, 2024, 1, "2024/1");
    verify_int32_partition_values(&batches_2024_2, 2024, 2, "2024/2");

    assert_eq!(count_rows(&batches_2023_1), 2);
    assert_eq!(count_rows(&batches_2023_2), 2);
    assert_eq!(count_rows(&batches_2024_1), 2);
    assert_eq!(count_rows(&batches_2024_2), 2);
}

#[tokio::test]
async fn test_multi_column_partition_three_columns_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("day", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // unsorted by (year, month, day)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2024, 2023, 2024, 2023])),
            Arc::new(Int32Array::from(vec![1, 1, 1, 1])),
            Arc::new(Int32Array::from(vec![15, 10, 10, 15])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}/day={{day}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month,day".to_string()),
        output_format: Some("arrow".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let file_2023_1_10 = temp_dir.path().join("year=2023/month=1/day=10.arrow");
    let file_2023_1_15 = temp_dir.path().join("year=2023/month=1/day=15.arrow");
    let file_2024_1_10 = temp_dir.path().join("year=2024/month=1/day=10.arrow");
    let file_2024_1_15 = temp_dir.path().join("year=2024/month=1/day=15.arrow");

    assert!(file_2023_1_10.exists(), "2023/1/10 should exist");
    assert!(file_2023_1_15.exists(), "2023/1/15 should exist");
    assert!(file_2024_1_10.exists(), "2024/1/10 should exist");
    assert!(file_2024_1_15.exists(), "2024/1/15 should exist");

    let batches = TestFile::read_arrow_auto(&file_2023_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = TestFile::read_arrow_auto(&file_2023_1_15);
    assert_eq!(count_rows(&batches), 1);
    let batches = TestFile::read_arrow_auto(&file_2024_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = TestFile::read_arrow_auto(&file_2024_1_15);
    assert_eq!(count_rows(&batches), 1);
}

#[tokio::test]
async fn test_multi_column_partition_three_columns_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("day", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2024, 2023, 2024, 2023])),
            Arc::new(Int32Array::from(vec![1, 1, 1, 1])),
            Arc::new(Int32Array::from(vec![15, 10, 10, 15])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}/day={{day}}.parquet");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month,day".to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let file_2023_1_10 = temp_dir.path().join("year=2023/month=1/day=10.parquet");
    let file_2023_1_15 = temp_dir.path().join("year=2023/month=1/day=15.parquet");
    let file_2024_1_10 = temp_dir.path().join("year=2024/month=1/day=10.parquet");
    let file_2024_1_15 = temp_dir.path().join("year=2024/month=1/day=15.parquet");

    assert!(file_2023_1_10.exists(), "2023/1/10 should exist");
    assert!(file_2023_1_15.exists(), "2023/1/15 should exist");
    assert!(file_2024_1_10.exists(), "2024/1/10 should exist");
    assert!(file_2024_1_15.exists(), "2024/1/15 should exist");

    let batches = TestFile::read_parquet(&file_2023_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = TestFile::read_parquet(&file_2023_1_15);
    assert_eq!(count_rows(&batches), 1);
    let batches = TestFile::read_parquet(&file_2024_1_10);
    assert_eq!(count_rows(&batches), 1);
    let batches = TestFile::read_parquet(&file_2024_1_15);
    assert_eq!(count_rows(&batches), 1);
}

#[tokio::test]
async fn test_multi_column_partition_mixed_types() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "us-west", "eu-west", "us-west", "eu-west", "us-west", "eu-west",
            ])),
            Arc::new(Int32Array::from(vec![2024, 2023, 2023, 2024, 2024, 2023])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir
        .path()
        .join("region={{region}}/year={{year}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("region,year".to_string()),
        output_format: Some("arrow".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let file_eu_2023 = temp_dir.path().join("region=eu-west/year=2023.arrow");
    let file_eu_2024 = temp_dir.path().join("region=eu-west/year=2024.arrow");
    let file_us_2023 = temp_dir.path().join("region=us-west/year=2023.arrow");
    let file_us_2024 = temp_dir.path().join("region=us-west/year=2024.arrow");

    assert!(file_eu_2023.exists(), "eu-west/2023 should exist");
    assert!(file_eu_2024.exists(), "eu-west/2024 should exist");
    assert!(file_us_2023.exists(), "us-west/2023 should exist");
    assert!(file_us_2024.exists(), "us-west/2024 should exist");

    // ids 2, 6
    assert_eq!(count_rows(&TestFile::read_arrow_auto(&file_eu_2023)), 2);
    // id 4
    assert_eq!(count_rows(&TestFile::read_arrow_auto(&file_eu_2024)), 1);
    // id 3
    assert_eq!(count_rows(&TestFile::read_arrow_auto(&file_us_2023)), 1);
    // ids 1, 5
    assert_eq!(count_rows(&TestFile::read_arrow_auto(&file_us_2024)), 2);

    let file_eu_2023_batches = TestFile::read_arrow_auto(&file_eu_2023);
    let file_eu_2024_batches = TestFile::read_arrow_auto(&file_eu_2024);
    let file_us_2023_batches = TestFile::read_arrow_auto(&file_us_2023);
    let file_us_2024_batches = TestFile::read_arrow_auto(&file_us_2024);

    verify_string_partition_values(&file_eu_2023_batches, "eu-west", 2023, "eu-west/2023");
    verify_string_partition_values(&file_eu_2024_batches, "eu-west", 2024, "eu-west/2024");
    verify_string_partition_values(&file_us_2023_batches, "us-west", 2023, "us-west/2023");
    verify_string_partition_values(&file_us_2024_batches, "us-west", 2024, "us-west/2024");
}

#[tokio::test]
async fn test_multi_column_partition_parquet_with_exclude() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir
        .path()
        .join("year={{year}}/month={{month}}.parquet");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        exclude_columns: vec!["year".to_string(), "month".to_string()],
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let file_2023 = temp_dir.path().join("year=2023/month=1.parquet");
    let file_2024 = temp_dir.path().join("year=2024/month=1.parquet");

    assert!(file_2023.exists());
    assert!(file_2024.exists());

    // verify partition columns are excluded from file
    let batches = TestFile::read_parquet(&file_2023);
    assert_eq!(batches[0].num_columns(), 1, "only one column should remain");
    assert_eq!(
        batches[0].schema().field(0).name(),
        "id",
        "only 'id' column should remain"
    );
}

#[tokio::test]
async fn test_multi_column_partition_arrow_with_exclude() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir.path().join("year={{year}}/month={{month}}.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        exclude_columns: vec!["year".to_string(), "month".to_string()],
        output_format: Some("arrow".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let file_2023 = temp_dir.path().join("year=2023/month=1.arrow");
    let file_2024 = temp_dir.path().join("year=2024/month=1.arrow");

    assert!(file_2023.exists());
    assert!(file_2024.exists());

    let batches = TestFile::read_arrow_auto(&file_2023);
    assert_eq!(batches[0].num_columns(), 1, "only one column should remain");
    assert_eq!(
        batches[0].schema().field(0).name(),
        "id",
        "only 'id' column should remain"
    );
}

#[tokio::test]
async fn test_multi_column_partition_verifies_output_paths_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let list_output = temp_dir.path().join("outputs.json");

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    // unsorted data to ensure sorting happens
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2024, 2023, 2024, 2023])),
            Arc::new(Int32Array::from(vec![12, 6, 6, 12])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir
        .path()
        .join("data/year={{year}}/month={{month}}/data.arrow");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("year,month".to_string()),
        list_outputs: Some(ListOutputsFormat::Json),
        list_outputs_file: Some(Utf8PathBuf::from_path_buf(list_output.clone()).unwrap()),
        output_format: Some("arrow".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let outputs_json = std::fs::read_to_string(&list_output).unwrap();
    let outputs: serde_json::Value = serde_json::from_str(&outputs_json).unwrap();
    let files = outputs.as_array().unwrap();
    assert_eq!(files.len(), 4, "should have 4 partition files");

    for file in files {
        let path = file["durable_locations"][0].as_str().unwrap();
        let partition_values = file["partition_fields"].as_array().unwrap();

        assert_eq!(partition_values.len(), 2);
        assert_eq!(partition_values[0]["field"], "year");
        assert_eq!(partition_values[1]["field"], "month");

        let year = partition_values[0]["value"].as_i64().unwrap();
        let month = partition_values[1]["value"].as_i64().unwrap();

        assert!(
            path.contains(&format!("year={}", year)),
            "path '{}' should contain year={}",
            path,
            year
        );
        assert!(
            path.contains(&format!("month={}", month)),
            "path '{}' should contain month={}",
            path,
            month
        );

        let durable_path = url::Url::parse(path).unwrap().to_file_path().unwrap();
        assert!(durable_path.exists(), "file should exist: {path}");

        let batches = TestFile::read_arrow_auto(&durable_path);
        let expected_year = i32::try_from(year).unwrap();
        let expected_month = i32::try_from(month).unwrap();
        for batch in &batches {
            let years = batch
                .column_by_name("year")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let months = batch
                .column_by_name("month")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                assert_eq!(
                    years.value(i),
                    expected_year,
                    "file {} row {} should have year={}",
                    path,
                    i,
                    year
                );
                assert_eq!(
                    months.value(i),
                    expected_month,
                    "file {} row {} should have month={}",
                    path,
                    i,
                    month
                );
            }
        }
    }
}

#[tokio::test]
async fn test_multi_column_partition_verifies_output_paths_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let list_output = temp_dir.path().join("outputs.json");

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "us-east", "eu-west", "us-east", "eu-west",
            ])),
            Arc::new(Int32Array::from(vec![2024, 2024, 2023, 2023])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = temp_dir
        .path()
        .join("output/region={{region}}/year={{year}}.parquet");

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("region,year".to_string()),
        list_outputs: Some(ListOutputsFormat::Json),
        list_outputs_file: Some(Utf8PathBuf::from_path_buf(list_output.clone()).unwrap()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    let outputs_json = std::fs::read_to_string(&list_output).unwrap();
    let outputs: serde_json::Value = serde_json::from_str(&outputs_json).unwrap();
    let files = outputs.as_array().unwrap();
    assert_eq!(files.len(), 4, "should have 4 partition files");

    for file in files {
        let path = file["durable_locations"][0].as_str().unwrap();
        let partition_values = file["partition_fields"].as_array().unwrap();

        assert_eq!(partition_values.len(), 2);
        assert_eq!(partition_values[0]["field"], "region");
        assert_eq!(partition_values[1]["field"], "year");

        let region = partition_values[0]["value"].as_str().unwrap();
        let year = partition_values[1]["value"].as_i64().unwrap();

        assert!(
            path.contains(&format!("region={}", region)),
            "path '{}' should contain region={}",
            path,
            region
        );
        assert!(
            path.contains(&format!("year={}", year)),
            "path '{}' should contain year={}",
            path,
            year
        );

        let durable_path = url::Url::parse(path).unwrap().to_file_path().unwrap();
        assert!(durable_path.exists(), "file should exist: {path}");

        let batches = TestFile::read_parquet(&durable_path);
        let expected_year = i32::try_from(year).unwrap();
        for batch in &batches {
            let regions = batch
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let years = batch
                .column_by_name("year")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                assert_eq!(
                    regions.value(i),
                    region,
                    "file {} row {} should have region={}",
                    path,
                    i,
                    region
                );
                assert_eq!(
                    years.value(i),
                    expected_year,
                    "file {} row {} should have year={}",
                    path,
                    i,
                    year
                );
            }
        }
    }

    // verify all 4 expected paths exist with correct partition values
    let expected_paths = [
        ("output/region=eu-west/year=2023.parquet", "eu-west", 2023),
        ("output/region=eu-west/year=2024.parquet", "eu-west", 2024),
        ("output/region=us-east/year=2023.parquet", "us-east", 2023),
        ("output/region=us-east/year=2024.parquet", "us-east", 2024),
    ];

    for (rel_path, expected_region, expected_year) in expected_paths {
        let full_path = temp_dir.path().join(rel_path);
        assert!(
            full_path.exists(),
            "partition file {} should exist",
            rel_path
        );

        // verify file contents match the path
        let batches = TestFile::read_parquet(&full_path);
        for batch in &batches {
            let regions = batch
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let years = batch
                .column_by_name("year")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                assert_eq!(
                    regions.value(i),
                    expected_region,
                    "file {} row {} should have region={}",
                    rel_path,
                    i,
                    expected_region
                );
                assert_eq!(
                    years.value(i),
                    expected_year,
                    "file {} row {} should have year={}",
                    rel_path,
                    i,
                    expected_year
                );
            }
        }
    }
}

#[tokio::test]
async fn test_partition_strategies_produce_same_output() {
    // both high-cardinality (with sort) and low-cardinality should produce identical results
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");

    // create unsorted data - partition values are interleaved
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec!["x", "y", "x", "y", "z", "x"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let high_card_dir = temp_dir.path().join("high_cardinality");
    let low_card_dir = temp_dir.path().join("low_cardinality");
    std::fs::create_dir_all(&high_card_dir).unwrap();
    std::fs::create_dir_all(&low_card_dir).unwrap();

    // run high-cardinality partitioning (requires sort)
    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(
            high_card_dir
                .join("{{category}}.parquet")
                .to_string_lossy()
                .to_string(),
        ),
        by: Some("category".to_string()),
        create_dirs: false,
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    // run low-cardinality partitioning (no global sort)
    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: None,
        to_many: Some(
            low_card_dir
                .join("{{category}}.parquet")
                .to_string_lossy()
                .to_string(),
        ),
        by: Some("category".to_string()),
        partition_strategy: PartitionStrategy::NosortMulti,
        create_dirs: false,
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    // helper to extract sorted (id, value) pairs from a partition file
    fn extract_data(dir: &std::path::Path, filename: &str) -> Vec<(i32, i32)> {
        let path = dir.join(filename);
        let batches = TestFile::read_parquet(&path);
        let mut data = Vec::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                data.push((ids.value(i), values.value(i)));
            }
        }
        data.sort();
        data
    }

    // both strategies should create the same files
    assert!(high_card_dir.join("x.parquet").exists());
    assert!(high_card_dir.join("y.parquet").exists());
    assert!(high_card_dir.join("z.parquet").exists());
    assert!(low_card_dir.join("x.parquet").exists());
    assert!(low_card_dir.join("y.parquet").exists());
    assert!(low_card_dir.join("z.parquet").exists());

    // data should be identical when sorted
    assert_eq!(
        extract_data(&high_card_dir, "x.parquet"),
        extract_data(&low_card_dir, "x.parquet")
    );
    assert_eq!(
        extract_data(&high_card_dir, "y.parquet"),
        extract_data(&low_card_dir, "y.parquet")
    );
    assert_eq!(
        extract_data(&high_card_dir, "z.parquet"),
        extract_data(&low_card_dir, "z.parquet")
    );

    // verify expected content
    assert_eq!(
        extract_data(&high_card_dir, "x.parquet"),
        vec![(1, 10), (3, 30), (6, 60)]
    );
    assert_eq!(
        extract_data(&high_card_dir, "y.parquet"),
        vec![(2, 20), (4, 40)]
    );
    assert_eq!(extract_data(&high_card_dir, "z.parquet"), vec![(5, 50)]);
}

#[tokio::test]
async fn test_transform_with_sequential_encoder() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-compression", "snappy"])
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_dictionary_prefix_matches_nested_columns() {
    // "person" prefix should enable dictionary on person.name and person.age
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with([
            "--parquet-dictionary-all-off",
            "--parquet-dictionary-column",
            "person:always",
        ])
    })
    .await
    .unwrap();

    let inspector = test_helpers::inspect(&output);
    test_helpers::assert_no_dictionary(&inspector, "id");
    test_helpers::assert_has_dictionary(&inspector, "person.name");
    test_helpers::assert_has_dictionary(&inspector, "person.age");
}

#[tokio::test]
async fn test_dictionary_specific_path_in_nested_column() {
    // "person.name" should only enable dictionary on person.name, not person.age
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with([
            "--parquet-dictionary-all-off",
            "--parquet-dictionary-column",
            "person.name:always",
        ])
    })
    .await
    .unwrap();

    let inspector = test_helpers::inspect(&output);
    test_helpers::assert_no_dictionary(&inspector, "id");
    test_helpers::assert_has_dictionary(&inspector, "person.name");
    test_helpers::assert_no_dictionary(&inspector, "person.age");
}

#[tokio::test]
async fn test_bloom_filter_prefix_matches_nested_columns() {
    // "person" prefix should enable bloom filters on person.name and person.age
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with(["--parquet-bloom-column", "person:fpp=0.01,ndv=100"])
    })
    .await
    .unwrap();

    let inspector = test_helpers::inspect(&output);
    test_helpers::assert_no_bloom_filter(&inspector, "id");
    test_helpers::assert_has_bloom_filter(&inspector, "person.name");
    test_helpers::assert_has_bloom_filter(&inspector, "person.age");
}

#[tokio::test]
async fn test_bloom_filter_prefix_with_exclusion() {
    // "person" enables bloom, but "person.age" is excluded
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults_with([
            "--parquet-bloom-column",
            "person:fpp=0.01,ndv=100",
            "--parquet-bloom-column-off",
            "person.age",
        ])
    })
    .await
    .unwrap();

    let inspector = test_helpers::inspect(&output);
    test_helpers::assert_no_bloom_filter(&inspector, "id");
    test_helpers::assert_has_bloom_filter(&inspector, "person.name");
    test_helpers::assert_no_bloom_filter(&inspector, "person.age");
}

#[tokio::test]
async fn test_transform_sort_uses_final_plan_statistics_for_spill_reservation() {
    // The final sort input statistics tune DataFusion's spill reservation.
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.parquet");

    // Several physical types exercise the statistics-based sizing path.
    let batch = TestBatch::builder()
        .column_i32("id", &[5, 3, 1, 4, 2])
        .column_string("name", &["echo", "charlie", "alpha", "delta", "bravo"])
        .column_f64("score", &[50.0, 30.0, 10.0, 40.0, 20.0])
        .build();
    TestFile::write_parquet_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        output_format: Some("parquet".to_owned()),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_transform_sort_multi_file_with_spill_reservation() {
    // Row-size measurement covers the combined logical input, not only its first file.
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("a.arrow");
    let input2 = temp_dir.path().join("b.arrow");
    let output = temp_dir.path().join("output.arrow");

    TestFile::write_arrow_batch(&input1, &TestBatch::simple_with(&[4, 2], &["d", "b"]));
    TestFile::write_arrow_batch(&input2, &TestBatch::simple_with(&[3, 1], &["c", "a"]));

    run_transform(TestTransformCommand {
        exact_references: vec![
            input1.to_string_lossy().to_string(),
            input2.to_string_lossy().to_string(),
        ],
        to: Some(output.to_string_lossy().to_string()),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_reserved_spill_pool_simple_transform() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        non_spillable_reserve: Some(PoolReserveSpec::Percent(10)),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_reserved_spill_pool_with_sorting() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        non_spillable_reserve: Some(PoolReserveSpec::Percent(10)),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[tokio::test]
async fn test_reserved_spill_pool_with_fixed_reserve() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[5, 2, 4, 1, 3], &["e", "b", "d", "a", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        output_format: Some("parquet".to_owned()),
        non_spillable_reserve: Some(PoolReserveSpec::Fixed(50 * 1024 * 1024)), // 50MB
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "name".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_parquet(&output);
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "a");
    assert_eq!(names.value(1), "b");
    assert_eq!(names.value(2), "c");
    assert_eq!(names.value(3), "d");
    assert_eq!(names.value(4), "e");
}

#[tokio::test]
async fn test_reserved_spill_pool_with_top_consumers() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    // exercise the top-consumers=0 (all) path alongside the reserved pool
    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to: Some(output.to_string_lossy().to_string()),
        non_spillable_reserve: Some(PoolReserveSpec::Percent(25)),
        memory_pool_top_consumers: 0,
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output.exists());
    let batches = TestFile::read_arrow_auto(&output);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[tokio::test]
async fn test_nosort_evict_partitioned_write() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["x", "y", "z", "x", "y"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    let template = output_dir.join("{{category}}_{{file_number}}.parquet");
    run_transform(TestTransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(template.to_string_lossy().to_string()),
        by: Some("category".to_string()),
        partition_strategy: PartitionStrategy::NosortEvict,
        max_open_partitions: Some(2),
        overwrite: true,
        ..transform_defaults()
    })
    .await
    .unwrap();

    assert!(output_dir.join("x_0.parquet").exists());
    assert!(output_dir.join("y_0.parquet").exists());
    assert!(output_dir.join("z_0.parquet").exists());
    assert!(
        output_dir.join("x_1.parquet").exists(),
        "the template should render the reopened partition's next file number"
    );
    assert!(output_dir.join("y_1.parquet").exists());

    // verify data correctness: all 5 rows present across files
    let mut total_rows = 0;
    for entry in std::fs::read_dir(&output_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "parquet") {
            let batches = TestFile::read_parquet(&path);
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
    }
    assert_eq!(total_rows, 5);
}
