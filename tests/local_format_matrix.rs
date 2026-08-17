use std::{
    fs,
    path::{Path, PathBuf},
};

use arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
};
use assert_cmd::cargo;
use silk_chiffon_test_support::{TestBatch, TestExtract, TestFile, vortex::encode_batches};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug)]
enum FileVariant {
    ArrowFile,
    ArrowStream,
    Parquet,
    Vortex,
}

impl FileVariant {
    const ALL: [Self; 4] = [
        Self::ArrowFile,
        Self::ArrowStream,
        Self::Parquet,
        Self::Vortex,
    ];

    fn slug(self) -> &'static str {
        match self {
            Self::ArrowFile => "arrow-file",
            Self::ArrowStream => "arrow-stream",
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::ArrowFile => "arrow",
            Self::ArrowStream => "arrows",
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
        }
    }

    fn output_arguments(self) -> &'static [&'static str] {
        match self {
            Self::ArrowFile => &["--output-format", "arrow", "--arrow-format", "file"],
            Self::ArrowStream => &["--output-format", "arrow", "--arrow-format", "stream"],
            Self::Parquet => &["--output-format", "parquet"],
            Self::Vortex => &["--output-format", "vortex"],
        }
    }
}

async fn write_input(path: &Path, variant: FileVariant) {
    let batch = TestBatch::simple_with(&[1, 2, 3, 4], &["a", "b", "a", "b"]);
    match variant {
        FileVariant::ArrowFile => TestFile::write_arrow_batch(path, &batch),
        FileVariant::ArrowStream => TestFile::write_arrow_stream(path, &[batch]),
        FileVariant::Parquet => TestFile::write_parquet_batch(path, &batch),
        FileVariant::Vortex => {
            let bytes = encode_batches(&batch.schema(), vec![batch]).await.unwrap();
            fs::write(path, bytes).unwrap();
        }
    }
}

fn transform(input: &Path, output: &Path, output_variant: FileVariant, extra: &[&str]) {
    let mut arguments = vec![
        "transform".to_owned(),
        "--from".to_owned(),
        input.to_string_lossy().into_owned(),
        "--to".to_owned(),
        output.to_string_lossy().into_owned(),
    ];
    arguments.extend(
        output_variant
            .output_arguments()
            .iter()
            .map(|value| (*value).to_owned()),
    );
    arguments.extend(extra.iter().map(|value| (*value).to_owned()));

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args(arguments)
        .assert()
        .success();
}

fn verify_rows(input: &Path, verification: &Path) {
    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            verification.to_str().unwrap(),
            "--output-format",
            "arrow",
            "--arrow-format",
            "file",
        ])
        .assert()
        .success();
    let batches = TestFile::read_arrow(verification);
    let mut ids = TestExtract::i32_all(&batches, "id");
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3, 4], "rows from {}", input.display());
    let mut names = string_values(&batches, "name");
    names.sort_unstable();
    assert_eq!(names, ["a", "a", "b", "b"], "rows from {}", input.display());
}

fn string_values(batches: &[arrow::record_batch::RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch.schema().index_of(column).unwrap();
            let values = arrow::compute::cast(batch.column(index), &DataType::Utf8).unwrap();
            let values = values.as_any().downcast_ref::<StringArray>().unwrap();
            (0..values.len())
                .map(|index| {
                    assert!(!values.is_null(index));
                    values.value(index).to_owned()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn files_below(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_local_file_variant_transforms_to_every_output_variant() {
    let temp = TempDir::new().unwrap();

    for input_variant in FileVariant::ALL {
        let input = temp.path().join(format!(
            "input-{}.{}",
            input_variant.slug(),
            input_variant.extension()
        ));
        write_input(&input, input_variant).await;

        for output_variant in FileVariant::ALL {
            let output = temp.path().join(format!(
                "{}-to-{}.{}",
                input_variant.slug(),
                output_variant.slug(),
                output_variant.extension()
            ));
            transform(&input, &output, output_variant, &[]);
            let verification = temp.path().join(format!(
                "verify-{}-to-{}.arrow",
                input_variant.slug(),
                output_variant.slug()
            ));
            verify_rows(&output, &verification);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_output_variant_supports_every_partition_strategy() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    write_input(&input, FileVariant::ArrowFile).await;

    for output_variant in FileVariant::ALL {
        for strategy in ["sort-single", "nosort-multi", "nosort-evict"] {
            let root = temp
                .path()
                .join(format!("{}-{strategy}", output_variant.slug()));
            fs::create_dir(&root).unwrap();
            let filename = if strategy == "nosort-evict" {
                format!(
                    "{{{{name}}}}-{{{{file_number}}}}.{}",
                    output_variant.extension()
                )
            } else {
                format!("{{{{name}}}}.{}", output_variant.extension())
            };
            let template = root.join(filename);
            let mut extra = vec!["--by", "name", "--partition-strategy", strategy];
            if strategy == "nosort-evict" {
                extra.extend(["--max-open-partitions", "1"]);
            }

            let mut arguments = vec![
                "transform".to_owned(),
                "--from".to_owned(),
                input.to_string_lossy().into_owned(),
                "--to-many".to_owned(),
                template.to_string_lossy().into_owned(),
            ];
            arguments.extend(
                output_variant
                    .output_arguments()
                    .iter()
                    .map(|value| (*value).to_owned()),
            );
            arguments.extend(extra.into_iter().map(str::to_owned));
            cargo::cargo_bin_cmd!("silk-chiffon")
                .args(arguments)
                .assert()
                .success();

            let outputs = files_below(&root);
            assert!(!outputs.is_empty(), "{output_variant:?} {strategy}");
            let mut ids = Vec::new();
            let mut names = Vec::new();
            for (index, output) in outputs.iter().enumerate() {
                let verification = temp.path().join(format!(
                    "verify-{}-{strategy}-{index}.arrow",
                    output_variant.slug()
                ));
                cargo::cargo_bin_cmd!("silk-chiffon")
                    .args([
                        "transform",
                        "--from",
                        output.to_str().unwrap(),
                        "--to",
                        verification.to_str().unwrap(),
                    ])
                    .assert()
                    .success();
                let batches = TestFile::read_arrow(&verification);
                ids.extend(TestExtract::i32_all(&batches, "id"));
                names.extend(string_values(&batches, "name"));
            }
            ids.sort_unstable();
            names.sort_unstable();
            assert_eq!(ids, [1, 2, 3, 4], "{output_variant:?} {strategy}");
            assert_eq!(names, ["a", "a", "b", "b"], "{output_variant:?} {strategy}");
        }
    }
}
