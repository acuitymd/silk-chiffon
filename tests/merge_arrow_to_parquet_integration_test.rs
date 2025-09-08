use assert_cmd::Command;
use parquet::{
    basic::Compression,
    file::reader::{FileReader, SerializedFileReader},
};
use silk_chiffon::utils::test_helpers::test_data;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn test_merge_arrow_to_parquet_basic() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 3);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 4, 3);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        6,
        "Expected 6 rows in merged file"
    );
}

#[test]
fn test_merge_arrow_to_parquet_with_compression() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_compressed.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 100);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 101, 100);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--compression",
            "zstd",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        200,
        "Expected 200 rows"
    );

    let row_group = metadata.row_group(0);
    let column = row_group.column(0);
    assert!(
        !matches!(column.compression(), Compression::UNCOMPRESSED),
        "Column should be compressed"
    );
}

#[test]
fn test_merge_arrow_to_parquet_with_sorting() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_sorted.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 10, 5);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 1, 5);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--sort-by",
            "id",
            "--write-sorted-metadata",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(metadata.file_metadata().num_rows(), 10, "Expected 10 rows");
}

#[test]
fn test_merge_arrow_to_parquet_with_bloom_filters() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_bloom.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 50);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 51, 50);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--bloom-column",
            "id",
            "--bloom-column",
            "name",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        100,
        "Expected 100 rows"
    );
}

#[test]
fn test_merge_arrow_to_parquet_with_row_group_size() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_small_rg.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 1000);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 1001, 1000);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--max-row-group-size",
            "500",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        2000,
        "Expected 2000 rows"
    );
    assert_eq!(
        metadata.num_row_groups(),
        4,
        "Expected 4 row groups (2000/500)"
    );
}

#[test]
fn test_merge_arrow_to_parquet_with_query() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_filtered.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 20);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 21, 20);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--query",
            "SELECT * FROM data WHERE id >= 10 AND id <= 30",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        21,
        "Expected 21 rows after filtering"
    );
}

#[test]
fn test_merge_arrow_to_parquet_with_statistics() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_stats.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 100);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 101, 100);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--statistics",
            "page",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        200,
        "Expected 200 rows"
    );
}

#[test]
fn test_merge_arrow_to_parquet_glob_pattern() {
    let temp_dir = tempdir().unwrap();
    let output_path = temp_dir.path().join("merged_glob.parquet");

    for i in 0..4 {
        let path = temp_dir.path().join(format!("part_{i}.arrow"));
        test_data::create_arrow_file_with_range_of_ids(&path, i * 25, 25);
    }

    let glob_pattern = format!("{}/part_*.arrow", temp_dir.path().display());
    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            &glob_pattern,
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        100,
        "Expected 100 rows from 4 files"
    );
}

#[test]
fn test_merge_arrow_to_parquet_writer_version() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("merged_v1.parquet");

    test_data::create_arrow_file_with_range_of_ids(&input_path, 1, 10);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-parquet",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--writer-version",
            "v1",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output file was not created");
}
