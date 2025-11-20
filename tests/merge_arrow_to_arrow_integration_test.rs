use arrow::{array::Int32Array, ipc::reader::FileReader};
use assert_cmd::cargo;
use silk_chiffon::utils::test_helpers::test_data;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn test_merge_arrow_to_arrow_basic() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged.arrow");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 3);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 4, 3);

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "merge-arrow-to-arrow",
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
    let reader = FileReader::try_new(file, None).unwrap();
    let mut total_rows = 0;

    for batch_result in reader {
        let batch = batch_result.unwrap();
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 6, "Expected 6 rows in merged file");
}

#[test]
fn test_merge_arrow_to_arrow_with_sorting() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_sorted.arrow");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 5, 3);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 1, 3);

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "merge-arrow-to-arrow",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--sort-by",
            "id",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let mut all_ids = Vec::new();

    for batch_result in reader {
        let batch = batch_result.unwrap();
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        all_ids.extend(id_array.values().iter().copied());
    }

    assert_eq!(all_ids, vec![1, 2, 3, 5, 6, 7], "IDs should be sorted");
}

#[test]
fn test_merge_arrow_to_arrow_with_glob_pattern() {
    let temp_dir = tempdir().unwrap();
    let output_path = temp_dir.path().join("merged_glob.arrow");

    for i in 0..3 {
        let path = temp_dir.path().join(format!("data_{i}.arrow"));
        test_data::create_arrow_file_with_range_of_ids(&path, i * 10, 5);
    }

    let glob_pattern = format!("{}/data_*.arrow", temp_dir.path().display());
    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "merge-arrow-to-arrow",
            &glob_pattern,
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let mut total_rows = 0;

    for batch_result in reader {
        let batch = batch_result.unwrap();
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 15, "Expected 15 rows in merged file");
}

#[test]
fn test_merge_arrow_to_arrow_with_compression() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_compressed.arrow");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 100);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 101, 100);

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "merge-arrow-to-arrow",
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
    assert!(output_path.exists(), "Output file was not created");

    let uncompressed_size =
        input1_path.metadata().unwrap().len() + input2_path.metadata().unwrap().len();
    let compressed_size = output_path.metadata().unwrap().len();

    assert!(
        compressed_size < uncompressed_size,
        "Compressed file should be smaller"
    );

    let file = File::open(&output_path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let mut total_rows = 0;

    for batch_result in reader {
        let batch = batch_result.unwrap();
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 200, "Expected 200 rows in merged file");
}

#[test]
fn test_merge_arrow_to_arrow_with_query() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged_filtered.arrow");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 10);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 11, 10);

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "merge-arrow-to-arrow",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "--query",
            "SELECT * FROM data WHERE id > 5 AND id < 16",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let file = File::open(&output_path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let mut all_ids = Vec::new();

    for batch_result in reader {
        let batch = batch_result.unwrap();
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        all_ids.extend(id_array.values().iter().copied());
    }

    assert_eq!(all_ids.len(), 10, "Expected 10 rows after filtering");
    assert!(
        all_ids.iter().all(|&id| id > 5 && id < 16),
        "All IDs should be between 6 and 15"
    );
}

#[test]
fn test_merge_arrow_to_arrow_empty_input_error() {
    let temp_dir = tempdir().unwrap();
    let output_path = temp_dir.path().join("merged.arrow");

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args(["merge-arrow-to-arrow", "-o", output_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute command");

    assert!(!output.status.success(), "Command should have failed");
}

#[test]
fn test_merge_arrow_to_arrow_nonexistent_file_error() {
    let temp_dir = tempdir().unwrap();
    let output_path = temp_dir.path().join("merged.arrow");

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "merge-arrow-to-arrow",
            "/nonexistent/file.arrow",
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(!output.status.success(), "Command should have failed");
}
