use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use assert_cmd::cargo;
use datafusion::prelude::*;
use silk_chiffon::utils::test_data::{TestBatch, TestExtract, TestFile};
use std::sync::Arc;
use tempfile::TempDir;

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_arrow_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![1, 2, 3]);
    assert_eq!(
        TestExtract::string_all(&batches, "name"),
        vec!["a", "b", "c"]
    );
}

#[test]
fn test_arrow_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[10, 20, 30], &["x", "y", "z"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
        ])
        .assert()
        .success();

    let batches = TestFile::read_parquet(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![10, 20, 30]);
    assert_eq!(
        TestExtract::string_all(&batches, "name"),
        vec!["x", "y", "z"]
    );
}

#[test]
fn test_merge_two_files() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("merged.arrow");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from-many",
            input1.to_str().unwrap(),
            "--from-many",
            input2.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(count_rows(&batches), 4);

    let mut ids = TestExtract::i32_all(&batches, "id");
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[test]
fn test_merge_with_glob() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("data1.arrow");
    let input2 = temp_dir.path().join("data2.arrow");
    let input3 = temp_dir.path().join("data3.arrow");
    let output = temp_dir.path().join("merged.parquet");

    TestFile::write_arrow_batch(&input1, &TestBatch::simple_with(&[1], &["a"]));
    TestFile::write_arrow_batch(&input2, &TestBatch::simple_with(&[2], &["b"]));
    TestFile::write_arrow_batch(&input3, &TestBatch::simple_with(&[3], &["c"]));

    let glob_pattern = temp_dir.path().join("data*.arrow");

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from-many",
            glob_pattern.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
        ])
        .assert()
        .success();

    let batches = TestFile::read_parquet(&output);
    let mut ids = TestExtract::i32_all(&batches, "id");
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn test_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3, 4, 5], &["x", "y", "x", "y", "z"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to-many",
            output_template.to_str().unwrap(),
            "--by",
            "name",
        ])
        .assert()
        .success();

    let output_x = temp_dir.path().join("x.arrow");
    let batches_x = TestFile::read_arrow(&output_x);
    assert_eq!(TestExtract::i32_all(&batches_x, "id"), vec![1, 3]);
    assert_eq!(TestExtract::string_all(&batches_x, "name"), vec!["x", "x"]);

    let output_y = temp_dir.path().join("y.arrow");
    let batches_y = TestFile::read_arrow(&output_y);
    assert_eq!(TestExtract::i32_all(&batches_y, "id"), vec![2, 4]);
    assert_eq!(TestExtract::string_all(&batches_y, "name"), vec!["y", "y"]);

    let output_z = temp_dir.path().join("z.arrow");
    let batches_z = TestFile::read_arrow(&output_z);
    assert_eq!(TestExtract::i32_all(&batches_z, "id"), vec![5]);
}

#[test]
fn test_partition_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("category_{{name}}.parquet");

    let batch = TestBatch::simple_with(&[100, 200, 300], &["alpha", "beta", "alpha"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to-many",
            output_template.to_str().unwrap(),
            "--by",
            "name",
        ])
        .assert()
        .success();

    let output_alpha = temp_dir.path().join("category_alpha.parquet");
    let batches_alpha = TestFile::read_parquet(&output_alpha);
    assert_eq!(TestExtract::i32_all(&batches_alpha, "id"), vec![100, 300]);

    let output_beta = temp_dir.path().join("category_beta.parquet");
    let batches_beta = TestFile::read_parquet(&output_beta);
    assert_eq!(TestExtract::i32_all(&batches_beta, "id"), vec![200]);
}

#[test]
fn test_merge_and_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output_template = temp_dir.path().join("{{name}}.parquet");

    let batch1 = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "a"]);
    let batch2 = TestBatch::simple_with(&[4, 5, 6], &["b", "c", "c"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from-many",
            input1.to_str().unwrap(),
            "--from-many",
            input2.to_str().unwrap(),
            "--to-many",
            output_template.to_str().unwrap(),
            "--by",
            "name",
        ])
        .assert()
        .success();

    let output_a = temp_dir.path().join("a.parquet");
    assert_eq!(
        TestExtract::i32_all(&TestFile::read_parquet(&output_a), "id"),
        vec![1, 3]
    );

    let output_b = temp_dir.path().join("b.parquet");
    assert_eq!(
        TestExtract::i32_all(&TestFile::read_parquet(&output_b), "id"),
        vec![2, 4]
    );

    let output_c = temp_dir.path().join("c.parquet");
    assert_eq!(
        TestExtract::i32_all(&TestFile::read_parquet(&output_c), "id"),
        vec![5, 6]
    );
}

#[test]
fn test_sort_ascending() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[5, 2, 8, 1, 3], &["e", "b", "h", "a", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--sort-by",
            "id",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![1, 2, 3, 5, 8]);
}

#[test]
fn test_sort_descending() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[5, 2, 8, 1, 3], &["e", "b", "h", "a", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--sort-by",
            "id:desc",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![8, 5, 3, 2, 1]);
}

#[test]
fn test_sort_by_name() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[5, 2, 8, 1, 3], &["zebra", "bear", "ant", "dog", "cat"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--sort-by",
            "name",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(
        TestExtract::string_all(&batches, "name"),
        vec!["ant", "bear", "cat", "dog", "zebra"]
    );
}

#[test]
fn test_query_filter() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT * FROM data WHERE id > 2 AND id < 5",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![3, 4]);
    assert_eq!(TestExtract::string_all(&batches, "name"), vec!["c", "d"]);
}

#[test]
fn test_query_projection() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT name FROM data",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(
        TestExtract::string_all(&batches, "name"),
        vec!["a", "b", "c"]
    );
}

#[test]
fn test_query_and_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[5, 2, 8, 1, 3], &["e", "b", "h", "a", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT * FROM data WHERE id <= 5",
            "--sort-by",
            "id:desc",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![5, 3, 2, 1]);
}

#[test]
fn test_query_and_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3, 4, 5, 6], &["a", "b", "a", "b", "c", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to-many",
            output_template.to_str().unwrap(),
            "--by",
            "name",
            "--query",
            "SELECT * FROM data WHERE id >= 3",
        ])
        .assert()
        .success();

    let output_a = temp_dir.path().join("a.arrow");
    assert_eq!(
        TestExtract::i32_all(&TestFile::read_arrow(&output_a), "id"),
        vec![3]
    );

    let output_b = temp_dir.path().join("b.arrow");
    assert_eq!(
        TestExtract::i32_all(&TestFile::read_arrow(&output_b), "id"),
        vec![4]
    );

    let output_c = temp_dir.path().join("c.arrow");
    assert_eq!(
        TestExtract::i32_all(&TestFile::read_arrow(&output_c), "id"),
        vec![5, 6]
    );
}

#[test]
fn test_merge_and_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch1 = TestBatch::simple_with(&[5, 2], &["e", "b"]);
    let batch2 = TestBatch::simple_with(&[8, 1, 3], &["h", "a", "c"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from-many",
            input1.to_str().unwrap(),
            "--from-many",
            input2.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--sort-by",
            "id",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![1, 2, 3, 5, 8]);
}

#[test]
fn test_list_outputs_text() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["x", "y", "x"]);
    TestFile::write_arrow_batch(&input, &batch);

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to-many",
            output_template.to_str().unwrap(),
            "--by",
            "name",
            "--list-outputs",
            "text",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("x.arrow"));
    assert!(stdout.contains("y.arrow"));

    let output_x = temp_dir.path().join("x.arrow");
    assert_eq!(count_rows(&TestFile::read_arrow(&output_x)), 2);
}

#[test]
fn test_list_outputs_json() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let batch = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    let output = cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to-many",
            output_template.to_str().unwrap(),
            "--by",
            "name",
            "--list-outputs",
            "json",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let files: Vec<serde_json::Value> = serde_json::from_str(&stdout).unwrap();

    // should have 2 output files (one for "a", one for "b")
    assert_eq!(files.len(), 2);

    // each file should have path, row_count, and partition_values
    for file in &files {
        assert!(file.get("path").is_some());
        assert!(file.get("row_count").is_some());
        assert!(file.get("partition_values").is_some());

        let partition_values = file.get("partition_values").unwrap().as_array().unwrap();
        assert_eq!(partition_values.len(), 1);
        assert_eq!(partition_values[0].get("column").unwrap(), "name");
    }
}

#[test]
fn test_empty_query_result() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT * FROM data WHERE id > 100",
        ])
        .assert()
        .success();

    let batches = TestFile::read_arrow(&output);
    assert_eq!(count_rows(&batches), 0);
}

#[test]
fn test_single_row() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[42], &["single"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
        ])
        .assert()
        .success();

    let batches = TestFile::read_parquet(&output);
    assert_eq!(TestExtract::i32_all(&batches, "id"), vec![42]);
}

#[tokio::test]
async fn test_arrow_to_vortex() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let output = temp.path().join("output.vortex");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["Alice", "Bob", "Charlie"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert!(output.exists());
    assert!(output.metadata().unwrap().len() > 0);
}

#[tokio::test]
async fn test_vortex_to_arrow() {
    let temp = TempDir::new().unwrap();
    let arrow1 = temp.path().join("input.arrow");
    let vortex = temp.path().join("intermediate.vortex");
    let arrow2 = temp.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["Alice", "Bob", "Charlie"]);
    TestFile::write_arrow_batch(&arrow1, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            arrow1.to_str().unwrap(),
            "--to",
            vortex.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex.to_str().unwrap(),
            "--to",
            arrow2.to_str().unwrap(),
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&arrow2);
    assert_eq!(result_batches.len(), 1);
    assert_eq!(result_batches[0].num_rows(), 3);
    assert_eq!(result_batches[0].num_columns(), 2);
}

#[tokio::test]
async fn test_vortex_round_trip() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let batches = vec![
        TestBatch::simple_with(&[1, 2], &["Alice", "Bob"]),
        TestBatch::simple_with(&[3, 4], &["Charlie", "Diana"]),
        TestBatch::simple_with(&[5], &["Eve"]),
    ];
    TestFile::write_arrow(&input, &batches);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            vortex_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex_file.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&output);
    let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);
}

#[tokio::test]
async fn test_vortex_with_query() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3, 4, 5], &["A", "B", "C", "D", "E"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            vortex_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex_file.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT * FROM data WHERE id > 2",
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&output);
    let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_vortex_with_sort() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[3, 1, 2], &["C", "A", "B"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            vortex_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex_file.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--sort-by",
            "id",
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&output);
    assert_eq!(result_batches.len(), 1);
    let id_array = result_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_array.value(0), 1);
    assert_eq!(id_array.value(1), 2);
    assert_eq!(id_array.value(2), 3);
}

#[tokio::test]
async fn test_vortex_with_batch_size() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let output = temp.path().join("output.vortex");

    let batch = TestBatch::simple_with(&[1, 2, 3, 4, 5], &["A", "B", "C", "D", "E"]);
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--vortex-record-batch-size",
            "2",
        ])
        .assert()
        .success();

    assert!(output.exists());
}

#[tokio::test]
async fn test_vortex_query_with_aggregates() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A"])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 250, 50])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            vortex_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex_file.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT category, SUM(amount) as total FROM data GROUP BY category ORDER BY category",
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&output);
    assert_eq!(result_batches.len(), 1);
    assert_eq!(result_batches[0].num_rows(), 2);
    assert_eq!(result_batches[0].num_columns(), 2);
}

#[tokio::test]
async fn test_vortex_query_with_filter_and_projection() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![25, 30, 35, 28, 32])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            vortex_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex_file.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT name, age FROM data WHERE age > 30",
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&output);
    assert_eq!(result_batches.len(), 1);
    assert_eq!(result_batches[0].num_rows(), 2);
    assert_eq!(result_batches[0].num_columns(), 2);
}

#[tokio::test]
async fn test_vortex_query_with_scalar_functions() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            vortex_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex_file.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT UPPER(name) as upper_name, value * 2 as doubled FROM data",
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&output);
    assert_eq!(result_batches.len(), 1);
    assert_eq!(result_batches[0].num_rows(), 3);
    assert_eq!(result_batches[0].num_columns(), 2);
}

#[tokio::test]
async fn test_vortex_query_with_count_and_avg() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "Engineering",
                "Engineering",
                "Sales",
                "Sales",
                "Sales",
            ])),
            Arc::new(Int32Array::from(vec![100000, 120000, 80000, 90000, 85000])),
        ],
    )
    .unwrap();
    TestFile::write_arrow_batch(&input, &batch);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            vortex_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            vortex_file.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--query",
            "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM data GROUP BY department ORDER BY department",
        ])
        .assert()
        .success();

    let result_batches = TestFile::read_arrow(&output);
    assert_eq!(result_batches.len(), 1);
    assert_eq!(result_batches[0].num_rows(), 2);
    assert_eq!(result_batches[0].num_columns(), 3);
}

#[tokio::test]
async fn test_parquet_row_order_preserved() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let output = temp.path().join("output.parquet");

    // create schema with row_id for tracking order
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("value", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let num_batches = 10;
    let rows_per_batch = 50_000;
    let total_rows = num_batches * rows_per_batch;

    let mut batches = Vec::new();
    for batch_idx in 0..num_batches {
        let start = batch_idx * rows_per_batch;
        let row_ids: Vec<i64> = (start..start + rows_per_batch).map(i64::from).collect();
        let values: Vec<i32> = row_ids.iter().map(|&id| (id * 7 % 1000) as i32).collect();
        let names: Vec<String> = row_ids.iter().map(|&id| format!("row_{:06}", id)).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(row_ids)),
                Arc::new(Int32Array::from(values)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        batches.push(batch);
    }

    TestFile::write_arrow(&input, &batches);

    cargo::cargo_bin_cmd!("silk-chiffon")
        .args([
            "transform",
            "--from",
            input.to_str().unwrap(),
            "--to",
            output.to_str().unwrap(),
            "--parquet-row-group-size",
            "100000",
        ])
        .assert()
        .success();

    // use DataFusion to verify row order
    let ctx = SessionContext::new();

    ctx.register_parquet(
        "output",
        output.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let count_df = ctx.sql("SELECT COUNT(*) as cnt FROM output").await.unwrap();
    let count_batches = count_df.collect().await.unwrap();
    let count = count_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, i64::from(total_rows), "row count mismatch");

    // verify each row is in the correct position by checking row_id matches expected sequence
    // we do this by adding a row number and comparing
    let verify_df = ctx
        .sql(
            "WITH numbered AS (
                SELECT row_id, ROW_NUMBER() OVER (ORDER BY row_id) - 1 as expected_pos
                FROM output
            )
            SELECT COUNT(*) as mismatches
            FROM numbered
            WHERE row_id != expected_pos",
        )
        .await
        .unwrap();

    let verify_batches = verify_df.collect().await.unwrap();
    let mismatches = verify_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(mismatches, 0, "found rows out of order");

    let value_check_df = ctx
        .sql(
            "SELECT COUNT(*) as bad_values
            FROM output
            WHERE value != CAST((row_id * 7) % 1000 AS INT)",
        )
        .await
        .unwrap();

    let value_batches = value_check_df.collect().await.unwrap();
    let bad_values = value_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(bad_values, 0, "found rows with incorrect values");

    let name_check_df = ctx
        .sql(
            "SELECT COUNT(*) as bad_names
            FROM output
            WHERE name != CONCAT('row_', LPAD(CAST(row_id AS VARCHAR), 6, '0'))",
        )
        .await
        .unwrap();

    let name_batches = name_check_df.collect().await.unwrap();
    let bad_names = name_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(bad_names, 0, "found rows with incorrect names");
}
