use assert_cmd::cargo;
use camino::Utf8Path;
use predicates::prelude::*;
use silk_chiffon::inspection::parquet::ParquetInspector;
use silk_chiffon::utils::test_data::{TestBatch, TestFile};
use tempfile::TempDir;

fn inspect(path: &std::path::Path) -> ParquetInspector {
    ParquetInspector::open(Utf8Path::from_path(path).unwrap()).unwrap()
}

#[test]
fn test_transform_help() {
    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["transform", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Transform data between formats"))
        .stdout(predicate::str::contains("Single input file"))
        .stdout(predicate::str::contains("Multiple input file"))
        .stdout(predicate::str::contains("Single output file"))
        .stdout(predicate::str::contains(
            "Output path template for partitioning",
        ));
}

#[test]
fn test_transform_missing_args() {
    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.arg("transform")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required").or(predicate::str::contains("Usage:")));
}

#[test]
fn test_transform_from_missing_to() {
    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["transform", "--from", "/non/existent/file.arrow"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required").or(predicate::str::contains("Usage:")));
}

#[test]
fn test_transform_from_many_missing_to() {
    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args(["transform", "--from-many", "*.arrow"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required").or(predicate::str::contains("Usage:")));
}

#[test]
fn test_transform_from_to_arrow_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
    assert!(std::fs::metadata(&output).unwrap().len() > 0);
}

#[test]
fn test_transform_from_to_arrow_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
    assert!(std::fs::metadata(&output).unwrap().len() > 0);
}

#[test]
fn test_transform_from_to_parquet_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
    assert!(std::fs::metadata(&output).unwrap().len() > 0);
}

#[test]
fn test_transform_from_to_parquet_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.parquet");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_parquet_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
    assert!(std::fs::metadata(&output).unwrap().len() > 0);
}

#[test]
fn test_transform_from_many_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
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

    assert!(output.exists());
    assert!(std::fs::metadata(&output).unwrap().len() > 0);
}

#[test]
fn test_transform_from_many_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
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

    assert!(output.exists());
    assert!(std::fs::metadata(&output).unwrap().len() > 0);
}

#[test]
fn test_transform_from_many_glob_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    let glob_pattern = temp_dir.path().join("*.arrow");

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from-many",
        glob_pattern.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
    assert!(std::fs::metadata(&output).unwrap().len() > 0);
}

#[test]
fn test_transform_from_to_many_arrow_partitioned() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "a"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
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

    // check that partition files were created
    let output_a = temp_dir.path().join("a.arrow");
    let output_b = temp_dir.path().join("b.arrow");
    assert!(output_a.exists());
    assert!(output_b.exists());
}

#[test]
fn test_transform_from_to_many_parquet_partitioned() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "a"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
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

    // check that partition files were created
    let output_a = temp_dir.path().join("a.parquet");
    let output_b = temp_dir.path().join("b.parquet");
    assert!(output_a.exists());
    assert!(output_b.exists());
}

#[test]
fn test_transform_from_many_to_many_partitioned() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output_template = temp_dir.path().join("{{name}}.parquet");

    let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
    let batch2 = TestBatch::simple_with(&[3, 4], &["a", "c"]);
    TestFile::write_arrow_batch(&input1, &batch1);
    TestFile::write_arrow_batch(&input2, &batch2);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
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

    // check that partition files were created
    let output_a = temp_dir.path().join("a.parquet");
    let output_b = temp_dir.path().join("b.parquet");
    let output_c = temp_dir.path().join("c.parquet");
    assert!(output_a.exists());
    assert!(output_b.exists());
    assert!(output_c.exists());
}

#[test]
fn test_transform_with_query() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--query",
        "SELECT * FROM data WHERE id > 1",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
}

#[test]
fn test_transform_with_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--sort-by",
        "id",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
}

#[test]
fn test_transform_with_arrow_compression() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--arrow-compression",
        "zstd",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
}

#[test]
fn test_transform_with_parquet_compression() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--parquet-compression",
        "snappy",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
}

#[test]
fn test_transform_explicit_formats() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.data");
    let output = temp_dir.path().join("output.data");

    let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--input-format",
        "arrow",
        "--output-format",
        "parquet",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(output.exists());
}

#[test]
fn test_cli_dictionary_prefix_matches_nested_columns() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
        "--parquet-dictionary-all-off",
        "--parquet-dictionary-column",
        "person:always",
    ])
    .assert()
    .success();

    let inspector = inspect(&output);
    let id_col = inspector.column("id").unwrap();
    let name_col = inspector.column("person.name").unwrap();
    let age_col = inspector.column("person.age").unwrap();

    assert!(!id_col.has_dictionary, "id should NOT have dictionary");
    assert!(
        name_col.has_dictionary,
        "person.name should have dictionary"
    );
    assert!(age_col.has_dictionary, "person.age should have dictionary");
}

#[test]
fn test_cli_dictionary_specific_nested_path() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
        "--parquet-dictionary-all-off",
        "--parquet-dictionary-column",
        "person.name:always",
    ])
    .assert()
    .success();

    let inspector = inspect(&output);
    let id_col = inspector.column("id").unwrap();
    let name_col = inspector.column("person.name").unwrap();
    let age_col = inspector.column("person.age").unwrap();

    assert!(!id_col.has_dictionary, "id should NOT have dictionary");
    assert!(
        name_col.has_dictionary,
        "person.name should have dictionary"
    );
    assert!(
        !age_col.has_dictionary,
        "person.age should NOT have dictionary"
    );
}

#[test]
fn test_cli_bloom_filter_prefix_matches_nested_columns() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
        "--parquet-bloom-column",
        "person:ndv=100",
    ])
    .assert()
    .success();

    let inspector = inspect(&output);
    let id_col = inspector.column("id").unwrap();
    let name_col = inspector.column("person.name").unwrap();
    let age_col = inspector.column("person.age").unwrap();

    assert!(!id_col.has_bloom_filter, "id should NOT have bloom filter");
    assert!(
        name_col.has_bloom_filter,
        "person.name should have bloom filter"
    );
    assert!(
        age_col.has_bloom_filter,
        "person.age should have bloom filter"
    );
}

#[test]
fn test_cli_bloom_filter_prefix_with_exclusion() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let batch = TestBatch::with_structs();
    TestFile::write_arrow_batch(&input, &batch);

    let mut cmd = cargo::cargo_bin_cmd!("silk-chiffon");
    cmd.args([
        "transform",
        "--from",
        input.to_str().unwrap(),
        "--to",
        output.to_str().unwrap(),
        "--parquet-bloom-column",
        "person:ndv=100",
        "--parquet-bloom-column-off",
        "person.age",
    ])
    .assert()
    .success();

    let inspector = inspect(&output);
    let id_col = inspector.column("id").unwrap();
    let name_col = inspector.column("person.name").unwrap();
    let age_col = inspector.column("person.age").unwrap();

    assert!(!id_col.has_bloom_filter, "id should NOT have bloom filter");
    assert!(
        name_col.has_bloom_filter,
        "person.name should have bloom filter"
    );
    assert!(
        !age_col.has_bloom_filter,
        "person.age should NOT have bloom filter"
    );
}
