use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use assert_cmd::cargo;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn simple_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn create_batch(schema: &Arc<Schema>, ids: &[i32], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn write_arrow_file(path: &Path, schema: &Arc<Schema>, batches: Vec<RecordBatch>) {
    use arrow::ipc::writer::FileWriter;

    let file = File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, schema).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
}

fn read_arrow_file(path: &Path) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    reader.collect::<Result<Vec<_>, _>>().unwrap()
}

fn read_parquet_file(path: &Path) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    reader.collect::<Result<Vec<_>, _>>().unwrap()
}

fn extract_ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch.column_by_name("id").unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..arr.len() {
            ids.push(arr.value(i));
        }
    }
    ids
}

fn extract_names(batches: &[RecordBatch]) -> Vec<String> {
    let mut names = Vec::new();
    for batch in batches {
        let col = batch.column_by_name("name").unwrap();
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..arr.len() {
            names.push(arr.value(i).to_string());
        }
    }
    names
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_arrow_to_arrow() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(extract_ids(&batches), vec![1, 2, 3]);
    assert_eq!(extract_names(&batches), vec!["a", "b", "c"]);
}

#[test]
fn test_arrow_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[10, 20, 30], &["x", "y", "z"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_parquet_file(&output);
    assert_eq!(extract_ids(&batches), vec![10, 20, 30]);
    assert_eq!(extract_names(&batches), vec!["x", "y", "z"]);
}

#[test]
fn test_merge_two_files() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("merged.arrow");

    let schema = simple_schema();
    let batch1 = create_batch(&schema, &[1, 2], &["a", "b"]);
    let batch2 = create_batch(&schema, &[3, 4], &["c", "d"]);
    write_arrow_file(&input1, &schema, vec![batch1]);
    write_arrow_file(&input2, &schema, vec![batch2]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(count_rows(&batches), 4);

    let mut ids = extract_ids(&batches);
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

    let schema = simple_schema();
    write_arrow_file(&input1, &schema, vec![create_batch(&schema, &[1], &["a"])]);
    write_arrow_file(&input2, &schema, vec![create_batch(&schema, &[2], &["b"])]);
    write_arrow_file(&input3, &schema, vec![create_batch(&schema, &[3], &["c"])]);

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

    let batches = read_parquet_file(&output);
    let mut ids = extract_ids(&batches);
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn test_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3, 4, 5], &["x", "y", "x", "y", "z"]);
    write_arrow_file(&input, &schema, vec![batch]);

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
    let batches_x = read_arrow_file(&output_x);
    assert_eq!(extract_ids(&batches_x), vec![1, 3]);
    assert_eq!(extract_names(&batches_x), vec!["x", "x"]);

    let output_y = temp_dir.path().join("y.arrow");
    let batches_y = read_arrow_file(&output_y);
    assert_eq!(extract_ids(&batches_y), vec![2, 4]);
    assert_eq!(extract_names(&batches_y), vec!["y", "y"]);

    let output_z = temp_dir.path().join("z.arrow");
    let batches_z = read_arrow_file(&output_z);
    assert_eq!(extract_ids(&batches_z), vec![5]);
}

#[test]
fn test_partition_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("category_{{name}}.parquet");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[100, 200, 300], &["alpha", "beta", "alpha"]);
    write_arrow_file(&input, &schema, vec![batch]);

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
    let batches_alpha = read_parquet_file(&output_alpha);
    assert_eq!(extract_ids(&batches_alpha), vec![100, 300]);

    let output_beta = temp_dir.path().join("category_beta.parquet");
    let batches_beta = read_parquet_file(&output_beta);
    assert_eq!(extract_ids(&batches_beta), vec![200]);
}

#[test]
fn test_merge_and_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output_template = temp_dir.path().join("{{name}}.parquet");

    let schema = simple_schema();
    let batch1 = create_batch(&schema, &[1, 2, 3], &["a", "b", "a"]);
    let batch2 = create_batch(&schema, &[4, 5, 6], &["b", "c", "c"]);
    write_arrow_file(&input1, &schema, vec![batch1]);
    write_arrow_file(&input2, &schema, vec![batch2]);

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
    assert_eq!(extract_ids(&read_parquet_file(&output_a)), vec![1, 3]);

    let output_b = temp_dir.path().join("b.parquet");
    assert_eq!(extract_ids(&read_parquet_file(&output_b)), vec![2, 4]);

    let output_c = temp_dir.path().join("c.parquet");
    assert_eq!(extract_ids(&read_parquet_file(&output_c)), vec![5, 6]);
}

#[test]
fn test_sort_ascending() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[5, 2, 8, 1, 3], &["e", "b", "h", "a", "c"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(extract_ids(&batches), vec![1, 2, 3, 5, 8]);
}

#[test]
fn test_sort_descending() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[5, 2, 8, 1, 3], &["e", "b", "h", "a", "c"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(extract_ids(&batches), vec![8, 5, 3, 2, 1]);
}

#[test]
fn test_sort_by_name() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(
        &schema,
        &[5, 2, 8, 1, 3],
        &["zebra", "bear", "ant", "dog", "cat"],
    );
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(
        extract_names(&batches),
        vec!["ant", "bear", "cat", "dog", "zebra"]
    );
}

#[test]
fn test_query_filter() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(extract_ids(&batches), vec![3, 4]);
    assert_eq!(extract_names(&batches), vec!["c", "d"]);
}

#[test]
fn test_query_projection() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(extract_names(&batches), vec!["a", "b", "c"]);
}

#[test]
fn test_query_and_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[5, 2, 8, 1, 3], &["e", "b", "h", "a", "c"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(extract_ids(&batches), vec![5, 3, 2, 1]);
}

#[test]
fn test_query_and_partition() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let schema = simple_schema();
    let batch = create_batch(
        &schema,
        &[1, 2, 3, 4, 5, 6],
        &["a", "b", "a", "b", "c", "c"],
    );
    write_arrow_file(&input, &schema, vec![batch]);

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
    assert_eq!(extract_ids(&read_arrow_file(&output_a)), vec![3]);

    let output_b = temp_dir.path().join("b.arrow");
    assert_eq!(extract_ids(&read_arrow_file(&output_b)), vec![4]);

    let output_c = temp_dir.path().join("c.arrow");
    assert_eq!(extract_ids(&read_arrow_file(&output_c)), vec![5, 6]);
}

#[test]
fn test_merge_and_sort() {
    let temp_dir = TempDir::new().unwrap();
    let input1 = temp_dir.path().join("input1.arrow");
    let input2 = temp_dir.path().join("input2.arrow");
    let output = temp_dir.path().join("output.arrow");

    let schema = simple_schema();
    let batch1 = create_batch(&schema, &[5, 2], &["e", "b"]);
    let batch2 = create_batch(&schema, &[8, 1, 3], &["h", "a", "c"]);
    write_arrow_file(&input1, &schema, vec![batch1]);
    write_arrow_file(&input2, &schema, vec![batch2]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(extract_ids(&batches), vec![1, 2, 3, 5, 8]);
}

#[test]
fn test_list_outputs_text() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3], &["x", "y", "x"]);
    write_arrow_file(&input, &schema, vec![batch]);

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
    assert_eq!(count_rows(&read_arrow_file(&output_x)), 2);
}

#[test]
fn test_list_outputs_json() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output_template = temp_dir.path().join("{{name}}.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2], &["a", "b"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_arrow_file(&output);
    assert_eq!(count_rows(&batches), 0);
}

#[test]
fn test_single_row() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let output = temp_dir.path().join("output.parquet");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[42], &["single"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let batches = read_parquet_file(&output);
    assert_eq!(extract_ids(&batches), vec![42]);
}

#[tokio::test]
async fn test_arrow_to_vortex() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let output = temp.path().join("output.vortex");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3], &["Alice", "Bob", "Charlie"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3], &["Alice", "Bob", "Charlie"]);
    write_arrow_file(&arrow1, &schema, vec![batch.clone()]);

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

    let result_batches = read_arrow_file(&arrow2);
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

    let schema = simple_schema();
    let batches = vec![
        create_batch(&schema, &[1, 2], &["Alice", "Bob"]),
        create_batch(&schema, &[3, 4], &["Charlie", "Diana"]),
        create_batch(&schema, &[5], &["Eve"]),
    ];
    write_arrow_file(&input, &schema, batches);

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

    let result_batches = read_arrow_file(&output);
    let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);
}

#[tokio::test]
async fn test_vortex_with_query() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3, 4, 5], &["A", "B", "C", "D", "E"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let result_batches = read_arrow_file(&output);
    let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_vortex_with_sort() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.arrow");
    let vortex_file = temp.path().join("data.vortex");
    let output = temp.path().join("output.arrow");

    let schema = simple_schema();
    let batch = create_batch(&schema, &[3, 1, 2], &["C", "A", "B"]);
    write_arrow_file(&input, &schema, vec![batch]);

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

    let result_batches = read_arrow_file(&output);
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

    let schema = simple_schema();
    let batch = create_batch(&schema, &[1, 2, 3, 4, 5], &["A", "B", "C", "D", "E"]);
    write_arrow_file(&input, &schema, vec![batch]);

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
    write_arrow_file(&input, &schema, vec![batch]);

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

    let result_batches = read_arrow_file(&output);
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
    write_arrow_file(&input, &schema, vec![batch]);

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

    let result_batches = read_arrow_file(&output);
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
    write_arrow_file(&input, &schema, vec![batch]);

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

    let result_batches = read_arrow_file(&output);
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
    write_arrow_file(&input, &schema, vec![batch]);

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

    let result_batches = read_arrow_file(&output);
    assert_eq!(result_batches.len(), 1);
    assert_eq!(result_batches[0].num_rows(), 2);
    assert_eq!(result_batches[0].num_columns(), 3);
}
