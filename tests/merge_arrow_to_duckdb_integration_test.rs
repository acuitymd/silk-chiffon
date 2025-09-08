use assert_cmd::Command;
use duckdb::Connection;
use silk_chiffon::utils::test_helpers::test_data;
use tempfile::tempdir;

#[test]
fn test_merge_arrow_to_duckdb_basic() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("merged.db");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 3);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 4, 3);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "merged_data",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");
    assert!(output_path.exists(), "Output database was not created");

    let conn = Connection::open(&output_path).unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM merged_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 6, "Expected 6 rows in merged table");

    let mut stmt = conn
        .prepare("SELECT column_name FROM information_schema.columns WHERE table_name = 'merged_data' ORDER BY ordinal_position")
        .unwrap();
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(columns, vec!["id", "name", "value"]);
}

#[test]
fn test_merge_arrow_to_duckdb_with_sorting() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("sorted.db");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 10, 5);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 1, 5);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "sorted_data",
            "--sort-by",
            "id",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let conn = Connection::open(&output_path).unwrap();
    let ids: Vec<i32> = conn
        .prepare("SELECT id FROM sorted_data")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5, 10, 11, 12, 13, 14],
        "IDs should be sorted"
    );
}

#[test]
fn test_merge_arrow_to_duckdb_with_query() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("filtered.db");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 20);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 21, 20);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "filtered_data",
            "--query",
            "SELECT * FROM data WHERE id >= 10 AND id <= 30",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let conn = Connection::open(&output_path).unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM filtered_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 21, "Expected 21 rows after filtering");

    let min_id: i32 = conn
        .query_row("SELECT MIN(id) FROM filtered_data", [], |row| row.get(0))
        .unwrap();
    let max_id: i32 = conn
        .query_row("SELECT MAX(id) FROM filtered_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(min_id, 10, "Minimum ID should be 10");
    assert_eq!(max_id, 30, "Maximum ID should be 30");
}

#[test]
fn test_merge_arrow_to_duckdb_drop_table() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("test.db");

    test_data::create_arrow_file_with_range_of_ids(&input_path, 1, 5);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "test_table",
        ])
        .output()
        .expect("Failed to execute command");
    assert!(output.status.success());

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "test_table",
        ])
        .output()
        .expect("Failed to execute command");
    assert!(!output.status.success(), "Should fail when table exists");

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "test_table",
            "--drop-table",
        ])
        .output()
        .expect("Failed to execute command");
    assert!(
        output.status.success(),
        "Should succeed with drop-table flag"
    );

    let conn = Connection::open(&output_path).unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM test_table", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 5, "Table should have been recreated");
}

#[test]
fn test_merge_arrow_to_duckdb_truncate() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("test.db");

    test_data::create_arrow_file_with_range_of_ids(&input_path, 1, 5);

    let conn = Connection::open(&output_path).unwrap();
    conn.execute("CREATE TABLE other_table (x INT)", [])
        .unwrap();
    conn.execute("INSERT INTO other_table VALUES (42)", [])
        .unwrap();
    drop(conn);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "new_table",
            "--truncate",
        ])
        .output()
        .expect("Failed to execute command");
    assert!(output.status.success());

    let conn = Connection::open(&output_path).unwrap();

    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM new_table", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 5);

    let result = conn.execute("SELECT * FROM other_table", []);
    assert!(result.is_err(), "Old table should not exist after truncate");
}

#[test]
fn test_merge_arrow_to_duckdb_glob_pattern() {
    let temp_dir = tempdir().unwrap();
    let output_path = temp_dir.path().join("merged_glob.db");

    for i in 0..4 {
        let path = temp_dir.path().join(format!("chunk_{i}.arrow"));
        test_data::create_arrow_file_with_range_of_ids(&path, i * 25, 25);
    }

    let glob_pattern = format!("{}/chunk_*.arrow", temp_dir.path().display());
    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            &glob_pattern,
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "glob_merged",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let conn = Connection::open(&output_path).unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM glob_merged", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 100, "Expected 100 rows from 4 files");
}

#[test]
fn test_merge_arrow_to_duckdb_with_record_batch_size() {
    let temp_dir = tempdir().unwrap();
    let input1_path = temp_dir.path().join("input1.arrow");
    let input2_path = temp_dir.path().join("input2.arrow");
    let output_path = temp_dir.path().join("batched.db");

    test_data::create_arrow_file_with_range_of_ids(&input1_path, 1, 1000);
    test_data::create_arrow_file_with_range_of_ids(&input2_path, 1001, 1000);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input1_path.to_str().unwrap(),
            input2_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-t",
            "batched_data",
            "--record-batch-size",
            "250",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {output:?}");

    let conn = Connection::open(&output_path).unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM batched_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2000, "Expected 2000 rows");
}

#[test]
fn test_merge_arrow_to_duckdb_error_no_table_name() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    test_data::create_arrow_file_with_range_of_ids(&input_path, 1, 5);

    let output = Command::cargo_bin("silk-chiffon")
        .unwrap()
        .args([
            "merge-arrow-to-duckdb",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        !output.status.success(),
        "Command should fail without table name"
    );
}
