use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use duckdb::Connection;
use std::fs::File;
use std::process::Command;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_duckdb_conversion_basic() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "people",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);
    assert!(output_path.exists(), "Database file was not created");

    let conn = Connection::open(&output_path).unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM people", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_duckdb_table_already_exists() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "people",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "people",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("already exists"));
}

#[test]
fn test_duckdb_with_drop_table() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![100])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch1).unwrap();
    writer.finish().unwrap();

    Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "data",
        ])
        .output()
        .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![200])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch2).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "data",
            "--drop-table",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    let conn = Connection::open(&output_path).unwrap();
    let value: i32 = conn
        .query_row("SELECT value FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 200);
}

#[test]
fn test_duckdb_with_sorting() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 1, 2])),
            Arc::new(StringArray::from(vec!["Charlie", "Alice", "Bob"])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "sorted_data",
            "--sort-by",
            "id",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let conn = Connection::open(&output_path).unwrap();
    let mut stmt = conn.prepare("SELECT id FROM sorted_data").unwrap();
    let ids: Vec<i32> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn test_duckdb_add_multiple_tables() {
    let temp_dir = tempdir().unwrap();

    let users_path = temp_dir.path().join("users.arrow");
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let users_batch = RecordBatch::try_new(
        users_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let file = File::create(&users_path).unwrap();
    let mut writer = FileWriter::try_new(file, &users_schema).unwrap();
    writer.write(&users_batch).unwrap();
    writer.finish().unwrap();

    let products_path = temp_dir.path().join("products.arrow");
    let products_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let products_batch = RecordBatch::try_new(
        products_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![100, 101, 102])),
            Arc::new(StringArray::from(vec!["Widget", "Gadget", "Tool"])),
        ],
    )
    .unwrap();

    let file = File::create(&products_path).unwrap();
    let mut writer = FileWriter::try_new(file, &products_schema).unwrap();
    writer.write(&products_batch).unwrap();
    writer.finish().unwrap();

    let output_path = temp_dir.path().join("multi_table.db");
    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            users_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "users",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Failed to create users table: {:?}",
        output
    );

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            products_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "products",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Failed to create products table: {:?}",
        output
    );

    let conn = Connection::open(&output_path).unwrap();

    let user_count: i32 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .unwrap();
    assert_eq!(user_count, 3);

    let product_count: i32 = conn
        .query_row("SELECT COUNT(*) FROM products", [], |row| row.get(0))
        .unwrap();
    assert_eq!(product_count, 3);

    let join_count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM users CROSS JOIN products",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(join_count, 9); // 3 users * 3 products
}

#[test]
fn test_duckdb_preserves_insertion_order() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("position", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let positions = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let values: Vec<String> = positions
        .iter()
        .map(|i| format!("value_{:03}", i))
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(positions.clone())),
            Arc::new(StringArray::from(values.clone())),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "test_order",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let conn = Connection::open(&output_path).unwrap();
    let mut stmt = conn
        .prepare("SELECT position, value FROM test_order")
        .unwrap();
    let results: Vec<(i32, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    for (i, (pos, val)) in results.iter().enumerate() {
        assert_eq!(*pos, i as i32, "Position {} found at index {}", pos, i);
        assert_eq!(
            val,
            &format!("value_{:03}", i),
            "Value mismatch at position {}",
            i
        );
    }
}

#[test]
fn test_duckdb_preserves_sorted_order() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![5, 3, 8, 1, 9, 2, 7, 4, 6])),
            Arc::new(StringArray::from(vec![
                "B", "A", "C", "A", "C", "A", "B", "B", "C",
            ])),
            Arc::new(Int32Array::from(vec![50, 30, 80, 10, 90, 20, 70, 40, 60])),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "sorted_data",
            "--sort-by",
            "category,id:desc",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let conn = Connection::open(&output_path).unwrap();
    let mut stmt = conn
        .prepare("SELECT category, id FROM sorted_data")
        .unwrap();
    let results: Vec<(String, i32)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let expected = vec![
        ("A", 3),
        ("A", 2),
        ("A", 1),
        ("B", 7),
        ("B", 5),
        ("B", 4),
        ("C", 9),
        ("C", 8),
        ("C", 6),
    ];

    for (i, ((cat, id), (exp_cat, exp_id))) in results.iter().zip(expected.iter()).enumerate() {
        assert_eq!(cat, exp_cat, "Category mismatch at position {}", i);
        assert_eq!(*id, *exp_id, "ID mismatch at position {}", i);
    }
}

#[test]
fn test_duckdb_preserves_order_with_limit_offset() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_path = temp_dir.path().join("output.db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let seq_nums: Vec<i32> = (0..100).collect();
    let data: Vec<String> = seq_nums.iter().map(|i| format!("row_{:03}", i)).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(seq_nums)),
            Arc::new(StringArray::from(data)),
        ],
    )
    .unwrap();

    let file = File::create(&input_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--",
            "duckdb",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            "--table-name",
            "seq_data",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    let conn = Connection::open(&output_path).unwrap();

    let mut stmt = conn.prepare("SELECT seq FROM seq_data LIMIT 5").unwrap();
    let first_5: Vec<i32> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(first_5, vec![0, 1, 2, 3, 4]);

    let mut stmt = conn
        .prepare("SELECT seq FROM seq_data LIMIT 5 OFFSET 10")
        .unwrap();
    let rows_10_14: Vec<i32> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows_10_14, vec![10, 11, 12, 13, 14]);

    let mut stmt = conn.prepare("SELECT seq FROM seq_data").unwrap();
    let all_rows: Vec<i32> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    for (i, seq) in all_rows.iter().enumerate() {
        assert_eq!(*seq, i as i32, "Sequence {} found at position {}", seq, i);
    }
}
