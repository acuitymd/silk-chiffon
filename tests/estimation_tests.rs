//! Tests for variable-width column size estimation across all formats.
//!
//! All formats share a unified estimation path: read data through DataFusion's
//! `as_stream()`, decode to Arrow, and measure with `array_data_size`. This
//! approach gives accurate in-memory size estimates regardless of the source
//! format's internal encoding.

use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use silk_chiffon::sources::arrow::ArrowDataSource;
use silk_chiffon::sources::data_source::DataSource;
use silk_chiffon::sources::parquet::ParquetDataSource;
use silk_chiffon::sources::vortex::VortexDataSource;
use silk_chiffon::utils::test_data::TestFile;

const N: usize = 10_000;

/// Batch with one variable-width column of each type.
/// Known, predictable sizes for assertion ranges.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn variable_width_batch() -> RecordBatch {
    // Utf8: 10-char strings → ~10 bytes/row data
    let short_strings: Vec<String> = (0..N).map(|i| format!("{:0>10}", i)).collect();
    let short_refs: Vec<&str> = short_strings.iter().map(String::as_str).collect();
    let utf8_col: ArrayRef = Arc::new(StringArray::from(short_refs.clone()));

    // LargeUtf8: same 10-char strings but with i64 offsets
    let large_utf8_col: ArrayRef = Arc::new(LargeStringArray::from(short_refs));

    // Binary: 16-byte varying blobs (must vary for parquet metadata accuracy)
    let binary_vecs: Vec<Vec<u8>> = (0..N)
        .map(|i| {
            let mut v = vec![0u8; 16];
            let bytes = (i as u64).to_le_bytes();
            v[..8].copy_from_slice(&bytes);
            v
        })
        .collect();
    let binary_refs: Vec<&[u8]> = binary_vecs.iter().map(|v| v.as_slice()).collect();
    let binary_col: ArrayRef = Arc::new(BinaryArray::from(binary_refs.clone()));

    // LargeBinary: same 16-byte blobs with i64 offsets
    let large_binary_col: ArrayRef = Arc::new(LargeBinaryArray::from(binary_refs));

    // List<Int32>: 3 elements per list → 12 bytes/row child data
    let list_i32_flat: Vec<i32> = (0..N * 3).map(|i| i as i32).collect();
    let list_i32_values: ArrayRef = Arc::new(Int32Array::from(list_i32_flat));
    let mut offsets = Vec::with_capacity(N + 1);
    for i in 0..=N {
        offsets.push((i * 3) as i32);
    }
    let list_i32_col: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, false)),
        OffsetBuffer::new(offsets.into()),
        list_i32_values,
        None,
    ));

    // List<Utf8>: 2 strings of 5 chars each per list → ~10 bytes/row string data
    let list_str_flat: Vec<String> = (0..N * 2).map(|i| format!("{:0>5}", i)).collect();
    let list_str_refs: Vec<&str> = list_str_flat.iter().map(String::as_str).collect();
    let list_str_values: ArrayRef = Arc::new(StringArray::from(list_str_refs));
    let mut str_offsets = Vec::with_capacity(N + 1);
    for i in 0..=N {
        str_offsets.push((i * 2) as i32);
    }
    let list_utf8_col: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Utf8, false)),
        OffsetBuffer::new(str_offsets.into()),
        list_str_values,
        None,
    ));

    // Dictionary<Int32, Utf8>: 100 unique 8-char values, N rows
    let dict_keys: Vec<i32> = (0..N).map(|i| (i % 100) as i32).collect();
    let dict_values: Vec<String> = (0..100).map(|i| format!("{:0>8}", i)).collect();
    let dict_value_refs: Vec<&str> = dict_values.iter().map(String::as_str).collect();
    let dict_col: ArrayRef = Arc::new(
        DictionaryArray::try_new(
            Int32Array::from(dict_keys),
            Arc::new(StringArray::from(dict_value_refs)),
        )
        .unwrap(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("utf8_col", DataType::Utf8, false),
        Field::new("large_utf8_col", DataType::LargeUtf8, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new("large_binary_col", DataType::LargeBinary, false),
        Field::new(
            "list_i32_col",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            false,
        ),
        Field::new(
            "list_utf8_col",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, false))),
            false,
        ),
        Field::new(
            "dict_col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            utf8_col,
            large_utf8_col,
            binary_col,
            large_binary_col,
            list_i32_col,
            list_utf8_col,
            dict_col,
        ],
    )
    .unwrap()
}

fn variable_col_names() -> Vec<String> {
    vec![
        "utf8_col".into(),
        "large_utf8_col".into(),
        "binary_col".into(),
        "large_binary_col".into(),
        "list_i32_col".into(),
        "list_utf8_col".into(),
        "dict_col".into(),
    ]
}

/// Batch with a struct column containing variable-width children.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn struct_batch(n: usize) -> RecordBatch {
    let names: Vec<String> = (0..n).map(|i| format!("{:0>10}", i)).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let ids: Vec<i32> = (0..n).map(|i| i as i32).collect();

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(StringArray::from(name_refs)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Int32Array::from(ids.clone())) as ArrayRef,
        ),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("id", DataType::Int32, false),
                ]
                .into(),
            ),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Batch with 50% null strings (20-char when present).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn nullable_string_batch(n: usize) -> RecordBatch {
    let values: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 2 == 0 {
                Some(format!("{:0>20}", i))
            } else {
                None
            }
        })
        .collect();
    let refs: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nullable_str",
            DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(refs)) as ArrayRef],
    )
    .unwrap()
}

/// Simplified batch without types that vortex doesn't support (LargeUtf8,
/// LargeBinary, Dictionary). Vortex normalizes these to their canonical forms
/// (Utf8, Binary, materialized strings), which changes the column types in the
/// round-tripped schema. Use a focused batch for clean testing.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn vortex_compatible_batch() -> RecordBatch {
    let n = N;
    let strings: Vec<String> = (0..n).map(|i| format!("{:0>10}", i)).collect();
    let string_refs: Vec<&str> = strings.iter().map(String::as_str).collect();
    let utf8_col: ArrayRef = Arc::new(StringArray::from(string_refs));

    let binary_vecs: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut v = vec![0u8; 16];
            let bytes = (i as u64).to_le_bytes();
            v[..8].copy_from_slice(&bytes);
            v
        })
        .collect();
    let binary_refs: Vec<&[u8]> = binary_vecs.iter().map(|v| v.as_slice()).collect();
    let binary_col: ArrayRef = Arc::new(BinaryArray::from(binary_refs));

    // List<Int32>: 3 elements per list
    let list_flat: Vec<i32> = (0..n * 3).map(|i| i as i32).collect();
    let list_values: ArrayRef = Arc::new(Int32Array::from(list_flat));
    let mut offsets = Vec::with_capacity(n + 1);
    for i in 0..=n {
        offsets.push((i * 3) as i32);
    }
    let list_col: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, false)),
        OffsetBuffer::new(offsets.into()),
        list_values,
        None,
    ));

    let schema = Arc::new(Schema::new(vec![
        Field::new("utf8_col", DataType::Utf8, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new(
            "list_i32_col",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            false,
        ),
    ]));

    RecordBatch::try_new(schema, vec![utf8_col, binary_col, list_col]).unwrap()
}

fn assert_range(name: &str, estimate: usize, min: usize, max: usize) {
    assert!(
        estimate >= min && estimate <= max,
        "{name}: estimate {estimate} outside expected range [{min}, {max}]"
    );
}

fn check_estimates(
    estimates: &std::collections::HashMap<String, usize>,
    context: &str,
    expectations: &[(&str, usize, usize)],
) {
    for (col, min, max) in expectations {
        let est = estimates
            .get(*col)
            .unwrap_or_else(|| panic!("{context}: missing estimate for {col}"));
        assert_range(&format!("{context}/{col}"), *est, *min, *max);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_arrow_ipc_all_variable_width_types() {
    let batch = variable_width_batch();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let cols = variable_col_names();
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    // arrow IPC reads directly — measures actual buffer lengths
    check_estimates(
        &estimates,
        "arrow_ipc",
        &[
            // Utf8: ~10 data + 4 offset = ~14/row
            ("utf8_col", 10, 20),
            // LargeUtf8: ~10 data + 8 offset = ~18/row
            ("large_utf8_col", 14, 24),
            // Binary: 16 data + 4 offset = ~20/row
            ("binary_col", 16, 24),
            // LargeBinary: 16 data + 8 offset = ~24/row
            ("large_binary_col", 20, 28),
            // List<i32>: 4 list offset + 3*4=12 child data = ~16/row
            ("list_i32_col", 12, 20),
            // List<Utf8>: 4 list offset + 2*(5 data + 4 str offset) = ~22/row
            ("list_utf8_col", 15, 28),
            // Dict<i32,Utf8>: 4 key + shared values (~8*100/10000 ≈ tiny) = ~4-5/row
            ("dict_col", 4, 10),
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_arrow_ipc_struct() {
    let batch = struct_batch(5_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("struct_test.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["data".to_string()];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    let est = *estimates
        .get("data")
        .expect("missing estimate for struct column");
    // struct{name: Utf8(10 chars), id: Int32}: ~10 data + 4 str_offset + 4 int = ~18/row
    assert_range("arrow_ipc/struct", est, 14, 24);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_arrow_ipc_nullable_strings() {
    let batch = nullable_string_batch(10_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nullable.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["nullable_str".to_string()];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    let est = *estimates.get("nullable_str").unwrap();
    // 50% nulls with 20-char strings: avg ~10 data + 4 offset + ~0.1 null bitmap ≈ 14
    assert_range("arrow_ipc/nullable_str", est, 10, 20);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_row_size_arrow_ipc() {
    let batch = variable_width_batch();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let row_size = source.row_size().unwrap();
    assert!(
        row_size < 200,
        "row_size {row_size} is suspiciously high — possible estimation inflation"
    );
    assert!(
        row_size > 50,
        "row_size {row_size} is suspiciously low — possible undercount"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_all_variable_width_types() {
    let batch = variable_width_batch();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let cols = variable_col_names();
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    // reads through DataFusion which coerces Utf8→Utf8View, Binary→BinaryView
    check_estimates(
        &estimates,
        "parquet",
        &[
            ("utf8_col", 8, 55),
            ("large_utf8_col", 8, 55),
            ("binary_col", 14, 65),
            ("large_binary_col", 14, 65),
            ("list_i32_col", 8, 50),
            ("list_utf8_col", 8, 65),
            ("dict_col", 1, 30),
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_struct() {
    let batch = struct_batch(5_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("struct_test.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["data".to_string()];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    let est = *estimates
        .get("data")
        .expect("missing estimate for struct column");
    // struct{name: Utf8View(10 chars + 16 view), id: Int32(4)}: ~30+/row
    assert_range("parquet/struct", est, 10, 70);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_nullable_strings() {
    let batch = nullable_string_batch(10_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nullable.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["nullable_str".to_string()];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    let est = *estimates.get("nullable_str").unwrap();
    // 50% nulls with 20-char strings, read through DataFusion as Utf8View (16-byte views)
    assert_range("parquet/nullable_str", est, 5, 50);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_row_size_parquet() {
    let batch = variable_width_batch();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let row_size = source.row_size().unwrap();
    // DataFusion reads as Utf8View/BinaryView which are larger than raw metadata sizes
    assert!(row_size < 400, "row_size {row_size} is suspiciously high");
    assert!(row_size > 30, "row_size {row_size} is suspiciously low");
}

/// Large list (10 ints per row) — verify estimate scales with list size.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_large_list() {
    // list with 10 elements each — num_values will be 10x num_rows
    let n = 5_000;
    let flat: Vec<i32> = (0..n * 10).map(|i| i as i32).collect();
    let values: ArrayRef = Arc::new(Int32Array::from(flat));
    let mut offsets = Vec::with_capacity(n + 1);
    for i in 0..=n {
        offsets.push((i * 10) as i32);
    }
    let list_col: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, false)),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ));

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "big_list",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            false,
        )])),
        vec![list_col],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big_list.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["big_list".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("big_list").unwrap();
    // 10 ints × 4 bytes = 40 bytes/row of actual data, plus list offsets
    assert_range("parquet/big_list", est, 30, 70);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_vortex_variable_width_types() {
    let batch = vortex_compatible_batch();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.vtx");
    TestFile::write_vortex_batch(&path, &batch);

    let source = VortexDataSource::new(path.to_str().unwrap().to_string());

    let cols = vec![
        "utf8_col".to_string(),
        "binary_col".to_string(),
        "list_i32_col".to_string(),
    ];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    check_estimates(
        &estimates,
        "vortex",
        &[
            // Utf8: ~10 data + overhead. vortex reads as Utf8View.
            ("utf8_col", 5, 30),
            // Binary: 16 bytes + overhead
            ("binary_col", 10, 35),
            // List<i32>: 3 ints per row (12 bytes) + overhead
            ("list_i32_col", 8, 40),
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_vortex_struct() {
    let batch = struct_batch(5_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("struct_test.vtx");
    TestFile::write_vortex_batch(&path, &batch);

    let source = VortexDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["data".to_string()];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    let est = *estimates
        .get("data")
        .expect("missing estimate for struct column");
    // struct{name: Utf8(10 chars), id: Int32}: ~18/row via array_data_size
    assert_range("vortex/struct", est, 10, 35);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_vortex_nullable_strings() {
    let batch = nullable_string_batch(10_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nullable.vtx");
    TestFile::write_vortex_batch(&path, &batch);

    let source = VortexDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["nullable_str".to_string()];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    let est = *estimates.get("nullable_str").unwrap();
    // 50% nulls with 20-char strings: avg ~10 data + vortex validity buffer overhead
    assert_range("vortex/nullable_str", est, 5, 35);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_row_size_vortex() {
    let batch = vortex_compatible_batch();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.vtx");
    TestFile::write_vortex_batch(&path, &batch);

    let source = VortexDataSource::new(path.to_str().unwrap().to_string());
    let row_size = source.row_size().unwrap();
    assert!(row_size < 150, "row_size {row_size} is suspiciously high");
    assert!(row_size > 20, "row_size {row_size} is suspiciously low");
}

/// Write the same batch to all 3 formats and verify row_size estimates
/// are in the same ballpark (no format produces wildly different results).
#[tokio::test(flavor = "multi_thread")]
async fn test_row_size_cross_format_consistency() {
    let batch = vortex_compatible_batch();
    let dir = tempfile::tempdir().unwrap();

    let arrow_path = dir.path().join("test.arrow");
    TestFile::write_arrow_batch(&arrow_path, &batch);
    let arrow_row_size = ArrowDataSource::new(arrow_path.to_str().unwrap().to_string())
        .row_size()
        .unwrap();

    let parquet_path = dir.path().join("test.parquet");
    TestFile::write_parquet_batch(&parquet_path, &batch);
    let parquet_row_size = ParquetDataSource::new(parquet_path.to_str().unwrap().to_string())
        .row_size()
        .unwrap();

    let vortex_path = dir.path().join("test.vtx");
    TestFile::write_vortex_batch(&vortex_path, &batch);
    let vortex_row_size = VortexDataSource::new(vortex_path.to_str().unwrap().to_string())
        .row_size()
        .unwrap();

    // all three should be in the same order of magnitude.
    let max = arrow_row_size.max(parquet_row_size).max(vortex_row_size);
    let min = arrow_row_size.min(parquet_row_size).min(vortex_row_size);
    #[allow(clippy::cast_precision_loss)]
    let ratio = max as f64 / min as f64;
    assert!(
        max <= min * 5,
        "cross-format row_size spread too wide: arrow={arrow_row_size}, \
         parquet={parquet_row_size}, vortex={vortex_row_size} — \
         max/min ratio = {ratio:.1}x (expected < 5x)",
    );
}

/// All-null string column should estimate near 0, not the 32-byte fallback.
#[allow(clippy::cast_possible_truncation)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_all_null_column() {
    let n = 5_000;
    let nulls: Vec<Option<&str>> = vec![None; n];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "all_null",
            DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(nulls)) as ArrayRef],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();

    // arrow
    let path = dir.path().join("null.arrow");
    TestFile::write_arrow_batch(&path, &batch);
    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["all_null".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("all_null").unwrap();
    assert!(
        est <= 5,
        "arrow all-null: estimate {est} should be near 0 (just offset + bitmap overhead)"
    );

    // parquet
    let path = dir.path().join("null.parquet");
    TestFile::write_parquet_batch(&path, &batch);
    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["all_null".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("all_null").unwrap();
    assert!(
        est <= 10,
        "parquet all-null: estimate {est} should be low (just def levels)"
    );
}

/// High null rate (90%) should still produce reasonable per-row estimates
/// by reading enough non-null values for a robust average.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_high_null_rate() {
    let n = 10_000;
    // 90% nulls, 10% non-null with 20-char strings
    let values: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 10 == 0 {
                Some(format!("{:0>20}", i))
            } else {
                None
            }
        })
        .collect();
    let refs: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "sparse_str",
            DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(refs)) as ArrayRef],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sparse.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["sparse_str".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("sparse_str").unwrap();
    // 10% non-null × ~24 bytes/value (20 data + 4 offset) ≈ 2-3 bytes/row
    assert_range("arrow/sparse_str_90pct_null", est, 0, 8);
}

/// Empty string column — strings exist but are all "".
/// Should estimate just the offset overhead, minimal data bytes.
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_empty_strings() {
    let n = 5_000;
    let empties: Vec<&str> = vec![""; n];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "empty_str",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(empties)) as ArrayRef],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty_str.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["empty_str".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("empty_str").unwrap();
    // just 4-byte offset per row, 0 data bytes → ~4 bytes/row
    assert_range("arrow/empty_strings", est, 2, 8);
}

/// Empty file (0 rows) should use 32-byte fallback for all formats.
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_empty_file() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "str_col",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();

    // arrow
    let path = dir.path().join("empty.arrow");
    TestFile::write_arrow_batch(&path, &batch);
    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["str_col".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("str_col").unwrap();
    assert_eq!(est, 32, "arrow empty file should use 32-byte fallback");

    // parquet
    let path = dir.path().join("empty.parquet");
    TestFile::write_parquet_batch(&path, &batch);
    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["str_col".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("str_col").unwrap();
    assert_eq!(est, 32, "parquet empty file should use 32-byte fallback");
}

/// Schema with only fixed-width columns — estimate_column_sizes should
/// return an empty map (no variable columns to estimate).
#[allow(clippy::cast_possible_wrap)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_all_fixed_schema() {
    let n = 1_000;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int64, false),
            Field::new("float_col", DataType::Float64, false),
            Field::new("bool_col", DataType::Boolean, false),
        ])),
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0; n])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true; n])) as ArrayRef,
        ],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixed.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    // no variable-width columns to estimate
    let estimates = source.estimate_column_sizes(&[], 100_000).unwrap();
    assert!(
        estimates.is_empty(),
        "all-fixed schema should have no variable column estimates"
    );

    // row_size should still work (all fixed)
    let row_size = source.row_size().unwrap();
    // int64(8) + float64(8) + bool(~0.125) = ~17 bytes/row
    assert_range("arrow/all_fixed_row_size", row_size, 10, 25);
}

/// All variable-width columns — no fixed columns to anchor on.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_all_variable_schema() {
    let n = 5_000;
    let strings: Vec<String> = (0..n).map(|i| format!("{:0>10}", i)).collect();
    let refs: Vec<&str> = strings.iter().map(String::as_str).collect();
    let binary_vecs: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut v = vec![0u8; 8];
            v[..8].copy_from_slice(&(i as u64).to_le_bytes());
            v
        })
        .collect();
    let binary_refs: Vec<&[u8]> = binary_vecs.iter().map(|v| v.as_slice()).collect();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("str_col", DataType::Utf8, false),
            Field::new("bin_col", DataType::Binary, false),
        ])),
        vec![
            Arc::new(StringArray::from(refs)) as ArrayRef,
            Arc::new(BinaryArray::from(binary_refs)) as ArrayRef,
        ],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all_var.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["str_col".to_string(), "bin_col".to_string()];
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    check_estimates(
        &estimates,
        "arrow/all_variable",
        &[("str_col", 10, 20), ("bin_col", 8, 16)],
    );

    let row_size = source.row_size().unwrap();
    assert_range("arrow/all_variable_row_size", row_size, 18, 40);
}

/// Parquet with multiple row groups — DataFusion streams across all groups.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_multi_row_group() {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let make_batch = |offset: usize, n: usize| {
        let strings: Vec<String> = (offset..offset + n)
            .map(|i| format!("{:0>10}", i))
            .collect();
        let refs: Vec<&str> = strings.iter().map(String::as_str).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "str_col",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(refs)) as ArrayRef],
        )
        .unwrap()
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi_rg.parquet");

    // write 3 row groups with different sizes
    let file = std::fs::File::create(&path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(2_000)
        .build();
    let batch1 = make_batch(0, 2_000);
    let mut writer = ArrowWriter::try_new(file, batch1.schema(), Some(props)).unwrap();
    writer.write(&batch1).unwrap();
    writer.flush().unwrap();
    writer.write(&make_batch(2_000, 2_000)).unwrap();
    writer.flush().unwrap();
    writer.write(&make_batch(4_000, 1_000)).unwrap();
    writer.close().unwrap();

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["str_col".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("str_col").unwrap();
    // same 10-char strings regardless of row group boundaries
    assert_range("parquet/multi_row_group", est, 8, 30);
}

/// Parquet column name prefix collision: "data" struct should not match
/// a column named "data_extra". Verifies exact path component matching.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_column_name_no_prefix_collision() {
    let n = 5_000;
    let strings: Vec<String> = (0..n).map(|i| format!("{:0>10}", i)).collect();
    let refs: Vec<&str> = strings.iter().map(String::as_str).collect();
    let extra_strings: Vec<String> = (0..n).map(|i| format!("{:0>30}", i)).collect();
    let extra_refs: Vec<&str> = extra_strings.iter().map(String::as_str).collect();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, false),
            Field::new("data_extra", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(refs)) as ArrayRef,
            Arc::new(StringArray::from(extra_refs)) as ArrayRef,
        ],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prefix.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());

    // estimate "data" alone — should NOT include "data_extra" sizes
    let estimates = source
        .estimate_column_sizes(&["data".to_string()], 100_000)
        .unwrap();
    let data_est = *estimates.get("data").unwrap();
    // 10-char strings read as Utf8View → ~16 view + 10 data ≈ 26/row
    assert_range("parquet/data_no_collision", data_est, 8, 40);

    // estimate "data_extra" alone — should be larger (30-char strings)
    let estimates = source
        .estimate_column_sizes(&["data_extra".to_string()], 100_000)
        .unwrap();
    let extra_est = *estimates.get("data_extra").unwrap();
    assert_range("parquet/data_extra_no_collision", extra_est, 25, 60);

    // "data" estimate should be meaningfully smaller than "data_extra"
    assert!(
        data_est < extra_est,
        "data ({data_est}) should be smaller than data_extra ({extra_est}) — \
         possible prefix collision in column name matching"
    );
}

/// Struct with identically-named child field: top-level "name" must not include
/// bytes from a struct child also named "name" (e.g. "data"."name").
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_leaf_name_collision() {
    let n = 5_000;

    // top-level "name": short 5-char strings
    let top_names: Vec<String> = (0..n).map(|i| format!("{:0>5}", i)).collect();
    let top_refs: Vec<&str> = top_names.iter().map(String::as_str).collect();

    // struct "data" with child "name": long 40-char strings
    let child_names: Vec<String> = (0..n).map(|i| format!("{:0>40}", i)).collect();
    let child_refs: Vec<&str> = child_names.iter().map(String::as_str).collect();
    let ids: Vec<i32> = (0..n).collect();

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(StringArray::from(child_refs)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Int32Array::from(ids)) as ArrayRef,
        ),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("id", DataType::Int32, false),
                ]
                .into(),
            ),
            false,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(top_refs)) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("leaf_collision.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());

    // estimate top-level "name" — should reflect 5-char strings as Utf8View,
    // NOT be inflated by the 40-char struct child "data"."name"
    let estimates = source
        .estimate_column_sizes(&["name".to_string()], 100_000)
        .unwrap();
    let name_est = *estimates.get("name").unwrap();
    // 5-char strings read as Utf8View → ~16 view + 5 data ≈ 21/row
    assert_range("parquet/leaf_name_no_collision", name_est, 3, 35);

    // estimate struct "data" — should include both "data"."name" and "data"."id"
    let estimates = source
        .estimate_column_sizes(&["data".to_string()], 100_000)
        .unwrap();
    let data_est = *estimates.get("data").unwrap();
    // 40-char name as Utf8View + 4-byte int + overhead ≈ 50-80/row
    assert_range("parquet/struct_data_with_name_child", data_est, 35, 90);

    // if column matching were broken, "name" would include the 40-char child strings
    assert!(
        name_est < 40,
        "top-level 'name' estimate ({name_est}) is too high — \
         likely including bytes from struct child 'data'.'name'"
    );
}

/// Tiny max_sample_rows (1 row) — should still produce reasonable estimates
/// without panicking or returning 0.
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_small_sample_size() {
    let batch = variable_width_batch();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let cols = vec!["utf8_col".to_string(), "binary_col".to_string()];

    // max_sample_rows=1 — should read at least one batch
    let estimates = source.estimate_column_sizes(&cols, 1).unwrap();
    let utf8_est = *estimates.get("utf8_col").unwrap();
    let bin_est = *estimates.get("binary_col").unwrap();
    assert!(
        utf8_est > 0,
        "utf8_col estimate should be non-zero even with 1-row sample"
    );
    assert!(
        bin_est > 0,
        "binary_col estimate should be non-zero even with 1-row sample"
    );
}

/// Multiple Arrow IPC batches with varying string lengths — the estimate
/// should reflect the weighted average across all sampled batches.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_arrow_varying_batch_sizes() {
    // batch 1: 1000 rows with 5-char strings
    let short: Vec<String> = (0..1_000).map(|i| format!("{:0>5}", i)).collect();
    let short_refs: Vec<&str> = short.iter().map(String::as_str).collect();
    let batch1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "str_col",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(short_refs)) as ArrayRef],
    )
    .unwrap();

    // batch 2: 3000 rows with 50-char strings
    let long: Vec<String> = (0..3_000).map(|i| format!("{:0>50}", i)).collect();
    let long_refs: Vec<&str> = long.iter().map(String::as_str).collect();
    let batch2 = RecordBatch::try_new(
        batch1.schema(),
        vec![Arc::new(StringArray::from(long_refs)) as ArrayRef],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("varying.arrow");
    TestFile::write_arrow(&path, &[batch1, batch2]);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let estimates = source
        .estimate_column_sizes(&["str_col".to_string()], 100_000)
        .unwrap();
    let est = *estimates.get("str_col").unwrap();
    // weighted average: (1000*~9 + 3000*~54) / 4000 ≈ ~43 bytes/row (with offsets)
    assert_range("arrow/varying_batches", est, 30, 60);
}

/// Batch with LargeList, Map, Utf8View, and BinaryView columns.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn extended_types_batch(n: usize) -> RecordBatch {
    // LargeList<Int32>: 3 elements per row
    let flat: Vec<i32> = (0..n * 3).map(|i| i as i32).collect();
    let values: ArrayRef = Arc::new(Int32Array::from(flat));
    let mut offsets = Vec::with_capacity(n + 1);
    for i in 0..=n {
        offsets.push((i * 3) as i64);
    }
    let large_list_col: ArrayRef = Arc::new(LargeListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, false)),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ));

    // Map<Utf8, Int32>: 2 key-value pairs per row
    let keys: Vec<String> = (0..n * 2).map(|i| format!("k{:0>4}", i)).collect();
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let map_keys = StringArray::from(key_refs);
    let map_values = Int32Array::from((0..n as i32 * 2).collect::<Vec<_>>());
    let map_entries = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(map_keys) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Int32, true)),
            Arc::new(map_values) as ArrayRef,
        ),
    ]);
    let mut map_offsets = Vec::with_capacity(n + 1);
    for i in 0..=n {
        map_offsets.push((i * 2) as i32);
    }
    let map_col: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )),
        OffsetBuffer::new(map_offsets.into()),
        map_entries,
        None,
        false,
    ));

    // Utf8View: mix of inline (<=12 bytes) and out-of-line strings
    let view_strings: Vec<String> = (0..n)
        .map(|i| {
            if i % 3 == 0 {
                format!("{:0>20}", i) // out-of-line (>12 bytes)
            } else {
                format!("{:0>8}", i) // inline (<=12 bytes)
            }
        })
        .collect();
    let view_refs: Vec<&str> = view_strings.iter().map(String::as_str).collect();
    let utf8_view_col: ArrayRef = Arc::new(StringViewArray::from(view_refs));

    // BinaryView: 16-byte blobs (all out-of-line)
    let bin_vecs: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut v = vec![0u8; 16];
            v[..8].copy_from_slice(&(i as u64).to_le_bytes());
            v
        })
        .collect();
    let bin_refs: Vec<&[u8]> = bin_vecs.iter().map(|v| v.as_slice()).collect();
    let binary_view_col: ArrayRef = Arc::new(BinaryViewArray::from(bin_refs));

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "large_list_col",
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, false))),
            false,
        ),
        Field::new(
            "map_col",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
        Field::new("utf8_view_col", DataType::Utf8View, false),
        Field::new("binary_view_col", DataType::BinaryView, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![large_list_col, map_col, utf8_view_col, binary_view_col],
    )
    .unwrap()
}

fn extended_col_names() -> Vec<String> {
    vec![
        "large_list_col".into(),
        "map_col".into(),
        "utf8_view_col".into(),
        "binary_view_col".into(),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_arrow_ipc_extended_types() {
    let batch = extended_types_batch(N);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("extended.arrow");
    TestFile::write_arrow_batch(&path, &batch);

    let source = ArrowDataSource::new(path.to_str().unwrap().to_string());
    let cols = extended_col_names();
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    check_estimates(
        &estimates,
        "arrow_ipc/extended",
        &[
            // LargeList<i32>: 8-byte offset + 3*4=12 child data = ~20/row
            ("large_list_col", 14, 28),
            // Map<Utf8,i32>: 4 offset + 2*(~5 key + 4 val + offsets) ≈ 25-35/row
            ("map_col", 15, 45),
            // Utf8View: 16-byte view + variable data (~12/row avg)
            ("utf8_view_col", 16, 35),
            // BinaryView: 16-byte view + 16 data = ~32/row
            ("binary_view_col", 25, 40),
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_estimation_parquet_extended_types() {
    let batch = extended_types_batch(N);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("extended.parquet");
    TestFile::write_parquet_batch(&path, &batch);

    let source = ParquetDataSource::new(path.to_str().unwrap().to_string());
    let cols = extended_col_names();
    let estimates = source.estimate_column_sizes(&cols, 100_000).unwrap();

    // DataFusion reads through as_stream, decoding to Arrow types.
    // LargeList may be decoded as List (i32 offsets).
    // strings decoded as Utf8View (schema_force_view_types=true).
    check_estimates(
        &estimates,
        "parquet/extended",
        &[
            ("large_list_col", 8, 40),
            ("map_col", 10, 60),
            ("utf8_view_col", 10, 55),
            ("binary_view_col", 16, 60),
        ],
    );
}
