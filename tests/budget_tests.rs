//! Integration tests for memory budget planning and budget-aware sink queue sizing.

use std::path::PathBuf;

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use camino::Utf8PathBuf;
use silk_chiffon::{
    DataFormat, MemoryBudgetSpec, ParquetCompression, SortColumn, SortDirection, SortSpec,
    utils::memory::WorkloadKind,
    utils::test_data::{TestBatch, TestBatchBuilder, TestFile, count_rows, sort_asc, sort_desc},
};
use tempfile::TempDir;

async fn run_transform(args: silk_chiffon::TransformCommand) {
    silk_chiffon::commands::transform::run(args).await.unwrap();
}

fn default_args(input: &str, output: &str) -> silk_chiffon::TransformCommand {
    silk_chiffon::TransformCommand {
        from: Some(input.to_string()),
        to: Some(output.to_string()),
        ..Default::default()
    }
}

fn assert_sorted_asc_i32(batches: &[RecordBatch], col: &str) {
    let mut prev = i32::MIN;
    for batch in batches {
        let arr = batch
            .column_by_name(col)
            .unwrap()
            .as_primitive::<arrow::datatypes::Int32Type>();
        for i in 0..arr.len() {
            let val = arr.value(i);
            assert!(
                val >= prev,
                "expected ascending order on '{col}': {val} < {prev}"
            );
            prev = val;
        }
    }
}

fn assert_sorted_desc_i32(batches: &[RecordBatch], col: &str) {
    let mut prev = i32::MAX;
    for batch in batches {
        let arr = batch
            .column_by_name(col)
            .unwrap()
            .as_primitive::<arrow::datatypes::Int32Type>();
        for i in 0..arr.len() {
            let val = arr.value(i);
            assert!(
                val <= prev,
                "expected descending order on '{col}': {val} > {prev}"
            );
            prev = val;
        }
    }
}

struct BudgetTest {
    dir: TempDir,
}

impl BudgetTest {
    fn new() -> Self {
        Self {
            dir: TempDir::new().unwrap(),
        }
    }

    fn write_narrow(&self, rows: usize) -> String {
        let input = self.dir.path().join("input.arrow");
        TestFile::write_arrow_batch(&input, &TestBatch::narrow(rows));
        input.to_str().unwrap().to_string()
    }

    fn write_wide(&self, rows: usize) -> String {
        let input = self.dir.path().join("input.arrow");
        TestFile::write_arrow_batch(&input, &TestBatch::wide(rows));
        input.to_str().unwrap().to_string()
    }

    fn parquet_output(&self) -> (String, PathBuf) {
        let p = self.dir.path().join("output.parquet");
        (p.to_str().unwrap().to_string(), p)
    }

    fn arrow_output(&self) -> (String, PathBuf) {
        let p = self.dir.path().join("output.arrow");
        (p.to_str().unwrap().to_string(), p)
    }

    fn write_parquet_narrow(&self, rows: usize) -> String {
        let input = self.dir.path().join("input.parquet");
        TestFile::write_parquet_batch(&input, &TestBatch::narrow(rows));
        input.to_str().unwrap().to_string()
    }

    /// mixed schema with many variable-width columns, resembling real analytics data
    fn write_string_heavy(&self, rows: usize) -> String {
        let input = self.dir.path().join("input.arrow");
        TestFile::write_arrow_batch(&input, &string_heavy_batch(rows));
        input.to_str().unwrap().to_string()
    }

    fn write_parquet_string_heavy(&self, rows: usize) -> String {
        let input = self.dir.path().join("input.parquet");
        TestFile::write_parquet_batch(&input, &string_heavy_batch(rows));
        input.to_str().unwrap().to_string()
    }

    fn spill_dir(&self) -> Utf8PathBuf {
        let p = self.dir.path().join("spill");
        std::fs::create_dir_all(&p).unwrap();
        Utf8PathBuf::from_path_buf(p).unwrap()
    }
}

const MB: usize = 1024 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_passthrough_narrow_to_parquet() {
    let t = BudgetTest::new();
    let input = t.write_narrow(10_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(50 * MB),
        output_format: Some(DataFormat::Parquet),
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_parquet(&output_path)), 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_passthrough_wide_to_parquet() {
    let t = BudgetTest::new();
    let input = t.write_wide(5_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(50 * MB),
        output_format: Some(DataFormat::Parquet),
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_parquet(&output_path)), 5_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_passthrough_to_arrow() {
    let t = BudgetTest::new();
    let input = t.write_narrow(10_000);
    let (output, output_path) = t.arrow_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(50 * MB),
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_arrow(&output_path)), 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_narrow_to_parquet() {
    let t = BudgetTest::new();
    let input = t.write_narrow(10_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(100 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_asc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 10_000);
    assert_sorted_asc_i32(&batches, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_wide_to_parquet() {
    let t = BudgetTest::new();
    let input = t.write_wide(5_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(100 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_desc("i32_0")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 5_000);
    assert_sorted_desc_i32(&batches, "i32_0");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_query_and_sort() {
    let t = BudgetTest::new();
    let input = t.write_narrow(5_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(50 * MB),
        output_format: Some(DataFormat::Parquet),
        query: Some("SELECT * FROM data WHERE id < 2500".to_string()),
        sort_by: Some(sort_desc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 2_500);
    assert_sorted_desc_i32(&batches, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_floor_sort_with_tiny_budget() {
    // 5MB total budget with sort: usable after 15% overhead is ~4.25MB, well below
    // the ~21MB df_floor, so overcommit kicks in to give DF enough room to sort.
    let t = BudgetTest::new();
    let input = t.write_narrow(500);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(5 * MB),
        output_format: Some(DataFormat::Parquet),
        parquet_compression: ParquetCompression::Snappy,
        sort_by: Some(sort_asc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 500);
    assert_sorted_asc_i32(&batches, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_floor_no_sort_with_tiny_budget() {
    // 1MB total budget without sort: DF floor is 2MB, sink floor is 1MB.
    let t = BudgetTest::new();
    let input = t.write_narrow(100);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(MB),
        output_format: Some(DataFormat::Parquet),
        parquet_compression: ParquetCompression::Snappy,
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_parquet(&output_path)), 100);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_stress_tiny_budget_passthrough() {
    // 2MB budget with narrow data → exercises buffer scaling and row group clamping
    let t = BudgetTest::new();
    let input = t.write_narrow(500);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(2 * MB),
        output_format: Some(DataFormat::Parquet),
        parquet_compression: ParquetCompression::Snappy,
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_parquet(&output_path)), 500);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_stress_wide_schema_tight() {
    // wide schema (20 cols, ~280 bytes/row actual) with tight budget
    let t = BudgetTest::new();
    let input = t.write_wide(10_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(20 * MB),
        output_format: Some(DataFormat::Parquet),
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_parquet(&output_path)), 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_stress_many_rows_narrow() {
    // 100K rows × ~36 bytes ≈ 3.6MB of data, tight 10MB budget
    let t = BudgetTest::new();
    let input = t.write_narrow(100_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(10 * MB),
        output_format: Some(DataFormat::Parquet),
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_parquet(&output_path)), 100_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_stress_arrow_output_tight() {
    // arrow output with tight budget — exercises arrow queue sizing
    let t = BudgetTest::new();
    let input = t.write_wide(5_000);
    let (output, output_path) = t.arrow_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(10 * MB),
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_arrow(&output_path)), 5_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_tight_memory_narrow() {
    // 50MB budget with 100K narrow rows (~3.6MB data). should scale partitions down
    // and complete without OOM.
    let t = BudgetTest::new();
    let input = t.write_narrow(100_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(50 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_asc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 100_000);
    assert_sorted_asc_i32(&batches, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_tight_memory_wide() {
    // 50MB budget with 50K wide rows (~14MB data). wide schema exercises the
    // partition limiter more aggressively since merge buffers are proportional to row width.
    let t = BudgetTest::new();
    let input = t.write_wide(50_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(50 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_desc("i32_0")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 50_000);
    assert_sorted_desc_i32(&batches, "i32_0");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_very_tight_memory() {
    // 20MB budget with sort — exercises the floor/overcommit path
    let t = BudgetTest::new();
    let input = t.write_narrow(10_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(20 * MB),
        output_format: Some(DataFormat::Parquet),
        parquet_compression: ParquetCompression::Snappy,
        sort_by: Some(sort_asc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 10_000);
    assert_sorted_asc_i32(&batches, "id");
}

/// Schema heavy on strings: 3 i32 + 7 utf8 columns with variable-length data.
/// Resembles analytics tables with many categorical/text columns.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn string_heavy_batch(row_count: usize) -> RecordBatch {
    let ids: Vec<i32> = (0..row_count).map(|i| i as i32).collect();
    let categories: Vec<i32> = (0..row_count).map(|i| (i % 100) as i32).collect();
    let scores: Vec<i32> = (0..row_count).map(|i| (i * 7 % 10_000) as i32).collect();

    // variable-length strings: mix of short and long values
    let short_strings: Vec<String> = (0..row_count).map(|i| format!("cat_{}", i % 50)).collect();
    let medium_strings: Vec<String> = (0..row_count)
        .map(|i| format!("region_{:0>16}", i % 200))
        .collect();
    let long_strings: Vec<String> = (0..row_count)
        .map(|i| format!("description_{:0>64}", i))
        .collect();

    let short_refs: Vec<&str> = short_strings.iter().map(String::as_str).collect();
    let medium_refs: Vec<&str> = medium_strings.iter().map(String::as_str).collect();
    let long_refs: Vec<&str> = long_strings.iter().map(String::as_str).collect();

    TestBatchBuilder::new()
        .column_i32("id", &ids)
        .column_i32("category", &categories)
        .column_i32("score", &scores)
        .column_string("label", &short_refs)
        .column_string("region", &medium_refs)
        .column_string("description", &long_refs)
        .column_string("tag_a", &short_refs)
        .column_string("tag_b", &medium_refs)
        .column_string("notes", &long_refs)
        .column_string("source", &short_refs)
        .build()
}

fn multi_sort(cols: &[(&str, SortDirection)]) -> SortSpec {
    SortSpec {
        columns: cols
            .iter()
            .map(|(name, dir)| SortColumn {
                name: name.to_string(),
                direction: dir.clone(),
            })
            .collect(),
    }
}

fn assert_sorted_asc_string(batches: &[RecordBatch], col: &str) {
    let mut prev = String::new();
    for batch in batches {
        let arr = batch
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            let val = arr.value(i);
            assert!(
                val >= prev.as_str(),
                "expected ascending order on '{col}': {val:?} < {prev:?}"
            );
            prev = val.to_string();
        }
    }
}

// multi-column sort on narrow data: sort by (id asc, name desc). exercises
// partition scaling + spill reservation with a string sort key.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_multi_col_sort_narrow() {
    let t = BudgetTest::new();
    let input = t.write_narrow(50_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(200 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("id", SortDirection::Ascending),
            ("name", SortDirection::Descending),
        ])),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 50_000);
    assert_sorted_asc_i32(&batches, "id");
}

// sort string-heavy data on multiple variable-width columns.
// this is the scenario closest to the original OOM: many sort keys,
// variable-width columns inflate RowConverter encoding overhead.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_string_heavy_multi_col() {
    let t = BudgetTest::new();
    let input = t.write_string_heavy(100_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(2048 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("region", SortDirection::Ascending),
            ("category", SortDirection::Ascending),
            ("score", SortDirection::Descending),
        ])),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 100_000);
    assert_sorted_asc_string(&batches, "region");
}

// sort with spilling: 200K narrow rows with a budget that forces spill.
// fixed-width sort key (id) keeps RowConverter overhead predictable.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_with_spill() {
    let t = BudgetTest::new();
    let input = t.write_narrow(200_000);
    let (output, output_path) = t.parquet_output();
    let spill = t.spill_dir();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(200 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_asc("id")),
        spill_path: Some(spill),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 200_000);
    assert_sorted_asc_i32(&batches, "id");
}

// sort string-heavy data with spilling. variable-width RowConverter encoding
// makes merge memory usage much higher per row, so the spill reservation
// must account for it correctly.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_sort_string_heavy_with_spill() {
    let t = BudgetTest::new();
    let input = t.write_string_heavy(50_000);
    let (output, output_path) = t.parquet_output();
    let spill = t.spill_dir();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(300 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("label", SortDirection::Ascending),
            ("id", SortDirection::Ascending),
        ])),
        spill_path: Some(spill),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 50_000);
    assert_sorted_asc_string(&batches, "label");
}

// parquet input: column size estimation uses stream-based sampling like other formats.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_parquet_input_sort() {
    let t = BudgetTest::new();
    let input = t.write_parquet_narrow(100_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(50 * MB),
        output_format: Some(DataFormat::Parquet),
        input_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_asc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 100_000);
    assert_sorted_asc_i32(&batches, "id");
}

// parquet input with string-heavy schema. exercises the parquet metadata
// path for variable-width columns (uses uncompressed_size / num_values).
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_parquet_input_string_heavy_sort() {
    let t = BudgetTest::new();
    let input = t.write_parquet_string_heavy(50_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(80 * MB),
        output_format: Some(DataFormat::Parquet),
        input_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("region", SortDirection::Ascending),
            ("score", SortDirection::Descending),
        ])),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 50_000);
    assert_sorted_asc_string(&batches, "region");
}

// wide schema sort with many sort columns (5 columns). exercises the
// encoding overhead calculation with many fixed-width sort keys.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_wide_many_sort_cols() {
    let t = BudgetTest::new();
    let input = t.write_wide(100_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(2048 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("i32_0", SortDirection::Ascending),
            ("i64_0", SortDirection::Descending),
            ("f64_0", SortDirection::Ascending),
            ("i32_1", SortDirection::Descending),
            ("i32_2", SortDirection::Ascending),
        ])),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 100_000);
    assert_sorted_asc_i32(&batches, "i32_0");
}

// query + sort + tight budget: filter reduces row count, sort must
// still complete within budget.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_query_sort_string_heavy() {
    let t = BudgetTest::new();
    let input = t.write_string_heavy(100_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(80 * MB),
        output_format: Some(DataFormat::Parquet),
        query: Some("SELECT * FROM data WHERE category < 10".to_string()),
        sort_by: Some(multi_sort(&[
            ("region", SortDirection::Ascending),
            ("id", SortDirection::Ascending),
        ])),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    // category goes 0..99, so ~10% of rows pass the filter
    let rows = count_rows(&batches);
    assert!(rows > 0 && rows <= 100_000);
    assert_sorted_asc_string(&batches, "region");
}

// large row count passthrough under tight budget (no sort).
// exercises sink buffer scaling and row group clamping.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_large_passthrough() {
    let t = BudgetTest::new();
    let input = t.write_narrow(1_000_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(30 * MB),
        output_format: Some(DataFormat::Parquet),
        ..default_args(&input, &output)
    })
    .await;

    assert_eq!(count_rows(&TestFile::read_parquet(&output_path)), 1_000_000);
}

// verify that row_size estimation for string-heavy data returns
// reasonable values (not inflated by Arrow IPC shared buffer overhead).
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_string_heavy_row_size_reasonable() {
    let t = BudgetTest::new();
    let input_path = t.write_string_heavy(100_000);

    use silk_chiffon::sources::data_source::DataSource;
    let source = silk_chiffon::sources::arrow::ArrowDataSource::new(input_path);
    let row_size = source.row_size().unwrap();

    // 3×i32 (12) + 7×variable (short ~9, medium ~27, long ~80) ≈ 150-400
    assert!(
        row_size > 100 && row_size < 500,
        "row_size should be ~250 bytes, got {row_size}"
    );
}

// 1M rows, string-heavy schema, sort on 6 columns (3 variable-width + 3 fixed).
// this is the closest test to the original OOM scenario: large row count,
// many sort keys including variable-width columns that blow up RowConverter
// encoding, all under a memory budget with spill enabled.
#[tokio::test(flavor = "multi_thread")]
async fn test_budget_million_row_multi_col_sort_with_strings() {
    let t = BudgetTest::new();
    let input = t.write_string_heavy(1_000_000);
    let (output, output_path) = t.parquet_output();
    let spill = t.spill_dir();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(2048 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("region", SortDirection::Ascending),
            ("label", SortDirection::Ascending),
            ("source", SortDirection::Descending),
            ("category", SortDirection::Ascending),
            ("score", SortDirection::Descending),
            ("id", SortDirection::Ascending),
        ])),
        spill_path: Some(spill),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 1_000_000);
    assert_sorted_asc_string(&batches, "region");
}

// --- stress tests: sort partition count capping ---
//
// These push large data through constrained budgets where partition capping
// (pool / 256MB) is the difference between success and OOM. Each test asserts
// correct row count AND sort order — if capping fails, the sort will either
// OOM or produce incorrect results.

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_sort_partition_cap_narrow_500k() {
    // 500K narrow rows, 300MB budget. DF pool ~220-250MB → 1 partition.
    // forces spills within a single partition.
    let t = BudgetTest::new();
    let input = t.write_narrow(500_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(300 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_asc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 500_000);
    assert_sorted_asc_i32(&batches, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_sort_partition_cap_wide_200k() {
    // 200K wide rows (~280 bytes/row, ~56MB data), 800MB budget.
    // sink estimate ~336MB (1 RG x 280 bytes x 1.2), DF pool ~340MB -> 1 partition.
    // wide schema has large RowConverter encoding overhead.
    let t = BudgetTest::new();
    let input = t.write_wide(200_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(800 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_desc("i32_0")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 200_000);
    assert_sorted_desc_i32(&batches, "i32_0");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_sort_partition_cap_string_heavy_300k() {
    // 300K string-heavy rows (~280 bytes/row), 800MB budget.
    // sink estimate ~336MB, DF pool ~340MB -> 1 partition.
    // variable-width sort keys exercise both partition capping AND
    // spill reservation (RowConverter overhead).
    let t = BudgetTest::new();
    let input = t.write_string_heavy(300_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(800 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("region", SortDirection::Ascending),
            ("category", SortDirection::Ascending),
            ("score", SortDirection::Descending),
        ])),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 300_000);
    assert_sorted_asc_string(&batches, "region");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_sort_partition_cap_at_boundary() {
    // 200K narrow rows, 600MB budget. DF pool ~480-510MB → ~2 partitions.
    // tests the 1→2 partition transition boundary.
    let t = BudgetTest::new();
    let input = t.write_narrow(200_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(600 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_asc("id")),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 200_000);
    assert_sorted_asc_i32(&batches, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_sort_partition_cap_multi_source() {
    // 10 small Arrow files (10K rows each = 100K total), 300MB budget, sort.
    // exercises multi-source sampling AND partition capping with glob input.
    let t = BudgetTest::new();
    let dir = t.dir.path();
    for i in 0..10 {
        let path = dir.join(format!("input_{i:02}.arrow"));
        TestFile::write_arrow_batch(&path, &TestBatch::narrow(10_000));
    }
    let glob_pattern = dir.join("input_*.arrow").to_str().unwrap().to_string();
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        from: None,
        from_many: vec![glob_pattern],
        memory_budget: MemoryBudgetSpec::Fixed(300 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(sort_asc("id")),
        ..default_args("unused", &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 100_000);
    assert_sorted_asc_i32(&batches, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_sort_partition_cap_multi_col_variable() {
    // 100K string-heavy rows (~280 bytes/row), 800MB budget. sort on 4
    // columns including multiple variable-width keys -- amplifies
    // RowConverter encoding overhead.
    let t = BudgetTest::new();
    let input = t.write_string_heavy(100_000);
    let (output, output_path) = t.parquet_output();

    run_transform(silk_chiffon::TransformCommand {
        memory_budget: MemoryBudgetSpec::Fixed(800 * MB),
        output_format: Some(DataFormat::Parquet),
        sort_by: Some(multi_sort(&[
            ("region", SortDirection::Ascending),
            ("label", SortDirection::Ascending),
            ("category", SortDirection::Ascending),
            ("id", SortDirection::Ascending),
        ])),
        ..default_args(&input, &output)
    })
    .await;

    let batches = TestFile::read_parquet(&output_path);
    assert_eq!(count_rows(&batches), 100_000);
    assert_sorted_asc_string(&batches, "region");
}

#[test]
fn test_budget_plan_invariant_across_sizes() {
    use silk_chiffon::sinks::parquet::ParquetSinkOptions;
    use silk_chiffon::utils::memory::BudgetPlan;

    let row_bytes = 36;
    let sink_needs = ParquetSinkOptions::new().estimate_sink_needs(row_bytes);

    let workloads = [WorkloadKind::Passthrough, WorkloadKind::Active];

    for total in [0, 1, 100, 999, 1_000_000, 16 * 1024 * MB] {
        for &workload in &workloads {
            let plan = BudgetPlan::new(total, workload, row_bytes, sink_needs);
            let sum = plan.overhead_reserve + plan.datafusion_pool + plan.sink_budget;
            assert!(
                sum >= plan.total,
                "budget parts should sum to at least total: sum={sum}, total={total}, workload={workload:?}"
            );
            // large budgets where no floor triggers should sum exactly
            if total >= 1024 * MB {
                assert_eq!(
                    sum, plan.total,
                    "large budget should not overcommit: sum={sum}, total={total}, workload={workload:?}"
                );
            }
        }
    }
}
