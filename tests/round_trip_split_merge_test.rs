//! Round-trip integration test: generate 10M rows → split by partition key → merge back → verify identity.

use std::sync::Arc;

use arrow::array::{Date32Array, Int16Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::SessionContext;
use silk_chiffon::utils::test_data::TestFile;
use silk_chiffon::{SortColumn, SortDirection, SortSpec};
use tempfile::TempDir;

const NUM_ROWS: usize = 10_000_000;
const BATCH_SIZE: usize = 500_000;

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("static_val", DataType::Int16, true),
        Field::new("partition_key", DataType::Int16, true),
        Field::new("row_id", DataType::Int32, true),
        Field::new("metric_a", DataType::Int32, true),
        Field::new("metric_b", DataType::Int32, true),
        Field::new("metric_c", DataType::Int32, true),
        Field::new("metric_d", DataType::Int32, true),
        Field::new("metric_e", DataType::Int32, true),
        Field::new("metric_f", DataType::Int32, true),
        Field::new("event_date", DataType::Date32, true),
    ]))
}

/// Generates a batch of `count` rows starting at `start_row`.
///
/// Data properties:
/// - `static_val`: always 42 (nullable, ~2% null)
/// - `partition_key`: 5 unique values 0..4, never null
/// - `row_id`: unique sequential integer per row, never null (used as sort key)
/// - `metric_a` through `metric_f`: varying cardinality, ~3-7% null each
/// - `event_date`: days since epoch, ~2% null
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::similar_names
)]
fn generate_batch(start_row: usize, count: usize, schema: &SchemaRef) -> RecordBatch {
    let end = start_row + count;

    let static_vals: Vec<Option<i16>> = (start_row..end)
        .map(|r| if r % 53 == 0 { None } else { Some(42) })
        .collect();

    let partition_keys: Vec<Option<i16>> = (start_row..end).map(|r| Some((r % 5) as i16)).collect();

    let row_ids: Vec<Option<i32>> = (start_row..end).map(|r| Some(r as i32)).collect();

    let metric_as: Vec<Option<i32>> = (start_row..end)
        .map(|r| {
            if r % 37 == 0 {
                None
            } else {
                Some((r * 7 % 100_000) as i32)
            }
        })
        .collect();
    let metric_bs: Vec<Option<i32>> = (start_row..end)
        .map(|r| {
            if r % 23 == 0 {
                None
            } else {
                Some((r * 13 % 50_000) as i32)
            }
        })
        .collect();
    let metric_cs: Vec<Option<i32>> = (start_row..end)
        .map(|r| {
            if r % 17 == 0 {
                None
            } else {
                Some((r * 31 % 200_000) as i32)
            }
        })
        .collect();
    let metric_ds: Vec<Option<i32>> = (start_row..end)
        .map(|r| {
            if r % 29 == 0 {
                None
            } else {
                Some((r * 43 % 80_000) as i32)
            }
        })
        .collect();
    let metric_es: Vec<Option<i32>> = (start_row..end)
        .map(|r| {
            if r % 41 == 0 {
                None
            } else {
                Some((r * 59 % 150_000) as i32)
            }
        })
        .collect();
    let metric_fs: Vec<Option<i32>> = (start_row..end)
        .map(|r| {
            if r % 47 == 0 {
                None
            } else {
                Some((r * 67 % 120_000) as i32)
            }
        })
        .collect();

    let dates: Vec<Option<i32>> = (start_row..end)
        .map(|r| {
            if r % 61 == 0 {
                None
            } else {
                Some(19000 + (r % 1000) as i32)
            }
        })
        .collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int16Array::from(static_vals)) as _,
            Arc::new(Int16Array::from(partition_keys)) as _,
            Arc::new(Int32Array::from(row_ids)) as _,
            Arc::new(Int32Array::from(metric_as)) as _,
            Arc::new(Int32Array::from(metric_bs)) as _,
            Arc::new(Int32Array::from(metric_cs)) as _,
            Arc::new(Int32Array::from(metric_ds)) as _,
            Arc::new(Int32Array::from(metric_es)) as _,
            Arc::new(Int32Array::from(metric_fs)) as _,
            Arc::new(Date32Array::from(dates)) as _,
        ],
    )
    .unwrap()
}

/// Runs a symmetric difference query between two registered arrow tables.
/// Returns the number of rows that differ — 0 means identical datasets.
async fn symmetric_diff_row_count(ctx: &SessionContext, table_a: &str, table_b: &str) -> usize {
    let query = format!(
        "(SELECT * FROM {table_a} EXCEPT ALL SELECT * FROM {table_b}) \
         UNION ALL \
         (SELECT * FROM {table_b} EXCEPT ALL SELECT * FROM {table_a})"
    );
    let diff = ctx.sql(&query).await.unwrap().collect().await.unwrap();
    diff.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn test_round_trip_split_and_merge_10m() {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join("input.arrow");
    let partition_dir = temp_dir.path().join("partitions");
    let output = temp_dir.path().join("merged.arrow");

    let schema = make_schema();
    let num_batches = NUM_ROWS / BATCH_SIZE;
    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|i| generate_batch(i * BATCH_SIZE, BATCH_SIZE, &schema))
        .collect();
    TestFile::write_arrow(&input, &batches);
    drop(batches);

    // split by partition_key into 5 files
    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(
            partition_dir
                .join("part_{{partition_key}}.arrow")
                .to_string_lossy()
                .to_string(),
        ),
        by: Some("partition_key".to_string()),
        create_dirs: true,
        ..Default::default()
    })
    .await
    .unwrap();

    // merge partitions back into a single sorted file (must run before validation
    // queries — DataFusion's EXCEPT ALL holds large hash tables in process memory,
    // which starves the merge sort's auto-detected memory budget)
    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from_many: vec![partition_dir.join("*.arrow").to_string_lossy().to_string()],
        to: Some(output.to_string_lossy().to_string()),
        sort_by: Some(SortSpec {
            columns: vec![SortColumn {
                name: "row_id".to_string(),
                direction: SortDirection::Ascending,
            }],
        }),
        ..Default::default()
    })
    .await
    .unwrap();

    // register all files as DataFusion tables for SQL-based validation
    let ctx = SessionContext::new();
    let arrow_opts = ArrowReadOptions::default();
    ctx.register_arrow("original", input.to_str().unwrap(), arrow_opts.clone())
        .await
        .unwrap();
    ctx.register_arrow("merged", output.to_str().unwrap(), arrow_opts.clone())
        .await
        .unwrap();

    // each partition file must contain exactly the rows where partition_key = key
    let mut total_partition_rows = 0usize;
    for key in 0..5i16 {
        let part_path = partition_dir.join(format!("part_{key}.arrow"));
        assert!(part_path.exists(), "partition file for key {key} missing");

        let table_name = format!("part_{key}");
        ctx.register_arrow(&table_name, part_path.to_str().unwrap(), arrow_opts.clone())
            .await
            .unwrap();

        let count_result = ctx
            .sql(&format!("SELECT COUNT(*) AS cnt FROM {table_name}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row_count = usize::try_from(
            count_result[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
        )
        .unwrap();
        total_partition_rows += row_count;

        let query = format!(
            "(SELECT * FROM {table_name} EXCEPT ALL SELECT * FROM original WHERE partition_key = {key}) \
             UNION ALL \
             (SELECT * FROM original WHERE partition_key = {key} EXCEPT ALL SELECT * FROM {table_name})"
        );
        let diff = ctx.sql(&query).await.unwrap().collect().await.unwrap();
        let diff_rows: usize = diff.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            diff_rows, 0,
            "partition {key} differs from expected: {diff_rows} rows in symmetric difference"
        );
    }

    assert_eq!(
        total_partition_rows, NUM_ROWS,
        "total rows across all partitions doesn't match input"
    );

    let diff_rows = symmetric_diff_row_count(&ctx, "original", "merged").await;
    assert_eq!(
        diff_rows, 0,
        "merged output differs from original: {diff_rows} rows in symmetric difference"
    );
}
