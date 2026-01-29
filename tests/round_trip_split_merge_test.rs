//! Round-trip integration test: generate 10M rows → split by partition key → merge back → verify identity.
//!
//! Tests arrow, parquet, and vortex formats to ensure data survives a split+merge round trip.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Date32Array, Int16Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use silk_chiffon::sinks::arrow::{ArrowSink, ArrowSinkOptions};
use silk_chiffon::sinks::data_sink::DataSink;
use silk_chiffon::sinks::parquet::ParquetSink;
use silk_chiffon::sinks::parquet::ParquetSinkOptions;
use silk_chiffon::sinks::parquet::pools::ParquetRuntimes;
use silk_chiffon::sinks::vortex::{VortexSink, VortexSinkOptions};
use silk_chiffon::sources::arrow::ArrowDataSource;
use silk_chiffon::sources::data_source::DataSource;
use silk_chiffon::sources::parquet::ParquetDataSource;
use silk_chiffon::sources::vortex::VortexDataSource;
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

/// Runs a symmetric difference query between two registered tables.
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

/// Returns the row count of a registered table.
async fn row_count(ctx: &SessionContext, table: &str) -> usize {
    let result = ctx
        .sql(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    usize::try_from(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}

/// Registers a file as a named DataFusion table using the format-appropriate DataSource.
async fn register_table(ctx: &mut SessionContext, name: &str, path: &Path, ext: &str) {
    let path_str = path.to_string_lossy().to_string();
    let source: Box<dyn DataSource> = match ext {
        "arrow" => Box::new(ArrowDataSource::new(path_str)),
        "parquet" => Box::new(ParquetDataSource::new(path_str)),
        "vortex" => Box::new(VortexDataSource::new(path_str)),
        _ => panic!("unsupported format: {ext}"),
    };
    let provider = source.as_table_provider(ctx).await.unwrap();
    ctx.register_table(name, provider).unwrap();
}

/// Streams 10M rows to a file one batch at a time via the appropriate `DataSink`.
async fn write_test_data(path: &Path, schema: &SchemaRef, ext: &str) {
    let mut sink: Box<dyn DataSink> = match ext {
        "arrow" => Box::new(
            ArrowSink::create(path.to_path_buf(), schema, ArrowSinkOptions::default()).unwrap(),
        ),
        "parquet" => {
            let runtimes = Arc::new(ParquetRuntimes::try_default().unwrap());
            Box::new(
                ParquetSink::create(
                    path.to_path_buf(),
                    schema,
                    &ParquetSinkOptions::default(),
                    runtimes,
                )
                .unwrap(),
            )
        }
        "vortex" => Box::new(
            VortexSink::create(path.to_path_buf(), schema, VortexSinkOptions::default()).unwrap(),
        ),
        _ => panic!("unsupported format: {ext}"),
    };
    let num_batches = NUM_ROWS / BATCH_SIZE;
    for i in 0..num_batches {
        let batch = generate_batch(i * BATCH_SIZE, BATCH_SIZE, schema);
        sink.write_batch(batch).await.unwrap();
    }
    sink.finish().await.unwrap();
}

/// Generates 10M rows in the target format, splits by partition_key, merges back, and verifies
/// the merged output is identical to the original using SQL symmetric difference.
///
/// All files use the target format. Each format's DataSource provides a TableProvider for
/// DataFusion validation.
async fn round_trip_split_merge(ext: &str) {
    let temp_dir = TempDir::new().unwrap();
    let input = temp_dir.path().join(format!("input.{ext}"));
    let partition_dir = temp_dir.path().join("partitions");
    let output = temp_dir.path().join(format!("merged.{ext}"));

    let schema = make_schema();
    write_test_data(&input, &schema, ext).await;

    // split by partition_key into target format
    let partition_template = format!("part_{{{{partition_key}}}}.{ext}");
    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from: Some(input.to_string_lossy().to_string()),
        to_many: Some(
            partition_dir
                .join(&partition_template)
                .to_string_lossy()
                .to_string(),
        ),
        by: Some("partition_key".to_string()),
        create_dirs: true,
        ..Default::default()
    })
    .await
    .unwrap();

    // merge partitions back (no sort needed — EXCEPT ALL is order-independent)
    let glob_pattern = format!("*.{ext}");
    silk_chiffon::commands::transform::run(silk_chiffon::TransformCommand {
        from_many: vec![
            partition_dir
                .join(&glob_pattern)
                .to_string_lossy()
                .to_string(),
        ],
        to: Some(output.to_string_lossy().to_string()),
        ..Default::default()
    })
    .await
    .unwrap();

    // register original and merged files as DataFusion tables
    let mut ctx = SessionContext::new();
    register_table(&mut ctx, "original", &input, ext).await;
    register_table(&mut ctx, "merged", &output, ext).await;

    assert_eq!(
        row_count(&ctx, "original").await,
        NUM_ROWS,
        "original file row count doesn't match expected"
    );
    assert_eq!(
        row_count(&ctx, "merged").await,
        NUM_ROWS,
        "merged file row count doesn't match expected"
    );

    // each partition file must contain exactly the rows where partition_key = key
    let mut total_partition_rows = 0usize;
    for key in 0..5i16 {
        let part_path = partition_dir.join(format!("part_{key}.{ext}"));
        assert!(part_path.exists(), "partition file for key {key} missing");

        let table_name = format!("part_{key}");
        register_table(&mut ctx, &table_name, &part_path, ext).await;

        let partition_rows = row_count(&ctx, &table_name).await;
        assert!(partition_rows > 0, "partition {key} is empty");
        total_partition_rows += partition_rows;

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

#[tokio::test]
async fn test_round_trip_split_and_merge_10m_arrow() {
    round_trip_split_merge("arrow").await;
}

#[tokio::test]
async fn test_round_trip_split_and_merge_10m_parquet() {
    round_trip_split_merge("parquet").await;
}

// vortex uses block_in_place which requires the multi-threaded runtime
#[tokio::test(flavor = "multi_thread")]
async fn test_round_trip_split_and_merge_10m_vortex() {
    round_trip_split_merge("vortex").await;
}
