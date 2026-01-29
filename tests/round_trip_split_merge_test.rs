//! Round-trip integration tests:
//!
//! 1. generate 10M rows
//! 2. split by partition key
//! 3. verify that each partition file contains exactly the rows where partition_key = key
//! 4. merge back
//! 5. verify that the merged output is identical to the original
//!
//! Tests arrow, parquet, and vortex formats to ensure data survives a split + merge round trip.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Date32Array, Int16Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
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

fn rand_i32(rng: &mut impl Rng, range: Range<i32>, null_pct: f64) -> Option<i32> {
    if rng.random_bool(null_pct) {
        None
    } else {
        Some(rng.random_range(range))
    }
}

fn maybe_null<T>(rng: &mut impl Rng, value: T, null_pct: f64) -> Option<T> {
    if rng.random_bool(null_pct) {
        None
    } else {
        Some(value)
    }
}

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
///
/// - `static_val`: i16, always 67, ~2% null
/// - `partition_key`: i16, 0..5, never null
/// - `row_id`: i32, unique sequential integer, never null (sort key)
/// - `metric_a`: i32, 0..100k, ~3% null
/// - `metric_b`: i32, 0..50k, ~4% null
/// - `metric_c`: i32, 0..200k, ~6% null
/// - `metric_d`: i32, 0..80k, ~3% null
/// - `metric_e`: i32, 0..150k, ~2% null
/// - `metric_f`: i32, 0..120k, ~2% null
/// - `event_date`: Date32, 19000..20000, ~2% null
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn generate_batch(
    rng: &mut impl Rng,
    start_row: usize,
    count: usize,
    schema: &SchemaRef,
) -> RecordBatch {
    let static_vals: Vec<_> = (0..count).map(|_| maybe_null(rng, 67i16, 0.02)).collect();
    let partition_keys: Vec<_> = (0..count)
        .map(|_| Some(rng.random_range(0..5i16)))
        .collect();
    let row_ids: Vec<_> = (start_row..start_row + count)
        .map(|r| Some(r as i32))
        .collect();
    let metric_as: Vec<_> = (0..count)
        .map(|_| rand_i32(rng, 0..100_000, 0.03))
        .collect();
    let metric_bs: Vec<_> = (0..count).map(|_| rand_i32(rng, 0..50_000, 0.04)).collect();
    let metric_cs: Vec<_> = (0..count)
        .map(|_| rand_i32(rng, 0..200_000, 0.06))
        .collect();
    let metric_ds: Vec<_> = (0..count).map(|_| rand_i32(rng, 0..80_000, 0.03)).collect();
    let metric_es: Vec<_> = (0..count)
        .map(|_| rand_i32(rng, 0..150_000, 0.02))
        .collect();
    let metric_fs: Vec<_> = (0..count)
        .map(|_| rand_i32(rng, 0..120_000, 0.02))
        .collect();
    let dates: Vec<_> = (0..count)
        .map(|_| rand_i32(rng, 19000..20000, 0.02))
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

async fn symmetric_diff_table_row_count(
    ctx: &SessionContext,
    table_a: &str,
    table_b: &str,
) -> usize {
    let query = format!(
        "(SELECT * FROM {table_a} EXCEPT ALL SELECT * FROM {table_b}) \
         UNION ALL \
         (SELECT * FROM {table_b} EXCEPT ALL SELECT * FROM {table_a})"
    );
    let diff = ctx.sql(&query).await.unwrap().collect().await.unwrap();
    diff.iter().map(|b| b.num_rows()).sum()
}

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

async fn write_test_data(path: &Path, schema: &SchemaRef, ext: &str) {
    // SmallRng is like 5x faster(!!) than the default RNG (ChaChaRng)
    let mut rng = SmallRng::from_os_rng();
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
    let num_batches = NUM_ROWS.div_ceil(BATCH_SIZE);
    for i in 0..num_batches {
        let start = i * BATCH_SIZE;
        let count = BATCH_SIZE.min(NUM_ROWS - start);
        let batch = generate_batch(&mut rng, start, count, schema);
        sink.write_batch(batch).await.unwrap();
    }
    sink.finish().await.unwrap();
}

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

    // merge partitions back
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

    let diff_rows = symmetric_diff_table_row_count(&ctx, "original", "merged").await;

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

#[tokio::test(flavor = "multi_thread")]
async fn test_round_trip_split_and_merge_10m_vortex() {
    round_trip_split_merge("vortex").await;
}
