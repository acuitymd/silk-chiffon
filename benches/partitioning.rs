//! Benchmarks comparing high-cardinality vs low-cardinality partitioning strategies.
//!
//! High-cardinality: DataFusion sorts the entire stream, then writes to one file at a time.
//! Low-cardinality: Keeps file handles open per partition, no sorting required.
//!
//! Input patterns tested:
//! - Interleaved: worst case - rows cycle through partitions (p0, p1, p2, p0, p1, ...)
//! - Sorted: best case - all rows for each partition are contiguous
//! - Clustered: realistic case - rows come in clusters of ~1000 before switching partitions

use std::sync::Arc;

use arrow::array::{Date32Array, Int16Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use silk_chiffon::commands::transform;
use silk_chiffon::{PartitionStrategy, TransformCommand};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Pre-created test files for a benchmark run
struct TestFixture {
    _temp_dir: TempDir,
    input_path: String,
    output_template: String,
}

#[derive(Clone, Copy)]
struct Config {
    rows: usize,
    partitions: usize,
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rows_k = self.rows / 1_000;
        write!(f, "{}Krows_{}parts", rows_k, self.partitions)
    }
}

/// Configs for stress-testing: vary partition count to find scaling limits
const STRESS_CONFIGS: &[Config] = &[
    Config {
        rows: 1_000_000,
        partitions: 4,
    },
    Config {
        rows: 1_000_000,
        partitions: 10,
    },
    Config {
        rows: 1_000_000,
        partitions: 50,
    },
    Config {
        rows: 10_000_000,
        partitions: 4,
    },
    Config {
        rows: 10_000_000,
        partitions: 10,
    },
    Config {
        rows: 10_000_000,
        partitions: 50,
    },
];

/// Realistic configs: 5 partitions (like region/status), varying data sizes
const REALISTIC_CONFIGS: &[Config] = &[
    Config {
        rows: 100_000,
        partitions: 5,
    },
    Config {
        rows: 1_000_000,
        partitions: 5,
    },
    Config {
        rows: 10_000_000,
        partitions: 5,
    },
];

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("field1", DataType::Int16, false),
        Field::new("field2", DataType::Int16, false),
        Field::new("field3", DataType::Int32, false),
        Field::new("field4", DataType::Int32, false),
        Field::new("field5", DataType::Int32, false),
        Field::new("field6", DataType::Int32, false),
        Field::new("field7", DataType::Int32, false),
        Field::new("field8", DataType::Int32, false),
        Field::new("field9", DataType::Date32, false),
        Field::new("field10", DataType::Int32, false),
    ]))
}

/// Build a batch from field1 values (partition keys) and fill other fields with deterministic data.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn build_batch(schema: &SchemaRef, field1_values: Vec<i16>, num_rows: usize) -> RecordBatch {
    let field2: Vec<i16> = (0..num_rows).map(|i| (i % 100) as i16).collect();
    let field3: Vec<i32> = (0..num_rows).map(|i| (i * 7) as i32).collect();
    let field4: Vec<i32> = (0..num_rows).map(|i| (i * 13) as i32).collect();
    let field5: Vec<i32> = (0..num_rows).map(|i| (i * 17) as i32).collect();
    let field6: Vec<i32> = (0..num_rows).map(|i| (i * 23) as i32).collect();
    let field7: Vec<i32> = (0..num_rows).map(|i| (i * 29) as i32).collect();
    let field8: Vec<i32> = (0..num_rows).map(|i| (i * 31) as i32).collect();
    // days since unix epoch, spread across ~10 years
    let field9: Vec<i32> = (0..num_rows).map(|i| 18000 + (i % 3650) as i32).collect();
    let field10: Vec<i32> = (0..num_rows).map(|i| (i * 41) as i32).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int16Array::from(field1_values)),
            Arc::new(Int16Array::from(field2)),
            Arc::new(Int32Array::from(field3)),
            Arc::new(Int32Array::from(field4)),
            Arc::new(Int32Array::from(field5)),
            Arc::new(Int32Array::from(field6)),
            Arc::new(Int32Array::from(field7)),
            Arc::new(Int32Array::from(field8)),
            Arc::new(Date32Array::from(field9)),
            Arc::new(Int32Array::from(field10)),
        ],
    )
    .unwrap()
}

/// Create interleaved data - worst case for low-cardinality partitioning.
/// Rows cycle through partition values: p0, p1, p2, ..., pN, p0, p1, ...
/// This creates maximum slice fragmentation.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn create_interleaved_batch(
    schema: &SchemaRef,
    num_rows: usize,
    num_partitions: usize,
) -> RecordBatch {
    let field1: Vec<i16> = (0..num_rows).map(|i| (i % num_partitions) as i16).collect();
    build_batch(schema, field1, num_rows)
}

/// Create sorted data - best case for low-cardinality partitioning.
/// All rows for partition 0, then all rows for partition 1, etc.
/// Each partition gets contiguous slices with no fragmentation.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn create_sorted_batch(schema: &SchemaRef, num_rows: usize, num_partitions: usize) -> RecordBatch {
    let rows_per_partition = num_rows / num_partitions;
    let field1: Vec<i16> = (0..num_rows)
        .map(|i| (i / rows_per_partition.max(1)) as i16)
        .collect();
    build_batch(schema, field1, num_rows)
}

/// Create clustered data - realistic case.
/// Rows come in clusters of ~1000 before switching to next partition.
/// Simulates data with natural locality (e.g., logs from same region arriving together).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn create_clustered_batch(
    schema: &SchemaRef,
    num_rows: usize,
    num_partitions: usize,
) -> RecordBatch {
    const CLUSTER_SIZE: usize = 1000;
    let field1: Vec<i16> = (0..num_rows)
        .map(|i| ((i / CLUSTER_SIZE) % num_partitions) as i16)
        .collect();
    build_batch(schema, field1, num_rows)
}

fn write_arrow_file(path: &std::path::Path, schema: &SchemaRef, batches: Vec<RecordBatch>) {
    use arrow::ipc::writer::FileWriter;
    let file = std::fs::File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, schema).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
}

fn default_transform_command() -> TransformCommand {
    TransformCommand {
        from: None,
        from_many: vec![],
        to: None,
        to_many: None,
        by: None,
        partition_strategy: PartitionStrategy::SortSingle,
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: true,
        query: None,
        dialect: Default::default(),
        sort_by: None,
        memory_limit: None,
        preserve_input_order: false,
        target_partitions: None,
        input_format: None,
        output_format: Some(silk_chiffon::DataFormat::Parquet),
        ..TransformCommand::default()
    }
}

type BatchCreator = fn(&SchemaRef, usize, usize) -> RecordBatch;

fn run_partition_benchmark(
    c: &mut Criterion,
    group_name: &str,
    configs: &[Config],
    partition_strategy: PartitionStrategy,
    create_batch: BatchCreator,
    sample_size: usize,
) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group(group_name);
    group.sample_size(sample_size);
    let schema = create_schema();

    // create all input files upfront (once per config)
    const BATCH_SIZE: usize = 128_800;
    let fixtures: Vec<_> = configs
        .iter()
        .map(|config| {
            let temp_dir = TempDir::new().unwrap();
            let input = temp_dir.path().join("input.arrow");
            let full_batch = create_batch(&schema, config.rows, config.partitions);
            // split into chunks of BATCH_SIZE
            let batches: Vec<_> = (0..config.rows)
                .step_by(BATCH_SIZE)
                .map(|start| {
                    let len = BATCH_SIZE.min(config.rows - start);
                    full_batch.slice(start, len)
                })
                .collect();
            write_arrow_file(&input, &schema, batches);
            TestFixture {
                input_path: input.to_string_lossy().to_string(),
                output_template: temp_dir
                    .path()
                    .join("out/{{field1}}.parquet")
                    .to_string_lossy()
                    .to_string(),
                _temp_dir: temp_dir,
            }
        })
        .collect();

    for (config, fixture) in configs.iter().zip(fixtures.iter()) {
        // 36 bytes per row: 2*2 (int16) + 7*4 (int32) + 4 (date32) = 36
        let size = (config.rows * 36) as u64;

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(config),
            &fixture,
            |b, fixture| {
                b.iter(|| {
                    // clean output dir between iterations
                    let out_dir = std::path::Path::new(&fixture.output_template)
                        .parent()
                        .unwrap();
                    let _ = std::fs::remove_dir_all(out_dir);

                    let cmd = TransformCommand {
                        from: Some(fixture.input_path.clone()),
                        to_many: Some(fixture.output_template.clone()),
                        by: Some("field1".to_string()),
                        partition_strategy,
                        ..default_transform_command()
                    };

                    rt.block_on(async {
                        transform::run(cmd).await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

// stress tests: interleaved (worst) and sorted (best) with varying partition counts
fn bench_high_card_interleaved(c: &mut Criterion) {
    run_partition_benchmark(
        c,
        "high_card/interleaved",
        STRESS_CONFIGS,
        PartitionStrategy::SortSingle,
        create_interleaved_batch,
        10,
    );
}

fn bench_high_card_sorted(c: &mut Criterion) {
    run_partition_benchmark(
        c,
        "high_card/sorted",
        STRESS_CONFIGS,
        PartitionStrategy::SortSingle,
        create_sorted_batch,
        30,
    );
}

fn bench_low_card_interleaved(c: &mut Criterion) {
    run_partition_benchmark(
        c,
        "low_card/interleaved",
        STRESS_CONFIGS,
        PartitionStrategy::NosortMulti,
        create_interleaved_batch,
        10,
    );
}

fn bench_low_card_sorted(c: &mut Criterion) {
    run_partition_benchmark(
        c,
        "low_card/sorted",
        STRESS_CONFIGS,
        PartitionStrategy::NosortMulti,
        create_sorted_batch,
        30,
    );
}

// realistic: 5 partitions, clustered data (like logs from different regions)
fn bench_high_card_realistic(c: &mut Criterion) {
    run_partition_benchmark(
        c,
        "high_card/realistic",
        REALISTIC_CONFIGS,
        PartitionStrategy::SortSingle,
        create_clustered_batch,
        30,
    );
}

fn bench_low_card_realistic(c: &mut Criterion) {
    run_partition_benchmark(
        c,
        "low_card/realistic",
        REALISTIC_CONFIGS,
        PartitionStrategy::NosortMulti,
        create_clustered_batch,
        30,
    );
}

criterion_group!(
    benches,
    bench_low_card_sorted,
    bench_high_card_sorted,
    bench_low_card_realistic,
    bench_high_card_realistic,
    bench_low_card_interleaved,
    bench_high_card_interleaved,
);
criterion_main!(benches);
