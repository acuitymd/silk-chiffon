//! Benchmarks comparing high-cardinality vs low-cardinality partitioning strategies.
//!
//! High-cardinality: DataFusion sorts the entire stream, then writes to one file at a time.
//! Low-cardinality: Keeps file handles open per partition, sorts per-batch only.

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use silk_chiffon::TransformCommand;
use silk_chiffon::commands::transform;
use tempfile::TempDir;
use tokio::runtime::Runtime;

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

const CONFIGS: &[Config] = &[
    // small dataset, few partitions
    Config {
        rows: 100_000,
        partitions: 4,
    },
    Config {
        rows: 100_000,
        partitions: 10,
    },
    Config {
        rows: 100_000,
        partitions: 50,
    },
    // medium dataset
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
    // larger dataset
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

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

/// Create interleaved data - worst case for high-cardinality partitioning.
/// Rows cycle through partition values: p0, p1, p2, ..., pN, p0, p1, ...
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn create_interleaved_batch(
    schema: &SchemaRef,
    num_rows: usize,
    num_partitions: usize,
) -> RecordBatch {
    let ids: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();
    let partition_keys: Vec<String> = (0..num_rows)
        .map(|i| format!("partition_{}", i % num_partitions))
        .collect();
    let values: Vec<i32> = (0..num_rows).map(|i| (i * 7) as i32).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(partition_keys)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
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
        low_cardinality_partition: false,
        exclude_columns: vec![],
        list_outputs: None,
        list_outputs_file: None,
        create_dirs: true,
        overwrite: true,
        query: None,
        dialect: Default::default(),
        sort_by: None,
        memory_limit: None,
        target_partitions: None,
        input_format: None,
        output_format: Some(silk_chiffon::DataFormat::Parquet),
        arrow_compression: None,
        arrow_format: None,
        arrow_record_batch_size: None,
        parquet_compression: None,
        parquet_bloom_all: None,
        parquet_bloom_column: vec![],
        parquet_row_group_size: None,
        parquet_buffer_size: None,
        parquet_parallelism: None,
        parquet_statistics: None,
        parquet_writer_version: None,
        parquet_no_dictionary: false,
        parquet_column_dictionary: vec![],
        parquet_column_no_dictionary: vec![],
        parquet_encoding: None,
        parquet_column_encoding: vec![],
        parquet_sorted_metadata: false,
        vortex_record_batch_size: None,
    }
}

fn bench_high_cardinality(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("partition_high_cardinality");

    for config in CONFIGS {
        let schema = create_schema();
        // estimate ~30 bytes per row (8 + ~14 + 4 + overhead)
        let size = (config.rows * 30) as u64;

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(config),
            &(&schema, config),
            |b, (schema, config)| {
                b.iter(|| {
                    rt.block_on(async {
                        let temp_dir = TempDir::new().unwrap();
                        let input = temp_dir.path().join("input.arrow");
                        let output_template = temp_dir
                            .path()
                            .join("out/{{partition_key}}.parquet")
                            .to_string_lossy()
                            .to_string();

                        // create input with interleaved partition values
                        let batch =
                            create_interleaved_batch(schema, config.rows, config.partitions);
                        write_arrow_file(&input, schema, vec![batch]);

                        let cmd = TransformCommand {
                            from: Some(input.to_string_lossy().to_string()),
                            to_many: Some(output_template),
                            by: Some("partition_key".to_string()),
                            low_cardinality_partition: false,
                            ..default_transform_command()
                        };

                        transform::run(cmd).await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_low_cardinality(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("partition_low_cardinality");

    for config in CONFIGS {
        let schema = create_schema();
        let size = (config.rows * 30) as u64;

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(config),
            &(&schema, config),
            |b, (schema, config)| {
                b.iter(|| {
                    rt.block_on(async {
                        let temp_dir = TempDir::new().unwrap();
                        let input = temp_dir.path().join("input.arrow");
                        let output_template = temp_dir
                            .path()
                            .join("out/{{partition_key}}.parquet")
                            .to_string_lossy()
                            .to_string();

                        let batch =
                            create_interleaved_batch(schema, config.rows, config.partitions);
                        write_arrow_file(&input, schema, vec![batch]);

                        let cmd = TransformCommand {
                            from: Some(input.to_string_lossy().to_string()),
                            to_many: Some(output_template),
                            by: Some("partition_key".to_string()),
                            low_cardinality_partition: true,
                            ..default_transform_command()
                        };

                        transform::run(cmd).await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_high_cardinality, bench_low_cardinality);
criterion_main!(benches);
