//! Benchmarks comparing sequential vs parallel parquet writing.

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use silk_chiffon::sinks::parquet::ParquetWriter;
use tempfile::tempdir;

#[derive(Clone, Copy)]
struct Config {
    cols: usize,
    rows: usize,
    row_group_size: usize,
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rows_m = self.rows / 1_000_000;
        let rg_m = self.row_group_size / 1_000_000;
        write!(f, "{}cols_{}Mrows_{}Mrg", self.cols, rows_m, rg_m)
    }
}

const CONFIGS: &[Config] = &[
    // 1M total rows
    Config {
        cols: 2,
        rows: 1_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 2,
        rows: 1_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 2,
        rows: 1_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 5,
        rows: 1_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 5,
        rows: 1_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 5,
        rows: 1_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 9,
        rows: 1_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 9,
        rows: 1_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 9,
        rows: 1_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 17,
        rows: 1_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 17,
        rows: 1_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 17,
        rows: 1_000_000,
        row_group_size: 8_000_000,
    },
    // 10M total rows
    Config {
        cols: 2,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 2,
        rows: 10_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 2,
        rows: 10_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 5,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 5,
        rows: 10_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 5,
        rows: 10_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 9,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 9,
        rows: 10_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 9,
        rows: 10_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 17,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 17,
        rows: 10_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 17,
        rows: 10_000_000,
        row_group_size: 8_000_000,
    },
    // 100M total rows
    Config {
        cols: 2,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 2,
        rows: 100_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 2,
        rows: 100_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 5,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 5,
        rows: 100_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 5,
        rows: 100_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 9,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 9,
        rows: 100_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 9,
        rows: 100_000_000,
        row_group_size: 8_000_000,
    },
    Config {
        cols: 17,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 17,
        rows: 100_000_000,
        row_group_size: 3_000_000,
    },
    Config {
        cols: 17,
        rows: 100_000_000,
        row_group_size: 8_000_000,
    },
];

const INPUT_BATCH_SIZE: usize = 500_000;

fn create_schema(num_columns: usize) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let field = match i % 5 {
            0 => Field::new(format!("int32_{}", i), DataType::Int32, false),
            1 => Field::new(format!("int64_{}", i), DataType::Int64, false),
            2 => Field::new(format!("float64_{}", i), DataType::Float64, false),
            3 => Field::new(format!("string_{}", i), DataType::Utf8, false),
            4 => Field::new(
                format!("timestamp_{}", i),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            _ => unreachable!(),
        };
        fields.push(field);
    }

    Arc::new(Schema::new(fields))
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss
)]
fn create_batch(schema: &Arc<Schema>, num_rows: usize) -> RecordBatch {
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        let col: Arc<dyn arrow::array::Array> = match field.data_type() {
            DataType::Int32 => {
                let values: Vec<i32> = (0..num_rows).map(|r| (r * (i + 1)) as i32).collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<i64> = (0..num_rows).map(|r| (r * (i + 1)) as i64).collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<f64> = (0..num_rows)
                    .map(|r| (r as f64) * (i as f64 + 1.0))
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::Utf8 => {
                let values: Vec<String> = (0..num_rows)
                    .map(|r| format!("value_{}_{}", i, r))
                    .collect();
                Arc::new(StringArray::from(values))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let values: Vec<i64> = (0..num_rows)
                    .map(|r| 1_700_000_000_000_000i64 + (r as i64) * 1_000_000)
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            _ => panic!("unexpected data type"),
        };
        columns.push(col);
    }

    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

fn split_into_batches(
    schema: &Arc<Schema>,
    total_rows: usize,
    batch_size: usize,
) -> Vec<RecordBatch> {
    let num_batches = total_rows.div_ceil(batch_size);
    let mut batches = Vec::with_capacity(num_batches);

    let mut remaining = total_rows;
    while remaining > 0 {
        let this_batch_size = remaining.min(batch_size);
        batches.push(create_batch(schema, this_batch_size));
        remaining -= this_batch_size;
    }

    batches
}

#[allow(clippy::cast_possible_truncation)]
fn estimate_batch_size(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|col| col.get_array_memory_size() as u64)
        .sum()
}

fn bench_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential");

    for config in CONFIGS {
        let schema = create_schema(config.cols);
        let batches = split_into_batches(&schema, config.rows, INPUT_BATCH_SIZE);
        let size: u64 = batches.iter().map(estimate_batch_size).sum();

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(config),
            &batches,
            |b, batches| {
                b.iter(|| {
                    let temp_dir = tempdir().unwrap();
                    let path = temp_dir.path().join("test.parquet");
                    let file = std::fs::File::create(&path).unwrap();
                    let props = WriterProperties::builder()
                        .set_max_row_group_size(config.row_group_size)
                        .build();

                    let mut writer =
                        ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
                    for batch in black_box(batches) {
                        writer.write(batch).unwrap();
                    }
                    writer.close().unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel");

    for config in CONFIGS {
        let schema = create_schema(config.cols);
        let batches = split_into_batches(&schema, config.rows, INPUT_BATCH_SIZE);
        let size: u64 = batches.iter().map(estimate_batch_size).sum();

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(config),
            &batches,
            |b, batches| {
                b.iter(|| {
                    let temp_dir = tempdir().unwrap();
                    let path = temp_dir.path().join("test.parquet");
                    let props = WriterProperties::builder().build();

                    let mut writer =
                        ParquetWriter::try_new(&path, &schema, props, config.row_group_size, None)
                            .unwrap();
                    for batch in black_box(batches) {
                        writer.write(batch).unwrap();
                    }
                    writer.close().unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_sequential, bench_parallel);
criterion_main!(benches);
