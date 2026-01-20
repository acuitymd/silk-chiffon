//! Benchmarks comparing sequential vs parallel parquet writing.

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use silk_chiffon::sinks::parquet::{ParallelParquetWriter, ParallelWriterConfig, ParquetPools};

use tempfile::tempdir;
use tokio::runtime::Runtime;

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
        cols: 5,
        rows: 1_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 9,
        rows: 1_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 17,
        rows: 1_000_000,
        row_group_size: 1_000_000,
    },
    // 10M total rows
    Config {
        cols: 2,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 5,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 9,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 17,
        rows: 10_000_000,
        row_group_size: 1_000_000,
    },
    // 100M total rows
    Config {
        cols: 2,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 5,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 9,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
    Config {
        cols: 17,
        rows: 100_000_000,
        row_group_size: 1_000_000,
    },
];

/// Realistic batch size from DataSource (DataFusion default)
const INPUT_BATCH_SIZE: usize = 8192;

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

/// Template batch that can be cloned and sliced for benchmarks
struct BatchTemplate {
    batch: RecordBatch,
    batch_size: usize,
}

impl BatchTemplate {
    fn new(schema: &Arc<Schema>, batch_size: usize) -> Self {
        Self {
            batch: create_batch(schema, batch_size),
            batch_size,
        }
    }

    fn estimated_row_size(&self) -> u64 {
        self.batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size() as u64)
            .sum::<u64>()
            / self.batch_size as u64
    }
}

/// Iterator that yields cloned batches up to total_rows
struct BatchIterator<'a> {
    template: &'a BatchTemplate,
    remaining: usize,
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let take = self.remaining.min(self.template.batch_size);
        self.remaining -= take;
        if take == self.template.batch_size {
            Some(self.template.batch.clone())
        } else {
            Some(self.template.batch.slice(0, take))
        }
    }
}

fn bench_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential");
    group.sample_size(10);

    for config in CONFIGS {
        let schema = create_schema(config.cols);
        let template = BatchTemplate::new(&schema, INPUT_BATCH_SIZE);
        let size = template.estimated_row_size() * config.rows as u64;

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(config),
            &(&schema, &template, config),
            |b, (schema, template, config)| {
                b.iter(|| {
                    let temp_dir = tempdir().unwrap();
                    let path = temp_dir.path().join("test.parquet");
                    let file = std::fs::File::create(&path).unwrap();
                    let props = WriterProperties::builder()
                        .set_max_row_group_size(config.row_group_size)
                        .build();

                    let mut writer =
                        ArrowWriter::try_new(file, Arc::clone(schema), Some(props)).unwrap();
                    for batch in black_box(BatchIterator {
                        template,
                        remaining: config.rows,
                    }) {
                        writer.write(&batch).unwrap();
                    }
                    writer.close().unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_parallel(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel");

    let pools = Arc::new(ParquetPools::default_pools().unwrap());

    for config in CONFIGS {
        let schema = create_schema(config.cols);
        let template = BatchTemplate::new(&schema, INPUT_BATCH_SIZE);
        let size = template.estimated_row_size() * config.rows as u64;

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(config),
            &(&schema, &template, config, &pools),
            |b, (schema, template, config, pools)| {
                b.iter(|| {
                    rt.block_on(async {
                        let temp_dir = tempdir().unwrap();
                        let path = temp_dir.path().join("test.parquet");
                        let props = WriterProperties::builder().build();

                        let writer_config = ParallelWriterConfig {
                            max_row_group_size: config.row_group_size,
                            max_row_group_concurrency: 4,
                            buffer_size: 8 * 1024 * 1024,
                            encoding_batch_size: 122_880,
                            batch_channel_size: 16,
                            encoded_channel_size: 4,
                            skip_arrow_metadata: true,
                        };

                        let mut writer = ParallelParquetWriter::new(
                            &path,
                            schema,
                            props,
                            Arc::clone(pools),
                            writer_config,
                        );

                        for batch in black_box(BatchIterator {
                            template,
                            remaining: config.rows,
                        }) {
                            writer.write(batch).await.unwrap();
                        }
                        writer.close().await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_sequential, bench_parallel);
criterion_main!(benches);
