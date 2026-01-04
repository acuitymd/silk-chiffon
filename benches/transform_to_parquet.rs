//! Benchmarks for full Arrow → Parquet transform using different writer strategies.
//!
//! This measures realistic end-to-end performance including reading from Arrow files.

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use silk_chiffon::sinks::parquet::{
    DEFAULT_BUFFER_SIZE, EagerParquetWriter, StreamParquetWriter, recommended_concurrency,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const DEFAULT_MIN_DISPATCH_ROWS: usize = 32768;

fn min_dispatch_rows() -> usize {
    std::env::var("SILK_MIN_DISPATCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MIN_DISPATCH_ROWS)
}

#[derive(Clone, Copy, Debug)]
enum WriterStrategy {
    Sequential,
    Stream,
    Eager,
}

impl std::fmt::Display for WriterStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterStrategy::Sequential => write!(f, "seq"),
            WriterStrategy::Stream => write!(f, "stream"),
            WriterStrategy::Eager => write!(f, "eager"),
        }
    }
}

#[derive(Clone, Copy)]
struct Config {
    cols: usize,
    rows: usize,
    row_group_size: usize,
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rows_m = self.rows / 1_000_000;
        let rg_k = self.row_group_size / 1_000;
        write!(f, "{}cols_{}Mrows_{}Krg", self.cols, rows_m, rg_k)
    }
}

const CONFIGS: &[Config] = &[
    // 10M rows with 1M row groups (10 row groups)
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
    // 100M rows with 1M row groups (100 row groups)
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

/// Batch size when writing Arrow files (matches DataFusion default)
const ARROW_BATCH_SIZE: usize = 8192;

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
fn create_batch(schema: &Arc<Schema>, start_row: usize, num_rows: usize) -> RecordBatch {
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        let col: Arc<dyn arrow::array::Array> = match field.data_type() {
            DataType::Int32 => {
                let values: Vec<i32> = (start_row..start_row + num_rows)
                    .map(|r| (r * (i + 1)) as i32)
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<i64> = (start_row..start_row + num_rows)
                    .map(|r| (r * (i + 1)) as i64)
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<f64> = (start_row..start_row + num_rows)
                    .map(|r| (r as f64) * (i as f64 + 1.0))
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::Utf8 => {
                let values: Vec<String> = (start_row..start_row + num_rows)
                    .map(|r| format!("value_{}_{}", i, r))
                    .collect();
                Arc::new(StringArray::from(values))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let values: Vec<i64> = (start_row..start_row + num_rows)
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

/// Pre-create Arrow file for a config
fn create_arrow_file(temp_dir: &TempDir, config: Config) -> PathBuf {
    let schema = create_schema(config.cols);
    let path = temp_dir.path().join(format!("{}.arrow", config));

    let file = File::create(&path).unwrap();
    let mut writer = FileWriter::try_new(BufWriter::new(file), &schema).unwrap();

    let mut remaining = config.rows;
    let mut offset = 0;
    while remaining > 0 {
        let batch_size = remaining.min(ARROW_BATCH_SIZE);
        let batch = create_batch(&schema, offset, batch_size);
        writer.write(&batch).unwrap();
        offset += batch_size;
        remaining -= batch_size;
    }

    writer.finish().unwrap();
    path
}

/// Estimate bytes per row based on schema
fn estimate_row_size(schema: &Schema) -> u64 {
    schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int32 => 4,
            DataType::Int64 => 8,
            DataType::Float64 => 8,
            DataType::Utf8 => 20, // average string length estimate
            DataType::Timestamp(_, _) => 8,
            _ => 8, // default
        })
        .sum()
}

fn bench_transform(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();

    // pre-create all Arrow files
    println!("Creating Arrow test files...");
    let arrow_files: Vec<(Config, PathBuf)> = CONFIGS
        .iter()
        .map(|&config| {
            println!("  Creating {}", config);
            let path = create_arrow_file(&temp_dir, config);
            (config, path)
        })
        .collect();
    println!("Done creating test files.\n");

    let mut group = c.benchmark_group("transform_to_parquet");
    group.sample_size(10);

    for (config, arrow_path) in &arrow_files {
        let schema = create_schema(config.cols);
        let row_size = estimate_row_size(&schema);
        let total_bytes = row_size * config.rows as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        for strategy in [
            WriterStrategy::Sequential,
            WriterStrategy::Stream,
            WriterStrategy::Eager,
        ] {
            let bench_id = BenchmarkId::new(format!("{}", strategy), format!("{}", config));

            group.bench_with_input(
                bench_id,
                &(arrow_path, config),
                |b, (arrow_path, config)| {
                    b.iter(|| {
                        // read Arrow file from disk each iteration
                        let file = File::open(arrow_path).unwrap();
                        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
                        let schema = reader.schema();

                        let out_path = temp_dir
                            .path()
                            .join(format!("out_{}_{}.parquet", config, strategy));
                        let props = WriterProperties::builder()
                            .set_max_row_group_size(config.row_group_size)
                            .build();

                        match strategy {
                            WriterStrategy::Sequential => {
                                let file = BufWriter::with_capacity(
                                    DEFAULT_BUFFER_SIZE,
                                    File::create(&out_path).unwrap(),
                                );
                                let mut writer =
                                    ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
                                        .unwrap();

                                for batch in reader {
                                    writer.write(&batch.unwrap()).unwrap();
                                }
                                writer.close().unwrap();
                            }
                            WriterStrategy::Stream => rt.block_on(async {
                                let concurrency = recommended_concurrency(config.cols);
                                let mut writer = StreamParquetWriter::new(
                                    &out_path,
                                    &schema,
                                    props,
                                    config.row_group_size,
                                    DEFAULT_BUFFER_SIZE,
                                    concurrency,
                                );

                                // need to collect since FileReader isn't Send
                                let file = File::open(arrow_path).unwrap();
                                let reader =
                                    arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
                                for batch in reader {
                                    writer.write(batch.unwrap()).await.unwrap();
                                }
                                writer.close().await.unwrap();
                            }),
                            WriterStrategy::Eager => rt.block_on(async {
                                // cap total column threads to ~3x CPU count
                                let num_cpus = std::thread::available_parallelism()
                                    .map(|n| n.get())
                                    .unwrap_or(8);
                                let max_threads = num_cpus * 3;
                                let concurrency = 4.min(max_threads / config.cols).max(1);
                                let mut writer = EagerParquetWriter::new(
                                    &out_path,
                                    &schema,
                                    props,
                                    config.row_group_size,
                                    DEFAULT_BUFFER_SIZE,
                                    concurrency,
                                    min_dispatch_rows(),
                                );

                                // need to collect since FileReader isn't Send
                                let file = File::open(arrow_path).unwrap();
                                let reader =
                                    arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
                                for batch in reader {
                                    writer.write(batch.unwrap()).await.unwrap();
                                }
                                writer.close().await.unwrap();
                            }),
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_transform);
criterion_main!(benches);
