use arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatchReader, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::catalog::stream::{StreamConfig, StreamProvider, StreamTable};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::DisplayFormatType;
use duckdb::Connection;
use silk_chiffon::utils::arrow_io::ArrowIPCFormat;
use silk_chiffon::{ArrowCompression, ListOutputsFormat, SplitToArrowArgs};
use std::fmt::Formatter;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[derive(Clone)]
struct BenchmarkScenario {
    name: &'static str,
    num_rows: usize,
    cardinality: usize,
    data_type: DataType,
}

fn generate_test_data(
    num_rows: usize,
    cardinality: usize,
    data_type: &DataType,
) -> Vec<RecordBatch> {
    let batch_size = 10_000;
    let num_batches = num_rows.div_ceil(batch_size);

    let split_field = Field::new("split_col", data_type.clone(), false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        split_field,
        Field::new("value", DataType::Float64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let mut batches = Vec::with_capacity(num_batches);
    let mut id_counter = 0i64;
    let payload_base = "x".repeat(100);

    for batch_idx in 0..num_batches {
        let rows_in_batch = batch_size.min(num_rows - batch_idx * batch_size);
        let mut ids = Vec::with_capacity(rows_in_batch);
        let mut values = Vec::with_capacity(rows_in_batch);
        let mut payloads = Vec::with_capacity(rows_in_batch);

        let split_array: ArrayRef = match data_type {
            DataType::Int32 => {
                let mut split_values = Vec::with_capacity(rows_in_batch);
                for row_idx in 0..rows_in_batch {
                    let value = ((batch_idx * batch_size + row_idx) % cardinality) as i32;
                    split_values.push(value);
                }
                Arc::new(Int32Array::from(split_values))
            }
            DataType::Utf8 => {
                let mut split_values = Vec::with_capacity(rows_in_batch);
                for row_idx in 0..rows_in_batch {
                    let value = (batch_idx * batch_size + row_idx) % cardinality;
                    split_values.push(format!("key_{value:06}"));
                }
                Arc::new(StringArray::from(split_values))
            }
            _ => panic!("Unsupported data type for benchmark: {data_type:?}"),
        };

        for _ in 0..rows_in_batch {
            ids.push(id_counter);
            values.push(rand::random::<f64>() * 1000.0);
            payloads.push(format!("{payload_base}{id_counter}"));
            id_counter += 1;
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                split_array,
                Arc::new(Float64Array::from(values)) as ArrayRef,
                Arc::new(StringArray::from(payloads)) as ArrayRef,
            ],
        )
        .unwrap();

        batches.push(batch);
    }

    batches
}

fn write_test_data(batches: &[RecordBatch], path: &std::path::Path) {
    let file = File::create(path).unwrap();
    let mut writer = StreamWriter::try_new(file, &batches[0].schema()).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
}

fn setup_benchmark_data(scenario: &BenchmarkScenario) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");

    let batches = generate_test_data(scenario.num_rows, scenario.cardinality, &scenario.data_type);
    write_test_data(&batches, &input_path);

    (temp_dir, input_path)
}

async fn run_silk_arrow(input_path: &std::path::Path, output_dir: &std::path::Path) {
    let args = SplitToArrowArgs {
        input: clio::Input::new(input_path).unwrap(),
        by: "split_col".to_string(),
        output_template: format!("{}/{{value}}.arrow", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::Lz4,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
        query: None,
        exclude_columns: vec![],
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();
}

fn run_duckdb_arrow(input_path: &std::path::Path, output_dir: &std::path::Path) {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("INSTALL nanoarrow FROM community", [])
        .unwrap();
    conn.execute("LOAD nanoarrow", []).unwrap();

    let query = format!(
        "COPY (SELECT * FROM read_arrow('{}')) TO '{}' (FORMAT ARROW, PARTITION_BY (split_col))",
        input_path.display(),
        output_dir.join("data.arrow").display()
    );

    conn.execute(&query, []).unwrap();
}

#[derive(Debug)]
struct ArrowIPCStreamProvider {
    schema: Arc<Schema>,
    location: PathBuf,
}

impl ArrowIPCStreamProvider {
    pub fn new(schema: Arc<Schema>, location: PathBuf) -> Self {
        Self { schema, location }
    }
}

impl StreamProvider for ArrowIPCStreamProvider {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn reader(
        &self,
    ) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>>>, DataFusionError>
    {
        Ok(Box::new(StreamReader::try_new_buffered(
            File::open(&self.location)?,
            None,
        )?))
    }

    fn stream_write_display(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("ArrowIPCStreamProvider")
            .field("location", &self.location)
            .finish()
    }
}

async fn run_datafusion_parquet(input_path: &std::path::Path, output_dir: &std::path::Path) {
    use datafusion::prelude::*;

    let ctx = SessionContext::new();

    // A little trick I figured out: https://gist.github.com/corasaurus-hex/96574afd82780e48c4d0c679c116b23a
    let file = File::open(input_path).unwrap();
    let reader = StreamReader::try_new_buffered(file, None).unwrap();
    let stream_provider = Arc::new(ArrowIPCStreamProvider::new(
        reader.schema(),
        input_path.to_path_buf(),
    ));
    let stream_table = StreamTable::new(Arc::new(StreamConfig::new(stream_provider)));

    ctx.register_table("arrow_table", Arc::new(stream_table))
        .unwrap();

    let df = ctx.table("arrow_table").await.unwrap();

    df.write_parquet(
        output_dir.to_str().unwrap(),
        datafusion::dataframe::DataFrameWriteOptions::new()
            .with_partition_by(vec!["split_col".to_string()]),
        None,
    )
    .await
    .unwrap();
}

fn bench_small_datasets(c: &mut Criterion) {
    let scenarios = vec![
        BenchmarkScenario {
            name: "small_low_cardinality",
            num_rows: 100_000,
            cardinality: 10,
            data_type: DataType::Int32,
        },
        BenchmarkScenario {
            name: "small_medium_cardinality",
            num_rows: 100_000,
            cardinality: 100,
            data_type: DataType::Int32,
        },
        BenchmarkScenario {
            name: "small_high_cardinality",
            num_rows: 100_000,
            cardinality: 1000,
            data_type: DataType::Int32,
        },
    ];

    let mut group = c.benchmark_group("partition_split_small");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for scenario in scenarios {
        group.throughput(Throughput::Elements(scenario.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("silk_arrow", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_benchmark_data(scenario),
                    |(temp_dir, input_path)| async move {
                        let output_dir = temp_dir.path().join("silk_output");
                        fs::create_dir_all(&output_dir).unwrap();
                        run_silk_arrow(&input_path, &output_dir).await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("duckdb_arrow", scenario.name),
            &scenario,
            |b, scenario| {
                b.iter_batched(
                    || setup_benchmark_data(scenario),
                    |(temp_dir, input_path)| {
                        let output_dir = temp_dir.path().join("duckdb_output");
                        fs::create_dir_all(&output_dir).unwrap();
                        run_duckdb_arrow(&input_path, &output_dir);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("datafusion_parquet", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_benchmark_data(scenario),
                    |(temp_dir, input_path)| async move {
                        let output_dir = temp_dir.path().join("datafusion_output");
                        fs::create_dir_all(&output_dir).unwrap();
                        run_datafusion_parquet(&input_path, &output_dir).await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_large_datasets(c: &mut Criterion) {
    let scenarios = vec![
        BenchmarkScenario {
            name: "1GB_low_cardinality",
            num_rows: 10_000_000,
            cardinality: 10,
            data_type: DataType::Int32,
        },
        BenchmarkScenario {
            name: "1GB_medium_cardinality",
            num_rows: 10_000_000,
            cardinality: 100,
            data_type: DataType::Int32,
        },
        BenchmarkScenario {
            name: "1GB_high_cardinality",
            num_rows: 10_000_000,
            cardinality: 1000,
            data_type: DataType::Int32,
        },
    ];

    let mut group = c.benchmark_group("partition_split_large");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));
    group.warm_up_time(Duration::from_secs(5));

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for scenario in scenarios {
        group.throughput(Throughput::Elements(scenario.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("silk_arrow", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_benchmark_data(scenario),
                    |(temp_dir, input_path)| async move {
                        let output_dir = temp_dir.path().join("silk_output");
                        fs::create_dir_all(&output_dir).unwrap();
                        run_silk_arrow(&input_path, &output_dir).await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_small_datasets, bench_large_datasets
}

criterion_main!(benches);
