use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use silk_chiffon::utils::arrow_io::ArrowIPCFormat;
use silk_chiffon::{ArrowArgs, ArrowCompression, ParquetArgs, ParquetCompression, QueryDialect};
use std::fs::File;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[derive(Clone)]
struct QueryScenario {
    name: &'static str,
    query: Option<String>,
    num_rows: usize,
}

fn generate_query_test_data(num_rows: usize) -> Vec<RecordBatch> {
    let batch_size = 10_000;
    let num_batches = num_rows.div_ceil(batch_size);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let mut batches = Vec::with_capacity(num_batches);
    let mut id_counter = 0i64;

    let categories = ["A", "B", "C", "D", "E"];
    let statuses = ["active", "inactive", "pending"];

    for batch_idx in 0..num_batches {
        let rows_in_batch = batch_size.min(num_rows - batch_idx * batch_size);
        let mut ids = Vec::with_capacity(rows_in_batch);
        let mut categories_vec = Vec::with_capacity(rows_in_batch);
        let mut values = Vec::with_capacity(rows_in_batch);
        let mut quantities = Vec::with_capacity(rows_in_batch);
        let mut statuses_vec = Vec::with_capacity(rows_in_batch);

        for _ in 0..rows_in_batch {
            ids.push(id_counter);
            categories_vec.push(categories[id_counter as usize % categories.len()]);
            values.push(rand::random::<f64>() * 1000.0);
            quantities.push((id_counter % 100) as i32);
            statuses_vec.push(statuses[id_counter as usize % statuses.len()]);
            id_counter += 1;
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(categories_vec)) as ArrayRef,
                Arc::new(Float64Array::from(values)) as ArrayRef,
                Arc::new(Int32Array::from(quantities)) as ArrayRef,
                Arc::new(StringArray::from(statuses_vec)) as ArrayRef,
            ],
        )
        .unwrap();

        batches.push(batch);
    }

    batches
}

fn write_arrow_file(batches: &[RecordBatch], path: &std::path::Path) {
    let file = File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, &batches[0].schema()).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
}

fn setup_query_benchmark(num_rows: usize) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");

    let batches = generate_query_test_data(num_rows);
    write_arrow_file(&batches, &input_path);

    (temp_dir, input_path)
}

async fn run_arrow_conversion(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
    query: Option<String>,
) {
    let args = ArrowArgs {
        input: clio::Input::new(input_path).unwrap(),
        output: clio::OutputPath::new(output_path).unwrap(),
        query,
        sort_by: None,
        dialect: QueryDialect::default(),
        compression: ArrowCompression::None,
        record_batch_size: 122_880,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::arrow::run(args).await.unwrap();
}

async fn run_parquet_conversion(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
    query: Option<String>,
) {
    let args = ParquetArgs {
        input: clio::Input::new(input_path).unwrap(),
        output: clio::OutputPath::new(output_path).unwrap(),
        query,
        dialect: QueryDialect::default(),
        sort_by: None,
        compression: ParquetCompression::None,
        statistics: silk_chiffon::ParquetStatistics::Page,
        max_row_group_size: 1_048_576,
        writer_version: silk_chiffon::ParquetWriterVersion::V2,
        no_dictionary: false,
        write_sorted_metadata: false,
        bloom_all: None,
        bloom_column: vec![],
        record_batch_size: 122_880,
    };

    silk_chiffon::commands::parquet::run(args).await.unwrap();
}

fn bench_query_vs_no_query(c: &mut Criterion) {
    let scenarios = vec![
        QueryScenario {
            name: "no_query",
            query: None,
            num_rows: 1_000_000,
        },
        QueryScenario {
            name: "filter_60_percent",
            query: Some("SELECT * FROM data WHERE category IN ('A', 'B', 'C')".to_string()),
            num_rows: 1_000_000,
        },
        QueryScenario {
            name: "filter_10_percent",
            query: Some("SELECT * FROM data WHERE quantity > 90".to_string()),
            num_rows: 1_000_000,
        },
        QueryScenario {
            name: "filter_1_percent",
            query: Some("SELECT * FROM data WHERE quantity > 99".to_string()),
            num_rows: 1_000_000,
        },
    ];

    let mut group = c.benchmark_group("query_filtering");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for scenario in scenarios {
        group.throughput(Throughput::Elements(scenario.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("arrow", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_query_benchmark(scenario.num_rows),
                    |(temp_dir, input_path)| async move {
                        let output_path = temp_dir.path().join("output.arrow");
                        run_arrow_conversion(&input_path, &output_path, scenario.query.clone())
                            .await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parquet", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_query_benchmark(scenario.num_rows),
                    |(temp_dir, input_path)| async move {
                        let output_path = temp_dir.path().join("output.parquet");
                        run_parquet_conversion(&input_path, &output_path, scenario.query.clone())
                            .await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_query_types(c: &mut Criterion) {
    let query_scenarios = vec![
        QueryScenario {
            name: "projection",
            query: Some("SELECT id, value FROM data".to_string()),
            num_rows: 500_000,
        },
        QueryScenario {
            name: "filter_and_project",
            query: Some("SELECT id, value FROM data WHERE status = 'active'".to_string()),
            num_rows: 500_000,
        },
        QueryScenario {
            name: "aggregation",
            query: Some("SELECT category, COUNT(*) as count, SUM(value) as total FROM data GROUP BY category".to_string()),
            num_rows: 500_000,
        },
        QueryScenario {
            name: "complex_filter",
            query: Some("SELECT * FROM data WHERE (category = 'A' OR category = 'B') AND value > 500.0 AND status = 'active'".to_string()),
            num_rows: 500_000,
        },
    ];

    let mut group = c.benchmark_group("query_types");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for scenario in query_scenarios {
        group.throughput(Throughput::Elements(scenario.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("arrow", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_query_benchmark(scenario.num_rows),
                    |(temp_dir, input_path)| async move {
                        let output_path = temp_dir.path().join("output.arrow");
                        run_arrow_conversion(&input_path, &output_path, scenario.query.clone())
                            .await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_query_with_sorting(c: &mut Criterion) {
    let scenarios = vec![
        QueryScenario {
            name: "query_then_sort",
            query: Some("SELECT * FROM data WHERE value > 500.0 ORDER BY value DESC".to_string()),
            num_rows: 500_000,
        },
        QueryScenario {
            name: "project_and_sort",
            query: Some("SELECT id, value FROM data ORDER BY value DESC LIMIT 10000".to_string()),
            num_rows: 500_000,
        },
    ];

    let mut group = c.benchmark_group("query_with_sorting");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for scenario in scenarios {
        group.throughput(Throughput::Elements(scenario.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("arrow", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_query_benchmark(scenario.num_rows),
                    |(temp_dir, input_path)| async move {
                        let output_path = temp_dir.path().join("output.arrow");
                        run_arrow_conversion(&input_path, &output_path, scenario.query.clone())
                            .await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_query_overhead(c: &mut Criterion) {
    let scenarios = vec![
        QueryScenario {
            name: "no_query",
            query: None,
            num_rows: 1_000_000,
        },
        QueryScenario {
            name: "with_noop_query",
            query: Some("SELECT * FROM data".to_string()),
            num_rows: 1_000_000,
        },
    ];

    let mut group = c.benchmark_group("query_overhead");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for scenario in scenarios {
        group.throughput(Throughput::Elements(scenario.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("arrow_to_arrow", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_query_benchmark(scenario.num_rows),
                    |(temp_dir, input_path)| async move {
                        let output_path = temp_dir.path().join("output.arrow");
                        run_arrow_conversion(&input_path, &output_path, scenario.query.clone())
                            .await;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("arrow_to_parquet", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_query_benchmark(scenario.num_rows),
                    |(temp_dir, input_path)| async move {
                        let output_path = temp_dir.path().join("output.parquet");
                        run_parquet_conversion(&input_path, &output_path, scenario.query.clone())
                            .await;
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
    targets = bench_query_vs_no_query, bench_query_types, bench_query_with_sorting, bench_query_overhead
}

criterion_main!(benches);
