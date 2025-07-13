use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use silk_chiffon::utils::arrow_io::ArrowIPCFormat;
use silk_chiffon::{ArrowCompression, ListOutputsFormat, SplitToArrowArgs};
use std::fs::{self, File};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

struct StringKeyScenario {
    name: &'static str,
    num_rows: usize,
    cardinality: usize,
    key_length: usize,
}

fn generate_test_data(scenario: &StringKeyScenario) -> Vec<RecordBatch> {
    let batch_size = 10_000;
    let num_batches = scenario.num_rows.div_ceil(batch_size);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("split_col", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let mut batches = Vec::with_capacity(num_batches);
    let mut id_counter = 0i64;
    let payload_base = "x".repeat(100);

    for batch_idx in 0..num_batches {
        let rows_in_batch = batch_size.min(scenario.num_rows - batch_idx * batch_size);
        let mut ids = Vec::with_capacity(rows_in_batch);
        let mut split_values = Vec::with_capacity(rows_in_batch);
        let mut values = Vec::with_capacity(rows_in_batch);
        let mut payloads = Vec::with_capacity(rows_in_batch);

        for row_idx in 0..rows_in_batch {
            ids.push(id_counter);
            let key_idx = (batch_idx * batch_size + row_idx) % scenario.cardinality;
            let split_value = format!(
                "key_{:0width$}",
                key_idx,
                width = scenario.key_length.saturating_sub(4)
            );
            split_values.push(split_value);
            values.push(rand::random::<f64>() * 1000.0);
            payloads.push(format!("{}{}", payload_base, id_counter % 1000));
            id_counter += 1;
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(split_values)) as ArrayRef,
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

fn setup_benchmark_data(scenario: &StringKeyScenario) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("input.arrow");

    let batches = generate_test_data(scenario);
    write_test_data(&batches, &input_path);

    (temp_dir, input_path)
}

async fn run_silk_chiffon(input_path: &std::path::Path, output_dir: &std::path::Path) {
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
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();
}

fn bench_string_keys(c: &mut Criterion) {
    let scenarios = vec![
        StringKeyScenario {
            name: "short_keys_low_cardinality",
            num_rows: 1_000_000,
            cardinality: 50,
            key_length: 10,
        },
        StringKeyScenario {
            name: "short_keys_high_cardinality",
            num_rows: 1_000_000,
            cardinality: 1000,
            key_length: 10,
        },
        StringKeyScenario {
            name: "long_keys_low_cardinality",
            num_rows: 1_000_000,
            cardinality: 50,
            key_length: 50,
        },
        StringKeyScenario {
            name: "long_keys_high_cardinality",
            num_rows: 1_000_000,
            cardinality: 1000,
            key_length: 50,
        },
    ];

    let mut group = c.benchmark_group("split_string_keys");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));
    group.warm_up_time(Duration::from_secs(5));

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for scenario in scenarios {
        group.throughput(Throughput::Elements(scenario.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("silk_chiffon", scenario.name),
            &scenario,
            |b, scenario| {
                b.to_async(&runtime).iter_batched(
                    || setup_benchmark_data(scenario),
                    |(temp_dir, input_path)| async move {
                        let output_dir = temp_dir.path().join("output");
                        fs::create_dir_all(&output_dir).unwrap();
                        run_silk_chiffon(&input_path, &output_dir).await;
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
    targets = bench_string_keys
}

criterion_main!(benches);
