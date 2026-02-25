//! Diagnostic tests for DataFusion's Arrow IPC batch-splitting buffer inflation.
//!
//! When DataFusion reads an Arrow IPC file, it splits large record batches into
//! sub-batches (default batch_size=8192). The sub-batches share underlying data
//! buffers, but buffer.len() reports the full original size. This causes
//! array_data_size() to count shared data multiple times when summing across
//! batches, inflating estimates by ~ceil(rows/8192)x.
//!
//! The fix: configure SessionContext with a large batch_size (e.g. max_total_rows)
//! to prevent splitting. These tests verify the fix works.

use std::fs::File;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use silk_chiffon::sources::data_source::array_data_size;
use silk_chiffon::utils::test_data::TestFile;

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn make_batch(n: usize) -> RecordBatch {
    let strings: Vec<String> = (0..n).map(|i| format!("{:0>10}", i)).collect();
    let refs: Vec<&str> = strings.iter().map(String::as_str).collect();

    let binary_vecs: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut v = vec![0u8; 16];
            v[..8].copy_from_slice(&(i as u64).to_le_bytes());
            v
        })
        .collect();
    let binary_refs: Vec<&[u8]> = binary_vecs.iter().map(|v| v.as_slice()).collect();

    let ids: Vec<i32> = (0..n as i32).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("str_col", DataType::Utf8, false),
        Field::new("bin_col", DataType::Binary, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(refs)),
            Arc::new(BinaryArray::from(binary_refs)),
        ],
    )
    .unwrap()
}

/// Measure total array_data_size across all batches from a DataFusion stream.
async fn measure_df_stream(path_str: &str, batch_size: usize) -> (Vec<usize>, usize, usize) {
    let config = SessionConfig::new().with_batch_size(batch_size);
    let state = SessionStateBuilder::new().with_config(config).build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_arrow("t", path_str, ArrowReadOptions::default())
        .await
        .unwrap();
    let df = ctx.table("t").await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();

    let num_cols = stream.schema().fields().len();
    let mut col_totals = vec![0usize; num_cols];
    let mut total_rows = 0;
    let mut num_batches = 0;
    while let Some(Ok(batch)) = stream.next().await {
        total_rows += batch.num_rows();
        num_batches += 1;
        for (i, total) in col_totals.iter_mut().enumerate() {
            *total += array_data_size(batch.column(i).as_ref());
        }
    }
    (col_totals, total_rows, num_batches)
}

/// With default batch_size (8192), DataFusion splits a 100K-row IPC batch into
/// ~13 sub-batches, each sharing the same data buffers. array_data_size counts
/// the shared data 13 times, producing ~10x inflation for variable-width cols.
#[tokio::test(flavor = "multi_thread")]
async fn test_default_batch_size_inflates_variable_width_columns() {
    let n = 100_000;
    let batch = make_batch(n);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    TestFile::write_arrow_batch(&path, &batch);
    let path_str = path.to_str().unwrap();

    // direct reading — baseline
    let reader = FileReader::try_new(File::open(path_str).unwrap(), None).unwrap();
    let mut direct_str_total = 0;
    for result in reader {
        let batch = result.unwrap();
        direct_str_total += array_data_size(batch.column(1).as_ref());
    }

    // DataFusion with default batch_size
    let (df_totals, _, num_batches) = measure_df_stream(path_str, 8192).await;
    let df_str_total = df_totals[1];

    assert!(
        num_batches > 1,
        "expected multiple batches from default batch_size, got {num_batches}"
    );
    #[allow(clippy::cast_precision_loss)]
    let ratio = df_str_total as f64 / direct_str_total as f64;
    assert!(
        ratio > 5.0,
        "expected >5x inflation with default batch_size, got {ratio:.1}x \
         (direct={direct_str_total}, df={df_str_total}, batches={num_batches})"
    );
}

/// With batch_size=usize::MAX, DataFusion doesn't split the IPC batch, so
/// array_data_size matches the direct FileReader path exactly.
#[tokio::test(flavor = "multi_thread")]
async fn test_large_batch_size_prevents_inflation() {
    let n = 100_000;
    let batch = make_batch(n);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    TestFile::write_arrow_batch(&path, &batch);
    let path_str = path.to_str().unwrap();

    // direct reading — baseline
    let reader = FileReader::try_new(File::open(path_str).unwrap(), None).unwrap();
    let mut direct_totals = [0usize; 3];
    for result in reader {
        let batch = result.unwrap();
        for (i, total) in direct_totals.iter_mut().enumerate() {
            *total += array_data_size(batch.column(i).as_ref());
        }
    }

    // DataFusion with large batch_size (the fix)
    let (df_totals, _, num_batches) = measure_df_stream(path_str, usize::MAX).await;

    assert_eq!(
        num_batches, 1,
        "expected 1 batch with large batch_size, got {num_batches}"
    );

    let col_names = ["id", "str_col", "bin_col"];
    for (i, col_name) in col_names.iter().enumerate() {
        #[allow(clippy::cast_precision_loss)]
        let ratio = df_totals[i] as f64 / direct_totals[i].max(1) as f64;
        assert!(
            ratio <= 1.1,
            "{}: DataFusion ({}) vs direct ({}) = {:.2}x — still inflated with large batch_size",
            col_name,
            df_totals[i],
            direct_totals[i],
            ratio
        );
    }
}
