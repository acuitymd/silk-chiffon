//! Stream-based parallel parquet writer using tokio-par-util.
//!
//! # Architecture
//!
//! This writer parallelizes parquet encoding at the column level within each row group:
//!
//! ```text
//! RecordBatches → BatchCoalescer → mpsc channel → Writer Task
//!                                                      ↓
//!                                         ┌────────────┴────────────┐
//!                                         │  For each row group:    │
//!                                         │  - Encode columns in    │
//!                                         │    parallel (spawn_     │
//!                                         │    blocking)            │
//!                                         │  - Write to file        │
//!                                         │    sequentially         │
//!                                         └─────────────────────────┘
//! ```
//!
//! # Threading Model
//!
//! - **Main task**: Receives batches via channel, orchestrates encoding
//! - **Column encoding**: Each column encoded in a separate `spawn_blocking` task
//! - **File I/O**: All file writes happen sequentially in `spawn_blocking` to avoid
//!   blocking the async runtime
//!
//! # Error Handling
//!
//! - Encoding errors cancel all in-flight work via `CancellationToken`
//! - Mutex poisoning (from panics) is detected and returned as an error
//! - Double-close is detected and returned as an error
//!
//! # Cleanup Responsibility
//!
//! On error, the caller is responsible for cleaning up any partial output file.
//! The writer will attempt to close the file gracefully, but the file may contain
//! incomplete data.

use std::{
    fs::File,
    io::BufWriter,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, compute::BatchCoalescer, datatypes::SchemaRef};
use futures::{StreamExt, TryStreamExt};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_writer::{
            ArrowColumnChunk, ArrowLeafColumn, ArrowRowGroupWriterFactory, ArrowWriterOptions,
            compute_leaves,
        },
    },
    file::properties::WriterProperties,
};
use tokio::sync::mpsc;
use tokio_par_util::StreamParExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

const WRITER_BUFFER_SIZE: usize = 32 * 1024 * 1024;

/// Calculate recommended concurrency for parallel parquet writing.
///
/// The formula balances CPU utilization with memory overhead:
/// - More columns means each row group uses more parallel tasks for encoding
/// - We want enough concurrency to keep CPUs busy without excessive memory
///
/// Returns `(available_cpus / num_columns + 1).max(4)` which ensures at least
/// 4 row groups can be encoded in parallel while scaling down for wide schemas.
pub fn recommended_concurrency(num_columns: usize) -> usize {
    let cpu_parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    (cpu_parallelism / num_columns + 1).max(4)
}

/// Encode a single row group with columns processed in parallel.
async fn encode_row_group(
    schema: SchemaRef,
    row_group_index: usize,
    row_group_factory: Arc<ArrowRowGroupWriterFactory>,
    batch: RecordBatch,
) -> Result<EncodedRowGroup> {
    let num_rows = batch.num_rows();
    let column_writers = row_group_factory.create_column_writers(row_group_index)?;

    let num_leaf_columns = column_writers.len();
    // most columns have 1 leaf (primitives), only nested types have more
    let mut leaves_per_column: Vec<Vec<ArrowLeafColumn>> = (0..num_leaf_columns)
        .map(|_| Vec::with_capacity(1))
        .collect();

    let mut col_idx = 0;
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        for leaf in compute_leaves(field.as_ref(), column)? {
            leaves_per_column[col_idx].push(leaf);
            col_idx += 1;
        }
    }

    let chunks: Vec<ArrowColumnChunk> =
        futures::stream::iter(column_writers.into_iter().zip(leaves_per_column).map(
            |(mut writer, leaves)| async move {
                tokio::task::spawn_blocking(move || {
                    for leaf in leaves {
                        writer.write(&leaf)?;
                    }
                    writer.close().map_err(|e| anyhow!(e))
                })
                .await
                .map_err(|e| anyhow!("column encoding panicked: {}", e))?
            },
        ))
        .parallel_buffered(num_leaf_columns)
        .try_collect()
        .await?;

    Ok(EncodedRowGroup { chunks, num_rows })
}

struct EncodedRowGroup {
    chunks: Vec<ArrowColumnChunk>,
    num_rows: usize,
}

/// Parallel parquet writer that encodes columns concurrently.
///
/// # Usage
///
/// ```ignore
/// let mut writer = StreamParquetWriter::new(&path, &schema, props, row_group_size, concurrency);
/// for batch in batches {
///     writer.write(batch).await?;
/// }
/// let rows_written = writer.close().await?;
/// ```
///
/// # Batching
///
/// Small input batches are coalesced into larger row groups automatically.
/// The `max_row_group_size` parameter controls the target size.
///
/// # Concurrency
///
/// The `concurrency` parameter controls how many row groups can be encoded
/// in parallel. Each row group's columns are also encoded in parallel.
pub struct StreamParquetWriter {
    batch_sender: Option<mpsc::Sender<RecordBatch>>,
    cancel_token: CancellationToken,
    writer_handle: Option<tokio::task::JoinHandle<Result<u64>>>,
    coalescer: BatchCoalescer,
}

impl StreamParquetWriter {
    pub fn new(
        path: impl AsRef<Path>,
        schema: &SchemaRef,
        props: WriterProperties,
        max_row_group_size: usize,
        concurrency: usize,
    ) -> Self {
        let path = path.as_ref().to_path_buf();
        let schema = Arc::clone(schema);
        let cancel_token = CancellationToken::new();
        let task_token = cancel_token.clone();

        let (batch_tx, batch_rx) = mpsc::channel::<RecordBatch>(concurrency);
        let coalescer = BatchCoalescer::new(Arc::clone(&schema), max_row_group_size);

        let writer_handle = tokio::spawn(async move {
            let file = tokio::task::spawn_blocking(move || -> Result<_> {
                Ok(BufWriter::with_capacity(
                    WRITER_BUFFER_SIZE,
                    File::create(&path)?,
                ))
            })
            .await
            .map_err(|e| anyhow!("file create panicked: {e}"))??;

            let arrow_writer = ArrowWriter::try_new_with_options(
                file,
                Arc::clone(&schema),
                ArrowWriterOptions::new().with_properties(props),
            )?;
            let (file_writer, row_group_factory) = arrow_writer.into_serialized_writer()?;
            let row_group_factory = Arc::new(row_group_factory);
            let file_writer = Arc::new(Mutex::new(Some(file_writer)));

            let cancel_on_err = task_token.clone();

            let result = ReceiverStream::new(batch_rx)
                .enumerate()
                .map(|(idx, batch)| {
                    let schema = Arc::clone(&schema);
                    let row_group_factory = Arc::clone(&row_group_factory);
                    async move { encode_row_group(schema, idx, row_group_factory, batch).await }
                })
                .parallel_buffered_with_token(concurrency, task_token.clone())
                .and_then(|encoded| {
                    let writer = Arc::clone(&file_writer);
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<usize> {
                            let mut guard = writer
                                .lock()
                                .map_err(|_| anyhow!("prior encoding task panicked"))?;
                            let fw = guard.as_mut().ok_or_else(|| anyhow!("writer closed"))?;
                            let mut rg = fw.next_row_group()?;
                            for chunk in encoded.chunks {
                                chunk.append_to_row_group(&mut rg)?;
                            }
                            rg.close()?;
                            Ok(encoded.num_rows)
                        })
                        .await
                        .map_err(|e| anyhow!("write panicked: {e}"))?
                    }
                })
                .inspect_err(|_| cancel_on_err.cancel())
                .try_fold(0u64, |total, rows| async move { Ok(total + rows as u64) })
                .await;

            let close_result = tokio::task::spawn_blocking(move || -> Result<()> {
                let mut guard = file_writer
                    .lock()
                    .map_err(|_| anyhow!("prior encoding task panicked"))?;
                if let Some(fw) = guard.take() {
                    fw.close()?;
                }
                Ok(())
            })
            .await
            .map_err(|e| anyhow!("close panicked: {e}"))?;

            result.and_then(|total| close_result.map(|()| total))
        });

        Self {
            batch_sender: Some(batch_tx),
            cancel_token,
            writer_handle: Some(writer_handle),
            coalescer,
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let sender = self
            .batch_sender
            .as_ref()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        self.coalescer.push_batch(batch)?;

        while let Some(completed) = self.coalescer.next_completed_batch() {
            sender
                .send(completed)
                .await
                .map_err(|_| anyhow!("writer task failed - call close() for details"))?;
        }

        Ok(())
    }

    pub async fn close(&mut self) -> Result<u64> {
        self.coalescer.finish_buffered_batch()?;

        if let Some(sender) = self.batch_sender.take() {
            while let Some(completed) = self.coalescer.next_completed_batch() {
                sender
                    .send(completed)
                    .await
                    .map_err(|_| anyhow!("writer task failed - call close() for details"))?;
            }
        }

        match self.writer_handle.take() {
            Some(handle) => handle
                .await
                .map_err(|e| anyhow!("writer task panicked: {e}"))?,
            None => Err(anyhow!("writer already closed")),
        }
    }

    /// Cancel the writer, stopping any pending work.
    ///
    /// In-flight row groups that have already started encoding will complete,
    /// and the file will be closed with whatever row groups were fully written.
    /// Returns the number of rows actually written to the file.
    ///
    /// Note: On error, the caller is responsible for cleaning up any partial
    /// output file.
    pub async fn cancel(mut self) -> Result<u64> {
        self.cancel_token.cancel();
        self.batch_sender.take();

        match self.writer_handle.take() {
            Some(handle) => handle
                .await
                .map_err(|e| anyhow!("writer task panicked: {e}"))?,
            None => Err(anyhow!("writer already closed")),
        }
    }
}

impl Drop for StreamParquetWriter {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.batch_sender.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
        LargeStringArray, StringArray, StructArray, TimestampMicrosecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_batch(schema: &SchemaRef, ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_basic() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);

        for i in 0..3 {
            let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
            let names: Vec<String> = ids.iter().map(|x| format!("name_{}", x)).collect();
            let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let batch = create_batch(&schema, &ids, &names_ref);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 300);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 300);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_coalesces_batches() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);

        for i in 0..10 {
            let ids: Vec<i32> = (i * 10..(i + 1) * 10).collect();
            let names: Vec<String> = ids.iter().map(|x| format!("name_{}", x)).collect();
            let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let batch = create_batch(&schema, &ids, &names_ref);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 100);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_preserves_order() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = StreamParquetWriter::new(&path, &schema, props, 1, 4);

        for i in 0..5 {
            let ids: Vec<i32> = vec![i];
            let names: Vec<&str> = vec!["test"];
            let batch = create_batch(&schema, &ids, &names);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 5);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();

        let mut all_ids = Vec::new();
        for batch in &batches {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..id_col.len() {
                all_ids.push(id_col.value(i));
            }
        }
        assert_eq!(all_ids, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_empty() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 0);

        // verify the file is valid parquet
        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_double_close_returns_error() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);

        // first close succeeds
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 0);

        // second close returns error
        let result = writer.close().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already closed"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_all_numeric_types() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("i8", DataType::Int8, false),
            Field::new("i16", DataType::Int16, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("u8", DataType::UInt8, false),
            Field::new("u16", DataType::UInt16, false),
            Field::new("u32", DataType::UInt32, false),
            Field::new("u64", DataType::UInt64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int8Array::from(vec![1i8, 2, 3])),
                Arc::new(Int16Array::from(vec![1i16, 2, 3])),
                Arc::new(Int32Array::from(vec![1i32, 2, 3])),
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(UInt8Array::from(vec![1u8, 2, 3])),
                Arc::new(UInt16Array::from(vec![1u16, 2, 3])),
                Arc::new(UInt32Array::from(vec![1u32, 2, 3])),
                Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
                Arc::new(Float32Array::from(vec![1.0f32, 2.0, 3.0])),
                Arc::new(Float64Array::from(vec![1.0f64, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder().build();
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);
        writer.write(batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        // verify round-trip
        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 10);

        // verify actual values
        let i8_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(i8_col.values(), &[1i8, 2, 3]);

        let i64_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(i64_col.values(), &[1i64, 2, 3]);

        let f64_col = batch
            .column(9)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(f64_col.values(), &[1.0f64, 2.0, 3.0]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_temporal_types() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("date32", DataType::Date32, false),
            Field::new("date64", DataType::Date64, false),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Date32Array::from(vec![18000, 18001, 18002])),
                Arc::new(Date64Array::from(vec![
                    1_600_000_000_000i64,
                    1_600_000_100_000,
                    1_600_000_200_000,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_600_000_000_000_000i64,
                    1_600_000_000_001_000,
                    1_600_000_000_002_000,
                ])),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder().build();
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);
        writer.write(batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // verify actual values
        let date32_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(date32_col.values(), &[18000, 18001, 18002]);

        let ts_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            ts_col.values(),
            &[
                1_600_000_000_000_000i64,
                1_600_000_000_001_000,
                1_600_000_000_002_000
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_binary_and_boolean() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
            Field::new("large_binary", DataType::LargeBinary, false),
            Field::new("large_string", DataType::LargeUtf8, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, false, true])),
                Arc::new(BinaryArray::from_vec(vec![b"a", b"bb", b"ccc"])),
                Arc::new(LargeBinaryArray::from_vec(vec![b"x", b"yy", b"zzz"])),
                Arc::new(LargeStringArray::from(vec!["one", "two", "three"])),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder().build();
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);
        writer.write(batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // verify actual values
        let bool_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_col.value(0));
        assert!(!bool_col.value(1));
        assert!(bool_col.value(2));

        let binary_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(binary_col.value(0), b"a");
        assert_eq!(binary_col.value(1), b"bb");
        assert_eq!(binary_col.value(2), b"ccc");

        // parquet preserves LargeUtf8 type
        let large_string_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(large_string_col.value(0), "one");
        assert_eq!(large_string_col.value(1), "two");
        assert_eq!(large_string_col.value(2), "three");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_nullable_columns() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder().build();
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);
        writer.write(batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let batch = &batches[0];

        // verify nulls and values for int column
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(!id_col.is_null(0));
        assert!(id_col.is_null(1));
        assert!(!id_col.is_null(2));
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(2), 3);

        // verify nulls and values for string column
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!name_col.is_null(0));
        assert!(!name_col.is_null(1));
        assert!(name_col.is_null(2));
        assert_eq!(name_col.value(0), "a");
        assert_eq!(name_col.value(1), "b");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_nested_struct() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let inner_field = Field::new("value", DataType::Int32, false);
        let struct_field = Field::new(
            "nested",
            DataType::Struct(vec![inner_field.clone()].into()),
            false,
        );
        let schema = Arc::new(Schema::new(vec![struct_field]));

        let inner_array = Int32Array::from(vec![10, 20, 30]);
        let struct_array =
            StructArray::from(vec![(Arc::new(inner_field), Arc::new(inner_array) as _)]);

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(struct_array)]).unwrap();

        let props = WriterProperties::builder().build();
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);
        writer.write(batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // verify nested struct values
        let struct_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let inner_col = struct_col
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(inner_col.values(), &[10, 20, 30]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_large_batch() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = StreamParquetWriter::new(&path, &schema, props, 50_000, 4);

        // write 100k rows in chunks
        for i in 0..10 {
            let ids: Vec<i32> = (i * 10_000..(i + 1) * 10_000).collect();
            let names: Vec<String> = ids.iter().map(|x| format!("name_{}", x)).collect();
            let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let batch = create_batch(&schema, &ids, &names_ref);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 100_000);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 2); // 100k rows / 50k per group
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_many_row_groups() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        // small row group size to force many groups
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 10, 4);

        for i in 0..100 {
            let ids: Vec<i32> = vec![i];
            let names: Vec<&str> = vec!["x"];
            let batch = create_batch(&schema, &ids, &names);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 100);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 10); // 100 rows / 10 per group
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_decimal_type() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(
                Decimal128Array::from(vec![12345i128, 67890, 11111])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            )],
        )
        .unwrap();

        let props = WriterProperties::builder().build();
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);
        writer.write(batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // verify decimal values
        let decimal_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimal_col.value(0), 12345i128);
        assert_eq!(decimal_col.value(1), 67890i128);
        assert_eq!(decimal_col.value(2), 11111i128);
        assert_eq!(decimal_col.precision(), 10);
        assert_eq!(decimal_col.scale(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_writer_list_type() {
        use arrow::array::{Int32Builder, ListArray, ListBuilder};

        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let list_field = Field::new_list("values", Field::new("item", DataType::Int32, true), true);
        let schema = Arc::new(Schema::new(vec![list_field]));

        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true); // [1, 2]
        builder.values().append_value(3);
        builder.append(true); // [3]
        builder.append(true); // [] empty list
        let list_array = builder.finish();

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list_array)]).unwrap();

        let props = WriterProperties::builder().build();
        let mut writer = StreamParquetWriter::new(&path, &schema, props, 100, 4);
        writer.write(batch).await.unwrap();
        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        // verify round-trip
        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // verify list values
        let list_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // first list: [1, 2]
        let first_list = list_col.value(0);
        let first_values = first_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first_values.len(), 2);
        assert_eq!(first_values.value(0), 1);
        assert_eq!(first_values.value(1), 2);

        // second list: [3]
        let second_list = list_col.value(1);
        let second_values = second_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(second_values.len(), 1);
        assert_eq!(second_values.value(0), 3);

        // third list: [] (empty)
        let third_list = list_col.value(2);
        assert_eq!(third_list.len(), 0);
    }
}
