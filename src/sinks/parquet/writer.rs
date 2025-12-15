//! Parallel parquet writer implementation.
//!
//! This module provides a parallel parquet writer that maximizes throughput by
//! encoding multiple row groups and columns concurrently while maintaining correct
//! row ordering in the output file.
//!
//! # Architecture
//!
//! The writer uses a pipeline of three threads plus two thread pools:
//!
//! ```text
//!    ┌──────────┐                    ┌─────────────────────────────────────────┐
//!    │  Caller  │   input_channel    │           Processor Thread              │
//!    │ write()  │ ──────────────────►│  Coalesces batches into row groups,     │
//!    └──────────┘   RecordBatch      │  dispatches to rg_pool via spawn()      │
//!                                    └──────────────────┬──────────────────────┘
//!                                                       │ rg_pool.spawn()
//!                                                       ▼
//!                                    ┌─────────────────────────────────────────┐
//!                                    │               rg_pool                   │
//!                                    │  Coordinates row group encoding tasks   │
//!                                    └──────────────────┬──────────────────────┘
//!                                                       │ column_pool.install()
//!                                                       ▼
//!                                    ┌─────────────────────────────────────────┐
//!                                    │             column_pool                 │
//!                                    │  Encodes columns in parallel            │
//!                                    └──────────────────┬──────────────────────┘
//!                                                       │ output_channel
//!                                                       ▼ EncodingResult
//!                                    ┌─────────────────────────────────────────┐
//!                                    │            Writer Thread                │
//!                                    │  Receives encoded row groups, buffers   │
//!                                    │  out-of-order arrivals, writes to file  │
//!                                    │  in sequential order                    │
//!                                    └─────────────────────────────────────────┘
//! ```
//!
//! # Thread responsibilities
//!
//! - **Caller thread**: calls `write()` to send batches, blocks if pipeline is full
//! - **Processor thread**: coalesces small batches into row-group-sized chunks,
//!   dispatches encoding work to the rg_pool
//! - **rg_pool**: coordinates row group encoding tasks (small pool, mainly for dispatch)
//! - **column_pool**: does the actual CPU-intensive column encoding work (sized to CPU count)
//! - **Writer thread**: receives encoded chunks, reorders if needed, writes to disk
//!
//! # Why two thread pools?
//!
//! Separating dispatch (rg_pool) from encoding (column_pool) ensures that:
//! 1. Column encoding always has dedicated CPU resources regardless of row group count
//! 2. Multiple row groups can have their columns encoded simultaneously
//! 3. The dispatch pool stays responsive even under heavy encoding load
//!
//! The rg_pool is sized based on `parallelism / num_columns + 1` to allow multiple
//! row groups in flight while the column_pool is sized to the full parallelism setting.
//!
//! # Data flow
//!
//! 1. Caller sends `RecordBatch` via `write()`
//! 2. Processor coalesces batches until row group size is reached
//! 3. Full row groups are dispatched to rg_pool
//! 4. rg_pool spawns encoding task that uses column_pool for parallel column encoding
//! 5. Encoded chunks sent to writer thread via output channel
//! 6. Writer buffers out-of-order results, writes in sequence
//!
//! # Backpressure
//!
//! A counting semaphore limits in-flight row groups to `max_in_flight` (see
//! [`IN_FLIGHT_MULTIPLIER`]). When the limit is reached, the processor thread blocks
//! on `semaphore.acquire()`, which causes the input channel to fill, which causes
//! `write()` to block. This prevents unbounded memory growth when encoding is slower
//! than input.
//!
//! # Row ordering
//!
//! Row groups may complete encoding out of order (e.g., row group 3 finishes before
//! row group 2). The writer thread maintains a `pending` buffer and only writes
//! row groups in sequential order. This ensures the output file has rows in the
//! same order they were written, which is required for correctness.
//!
//! # Error handling
//!
//! Encoding errors are captured as `EncodingResult::Failure` and sent to the writer
//! thread, which returns the error. Thread panics are caught on `close()` and
//! converted to errors with the panic payload included for debugging.

use std::{
    cmp::{max, min},
    collections::HashMap,
    fs::File,
    io::BufWriter,
    path::Path,
    result,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread,
};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, compute::BatchCoalescer, datatypes::SchemaRef};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_writer::{ArrowColumnChunk, compute_leaves},
    },
    errors::ParquetError,
    file::properties::WriterProperties,
};
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

/// Multiplier for max in-flight row groups relative to rg_pool size.
///
/// Higher values improve throughput by keeping the pipeline fed but increase memory usage.
/// 3x provides a good balance: one row group actively encoding per worker, one queued,
/// and one buffer slot for out-of-order completion handling.
const IN_FLIGHT_MULTIPLIER: usize = 3;

/// Buffer size for the file writer (32 MiB).
///
/// Larger buffers reduce syscall overhead for sequential writes.
const WRITER_BUFFER_SIZE: usize = 32 * 1024 * 1024;

/// Simple counting semaphore for limiting in-flight tasks.
struct Semaphore {
    state: Mutex<usize>,
    cond: Condvar,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Self {
            state: Mutex::new(permits),
            cond: Condvar::new(),
        }
    }

    fn acquire(&self) -> Result<()> {
        let mut count = self
            .state
            .lock()
            .map_err(|_| anyhow!("semaphore mutex poisoned"))?;
        while *count == 0 {
            count = self
                .cond
                .wait(count)
                .map_err(|_| anyhow!("semaphore condvar wait failed"))?;
        }
        *count -= 1;
        Ok(())
    }

    fn release(&self) {
        if let Ok(mut count) = self.state.lock() {
            *count += 1;
            self.cond.notify_one();
        }
    }
}

/// Result from encoding a row group - either success with chunks or failure with error.
enum EncodingResult {
    Success {
        index: usize,
        chunks: Vec<ArrowColumnChunk>,
        num_rows: usize,
    },
    Failure {
        index: usize,
        error: String,
    },
}

/// Messages sent from write() to processor thread.
enum InputMessage {
    Batch(RecordBatch),
    Finish,
}

/// Count the number of leaf columns in an Arrow schema (handles nested types).
fn count_leaf_columns(schema: &SchemaRef) -> usize {
    // create a dummy writer to get the parquet schema which has correct leaf count
    let buffer = Vec::new();
    if let Ok(writer) = ArrowWriter::try_new(buffer, Arc::clone(schema), None)
        && let Ok((_, factory)) = writer.into_serialized_writer()
        && let Ok(writers) = factory.create_column_writers(0)
    {
        return writers.len();
    }
    // fallback for simple schemas
    schema.fields().len()
}

/// Encode a single row group with columns processed in parallel.
fn encode_row_group_parallel(
    schema: &SchemaRef,
    props: &WriterProperties,
    row_group_index: usize,
    batch: &RecordBatch,
) -> Result<Vec<ArrowColumnChunk>> {
    let buffer = Vec::new();
    let arrow_writer = ArrowWriter::try_new(buffer, Arc::clone(schema), Some(props.clone()))?;
    let (_, row_group_factory) = arrow_writer.into_serialized_writer()?;
    let column_writers = row_group_factory.create_column_writers(row_group_index)?;

    // use the actual number of column writers, not schema.fields().len()
    let num_leaf_columns = column_writers.len();
    let mut leaves_per_column: Vec<Vec<_>> = (0..num_leaf_columns).map(|_| Vec::new()).collect();

    let mut col_idx = 0;
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        for leaf in compute_leaves(field.as_ref(), column)? {
            leaves_per_column[col_idx].push(leaf);
            col_idx += 1;
        }
    }

    let chunks: Vec<result::Result<ArrowColumnChunk, ParquetError>> = column_writers
        .into_par_iter()
        .zip(leaves_per_column.into_par_iter())
        .map(|(mut writer, leaves)| {
            for leaf in leaves {
                writer.write(&leaf)?;
            }
            writer.close()
        })
        .collect();

    chunks
        .into_iter()
        .map(|r| r.map_err(|e| anyhow!(e)))
        .collect()
}

/// Parallel row groups and columns writer.
///
/// Uses two dedicated thread pools:
/// - rg_pool: dispatches row group encoding tasks (sized dynamically based on schema)
/// - column_pool: runs parallel column encoding (sized to available parallelism via
///   `thread::available_parallelism()`)
///
/// This separates coordination from CPU-bound work, ensuring column encoding gets
/// dedicated resources regardless of how many row groups are being processed.
pub struct ParquetWriter {
    input_sender: Option<mpsc::SyncSender<InputMessage>>,
    processor_handle: Option<thread::JoinHandle<Result<()>>>,
    writer_handle: Option<thread::JoinHandle<Result<u64>>>,
    closed: bool,
}

impl ParquetWriter {
    /// Create a new streaming parallel writer.
    ///
    /// `max_row_group_size` - target number of rows per row group
    /// `max_parallelism` - cap on thread count (None = use all available CPUs)
    pub fn try_new(
        path: impl AsRef<Path>,
        schema: &SchemaRef,
        props: WriterProperties,
        max_row_group_size: usize,
        max_parallelism: Option<usize>,
    ) -> Result<Self> {
        let available_cpus = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        let parallelism = max_parallelism
            .map(|max| min(max, available_cpus))
            .unwrap_or(available_cpus);

        // use leaf column count for proper sizing with nested types
        let num_columns = count_leaf_columns(schema);
        let rg_pool_size = if num_columns == 0 {
            2
        } else {
            max(1, parallelism / num_columns) + 1
        };
        let max_in_flight = rg_pool_size * IN_FLIGHT_MULTIPLIER;

        let (input_sender, input_receiver) = mpsc::sync_channel::<InputMessage>(max_in_flight);

        let (output_sender, output_receiver) = mpsc::sync_channel::<EncodingResult>(max_in_flight);

        let semaphore = Arc::new(Semaphore::new(max_in_flight));

        let writer_schema = Arc::clone(schema);
        let writer_props = props.clone();
        let path = path.as_ref().to_path_buf();
        let writer_semaphore = Arc::clone(&semaphore);

        let writer_handle = thread::spawn(move || {
            let file = BufWriter::with_capacity(WRITER_BUFFER_SIZE, File::create(&path)?);
            let arrow_writer = ArrowWriter::try_new(file, writer_schema, Some(writer_props))?;
            let (mut file_writer, _) = arrow_writer.into_serialized_writer()?;

            let mut pending: HashMap<usize, (Vec<ArrowColumnChunk>, usize)> = HashMap::new();
            let mut next_to_write: usize = 0;
            let mut total_rows: u64 = 0;

            for result in output_receiver {
                match result {
                    EncodingResult::Failure { index, error } => {
                        return Err(anyhow!(
                            "encoding failed for row group {}: {}",
                            index,
                            error
                        ));
                    }
                    EncodingResult::Success {
                        index,
                        chunks,
                        num_rows,
                    } => {
                        if index == next_to_write {
                            let mut row_group_writer = file_writer.next_row_group()?;
                            for chunk in chunks {
                                chunk.append_to_row_group(&mut row_group_writer)?;
                            }
                            row_group_writer.close()?;
                            total_rows += num_rows as u64;
                            next_to_write += 1;
                            writer_semaphore.release();

                            while let Some((buffered_chunks, buffered_rows)) =
                                pending.remove(&next_to_write)
                            {
                                let mut row_group_writer = file_writer.next_row_group()?;
                                for chunk in buffered_chunks {
                                    chunk.append_to_row_group(&mut row_group_writer)?;
                                }
                                row_group_writer.close()?;
                                total_rows += buffered_rows as u64;
                                next_to_write += 1;
                                writer_semaphore.release();
                            }
                        } else {
                            pending.insert(index, (chunks, num_rows));
                        }
                    }
                }
            }

            file_writer.close()?;
            Ok(total_rows)
        });

        let processor_schema = Arc::clone(schema);
        let processor_props = props;
        let processor_semaphore = Arc::clone(&semaphore);

        let processor_handle = thread::spawn(move || {
            let rg_pool = ThreadPoolBuilder::new()
                .num_threads(rg_pool_size)
                .thread_name(|i| format!("rg-pool-{}", i))
                .build()
                .map_err(|e| anyhow!("failed to create rg_pool: {}", e))?;

            let column_pool = Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(parallelism)
                    .thread_name(|i| format!("col-pool-{}", i))
                    .build()
                    .map_err(|e| anyhow!("failed to create column_pool: {}", e))?,
            );

            let mut coalescer =
                BatchCoalescer::new(Arc::clone(&processor_schema), max_row_group_size);
            let mut next_index: usize = 0;

            // helper to dispatch a batch for encoding
            let dispatch_batch = |rg_pool: &ThreadPool,
                                  batch: RecordBatch,
                                  index: usize,
                                  schema: SchemaRef,
                                  props: WriterProperties,
                                  sender: mpsc::SyncSender<EncodingResult>,
                                  semaphore: &Arc<Semaphore>,
                                  col_pool: Arc<ThreadPool>|
             -> Result<()> {
                let num_rows = batch.num_rows();

                semaphore.acquire()?;
                let task_semaphore = Arc::clone(semaphore);

                rg_pool.spawn(move || {
                    let result = col_pool
                        .install(|| encode_row_group_parallel(&schema, &props, index, &batch));

                    let msg = match result {
                        Ok(chunks) => EncodingResult::Success {
                            index,
                            chunks,
                            num_rows,
                        },
                        Err(e) => EncodingResult::Failure {
                            index,
                            error: e.to_string(),
                        },
                    };

                    // normally writer thread releases the permit after writing;
                    // if send fails (writer dead), release here to prevent deadlock
                    if sender.send(msg).is_err() {
                        task_semaphore.release();
                    }
                });

                Ok(())
            };

            for msg in input_receiver {
                let done = match msg {
                    InputMessage::Batch(batch) => {
                        coalescer.push_batch(batch)?;
                        false
                    }
                    InputMessage::Finish => {
                        coalescer.finish_buffered_batch()?;
                        true
                    }
                };

                while let Some(batch) = coalescer.next_completed_batch() {
                    let index = next_index;
                    next_index += 1;

                    dispatch_batch(
                        &rg_pool,
                        batch,
                        index,
                        Arc::clone(&processor_schema),
                        processor_props.clone(),
                        output_sender.clone(),
                        &processor_semaphore,
                        Arc::clone(&column_pool),
                    )?;
                }

                if done {
                    break;
                }
            }
            Ok(())
        });

        Ok(Self {
            input_sender: Some(input_sender),
            processor_handle: Some(processor_handle),
            writer_handle: Some(writer_handle),
            closed: false,
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.closed {
            return Err(anyhow!("cannot write to closed writer"));
        }
        if let Some(sender) = &self.input_sender {
            sender
                .send(InputMessage::Batch(batch.clone()))
                .map_err(|_| anyhow!("processor thread died"))?;
        }
        Ok(())
    }

    pub fn close(mut self) -> Result<u64> {
        self.closed = true;

        if let Some(sender) = self.input_sender.take() {
            sender
                .send(InputMessage::Finish)
                .map_err(|_| anyhow!("processor thread died before finish signal"))?;
        }

        if let Some(handle) = self.processor_handle.take() {
            handle
                .join()
                .map_err(|e| anyhow!("processor thread panicked: {:?}", e))??;
        }

        self.writer_handle
            .take()
            .expect("writer handle missing")
            .join()
            .map_err(|e| anyhow!("writer thread panicked: {:?}", e))?
    }
}

#[cfg(test)]
#[allow(
    clippy::similar_names,
    clippy::cast_lossless,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, Float64Array, Int32Array, Int32Builder, Int64Array, Int64Builder, ListBuilder,
        StringArray, StringBuilder, StructArray, StructBuilder, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use tempfile::tempdir;

    #[test]
    fn test_parallel_row_groups_and_columns_writer() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test_parallel.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder().build();

        let mut writer = ParquetWriter::try_new(path.clone(), &schema, props, 200, None).unwrap();

        for i in 0..10 {
            let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
            let names: Vec<String> = (i * 100..(i + 1) * 100)
                .map(|x| format!("name_{}", x))
                .collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }

        let rows = writer.close().unwrap();

        assert_eq!(rows, 1000);
        assert!(path.exists());
    }

    fn read_parquet_to_batches(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        reader.map(|r| r.unwrap()).collect()
    }

    fn concat_batches(batches: &[RecordBatch]) -> RecordBatch {
        if batches.is_empty() {
            panic!("no batches to concat");
        }
        arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
    }

    fn batches_equal(a: &RecordBatch, b: &RecordBatch) -> bool {
        if a.schema() != b.schema() {
            return false;
        }
        if a.num_rows() != b.num_rows() {
            return false;
        }
        for (col_a, col_b) in a.columns().iter().zip(b.columns().iter()) {
            if col_a != col_b {
                return false;
            }
        }
        true
    }

    fn create_test_schema(num_columns: usize) -> Arc<Schema> {
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
    fn create_test_batch(schema: &Arc<Schema>, num_rows: usize, offset: usize) -> RecordBatch {
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            let col: Arc<dyn Array> = match field.data_type() {
                DataType::Int32 => {
                    let values: Vec<i32> = (0..num_rows)
                        .map(|r| ((offset + r) * (i + 1)) as i32)
                        .collect();
                    Arc::new(Int32Array::from(values))
                }
                DataType::Int64 => {
                    let values: Vec<i64> = (0..num_rows)
                        .map(|r| ((offset + r) * (i + 1)) as i64)
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<f64> = (0..num_rows)
                        .map(|r| ((offset + r) * (i + 1)) as f64)
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Utf8 => {
                    let values: Vec<String> = (0..num_rows)
                        .map(|r| format!("val_{}_{}", i, offset + r))
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let values: Vec<i64> = (0..num_rows)
                        .map(|r| 1_700_000_000_000_000i64 + ((offset + r) as i64) * 1_000_000)
                        .collect();
                    Arc::new(TimestampMicrosecondArray::from(values))
                }
                _ => panic!("unexpected data type"),
            };
            columns.push(col);
        }
        RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
    }

    #[test]
    fn test_output_matches_arrow_writer_simple() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batches: Vec<RecordBatch> = (0..5)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(
                            (i * 100..(i + 1) * 100).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            (i * 100..(i + 1) * 100)
                                .map(|x| i64::from(x) * 2)
                                .collect::<Vec<_>>(),
                        )),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_batches = read_parquet_to_batches(&sequential_path);
        let parallel_batches = read_parquet_to_batches(&parallel_path);

        let sequential_data = concat_batches(&sequential_batches);
        let parallel_data = concat_batches(&parallel_batches);

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallel writer output differs from sequential ArrowWriter"
        );
    }

    #[test]
    fn test_output_matches_arrow_writer_multiple_types() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(5);
        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| create_test_batch(&schema, 100, i * 100))
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(300)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 300, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_batches = read_parquet_to_batches(&sequential_path);
        let parallel_batches = read_parquet_to_batches(&parallel_path);

        let sequential_data = concat_batches(&sequential_batches);
        let parallel_data = concat_batches(&parallel_batches);

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallel writer output differs from sequential ArrowWriter with multiple data types"
        );
    }

    #[test]
    fn test_output_matches_arrow_writer_many_columns() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(17);
        let batches: Vec<RecordBatch> = (0..8)
            .map(|i| create_test_batch(&schema, 500, i * 500))
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(1000)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 1000, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_batches = read_parquet_to_batches(&sequential_path);
        let parallel_batches = read_parquet_to_batches(&parallel_path);

        let sequential_data = concat_batches(&sequential_batches);
        let parallel_data = concat_batches(&parallel_batches);

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallel writer output differs from sequential ArrowWriter with 17 columns"
        );
    }

    #[test]
    fn test_output_matches_arrow_writer_single_row_group() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(5);
        let batches: Vec<RecordBatch> = (0..3)
            .map(|i| create_test_batch(&schema, 100, i * 100))
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(1_000_000)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 1_000_000, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_batches = read_parquet_to_batches(&sequential_path);
        let parallel_batches = read_parquet_to_batches(&parallel_path);

        let sequential_data = concat_batches(&sequential_batches);
        let parallel_data = concat_batches(&parallel_batches);

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallel writer output differs from sequential ArrowWriter with single row group"
        );
    }

    #[test]
    fn test_output_matches_arrow_writer_many_small_batches() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(5);
        let batches: Vec<RecordBatch> = (0..100)
            .map(|i| create_test_batch(&schema, 10, i * 10))
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_batches = read_parquet_to_batches(&sequential_path);
        let parallel_batches = read_parquet_to_batches(&parallel_path);

        let sequential_data = concat_batches(&sequential_batches);
        let parallel_data = concat_batches(&parallel_batches);

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallel writer output differs from sequential ArrowWriter with many small batches"
        );
    }

    #[test]
    fn test_output_matches_arrow_writer_nested_struct() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        // schema with a struct containing two fields (3 leaf columns total)
        let inner_fields: Fields = vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int64, false),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("point", DataType::Struct(inner_fields.clone()), false),
        ]));

        let batches: Vec<RecordBatch> = (0..5)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                let x_values: Vec<i32> = (i * 100..(i + 1) * 100).map(|v| v * 2).collect();
                let y_values: Vec<i64> =
                    (i * 100..(i + 1) * 100).map(|v| i64::from(v) * 3).collect();
                let struct_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("x", DataType::Int32, false)),
                        Arc::new(Int32Array::from(x_values)) as Arc<dyn Array>,
                    ),
                    (
                        Arc::new(Field::new("y", DataType::Int64, false)),
                        Arc::new(Int64Array::from(y_values)) as Arc<dyn Array>,
                    ),
                ]);
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(struct_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_batches = read_parquet_to_batches(&sequential_path);
        let parallel_batches = read_parquet_to_batches(&parallel_path);

        let sequential_data = concat_batches(&sequential_batches);
        let parallel_data = concat_batches(&parallel_batches);

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallel writer output differs from sequential ArrowWriter with nested struct"
        );
    }

    #[test]
    fn test_stress_many_row_groups() {
        // stress test: 100 row groups with 1000 rows each = 100k rows
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("stress.parquet");

        let schema = create_test_schema(10);
        let props = WriterProperties::builder().build();

        let mut writer = ParquetWriter::try_new(&path, &schema, props, 1000, None).unwrap();

        for i in 0..100 {
            let batch = create_test_batch(&schema, 1000, i * 1000);
            writer.write(&batch).unwrap();
        }

        let rows = writer.close().unwrap();
        assert_eq!(rows, 100_000);

        // verify data integrity
        let batches = read_parquet_to_batches(&path);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100_000);
    }

    #[test]
    fn test_stress_many_small_row_groups() {
        // stress test: 500 row groups with 100 rows each
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("many_small.parquet");

        let schema = create_test_schema(5);
        let props = WriterProperties::builder().build();

        let mut writer = ParquetWriter::try_new(&path, &schema, props, 100, None).unwrap();

        for i in 0..500 {
            let batch = create_test_batch(&schema, 100, i * 100);
            writer.write(&batch).unwrap();
        }

        let rows = writer.close().unwrap();
        assert_eq!(rows, 50_000);
    }

    #[test]
    fn test_parallelism_limited_to_one() {
        // edge case: single thread - should still work correctly
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(5);
        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| create_test_batch(&schema, 100, i * 100))
            .collect();

        // sequential writer
        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        // parallel writer with parallelism=1
        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, Some(1)).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallelism=1 should produce identical output to sequential writer"
        );
    }

    #[test]
    fn test_parallelism_limited_to_two() {
        // test with parallelism=2
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(8);
        let batches: Vec<RecordBatch> = (0..20)
            .map(|i| create_test_batch(&schema, 500, i * 500))
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(1000)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 1000, Some(2)).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "parallelism=2 should produce identical output"
        );
    }

    #[test]
    fn test_high_parallelism_few_columns() {
        // many threads, few columns - tests thread pool sizing edge case
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batches: Vec<RecordBatch> = (0..50)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(
                            (i * 100..(i + 1) * 100).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            (i * 100..(i + 1) * 100)
                                .map(|x| i64::from(x) * 2)
                                .collect::<Vec<_>>(),
                        )),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        // request 32 threads with only 2 columns
        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, Some(32)).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "high parallelism with few columns should produce identical output"
        );
    }

    #[test]
    fn test_write_after_close_fails() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let props = WriterProperties::builder().build();

        let mut writer = ParquetWriter::try_new(&path, &schema, props, 100, None).unwrap();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        writer.write(&batch).unwrap();

        // close the writer - this consumes it, so we can't write after
        // the API enforces this at compile time since close takes self by value
        let rows = writer.close().unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_row_ordering_preserved() {
        // verify data order is preserved even with parallel encoding
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("ordered.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "sequence",
            DataType::Int64,
            false,
        )]));
        let props = WriterProperties::builder().build();

        let mut writer = ParquetWriter::try_new(&path, &schema, props, 1000, None).unwrap();

        // write sequence 0..10000
        for i in 0..10 {
            let values: Vec<i64> = (i * 1000..(i + 1) * 1000).map(i64::from).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }

        writer.close().unwrap();

        // read back and verify sequence
        let batches = read_parquet_to_batches(&path);
        let mut expected: i64 = 0;
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                assert_eq!(
                    col.value(i),
                    expected,
                    "row ordering not preserved at position {}",
                    expected
                );
                expected += 1;
            }
        }
        assert_eq!(expected, 10000);
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_nullable_columns() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nullable_int", DataType::Int64, true),
            Field::new("nullable_string", DataType::Utf8, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                // every 3rd value is null
                let nullable_ints: Vec<Option<i64>> = (i * 100..(i + 1) * 100)
                    .map(|x| if x % 3 == 0 { None } else { Some(i64::from(x)) })
                    .collect();
                // every 5th value is null
                let nullable_strings: Vec<Option<String>> = (i * 100..(i + 1) * 100)
                    .map(|x| {
                        if x % 5 == 0 {
                            None
                        } else {
                            Some(format!("str_{}", x))
                        }
                    })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(Int64Array::from(nullable_ints)),
                        Arc::new(StringArray::from(nullable_strings)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "nullable columns should be handled correctly"
        );
    }

    #[test]
    fn test_with_compression() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(5);
        let batches: Vec<RecordBatch> = (0..20)
            .map(|i| create_test_batch(&schema, 500, i * 500))
            .collect();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_size(1000)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 1000, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "compression should produce identical data"
        );
    }

    #[test]
    fn test_very_wide_schema() {
        // test with 50 columns
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(50);
        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| create_test_batch(&schema, 200, i * 200))
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(500)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 500, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "50 column schema should produce identical data"
        );
    }

    #[test]
    fn test_single_row_batches() {
        // edge case: batches with single rows
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batches: Vec<RecordBatch> = (0..100)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(vec![i])),
                        Arc::new(StringArray::from(vec![format!("value_{}", i)])),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(10)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 10, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "single row batches should produce identical data"
        );
    }

    #[test]
    fn test_large_single_batch() {
        // single large batch that spans multiple row groups
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        let schema = create_test_schema(5);
        // single batch with 10000 rows
        let batch = create_test_batch(&schema, 10000, 0);

        let props = WriterProperties::builder()
            .set_max_row_group_size(1000)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        sequential_writer.write(&batch).unwrap();
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 1000, None).unwrap();
        parallel_writer.write(&batch).unwrap();
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "large single batch should produce identical data"
        );
    }

    #[test]
    fn test_multiple_writers_concurrent() {
        let temp_dir = tempdir().unwrap();
        let schema = create_test_schema(5);

        let handles: Vec<_> = (0..4)
            .map(|writer_id| {
                let path = temp_dir
                    .path()
                    .join(format!("concurrent_{}.parquet", writer_id));
                let schema = Arc::clone(&schema);

                thread::spawn(move || {
                    let props = WriterProperties::builder().build();
                    let mut writer =
                        ParquetWriter::try_new(&path, &schema, props, 500, None).unwrap();

                    for i in 0..20 {
                        let batch = create_test_batch(&schema, 250, i * 250);
                        writer.write(&batch).unwrap();
                    }

                    let rows = writer.close().unwrap();
                    (path, rows)
                })
            })
            .collect();

        for handle in handles {
            let (path, rows) = handle.join().expect("writer thread panicked");
            assert_eq!(rows, 5000);
            assert!(path.exists());

            // verify data integrity
            let batches = read_parquet_to_batches(&path);
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 5000);
        }
    }

    #[test]
    fn test_deeply_nested_struct() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        // inner struct: {a: i32, b: i64}
        let inner_fields: Fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ]
        .into();

        // middle struct: {inner: {a, b}, c: f64}
        let middle_fields: Fields = vec![
            Field::new("inner", DataType::Struct(inner_fields.clone()), false),
            Field::new("c", DataType::Float64, false),
        ]
        .into();

        // outer schema: {id: i32, nested: {inner: {a, b}, c}}
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", DataType::Struct(middle_fields.clone()), false),
        ]));

        let batches: Vec<RecordBatch> = (0..5)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 100..(batch_idx + 1) * 100).collect();
                let a_values: Vec<i32> = (batch_idx * 100..(batch_idx + 1) * 100)
                    .map(|v| v * 2)
                    .collect();
                let b_values: Vec<i64> = (batch_idx * 100..(batch_idx + 1) * 100)
                    .map(|v| i64::from(v) * 3)
                    .collect();
                let c_values: Vec<f64> = (batch_idx * 100..(batch_idx + 1) * 100)
                    .map(|v| f64::from(v) * 1.5)
                    .collect();

                let inner_struct = StructArray::from(vec![
                    (
                        Arc::new(Field::new("a", DataType::Int32, false)),
                        Arc::new(Int32Array::from(a_values)) as Arc<dyn Array>,
                    ),
                    (
                        Arc::new(Field::new("b", DataType::Int64, false)),
                        Arc::new(Int64Array::from(b_values)) as Arc<dyn Array>,
                    ),
                ]);

                let middle_struct = StructArray::from(vec![
                    (
                        Arc::new(Field::new(
                            "inner",
                            DataType::Struct(inner_fields.clone()),
                            false,
                        )),
                        Arc::new(inner_struct) as Arc<dyn Array>,
                    ),
                    (
                        Arc::new(Field::new("c", DataType::Float64, false)),
                        Arc::new(Float64Array::from(c_values)) as Arc<dyn Array>,
                    ),
                ]);

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(middle_struct)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "deeply nested struct should produce identical data"
        );
    }

    #[test]
    #[allow(
        clippy::cast_possible_wrap,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn test_list_column() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        // use nullable item field to match what ListBuilder produces
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50).collect();

                let mut list_builder = ListBuilder::new(Int32Builder::new());
                for i in batch_idx * 50..(batch_idx + 1) * 50 {
                    // each row has i % 5 + 1 elements
                    let num_elements = (i % 5 + 1) as usize;
                    for j in 0..num_elements {
                        list_builder.values().append_value(i * 10 + j as i32);
                    }
                    list_builder.append(true);
                }
                let list_array = list_builder.finish();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(list_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 100, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "list columns should produce identical data"
        );
    }

    #[test]
    fn test_empty_batches_skipped() {
        // verify empty batches don't cause issues
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("with_empty.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let props = WriterProperties::builder().build();

        let mut writer = ParquetWriter::try_new(&path, &schema, props, 100, None).unwrap();

        // write normal batch
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        writer.write(&batch1).unwrap();

        // write empty batch
        let empty_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )
        .unwrap();
        writer.write(&empty_batch).unwrap();

        // write another normal batch
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
        )
        .unwrap();
        writer.write(&batch2).unwrap();

        let rows = writer.close().unwrap();
        assert_eq!(rows, 6);

        let batches = read_parquet_to_batches(&path);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6);
    }

    #[test]
    fn test_type_boolean() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::BooleanArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("nullable_flag", DataType::Boolean, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                let flags: Vec<bool> = (i * 100..(i + 1) * 100).map(|x| x % 2 == 0).collect();
                let nullable_flags: Vec<Option<bool>> = (i * 100..(i + 1) * 100)
                    .map(|x| if x % 3 == 0 { None } else { Some(x % 2 == 0) })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(BooleanArray::from(flags)),
                        Arc::new(BooleanArray::from(nullable_flags)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "boolean columns should produce identical data"
        );
    }

    #[test]
    fn test_type_binary() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::{BinaryArray, LargeBinaryArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Binary, false),
            Field::new("large_data", DataType::LargeBinary, false),
            Field::new("nullable_data", DataType::Binary, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                let data: Vec<&[u8]> = (i * 100..(i + 1) * 100)
                    .map(|x| {
                        let bytes: &[u8] = match x % 4 {
                            0 => b"hello",
                            1 => b"world",
                            2 => b"\x00\x01\x02\x03",
                            _ => b"test data here",
                        };
                        bytes
                    })
                    .collect();
                let large_data: Vec<&[u8]> = data.clone();
                let nullable_data: Vec<Option<&[u8]>> = (i * 100..(i + 1) * 100)
                    .map(|x| {
                        if x % 5 == 0 {
                            None
                        } else {
                            Some(b"value" as &[u8])
                        }
                    })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(BinaryArray::from(data)),
                        Arc::new(LargeBinaryArray::from(large_data)),
                        Arc::new(BinaryArray::from(nullable_data)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "binary columns should produce identical data"
        );
    }

    #[test]
    fn test_type_date() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::{Date32Array, Date64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("date32", DataType::Date32, false),
            Field::new("date64", DataType::Date64, false),
            Field::new("nullable_date", DataType::Date32, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                // date32: days since epoch
                let date32_values: Vec<i32> = (i * 100..(i + 1) * 100).map(|x| 18000 + x).collect();
                // date64: milliseconds since epoch
                let date64_values: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| (18000 + x) as i64 * 86400 * 1000)
                    .collect();
                let nullable_dates: Vec<Option<i32>> = (i * 100..(i + 1) * 100)
                    .map(|x| if x % 4 == 0 { None } else { Some(18000 + x) })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(Date32Array::from(date32_values)),
                        Arc::new(Date64Array::from(date64_values)),
                        Arc::new(Date32Array::from(nullable_dates)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "date columns should produce identical data"
        );
    }

    #[test]
    fn test_type_time() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::{Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("time32_ms", DataType::Time32(TimeUnit::Millisecond), false),
            Field::new("time64_us", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new("time64_ns", DataType::Time64(TimeUnit::Nanosecond), false),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                // time32: milliseconds since midnight (max ~86400000)
                let time32_values: Vec<i32> = (i * 100..(i + 1) * 100)
                    .map(|x| (x * 1000) % 86400000)
                    .collect();
                // time64: microseconds since midnight
                let time64_us_values: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| ((x * 1000) % 86400000) as i64 * 1000)
                    .collect();
                // time64: nanoseconds since midnight
                let time64_ns_values: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| ((x * 1000) % 86400000) as i64 * 1000000)
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(Time32MillisecondArray::from(time32_values)),
                        Arc::new(Time64MicrosecondArray::from(time64_us_values)),
                        Arc::new(Time64NanosecondArray::from(time64_ns_values)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "time columns should produce identical data"
        );
    }

    #[test]
    fn test_type_duration() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::DurationMicrosecondArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "duration_us",
                DataType::Duration(TimeUnit::Microsecond),
                false,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                let durations: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| x as i64 * 1_000_000)
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(DurationMicrosecondArray::from(durations)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "duration columns should produce identical data"
        );
    }

    #[test]
    fn test_type_decimal128() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::Decimal128Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("price", DataType::Decimal128(18, 2), false),
            Field::new("nullable_price", DataType::Decimal128(18, 4), true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                // decimal values stored as i128, scale=2 means 12345 = 123.45
                let prices: Vec<i128> = (i * 100..(i + 1) * 100)
                    .map(|x| x as i128 * 100 + 99)
                    .collect();
                let nullable_prices: Vec<Option<i128>> = (i * 100..(i + 1) * 100)
                    .map(|x| {
                        if x % 7 == 0 {
                            None
                        } else {
                            Some(x as i128 * 10000 + 1234)
                        }
                    })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(
                            Decimal128Array::from(prices)
                                .with_precision_and_scale(18, 2)
                                .unwrap(),
                        ),
                        Arc::new(
                            Decimal128Array::from(nullable_prices)
                                .with_precision_and_scale(18, 4)
                                .unwrap(),
                        ),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "decimal128 columns should produce identical data"
        );
    }

    #[test]
    fn test_type_large_utf8() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::LargeStringArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::LargeUtf8, false),
            Field::new("nullable_text", DataType::LargeUtf8, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                let texts: Vec<String> = (i * 100..(i + 1) * 100)
                    .map(|x| format!("large_string_value_{}_with_more_content", x))
                    .collect();
                let nullable_texts: Vec<Option<String>> = (i * 100..(i + 1) * 100)
                    .map(|x| {
                        if x % 6 == 0 {
                            None
                        } else {
                            Some(format!("nullable_{}", x))
                        }
                    })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(LargeStringArray::from(texts)),
                        Arc::new(LargeStringArray::from(nullable_texts)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "large utf8 columns should produce identical data"
        );
    }

    #[test]
    fn test_type_large_list() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::LargeListBuilder;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "values",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50).collect();

                let mut list_builder = LargeListBuilder::new(Int64Builder::new());
                for i in batch_idx * 50..(batch_idx + 1) * 50 {
                    let num_elements = (i % 5 + 1) as usize;
                    for j in 0..num_elements {
                        list_builder.values().append_value(i as i64 * 10 + j as i64);
                    }
                    list_builder.append(true);
                }
                let list_array = list_builder.finish();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(list_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 100, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "large list columns should produce identical data"
        );
    }

    #[test]
    fn test_type_fixed_size_list() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::FixedSizeListArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "coords",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 3),
                false,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 100..(batch_idx + 1) * 100).collect();
                // 3 floats per row (x, y, z coordinates)
                let coords: Vec<f64> = (batch_idx * 100..(batch_idx + 1) * 100)
                    .flat_map(|i| vec![i as f64, i as f64 * 2.0, i as f64 * 3.0])
                    .collect();

                let field = Arc::new(Field::new("item", DataType::Float64, false));
                let coords_array =
                    FixedSizeListArray::new(field, 3, Arc::new(Float64Array::from(coords)), None);

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(coords_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "fixed size list columns should produce identical data"
        );
    }

    #[test]
    fn test_type_map() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::MapBuilder;

        // use field names that MapBuilder produces by default
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "attributes",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50).collect();

                let mut map_builder =
                    MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());

                for i in batch_idx * 50..(batch_idx + 1) * 50 {
                    // each row has i % 3 + 1 key-value pairs
                    let num_pairs = (i % 3 + 1) as usize;
                    for j in 0..num_pairs {
                        map_builder.keys().append_value(format!("key_{}", j));
                        map_builder.values().append_value(i * 10 + j as i32);
                    }
                    map_builder.append(true).unwrap();
                }
                let map_array = map_builder.finish();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(map_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 100, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "map columns should produce identical data"
        );
    }

    #[test]
    fn test_type_nested_list() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        // List<List<Int32>> - nested lists
        // use default field names that ListBuilder produces
        let inner_field = Field::new("item", DataType::Int32, true);
        let outer_field = Field::new("item", DataType::List(Arc::new(inner_field.clone())), true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("matrix", DataType::List(Arc::new(outer_field)), false),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50).collect();

                let inner_builder = ListBuilder::new(Int32Builder::new());
                let mut outer_builder = ListBuilder::new(inner_builder);

                for i in batch_idx * 50..(batch_idx + 1) * 50 {
                    // each row has i % 3 + 1 inner lists
                    let num_inner_lists = (i % 3 + 1) as usize;
                    for j in 0..num_inner_lists {
                        // each inner list has j + 1 elements
                        for k in 0..=j {
                            outer_builder
                                .values()
                                .values()
                                .append_value(i * 100 + j as i32 * 10 + k as i32);
                        }
                        outer_builder.values().append(true);
                    }
                    outer_builder.append(true);
                }
                let nested_list_array = outer_builder.finish();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(nested_list_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 100, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "nested list columns should produce identical data"
        );
    }

    #[test]
    fn test_type_list_of_struct() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        // List<Struct<x: Int32, y: Int32>>
        // use "item" field name to match ListBuilder default
        let struct_fields: Fields = vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "points",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(struct_fields.clone()),
                    true,
                ))),
                false,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50).collect();

                // build a list of structs
                let x_builder = Int32Builder::new();
                let y_builder = Int32Builder::new();
                let struct_builder = StructBuilder::new(
                    struct_fields.clone(),
                    vec![Box::new(x_builder), Box::new(y_builder)],
                );
                let mut list_builder = ListBuilder::new(struct_builder);

                for i in batch_idx * 50..(batch_idx + 1) * 50 {
                    let num_points = (i % 4 + 1) as usize;
                    for j in 0..num_points {
                        list_builder
                            .values()
                            .field_builder::<Int32Builder>(0)
                            .unwrap()
                            .append_value(i * 10 + j as i32);
                        list_builder
                            .values()
                            .field_builder::<Int32Builder>(1)
                            .unwrap()
                            .append_value(i * 10 + j as i32 + 100);
                        list_builder.values().append(true);
                    }
                    list_builder.append(true);
                }
                let list_array = list_builder.finish();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(list_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 100, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "list of struct columns should produce identical data"
        );
    }

    #[test]
    fn test_type_struct_containing_list() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        // Struct<name: Utf8, scores: List<Int32>>
        let list_field = Field::new(
            "scores",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        );
        let struct_fields: Fields =
            vec![Field::new("name", DataType::Utf8, false), list_field].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("player", DataType::Struct(struct_fields.clone()), false),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50).collect();

                // build struct with list inside
                let name_builder = StringBuilder::new();
                let scores_builder = ListBuilder::new(Int32Builder::new());
                let mut struct_builder = StructBuilder::new(
                    struct_fields.clone(),
                    vec![Box::new(name_builder), Box::new(scores_builder)],
                );

                for i in batch_idx * 50..(batch_idx + 1) * 50 {
                    struct_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(format!("player_{}", i));

                    let scores_list_builder = struct_builder
                        .field_builder::<ListBuilder<Int32Builder>>(1)
                        .unwrap();
                    let num_scores = (i % 5 + 1) as usize;
                    for j in 0..num_scores {
                        scores_list_builder.values().append_value(i * 10 + j as i32);
                    }
                    scores_list_builder.append(true);

                    struct_builder.append(true);
                }
                let struct_array = struct_builder.finish();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(ids)), Arc::new(struct_array)],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 100, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "struct containing list should produce identical data"
        );
    }

    #[test]
    fn test_type_all_timestamps() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::{
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts_sec", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "ts_with_tz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                let base_ts = 1_700_000_000i64;
                let ts_sec: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| base_ts + x as i64)
                    .collect();
                let ts_ms: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| (base_ts + x as i64) * 1000)
                    .collect();
                let ts_us: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| (base_ts + x as i64) * 1_000_000)
                    .collect();
                let ts_ns: Vec<i64> = (i * 100..(i + 1) * 100)
                    .map(|x| (base_ts + x as i64) * 1_000_000_000)
                    .collect();
                let ts_with_tz: Vec<Option<i64>> = (i * 100..(i + 1) * 100)
                    .map(|x| {
                        if x % 8 == 0 {
                            None
                        } else {
                            Some((base_ts + x as i64) * 1_000_000)
                        }
                    })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(TimestampSecondArray::from(ts_sec)),
                        Arc::new(TimestampMillisecondArray::from(ts_ms)),
                        Arc::new(TimestampMicrosecondArray::from(ts_us)),
                        Arc::new(TimestampNanosecondArray::from(ts_ns)),
                        Arc::new(TimestampMicrosecondArray::from(ts_with_tz).with_timezone("UTC")),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "all timestamp variants should produce identical data"
        );
    }

    #[test]
    fn test_type_float32() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::Float32Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("nullable_f32", DataType::Float32, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
                let f32_values: Vec<f32> =
                    (i * 100..(i + 1) * 100).map(|x| x as f32 * 1.5).collect();
                let nullable_f32: Vec<Option<f32>> = (i * 100..(i + 1) * 100)
                    .map(|x| {
                        if x % 9 == 0 {
                            None
                        } else {
                            Some(x as f32 * 0.5)
                        }
                    })
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(Float32Array::from(f32_values)),
                        Arc::new(Float32Array::from(nullable_f32)),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 200, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "float32 columns should produce identical data"
        );
    }

    #[test]
    fn test_parallel_stress_mixed_types_many_row_groups() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::{BooleanArray, Date32Array, Decimal128Array, Float32Array};

        // wide schema with many different types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("bool_col", DataType::Boolean, false),
            Field::new("i32_col", DataType::Int32, true),
            Field::new("i64_col", DataType::Int64, false),
            Field::new("f32_col", DataType::Float32, true),
            Field::new("f64_col", DataType::Float64, false),
            Field::new("str_col", DataType::Utf8, true),
            Field::new("date_col", DataType::Date32, false),
            Field::new(
                "ts_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("decimal_col", DataType::Decimal128(18, 2), false),
        ]));

        // create many small batches to force many row groups
        let batches: Vec<RecordBatch> = (0..50)
            .map(|i| {
                let size = 100;
                let ids: Vec<i32> = (i * size..(i + 1) * size).collect();
                let bools: Vec<bool> = (i * size..(i + 1) * size).map(|x| x % 2 == 0).collect();
                let i32s: Vec<Option<i32>> = (i * size..(i + 1) * size)
                    .map(|x| if x % 7 == 0 { None } else { Some(x) })
                    .collect();
                let i64s: Vec<i64> = (i * size..(i + 1) * size).map(|x| x as i64 * 2).collect();
                let f32s: Vec<Option<f32>> = (i * size..(i + 1) * size)
                    .map(|x| {
                        if x % 11 == 0 {
                            None
                        } else {
                            Some(x as f32 * 0.5)
                        }
                    })
                    .collect();
                let f64s: Vec<f64> = (i * size..(i + 1) * size).map(|x| x as f64 * 1.5).collect();
                let strs: Vec<Option<&str>> = (i * size..(i + 1) * size)
                    .map(|x| if x % 5 == 0 { None } else { Some("value") })
                    .collect();
                let dates: Vec<i32> = (i * size..(i + 1) * size).map(|x| 18000 + x).collect();
                let timestamps: Vec<Option<i64>> = (i * size..(i + 1) * size)
                    .map(|x| {
                        if x % 13 == 0 {
                            None
                        } else {
                            Some(1_700_000_000_000_000i64 + x as i64 * 1_000_000)
                        }
                    })
                    .collect();
                let decimals: Vec<i128> = (i * size..(i + 1) * size)
                    .map(|x| x as i128 * 100)
                    .collect();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(BooleanArray::from(bools)),
                        Arc::new(Int32Array::from(i32s)),
                        Arc::new(Int64Array::from(i64s)),
                        Arc::new(Float32Array::from(f32s)),
                        Arc::new(Float64Array::from(f64s)),
                        Arc::new(StringArray::from(strs)),
                        Arc::new(Date32Array::from(dates)),
                        Arc::new(TimestampMicrosecondArray::from(timestamps)),
                        Arc::new(
                            Decimal128Array::from(decimals)
                                .with_precision_and_scale(18, 2)
                                .unwrap(),
                        ),
                    ],
                )
                .unwrap()
            })
            .collect();

        // small row group size to create many row groups
        let props = WriterProperties::builder()
            .set_max_row_group_size(150)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 150, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "mixed types with many row groups should produce identical data"
        );
    }

    #[test]
    fn test_parallel_stress_nested_types_many_row_groups() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        // complex nested schema
        let inner_struct: Fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "list_col",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
            Field::new("struct_col", DataType::Struct(inner_struct.clone()), false),
        ]));

        let batches: Vec<RecordBatch> = (0..30)
            .map(|batch_idx| {
                let ids: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50).collect();

                // list column
                let mut list_builder = ListBuilder::new(Int64Builder::new());
                for i in batch_idx * 50..(batch_idx + 1) * 50 {
                    let num_elements = (i % 5 + 1) as usize;
                    for j in 0..num_elements {
                        if (i + j as i32) % 3 == 0 {
                            list_builder.values().append_null();
                        } else {
                            list_builder.values().append_value(i as i64 * 10 + j as i64);
                        }
                    }
                    list_builder.append(true);
                }
                let list_array = list_builder.finish();

                // struct column
                let a_values: Vec<i32> = (batch_idx * 50..(batch_idx + 1) * 50)
                    .map(|x| x * 2)
                    .collect();
                let b_values: Vec<Option<String>> = (batch_idx * 50..(batch_idx + 1) * 50)
                    .map(|x| {
                        if x % 4 == 0 {
                            None
                        } else {
                            Some(format!("str_{}", x))
                        }
                    })
                    .collect();
                let struct_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("a", DataType::Int32, false)),
                        Arc::new(Int32Array::from(a_values)) as Arc<dyn Array>,
                    ),
                    (
                        Arc::new(Field::new("b", DataType::Utf8, true)),
                        Arc::new(StringArray::from(b_values)) as Arc<dyn Array>,
                    ),
                ]);

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(list_array),
                        Arc::new(struct_array),
                    ],
                )
                .unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_max_row_group_size(75)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 75, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "nested types with many row groups should produce identical data"
        );
    }

    #[test]
    fn test_parallel_stress_deeply_nested_types() {
        let temp_dir = tempdir().unwrap();
        let sequential_path = temp_dir.path().join("sequential.parquet");
        let parallel_path = temp_dir.path().join("parallel.parquet");

        use arrow::array::{FixedSizeListArray, MapBuilder};

        // deeply nested schema: struct containing list, list of struct, nested lists, map
        let inner_struct_fields: Fields = vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Float64, true),
        ]
        .into();

        // Struct<name, scores: List<Int32>>
        let struct_with_list_fields: Fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            ),
        ]
        .into();

        // List<List<Int32>>
        let nested_list_inner = Field::new("item", DataType::Int32, true);
        let nested_list_outer =
            Field::new("item", DataType::List(Arc::new(nested_list_inner)), true);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            // Struct<x, y>
            Field::new(
                "point",
                DataType::Struct(inner_struct_fields.clone()),
                false,
            ),
            // List<Struct<x, y>>
            Field::new(
                "points",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(inner_struct_fields.clone()),
                    true,
                ))),
                false,
            ),
            // Struct<name, List<Int32>>
            Field::new(
                "player",
                DataType::Struct(struct_with_list_fields.clone()),
                false,
            ),
            // List<List<Int32>>
            Field::new("matrix", DataType::List(Arc::new(nested_list_outer)), false),
            // FixedSizeList<Float64, 3>
            Field::new(
                "coords",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 3),
                false,
            ),
            // Map<Utf8, Int32>
            Field::new(
                "attrs",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ]));

        let batches: Vec<RecordBatch> = (0..40)
            .map(|batch_idx| {
                let size = 50;
                let ids: Vec<i32> = (batch_idx * size..(batch_idx + 1) * size).collect();

                // point: Struct<x, y>
                let x_values: Vec<i32> = (batch_idx * size..(batch_idx + 1) * size)
                    .map(|i| i * 2)
                    .collect();
                let y_values: Vec<Option<f64>> = (batch_idx * size..(batch_idx + 1) * size)
                    .map(|i| {
                        if i % 5 == 0 {
                            None
                        } else {
                            Some(i as f64 * 1.5)
                        }
                    })
                    .collect();
                let point_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("x", DataType::Int32, false)),
                        Arc::new(Int32Array::from(x_values)) as Arc<dyn Array>,
                    ),
                    (
                        Arc::new(Field::new("y", DataType::Float64, true)),
                        Arc::new(Float64Array::from(y_values)) as Arc<dyn Array>,
                    ),
                ]);

                // points: List<Struct<x, y>>
                let x_builder = Int32Builder::new();
                let y_builder = arrow::array::Float64Builder::new();
                let struct_builder = StructBuilder::new(
                    inner_struct_fields.clone(),
                    vec![Box::new(x_builder), Box::new(y_builder)],
                );
                let mut points_list_builder = ListBuilder::new(struct_builder);
                for i in batch_idx * size..(batch_idx + 1) * size {
                    let num_points = (i % 3 + 1) as usize;
                    for j in 0..num_points {
                        points_list_builder
                            .values()
                            .field_builder::<Int32Builder>(0)
                            .unwrap()
                            .append_value(i * 10 + j as i32);
                        points_list_builder
                            .values()
                            .field_builder::<arrow::array::Float64Builder>(1)
                            .unwrap()
                            .append_option(if j % 2 == 0 {
                                Some((i * 10 + j as i32) as f64)
                            } else {
                                None
                            });
                        points_list_builder.values().append(true);
                    }
                    points_list_builder.append(true);
                }
                let points_array = points_list_builder.finish();

                // player: Struct<name, List<Int32>>
                let name_builder = StringBuilder::new();
                let scores_builder = ListBuilder::new(Int32Builder::new());
                let mut player_builder = StructBuilder::new(
                    struct_with_list_fields.clone(),
                    vec![Box::new(name_builder), Box::new(scores_builder)],
                );
                for i in batch_idx * size..(batch_idx + 1) * size {
                    player_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(format!("player_{}", i));
                    let scores_list = player_builder
                        .field_builder::<ListBuilder<Int32Builder>>(1)
                        .unwrap();
                    let num_scores = (i % 4 + 1) as usize;
                    for j in 0..num_scores {
                        if j % 3 == 0 {
                            scores_list.values().append_null();
                        } else {
                            scores_list.values().append_value(i * 100 + j as i32);
                        }
                    }
                    scores_list.append(true);
                    player_builder.append(true);
                }
                let player_array = player_builder.finish();

                // matrix: List<List<Int32>>
                let inner_builder = ListBuilder::new(Int32Builder::new());
                let mut matrix_builder = ListBuilder::new(inner_builder);
                for i in batch_idx * size..(batch_idx + 1) * size {
                    let num_rows = (i % 3 + 1) as usize;
                    for row in 0..num_rows {
                        let num_cols = (i % 2 + 1) as usize;
                        for col in 0..num_cols {
                            matrix_builder
                                .values()
                                .values()
                                .append_value(i * 100 + row as i32 * 10 + col as i32);
                        }
                        matrix_builder.values().append(true);
                    }
                    matrix_builder.append(true);
                }
                let matrix_array = matrix_builder.finish();

                // coords: FixedSizeList<Float64, 3>
                let coords: Vec<f64> = (batch_idx * size..(batch_idx + 1) * size)
                    .flat_map(|i| vec![i as f64, i as f64 * 2.0, i as f64 * 3.0])
                    .collect();
                let coords_field = Arc::new(Field::new("item", DataType::Float64, false));
                let coords_array = FixedSizeListArray::new(
                    coords_field,
                    3,
                    Arc::new(Float64Array::from(coords)),
                    None,
                );

                // attrs: Map<Utf8, Int32>
                let mut map_builder =
                    MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
                for i in batch_idx * size..(batch_idx + 1) * size {
                    let num_attrs = (i % 3 + 1) as usize;
                    for j in 0..num_attrs {
                        map_builder.keys().append_value(format!("attr_{}", j));
                        if j % 2 == 0 {
                            map_builder.values().append_value(i * 10 + j as i32);
                        } else {
                            map_builder.values().append_null();
                        }
                    }
                    map_builder.append(true).unwrap();
                }
                let map_array = map_builder.finish();

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(point_array),
                        Arc::new(points_array),
                        Arc::new(player_array),
                        Arc::new(matrix_array),
                        Arc::new(coords_array),
                        Arc::new(map_array),
                    ],
                )
                .unwrap()
            })
            .collect();

        // small row group size to create many row groups with complex nesting
        let props = WriterProperties::builder()
            .set_max_row_group_size(60)
            .build();
        let file = File::create(&sequential_path).unwrap();
        let mut sequential_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for batch in &batches {
            sequential_writer.write(batch).unwrap();
        }
        sequential_writer.close().unwrap();

        let props = WriterProperties::builder().build();
        let mut parallel_writer =
            ParquetWriter::try_new(&parallel_path, &schema, props, 60, None).unwrap();
        for batch in &batches {
            parallel_writer.write(batch).unwrap();
        }
        parallel_writer.close().unwrap();

        let sequential_data = concat_batches(&read_parquet_to_batches(&sequential_path));
        let parallel_data = concat_batches(&read_parquet_to_batches(&parallel_path));

        assert!(
            batches_equal(&sequential_data, &parallel_data),
            "deeply nested types should produce identical data under parallel execution"
        );
    }
}
