//! Parallel parquet writer with bounded threading.
//!
//! Uses rayon threadpools for controlled concurrency instead of unbounded spawn_blocking.
//! Two-stage pipeline: coordinator (spawns row_group_tasks) → writer.

use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, ArrowWriterOptions,
    compute_leaves,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{ParquetPools, ParquetWriter};
use crate::sinks::parquet::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ROW_GROUP_SIZE};
use crate::utils::cancellable_channel::{
    CancellableReceiver, CancellableSender, ChannelError, cancellable_channel,
};
use crate::utils::first_error::FirstError;
use crate::utils::ordered_channel::{OrderedReceiver, OrderedSender, ordered_channel};
use crate::utils::rayon_bridge::spawn_on_pool;

/// Configuration for the parallel parquet writer.
#[derive(Debug, Clone)]
pub struct ParallelWriterConfig {
    /// Maximum number of rows per Parquet row group.
    pub max_row_group_size: usize,
    /// Buffer size for the parquet file writer.
    pub buffer_size: usize,
    /// Maximum number of row groups that can be encoding concurrently.
    pub max_row_group_concurrency: usize,
    /// Rows to coalesce before sending to encoding tasks. Larger values reduce
    /// spawn overhead but increase memory. Should be <= max_row_group_size.
    pub encoding_batch_size: usize,
    /// Channel buffer size for batches sent to row group encoder tasks.
    pub batch_channel_size: usize,
    /// Channel buffer size for encoded row groups sent to the writer task.
    pub encoded_channel_size: usize,
    /// Whether to skip embedding Arrow schema in the parquet file metadata.
    pub skip_arrow_metadata: bool,
}

impl Default for ParallelWriterConfig {
    fn default() -> Self {
        Self {
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            buffer_size: DEFAULT_BUFFER_SIZE,
            max_row_group_concurrency: 4,
            encoding_batch_size: 122_880,
            batch_channel_size: 16,
            encoded_channel_size: 4,
            skip_arrow_metadata: true,
        }
    }
}

/// Encoded row group ready for writing.
struct EncodedRowGroup {
    chunks: Vec<ArrowColumnChunk>,
    num_rows: usize,
}

/// Parallel parquet writer that uses rayon pools for bounded threading.
///
/// Pipeline architecture:
/// ```text
/// write_batch()
///      │
///      ▼ input_channel (tokio mpsc)
/// ┌─────────────────────────────────────────────────────────┐
/// │ Coordinator task                                        │
/// │  - assigns row group index at spawn time                │
/// │  - routes batches to current row_group_task             │
/// │  - slices batches at row group boundaries               │
/// │  - spawns new row_group_task when current is full       │
/// └────────┬────────────────────────────────────────────────┘
///          │ spawns
///          ▼
/// ┌─────────────────────────────────────────────────────────┐
/// │ Row group tasks (spawned as needed)                     │
/// │  - receives batches via dedicated channel               │
/// │  - encodes columns incrementally on rayon pool          │
/// │  - sends EncodedRowGroup to writer when channel closes  │
/// └────────┬────────────────────────────────────────────────┘
///          │
///          ▼ encoded_channel (tokio mpsc)
/// ┌─────────────────────────────────────────────────────────┐
/// │ Writer task                                             │
/// │  - buffers encoded row groups for ordering              │
/// │  - writes to file in order on I/O pool                  │
/// └─────────────────────────────────────────────────────────┘
/// ```
pub struct ParallelParquetWriter {
    batch_sender: Option<mpsc::Sender<RecordBatch>>,
    cancel_token: CancellationToken,
    monitor_handle: Option<tokio::task::JoinHandle<Result<u64>>>,
}

impl ParallelParquetWriter {
    pub fn new(
        path: impl AsRef<Path>,
        schema: &SchemaRef,
        props: WriterProperties,
        pools: Arc<ParquetPools>,
        config: ParallelWriterConfig,
    ) -> Self {
        assert!(
            config.max_row_group_size > 0,
            "max_row_group_size must be > 0"
        );
        assert!(
            config.max_row_group_concurrency > 0,
            "max_row_group_concurrency must be > 0"
        );
        assert!(
            config.encoded_channel_size > 0,
            "encoded_channel_size must be > 0"
        );
        assert!(
            config.batch_channel_size > 0,
            "batch_channel_size must be > 0"
        );
        assert!(config.buffer_size > 0, "buffer_size must be > 0");
        assert!(
            config.encoding_batch_size > 0,
            "encoding_batch_size must be > 0"
        );
        let path = path.as_ref().to_path_buf();
        let schema = Arc::clone(schema);
        let cancel_token = CancellationToken::new();

        let (batch_tx, batch_rx) = mpsc::channel::<RecordBatch>(config.batch_channel_size);

        let monitor_handle = tokio::spawn({
            let cancel = cancel_token.clone();
            async move { run_pipeline(path, schema, props, pools, config, batch_rx, cancel).await }
        });

        Self {
            batch_sender: Some(batch_tx),
            cancel_token,
            monitor_handle: Some(monitor_handle),
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let sender = self
            .batch_sender
            .as_ref()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        if self
            .monitor_handle
            .as_ref()
            .is_some_and(|h| h.is_finished())
        {
            return Err(anyhow!("writer task failed"));
        }

        sender
            .send(batch)
            .await
            .map_err(|_| anyhow!("writer task failed"))
    }

    pub fn blocking_write(&mut self, batch: RecordBatch) -> Result<()> {
        let sender = self
            .batch_sender
            .as_ref()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        if self
            .monitor_handle
            .as_ref()
            .is_some_and(|h| h.is_finished())
        {
            return Err(anyhow!("writer task failed"));
        }

        sender
            .blocking_send(batch)
            .map_err(|_| anyhow!("writer task failed"))
    }

    pub async fn close(mut self) -> Result<u64> {
        self.batch_sender.take();

        let handle = self
            .monitor_handle
            .take()
            .ok_or_else(|| anyhow!("already closed"))?;
        handle
            .await
            .map_err(|e| anyhow!("writer task panicked: {e}"))?
    }

    pub fn blocking_close(self) -> Result<u64> {
        tokio::runtime::Handle::current().block_on(self.close())
    }

    pub async fn cancel(mut self) -> Result<()> {
        self.cancel_token.cancel();
        self.batch_sender.take();

        if let Some(handle) = self.monitor_handle.take() {
            let _ = handle.await;
        }
        Ok(())
    }

    pub fn blocking_cancel(self) -> Result<()> {
        tokio::runtime::Handle::current().block_on(self.cancel())
    }
}

impl Drop for ParallelParquetWriter {
    fn drop(&mut self) {
        if let Some(_handle) = self.monitor_handle.take() {
            self.cancel_token.cancel();
        }
    }
}

#[async_trait]
impl ParquetWriter for ParallelParquetWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        ParallelParquetWriter::write(self, batch).await
    }

    fn blocking_write(&mut self, batch: RecordBatch) -> Result<()> {
        ParallelParquetWriter::blocking_write(self, batch)
    }

    async fn close(self: Box<Self>) -> Result<u64> {
        ParallelParquetWriter::close(*self).await
    }

    fn blocking_close(self: Box<Self>) -> Result<u64> {
        ParallelParquetWriter::blocking_close(*self)
    }

    async fn cancel(self: Box<Self>) -> Result<()> {
        ParallelParquetWriter::cancel(*self).await
    }

    fn blocking_cancel(self: Box<Self>) -> Result<()> {
        ParallelParquetWriter::blocking_cancel(*self)
    }
}

async fn send_coalesced_batches(
    coalescer: &mut BatchCoalescer,
    tx: &CancellableSender<RecordBatch>,
) -> Result<(), ChannelError> {
    while let Some(batch) = coalescer.next_completed_batch() {
        tx.send(batch).await?;
    }
    Ok(())
}

enum TaskResult {
    Coordinator,
    Writer(u64),
}

/// Main pipeline orchestrator.
async fn run_pipeline(
    path: PathBuf,
    schema: SchemaRef,
    props: WriterProperties,
    pools: Arc<ParquetPools>,
    config: ParallelWriterConfig,
    batch_rx: mpsc::Receiver<RecordBatch>,
    cancel: CancellationToken,
) -> Result<u64> {
    let first_error = Arc::new(FirstError::new(cancel.clone()));

    let file = spawn_on_pool(&pools.io, {
        let path = path.clone();
        let buffer_size = config.buffer_size;
        move || -> Result<_> { Ok(BufWriter::with_capacity(buffer_size, File::create(&path)?)) }
    })
    .await
    .context("file create task failed")??;

    let arrow_writer = ArrowWriter::try_new_with_options(
        file,
        Arc::clone(&schema),
        ArrowWriterOptions::new()
            .with_properties(props)
            .with_skip_arrow_metadata(config.skip_arrow_metadata),
    )?;
    let (file_writer, row_group_factory) = arrow_writer.into_serialized_writer()?;
    let row_group_factory = Arc::new(row_group_factory);

    let (encoded_tx, encoded_rx) =
        ordered_channel::<EncodedRowGroup>(config.encoded_channel_size, cancel.clone());

    let mut join_set: JoinSet<Result<TaskResult>> = JoinSet::new();

    join_set.spawn(coordinator_task(
        batch_rx,
        encoded_tx,
        Arc::clone(&pools),
        row_group_factory,
        schema,
        config.max_row_group_size,
        config.max_row_group_concurrency,
        config.encoding_batch_size,
        config.batch_channel_size,
        cancel.clone(),
    ));

    join_set.spawn(writer_task(encoded_rx, file_writer, pools));

    let mut total_rows = 0u64;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(task_result)) => match task_result {
                TaskResult::Coordinator => {}
                TaskResult::Writer(rows) => total_rows = rows,
            },
            Ok(Err(e)) => {
                first_error.set(e);
            }
            Err(e) => {
                first_error.set(anyhow!("task panicked: {e}"));
            }
        }
    }

    if let Some(err) = first_error.take() {
        return Err(err);
    }

    Ok(total_rows)
}

/// Coordinator task: routes batches to row group tasks, handles batch slicing at boundaries.
/// Coalesces small batches up to encoding_batch_size before sending to reduce spawn overhead.
#[allow(clippy::too_many_arguments)]
async fn coordinator_task(
    batch_rx: mpsc::Receiver<RecordBatch>,
    encoded_tx: OrderedSender<EncodedRowGroup>,
    pools: Arc<ParquetPools>,
    factory: Arc<ArrowRowGroupWriterFactory>,
    schema: SchemaRef,
    max_row_group_size: usize,
    max_row_group_concurrency: usize,
    encoding_batch_size: usize,
    batch_channel_size: usize,
    cancel: CancellationToken,
) -> Result<TaskResult> {
    let first_error = Arc::new(FirstError::new(cancel.clone()));
    let mut join_set: JoinSet<()> = JoinSet::new();

    let result = coordinator_loop(
        batch_rx,
        encoded_tx,
        pools,
        factory,
        schema,
        max_row_group_size,
        max_row_group_concurrency,
        encoding_batch_size,
        batch_channel_size,
        cancel,
        Arc::clone(&first_error),
        &mut join_set,
    )
    .await;

    if let Err(e) = result {
        first_error.set(e);
    }

    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            first_error.set(anyhow!("row group task panicked: {e}"));
        }
    }

    if let Some(e) = first_error.take() {
        return Err(e);
    }

    Ok(TaskResult::Coordinator)
}

#[allow(clippy::too_many_arguments)]
async fn coordinator_loop(
    batch_rx: mpsc::Receiver<RecordBatch>,
    encoded_tx: OrderedSender<EncodedRowGroup>,
    pools: Arc<ParquetPools>,
    factory: Arc<ArrowRowGroupWriterFactory>,
    schema: SchemaRef,
    max_row_group_size: usize,
    max_row_group_concurrency: usize,
    encoding_batch_size: usize,
    batch_channel_size: usize,
    cancel: CancellationToken,
    first_error: Arc<FirstError>,
    join_set: &mut JoinSet<()>,
) -> Result<()> {
    let mut batch_rx = CancellableReceiver::new(batch_rx, cancel.clone(), "input_batches");
    let mut row_group_index = 0usize;
    let mut current_rows = 0usize;
    let mut coalescer = BatchCoalescer::new(
        Arc::clone(&schema),
        encoding_batch_size.min(max_row_group_size),
    );

    let (current_tx, rx) =
        cancellable_channel::<RecordBatch>(batch_channel_size, cancel.clone(), "row_group");
    let column_writers = factory.create_column_writers(row_group_index)?;
    join_set.spawn(row_group_task(
        row_group_index,
        rx,
        encoded_tx.clone(),
        Arc::clone(&pools),
        column_writers,
        Arc::clone(&schema),
        Arc::clone(&first_error),
    ));
    let mut current_tx = Some(current_tx);

    while let Ok(batch) = batch_rx.recv().await {
        let mut remaining = Some(batch);

        while let Some(batch) = remaining.take() {
            let batch_rows = batch.num_rows();
            let space_left = max_row_group_size.saturating_sub(current_rows);

            if batch_rows <= space_left {
                coalescer.push_batch(batch)?;
                current_rows += batch_rows;

                match send_coalesced_batches(
                    &mut coalescer,
                    current_tx
                        .as_ref()
                        .ok_or_else(|| anyhow!("current tx missing"))?,
                )
                .await
                {
                    Ok(()) => {}
                    Err(ChannelError::Cancelled) => return Ok(()),
                    Err(ChannelError::Disconnected { .. }) => {
                        return Err(anyhow!("row group task channel disconnected"));
                    }
                }
            } else {
                if space_left > 0 {
                    coalescer.push_batch(batch.slice(0, space_left))?;
                }

                coalescer.finish_buffered_batch()?;
                match send_coalesced_batches(
                    &mut coalescer,
                    current_tx
                        .as_ref()
                        .ok_or_else(|| anyhow!("current tx missing"))?,
                )
                .await
                {
                    Ok(()) => {}
                    Err(ChannelError::Cancelled) => return Ok(()),
                    Err(ChannelError::Disconnected { .. }) => {
                        return Err(anyhow!("row group task channel disconnected"));
                    }
                }

                current_tx.take();
                row_group_index += 1;
                coalescer = BatchCoalescer::new(
                    Arc::clone(&schema),
                    encoding_batch_size.min(max_row_group_size),
                );

                while join_set.len() >= max_row_group_concurrency {
                    if let Some(Err(e)) = join_set.join_next().await {
                        first_error.set(anyhow!("row group task panicked: {e}"));
                    }
                    if first_error.is_cancelled() {
                        return Ok(());
                    }
                }

                let (tx, rx) = cancellable_channel::<RecordBatch>(
                    batch_channel_size,
                    cancel.clone(),
                    "row_group",
                );
                let column_writers = factory.create_column_writers(row_group_index)?;
                join_set.spawn(row_group_task(
                    row_group_index,
                    rx,
                    encoded_tx.clone(),
                    Arc::clone(&pools),
                    column_writers,
                    Arc::clone(&schema),
                    Arc::clone(&first_error),
                ));
                current_tx = Some(tx);
                current_rows = 0;

                let remainder_len = batch_rows - space_left;
                if remainder_len > 0 {
                    remaining = Some(batch.slice(space_left, remainder_len));
                }
            }
        }
    }

    coalescer.finish_buffered_batch()?;
    if let Some(tx) = &current_tx {
        match send_coalesced_batches(&mut coalescer, tx).await {
            Ok(()) => {}
            Err(ChannelError::Cancelled) => return Ok(()),
            Err(ChannelError::Disconnected { .. }) => {
                return Err(anyhow!("row group task channel disconnected"));
            }
        }
    }
    current_tx.take();
    Ok(())
}

/// Row group task: receives batches, encodes columns incrementally, sends to writer.
///
/// Column writers are created by the coordinator before spawning this task to ensure
/// they're created in sequential order (the factory is not thread-safe for concurrent
/// out-of-order creation).
async fn row_group_task(
    row_group_index: usize,
    mut batch_rx: CancellableReceiver<RecordBatch>,
    encoded_tx: OrderedSender<EncodedRowGroup>,
    pools: Arc<ParquetPools>,
    column_writers: Vec<ArrowColumnWriter>,
    schema: SchemaRef,
    first_error: Arc<FirstError>,
) {
    let num_columns = column_writers.len();
    let mut writers: Vec<Option<ArrowColumnWriter>> =
        column_writers.into_iter().map(Some).collect();
    let mut num_rows = 0usize;
    let mut handles = Vec::with_capacity(num_columns);

    while let Ok(batch) = batch_rx.recv().await {
        num_rows += batch.num_rows();

        let mut leaves_per_column: Vec<Vec<_>> = (0..num_columns).map(|_| Vec::new()).collect();
        let mut col_idx = 0;
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            match compute_leaves(field.as_ref(), column) {
                Ok(leaves) => {
                    for leaf in leaves {
                        leaves_per_column[col_idx].push(leaf);
                        col_idx += 1;
                    }
                }
                Err(e) => {
                    first_error.set(e.into());
                    return;
                }
            }
        }

        for (writer_slot, leaves) in writers.iter_mut().zip(leaves_per_column) {
            let Some(mut writer) = writer_slot.take() else {
                first_error.set(anyhow!("writer already consumed"));
                return;
            };
            let handle = spawn_on_pool(&pools.encoding, move || -> Result<ArrowColumnWriter> {
                for leaf in leaves {
                    writer.write(&leaf)?;
                }
                Ok(writer)
            });
            handles.push(handle);
        }

        for (i, handle) in handles.drain(..).enumerate() {
            match handle.await.context("column write task failed") {
                Ok(Ok(w)) => writers[i] = Some(w),
                Ok(Err(e)) => {
                    first_error.set(e);
                    return;
                }
                Err(e) => {
                    first_error.set(e);
                    return;
                }
            }
        }
    }

    let mut chunk_handles = Vec::with_capacity(num_columns);
    for writer_slot in writers {
        let Some(writer) = writer_slot else {
            first_error.set(anyhow!("writer missing"));
            return;
        };
        let handle = spawn_on_pool(&pools.encoding, move || -> Result<ArrowColumnChunk> {
            writer.close().map_err(|e| anyhow!(e))
        });
        chunk_handles.push(handle);
    }

    let mut chunks = Vec::with_capacity(num_columns);
    for handle in chunk_handles {
        match handle.await.context("column close task failed") {
            Ok(Ok(chunk)) => chunks.push(chunk),
            Ok(Err(e)) => {
                first_error.set(e);
                return;
            }
            Err(e) => {
                first_error.set(e);
                return;
            }
        }
    }

    let encoded = EncodedRowGroup { chunks, num_rows };
    let _ = encoded_tx.send(row_group_index, encoded).await;
}

async fn write_row_group(
    pools: &ParquetPools,
    fw: SerializedFileWriter<BufWriter<File>>,
    rg: EncodedRowGroup,
) -> Result<(SerializedFileWriter<BufWriter<File>>, u64)> {
    let num_rows = rg.num_rows as u64;
    let fw = spawn_on_pool(&pools.io, move || -> Result<_> {
        let mut fw = fw;
        let mut row_group = fw.next_row_group()?;
        for chunk in rg.chunks {
            chunk.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;
        Ok(fw)
    })
    .await
    .context("row group write task failed")??;
    Ok((fw, num_rows))
}

async fn writer_task(
    mut encoded_rx: OrderedReceiver<EncodedRowGroup>,
    file_writer: SerializedFileWriter<BufWriter<File>>,
    pools: Arc<ParquetPools>,
) -> Result<TaskResult> {
    let mut file_writer = Some(file_writer);
    let mut total_rows = 0u64;

    while let Ok(rg) = encoded_rx.recv().await {
        let fw = file_writer.take().unwrap();
        let (fw, rows) = write_row_group(&pools, fw, rg).await?;
        file_writer = Some(fw);
        total_rows += rows;
    }

    if let Some(fw) = file_writer.take() {
        spawn_on_pool(&pools.io, move || -> Result<_> {
            fw.close()?;
            Ok(())
        })
        .await
        .context("file close task failed")??;
    }

    Ok(TaskResult::Writer(total_rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::TempDir;

    fn test_schema() -> SchemaRef {
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

    fn test_pools() -> Arc<ParquetPools> {
        Arc::new(ParquetPools::new(2, 2).unwrap())
    }

    #[tokio::test]
    async fn test_parallel_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig::default(),
        );

        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
        writer.write(batch).await.unwrap();

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_parallel_writer_multiple_row_groups() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig {
                max_row_group_size: 5,
                ..Default::default()
            },
        );

        // write 12 rows in batches of 3, should create 3 row groups (5, 5, 2)
        for i in 0..4 {
            let batch = create_batch(
                &schema,
                &[i * 3 + 1, i * 3 + 2, i * 3 + 3],
                &["a", "b", "c"],
            );
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 12);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 12);
    }

    #[tokio::test]
    async fn test_parallel_writer_exact_row_group_boundary() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig {
                max_row_group_size: 3,
                ..Default::default()
            },
        );

        // write exactly 6 rows in batches of 3, should create 2 row groups (3, 3)
        for i in 0..2 {
            let batch = create_batch(
                &schema,
                &[i * 3 + 1, i * 3 + 2, i * 3 + 3],
                &["a", "b", "c"],
            );
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 6);
    }

    #[tokio::test]
    async fn test_parallel_writer_batch_larger_than_row_group() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig {
                max_row_group_size: 2,
                ..Default::default()
            },
        );

        // write a batch of 5 rows with max_row_group_size=2
        // should create 3 row groups (2, 2, 1)
        let batch = create_batch(&schema, &[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
        writer.write(batch).await.unwrap();

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 5);

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.metadata();
        assert_eq!(metadata.num_row_groups(), 3);
        assert_eq!(metadata.row_group(0).num_rows(), 2);
        assert_eq!(metadata.row_group(1).num_rows(), 2);
        assert_eq!(metadata.row_group(2).num_rows(), 1);
    }

    #[tokio::test]
    async fn test_parallel_writer_coalesces_small_batches() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig {
                max_row_group_size: 100,
                encoding_batch_size: 10,
                ..Default::default()
            },
        );

        // write 20 tiny batches of 1 row each
        // should coalesce into 2 batches of 10 rows for the row group task
        for i in 0..20 {
            let batch = create_batch(&schema, &[i], &["x"]);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 20);

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.row_group(0).num_rows(), 20);
    }

    #[tokio::test]
    async fn test_parallel_writer_batch_larger_than_encoding_batch_size() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig {
                max_row_group_size: 100,
                encoding_batch_size: 5,
                ..Default::default()
            },
        );

        // write a batch larger than encoding_batch_size,
        // coalescer will produce multiple coalesced batches
        let ids: Vec<i32> = (0..15).collect();
        let names: Vec<&str> = (0..15).map(|_| "test").collect();
        let batch = create_batch(&schema, &ids, &names);
        writer.write(batch).await.unwrap();

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 15);

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.row_group(0).num_rows(), 15);
    }

    #[tokio::test]
    async fn test_parallel_writer_batch_spans_multiple_row_groups() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig {
                max_row_group_size: 10,
                encoding_batch_size: 5,
                ..Default::default()
            },
        );

        // write a batch of 25 rows with max_row_group_size=10
        // should create 3 row groups (10, 10, 5)
        let ids: Vec<i32> = (0..25).collect();
        let names: Vec<&str> = (0..25).map(|_| "test").collect();
        let batch = create_batch(&schema, &ids, &names);
        writer.write(batch).await.unwrap();

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 25);

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.metadata();
        assert_eq!(metadata.num_row_groups(), 3);
        assert_eq!(metadata.row_group(0).num_rows(), 10);
        assert_eq!(metadata.row_group(1).num_rows(), 10);
        assert_eq!(metadata.row_group(2).num_rows(), 5);
    }

    #[tokio::test]
    async fn test_parallel_writer_mixed_batch_sizes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let pools = test_pools();

        let mut writer = ParallelParquetWriter::new(
            &path,
            &schema,
            WriterProperties::default(),
            pools,
            ParallelWriterConfig {
                max_row_group_size: 20,
                encoding_batch_size: 8,
                ..Default::default()
            },
        );

        // write mixed batch sizes: 3, 15, 2, 8, 1 = 29 rows
        // should create 2 row groups (20, 9)
        writer
            .write(create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]))
            .await
            .unwrap();
        let ids: Vec<i32> = (10..25).collect();
        let names: Vec<&str> = (0..15).map(|_| "x").collect();
        writer
            .write(create_batch(&schema, &ids, &names))
            .await
            .unwrap();
        writer
            .write(create_batch(&schema, &[30, 31], &["y", "z"]))
            .await
            .unwrap();
        let ids: Vec<i32> = (40..48).collect();
        let names: Vec<&str> = (0..8).map(|_| "w").collect();
        writer
            .write(create_batch(&schema, &ids, &names))
            .await
            .unwrap();
        writer
            .write(create_batch(&schema, &[50], &["last"]))
            .await
            .unwrap();

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 29);

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.metadata();
        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).num_rows(), 20);
        assert_eq!(metadata.row_group(1).num_rows(), 9);
    }
}
