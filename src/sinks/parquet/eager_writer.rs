//! Eager parallel parquet writer that starts encoding before it has a whole row group.

use std::{
    fmt::{self, Debug, Formatter},
    fs::File,
    io::BufWriter,
    path::Path,
    sync::Arc,
};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, compute::BatchCoalescer, datatypes::SchemaRef};
use futures::{Stream, StreamExt};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_writer::{
            ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory,
            ArrowWriterOptions, compute_leaves,
        },
    },
    file::properties::WriterProperties,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::utils::{
    ordered_channel::{OrderedReceiver, block_on},
    ordered_demux::{OrderedDemuxConfig, OrderedDemuxExt, Partitioned},
};

// Default minimum rows to accumulate before dispatching to column threads.
// Larger values reduce coordination overhead but delay encoding start.
pub const DEFAULT_MIN_DISPATCH_ROWS: usize = 32768;

// Get min dispatch rows from env var or default. I got sick of waiting
// for recompiles just to change this value and it wasn't worth a cli
// parameter
pub fn min_dispatch_rows() -> usize {
    std::env::var("SILK_MIN_DISPATCH_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MIN_DISPATCH_ROWS)
        .max(1)
}

/// Batch with row group and batch indices for ordered processing.
struct SplitBatch {
    row_group_index: usize,
    batch_index: usize,
    batch: RecordBatch,
    is_last: bool,
}

impl Partitioned for SplitBatch {
    type Key = usize;

    fn partition_key(&self) -> Self::Key {
        self.row_group_index
    }

    fn sequence_index(&self) -> usize {
        self.batch_index
    }

    fn is_last(&self) -> bool {
        self.is_last
    }
}

/// Split a stream of batches at row group boundaries
fn split_into_row_groups(
    mut inner: impl Stream<Item = RecordBatch> + Unpin,
    max_row_group_size: usize,
    schema: SchemaRef,
) -> impl Stream<Item = SplitBatch> {
    // Still not sure how I feel about `async_stream` but it's really nice to not have to write
    // `poll_next` with all the possible states and edge cases.
    async_stream::stream! {
        let mut rg_idx = 0;
        let mut batch_idx = 0;
        let mut buffered = 0;
        let mut pending: Option<RecordBatch> = None;

        loop {
            let batch = if let Some(b) = pending.take() {
                b
            } else {
                match inner.next().await {
                    Some(b) if b.num_rows() > 0 => b,
                    // technically shouldn't happen but if we change stuff later it's a free optimization
                    Some(_) => continue,
                    None => break,
                }
            };

            let rows = batch.num_rows();
            let space_remaining_in_row_group = max_row_group_size - buffered;

            if rows <= space_remaining_in_row_group {
                // fits into the current row group
                buffered += rows;
                let is_last = buffered == max_row_group_size;
                yield SplitBatch { row_group_index: rg_idx, batch_index: batch_idx, batch, is_last };
                if is_last {
                    // fits *perfectly* into the current row group, so we need to start a new row group
                    rg_idx += 1;
                    batch_idx = 0;
                    buffered = 0;
                } else {
                    batch_idx += 1;
                }
            } else {
                // doesn't fit into the current row group so we need to split the batch
                // the remaining part will be processed at the start of the next loop
                pending = Some(batch.slice(space_remaining_in_row_group, rows - space_remaining_in_row_group));
                yield SplitBatch {
                    row_group_index: rg_idx,
                    batch_index: batch_idx,
                    batch: batch.slice(0, space_remaining_in_row_group),
                    is_last: true,
                };
                rg_idx += 1;
                batch_idx = 0;
                buffered = 0;
            }
        }

        if buffered > 0 {
            // send through leftover data as a final batch
            yield SplitBatch {
                row_group_index: rg_idx,
                batch_index: batch_idx,
                batch: RecordBatch::new_empty(schema),
                is_last: true,
            };
        }
    }
}

/// Encoded row group ready to write to file.
struct EncodedRowGroup {
    row_group_index: usize,
    chunks: Vec<ArrowColumnChunk>,
    num_rows: usize,
}

impl Debug for EncodedRowGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EncodedRowGroup {{ row_group_index: {}, num_rows: {} }}",
            self.row_group_index, self.num_rows
        )
    }
}

/// Encode a row group from a stream of batches.
/// Called by OrderedDemux for each partition.
async fn encode_row_group(
    row_group_index: usize,
    schema: SchemaRef,
    factory: Arc<ArrowRowGroupWriterFactory>,
    batch_rx: OrderedReceiver<SplitBatch>,
    cancel: CancellationToken,
    min_dispatch_rows: usize,
) -> Result<EncodedRowGroup> {
    tokio::task::spawn_blocking(move || {
        run_encoder(
            row_group_index,
            &schema,
            &factory,
            batch_rx,
            &cancel,
            min_dispatch_rows,
        )
    })
    .await
    .map_err(|e| anyhow!("encoder task panicked: {e}"))?
}

/// Blocking encoder that processes batches and produces encoded chunks.
fn run_encoder(
    row_group_index: usize,
    schema: &SchemaRef,
    factory: &Arc<ArrowRowGroupWriterFactory>,
    mut batch_rx: OrderedReceiver<SplitBatch>,
    cancel: &CancellationToken,
    min_dispatch_rows: usize,
) -> Result<EncodedRowGroup> {
    let column_writers = factory.create_column_writers(row_group_index)?;

    let (col_txs, col_handles): (Vec<_>, Vec<_>) = column_writers
        .into_iter()
        .map(|writer| {
            let (tx, rx) = std::sync::mpsc::channel::<Option<ArrowLeafColumn>>();
            let handle = tokio::task::spawn_blocking(move || run_column_thread(writer, &rx));
            (tx, handle)
        })
        .unzip();

    let mut total_rows = 0;
    let mut coalescer = BatchCoalescer::new(Arc::clone(schema), min_dispatch_rows);

    let dispatch_batch = |batch: RecordBatch,
                          col_txs: &[std::sync::mpsc::Sender<Option<ArrowLeafColumn>>],
                          schema: &SchemaRef|
     -> Result<()> {
        let mut col_idx = 0;
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            for leaf in compute_leaves(field.as_ref(), column)? {
                col_txs[col_idx]
                    .send(Some(leaf))
                    .map_err(|_| anyhow!("column thread closed unexpectedly"))?;
                col_idx += 1;
            }
        }
        Ok(())
    };

    loop {
        let split = match batch_rx.blocking_recv_cancellable(cancel) {
            Ok(split) => split,
            Err(_) => break,
        };

        if split.batch.num_rows() == 0 {
            break;
        }

        total_rows += split.batch.num_rows();
        coalescer.push_batch(split.batch)?;

        while let Some(coalesced) = coalescer.next_completed_batch() {
            dispatch_batch(coalesced, &col_txs, schema)?;
        }
    }

    coalescer.finish_buffered_batch()?;
    while let Some(coalesced) = coalescer.next_completed_batch() {
        dispatch_batch(coalesced, &col_txs, schema)?;
    }

    for tx in &col_txs {
        let _ = tx.send(None);
    }
    drop(col_txs);

    let mut chunks = Vec::with_capacity(col_handles.len());
    for handle in col_handles {
        let chunk = block_on(handle).map_err(|e| anyhow!("column thread panicked: {e}"))??;
        chunks.push(chunk);
    }

    Ok(EncodedRowGroup {
        row_group_index,
        chunks,
        num_rows: total_rows,
    })
}

fn run_column_thread(
    mut writer: ArrowColumnWriter,
    rx: &std::sync::mpsc::Receiver<Option<ArrowLeafColumn>>,
) -> Result<ArrowColumnChunk> {
    while let Ok(Some(leaf)) = rx.recv() {
        writer.write(&leaf)?;
    }
    writer.close().map_err(|e| anyhow!(e))
}

/// Eager parallel parquet writer that starts encoding immediately.
///
/// # Usage
///
/// ```ignore
/// let mut writer = EagerParquetWriter::new(&path, &schema, props, row_group_size, buffer_size, concurrency, min_dispatch_rows);
/// for batch in batches {
///     writer.write(batch).await?;
/// }
/// let rows_written = writer.close().await?;
/// ```
pub struct EagerParquetWriter {
    batch_sender: Option<mpsc::Sender<RecordBatch>>,
    cancel_token: CancellationToken,
    writer_handle: Option<tokio::task::JoinHandle<Result<u64>>>,
}

impl EagerParquetWriter {
    // Panics if `max_row_group_size` or `concurrency` is 0.
    pub fn new(
        path: impl AsRef<Path>,
        schema: &SchemaRef,
        props: WriterProperties,
        max_row_group_size: usize,
        buffer_size: usize,
        concurrency: usize,
        min_dispatch_rows: usize,
    ) -> Self {
        assert!(max_row_group_size > 0, "max_row_group_size must be > 0");
        assert!(concurrency > 0, "concurrency must be > 0");
        let path = path.as_ref().to_path_buf();
        let schema = Arc::clone(schema);
        let cancel_token = CancellationToken::new();
        let task_cancel = cancel_token.clone();

        let (batch_tx, batch_rx) = mpsc::channel::<RecordBatch>(concurrency);

        let writer_handle = tokio::spawn(async move {
            Self::background_task(
                path,
                schema,
                props,
                max_row_group_size,
                buffer_size,
                concurrency,
                min_dispatch_rows,
                batch_rx,
                task_cancel,
            )
            .await
        });

        Self {
            batch_sender: Some(batch_tx),
            cancel_token,
            writer_handle: Some(writer_handle),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn background_task(
        path: std::path::PathBuf,
        schema: SchemaRef,
        props: WriterProperties,
        max_row_group_size: usize,
        buffer_size: usize,
        concurrency: usize,
        min_dispatch_rows: usize,
        batch_rx: mpsc::Receiver<RecordBatch>,
        cancel: CancellationToken,
    ) -> Result<u64> {
        // we want this to be a non-blocking function in general but the writers are all synchronous so we need to block
        // every time we touch them
        let file = tokio::task::spawn_blocking({
            let path = path.clone();
            move || -> Result<_> { Ok(BufWriter::with_capacity(buffer_size, File::create(&path)?)) }
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

        let batch_stream = ReceiverStream::new(batch_rx);
        let splitter = split_into_row_groups(batch_stream, max_row_group_size, Arc::clone(&schema));

        let demux_config = OrderedDemuxConfig {
            max_concurrent: concurrency,
            worker_buffer_size: 16,
            output_buffer_size: concurrency,
        };

        let encoded_stream = splitter.ordered_demux(
            demux_config,
            cancel.clone(),
            move |rg_idx, batch_rx, worker_cancel| {
                let schema = Arc::clone(&schema);
                let factory = Arc::clone(&row_group_factory);
                async move {
                    encode_row_group(
                        rg_idx,
                        schema,
                        factory,
                        batch_rx,
                        worker_cancel,
                        min_dispatch_rows,
                    )
                    .await
                }
            },
        );

        let mut file_writer = file_writer;
        let mut total_rows = 0u64;

        futures::pin_mut!(encoded_stream);

        while let Some(result) = encoded_stream.next().await {
            let encoded = result?;

            let mut rg = file_writer.next_row_group()?;
            for chunk in encoded.chunks {
                chunk.append_to_row_group(&mut rg)?;
            }
            rg.close()?;

            total_rows += encoded.num_rows as u64;
        }

        file_writer.close()?;

        Ok(total_rows)
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let sender = self
            .batch_sender
            .as_ref()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        if self.writer_handle.as_ref().is_some_and(|h| h.is_finished()) {
            // not a perfect check but it's better than nothing
            return Err(anyhow!("writer task failed"));
        }

        sender
            .send(batch)
            .await
            .map_err(|_| anyhow!("writer task failed"))
    }

    pub fn blocking_write(&mut self, batch: RecordBatch) -> Result<()> {
        tokio::task::block_in_place(|| {
            let current_runtime = tokio::runtime::Handle::current();
            current_runtime.block_on(self.write(batch))
        })
    }

    pub async fn close(&mut self) -> Result<u64> {
        self.batch_sender.take();

        match self.writer_handle.take() {
            Some(handle) => handle
                .await
                .map_err(|e| anyhow!("writer task panicked: {e}"))?,
            None => Err(anyhow!("writer already closed")),
        }
    }

    pub fn blocking_close(&mut self) -> Result<u64> {
        tokio::task::block_in_place(|| {
            let current_runtime = tokio::runtime::Handle::current();
            current_runtime.block_on(self.close())
        })
    }

    pub async fn cancel(&mut self) -> Result<u64> {
        self.cancel_token.cancel();
        self.batch_sender.take();

        match self.writer_handle.take() {
            Some(handle) => handle
                .await
                .map_err(|e| anyhow!("writer task panicked: {e}"))?,
            None => Err(anyhow!("writer already closed")),
        }
    }

    pub fn blocking_cancel(&mut self) -> Result<u64> {
        tokio::task::block_in_place(|| {
            let current_runtime = tokio::runtime::Handle::current();
            current_runtime.block_on(self.cancel())
        })
    }
}

impl Drop for EagerParquetWriter {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.batch_sender.take();

        if let Some(handle) = self.writer_handle.take() {
            // we can't await in drop and this implies shutdown so we just abort
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn simple_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Utf8, true),
        ]))
    }

    fn create_batch(schema: &SchemaRef, ids: Vec<i32>, values: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(ids)),
                Arc::new(arrow::array::StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_eager_writer_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = EagerParquetWriter::new(&path, &schema, props, 1000, 8192, 2, 100);

        let batch = create_batch(&schema, vec![1, 2, 3], vec![Some("a"), None, Some("c")]);
        writer.write(batch).await.unwrap();

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_eager_writer_multiple_row_groups() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_rg.parquet");
        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer = EagerParquetWriter::new(&path, &schema, props, 5, 8192, 2, 2);

        for i in 0..3 {
            let start = i * 5;
            let batch = create_batch(&schema, (start..start + 5).collect(), vec![Some("x"); 5]);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 15);

        let file = std::fs::File::open(&path).unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 3);
    }

    #[tokio::test]
    async fn test_row_group_splitter() {
        let schema = simple_schema();
        let batches = vec![
            create_batch(&schema, vec![1, 2], vec![Some("a"), Some("b")]),
            create_batch(&schema, vec![3, 4], vec![Some("c"), Some("d")]),
            create_batch(&schema, vec![5], vec![Some("e")]),
        ];

        let stream = futures::stream::iter(batches);
        let mut splitter = std::pin::pin!(split_into_row_groups(stream, 3, schema));

        let mut splits = vec![];
        while let Some(split) = splitter.next().await {
            splits.push((
                split.row_group_index,
                split.batch_index,
                split.batch.num_rows(),
                split.is_last,
            ));
        }

        // first 2 rows -> rg 0, batch 0
        // split batch2: 1 row closes rg 0, 1 row starts rg 1
        // batch3 1 row -> rg 1, batch 1
        // final empty -> rg 1, batch 2 (closes rg 1)
        assert_eq!(splits.len(), 5);
        assert_eq!(splits[0], (0, 0, 2, false)); // 2 rows in rg 0
        assert_eq!(splits[1], (0, 1, 1, true)); // 1 row, closes rg 0
        assert_eq!(splits[2], (1, 0, 1, false)); // 1 row in rg 1 (from split)
        assert_eq!(splits[3], (1, 1, 1, false)); // 1 row in rg 1 (batch3)
        assert_eq!(splits[4], (1, 2, 0, true)); // empty, closes rg 1
    }
}
