//! Stream-based parallel parquet writer using tokio-par-util.
//!
//! Simple implementation: coalesce batches, encode columns in parallel via
//! spawn_blocking, write row groups sequentially.

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
        arrow_writer::{ArrowColumnChunk, ArrowLeafColumn, compute_leaves},
    },
    file::properties::WriterProperties,
};
use tokio::sync::mpsc;
use tokio_par_util::StreamParExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

const WRITER_BUFFER_SIZE: usize = 32 * 1024 * 1024;

/// Encode a single row group with columns processed in parallel.
async fn encode_row_group(
    schema: SchemaRef,
    props: WriterProperties,
    row_group_index: usize,
    batch: RecordBatch,
) -> Result<EncodedRowGroup> {
    let num_rows = batch.num_rows();
    let buffer = Vec::new();
    let arrow_writer = ArrowWriter::try_new(buffer, Arc::clone(&schema), Some(props.clone()))?;
    let (_, row_group_factory) = arrow_writer.into_serialized_writer()?;
    let column_writers = row_group_factory.create_column_writers(row_group_index)?;

    let num_leaf_columns = column_writers.len();
    let mut leaves_per_column: Vec<Vec<ArrowLeafColumn>> =
        (0..num_leaf_columns).map(|_| Vec::new()).collect();

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

/// Simple stream-based parallel parquet writer.
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

            let arrow_writer =
                ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.clone()))?;
            let (file_writer, _) = arrow_writer.into_serialized_writer()?;
            let file_writer = Arc::new(Mutex::new(Some(file_writer)));

            let cancel_on_err = task_token.clone();

            let result = ReceiverStream::new(batch_rx)
                .enumerate()
                .map(|(idx, batch)| {
                    let schema = Arc::clone(&schema);
                    let props = props.clone();
                    async move { encode_row_group(schema, props, idx, batch).await }
                })
                .parallel_buffered_with_token(concurrency, task_token.clone())
                .and_then(|encoded| {
                    let writer = Arc::clone(&file_writer);
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<usize> {
                            let mut guard = writer.lock().unwrap();
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
                let mut guard = file_writer.lock().unwrap();
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
                .map_err(|_| anyhow!("writer task died"))?;
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
                    .map_err(|_| anyhow!("writer task died"))?;
            }
        }

        self.writer_handle
            .take()
            .expect("writer handle missing")
            .await
            .map_err(|e| anyhow!("writer task panicked: {e}"))?
    }

    pub async fn cancel(mut self) -> Result<u64> {
        self.cancel_token.cancel();
        self.batch_sender.take();

        self.writer_handle
            .take()
            .expect("writer handle missing")
            .await
            .map_err(|e| anyhow!("writer task panicked: {e}"))?
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
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
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
    }
}
