//! Sequential parquet writer with async API.
//!
//! Wraps ArrowWriter with an async interface matching ParallelParquetWriter.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::file::properties::WriterProperties;

use crate::sinks::parquet::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ROW_GROUP_SIZE};

use super::ParquetWriter;

/// Configuration for the sequential parquet writer.
#[derive(Debug, Clone, Copy)]
pub struct SequentialWriterConfig {
    pub max_row_group_size: usize,
    pub buffer_size: usize,
}

impl Default for SequentialWriterConfig {
    fn default() -> Self {
        Self {
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

/// Sequential parquet writer with async API.
///
/// Uses ArrowWriter internally. The async methods are thin wrappers since
/// ArrowWriter operations are synchronous but fast per-call.
pub struct SequentialParquetWriter {
    writer: Option<ArrowWriter<BufWriter<File>>>,
    rows_written: u64,
}

impl SequentialParquetWriter {
    pub fn new(
        path: impl AsRef<Path>,
        schema: &SchemaRef,
        props: WriterProperties,
        config: SequentialWriterConfig,
    ) -> Result<Self> {
        let file = File::create(path.as_ref())?;
        let buf_writer = BufWriter::with_capacity(config.buffer_size, file);
        let writer = ArrowWriter::try_new_with_options(
            buf_writer,
            Arc::clone(schema),
            ArrowWriterOptions::new().with_properties(props),
        )?;

        Ok(Self {
            writer: Some(writer),
            rows_written: 0,
        })
    }

    #[allow(clippy::needless_pass_by_value)] // match ParallelParquetWriter API
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.blocking_write(batch)
    }

    #[allow(clippy::needless_pass_by_value)] // match ParallelParquetWriter API
    pub fn blocking_write(&mut self, batch: RecordBatch) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        self.rows_written += batch.num_rows() as u64;
        writer.write(&batch)?;
        Ok(())
    }

    pub async fn close(self) -> Result<u64> {
        self.blocking_close()
    }

    pub fn blocking_close(mut self) -> Result<u64> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| anyhow!("writer already closed"))?;
        writer.close()?;
        Ok(self.rows_written)
    }

    pub async fn cancel(self) -> Result<()> {
        self.blocking_cancel()
    }

    pub fn blocking_cancel(mut self) -> Result<()> {
        self.writer.take();
        Ok(())
    }
}

impl Drop for SequentialParquetWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = writer.close();
        }
    }
}

#[async_trait]
impl ParquetWriter for SequentialParquetWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        Self::write(self, batch).await
    }

    fn blocking_write(&mut self, batch: RecordBatch) -> Result<()> {
        Self::blocking_write(self, batch)
    }

    async fn close(self: Box<Self>) -> Result<u64> {
        Self::close(*self).await
    }

    fn blocking_close(self: Box<Self>) -> Result<u64> {
        Self::blocking_close(*self)
    }

    async fn cancel(self: Box<Self>) -> Result<()> {
        Self::cancel(*self).await
    }

    fn blocking_cancel(self: Box<Self>) -> Result<()> {
        Self::blocking_cancel(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn create_batch(schema: &SchemaRef, ids: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(ids.to_vec()))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_sequential_writer_basic() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let props = WriterProperties::builder().build();

        let mut writer =
            SequentialParquetWriter::new(&path, &schema, props, SequentialWriterConfig::default())
                .unwrap();

        let batch = create_batch(&schema, &[1, 2, 3]);
        writer.write(batch).await.unwrap();

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 3);

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 3);
    }

    #[tokio::test]
    async fn test_sequential_writer_multiple_batches() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = test_schema();
        let props = WriterProperties::builder().build();

        let mut writer =
            SequentialParquetWriter::new(&path, &schema, props, SequentialWriterConfig::default())
                .unwrap();

        for i in 0..5 {
            let ids: Vec<i32> = (i * 10..(i + 1) * 10).collect();
            let batch = create_batch(&schema, &ids);
            writer.write(batch).await.unwrap();
        }

        let rows = writer.close().await.unwrap();
        assert_eq!(rows, 50);

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 50);
    }
}
