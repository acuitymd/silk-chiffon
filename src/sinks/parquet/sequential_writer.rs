//! Sequential parquet writer using parquet's built-in ArrowWriter.
//!
//! This is the simplest writer - all encoding happens synchronously on the calling thread.
//! Use this as a baseline for benchmarking parallel implementations.

use std::{fs::File, io::BufWriter, path::Path, sync::Arc};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

/// Sequential parquet writer that encodes on the calling thread.
#[allow(dead_code)]
pub struct SequentialParquetWriter {
    writer: Option<ArrowWriter<BufWriter<File>>>,
    rows_written: u64,
}

#[allow(dead_code)]
impl SequentialParquetWriter {
    pub fn new(
        path: impl AsRef<Path>,
        schema: &SchemaRef,
        props: WriterProperties,
        buffer_size: usize,
    ) -> Result<Self> {
        let file = BufWriter::with_capacity(buffer_size, File::create(path.as_ref())?);
        let writer = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))?;

        Ok(Self {
            writer: Some(writer),
            rows_written: 0,
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        self.rows_written += batch.num_rows() as u64;
        writer.write(batch)?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<u64> {
        match self.writer.take() {
            Some(writer) => {
                writer.close()?;
                Ok(self.rows_written)
            }
            None => Err(anyhow!("writer already closed")),
        }
    }

    // async versions for API compatibility
    pub async fn async_write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write(batch)
    }

    pub async fn async_close(&mut self) -> Result<u64> {
        self.close()
    }
}

impl Drop for SequentialParquetWriter {
    fn drop(&mut self) {
        // try to close cleanly if not already closed
        if let Some(writer) = self.writer.take() {
            let _ = writer.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sinks::parquet::DEFAULT_BUFFER_SIZE;

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

    #[test]
    fn test_sequential_writer_basic() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();

        let mut writer =
            SequentialParquetWriter::new(&path, &schema, props, DEFAULT_BUFFER_SIZE).unwrap();

        for i in 0..3 {
            let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
            let names: Vec<String> = ids.iter().map(|x| format!("name_{}", x)).collect();
            let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let batch = create_batch(&schema, &ids, &names_ref);
            writer.write(&batch).unwrap();
        }

        let rows = writer.close().unwrap();
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

    #[test]
    fn test_sequential_writer_empty() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer =
            SequentialParquetWriter::new(&path, &schema, props, DEFAULT_BUFFER_SIZE).unwrap();

        let rows = writer.close().unwrap();
        assert_eq!(rows, 0);

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
    }

    #[test]
    fn test_sequential_writer_double_close() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let props = WriterProperties::builder().build();

        let mut writer =
            SequentialParquetWriter::new(&path, &schema, props, DEFAULT_BUFFER_SIZE).unwrap();

        let rows = writer.close().unwrap();
        assert_eq!(rows, 0);

        let result = writer.close();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already closed"));
    }
}
