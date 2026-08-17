//! Trait for parquet writers with async API.

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;

/// Trait for parquet writers supporting both async and blocking operations.
///
/// Implemented by both `SequentialParquetWriter` and `ParallelParquetWriter`.
#[async_trait]
pub trait ParquetWriter: Send {
    /// Write a batch of records.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;

    /// Write a batch of records (blocking version).
    fn blocking_write(&mut self, batch: RecordBatch) -> Result<()>;

    /// Close the writer and return the number of rows written.
    ///
    /// Takes `Box<Self>` to support trait objects.
    async fn close(self: Box<Self>) -> Result<u64>;

    /// Close the writer (blocking version).
    fn blocking_close(self: Box<Self>) -> Result<u64>;

    /// Cancel writing and discard partial output.
    async fn cancel(self: Box<Self>) -> Result<()>;

    /// Cancel writing (blocking version).
    fn blocking_cancel(self: Box<Self>) -> Result<()>;
}
