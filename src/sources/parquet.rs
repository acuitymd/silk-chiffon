//! Parquet data source for reading Apache Parquet files.

use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    prelude::{ParquetReadOptions, SessionContext},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

use crate::sources::data_source::DataSource;

pub struct ParquetDataSource {
    path: String,
    variable_col_cache: Mutex<HashMap<String, usize>>,
}

impl std::fmt::Debug for ParquetDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetDataSource")
            .field("path", &self.path)
            .finish()
    }
}

impl ParquetDataSource {
    pub fn new(path: String) -> Self {
        Self {
            path,
            variable_col_cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl DataSource for ParquetDataSource {
    fn name(&self) -> &str {
        "parquet"
    }

    fn schema(&self) -> Result<SchemaRef> {
        let file = File::open(&self.path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
        Ok(Arc::clone(reader.schema()))
    }

    fn row_count(&self) -> Result<usize> {
        let file = File::open(&self.path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = reader.metadata();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let count = metadata.file_metadata().num_rows() as usize;
        Ok(count)
    }

    fn variable_column_size_cache(&self) -> Option<&Mutex<HashMap<String, usize>>> {
        Some(&self.variable_col_cache)
    }

    async fn as_table_provider(&self, ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        let table_name = format!("parquet_{}", Uuid::new_v4().as_simple());
        ctx.register_parquet(&table_name, &self.path, ParquetReadOptions::default())
            .await?;
        let table = ctx.table(&table_name).await?;
        Ok(table.into_view())
    }

    fn supports_table_provider(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    const TEST_PARQUET_PATH: &str = "tests/files/people.parquet";

    #[test]
    fn test_new() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        assert_eq!(source.path, TEST_PARQUET_PATH);
    }

    #[test]
    fn test_name() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        assert_eq!(source.name(), "parquet");
    }

    #[test]
    fn test_row_count() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        let count = source.row_count().unwrap();
        assert!(count > 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_row_size() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        let size = source.row_size().unwrap();
        assert!(size > 0);
    }

    #[tokio::test]
    async fn test_as_table_provider() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        let mut ctx = SessionContext::new();
        let table_provider = source.as_table_provider(&mut ctx).await.unwrap();
        assert!(!table_provider.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_as_table_provider_can_be_queried() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        let mut ctx = SessionContext::new();
        let table_provider = source.as_table_provider(&mut ctx).await.unwrap();

        ctx.register_table("test_table", table_provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_table LIMIT 1").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let batch = batches[0].clone();
        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_as_stream() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        let mut stream = source.as_stream().await.unwrap();

        assert!(!stream.schema().fields().is_empty());
        let batch = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(batch.num_rows() > 0);
    }
}
