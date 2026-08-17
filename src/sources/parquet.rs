use std::{fs::File, sync::Arc};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    prelude::{ParquetReadOptions, SessionContext},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader as _, SerializedFileReader};
use uuid::Uuid;

use crate::sources::data_source::DataSource;

#[derive(Debug)]
pub struct ParquetDataSource {
    path: String,
}

impl ParquetDataSource {
    pub fn new(path: String) -> Self {
        Self { path }
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

    #[allow(clippy::cast_sign_loss)]
    fn row_count(&self) -> Result<usize> {
        let file = File::open(&self.path)?;
        let reader = SerializedFileReader::new(file)?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(reader.metadata().file_metadata().num_rows() as usize)
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

    #[tokio::test]
    async fn test_row_count() {
        let source = ParquetDataSource::new(TEST_PARQUET_PATH.to_string());
        let count = source.row_count().unwrap();

        // verify against actually streaming all rows
        let mut stream = source.as_stream().await.unwrap();
        let mut streamed = 0;
        while let Some(batch) = stream.next().await {
            streamed += batch.unwrap().num_rows();
        }
        assert_eq!(count, streamed);
    }

    #[test]
    fn test_row_count_written_file() {
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let batch = TestBatch::builder()
            .column_i32("id", &[1, 2, 3, 4, 5])
            .column_string("name", &["a", "b", "c", "d", "e"])
            .build();
        TestFile::write_parquet_batch(&path, &batch);

        let source = ParquetDataSource::new(path.to_string_lossy().to_string());
        assert_eq!(source.row_count().unwrap(), 5);
    }
}
