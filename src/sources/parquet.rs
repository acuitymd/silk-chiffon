use std::{fs::File, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    prelude::{ParquetReadOptions, SessionContext},
};
use parquet::file::reader::{FileReader as _, SerializedFileReader};
use uuid::Uuid;

use crate::sources::data_source::{DataSource, Replayability, RowCount, RowCountCapability};

/// A replayable Parquet input backed by a command-owned DataFusion session.
pub struct ParquetDataSource {
    path: String,
    session: SessionContext,
}

impl ParquetDataSource {
    /// Creates a source for one Parquet file.
    pub fn new(path: String, session: SessionContext) -> Self {
        Self { path, session }
    }
}

#[async_trait]
impl DataSource for ParquetDataSource {
    fn name(&self) -> &str {
        "parquet"
    }

    fn replayability(&self) -> Replayability {
        Replayability::Replayable
    }

    fn row_count_capability(&self) -> Option<&dyn RowCountCapability> {
        Some(self)
    }

    async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let table_name = format!("parquet_{}", Uuid::new_v4().as_simple());
        self.session
            .register_parquet(&table_name, &self.path, ParquetReadOptions::default())
            .await?;
        let table = self.session.table(&table_name).await?;
        Ok(table.into_view())
    }
}

#[async_trait]
impl RowCountCapability for ParquetDataSource {
    #[allow(clippy::cast_sign_loss)]
    async fn row_count(&self) -> Result<RowCount> {
        let file = File::open(&self.path)?;
        let reader = SerializedFileReader::new(file)?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(RowCount::Exact(
            reader.metadata().file_metadata().num_rows() as u64,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PARQUET_PATH: &str = "tests/files/people.parquet";

    fn test_source(path: impl Into<String>) -> ParquetDataSource {
        ParquetDataSource::new(path.into(), SessionContext::new())
    }

    #[tokio::test]
    async fn test_table_provider() {
        let source = test_source(TEST_PARQUET_PATH);
        let table_provider = source.table_provider().await.unwrap();
        assert!(!table_provider.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_table_provider_can_be_queried() {
        let source = test_source(TEST_PARQUET_PATH);
        let table_provider = source.table_provider().await.unwrap();
        let ctx = &source.session;

        ctx.register_table("test_table", table_provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_table LIMIT 1").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let batch = batches[0].clone();
        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_row_count() {
        let source = test_source(TEST_PARQUET_PATH);
        let count = source.row_count().await.unwrap();

        // verify against actually streaming all rows
        let provider = source.table_provider().await.unwrap();
        let streamed = source
            .session
            .read_table(provider)
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>();
        assert_eq!(count, RowCount::Exact(streamed as u64));
    }

    #[tokio::test]
    async fn test_row_count_written_file() {
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let batch = TestBatch::builder()
            .column_i32("id", &[1, 2, 3, 4, 5])
            .column_string("name", &["a", "b", "c", "d", "e"])
            .build();
        TestFile::write_parquet_batch(&path, &batch);

        let source = test_source(path.to_string_lossy());
        assert_eq!(source.row_count().await.unwrap(), RowCount::Exact(5));
    }
}
