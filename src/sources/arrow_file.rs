use std::{
    fs::File,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use arrow::{datatypes::SchemaRef, ipc::reader::FileReader};
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, execution::options::ArrowReadOptions, prelude::SessionContext,
};
use uuid::Uuid;

use crate::sources::data_source::DataSource;

#[derive(Debug)]
pub struct ArrowFileDataSource {
    path: String,
    schema: Mutex<Option<SchemaRef>>,
}

impl ArrowFileDataSource {
    pub fn new(path: String) -> Self {
        Self {
            path,
            schema: Mutex::new(None),
        }
    }
}

#[async_trait]
impl DataSource for ArrowFileDataSource {
    fn name(&self) -> &str {
        "arrow_file"
    }

    fn schema(&self) -> Result<SchemaRef> {
        let mut guard = self
            .schema
            .lock()
            .map_err(|e| anyhow!("Failed to lock schema: {}", e))?;
        if let Some(ref schema) = *guard {
            return Ok(Arc::clone(schema));
        }
        let file = File::open(&self.path)?;
        let reader = FileReader::try_new(file, None)?;
        *guard = Some(reader.schema());
        Ok(reader.schema())
    }

    async fn as_table_provider(&self, ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        let table_name = format!("arrow_file_{}", Uuid::new_v4().as_simple());
        ctx.register_arrow(&table_name, &self.path, ArrowReadOptions::default())
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
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    use super::*;

    const TEST_ARROW_FILE_PATH: &str = "tests/files/people.file.arrow";

    #[test]
    fn test_new() {
        let source = ArrowFileDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        assert_eq!(source.path, TEST_ARROW_FILE_PATH);
    }

    #[test]
    fn test_name() {
        let source = ArrowFileDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        assert_eq!(source.name(), "arrow_file");
    }

    #[tokio::test]
    async fn test_as_table_provider() {
        let source = ArrowFileDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        let mut ctx = SessionContext::new();
        let table_provider = source.as_table_provider(&mut ctx).await.unwrap();
        assert!(!table_provider.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_as_table_provider_can_be_queried() {
        let source = ArrowFileDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        let mut ctx = SessionContext::new();
        let table_provider = source.as_table_provider(&mut ctx).await.unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", table_provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_table LIMIT 1").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let batch = batches[0].clone();
        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_as_stream() {
        let source = ArrowFileDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        let mut stream = source.as_stream().await.unwrap();

        assert!(!stream.schema().fields().is_empty());
        let batch = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(batch.num_rows() > 0);
    }
}
