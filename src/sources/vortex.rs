use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use vortex::VortexSessionDefault;
use vortex::arrow::ToArrowType;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex_datafusion::v2::VortexTable;

use crate::sources::data_source::{DataSource, Replayability, RowCount};

pub struct VortexDataSource {
    path: String,
}

impl VortexDataSource {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

#[async_trait]
impl DataSource for VortexDataSource {
    fn name(&self) -> &str {
        "vortex"
    }

    fn replayability(&self) -> Replayability {
        Replayability::Replayable
    }

    async fn schema(&self) -> Result<SchemaRef> {
        let session = VortexSession::default();
        let vortex_file = session
            .open_options()
            .open_path(self.path.as_str())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open Vortex file: {}", e))?;

        let dtype = vortex_file.dtype();
        let arrow_schema = dtype.to_arrow_schema().map_err(|e| {
            anyhow::anyhow!("Failed to convert Vortex DType to Arrow Schema: {}", e)
        })?;

        Ok(Arc::new(arrow_schema))
    }

    async fn row_count(&self) -> Result<RowCount> {
        let session = VortexSession::default();
        let vortex_file = session
            .open_options()
            .open_path(self.path.as_str())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open Vortex file: {}", e))?;

        Ok(RowCount::Exact(vortex_file.row_count()))
    }

    async fn as_table_provider(&self, _ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        let session = VortexSession::default().with_tokio();
        let vortex_file = session
            .open_options()
            .open_path(self.path.as_str())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open Vortex file: {}", e))?;
        let arrow_schema = vortex_file.dtype().to_arrow_schema().map_err(|e| {
            anyhow::anyhow!("Failed to convert Vortex DType to Arrow Schema: {}", e)
        })?;
        let data_source = vortex_file.data_source()?;

        Ok(Arc::new(VortexTable::new(
            data_source,
            session,
            Arc::new(arrow_schema),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use tempfile::TempDir;

    use super::*;
    use crate::sinks::data_sink::DataSink;
    use crate::sinks::vortex::{VortexSink, VortexSinkOptions};

    async fn write_vortex_file(path: &Path, schema: &SchemaRef, batch: RecordBatch) {
        let mut sink =
            VortexSink::create(path.to_path_buf(), schema, VortexSinkOptions::new()).unwrap();
        sink.write_batch(batch).await.unwrap();
        sink.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_row_count() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.vortex");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();
        write_vortex_file(&path, &schema, batch).await;

        let source = VortexDataSource::new(path.to_string_lossy().to_string());
        let count = source.row_count().await.unwrap();
        assert_eq!(count, RowCount::Exact(5));
    }
}
