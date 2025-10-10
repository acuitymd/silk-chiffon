use ::duckdb::Connection;
use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;
use pg_escape::quote_identifier;
use std::{
    pin::Pin,
    task::{Context, Poll},
    thread,
};
use tokio::sync::mpsc;

use crate::sources::data_source::DataSource;

#[derive(Debug)]
pub struct DuckDBDataSource {
    path: String,
    table_name: String,
}

impl DuckDBDataSource {
    pub fn new(path: String, table_name: String) -> Self {
        Self { path, table_name }
    }
}

struct DuckDBChannelStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
}

impl DuckDBChannelStream {
    fn new(
        schema: SchemaRef,
        receiver: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
    ) -> Self {
        Self { schema, receiver }
    }
}

impl Stream for DuckDBChannelStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl RecordBatchStream for DuckDBChannelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl DataSource for DuckDBDataSource {
    fn name(&self) -> &str {
        "duckdb"
    }

    async fn as_stream(&self) -> Result<SendableRecordBatchStream> {
        let path = self.path.clone();
        let table_name = self.table_name.clone();

        let conn = Connection::open(&self.path).unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT * FROM {} LIMIT 0",
            quote_identifier(&self.table_name)
        ))?;
        let result = stmt.query_arrow([])?;
        let schema = result.get_schema();
        let returned_schema = schema.clone();
        drop(stmt);
        drop(conn);

        let (tx, rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(32);

        thread::spawn(move || {
            let result = (|| -> Result<()> {
                let conn = Connection::open(&path).unwrap();
                let mut stmt =
                    conn.prepare(&format!("SELECT * FROM {}", quote_identifier(&table_name)))?;

                let mut stream = stmt.stream_arrow([], schema)?;

                while let Some(batch) = stream.next() {
                    if tx.blocking_send(Ok(batch)).is_err() {
                        break; // rx was dropped
                    }
                }

                Ok(())
            })();

            if let Err(e) = result {
                tx.blocking_send(Err(DataFusionError::Execution(e.to_string())))
                    .unwrap();
            }
        });

        Ok(Box::pin(DuckDBChannelStream::new(returned_schema, rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    const TEST_DUCKDB_PATH: &str = "tests/files/people.duckdb";
    const TEST_DUCKDB_TABLE_NAME: &str = "people";

    #[test]
    fn test_new() {
        let source = DuckDBDataSource::new(
            TEST_DUCKDB_PATH.to_string(),
            TEST_DUCKDB_TABLE_NAME.to_string(),
        );
        assert_eq!(source.path, TEST_DUCKDB_PATH);
    }

    #[test]
    fn test_name() {
        let source = DuckDBDataSource::new(
            TEST_DUCKDB_PATH.to_string(),
            TEST_DUCKDB_TABLE_NAME.to_string(),
        );
        assert_eq!(source.name(), "duckdb");
    }

    #[tokio::test]
    async fn test_as_table_provider() {
        let source = DuckDBDataSource::new(
            TEST_DUCKDB_PATH.to_string(),
            TEST_DUCKDB_TABLE_NAME.to_string(),
        );
        let table_provider = source.as_table_provider().await;
        assert!(matches!(table_provider, Err(_)));
    }

    #[tokio::test]
    async fn test_as_stream() {
        let source = DuckDBDataSource::new(
            TEST_DUCKDB_PATH.to_string(),
            TEST_DUCKDB_TABLE_NAME.to_string(),
        );
        let mut stream = source.as_stream().await.unwrap();

        assert!(stream.schema().fields().len() > 0);
        let batch = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(batch.num_rows() > 0);
    }
}
