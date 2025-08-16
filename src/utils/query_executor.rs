use anyhow::{Context, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::empty::EmptyTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::*;
use std::sync::Arc;

#[cfg(test)]
use arrow::array::RecordBatch;
#[cfg(test)]
use datafusion::datasource::MemTable;

pub struct QueryExecutor {
    ctx: SessionContext,
    query: String,
}

impl QueryExecutor {
    pub fn new(query: String) -> Self {
        let cfg = SessionConfig::new()
            .set_bool("datafusion.sql_parser.map_string_types_to_utf8view", false);
        let ctx = SessionContext::new_with_config(cfg);
        Self { ctx, query }
    }

    pub async fn execute_on_file(
        &self,
        file_path: &str,
        table_name: &str,
    ) -> Result<SendableRecordBatchStream> {
        self.ctx
            .register_arrow(table_name, file_path, ArrowReadOptions::default())
            .await
            .context("Failed to register Arrow file")?;

        let df = self.sql_to_dataframe(&self.query).await?;

        df.execute_stream()
            .await
            .context("Failed to execute SQL query")
    }

    async fn sql_to_dataframe(&self, query: &str) -> Result<DataFrame> {
        self.ctx
            .sql(query)
            .await
            .context("Failed to parse SQL query")
    }

    /// Executes a query on a vector of record batches. A convenience method for testing.
    #[cfg(test)]
    pub async fn execute_on_batches(
        &self,
        batches: Vec<RecordBatch>,
        table_name: &str,
    ) -> Result<Vec<RecordBatch>> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        let schema = batches[0].schema();
        let mem_table = MemTable::try_new(schema, vec![batches])?;

        self.ctx.register_table(table_name, Arc::new(mem_table))?;

        let df = self.sql_to_dataframe(&self.query).await?;
        df.collect().await.context("Failed to collect results")
    }

    /// A bit of a hack to get the schema of the result of a query. Should be extremely fast
    /// because there's no data it's executed on.
    pub async fn get_result_schema(&self, input_schema: SchemaRef) -> Result<SchemaRef> {
        let empty_table = EmptyTable::new(input_schema.clone());

        self.ctx.register_table("data", Arc::new(empty_table))?;

        let df = self.sql_to_dataframe(&self.query).await?;

        Ok(df.schema().inner().clone())
    }
}

/// Adds a sort clause to an existing query by wrapping it as a `FROM` clause.
///
/// The query passed to Silk Chiffon is separate from the sort option and so we need to represent
/// both here. We also need to strictly enforce the sort order for splitting since the algorithm
/// demands it.
pub fn build_query_with_sort(base_query: &str, sort_columns: &[(String, bool)]) -> String {
    if sort_columns.is_empty() {
        return base_query.to_string();
    }

    let order_by = sort_columns
        .iter()
        .map(|(col, ascending)| {
            if *ascending {
                format!("{col} ASC")
            } else {
                format!("{col} DESC")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!("SELECT * FROM ({base_query}) AS subquery ORDER BY {order_by}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 5, 10, 15, 20]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_simple_filter_query() {
        let query = "SELECT * FROM data WHERE id > 10";
        let executor = QueryExecutor::new(query.to_string());

        let batch = create_test_batch();
        let results = executor
            .execute_on_batches(vec![batch], "data")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        let result_batch = &results[0];
        assert_eq!(result_batch.num_rows(), 2);

        let id_column = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 15);
        assert_eq!(id_column.value(1), 20);
    }

    #[tokio::test]
    async fn test_projection_query() {
        let query = "SELECT name FROM data";
        let executor = QueryExecutor::new(query.to_string());

        let batch = create_test_batch();
        let results = executor
            .execute_on_batches(vec![batch], "data")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        let result_batch = &results[0];
        assert_eq!(result_batch.num_columns(), 1);
        assert_eq!(result_batch.schema().field(0).name(), "name");
    }

    #[tokio::test]
    async fn test_invalid_column_query() {
        let query = "SELECT nonexistent FROM data";
        let executor = QueryExecutor::new(query.to_string());

        let batch = create_test_batch();
        let result = executor.execute_on_batches(vec![batch], "data").await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to parse SQL query"));
    }

    #[test]
    fn test_build_query_with_sort() {
        let base_query = "SELECT * FROM data WHERE id > 10";
        let sort_columns = vec![("name".to_string(), true), ("id".to_string(), false)];

        let result = build_query_with_sort(base_query, &sort_columns);
        assert_eq!(
            result,
            "SELECT * FROM (SELECT * FROM data WHERE id > 10) AS subquery ORDER BY name ASC, id DESC"
        );
    }

    #[test]
    fn test_build_query_with_empty_sort() {
        let base_query = "SELECT * FROM data";
        let sort_columns = vec![];

        let result = build_query_with_sort(base_query, &sort_columns);
        assert_eq!(result, "SELECT * FROM data");
    }

    #[tokio::test]
    async fn test_build_query_with_string_types_that_are_not_utf8view() {
        let base_query = "SELECT CAST('foo' AS STRING)";

        // Check that it defaults to utf8view, so that we can be sure we are
        // disabling it correctly and detect if behavior changes.
        let cfg = SessionConfig::new();
        let ctx = SessionContext::new_with_config(cfg);
        let result_schema = ctx
            .sql(base_query)
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap()
            .schema();
        assert_eq!(result_schema.field(0).data_type(), &DataType::Utf8View);

        // Check that we actually disable the behavior, as a contrast
        let executor = QueryExecutor::new(base_query.to_string());
        let result = executor
            .sql_to_dataframe(base_query)
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();

        let result = result.schema();
        assert_eq!(result.field(0).data_type(), &DataType::Utf8);
    }
}
