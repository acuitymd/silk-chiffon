use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext, col};

use crate::{SortColumn, SortDirection, operations::data_operation::DataOperation};

pub struct SortOperation {
    columns: Vec<SortColumn>,
}

impl SortOperation {
    pub fn new(columns: Vec<SortColumn>) -> Self {
        Self { columns }
    }
}

#[async_trait]
impl DataOperation for SortOperation {
    async fn apply(&self, _ctx: &mut SessionContext, df: DataFrame) -> Result<DataFrame> {
        Ok(df.sort(
            self.columns
                .iter()
                .map(|c| {
                    let sort_ascending = c.direction == SortDirection::Ascending;
                    let sort_nulls_first = !sort_ascending;
                    // match the behavior of postgres here, where nulls are first for descending
                    col(&c.name).sort(sort_ascending, sort_nulls_first)
                })
                .collect(),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sort_ascending_nulls_last() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![
                Some(3),
                None,
                Some(1),
                None,
                Some(2),
            ]))],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).unwrap();

        let sort_op = SortOperation::new(vec![SortColumn {
            name: "id".to_string(),
            direction: SortDirection::Ascending,
        }]);

        let mut ctx_mut = SessionContext::new();
        let result_df = sort_op.apply(&mut ctx_mut, df).await.unwrap();
        let result = result_df.collect().await.unwrap();

        let ids = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
        assert!(ids.is_null(3));
        assert!(ids.is_null(4));
    }

    #[tokio::test]
    async fn test_sort_descending_nulls_first() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![
                Some(3),
                None,
                Some(1),
                None,
                Some(2),
            ]))],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).unwrap();

        let sort_op = SortOperation::new(vec![SortColumn {
            name: "id".to_string(),
            direction: SortDirection::Descending,
        }]);

        let mut ctx_mut = SessionContext::new();
        let result_df = sort_op.apply(&mut ctx_mut, df).await.unwrap();
        let result = result_df.collect().await.unwrap();

        let ids = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert!(ids.is_null(0));
        assert!(ids.is_null(1));
        assert_eq!(ids.value(2), 3);
        assert_eq!(ids.value(3), 2);
        assert_eq!(ids.value(4), 1);
    }
}
