use anyhow::{Result, anyhow};
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};

use crate::operations::data_operation::DataOperation;

pub const DEFAULT_TABLE_NAME: &str = "data";

pub struct QueryOperation {
    query: String,
}

impl QueryOperation {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

#[async_trait]
impl DataOperation for QueryOperation {
    async fn apply(&self, ctx: &mut SessionContext, df: DataFrame) -> Result<DataFrame> {
        ctx.register_table(DEFAULT_TABLE_NAME, df.into_view())?;
        ctx.sql(&self.query).await.map_err(|e| anyhow!(e))
    }
}
