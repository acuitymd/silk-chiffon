use anyhow::{Result, anyhow};
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};

use crate::operations::data_operation::DataOperation;

pub const DEFAULT_TABLE_NAME: &str = "data";

pub struct QueryOperation {
    ctx: SessionContext,
    query: String,
}

impl QueryOperation {
    pub fn new(ctx: SessionContext, query: String) -> Self {
        Self { ctx, query }
    }
}

#[async_trait]
impl DataOperation for QueryOperation {
    async fn apply(&self, df: DataFrame) -> Result<DataFrame> {
        self.ctx
            .register_table(DEFAULT_TABLE_NAME, df.into_view())?;
        self.ctx.sql(&self.query).await.map_err(|e| anyhow!(e))
    }
}
