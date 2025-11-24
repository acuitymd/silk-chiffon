use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};

#[async_trait]
pub trait DataOperation {
    async fn apply(&self, ctx: &mut SessionContext, df: DataFrame) -> Result<DataFrame>;
}
