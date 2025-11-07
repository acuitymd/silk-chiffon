use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;

#[async_trait]
pub trait DataOperation {
    async fn apply(&self, df: DataFrame) -> Result<DataFrame>;
}
