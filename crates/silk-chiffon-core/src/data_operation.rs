use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};

#[async_trait]
/// One logical transformation applied before the final physical plan is built.
pub trait DataOperation {
    /// Applies this operation to the current frame in the shared session.
    async fn apply(&self, ctx: &mut SessionContext, df: DataFrame) -> Result<DataFrame>;
}
