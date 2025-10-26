use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, execution::SendableRecordBatchStream, prelude::SessionContext,
};

#[async_trait]
pub trait DataSource: Send + Sync {
    fn name(&self) -> &str;

    async fn as_table_provider(&self, ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        Err(anyhow!("as_table_provider is not implemented"))
    }

    fn supports_table_provider(&self) -> bool {
        false
    }

    async fn as_stream(&self) -> Result<SendableRecordBatchStream> {
        let mut ctx = SessionContext::new();
        self.as_stream_with_session_context(&mut ctx).await
    }

    async fn as_stream_with_session_context(
        &self,
        ctx: &mut SessionContext,
    ) -> Result<SendableRecordBatchStream> {
        let table = self.as_table_provider(ctx).await?;
        let df = ctx.read_table(table)?;
        df.execute_stream().await.map_err(|e| anyhow!(e))
    }
}
