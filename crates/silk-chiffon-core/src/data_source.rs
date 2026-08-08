use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, execution::SendableRecordBatchStream, prelude::SessionContext,
};

/// Whether the same logical input can be read from the beginning more than once.
///
/// Replayability concerns repeated consumption of the same logical input. It does not imply
/// arbitrary byte seeking; a source may replay by reopening a pinned snapshot or restarting a
/// read session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Replayability {
    /// Reading the input consumes its only available pass.
    SinglePass,
    /// A later read can reproduce the same logical input from the beginning.
    Replayable,
}

/// A cardinality hint for sizing work without requiring every stream to know its length.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RowCount {
    Exact(u64),
    Estimated(u64),
    #[default]
    Unknown,
}

/// A format-independent source of Arrow record batches.
#[async_trait]
pub trait DataSource: Send + Sync {
    fn name(&self) -> &str;

    /// Reports whether the input can be read from the beginning more than once.
    fn replayability(&self) -> Replayability;

    /// Returns schema metadata, awaiting I/O when necessary.
    async fn schema(&self) -> Result<SchemaRef>;

    /// Returns a sizing hint, where `Unknown` differs from a failed metadata lookup.
    async fn row_count(&self) -> Result<RowCount> {
        Ok(RowCount::Unknown)
    }

    /// Uses DataFusion's provider boundary so a source need not expose a file or object store.
    async fn as_table_provider(&self, ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>>;

    /// Uses the caller's session so its runtime and object-store configuration apply.
    async fn as_stream(&self, ctx: &mut SessionContext) -> Result<SendableRecordBatchStream> {
        let table = self.as_table_provider(ctx).await?;
        let data_frame = ctx.read_table(table)?;
        data_frame
            .execute_stream()
            .await
            .map_err(anyhow::Error::from)
    }
}
