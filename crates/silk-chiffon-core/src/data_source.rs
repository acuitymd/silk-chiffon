use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, execution::SendableRecordBatchStream, prelude::SessionContext,
};

/// Controls whether operations that require end-of-input are valid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamBoundedness {
    Finite,
    Infinite,
}

/// Controls whether preflight work may revisit encoded input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputAccess {
    /// Input must be consumed from beginning to end without arbitrary seeking.
    Sequential,
    /// Input supports reads from arbitrary earlier or later positions.
    RandomAccess,
}

/// Source properties needed before DataFusion builds an execution plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataSourceCapabilities {
    boundedness: StreamBoundedness,
    input_access: InputAccess,
}

impl DataSourceCapabilities {
    pub const fn new(boundedness: StreamBoundedness, input_access: InputAccess) -> Self {
        Self {
            boundedness,
            input_access,
        }
    }

    pub const fn boundedness(self) -> StreamBoundedness {
        self.boundedness
    }

    pub const fn input_access(self) -> InputAccess {
        self.input_access
    }
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

    /// Reports behavior needed to validate planning and preflight work.
    fn capabilities(&self) -> DataSourceCapabilities;

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
