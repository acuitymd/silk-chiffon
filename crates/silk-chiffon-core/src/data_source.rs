use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;

/// Whether a command may read the same logical input more than once.
///
/// Replayability lets a caller decide whether it may consume rows to measure them before the
/// planned query runs. It does not promise byte seeking. A remote source can be replayable by
/// reopening a pinned snapshot or restarting a read session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Replayability {
    /// Reading any rows may consume input needed by the command's execution.
    SinglePass,
    /// A later read reproduces the same logical input from the beginning.
    Replayable,
}

/// Cardinality metadata available without consuming the source.
///
/// Cardinality is independent of replayability and DataFusion's physical-plan boundedness. A
/// finite source may have an unknown row count, and a replayable source may provide only an
/// estimate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RowCount {
    /// The source contains exactly this many rows.
    Exact(u64),
    /// The source is expected to contain approximately this many rows.
    Estimated(u64),
    /// The source cannot provide a useful count at this point in its lifecycle.
    #[default]
    Unknown,
}

/// A format-independent input that DataFusion can plan and execute.
///
/// A format creates one source for each command input. The source owns any state needed to open
/// that input and creates its table provider in the command's shared DataFusion session.
/// DataFusion remains responsible for physical boundedness, partitioning, projection, and filter
/// pushdown.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Returns the source's stable diagnostic identifier.
    fn name(&self) -> &str;

    /// Reports whether the input can be read from the beginning more than once.
    fn replayability(&self) -> Replayability;

    /// Returns an operation for reading cardinality metadata when the source supports one.
    ///
    /// The capability's presence is stable even when the value it returns changes from
    /// [`RowCount::Unknown`] after the source initializes shared state.
    fn row_count_capability(&self) -> Option<&dyn RowCountCapability> {
        None
    }

    /// Creates the provider DataFusion uses to plan and execute this input.
    ///
    /// The provider may defer reading rows until DataFusion executes its physical plan.
    async fn table_provider(&self) -> Result<Arc<dyn TableProvider>>;

    /// Returns the source schema.
    ///
    /// The default initializes a table provider and uses its schema so providers have one schema
    /// authority. Sources may override this when they already retain the same schema.
    async fn schema(&self) -> Result<SchemaRef> {
        Ok(self.table_provider().await?.schema())
    }
}

/// A source capability that obtains cardinality metadata without consuming rows.
///
/// The operation may perform metadata I/O and may return [`RowCount::Unknown`]. A failed metadata
/// request is distinct from a successful request that cannot produce a useful count.
#[async_trait]
pub trait RowCountCapability: Send + Sync {
    /// Returns the cardinality information available at the time of the call.
    async fn row_count(&self) -> Result<RowCount>;
}
