use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use silk_chiffon_storage::StorageHandle;
use url::Url;

/// Command-scoped format state that opens one or more output sinks.
///
/// A format binds its parsed CLI settings and shared resources once, after the
/// input plan has been validated. Partitioned output can then open many
/// [`DataSink`] values without rebuilding that state for each file.
#[async_trait]
pub trait SinkBinding: Send + Sync {
    /// Opens a sink for one storage handle and its projected schema.
    async fn open_sink(
        &self,
        handle: StorageHandle,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>>;
}

/// A format-independent writer for one logical output.
///
/// Writing and completion are separate because a sink may buffer encoded data,
/// upload parts, or write a format footer after its last input batch.
#[async_trait]
pub trait DataSink: Send + Sync {
    /// Writes every batch in a DataFusion stream without completing the sink.
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<()> {
        while let Some(batch) = stream.next().await {
            self.write_batch(batch?).await?;
        }
        Ok(())
    }

    /// Writes one batch without completing the sink.
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Completes the output and reports the durable objects it produced.
    async fn finish(self: Box<Self>) -> Result<SinkCompletion>;
}

/// Durable locations and row count produced by one completed sink.
///
/// Construction requires one location because a successful file sink must
/// identify at least one durable output. Additional locations let a format
/// report a multi-object completion without inventing per-object row counts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkCompletion {
    durable_locations: Vec<Url>,
    rows_written: u64,
}

impl SinkCompletion {
    /// Creates a completion with its required first durable location.
    pub fn new(
        first_durable_location: Url,
        additional_durable_locations: impl IntoIterator<Item = Url>,
        rows_written: u64,
    ) -> Self {
        let mut durable_locations = vec![first_durable_location];
        durable_locations.extend(additional_durable_locations);
        Self {
            durable_locations,
            rows_written,
        }
    }

    /// Returns every durable location produced by this logical sink.
    pub fn durable_locations(&self) -> &[Url] {
        &self.durable_locations
    }

    /// Returns the number of rows accepted by this logical sink.
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }
}
