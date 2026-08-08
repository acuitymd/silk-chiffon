use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    catalog::TableProvider,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
    prelude::{DataFrame, SessionContext},
};
use futures::stream::{SelectAll, Stream, select_all};

use crate::sources::data_source::{DataSource, Replayability, RowCount};

struct MergedSendableRecordBatchStreams {
    schema: SchemaRef,
    inner: SelectAll<SendableRecordBatchStream>,
}

impl Stream for MergedSendableRecordBatchStreams {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl RecordBatchStream for MergedSendableRecordBatchStreams {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub enum InputStrategy {
    Single(Box<dyn DataSource>),
    Multiple(Vec<Box<dyn DataSource>>),
}

impl InputStrategy {
    pub async fn as_table_provider(
        &self,
        ctx: &mut SessionContext,
        _working_directory: Option<String>,
    ) -> Result<Arc<dyn TableProvider>> {
        match self {
            InputStrategy::Single(source) => source.as_table_provider(ctx).await,
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    anyhow::bail!("No sources provided");
                }

                let mut providers: Vec<Arc<dyn TableProvider>> = Vec::new();

                for source in sources {
                    providers.push(source.as_table_provider(ctx).await?);
                }

                let mut df: DataFrame = ctx.read_table(Arc::clone(&providers[0]))?;

                for provider in &providers[1..] {
                    df = df.union(ctx.read_table(Arc::clone(provider))?)?;
                }

                Ok(df.into_view())
            }
        }
    }

    pub fn replayability(&self) -> Replayability {
        match self {
            InputStrategy::Single(source) => source.replayability(),
            InputStrategy::Multiple(sources) => {
                if sources
                    .iter()
                    .all(|source| source.replayability() == Replayability::Replayable)
                {
                    Replayability::Replayable
                } else {
                    Replayability::SinglePass
                }
            }
        }
    }

    pub async fn row_count(&self) -> Result<RowCount> {
        match self {
            InputStrategy::Single(source) => source.row_count().await,
            InputStrategy::Multiple(sources) => {
                let mut total = 0_u64;
                let mut estimated = false;
                for source in sources {
                    match source.row_count().await? {
                        RowCount::Exact(count) => total = total.saturating_add(count),
                        RowCount::Estimated(count) => {
                            total = total.saturating_add(count);
                            estimated = true;
                        }
                        RowCount::Unknown => return Ok(RowCount::Unknown),
                    }
                }
                if estimated {
                    Ok(RowCount::Estimated(total))
                } else {
                    Ok(RowCount::Exact(total))
                }
            }
        }
    }

    pub async fn as_stream(&self, ctx: &mut SessionContext) -> Result<SendableRecordBatchStream> {
        match self {
            InputStrategy::Single(source) => source.as_stream(ctx).await,
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    anyhow::bail!("No sources provided");
                }

                let first_stream = sources[0].as_stream(ctx).await?;
                let schema = first_stream.schema();

                let mut streams = vec![first_stream];
                for source in &sources[1..] {
                    let stream = source.as_stream(ctx).await?;
                    if stream.schema() != schema {
                        anyhow::bail!("Schemas do not match");
                    }
                    streams.push(stream);
                }

                let merged = MergedSendableRecordBatchStreams {
                    schema,
                    inner: select_all(streams),
                };

                Ok(Box::pin(merged))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::empty::EmptyTable;

    use super::*;

    struct TestSource(Replayability);

    #[async_trait::async_trait]
    impl DataSource for TestSource {
        fn name(&self) -> &str {
            "test"
        }

        fn replayability(&self) -> Replayability {
            self.0
        }

        async fn schema(&self) -> Result<SchemaRef> {
            Ok(Arc::new(arrow::datatypes::Schema::empty()))
        }

        async fn as_table_provider(
            &self,
            _: &mut SessionContext,
        ) -> Result<Arc<dyn TableProvider>> {
            Ok(Arc::new(EmptyTable::new(self.schema().await?)))
        }
    }

    #[test]
    fn multiple_inputs_aggregate_replayability() {
        let mixed = InputStrategy::Multiple(vec![
            Box::new(TestSource(Replayability::Replayable)),
            Box::new(TestSource(Replayability::SinglePass)),
        ]);
        assert_eq!(mixed.replayability(), Replayability::SinglePass);

        let replayable = InputStrategy::Multiple(vec![
            Box::new(TestSource(Replayability::Replayable)),
            Box::new(TestSource(Replayability::Replayable)),
        ]);
        assert_eq!(replayable.replayability(), Replayability::Replayable);
    }
}
