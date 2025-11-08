use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    catalog::TableProvider,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
    prelude::{DataFrame, SessionContext},
};
use futures::stream::{SelectAll, Stream, select_all};

use crate::sources::{data_source::DataSource, prepared_source::PreparedSource};

struct MergedRecordBatchStream {
    schema: SchemaRef,
    inner: SelectAll<SendableRecordBatchStream>,
}

impl Stream for MergedRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl RecordBatchStream for MergedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        working_directory: Option<String>,
    ) -> Result<Arc<dyn TableProvider>> {
        match self {
            InputStrategy::Single(source) => {
                Ok(
                    PreparedSource::from_data_source(source, ctx, working_directory.clone())
                        .await?
                        .table_provider(),
                )
            }
            InputStrategy::Multiple(sources) => {
                build_multiple_table_providers(ctx, sources, working_directory.clone()).await
            }
        }
    }

    pub async fn as_stream(&self, ctx: &mut SessionContext) -> Result<SendableRecordBatchStream> {
        match self {
            InputStrategy::Single(source) => source.as_stream_with_session_context(ctx).await,
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    return Err(anyhow!("No sources provided"));
                }

                let first_stream = sources[0].as_stream_with_session_context(ctx).await?;
                let schema = first_stream.schema();

                let mut streams = vec![first_stream];
                for source in &sources[1..] {
                    let stream = source.as_stream_with_session_context(ctx).await?;
                    if stream.schema() != schema {
                        return Err(anyhow!("Schemas do not match"));
                    }
                    streams.push(stream);
                }

                let merged = MergedRecordBatchStream {
                    schema,
                    inner: select_all(streams),
                };

                Ok(Box::pin(merged))
            }
        }
    }
}

async fn build_multiple_table_providers(
    ctx: &mut SessionContext,
    sources: &Vec<Box<dyn DataSource>>,
    working_directory: Option<String>,
) -> Result<Arc<dyn TableProvider>> {
    if sources.is_empty() {
        return Err(anyhow!("No sources provided"));
    }

    let mut providers: Vec<Arc<dyn TableProvider>> = Vec::new();

    for source in sources {
        providers.push(
            PreparedSource::from_data_source(source, ctx, working_directory.clone())
                .await?
                .table_provider(),
        );
    }

    let mut df: DataFrame = ctx.read_empty()?;

    for provider in providers {
        df = df.union(ctx.read_table(provider.clone())?)?;
    }

    Ok(df.into_view())
}
