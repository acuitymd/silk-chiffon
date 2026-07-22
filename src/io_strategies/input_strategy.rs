//! Input source registration, metadata validation, and deterministic unions.
//!
//! Each unique object store is registered before providers are built. Provider
//! futures run concurrently under a fixed bound, while `buffered` retains input
//! order for the resulting DataFusion union.

use std::collections::HashSet;
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
use futures::{
    StreamExt, TryStreamExt,
    stream::{SelectAll, Stream, select_all},
};

use crate::sources::data_source::DataSource;

const PROVIDER_BUILD_CONCURRENCY: usize = 16;

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
        ctx: &SessionContext,
        _working_directory: Option<String>,
    ) -> Result<Arc<dyn TableProvider>> {
        match self {
            InputStrategy::Single(source) => {
                register_object_stores(ctx, std::iter::once(source.as_ref()));
                source.as_table_provider(ctx).await
            }
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    anyhow::bail!("No sources provided");
                }

                register_object_stores(ctx, sources.iter().map(Box::as_ref));
                let providers: Vec<Arc<dyn TableProvider>> = futures::stream::iter(
                    sources.iter().map(|source| source.as_table_provider(ctx)),
                )
                .buffered(PROVIDER_BUILD_CONCURRENCY)
                .try_collect()
                .await?;

                let mut df: DataFrame = ctx.read_table(Arc::clone(&providers[0]))?;

                for provider in &providers[1..] {
                    df = df.union(ctx.read_table(Arc::clone(provider))?)?;
                }

                Ok(df.into_view())
            }
        }
    }

    pub async fn row_count(&self) -> Result<usize> {
        match self {
            InputStrategy::Single(source) => source.row_count().await,
            InputStrategy::Multiple(sources) => {
                let counts: Vec<usize> =
                    futures::stream::iter(sources.iter().map(|source| source.row_count()))
                        .buffered(PROVIDER_BUILD_CONCURRENCY)
                        .try_collect()
                        .await?;
                Ok(counts.into_iter().sum())
            }
        }
    }

    pub async fn validate_schemas(&self) -> Result<SchemaRef> {
        let sources = match self {
            InputStrategy::Single(source) => return source.schema().await,
            InputStrategy::Multiple(sources) if sources.is_empty() => {
                anyhow::bail!("No sources provided");
            }
            InputStrategy::Multiple(sources) => sources,
        };
        let schemas: Vec<SchemaRef> =
            futures::stream::iter(sources.iter().map(|source| source.schema()))
                .buffered(PROVIDER_BUILD_CONCURRENCY)
                .try_collect()
                .await?;
        let schema = Arc::clone(&schemas[0]);
        if let Some((index, _)) = schemas
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, candidate)| candidate.as_ref() != schema.as_ref())
        {
            anyhow::bail!(
                "Schema mismatch for input file {} (does not match other file(s))",
                sources[index].input().location().display()
            );
        }
        Ok(schema)
    }

    pub async fn as_stream(&self, ctx: &SessionContext) -> Result<SendableRecordBatchStream> {
        match self {
            InputStrategy::Single(source) => {
                register_object_stores(ctx, std::iter::once(source.as_ref()));
                source.as_stream_with_session_context(ctx).await
            }
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    anyhow::bail!("No sources provided");
                }

                register_object_stores(ctx, sources.iter().map(Box::as_ref));
                let mut streams: Vec<SendableRecordBatchStream> = futures::stream::iter(
                    sources
                        .iter()
                        .map(|source| source.as_stream_with_session_context(ctx)),
                )
                .buffered(PROVIDER_BUILD_CONCURRENCY)
                .try_collect()
                .await?;
                let first_stream = streams.remove(0);
                let schema = first_stream.schema();

                for stream in &streams {
                    if stream.schema() != schema {
                        anyhow::bail!("Schemas do not match");
                    }
                }
                streams.insert(0, first_stream);

                let merged = MergedSendableRecordBatchStreams {
                    schema,
                    inner: select_all(streams),
                };

                Ok(Box::pin(merged))
            }
        }
    }
}

pub(crate) fn register_object_stores<'a>(
    ctx: &SessionContext,
    sources: impl IntoIterator<Item = &'a dyn DataSource>,
) -> usize {
    let mut registered = HashSet::new();
    for source in sources {
        let store = source.input().location().store();
        if registered.insert(store.store_url().as_str().to_string()) {
            ctx.register_object_store(store.store_url().as_ref(), Arc::clone(store.object_store()));
        }
    }
    registered.len()
}
