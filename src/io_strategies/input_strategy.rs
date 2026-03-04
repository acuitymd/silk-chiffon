use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    catalog::TableProvider,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream, SessionStateBuilder},
    prelude::{DataFrame, SessionContext},
};
use futures::stream::{SelectAll, Stream, select_all};

use crate::sources::data_source::{
    DataSource, TARGET_SAMPLE_ROWS, sample_column_sizes_from_batch_stream,
};
use crate::utils::memory::estimate_fixed_type_bytes;

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
    pub fn schema(&self) -> Result<SchemaRef> {
        match self {
            InputStrategy::Single(source) => source.schema(),
            InputStrategy::Multiple(sources) => sources
                .first()
                .ok_or_else(|| anyhow::anyhow!("No sources provided"))
                .and_then(|s| s.schema()),
        }
    }

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

    /// Estimate average in-memory bytes per row by sampling across all sources.
    ///
    /// Fixed-width columns use exact sizes from the schema. Variable-width
    /// columns are sampled from a merged stream spanning all sources, so small
    /// files don't produce unreliable estimates.
    ///
    /// # Panics
    ///
    /// Panics if called from a single-threaded (`current_thread`) tokio runtime.
    pub fn row_size(&self, schema: &SchemaRef) -> Result<usize> {
        let all_cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let sizes = self.estimate_column_sizes(schema, &all_cols, TARGET_SAMPLE_ROWS)?;
        let total: usize = sizes.values().sum();
        Ok(total.max(1))
    }

    /// Estimate average value sizes for the given columns by sampling all sources.
    ///
    /// # Panics
    ///
    /// Panics if called from a single-threaded (`current_thread`) tokio runtime.
    pub fn estimate_column_sizes(
        &self,
        schema: &SchemaRef,
        columns: &[String],
        max_sample_rows: usize,
    ) -> Result<HashMap<String, usize>> {
        let mut result = HashMap::with_capacity(columns.len());
        let mut variable_cols: Vec<(String, usize)> = Vec::new();

        for col_name in columns {
            match schema.column_with_name(col_name) {
                Some((idx, field)) => {
                    if let Some(size) = estimate_fixed_type_bytes(field.data_type()) {
                        result.insert(col_name.clone(), size);
                    } else {
                        variable_cols.push((col_name.clone(), idx));
                    }
                }
                None => continue,
            }
        }

        if variable_cols.is_empty() {
            return Ok(result);
        }

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let max_total_rows = max_sample_rows.saturating_mul(10);
                let config =
                    datafusion::prelude::SessionConfig::new().with_batch_size(max_total_rows);
                let state = SessionStateBuilder::new().with_config(config).build();
                let mut ctx = SessionContext::new_with_state(state);
                let stream = self.as_stream(&mut ctx).await?;
                sample_column_sizes_from_batch_stream(
                    stream,
                    &variable_cols,
                    max_sample_rows,
                    &mut result,
                )
                .await?;
                Ok(result)
            })
        })
    }

    pub async fn as_stream(&self, ctx: &mut SessionContext) -> Result<SendableRecordBatchStream> {
        match self {
            InputStrategy::Single(source) => source.as_stream_with_session_context(ctx).await,
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    anyhow::bail!("No sources provided");
                }

                let first_stream = sources[0].as_stream_with_session_context(ctx).await?;
                let schema = first_stream.schema();

                let mut streams = vec![first_stream];
                for source in &sources[1..] {
                    let stream = source.as_stream_with_session_context(ctx).await?;
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
