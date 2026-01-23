//! Cardinality analysis using DataFusion's streaming aggregation.
//!
//! Analyzes columns for cardinality statistics (approx_distinct, avg_value_size)
//! to make informed decisions about dictionary encoding and bloom filters.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::array::{AsArray, RecordBatch};
use arrow::compute::BatchCoalescer;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::functions::string::expr_fn::octet_length;
use datafusion::functions_aggregate::expr_fn::{approx_distinct, avg};
use datafusion::prelude::{SessionConfig, SessionContext, col};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::utils::cancellable_channel::{
    CancellableReceiver, CancellableSender, cancellable_channel_bounded,
};
use crate::utils::channel_stream_provider::ChannelTableProvider;

#[derive(Debug, Clone)]
pub struct ColumnAnalysis {
    pub approx_distinct: u64,
    pub avg_value_size: Option<f64>,
}

/// Accumulates batches and runs streaming cardinality analysis.
pub(crate) struct RowGroupAnalysisState {
    coalescer: BatchCoalescer,
    accumulated_rows: usize,
    analysis_tx: Option<CancellableSender<RecordBatch>>,
    query_handle: Option<JoinHandle<Result<HashMap<String, ColumnAnalysis>>>>,
}

impl RowGroupAnalysisState {
    pub fn new(
        schema: &SchemaRef,
        columns_to_analyze: &[String],
        cancel_signal: &CancellationToken,
        max_row_group_size: usize,
    ) -> Self {
        let coalescer = BatchCoalescer::new(Arc::clone(schema), max_row_group_size);

        if columns_to_analyze.is_empty() {
            return Self {
                coalescer,
                accumulated_rows: 0,
                analysis_tx: None,
                query_handle: None,
            };
        }

        let (tx, rx) = cancellable_channel_bounded(16, cancel_signal.clone(), "analysis");
        let query_handle = tokio::spawn(run_streaming_analysis(
            Arc::clone(schema),
            rx,
            columns_to_analyze.to_vec(),
        ));

        Self {
            coalescer,
            accumulated_rows: 0,
            analysis_tx: Some(tx),
            query_handle: Some(query_handle),
        }
    }

    pub async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.accumulated_rows += batch.num_rows();
        if let Some(ref tx) = self.analysis_tx {
            tx.send(batch.clone())
                .await
                .context("analysis channel disconnected")?;
        }
        self.coalescer.push_batch(batch)?;
        Ok(())
    }

    pub fn rows(&self) -> usize {
        self.accumulated_rows
    }

    pub async fn finalize(self) -> Result<(RecordBatch, usize, HashMap<String, ColumnAnalysis>)> {
        drop(self.analysis_tx);

        let analysis = if let Some(handle) = self.query_handle {
            handle.await.context("analysis task panicked")??
        } else {
            HashMap::new()
        };

        let mut coalescer = self.coalescer;
        coalescer.finish_buffered_batch()?;
        let rows = self.accumulated_rows;
        let batch = coalescer
            .next_completed_batch()
            .ok_or_else(|| anyhow!("coalescer produced no batch (accumulated {} rows)", rows))?;

        Ok((batch, rows, analysis))
    }
}

/// Run streaming DataFusion analysis using approx_distinct().
/// Only primitive columns can be analyzed - nested types (List, Struct, Map) are skipped.
async fn run_streaming_analysis(
    schema: SchemaRef,
    rx: CancellableReceiver<RecordBatch>,
    columns: Vec<String>,
) -> Result<HashMap<String, ColumnAnalysis>> {
    let columns: Vec<String> = columns
        .into_iter()
        .filter(|col| {
            schema
                .field_with_name(col)
                .map(|f| is_primitive_for_analysis(f.data_type()))
                .unwrap_or(false)
        })
        .collect();

    if columns.is_empty() {
        let mut rx = rx;
        while rx.recv().await.is_ok() {}
        return Ok(HashMap::new());
    }

    let provider = ChannelTableProvider::new(Arc::clone(&schema), rx);
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("data", Arc::new(provider))?;

    let mut agg_exprs = Vec::new();
    for col_name in &columns {
        agg_exprs.push(approx_distinct(col(col_name)).alias(format!("{col_name}_ndv")));
        if let Ok(field) = schema.field_with_name(col_name)
            && is_variable_length_type(field.data_type())
        {
            agg_exprs.push(avg(octet_length(col(col_name))).alias(format!("{col_name}_avg_len")));
        }
    }

    let df = ctx.table("data").await?;
    let results = df.aggregate(vec![], agg_exprs)?.collect().await?;

    if results.is_empty() || results[0].num_rows() == 0 {
        return Ok(HashMap::new());
    }

    extract_analysis_from_results(&results[0], &columns)
}

/// Types that can be analyzed by DataFusion's approx_distinct().
///
/// Floats are excluded because they rarely benefit from dictionary encoding
/// or bloom filters due to their high cardinality in practice.
fn is_primitive_for_analysis(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Boolean
            | DataType::Dictionary(_, _)
    )
}

fn is_variable_length_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    )
}

fn extract_analysis_from_results(
    batch: &RecordBatch,
    columns: &[String],
) -> Result<HashMap<String, ColumnAnalysis>> {
    let mut analysis = HashMap::new();

    for col in columns {
        let ndv_col = format!("{col}_ndv");
        let avg_col = format!("{col}_avg_len");

        let approx_distinct = batch
            .column_by_name(&ndv_col)
            .and_then(|arr| arr.as_primitive_opt::<arrow::datatypes::UInt64Type>())
            .map(|arr| arr.value(0))
            .unwrap_or(0);

        let avg_value_size = batch
            .column_by_name(&avg_col)
            .and_then(|arr| arr.as_primitive_opt::<arrow::datatypes::Float64Type>())
            .map(|arr| arr.value(0));

        analysis.insert(
            col.clone(),
            ColumnAnalysis {
                approx_distinct,
                avg_value_size,
            },
        );
    }

    Ok(analysis)
}

/// Simple accumulator for when no analysis is needed.
pub(crate) struct SimpleAccumulator {
    coalescer: BatchCoalescer,
    rows: usize,
}

impl SimpleAccumulator {
    pub fn new(schema: SchemaRef, max_row_group_size: usize) -> Self {
        Self {
            coalescer: BatchCoalescer::new(schema, max_row_group_size),
            rows: 0,
        }
    }

    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.rows += batch.num_rows();
        self.coalescer.push_batch(batch)?;
        Ok(())
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn take(&mut self) -> Result<(RecordBatch, usize)> {
        self.coalescer.finish_buffered_batch()?;
        let rows = self.rows;
        let batch = self
            .coalescer
            .next_completed_batch()
            .ok_or_else(|| anyhow!("coalescer produced no batch (accumulated {} rows)", rows))?;
        self.rows = 0;
        Ok((batch, rows))
    }
}
