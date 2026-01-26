//! Cardinality analysis using DataFusion's streaming aggregation.
//!
//! Analyzes columns for cardinality statistics (approx_distinct, avg_value_size)
//! to make informed decisions about dictionary encoding and bloom filters.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::functions::string::expr_fn::octet_length;
use datafusion::functions_aggregate::expr_fn::{approx_distinct, avg, count_distinct};
use datafusion::prelude::{SessionConfig, SessionContext, cast, col};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::sinks::parquet::adaptive_writer::encoding::is_analyzable;
use crate::utils::channel_stream_provider::ChannelTableProvider;

#[derive(Debug, Clone)]
pub struct ColumnAnalysis {
    pub approx_distinct: u64,
    pub avg_value_size: Option<f64>,
}

/// Streams batches to background cardinality analysis.
pub(crate) struct RowGroupAnalysisState {
    analysis_tx: mpsc::Sender<RecordBatch>,
    query_handle: JoinHandle<Result<HashMap<String, ColumnAnalysis>>>,
}

impl RowGroupAnalysisState {
    pub fn try_new(schema: &SchemaRef, columns_to_analyze: &[String]) -> Result<Self> {
        if columns_to_analyze.is_empty() {
            anyhow::bail!("no columns to analyze; use passthrough mode instead");
        }

        let (tx, rx) = mpsc::channel(16);
        let query_handle = tokio::spawn(run_streaming_analysis(
            Arc::clone(schema),
            rx,
            columns_to_analyze.to_vec(),
        ));

        Ok(Self {
            analysis_tx: tx,
            query_handle,
        })
    }

    pub async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.analysis_tx
            .send(batch)
            .await
            .context("analysis channel disconnected")
    }

    pub async fn finalize(self) -> Result<HashMap<String, ColumnAnalysis>> {
        drop(self.analysis_tx);
        self.query_handle.await.context("analysis task panicked")?
    }
}

/// Run streaming DataFusion analysis for cardinality.
/// Uses approx_distinct() when supported, falls back to count_distinct() otherwise.
/// Nested types (List, Struct, Map) are skipped entirely.
async fn run_streaming_analysis(
    schema: SchemaRef,
    rx: mpsc::Receiver<RecordBatch>,
    columns: Vec<String>,
) -> Result<HashMap<String, ColumnAnalysis>> {
    let columns: Vec<String> = columns
        .into_iter()
        .filter(|col| {
            schema
                .field_with_name(col)
                .map(|f| is_analyzable(f.data_type()))
                .unwrap_or(false)
        })
        .collect();

    if columns.is_empty() {
        anyhow::bail!("no analyzable columns after filtering");
    }

    let provider = ChannelTableProvider::new(Arc::clone(&schema), rx);
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("data", Arc::new(provider))?;

    let mut agg_exprs = Vec::new();
    for col_name in &columns {
        let ndv_expr = if let Ok(field) = schema.field_with_name(col_name)
            && supports_approx_distinct(field.data_type())
        {
            approx_distinct(col(col_name))
        } else {
            // count_distinct returns Int64, cast to UInt64 for consistency
            cast(count_distinct(col(col_name)), DataType::UInt64)
        };
        agg_exprs.push(ndv_expr.alias(format!("{col_name}_ndv")));

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

/// Types that support DataFusion's approx_distinct() (HyperLogLog).
fn supports_approx_distinct(data_type: &DataType) -> bool {
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
            .and_then(|arr| arr.is_valid(0).then(|| arr.value(0)))
            .unwrap_or(0);

        let avg_value_size = batch
            .column_by_name(&avg_col)
            .and_then(|arr| arr.as_primitive_opt::<arrow::datatypes::Float64Type>())
            .and_then(|arr| arr.is_valid(0).then(|| arr.value(0)));

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
