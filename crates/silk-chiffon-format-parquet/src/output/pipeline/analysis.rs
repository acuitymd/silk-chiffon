//! Cardinality analysis of a row group's columns.
//!
//! Analyzes columns for cardinality statistics (approx_distinct, avg_value_size)
//! to make informed decisions about dictionary encoding and bloom filters.

use std::collections::HashMap;

use anyhow::{Context, Result};
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::functions::string::expr_fn::octet_length;
use datafusion::functions_aggregate::expr_fn::{approx_distinct, avg, count_distinct};
use datafusion::prelude::{SessionConfig, SessionContext, ident};
use tokio::runtime::Handle;
use tokio::task::JoinSet;

use crate::output::pipeline::{PipelineTaskScope, encoding::is_analyzable};

#[derive(Debug, Clone)]
pub struct ColumnAnalysis {
    pub approx_distinct: u64,
    pub avg_value_size: Option<f64>,
}

/// Runs the cardinality queries of a file's row groups.
pub(crate) struct RowGroupAnalyzer {
    /// Top-level columns whose cardinality decides how they are encoded.
    columns: Vec<String>,
    context: SessionContext,
}

impl RowGroupAnalyzer {
    pub(crate) fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            context: SessionContext::new_with_config(
                SessionConfig::new().with_target_partitions(1),
            ),
        }
    }

    /// Analyzes one row group, given as the batches that make it up, a task
    /// per column on `handle`. Nested types (List, Struct, Map) are skipped
    /// entirely.
    ///
    /// A row group is analyzed as it is encoded, not as its batches arrive,
    /// so that several row groups and all of their columns are analyzed at
    /// once. A query per row group, run one at a time as its batches
    /// arrived, was a single thread that every row passed through, and it was
    /// slower than the encoders it fed.
    pub(crate) async fn analyze(
        &self,
        row_group: &[RecordBatch],
        handle: &Handle,
        scope: &PipelineTaskScope,
    ) -> Result<HashMap<String, ColumnAnalysis>> {
        let Some(first) = row_group.first() else {
            return Ok(HashMap::new());
        };
        let schema = first.schema();
        let mut tasks = JoinSet::new();
        for name in &self.columns {
            let Ok(index) = schema.index_of(name) else {
                continue;
            };
            if !is_analyzable(schema.field(index).data_type()) {
                continue;
            }
            // one schema for every piece: the sink accepts batches whose
            // schemas differ in metadata or nullability, and a query over
            // batches rejects any that its first batch's schema does not
            // contain
            let field = schema.field(index);
            let column_schema = Arc::new(Schema::new(vec![Field::new(
                field.name(),
                field.data_type().clone(),
                true,
            )]));
            let column = row_group
                .iter()
                .map(|batch| {
                    RecordBatch::try_new(
                        Arc::clone(&column_schema),
                        vec![Arc::clone(batch.column(index))],
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (context, name) = (self.context.clone(), name.clone());
            scope.spawn_in_on(
                &mut tasks,
                async move {
                    let analysis = analyze_column(&context, &name, column).await?;
                    Ok::<_, anyhow::Error>(analysis.map(|analysis| (name, analysis)))
                },
                handle,
            );
        }
        let mut analysis = HashMap::new();
        while let Some(result) = tasks.join_next().await {
            analysis.extend(result.context("column analysis task failed")??);
        }
        Ok(analysis)
    }
}

/// Uses approx_distinct() when supported, falls back to count_distinct() otherwise.
async fn analyze_column(
    context: &SessionContext,
    name: &str,
    column: Vec<RecordBatch>,
) -> Result<Option<ColumnAnalysis>> {
    let Some(first) = column.first() else {
        return Ok(None);
    };
    let schema = first.schema();
    let data_type = schema.field(0).data_type();
    // approx_distinct uses HyperLogLog and returns UInt64 but only supports
    // a subset of types. count_distinct supports all types but returns Int64.
    // we handle both return types in extract_analysis because DataFusion's
    // cast(count_distinct(...), UInt64) breaks with certain column types
    // (e.g. Boolean) where the count_distinct implementation doesn't support
    // the cast expression.
    //
    // `ident` takes the name as it is: `col` would read it as SQL, folding
    // its case and splitting it at dots.
    let ndv = if supports_approx_distinct(data_type) {
        approx_distinct(ident(name))
    } else {
        count_distinct(ident(name))
    };
    let mut aggregates = vec![ndv.alias(NDV)];
    if is_variable_length_type(data_type) {
        aggregates.push(avg(octet_length(ident(name))).alias(AVERAGE_LENGTH));
    }
    let results = context
        .read_batches(column)?
        .aggregate(vec![], aggregates)?
        .collect()
        .await?;
    Ok(results
        .first()
        .filter(|result| result.num_rows() > 0)
        .map(extract_analysis))
}

const NDV: &str = "ndv";
const AVERAGE_LENGTH: &str = "avg_len";

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

fn extract_analysis(result: &RecordBatch) -> ColumnAnalysis {
    let approx_distinct = result
        .column_by_name(NDV)
        .and_then(|arr| {
            // approx_distinct returns UInt64, count_distinct returns Int64
            arr.as_primitive_opt::<arrow::datatypes::UInt64Type>()
                .and_then(|a| a.is_valid(0).then(|| a.value(0)))
                .or_else(|| {
                    arr.as_primitive_opt::<arrow::datatypes::Int64Type>()
                        .and_then(|a| a.is_valid(0).then(|| a.value(0).max(0).cast_unsigned()))
                })
        })
        .unwrap_or(0);

    let avg_value_size = result
        .column_by_name(AVERAGE_LENGTH)
        .and_then(|arr| arr.as_primitive_opt::<arrow::datatypes::Float64Type>())
        .and_then(|arr| arr.is_valid(0).then(|| arr.value(0)));

    ColumnAnalysis {
        approx_distinct,
        avg_value_size,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Decimal128Array, Float64Array, Int64Array, ListArray, StringViewArray,
    };
    use arrow::datatypes::Int32Type;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn row_group(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).unwrap()
    }

    #[tokio::test]
    async fn pieces_whose_schemas_differ_in_metadata_or_nullability_are_analyzed_together() {
        use std::collections::HashMap as Metadata;

        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let piece = |nullable: bool, note: &str, values: Vec<&str>| {
            let field = Field::new("s", DataType::Utf8, nullable)
                .with_metadata(Metadata::from([("note".to_owned(), note.to_owned())]));
            let schema = Schema::new(vec![field])
                .with_metadata(Metadata::from([("file".to_owned(), note.to_owned())]));
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(values))])
                .unwrap()
        };
        // the first piece's schema contains neither the second's nullability
        // nor its metadata
        let row_group = [
            piece(false, "a", vec!["x", "y"]),
            piece(true, "b", vec!["z", "x"]),
        ];
        let analysis = RowGroupAnalyzer::new(vec!["s".to_owned()])
            .analyze(
                &row_group,
                &Handle::current(),
                &PipelineTaskScope::new(CancellationToken::new()),
            )
            .await
            .unwrap();
        assert_eq!(analysis["s"].approx_distinct, 3);
    }

    /// Analyzes a row group as two batches, the way its rows arrive.
    async fn analyze(row_group: &RecordBatch) -> HashMap<String, ColumnAnalysis> {
        let columns = row_group
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let half = row_group.num_rows() / 2;
        let pieces = [
            row_group.slice(0, half),
            row_group.slice(half, row_group.num_rows() - half),
        ];
        RowGroupAnalyzer::new(columns)
            .analyze(
                &pieces,
                &Handle::current(),
                &PipelineTaskScope::new(CancellationToken::new()),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn counts_distinct_values_and_measures_strings() {
        let analysis = analyze(&row_group(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2, 2, 3]))),
            (
                "name",
                Arc::new(StringViewArray::from(vec![
                    Some("ab"),
                    Some("ab"),
                    None,
                    Some("abcd"),
                ])),
            ),
            // counted exactly, as approx_distinct takes no decimals
            (
                "amount",
                Arc::new(Decimal128Array::from(vec![
                    Some(10),
                    Some(10),
                    Some(20),
                    None,
                ])),
            ),
        ]))
        .await;

        assert_eq!(analysis["id"].approx_distinct, 3);
        assert_eq!(analysis["id"].avg_value_size, None);
        assert_eq!(analysis["name"].approx_distinct, 2);
        assert_eq!(analysis["name"].avg_value_size, Some(8.0 / 3.0));
        assert_eq!(analysis["amount"].approx_distinct, 2);
    }

    #[tokio::test]
    async fn skips_columns_it_cannot_analyze() {
        let analysis = analyze(&row_group(vec![
            ("score", Arc::new(Float64Array::from(vec![1.0, 2.0]))),
            (
                "tags",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1)]),
                    Some(vec![Some(2)]),
                ])),
            ),
            ("id", Arc::new(Int64Array::from(vec![1, 2]))),
        ]))
        .await;

        assert_eq!(analysis.keys().collect::<Vec<_>>(), ["id"]);
    }

    #[tokio::test]
    async fn takes_column_names_as_they_are() {
        let analysis = analyze(&row_group(vec![
            ("Mixed.Case", Arc::new(Int64Array::from(vec![1, 1, 2]))),
            ("with space", Arc::new(Int64Array::from(vec![5, 5, 5]))),
        ]))
        .await;

        assert_eq!(analysis["Mixed.Case"].approx_distinct, 2);
        assert_eq!(analysis["with space"].approx_distinct, 1);
    }
}
