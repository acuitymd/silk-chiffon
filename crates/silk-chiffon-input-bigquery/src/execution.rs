//! Bounded DataFusion physical plan backed by one Storage Read stream per partition.

use std::{fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::{DataFusionError, Statistics, stats::Precision},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
        PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};

use crate::{
    args::BigQueryInputArgs, read_stream, resources::CommandResources, session::SessionLease,
};

pub(crate) struct BigQueryReadExec {
    lease: SessionLease,
    output_schema: SchemaRef,
    batch_projection: Arc<[usize]>,
    resources: Arc<CommandResources>,
    args: BigQueryInputArgs,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl BigQueryReadExec {
    pub(crate) fn new(
        lease: SessionLease,
        output_schema: SchemaRef,
        batch_projection: Vec<usize>,
        resources: Arc<CommandResources>,
        args: &BigQueryInputArgs,
    ) -> Self {
        let properties = plan_properties(
            Arc::clone(&output_schema),
            partition_count(lease.streams().len()),
        );
        Self {
            lease,
            output_schema,
            batch_projection: batch_projection.into(),
            resources,
            args: args.clone(),
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn estimated_total_bytes_scanned(&self) -> u64 {
        self.lease.estimated_total_bytes_scanned()
    }
}

fn partition_count(stream_count: usize) -> usize {
    stream_count.max(1)
}

fn plan_properties(schema: SchemaRef, partition_count: usize) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(partition_count),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

impl ExecutionPlan for BigQueryReadExec {
    fn name(&self) -> &str {
        "BigQueryReadExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "BigQueryReadExec cannot have children".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if self.lease.streams().is_empty() {
            if partition == 0 {
                return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                    &self.output_schema,
                ))));
            }
            return Err(DataFusionError::Execution(format!(
                "BigQuery partition {partition} is out of bounds"
            )));
        }
        let stream_name = self.lease.streams().get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!("BigQuery partition {partition} is out of bounds"))
        })?;
        read_stream::read_rows_stream(read_stream::ReadPartition {
            ordinal: partition,
            stream_name: stream_name.clone(),
            session_schema: self.lease.schema().clone(),
            session_deadline: self.lease.conservative_deadline(),
            output_schema: Arc::clone(&self.output_schema),
            batch_projection: Arc::clone(&self.batch_projection),
            resources: read_stream::StreamResources::from_command(&self.resources),
            args: self.args.clone(),
            metrics: self.metrics.clone(),
            task_context: context,
        })
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Arc<Statistics>> {
        execution_statistics(
            &self.output_schema,
            self.lease.streams().len(),
            self.lease.estimated_row_count(),
            partition,
        )
    }
}

fn execution_statistics(
    schema: &arrow::datatypes::Schema,
    stream_count: usize,
    estimated_rows: Option<usize>,
    partition: Option<usize>,
) -> datafusion::common::Result<Arc<Statistics>> {
    if partition.is_some_and(|index| index >= partition_count(stream_count)) {
        return Err(DataFusionError::Internal(
            "BigQuery statistics partition is out of bounds".to_owned(),
        ));
    }
    let mut statistics = Statistics::new_unknown(schema);
    if stream_count == 0 {
        statistics.num_rows = Precision::Exact(0);
        statistics.total_byte_size = Precision::Exact(0);
    } else if partition.is_none()
        && let Some(rows) = estimated_rows
    {
        statistics.num_rows = Precision::Inexact(rows);
    }
    Ok(Arc::new(statistics))
}

impl DisplayAs for BigQueryReadExec {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            formatter,
            "BigQueryReadExec: streams={}, fields={}",
            self.lease.streams().len(),
            self.output_schema.fields().len()
        )
    }
}

impl fmt::Debug for BigQueryReadExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BigQueryReadExec")
            .field("source_identity", self.lease.source_identity())
            .field("stream_count", &self.lease.streams().len())
            .field(
                "partition_count",
                &partition_count(self.lease.streams().len()),
            )
            .field("field_count", &self.output_schema.fields().len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn only_aggregate_rows_are_estimated_and_bytes_stay_absent() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
        let aggregate = execution_statistics(&schema, 2, Some(42), None).unwrap();
        let partition = execution_statistics(&schema, 2, Some(42), Some(0)).unwrap();

        assert_eq!(aggregate.num_rows, Precision::Inexact(42));
        assert_eq!(aggregate.total_byte_size, Precision::Absent);
        assert_eq!(aggregate.column_statistics.len(), 1);
        assert_eq!(partition.num_rows, Precision::Absent);
        assert_eq!(partition.total_byte_size, Precision::Absent);
        assert!(execution_statistics(&schema, 2, Some(42), Some(2)).is_err());
    }

    #[test]
    fn plan_is_bounded_incremental_unordered_and_one_partition_per_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let properties = plan_properties(schema, 3);

        assert!(matches!(
            properties.partitioning,
            Partitioning::UnknownPartitioning(3)
        ));
        assert_eq!(properties.emission_type, EmissionType::Incremental);
        assert_eq!(properties.boundedness, Boundedness::Bounded);
        assert!(properties.eq_properties.output_ordering().is_none());
    }

    #[test]
    fn empty_results_have_one_partition_and_exact_zero_statistics() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
        let properties = plan_properties(Arc::new(schema.clone()), partition_count(0));
        let statistics = execution_statistics(&schema, 0, None, Some(0)).unwrap();

        assert!(matches!(
            properties.partitioning,
            Partitioning::UnknownPartitioning(1)
        ));
        assert_eq!(statistics.num_rows, Precision::Exact(0));
        assert_eq!(statistics.total_byte_size, Precision::Exact(0));
        assert!(execution_statistics(&schema, 0, None, Some(1)).is_err());
    }
}
