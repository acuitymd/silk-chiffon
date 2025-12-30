//! Stream adapter for column projection on record batch streams.
//!
//! This module provides utilities for projecting (selecting) specific columns from a
//! `SendableRecordBatchStream`. Use this when you need lightweight column filtering
//! without the overhead of a full DataFusion query plan.
//!
//! For complex projections involving expressions, filtering, or aggregation, prefer
//! DataFusion's projection capabilities instead.

use futures::stream::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use datafusion::{
    error::DataFusionError,
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
};

pub struct ProjectedStream {
    input: SendableRecordBatchStream,
    indices: Vec<usize>,
    schema: SchemaRef,
}

impl ProjectedStream {
    pub fn new(
        input: SendableRecordBatchStream,
        indices: Vec<usize>,
    ) -> Result<Self, DataFusionError> {
        let input_schema = input.schema();

        for &idx in &indices {
            if idx >= input_schema.fields().len() {
                return Err(ArrowError::SchemaError(format!(
                    "Column index {} out of bounds for schema with {} columns",
                    idx,
                    input_schema.fields().len()
                ))
                .into());
            }
        }

        let projected_fields: Vec<_> = indices
            .iter()
            .map(|&i| input_schema.field(i).clone())
            .collect();

        let schema = Arc::new(Schema::new(projected_fields));

        Ok(Self {
            input,
            indices,
            schema,
        })
    }
}

impl Stream for ProjectedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => match batch.project(&self.indices) {
                Ok(projected) => Poll::Ready(Some(Ok(projected))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ProjectedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub fn project_stream(
    stream: SendableRecordBatchStream,
    indices: Vec<usize>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    Ok(Box::pin(ProjectedStream::new(stream, indices)?))
}

pub fn project_stream_with_column_names(
    stream: SendableRecordBatchStream,
    column_names: &[&str],
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let schema = stream.schema();

    let indices: Result<Vec<usize>, ArrowError> = column_names
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| ArrowError::SchemaError(format!("Column '{}' not found", name)))
        })
        .collect();

    project_stream(stream, indices?)
}

pub fn project_stream_with_excluded_column_names(
    stream: SendableRecordBatchStream,
    excluded_column_names: &[&str],
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let schema = stream.schema();

    let excluded_column_indices: Vec<usize> = excluded_column_names
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| ArrowError::SchemaError(format!("Column '{}' not found", name)))
        })
        .collect::<Result<Vec<usize>, ArrowError>>()?;

    let indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| !excluded_column_indices.contains(i))
        .map(|(i, _)| i)
        .collect::<Vec<usize>>();

    project_stream(stream, indices)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::physical_plan::memory::MemoryStream;
    use futures::StreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
            Field::new("value", arrow::datatypes::DataType::Int32, false),
        ]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap()
    }

    fn test_stream(schema: &SchemaRef) -> SendableRecordBatchStream {
        let batch = test_batch(schema);
        Box::pin(MemoryStream::try_new(vec![batch], Arc::clone(schema), None).unwrap())
    }

    #[tokio::test]
    async fn test_project_stream_by_indices() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let mut projected = project_stream(stream, vec![0, 2]).unwrap();

        assert_eq!(projected.schema().fields().len(), 2);
        assert_eq!(projected.schema().field(0).name(), "id");
        assert_eq!(projected.schema().field(1).name(), "value");

        let batch = projected.next().await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);

        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[10, 20, 30]);
    }

    #[tokio::test]
    async fn test_project_stream_reorders_columns() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let mut projected = project_stream(stream, vec![2, 0]).unwrap();

        assert_eq!(projected.schema().field(0).name(), "value");
        assert_eq!(projected.schema().field(1).name(), "id");

        let batch = projected.next().await.unwrap().unwrap();
        let first_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(first_col.values(), &[10, 20, 30]);
    }

    #[tokio::test]
    async fn test_project_stream_with_column_names() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let mut projected = project_stream_with_column_names(stream, &["name", "id"]).unwrap();

        assert_eq!(projected.schema().fields().len(), 2);
        assert_eq!(projected.schema().field(0).name(), "name");
        assert_eq!(projected.schema().field(1).name(), "id");

        let batch = projected.next().await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_project_stream_with_excluded_columns() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let mut projected = project_stream_with_excluded_column_names(stream, &["name"]).unwrap();

        assert_eq!(projected.schema().fields().len(), 2);
        assert_eq!(projected.schema().field(0).name(), "id");
        assert_eq!(projected.schema().field(1).name(), "value");

        let batch = projected.next().await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_project_stream_exclude_multiple_columns() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let mut projected =
            project_stream_with_excluded_column_names(stream, &["id", "value"]).unwrap();

        assert_eq!(projected.schema().fields().len(), 1);
        assert_eq!(projected.schema().field(0).name(), "name");

        let batch = projected.next().await.unwrap().unwrap();
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
    }

    #[test]
    fn test_project_stream_index_out_of_bounds() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let result = project_stream(stream, vec![0, 5]);
        let err = result.err().expect("should be an error");
        assert!(err.to_string().contains("out of bounds"));
    }

    #[test]
    fn test_project_stream_column_not_found() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let result = project_stream_with_column_names(stream, &["nonexistent"]);
        let err = result.err().expect("should be an error");
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_project_stream_excluded_column_not_found() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let result = project_stream_with_excluded_column_names(stream, &["nonexistent"]);
        let err = result.err().expect("should be an error");
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_project_stream_empty_indices() {
        let schema = test_schema();
        let stream = test_stream(&schema);

        let mut projected = project_stream(stream, vec![]).unwrap();

        assert_eq!(projected.schema().fields().len(), 0);

        let batch = projected.next().await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 3);
    }
}
