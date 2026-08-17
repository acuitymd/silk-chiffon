use std::{
    collections::HashMap,
    ops::Range,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{Result, anyhow};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::partition;
use arrow::datatypes::{DataType, Schema};
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::Stream;

use crate::io_strategies::output_file_info::format_scalar_value;

/// A HashMap of column names to single-row arrays representing a partition value for a column
pub type PartitionValues = HashMap<String, ArrayRef>;

/// Compare two PartitionValues by their array contents, not pointers.
/// Returns true if both have the same keys and all arrays are equal.
pub fn partition_values_equal(a: &PartitionValues, b: &PartitionValues) -> bool {
    if a.len() != b.len() {
        return false;
    }

    a.iter().all(|(key, a_array)| {
        b.get(key)
            .map(|b_array| a_array.as_ref() == b_array.as_ref())
            .unwrap_or(false)
    })
}

/// Create a hashable string key from partition values.
/// Uses length-prefix encoding to avoid collisions when values contain delimiters.
/// Null values use "!" marker to distinguish from the string "null".
///
/// Examples:
/// - ["us-west", "2024"] -> "7:us-west,4:2024"
/// - ["a|b", "c"] -> "3:a|b,1:c" (no collision with ["a", "b|c"] -> "1:a,3:b|c")
/// - ["us-west", NULL] -> "7:us-west,!" (no collision with ["us-west", "null"] -> "7:us-west,4:null")
/// - ["us-west", ""] -> "7:us-west,0:" (empty string is distinct from null)
pub fn partition_key(values: &PartitionValues, column_order: &[String]) -> String {
    column_order
        .iter()
        .map(|col| match values.get(col) {
            Some(arr) if arr.is_null(0) => "!".to_string(),
            arr => {
                let v = format_scalar_value(arr);
                format!("{}:{}", v.len(), v)
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Check if a data type is primitive (supported for partitioning).
pub fn is_primitive_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Validate that all partition columns are primitive types.
pub fn validate_partition_columns_primitive(schema: &Schema, columns: &[String]) -> Result<()> {
    for col in columns {
        let field = schema
            .field_with_name(col)
            .map_err(|_| anyhow!("partition column '{}' not found in schema", col))?;
        if !is_primitive_type(field.data_type()) {
            anyhow::bail!(
                "partition column '{}' has non-primitive type {:?}; \
                 only primitive types are supported for partitioning",
                col,
                field.data_type()
            );
        }
    }
    Ok(())
}

/// Stream that partitions record batches by column values, yielding
/// (partition_values, sliced_batch) tuples where partition_values contains
/// single-row arrays for each partition column.
///
/// NOTE: This will yield partial ranges. You are expected to batch these up yourself
///       in the consumer of the stream.
pub struct PartitionedBatchStream {
    inner: SendableRecordBatchStream,
    columns: Vec<String>,
    current_batch: Option<RecordBatch>,
    current_ranges: Option<Vec<Range<usize>>>,
    current_range_idx: usize,
}

impl PartitionedBatchStream {
    pub fn new(stream: SendableRecordBatchStream, columns: Vec<String>) -> Self {
        Self {
            inner: stream,
            columns,
            current_batch: None,
            current_ranges: None,
            current_range_idx: 0,
        }
    }

    fn extract_partition_values(
        batch: &RecordBatch,
        row_idx: usize,
        column_names: &[String],
    ) -> Result<PartitionValues> {
        column_names
            .iter()
            .map(|name| {
                let array = batch
                    .column_by_name(name)
                    .ok_or_else(|| anyhow!("Partition column '{}' not found in batch", name))?;
                Ok((name.clone(), array.slice(row_idx, 1)))
            })
            .collect()
    }
}

impl Stream for PartitionedBatchStream {
    type Item = Result<(PartitionValues, RecordBatch)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // if we have ranges to process, yield the next one
            if self.current_batch.is_some() && self.current_ranges.is_some() {
                let ranges = self.current_ranges.as_ref().unwrap();
                if self.current_range_idx < ranges.len() {
                    let range = ranges[self.current_range_idx].clone();
                    self.current_range_idx += 1;

                    let batch = self.current_batch.as_ref().unwrap();

                    // slice the batch for this partition group
                    let sliced = batch.slice(range.start, range.end - range.start);

                    // extract partition values from first row of slice
                    let partition_values =
                        match Self::extract_partition_values(&sliced, 0, &self.columns) {
                            Ok(values) => values,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        };

                    return Poll::Ready(Some(Ok((partition_values, sliced))));
                }

                // exhausted current batch's ranges, clear state and get next batch
                self.current_batch = None;
                self.current_ranges = None;
                self.current_range_idx = 0;
            }

            // poll for next batch from inner stream
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // compute partition ranges using arrow::compute::partition
                    // super efficient way to get the partitions across a list of columns
                    let partition_columns: Vec<ArrayRef> = self
                        .columns
                        .iter()
                        .filter_map(|name| batch.column_by_name(name).cloned())
                        .collect();

                    if partition_columns.len() != self.columns.len() {
                        return Poll::Ready(Some(Err(anyhow!(
                            "Not all partition columns found in batch"
                        ))));
                    }

                    let partitions = match partition(&partition_columns) {
                        Ok(p) => p,
                        Err(e) => return Poll::Ready(Some(Err(e.into()))),
                    };

                    // store batch and ranges, reset index
                    self.current_batch = Some(batch);
                    self.current_ranges = Some(partitions.ranges().to_vec());
                    self.current_range_idx = 0;

                    // continue loop to process first range
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub struct Partitioner {
    columns: Vec<String>,
}

impl Partitioner {
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }

    pub fn partition_stream(&self, stream: SendableRecordBatchStream) -> PartitionedBatchStream {
        PartitionedBatchStream::new(stream, self.columns.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };
    use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
    use futures::{StreamExt, stream};

    use super::*;

    fn create_test_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    #[tokio::test]
    async fn test_single_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["category".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_multiple_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["category".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1.num_rows(), 2); // category 1
        assert_eq!(results[1].1.num_rows(), 2); // category 2
        assert_eq!(results[2].1.num_rows(), 2); // category 3

        // verify partition values
        let val1 = results[0].0.get("category").unwrap();
        let arr1 = val1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr1.value(0), 1);
    }

    #[tokio::test]
    async fn test_partition_with_string_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["us-west", "us-west", "us-east"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["region".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.num_rows(), 2); // us-west
        assert_eq!(results[1].1.num_rows(), 1); // us-east
    }

    #[tokio::test]
    async fn test_partition_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 2])),
                Arc::new(Int32Array::from(vec![30, 40, 50])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch1, batch2]);
        let partitioner = Partitioner::new(vec!["category".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        // should get 3 slices: first batch (category 1), second batch first part (category 1), second batch second part (category 2)
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_partition_multi_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int32, false),
            Field::new("month", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2024, 2024, 2024, 2025, 2025])),
                Arc::new(Int32Array::from(vec![1, 1, 2, 1, 1])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["year".to_string(), "month".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        // should have 3 partitions: (2024,1), (2024,2), (2025,1)
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1.num_rows(), 2); // 2024-01
        assert_eq!(results[1].1.num_rows(), 1); // 2024-02
        assert_eq!(results[2].1.num_rows(), 2); // 2025-01

        // verify BOTH partition columns are present and have correct values
        // partition 0: (2024, 1)
        assert!(
            results[0].0.contains_key("year"),
            "partition 0 missing 'year' key"
        );
        assert!(
            results[0].0.contains_key("month"),
            "partition 0 missing 'month' key"
        );
        let year0 = results[0]
            .0
            .get("year")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let month0 = results[0]
            .0
            .get("month")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(year0.value(0), 2024);
        assert_eq!(month0.value(0), 1);

        // partition 1: (2024, 2)
        let year1 = results[1]
            .0
            .get("year")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let month1 = results[1]
            .0
            .get("month")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(year1.value(0), 2024);
        assert_eq!(month1.value(0), 2);

        // partition 2: (2025, 1)
        let year2 = results[2]
            .0
            .get("year")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let month2 = results[2]
            .0
            .get("month")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(year2.value(0), 2025);
        assert_eq!(month2.value(0), 1);
    }

    #[tokio::test]
    async fn test_partition_values_are_single_row_arrays() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["category".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        if let Some(Ok((values, _batch))) = partitioned_stream.next().await {
            let category_array = values.get("category").unwrap();
            // verify it's a single-row array
            assert_eq!(category_array.len(), 1);
        } else {
            panic!("Expected at least one result");
        }
    }

    #[test]
    fn test_partition_values_equal_same_values() {
        // test that partition_values_equal returns true for same values with different pointers
        let mut values1 = HashMap::new();
        values1.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );
        values1.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );
        values2.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        );

        // different pointers, same values
        assert!(partition_values_equal(&values1, &values2));
    }

    #[test]
    fn test_partition_values_equal_different_values() {
        let mut values1 = HashMap::new();
        values1.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-east"])) as ArrayRef,
        );

        assert!(!partition_values_equal(&values1, &values2));
    }

    #[test]
    fn test_partition_values_equal_different_keys() {
        let mut values1 = HashMap::new();
        values1.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "zone".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );

        assert!(!partition_values_equal(&values1, &values2));
    }

    #[test]
    fn test_partition_values_equal_different_lengths() {
        let mut values1 = HashMap::new();
        values1.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );
        values2.insert(
            "zone".to_string(),
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
        );

        assert!(!partition_values_equal(&values1, &values2));
    }

    #[test]
    fn test_partition_values_equal_with_nulls_same() {
        let mut values1 = HashMap::new();
        values1.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        );

        assert!(partition_values_equal(&values1, &values2));
    }

    #[test]
    fn test_partition_values_equal_with_nulls_different() {
        let mut values1 = HashMap::new();
        values1.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
        );

        assert!(!partition_values_equal(&values1, &values2));
    }

    #[test]
    fn test_partition_values_equal_with_nulls_multi_column() {
        let mut values1 = HashMap::new();
        values1.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec![Some("us-west")])) as ArrayRef,
        );
        values1.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec![Some("us-west")])) as ArrayRef,
        );
        values2.insert(
            "category".to_string(),
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        );

        assert!(partition_values_equal(&values1, &values2));
    }

    #[tokio::test]
    async fn test_partition_stream_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, true), // nullable
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, None, Some(2)])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["category".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        // should have 3 partitions: category=1, category=null, category=2
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1.num_rows(), 1); // category 1
        assert_eq!(results[1].1.num_rows(), 2); // category null
        assert_eq!(results[2].1.num_rows(), 1); // category 2

        // verify first partition has value 1
        let cat1 = results[0].0.get("category").unwrap();
        let arr1 = cat1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(!arr1.is_null(0));
        assert_eq!(arr1.value(0), 1);

        // verify second partition has null
        let cat_null = results[1].0.get("category").unwrap();
        let arr_null = cat_null.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(arr_null.is_null(0));

        // verify third partition has value 2
        let cat2 = results[2].0.get("category").unwrap();
        let arr2 = cat2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(!arr2.is_null(0));
        assert_eq!(arr2.value(0), 2);
    }

    #[tokio::test]
    async fn test_partition_stream_multi_column_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int32, true),
            Field::new("month", DataType::Int32, true),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(2024),
                    Some(2024),
                    None,
                    None,
                    Some(2024),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    None,
                ])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["year".to_string(), "month".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        // should have 4 partitions: (2024,1), (null,2), (null,null), (2024,null)
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].1.num_rows(), 2); // 2024-01
        assert_eq!(results[1].1.num_rows(), 1); // null-02
        assert_eq!(results[2].1.num_rows(), 1); // null-null
        assert_eq!(results[3].1.num_rows(), 1); // 2024-null

        // verify first partition: (2024, 1)
        let year0 = results[0].0.get("year").unwrap();
        let year_arr0 = year0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(year_arr0.value(0), 2024);
        let month0 = results[0].0.get("month").unwrap();
        let month_arr0 = month0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(month_arr0.value(0), 1);

        // verify second partition: (null, 2)
        let year1 = results[1].0.get("year").unwrap();
        let year_arr1 = year1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(year_arr1.is_null(0));
        let month1 = results[1].0.get("month").unwrap();
        let month_arr1 = month1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(month_arr1.value(0), 2);

        // verify third partition: (null, null)
        let year2 = results[2].0.get("year").unwrap();
        let year_arr2 = year2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(year_arr2.is_null(0));
        let month2 = results[2].0.get("month").unwrap();
        let month_arr2 = month2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(month_arr2.is_null(0));
    }

    #[tokio::test]
    async fn test_partition_stream_string_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("us-west"),
                    Some("us-west"),
                    None,
                    Some("us-east"),
                ])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["region".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            results.push((values, batch));
        }

        // should have 3 partitions: us-west, null, us-east
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1.num_rows(), 2); // us-west
        assert_eq!(results[1].1.num_rows(), 1); // null
        assert_eq!(results[2].1.num_rows(), 1); // us-east

        // verify null partition
        let region_null = results[1].0.get("region").unwrap();
        let arr_null = region_null.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(arr_null.is_null(0));
    }

    #[test]
    fn test_partition_key_single_column() {
        let mut values = HashMap::new();
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );

        let key = partition_key(&values, &["region".to_string()]);
        // length-prefix format: "7:us-west"
        assert_eq!(key, "7:us-west");
    }

    #[test]
    fn test_partition_key_multi_column() {
        let mut values = HashMap::new();
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );
        values.insert(
            "year".to_string(),
            Arc::new(Int32Array::from(vec![2024])) as ArrayRef,
        );

        // order matters - length-prefix format
        let key1 = partition_key(&values, &["region".to_string(), "year".to_string()]);
        let key2 = partition_key(&values, &["year".to_string(), "region".to_string()]);
        assert_eq!(key1, "7:us-west,4:2024");
        assert_eq!(key2, "4:2024,7:us-west");
    }

    #[test]
    fn test_partition_key_with_null() {
        let mut values = HashMap::new();
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec![None as Option<&str>])) as ArrayRef,
        );

        let key = partition_key(&values, &["region".to_string()]);
        // null uses special "!" marker
        assert_eq!(key, "!");
    }

    #[test]
    fn test_partition_key_null_vs_string_null() {
        // actual null should not collide with the string "null"
        let mut null_value = HashMap::new();
        null_value.insert(
            "x".to_string(),
            Arc::new(StringArray::from(vec![None as Option<&str>])) as ArrayRef,
        );

        let mut string_null = HashMap::new();
        string_null.insert(
            "x".to_string(),
            Arc::new(StringArray::from(vec!["null"])) as ArrayRef,
        );

        let key1 = partition_key(&null_value, &["x".to_string()]);
        let key2 = partition_key(&string_null, &["x".to_string()]);

        assert_ne!(key1, key2);
        assert_eq!(key1, "!");
        assert_eq!(key2, "4:null");
    }

    #[test]
    fn test_partition_key_empty_string() {
        // empty string should be distinct from null
        let mut empty = HashMap::new();
        empty.insert(
            "x".to_string(),
            Arc::new(StringArray::from(vec![""])) as ArrayRef,
        );

        let mut null_value = HashMap::new();
        null_value.insert(
            "x".to_string(),
            Arc::new(StringArray::from(vec![None as Option<&str>])) as ArrayRef,
        );

        let empty_key = partition_key(&empty, &["x".to_string()]);
        let null_key = partition_key(&null_value, &["x".to_string()]);

        assert_eq!(empty_key, "0:");
        assert_eq!(null_key, "!");
        assert_ne!(empty_key, null_key);
    }

    #[test]
    fn test_partition_key_collision_avoided() {
        // test that values containing delimiters don't collide
        let mut values1 = HashMap::new();
        values1.insert(
            "a".to_string(),
            Arc::new(StringArray::from(vec!["x,y"])) as ArrayRef,
        );
        values1.insert(
            "b".to_string(),
            Arc::new(StringArray::from(vec!["z"])) as ArrayRef,
        );

        let mut values2 = HashMap::new();
        values2.insert(
            "a".to_string(),
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        );
        values2.insert(
            "b".to_string(),
            Arc::new(StringArray::from(vec!["y,z"])) as ArrayRef,
        );

        let key1 = partition_key(&values1, &["a".to_string(), "b".to_string()]);
        let key2 = partition_key(&values2, &["a".to_string(), "b".to_string()]);

        // these should NOT be equal due to length-prefix encoding
        assert_ne!(key1, key2);
        assert_eq!(key1, "3:x,y,1:z");
        assert_eq!(key2, "1:x,3:y,z");
    }

    #[test]
    fn test_is_primitive_type() {
        // primitive types
        assert!(is_primitive_type(&DataType::Boolean));
        assert!(is_primitive_type(&DataType::Int32));
        assert!(is_primitive_type(&DataType::Int64));
        assert!(is_primitive_type(&DataType::Float64));
        assert!(is_primitive_type(&DataType::Utf8));
        assert!(is_primitive_type(&DataType::Date32));
        assert!(is_primitive_type(&DataType::Timestamp(
            TimeUnit::Millisecond,
            None
        )));

        // complex types
        assert!(!is_primitive_type(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true
        )))));
        assert!(!is_primitive_type(&DataType::Struct(
            vec![Field::new("a", DataType::Int32, true)].into()
        )));
    }

    #[test]
    fn test_validate_partition_columns_primitive_success() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let result =
            validate_partition_columns_primitive(&schema, &["id".to_string(), "name".to_string()]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_partition_columns_primitive_fails_for_list() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        )]);

        let result = validate_partition_columns_primitive(&schema, &["tags".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-primitive"));
    }

    #[test]
    fn test_validate_partition_columns_primitive_missing_column() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let result = validate_partition_columns_primitive(&schema, &["missing".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_partitioner_interleaved() {
        // unsorted input: categories are interleaved
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["b", "a", "b", "a", "c"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let partitioner = Partitioner::new(vec!["category".to_string()]);
        let mut partitioned_stream = partitioner.partition_stream(stream);

        let mut results = Vec::new();
        while let Some(Ok((values, batch))) = partitioned_stream.next().await {
            let cat = values
                .get("category")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            results.push((cat.to_string(), batch.num_rows()));
        }

        // without sorting, partitions are fragmented: b(1), a(1), b(1), a(1), c(1)
        assert_eq!(results.len(), 5);
        assert_eq!(results[0], ("b".to_string(), 1));
        assert_eq!(results[1], ("a".to_string(), 1));
        assert_eq!(results[2], ("b".to_string(), 1));
        assert_eq!(results[3], ("a".to_string(), 1));
        assert_eq!(results[4], ("c".to_string(), 1));
    }
}
