use std::{
    collections::HashMap,
    ops::Range,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{Result, anyhow};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::partition;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::Stream;

/// A HashMap of column names to single-row arrays representing a partition value for a column
pub type PartitionValues = HashMap<String, ArrayRef>;

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
