use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;

pub struct SliceCoalescer {
    schema: SchemaRef,
    target_batch_size: usize,
    pending_slices: Vec<RecordBatch>,
    pending_rows: usize,
}

impl SliceCoalescer {
    pub fn new(schema: SchemaRef, target_batch_size: usize) -> Self {
        Self {
            schema,
            target_batch_size,
            pending_slices: Vec::new(),
            pending_rows: 0,
        }
    }

    pub fn push_slice(&mut self, slice: RecordBatch) -> Result<Vec<RecordBatch>> {
        let slice_rows = slice.num_rows();
        let mut completed_batches = Vec::new();

        if slice_rows >= self.target_batch_size {
            if !self.pending_slices.is_empty() {
                completed_batches.push(self.flush_batch()?);
            }

            completed_batches.push(slice);
            return Ok(completed_batches);
        }

        if self.pending_rows + slice_rows > self.target_batch_size && self.pending_rows > 0 {
            completed_batches.push(self.flush_batch()?);
        }

        self.pending_slices.push(slice);
        self.pending_rows += slice_rows;

        if self.pending_rows >= self.target_batch_size {
            completed_batches.push(self.flush_batch()?);
        }

        Ok(completed_batches)
    }

    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending_slices.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.flush_batch()?))
        }
    }

    #[cfg(test)]
    pub fn pending_rows(&self) -> usize {
        self.pending_rows
    }

    fn flush_batch(&mut self) -> Result<RecordBatch> {
        assert!(!self.pending_slices.is_empty());

        let batch = if self.pending_slices.len() == 1 {
            self.pending_slices.pop().unwrap()
        } else {
            concat_batches(&self.schema, &self.pending_slices)?
        };

        self.pending_slices.clear();
        self.pending_rows = 0;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(start: i32, size: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let ids: Vec<i32> = (start..start + size as i32).collect();
        let values: Vec<String> = ids.iter().map(|i| format!("value_{}", i)).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_basic_coalescing() {
        let batch = create_test_batch(0, 100);
        let schema = batch.schema();
        let mut coalescer = SliceCoalescer::new(schema, 150);

        let slice1 = batch.slice(0, 50);
        let completed = coalescer.push_slice(slice1).unwrap();
        assert!(completed.is_empty());
        assert_eq!(coalescer.pending_rows(), 50);

        let slice2 = batch.slice(50, 50);
        let completed = coalescer.push_slice(slice2).unwrap();
        assert!(completed.is_empty());
        assert_eq!(coalescer.pending_rows(), 100);

        let batch2 = create_test_batch(100, 100);
        let slice3 = batch2.slice(0, 60);
        let completed = coalescer.push_slice(slice3).unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].num_rows(), 100);
        assert_eq!(coalescer.pending_rows(), 60);
    }

    #[test]
    fn test_large_slice() {
        let batch = create_test_batch(0, 200);
        let schema = batch.schema();
        let mut coalescer = SliceCoalescer::new(schema, 100);

        let large_slice = batch.slice(0, 150);
        let completed = coalescer.push_slice(large_slice).unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].num_rows(), 150);
        assert_eq!(coalescer.pending_rows(), 0);
    }

    #[test]
    fn test_cross_batch_coalescing() {
        let batch1 = create_test_batch(0, 100);
        let batch2 = create_test_batch(100, 100);
        let schema = batch1.schema();
        let mut coalescer = SliceCoalescer::new(schema, 120);

        let slice1 = batch1.slice(0, 80);
        let completed = coalescer.push_slice(slice1).unwrap();
        assert!(completed.is_empty());

        let slice2 = batch2.slice(0, 50);
        let completed = coalescer.push_slice(slice2).unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].num_rows(), 80);
        assert_eq!(coalescer.pending_rows(), 50);

        let ids = completed[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 0);
        assert_eq!(ids.value(79), 79);
    }

    #[test]
    fn test_flush() {
        let batch = create_test_batch(0, 100);
        let schema = batch.schema();
        let mut coalescer = SliceCoalescer::new(schema, 1000);

        let slice = batch.slice(0, 30);
        let completed = coalescer.push_slice(slice).unwrap();
        assert!(completed.is_empty());

        let flushed = coalescer.flush().unwrap();
        assert!(flushed.is_some());
        assert_eq!(flushed.unwrap().num_rows(), 30);
        assert_eq!(coalescer.pending_rows(), 0);

        let flushed = coalescer.flush().unwrap();
        assert!(flushed.is_none());
    }
}
