//! Typed column extraction helpers for assertions.

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};

pub struct TestExtract;

impl TestExtract {
    /// panics on null
    pub fn i32(batch: &RecordBatch, column: &str) -> Vec<i32> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not Int32"));
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    pub fn i32_nullable(batch: &RecordBatch, column: &str) -> Vec<Option<i32>> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not Int32"));
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    /// panics on null
    pub fn i64(batch: &RecordBatch, column: &str) -> Vec<i64> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not Int64"));
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    /// panics on null
    pub fn string(batch: &RecordBatch, column: &str) -> Vec<String> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column '{column}' is not String"));
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
    }

    pub fn string_nullable(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column '{column}' is not String"));
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect()
    }

    pub fn i32_all(batches: &[RecordBatch], column: &str) -> Vec<i32> {
        batches.iter().flat_map(|b| Self::i32(b, column)).collect()
    }

    pub fn string_all(batches: &[RecordBatch], column: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| Self::string(b, column))
            .collect()
    }
}
