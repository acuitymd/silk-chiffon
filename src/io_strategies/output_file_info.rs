//! Output file information for partitioned writes.

use arrow::array::ArrayRef;
use arrow::util::display::ArrayFormatter;
use serde::Serialize;
use serde_json::Value;

use super::partitioner::PartitionValues;

/// Information about a single output file from a partitioned write.
#[derive(Debug, Clone, Serialize)]
pub struct OutputFileInfo {
    pub path: String,
    pub row_count: u64,
    pub partition_values: Vec<PartitionColumnValue>,
}

/// A single partition column and its value.
#[derive(Debug, Clone, Serialize)]
pub struct PartitionColumnValue {
    pub column: String,
    pub value: Value,
}

/// Convert partition values to a serializable format, preserving column order.
pub fn partition_values_to_json(
    values: &PartitionValues,
    column_order: &[String],
) -> Vec<PartitionColumnValue> {
    column_order
        .iter()
        .filter_map(|col| {
            values.get(col).map(|arr| PartitionColumnValue {
                column: col.clone(),
                value: array_to_json_value(arr),
            })
        })
        .collect()
}

// extracts scalar value from single-row Arrow array and converts to JSON
fn array_to_json_value(arr: &ArrayRef) -> Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if arr.is_empty() {
        return Value::Null;
    }

    if arr.is_null(0) {
        return Value::Null;
    }

    match arr.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(0))
        }
        DataType::Int8 => {
            let arr = arr.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::Int16 => {
            let arr = arr.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::Int32 => {
            let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::Int64 => {
            let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::UInt8 => {
            let arr = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::UInt16 => {
            let arr = arr.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::UInt32 => {
            let arr = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::UInt64 => {
            let arr = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            let val = f64::from(arr.value(0));
            serde_json::Number::from_f64(val)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = arr.value(0);
            serde_json::Number::from_f64(val)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Utf8 => {
            let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(0).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(0).to_string())
        }
        DataType::Date32 => {
            let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        DataType::Date64 => {
            let arr = arr.as_any().downcast_ref::<Date64Array>().unwrap();
            Value::Number(arr.value(0).into())
        }
        // for other types, convert to string representation
        _ => {
            let formatter = ArrayFormatter::try_new(arr.as_ref(), &Default::default());
            match formatter {
                Ok(fmt) => Value::String(fmt.value(0).to_string()),
                Err(_) => Value::String(format!("<{}>", arr.data_type())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_partition_values_to_json_preserves_order() {
        let mut values: PartitionValues = HashMap::new();
        values.insert(
            "year".to_string(),
            Arc::new(Int32Array::from(vec![2023])) as ArrayRef,
        );
        values.insert(
            "month".to_string(),
            Arc::new(Int32Array::from(vec![12])) as ArrayRef,
        );
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as ArrayRef,
        );

        let order = vec![
            "region".to_string(),
            "year".to_string(),
            "month".to_string(),
        ];
        let result = partition_values_to_json(&values, &order);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].column, "region");
        assert_eq!(result[0].value, Value::String("us-west".to_string()));
        assert_eq!(result[1].column, "year");
        assert_eq!(result[1].value, Value::Number(2023.into()));
        assert_eq!(result[2].column, "month");
        assert_eq!(result[2].value, Value::Number(12.into()));
    }

    #[test]
    fn test_array_to_json_handles_null() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![None as Option<i32>]));
        assert_eq!(array_to_json_value(&arr), Value::Null);
    }

    #[test]
    fn test_array_to_json_handles_empty() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![] as Vec<i32>));
        assert_eq!(array_to_json_value(&arr), Value::Null);
    }
}
