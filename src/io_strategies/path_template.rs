use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::io_strategies::partitioner::PartitionValues;

const NULL_VALUE: &str = "__NULL__";

pub struct PathTemplate {
    pattern: String,
}

impl PathTemplate {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    pub fn resolve(&self, values: &PartitionValues) -> String {
        let mut result = self.pattern.clone();
        for (column, value) in values {
            let formatter =
                ArrayFormatter::try_new(value, &FormatOptions::default().with_null(NULL_VALUE))
                    .unwrap();
            result = result.replace(&format!("{{{column}}}"), &formatter.value(0).to_string());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
        TimestampMicrosecondArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_resolve_with_single_column() {
        let template = PathTemplate::new("output/{year}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "year".to_string(),
            Arc::new(Int32Array::from(vec![2024])) as _,
        );

        let result = template.resolve(&values);
        assert_eq!(result, "output/2024.parquet");
    }

    #[test]
    fn test_resolve_with_multiple_columns() {
        let template = PathTemplate::new("output/{year}/{month}/{day}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "year".to_string(),
            Arc::new(Int32Array::from(vec![2024])) as _,
        );
        values.insert(
            "month".to_string(),
            Arc::new(Int32Array::from(vec![11])) as _,
        );
        values.insert("day".to_string(), Arc::new(Int32Array::from(vec![22])) as _);

        let result = template.resolve(&values);
        assert_eq!(result, "output/2024/11/22.parquet");
    }

    #[test]
    fn test_resolve_with_string_column() {
        let template = PathTemplate::new("output/{region}/{city}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as _,
        );
        values.insert(
            "city".to_string(),
            Arc::new(StringArray::from(vec!["seattle"])) as _,
        );

        let result = template.resolve(&values);
        assert_eq!(result, "output/us-west/seattle.parquet");
    }

    #[test]
    fn test_resolve_with_null_value() {
        let template = PathTemplate::new("output/{category}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "category".to_string(),
            Arc::new(StringArray::from(vec![None::<&str>])) as _,
        );

        let result = template.resolve(&values);
        assert_eq!(result, "output/__NULL__.parquet");
    }

    #[test]
    fn test_resolve_with_no_placeholders() {
        let template = PathTemplate::new("output/data.parquet".to_string());
        let values = HashMap::new();

        let result = template.resolve(&values);
        assert_eq!(result, "output/data.parquet");
    }

    #[test]
    fn test_resolve_with_repeated_placeholder() {
        let template = PathTemplate::new("output/{id}/{id}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert("id".to_string(), Arc::new(Int32Array::from(vec![42])) as _);

        let result = template.resolve(&values);
        assert_eq!(result, "output/42/42.parquet");
    }

    #[test]
    fn test_all_integer_types() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // Int8
        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(Int8Array::from(vec![127])) as _);
        assert_eq!(template.resolve(&values), "output/127.parquet");

        // Int16
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Int16Array::from(vec![32767])) as _,
        );
        assert_eq!(template.resolve(&values), "output/32767.parquet");

        // Int64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Int64Array::from(vec![9223372036854775807i64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/9223372036854775807.parquet"
        );

        // UInt8
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt8Array::from(vec![255])) as _,
        );
        assert_eq!(template.resolve(&values), "output/255.parquet");

        // UInt16
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt16Array::from(vec![65535])) as _,
        );
        assert_eq!(template.resolve(&values), "output/65535.parquet");

        // UInt32
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt32Array::from(vec![4294967295])) as _,
        );
        assert_eq!(template.resolve(&values), "output/4294967295.parquet");

        // UInt64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt64Array::from(vec![18446744073709551615u64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/18446744073709551615.parquet"
        );
    }

    #[test]
    fn test_float_types() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // Float32
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Float32Array::from(vec![1.23f32])) as _,
        );
        assert_eq!(template.resolve(&values), "output/1.23.parquet");

        // Float64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Float64Array::from(vec![4.56789])) as _,
        );
        assert_eq!(template.resolve(&values), "output/4.56789.parquet");
    }

    #[test]
    fn test_boolean_type() {
        let template = PathTemplate::new("output/{flag}.parquet".to_string());

        // true
        let mut values = HashMap::new();
        values.insert(
            "flag".to_string(),
            Arc::new(BooleanArray::from(vec![true])) as _,
        );
        assert_eq!(template.resolve(&values), "output/true.parquet");

        // false
        let mut values = HashMap::new();
        values.insert(
            "flag".to_string(),
            Arc::new(BooleanArray::from(vec![false])) as _,
        );
        assert_eq!(template.resolve(&values), "output/false.parquet");
    }

    #[test]
    fn test_string_types() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // Utf8 (already tested in test_resolve_with_string_column)

        // LargeUtf8
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(LargeStringArray::from(vec!["large-string"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/large-string.parquet");
    }

    #[test]
    fn test_date_and_time_types() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // Date32 (days since epoch) - formats as YYYY-MM-DD
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Date32Array::from(vec![19000])) as _,
        );
        assert_eq!(template.resolve(&values), "output/2022-01-08.parquet");

        // Date64 (milliseconds since epoch) - formats as ISO8601 with time
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Date64Array::from(vec![1640995200000i64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/2022-01-01T00:00:00.parquet"
        );

        // TimestampNanosecond - formats as ISO8601 (2022-01-01 15:30:45)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampNanosecondArray::from(vec![1641051045000000000i64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/2022-01-01T15:30:45.parquet"
        );

        // TimestampMicrosecond - formats as ISO8601 (2022-01-01 15:30:45)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampMicrosecondArray::from(vec![1641051045000000i64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/2022-01-01T15:30:45.parquet"
        );
    }

    #[test]
    fn test_binary_types() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // Binary
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(BinaryArray::from_vec(vec![b"hello"])) as _,
        );
        let result = template.resolve(&values);
        assert!(result.starts_with("output/"));
        assert!(result.ends_with(".parquet"));

        // LargeBinary
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(LargeBinaryArray::from_vec(vec![b"world"])) as _,
        );
        let result = template.resolve(&values);
        assert!(result.starts_with("output/"));
        assert!(result.ends_with(".parquet"));
    }

    #[test]
    fn test_decimal_type() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // Decimal128 with precision 10, scale 2 (e.g., for money: 123.45)
        let mut values = HashMap::new();
        let decimal_array =
            Decimal128Array::from(vec![12345]).with_data_type(DataType::Decimal128(10, 2));
        values.insert("val".to_string(), Arc::new(decimal_array) as _);
        assert_eq!(template.resolve(&values), "output/123.45.parquet");
    }

    #[test]
    fn test_null_values_for_various_types() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // Null Int32
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Int32Array::from(vec![None])) as _,
        );
        assert_eq!(template.resolve(&values), "output/__NULL__.parquet");

        // Null Float64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Float64Array::from(vec![None])) as _,
        );
        assert_eq!(template.resolve(&values), "output/__NULL__.parquet");

        // Null Boolean
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(BooleanArray::from(vec![None])) as _,
        );
        assert_eq!(template.resolve(&values), "output/__NULL__.parquet");

        // Null String (already tested but included for completeness)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec![None::<&str>])) as _,
        );
        assert_eq!(template.resolve(&values), "output/__NULL__.parquet");
    }

    #[test]
    fn test_list_type() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // create a list array [1, 2, 3]
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let list_array = ListArray::new(field, offsets, Arc::new(values_data), None);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(list_array) as _);

        let result = template.resolve(&values);
        assert_eq!(result, "output/[1, 2, 3].parquet");
    }

    #[test]
    fn test_struct_type() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // create a struct with fields {name: "Alice", age: 30}
        let name_array = Arc::new(StringArray::from(vec!["Alice"]));
        let age_array = Arc::new(Int32Array::from(vec![30]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                name_array as ArrayRef,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                age_array as ArrayRef,
            ),
        ]);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(struct_array) as _);

        let result = template.resolve(&values);
        assert_eq!(result, "output/{name: Alice, age: 30}.parquet");
    }

    #[test]
    fn test_map_type() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // create a map {"key1": 100}
        let keys = Arc::new(StringArray::from(vec!["key1"]));
        let values_arr = Arc::new(Int32Array::from(vec![100]));

        let entry_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                keys as ArrayRef,
            ),
            (
                Arc::new(Field::new("values", DataType::Int32, false)),
                values_arr as ArrayRef,
            ),
        ]);

        let entry_offsets = OffsetBuffer::new(vec![0, 1].into());
        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("keys", DataType::Utf8, false)),
                    Arc::new(Field::new("values", DataType::Int32, false)),
                ]
                .into(),
            ),
            false,
        ));

        let map_array = MapArray::new(map_field, entry_offsets, entry_struct, None, false);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(map_array) as _);

        let result = template.resolve(&values);
        // map formatting varies, just verify it works
        assert!(result.starts_with("output/"));
        assert!(result.ends_with(".parquet"));
    }

    #[test]
    fn test_nested_list() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // create a list of lists [[1, 2], [3]]
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let inner_offsets = OffsetBuffer::new(vec![0, 2, 3].into());
        let inner_field = Arc::new(Field::new("item", DataType::Int32, false));
        let inner_list = ListArray::new(
            Arc::clone(&inner_field),
            inner_offsets,
            Arc::new(values_data),
            None,
        );

        let outer_offsets = OffsetBuffer::new(vec![0, 2].into()); // includes both inner lists
        let outer_field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::clone(&inner_field)),
            false,
        ));
        let outer_list = ListArray::new(outer_field, outer_offsets, Arc::new(inner_list), None);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(outer_list) as _);

        let result = template.resolve(&values);
        assert_eq!(result, "output/[[1, 2], [3]].parquet");
    }
}
