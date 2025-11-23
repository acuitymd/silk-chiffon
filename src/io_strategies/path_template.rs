//! Path template resolution with Hive-style percent encoding.
//!
//! This module provides [`PathTemplate`] for resolving partition value placeholders
//! in path patterns. All partition values are automatically percent-encoded according
//! to Apache Hive's partitioning conventions before substitution.
//!
//! Uses the [`percent-encoding`](https://docs.rs/percent-encoding/) crate with a custom
//! character set matching Hive's behavior.
//!
//! # Encoding Rules
//!
//! Matches the behavior of `org.apache.hadoop.hive.common.FileUtils.escapePathName`:
//!
//! - **Null or empty values**: Replaced with `__HIVE_DEFAULT_PARTITION__`
//! - **Control characters** (ASCII 0x00-0x1F and 0x7F) are encoded as `%XX`
//! - **Special characters** are encoded: `"` `#` `%` `'` `*` `/` `:` `=` `?` `\` `{` `[` `]` `^`
//! - **Not encoded**: alphanumerics, spaces, commas, closing braces `}`, hyphens, underscores, dots
//!
//! # Examples
//!
//! ```
//! # use std::collections::HashMap;
//! # use std::sync::Arc;
//! # use arrow::array::{Int32Array, StringArray};
//! # use silk_chiffon::io_strategies::path_template::PathTemplate;
//! let template = PathTemplate::new("data/{region}/{date}.parquet".to_string());
//!
//! let mut values = HashMap::new();
//! values.insert("region".to_string(), Arc::new(StringArray::from(vec!["us-west"])) as _);
//! values.insert("date".to_string(), Arc::new(StringArray::from(vec!["2024/01/15"])) as _);
//!
//! // Forward slashes in partition values are escaped
//! let result = template.resolve(&values);
//! assert_eq!(result, "data/us-west/2024%2F01%2F15.parquet");
//! ```

use arrow::util::display::{ArrayFormatter, FormatOptions};
use percent_encoding::{AsciiSet, CONTROLS, percent_encode};

use crate::io_strategies::partitioner::PartitionValues;

/// Default value for null or empty partition values, matching Hive's default.
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Hive partition encoding set: control characters plus special characters.
/// Matches org.apache.hadoop.hive.common.FileUtils.escapePathName behavior.
const HIVE_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'\'')
    .add(b'*')
    .add(b'/')
    .add(b':')
    .add(b'=')
    .add(b'?')
    .add(b'\\')
    .add(b'{')
    .add(b'[')
    .add(b']')
    .add(b'^');

/// Template for generating partition paths with Hive-style percent encoding.
///
/// Resolves placeholders in path patterns by substituting partition values.
/// All values are automatically percent-encoded according to Hive partitioning rules.
pub struct PathTemplate {
    pattern: String,
}

impl PathTemplate {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    /// Escape a path name according to Hive partitioning conventions.
    /// This matches the behavior of org.apache.hadoop.hive.common.FileUtils.escapePathName
    ///
    /// Null or empty strings return `__HIVE_DEFAULT_PARTITION__`.
    fn escape_path_name(path: &str) -> String {
        if path.is_empty() {
            return HIVE_DEFAULT_PARTITION.to_string();
        }

        percent_encode(path.as_bytes(), HIVE_ENCODE_SET).to_string()
    }

    /// Resolve partition value placeholders in the template pattern.
    ///
    /// Replaces `{{column_name}}` placeholders with the corresponding partition values.
    /// Values are formatted using Arrow's display formatting and then percent-encoded
    /// according to Hive partitioning rules.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::collections::HashMap;
    /// # use std::sync::Arc;
    /// # use arrow::array::StringArray;
    /// # use silk_chiffon::io_strategies::path_template::PathTemplate;
    /// let template = PathTemplate::new("output/{region}/{date}.parquet".to_string());
    ///
    /// let mut values = HashMap::new();
    /// values.insert("region".to_string(), Arc::new(StringArray::from(vec!["US/West"])) as _);
    /// values.insert("date".to_string(), Arc::new(StringArray::from(vec!["2024:01:15"])) as _);
    ///
    /// let path = template.resolve(&values);
    /// // Special characters / and : are percent-encoded
    /// assert_eq!(path, "output/US%2FWest/2024%3A01%3A15.parquet");
    /// ```
    pub fn resolve(&self, values: &PartitionValues) -> String {
        let mut result = self.pattern.clone();
        for (column, value) in values {
            let formatter = ArrayFormatter::try_new(
                value,
                &FormatOptions::default().with_null(HIVE_DEFAULT_PARTITION),
            )
            .unwrap();
            let value_str = formatter.value(0).to_string();
            let escaped = Self::escape_path_name(&value_str);
            result = result.replace(&format!("{{{column}}}"), &escaped);
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
        assert_eq!(result, "output/__HIVE_DEFAULT_PARTITION__.parquet");
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

        // Date64 (milliseconds since epoch) - formats as ISO8601 with time (colons escaped)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Date64Array::from(vec![1640995200000i64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/2022-01-01T00%3A00%3A00.parquet"
        );

        // TimestampNanosecond - formats as ISO8601 with colons escaped
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampNanosecondArray::from(vec![1641051045000000000i64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/2022-01-01T15%3A30%3A45.parquet"
        );

        // TimestampMicrosecond - formats as ISO8601 with colons escaped
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampMicrosecondArray::from(vec![1641051045000000i64])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/2022-01-01T15%3A30%3A45.parquet"
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
        assert_eq!(
            template.resolve(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );

        // Null Float64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Float64Array::from(vec![None])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );

        // Null Boolean
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(BooleanArray::from(vec![None])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );

        // Null String (already tested but included for completeness)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec![None::<&str>])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );
    }

    #[test]
    fn test_empty_string_handling() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // empty string should also use HIVE_DEFAULT_PARTITION
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec![""])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );
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
        // [1, 2, 3] -> %5B1, 2, 3%5D (brackets escaped, commas not)
        assert_eq!(result, "output/%5B1, 2, 3%5D.parquet");
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
        // {name: Alice, age: 30} -> %7Bname%3A Alice, age%3A 30}
        assert_eq!(result, "output/%7Bname%3A Alice, age%3A 30}.parquet");
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
        // [[1, 2], [3]] -> %5B%5B1, 2%5D, %5B3%5D%5D (commas not escaped)
        assert_eq!(result, "output/%5B%5B1, 2%5D, %5B3%5D%5D.parquet");
    }

    #[test]
    fn test_hive_escaping_special_characters() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // test forward slash
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["a/b"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/a%2Fb.parquet");

        // test colon
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["10:30"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/10%3A30.parquet");

        // test equals
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["key=value"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/key%3Dvalue.parquet");

        // test hash
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["tag#1"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/tag%231.parquet");

        // test percent
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["50%"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/50%25.parquet");

        // test question mark
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["what?"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/what%3F.parquet");

        // test backslash
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["a\\b"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/a%5Cb.parquet");

        // test asterisk
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["*.txt"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/%2A.txt.parquet");

        // test brackets and caret (note: } is not escaped, only { [ ] ^)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["a[0]{x}^2"])) as _,
        );
        assert_eq!(
            template.resolve(&values),
            "output/a%5B0%5D%7Bx}%5E2.parquet"
        );
    }

    #[test]
    fn test_hive_escaping_no_escape_needed() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // simple alphanumeric
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["simple123"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/simple123.parquet");

        // with hyphens, underscores, dots
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["test-value_2.0"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/test-value_2.0.parquet");
    }

    #[test]
    fn test_hive_escaping_quotes() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // double quote
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["say \"hello\""])) as _,
        );
        assert_eq!(template.resolve(&values), "output/say %22hello%22.parquet");

        // single quote
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["it's"])) as _,
        );
        assert_eq!(template.resolve(&values), "output/it%27s.parquet");
    }

    #[test]
    fn test_hive_escaping_struct_formatting() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // struct formatting includes special characters that need escaping
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
        // {name: Alice, age: 30} -> %7Bname%3A Alice, age%3A 30}
        // note: only { is escaped, not } or ,
        assert_eq!(result, "output/%7Bname%3A Alice, age%3A 30}.parquet");
    }

    #[test]
    fn test_hive_escaping_list_formatting() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // list formatting includes brackets and commas
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let list_array = ListArray::new(field, offsets, Arc::new(values_data), None);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(list_array) as _);

        let result = template.resolve(&values);
        // [1, 2, 3] -> %5B1, 2, 3%5D (note: comma is not escaped)
        assert_eq!(result, "output/%5B1, 2, 3%5D.parquet");
    }

    #[test]
    fn test_hive_escaping_timestamp_formatting() {
        let template = PathTemplate::new("output/{val}.parquet".to_string());

        // timestamp includes colons
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampNanosecondArray::from(vec![1641051045000000000i64])) as _,
        );

        let result = template.resolve(&values);
        // 2022-01-01T15:30:45 -> 2022-01-01T15%3A30%3A45
        assert_eq!(result, "output/2022-01-01T15%3A30%3A45.parquet");
    }
}
