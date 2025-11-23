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

    use arrow::array::{Int32Array, StringArray};

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
}
