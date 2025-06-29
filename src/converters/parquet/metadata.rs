use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;
use parquet::{file::properties::WriterPropertiesBuilder, format::SortingColumn};

use crate::{SortDirection, SortSpec};

pub struct SortMetadataBuilder {
    sort_spec: SortSpec,
}

impl SortMetadataBuilder {
    pub fn new(sort_spec: SortSpec) -> Self {
        Self { sort_spec }
    }

    pub fn apply(
        &self,
        builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
    ) -> Result<WriterPropertiesBuilder> {
        let sorting_columns = self.build_sorting_columns(schema)?;
        Ok(builder.set_sorting_columns(Some(sorting_columns)))
    }

    fn build_sorting_columns(&self, schema: &SchemaRef) -> Result<Vec<SortingColumn>> {
        let mut sorting_columns = Vec::new();

        for sort_col in &self.sort_spec.columns {
            let column_idx = schema
                .index_of(&sort_col.name)
                .map_err(|_| anyhow!("Sort column '{}' not found in schema", sort_col.name))?;

            let descending = sort_col.direction == SortDirection::Descending;

            sorting_columns.push(SortingColumn {
                column_idx: column_idx as i32,
                descending,
                nulls_first: descending,
            });
        }

        Ok(sorting_columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SortColumn, SortDirection};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ]))
    }

    #[test]
    fn test_build_sorting_columns_single_ascending() {
        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let result = builder.build_sorting_columns(&schema).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_idx, 0);
        assert!(!result[0].descending);
        assert!(!result[0].nulls_first);
    }

    #[test]
    fn test_build_sorting_columns_single_descending() {
        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "value".to_string(),
                direction: SortDirection::Descending,
            }],
        };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let result = builder.build_sorting_columns(&schema).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_idx, 2);
        assert!(result[0].descending);
        assert!(result[0].nulls_first);
    }

    #[test]
    fn test_build_sorting_columns_multiple() {
        let sort_spec = SortSpec {
            columns: vec![
                SortColumn {
                    name: "name".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "value".to_string(),
                    direction: SortDirection::Descending,
                },
                SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                },
            ],
        };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let result = builder.build_sorting_columns(&schema).unwrap();

        assert_eq!(result.len(), 3);

        assert_eq!(result[0].column_idx, 1);
        assert!(!result[0].descending);
        assert!(!result[0].nulls_first);

        assert_eq!(result[1].column_idx, 2);
        assert!(result[1].descending);
        assert!(result[1].nulls_first);

        assert_eq!(result[2].column_idx, 0);
        assert!(!result[2].descending);
        assert!(!result[2].nulls_first);
    }

    #[test]
    fn test_build_sorting_columns_invalid_column() {
        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "nonexistent".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let result = builder.build_sorting_columns(&schema);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Sort column 'nonexistent' not found in schema")
        );
    }

    #[test]
    fn test_build_sorting_columns_mixed_invalid() {
        let sort_spec = SortSpec {
            columns: vec![
                SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "invalid_column".to_string(),
                    direction: SortDirection::Descending,
                },
            ],
        };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let result = builder.build_sorting_columns(&schema);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Sort column 'invalid_column' not found in schema")
        );
    }

    #[test]
    fn test_apply_success() {
        let sort_spec = SortSpec {
            columns: vec![
                SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "value".to_string(),
                    direction: SortDirection::Descending,
                },
            ],
        };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let props_builder = parquet::file::properties::WriterProperties::builder();

        let result = builder.apply(props_builder, &schema);
        assert!(result.is_ok());

        let _props = result.unwrap().build();
        // The properties are built successfully with sorting columns
    }

    #[test]
    fn test_column_indices() {
        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "timestamp".to_string(),
                direction: SortDirection::Descending,
            }],
        };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let result = builder.build_sorting_columns(&schema).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_idx, 3); // timestamp is the 4th column (index 3)
        assert!(result[0].descending);
        assert!(result[0].nulls_first);
    }

    #[test]
    fn test_empty_sort_spec() {
        let sort_spec = SortSpec { columns: vec![] };

        let builder = SortMetadataBuilder::new(sort_spec);
        let schema = create_test_schema();
        let result = builder.build_sorting_columns(&schema).unwrap();

        assert_eq!(result.len(), 0);
    }
}
