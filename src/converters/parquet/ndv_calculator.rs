use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, datatypes::Schema};
use datafusion::{
    execution::options::ArrowReadOptions,
    functions_aggregate::expr_fn::count_distinct,
    prelude::{SessionContext, col},
};
use std::{collections::HashMap, path::Path};

use crate::{BloomFilterConfig, utils::arrow_io::ArrowIPCReader};

pub struct NdvCalculator {
    bloom_filters: BloomFilterConfig,
}

impl NdvCalculator {
    pub fn new(bloom_filters: BloomFilterConfig) -> Self {
        Self { bloom_filters }
    }

    pub fn needs_calculation(&self) -> bool {
        self.bloom_filters.is_configured()
    }

    pub async fn calculate(&self, arrow_file_path: &Path) -> Result<HashMap<String, u64>> {
        if !self.needs_calculation() {
            return Ok(HashMap::new());
        }

        let ctx = SessionContext::new();

        let arrow_reader = ArrowIPCReader::from_path(
            arrow_file_path
                .to_str()
                .ok_or_else(|| anyhow!("Invalid path: {:?}", arrow_file_path))?,
        )?;
        let schema = arrow_reader.schema()?;

        ctx.register_arrow(
            "arrow_table",
            arrow_file_path
                .to_str()
                .ok_or_else(|| anyhow!("Invalid path: {:?}", arrow_file_path))?,
            ArrowReadOptions::default(),
        )
        .await?;
        let df = ctx.table("arrow_table").await?;

        let columns = self.get_columns_needing_ndv(&schema)?;
        self.validate_column_names(&columns, &schema)?;

        if columns.is_empty() {
            return Ok(HashMap::new());
        }

        let agg_exprs: Vec<_> = columns
            .iter()
            .map(|col_name| count_distinct(col(col_name)).alias(col_name))
            .collect();

        let result_df = df.aggregate(vec![], agg_exprs)?;
        let batches = result_df.collect().await?;

        self.extract_ndv_results(batches, &columns)
    }

    fn get_columns_needing_ndv(&self, schema: &Schema) -> Result<Vec<String>> {
        match &self.bloom_filters {
            BloomFilterConfig::None => Ok(vec![]),
            BloomFilterConfig::All(_) => Ok(schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect()),
            BloomFilterConfig::Columns(columns) => Ok(columns
                .iter()
                .filter(|col| col.size_config.ndv.is_none())
                .map(|col| col.name.clone())
                .collect()),
        }
    }

    fn validate_column_names(&self, columns: &[String], schema: &Schema) -> Result<()> {
        let invalid_columns: Vec<_> = columns
            .iter()
            .filter(|col_name| schema.field_with_name(col_name).is_err())
            .collect();

        if !invalid_columns.is_empty() {
            let available_columns: Vec<_> = schema.fields().iter().map(|f| f.name()).collect();
            return Err(anyhow!(
                "Column(s) {:?} not found in schema. Available columns: {:?}",
                invalid_columns,
                available_columns
            ));
        }

        Ok(())
    }

    fn extract_ndv_results(
        &self,
        batches: Vec<RecordBatch>,
        column_names: &[String],
    ) -> Result<HashMap<String, u64>> {
        if batches.is_empty() {
            return Ok(HashMap::new());
        }

        if batches.len() != 1 {
            return Err(anyhow!(
                "Expected exactly one result batch, got {}",
                batches.len()
            ));
        }

        let batch = &batches[0];
        let mut results = HashMap::new();

        for column_name in column_names {
            let array = batch
                .column_by_name(column_name)
                .ok_or_else(|| anyhow!("NDV result column '{}' not found", column_name))?;

            if array.len() != 1 {
                return Err(anyhow!(
                    "Expected single NDV result row, got {}",
                    array.len()
                ));
            }

            let count_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| anyhow!("NDV result is not Int64 type"))?;

            let ndv_value = count_array.value(0);
            results.insert(column_name.clone(), ndv_value as u64);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AllColumnsBloomFilterSizeConfig, ColumnBloomFilterConfig, ColumnBloomFilterSizeConfig,
    };
    use arrow::{
        array::{Int32Array, Int64Array},
        datatypes::{DataType, Field},
    };
    use std::sync::Arc;

    #[test]
    fn test_needs_calculation_with_bloom_filters() {
        let bloom_config =
            BloomFilterConfig::All(AllColumnsBloomFilterSizeConfig { fpp: Some(0.01) });
        let calculator = NdvCalculator::new(bloom_config);
        assert!(calculator.needs_calculation());
    }

    #[test]
    fn test_needs_calculation_without_bloom_filters() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);
        assert!(!calculator.needs_calculation());
    }

    #[test]
    fn test_get_columns_needing_ndv_all_columns() {
        let bloom_config =
            BloomFilterConfig::All(AllColumnsBloomFilterSizeConfig { fpp: Some(0.01) });
        let calculator = NdvCalculator::new(bloom_config);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);

        let columns = calculator.get_columns_needing_ndv(&schema).unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns, vec!["id", "name", "age"]);
    }

    #[test]
    fn test_get_columns_needing_ndv_specific_columns() {
        let bloom_config = BloomFilterConfig::Columns(vec![
            ColumnBloomFilterConfig {
                name: "id".to_string(),
                size_config: ColumnBloomFilterSizeConfig {
                    fpp: Some(0.01),
                    ndv: None,
                },
            },
            ColumnBloomFilterConfig {
                name: "name".to_string(),
                size_config: ColumnBloomFilterSizeConfig {
                    fpp: Some(0.01),
                    ndv: Some(100),
                },
            },
        ]);
        let calculator = NdvCalculator::new(bloom_config);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let columns = calculator.get_columns_needing_ndv(&schema).unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns, vec!["id"]);
    }

    #[test]
    fn test_validate_column_names_all_valid() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let columns = vec!["id".to_string(), "name".to_string()];
        assert!(calculator.validate_column_names(&columns, &schema).is_ok());
    }

    #[test]
    fn test_validate_column_names_invalid_column() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let columns = vec!["id".to_string(), "nonexistent".to_string()];
        let result = calculator.validate_column_names(&columns, &schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("nonexistent"));
        assert!(err_msg.contains("not found in schema"));
    }

    #[test]
    fn test_extract_ndv_results_empty_batches() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);
        let batches = vec![];
        let columns = vec!["id".to_string()];

        let result = calculator.extract_ndv_results(batches, &columns).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_ndv_results_success() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Int64, false),
        ]));

        let id_array = Int64Array::from(vec![3]);
        let name_array = Int64Array::from(vec![5]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap();

        let columns = vec!["id".to_string(), "name".to_string()];
        let result = calculator
            .extract_ndv_results(vec![batch], &columns)
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.get("id"), Some(&3));
        assert_eq!(result.get("name"), Some(&5));
    }

    #[test]
    fn test_extract_ndv_results_multiple_batches_error() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            false,
        )]));

        let batch1 = RecordBatch::new_empty(schema.clone());
        let batch2 = RecordBatch::new_empty(schema);
        let batches = vec![batch1, batch2];
        let columns = vec!["id".to_string()];

        let result = calculator.extract_ndv_results(batches, &columns);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Expected exactly one result batch")
        );
    }

    #[test]
    fn test_extract_ndv_results_missing_column() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let id_array = Int64Array::from(vec![3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap();

        let columns = vec!["nonexistent".to_string()];
        let result = calculator.extract_ndv_results(vec![batch], &columns);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("NDV result column 'nonexistent' not found")
        );
    }

    #[test]
    fn test_extract_ndv_results_wrong_type() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let id_array = Int32Array::from(vec![3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap();

        let columns = vec!["id".to_string()];
        let result = calculator.extract_ndv_results(vec![batch], &columns);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("NDV result is not Int64 type")
        );
    }
}
