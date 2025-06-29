use anyhow::{Result, anyhow};
use arrow::datatypes::{DataType, Schema};
use datafusion::{
    execution::options::ArrowReadOptions,
    functions_aggregate::expr_fn::{count_distinct, max},
    functions_window::expr_fn::row_number,
    logical_expr::cast,
    prelude::{SessionContext, col, lit},
};
use std::{collections::HashMap, path::Path};

use crate::{BloomFilterConfig, utils::arrow_io::ArrowIPCReader};

pub struct NdvCalculator {
    bloom_filters: BloomFilterConfig,
    row_group_size: usize,
}

impl NdvCalculator {
    pub fn new(bloom_filters: BloomFilterConfig, row_group_size: usize) -> Self {
        Self {
            bloom_filters,
            row_group_size,
        }
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

        self.calculate_max_ndv_per_row_group(df, &columns).await
    }

    fn get_columns_needing_ndv(&self, schema: &Schema) -> Result<Vec<String>> {
        match &self.bloom_filters {
            BloomFilterConfig::None => Ok(vec![]),
            BloomFilterConfig::All(_) => Ok(schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect()),
            BloomFilterConfig::Columns(columns) => {
                Ok(columns.iter().map(|col| col.name.clone()).collect())
            }
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

    async fn calculate_max_ndv_per_row_group(
        &self,
        df: datafusion::dataframe::DataFrame,
        columns: &[String],
    ) -> Result<HashMap<String, u64>> {
        let df_with_row_num = df.with_column("_row_num", row_number().alias("_row_num"))?;

        let df_with_group = df_with_row_num.with_column(
            "_row_group",
            cast(
                (col("_row_num") - lit(1i64)) / lit(self.row_group_size as i64),
                DataType::Int64,
            )
            .alias("_row_group"),
        )?;

        let mut results = HashMap::new();

        for col_name in columns {
            let group_ndv = df_with_group.clone().aggregate(
                vec![col("_row_group")],
                vec![count_distinct(col(col_name)).alias("ndv")],
            )?;

            let max_ndv = group_ndv.aggregate(vec![], vec![max(col("ndv")).alias("max_ndv")])?;

            let batches = max_ndv.collect().await?;

            if batches.is_empty() {
                continue;
            }

            let batch = &batches[0];
            let array = batch
                .column_by_name("max_ndv")
                .ok_or_else(|| anyhow!("max_ndv column not found"))?;

            if array.len() != 1 {
                return Err(anyhow!(
                    "Expected single max NDV result row, got {}",
                    array.len()
                ));
            }

            let max_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| anyhow!("max_ndv is not Int64 type"))?;

            let max_ndv_value = max_array.value(0);
            results.insert(col_name.clone(), max_ndv_value as u64);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AllColumnsBloomFilterConfig, ColumnBloomFilterConfig, ColumnSpecificBloomFilterConfig,
    };
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_needs_calculation_with_bloom_filters() {
        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: Some(0.01) });
        let calculator = NdvCalculator::new(bloom_config, 1_000_000);
        assert!(calculator.needs_calculation());
    }

    #[test]
    fn test_needs_calculation_without_bloom_filters() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config, 1_000_000);
        assert!(!calculator.needs_calculation());
    }

    #[test]
    fn test_get_columns_needing_ndv_all_columns() {
        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: Some(0.01) });
        let calculator = NdvCalculator::new(bloom_config, 1_000_000);

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
            ColumnSpecificBloomFilterConfig {
                name: "id".to_string(),
                config: ColumnBloomFilterConfig { fpp: Some(0.01) },
            },
            ColumnSpecificBloomFilterConfig {
                name: "name".to_string(),
                config: ColumnBloomFilterConfig { fpp: Some(0.01) },
            },
        ]);
        let calculator = NdvCalculator::new(bloom_config, 1_000_000);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let columns = calculator.get_columns_needing_ndv(&schema).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns, vec!["id", "name"]);
    }

    #[test]
    fn test_validate_column_names_all_valid() {
        let bloom_config = BloomFilterConfig::None;
        let calculator = NdvCalculator::new(bloom_config, 1_000_000);
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
        let calculator = NdvCalculator::new(bloom_config, 1_000_000);
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

    #[tokio::test]
    async fn test_calculate_max_ndv_per_row_group() {
        use crate::utils::test_helpers::{file_helpers, test_data};
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let arrow_path = temp_dir.path().join("test.arrow");

        let schema = test_data::simple_schema();
        let mut batches = Vec::new();

        let ids1 = vec![1, 2, 3, 1, 2];
        let names1 = vec!["A", "B", "C", "A", "B"];
        batches.push(test_data::create_batch_with_ids_and_names(
            &schema, &ids1, &names1,
        ));

        let ids2 = vec![4, 5, 4, 5, 4];
        let names2 = vec!["D", "E", "D", "E", "D"];
        batches.push(test_data::create_batch_with_ids_and_names(
            &schema, &ids2, &names2,
        ));

        let ids3 = vec![1, 2, 3, 4, 5];
        let names3 = vec!["A", "B", "C", "D", "E"];
        batches.push(test_data::create_batch_with_ids_and_names(
            &schema, &ids3, &names3,
        ));

        file_helpers::write_arrow_file(&arrow_path, &schema, batches).unwrap();

        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: Some(0.01) });
        let calculator = NdvCalculator::new(bloom_config, 5);

        let result = calculator.calculate(&arrow_path).await.unwrap();

        assert_eq!(result.get("id"), Some(&5));
        assert_eq!(result.get("name"), Some(&5));
    }
}
