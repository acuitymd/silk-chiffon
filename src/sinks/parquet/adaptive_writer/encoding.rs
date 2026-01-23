//! Encoding decision logic for adaptive Parquet writing.
//!
//! Contains functions to determine dictionary and bloom filter settings per column
//! based on cardinality analysis and data type eligibility.

use std::collections::HashMap;

use arrow::datatypes::{DataType, SchemaRef};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder, WriterPropertiesPtr};
use parquet::schema::types::ColumnPath;

use super::analysis::ColumnAnalysis;
use super::config::{
    AdaptiveWriterConfig, ResolvedBloomFilterMode, ResolvedColumnConfigs, ResolvedDictionaryMode,
    enumerate_leaf_columns,
};

/// Cardinality threshold for dictionary encoding: if distinct values exceed
/// row_count / DICTIONARY_CARDINALITY_THRESHOLD_DIVISOR (default 5 = 20%),
/// dictionary encoding is disabled for that column as the overhead exceeds the benefit.
pub(crate) const DICTIONARY_CARDINALITY_THRESHOLD_DIVISOR: usize = 5;

/// Build WriterProperties for a row group based on cardinality analysis.
pub fn build_row_group_properties(
    schema: &SchemaRef,
    base_props: &WriterPropertiesPtr,
    config: &AdaptiveWriterConfig,
    resolved: &ResolvedColumnConfigs,
    analysis: &HashMap<String, ColumnAnalysis>,
    row_count: usize,
) -> anyhow::Result<WriterProperties> {
    let mut builder = WriterProperties::builder()
        .set_writer_version(base_props.writer_version())
        .set_compression(base_props.compression(&ColumnPath::from("")))
        .set_data_page_size_limit(base_props.data_page_size_limit())
        .set_data_page_row_count_limit(base_props.data_page_row_count_limit())
        .set_write_batch_size(base_props.write_batch_size())
        .set_dictionary_page_size_limit(base_props.dictionary_page_size_limit())
        .set_dictionary_enabled(false)
        .set_bloom_filter_enabled(false);

    let created_by = base_props.created_by();
    if !created_by.is_empty() {
        builder = builder.set_created_by(created_by.to_string());
    }

    for leaf in enumerate_leaf_columns(schema) {
        let col_path = ColumnPath::from(leaf.path.split('.').map(String::from).collect::<Vec<_>>());

        builder = configure_leaf_encoding(
            builder,
            col_path,
            &leaf.path,
            &leaf.data_type,
            analysis,
            row_count,
            config,
            resolved,
            base_props,
        )?;
    }

    Ok(builder.build())
}

#[allow(clippy::too_many_arguments)]
fn configure_leaf_encoding(
    mut builder: WriterPropertiesBuilder,
    col_path: ColumnPath,
    leaf_path: &str,
    data_type: &DataType,
    analysis: &HashMap<String, ColumnAnalysis>,
    row_count: usize,
    config: &AdaptiveWriterConfig,
    resolved: &ResolvedColumnConfigs,
    base_props: &WriterPropertiesPtr,
) -> anyhow::Result<WriterPropertiesBuilder> {
    let resolved_cfg = resolved.get(leaf_path);

    let dict_mode = resolved_cfg
        .map(|c| c.dictionary)
        .unwrap_or(ResolvedDictionaryMode::NotEligible);

    let use_dict = match dict_mode {
        ResolvedDictionaryMode::NotEligible | ResolvedDictionaryMode::Disabled => false,
        ResolvedDictionaryMode::Always => true,
        ResolvedDictionaryMode::Analyze => {
            if let Some(col_analysis) = analysis.get(leaf_path) {
                should_use_dictionary(
                    col_analysis,
                    row_count,
                    data_type,
                    config.dictionary_page_size_limit,
                )
            } else {
                false
            }
        }
    };

    builder = builder.set_column_dictionary_enabled(col_path.clone(), use_dict);

    let bloom_mode = resolved_cfg
        .map(|c| &c.bloom_filter)
        .cloned()
        .unwrap_or(ResolvedBloomFilterMode::Disabled);

    match bloom_mode {
        ResolvedBloomFilterMode::Disabled => {
            builder = builder.set_column_bloom_filter_enabled(col_path.clone(), false);
        }
        ResolvedBloomFilterMode::Enabled { fpp, ndv } => {
            let enable_bloom = if ndv.is_some() { true } else { use_dict };

            if enable_bloom {
                let effective_ndv =
                    ndv.or_else(|| analysis.get(leaf_path).map(|a| a.approx_distinct));

                if let Some(n) = effective_ndv {
                    builder = builder
                        .set_column_bloom_filter_enabled(col_path.clone(), true)
                        .set_column_bloom_filter_fpp(col_path.clone(), fpp)
                        .set_column_bloom_filter_ndv(col_path.clone(), n);
                } else {
                    builder = builder.set_column_bloom_filter_enabled(col_path.clone(), false);
                }
            } else {
                builder = builder.set_column_bloom_filter_enabled(col_path.clone(), false);
            }
        }
    }

    builder = builder
        .set_column_compression(col_path.clone(), base_props.compression(&col_path))
        .set_column_statistics_enabled(col_path.clone(), base_props.statistics_enabled(&col_path));

    if base_props.write_page_header_statistics(&col_path) {
        builder = builder.set_column_write_page_header_statistics(col_path.clone(), true);
    }

    if let Some(encoding) = base_props.encoding(&col_path) {
        builder = builder.set_column_encoding(col_path, encoding);
    }

    Ok(builder)
}

/// Check if a data type is eligible for bloom filters.
pub fn is_bloom_filter_eligible(data_type: &DataType) -> bool {
    match data_type {
        // floats are excluded - they rarely benefit from bloom filters
        DataType::Float32 | DataType::Float64 => false,

        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => true,

        DataType::Dictionary(_, value_type) => is_bloom_filter_eligible(value_type),

        DataType::Boolean
        | DataType::Null
        | DataType::Float16
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => false,
    }
}

#[allow(clippy::cast_precision_loss)]
pub fn should_use_dictionary(
    analysis: &ColumnAnalysis,
    row_count: usize,
    data_type: &DataType,
    dict_page_size_limit: usize,
) -> bool {
    if row_count == 0 {
        return false;
    }

    let cardinality_ratio = (analysis.approx_distinct as f64) / (row_count as f64);
    if cardinality_ratio >= 1.0 / DICTIONARY_CARDINALITY_THRESHOLD_DIVISOR as f64 {
        return false;
    }

    let estimated_dict_size = estimate_dictionary_size(analysis, data_type);
    estimated_dict_size <= dict_page_size_limit
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn estimate_dictionary_size(analysis: &ColumnAnalysis, data_type: &DataType) -> usize {
    let cardinality = analysis.approx_distinct as usize;

    let value_size = match data_type {
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => 8,
        DataType::Time32(_) => 4,
        DataType::Decimal128(_, _) => 16,
        DataType::Decimal256(_, _) => 32,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => analysis
            .avg_value_size
            .map(|s| s.round().max(0.0) as usize)
            .unwrap_or(32),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => analysis
            .avg_value_size
            .map(|s| s.round().max(0.0) as usize)
            .unwrap_or(64),
        DataType::FixedSizeBinary(size) => (*size).max(0) as usize,
        DataType::Dictionary(_, value_type) => {
            return estimate_dictionary_size(analysis, value_type);
        }
        _ => 16,
    };

    cardinality * value_size
}

/// Check if a data type is eligible for dictionary encoding.
pub fn is_dictionary_eligible(data_type: &DataType) -> bool {
    match data_type {
        // floats are excluded - they almost always have high cardinality
        DataType::Float32 | DataType::Float64 => false,

        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => true,

        DataType::Dictionary(_, value_type) => is_dictionary_eligible(value_type),

        DataType::Boolean
        | DataType::Null
        | DataType::Float16
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_dictionary_eligible() {
        assert!(is_dictionary_eligible(&DataType::Int32));
        assert!(is_dictionary_eligible(&DataType::Utf8));
        assert!(is_dictionary_eligible(&DataType::Binary));
        assert!(!is_dictionary_eligible(&DataType::Boolean));
        assert!(!is_dictionary_eligible(&DataType::Float32));
        assert!(!is_dictionary_eligible(&DataType::Float64));
    }

    #[test]
    fn test_should_use_dictionary_low_cardinality() {
        let analysis = ColumnAnalysis {
            approx_distinct: 10,
            avg_value_size: None,
        };
        assert!(should_use_dictionary(
            &analysis,
            1000,
            &DataType::Int32,
            1024 * 1024
        ));
    }

    #[test]
    fn test_should_use_dictionary_high_cardinality() {
        let analysis = ColumnAnalysis {
            approx_distinct: 500,
            avg_value_size: None,
        };
        assert!(!should_use_dictionary(
            &analysis,
            1000,
            &DataType::Int32,
            1024 * 1024
        ));
    }

    #[test]
    fn test_estimate_dictionary_size_numeric() {
        let analysis = ColumnAnalysis {
            approx_distinct: 100,
            avg_value_size: None,
        };
        assert_eq!(estimate_dictionary_size(&analysis, &DataType::Int32), 400);
        assert_eq!(estimate_dictionary_size(&analysis, &DataType::Int64), 800);
    }

    #[test]
    fn test_estimate_dictionary_size_string() {
        let analysis = ColumnAnalysis {
            approx_distinct: 100,
            avg_value_size: Some(10.0),
        };
        assert_eq!(estimate_dictionary_size(&analysis, &DataType::Utf8), 1000);
    }

    #[test]
    fn test_estimate_dictionary_size_binary() {
        let analysis = ColumnAnalysis {
            approx_distinct: 100,
            avg_value_size: Some(20.0),
        };
        assert_eq!(estimate_dictionary_size(&analysis, &DataType::Binary), 2000);
    }
}
