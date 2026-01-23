//! Configuration types for the adaptive Parquet writer.

use std::collections::HashMap;

use arrow::datatypes::{DataType, SchemaRef};

use super::encoding::{is_bloom_filter_eligible, is_dictionary_eligible};
use crate::BloomFilterConfig;
use crate::sinks::parquet::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ROW_GROUP_SIZE};

/// Info about a leaf column in the Parquet schema.
#[derive(Debug, Clone)]
pub(crate) struct LeafColumn {
    /// Dot-separated path using Parquet naming conventions (e.g., "col.list.element")
    pub path: String,
    /// Top-level Arrow column name (first segment of path)
    pub top_level: String,
    /// Arrow data type for this leaf
    pub data_type: DataType,
}

/// Enumerate all leaf columns in an Arrow schema using Parquet naming conventions.
///
/// Returns paths like "col.list.element" for lists, "col.key_value.key" for maps, etc.
/// This is the SINGLE source of truth for path enumeration - used by config resolution
/// and encoding configuration.
pub(crate) fn enumerate_leaf_columns(schema: &SchemaRef) -> Vec<LeafColumn> {
    let mut leaves = Vec::new();
    for field in schema.fields() {
        enumerate_field(
            field.name(),
            field.data_type(),
            vec![field.name().to_string()],
            &mut leaves,
        );
    }
    leaves
}

fn enumerate_field(
    top_level: &str,
    data_type: &DataType,
    path_segments: Vec<String>,
    leaves: &mut Vec<LeafColumn>,
) {
    match data_type {
        DataType::List(elem_field)
        | DataType::ListView(elem_field)
        | DataType::LargeList(elem_field)
        | DataType::LargeListView(elem_field)
        | DataType::FixedSizeList(elem_field, _) => {
            let mut new_path = path_segments;
            new_path.extend(["list".to_string(), "element".to_string()]);
            enumerate_field(top_level, elem_field.data_type(), new_path, leaves);
        }
        DataType::Struct(fields) => {
            for struct_field in fields {
                let mut new_path = path_segments.clone();
                new_path.push(struct_field.name().to_string());
                enumerate_field(top_level, struct_field.data_type(), new_path, leaves);
            }
        }
        DataType::Map(struct_field, _) => {
            if let DataType::Struct(map_fields) = struct_field.data_type() {
                if let Some(key_field) = map_fields.first() {
                    let mut key_path = path_segments.clone();
                    key_path.extend(["key_value".to_string(), "key".to_string()]);
                    enumerate_field(top_level, key_field.data_type(), key_path, leaves);
                }
                if let Some(value_field) = map_fields.get(1) {
                    let mut value_path = path_segments;
                    value_path.extend(["key_value".to_string(), "value".to_string()]);
                    enumerate_field(top_level, value_field.data_type(), value_path, leaves);
                }
            }
        }
        _ => {
            leaves.push(LeafColumn {
                path: path_segments.join("."),
                top_level: top_level.to_string(),
                data_type: data_type.clone(),
            });
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdaptiveWriterConfig {
    pub max_row_group_size: usize,
    pub buffer_size: usize,
    pub max_row_group_concurrency: usize,
    pub ingest_queue_size: usize,
    pub encoding_queue_size: usize,
    pub write_queue_size: usize,
    pub skip_arrow_metadata: bool,
    pub dictionary_page_size_limit: usize,
    pub dictionary_enabled_default: bool,
    pub user_disabled_dictionary: Vec<String>,
    pub user_enabled_dictionary_always: Vec<String>,
    pub user_enabled_dictionary_analyze: Vec<String>,
    pub bloom_filter_config: Option<BloomFilterConfig>,
    /// pre-computed NDV values per column (fallback when not in bloom filter config)
    pub ndv_map: HashMap<String, u64>,
}

impl Default for AdaptiveWriterConfig {
    fn default() -> Self {
        Self {
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            buffer_size: DEFAULT_BUFFER_SIZE,
            max_row_group_concurrency: 4,
            ingest_queue_size: 1,
            encoding_queue_size: 4,
            write_queue_size: 4,
            skip_arrow_metadata: true,
            dictionary_page_size_limit: 1024 * 1024 * 1024, // 1GiB
            dictionary_enabled_default: true,
            user_disabled_dictionary: Vec::new(),
            user_enabled_dictionary_always: Vec::new(),
            user_enabled_dictionary_analyze: Vec::new(),
            bloom_filter_config: None,
            ndv_map: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResolvedDictionaryMode {
    /// type not eligible for dictionary encoding (Boolean, List, etc.)
    #[default]
    NotEligible,
    /// user explicitly disabled dictionary encoding
    Disabled,
    /// user forced dictionary encoding on
    Always,
    /// use cardinality analysis to decide
    Analyze,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedBloomFilterMode {
    Disabled,
    Enabled { fpp: f64, ndv: Option<u64> },
}

#[derive(Debug, Clone)]
pub struct ResolvedLeafConfig {
    pub dictionary: ResolvedDictionaryMode,
    pub bloom_filter: ResolvedBloomFilterMode,
}

#[derive(Debug, Clone)]
pub struct ResolvedColumnConfigs {
    leaves: HashMap<String, ResolvedLeafConfig>,
}

impl ResolvedColumnConfigs {
    pub fn resolve(schema: &SchemaRef, config: &AdaptiveWriterConfig) -> Self {
        let leaves = enumerate_leaf_columns(schema)
            .into_iter()
            .map(|leaf| {
                let cfg = Self::resolve_leaf(&leaf.path, &leaf.top_level, &leaf.data_type, config);
                (leaf.path, cfg)
            })
            .collect();

        Self { leaves }
    }

    fn resolve_leaf(
        leaf_path: &str,
        top_level_name: &str,
        data_type: &DataType,
        config: &AdaptiveWriterConfig,
    ) -> ResolvedLeafConfig {
        ResolvedLeafConfig {
            dictionary: Self::resolve_dictionary_for_leaf(
                leaf_path,
                top_level_name,
                data_type,
                config,
            ),
            bloom_filter: Self::resolve_bloom_for_leaf(
                leaf_path,
                top_level_name,
                data_type,
                config,
            ),
        }
    }

    fn resolve_dictionary_for_leaf(
        leaf_path: &str,
        top_level_name: &str,
        data_type: &DataType,
        config: &AdaptiveWriterConfig,
    ) -> ResolvedDictionaryMode {
        if !is_dictionary_eligible(data_type) {
            return ResolvedDictionaryMode::NotEligible;
        }

        if config
            .user_disabled_dictionary
            .iter()
            .any(|s| s == leaf_path || s == top_level_name)
        {
            return ResolvedDictionaryMode::Disabled;
        }

        if config
            .user_enabled_dictionary_always
            .iter()
            .any(|s| s == leaf_path || s == top_level_name)
        {
            return ResolvedDictionaryMode::Always;
        }

        if config
            .user_enabled_dictionary_analyze
            .iter()
            .any(|s| s == leaf_path || s == top_level_name)
        {
            if leaf_path == top_level_name {
                return ResolvedDictionaryMode::Analyze;
            } else {
                return ResolvedDictionaryMode::Always;
            }
        }

        let is_nested = leaf_path != top_level_name;

        if config.dictionary_enabled_default {
            if is_nested {
                ResolvedDictionaryMode::Always
            } else {
                ResolvedDictionaryMode::Analyze
            }
        } else {
            ResolvedDictionaryMode::Disabled
        }
    }

    fn resolve_bloom_for_leaf(
        leaf_path: &str,
        top_level_name: &str,
        data_type: &DataType,
        config: &AdaptiveWriterConfig,
    ) -> ResolvedBloomFilterMode {
        if !is_bloom_filter_eligible(data_type) {
            return ResolvedBloomFilterMode::Disabled;
        }

        let Some(bloom_config) = config.bloom_filter_config.as_ref() else {
            return ResolvedBloomFilterMode::Disabled;
        };

        if bloom_config.is_column_disabled(leaf_path)
            || bloom_config.is_column_disabled(top_level_name)
        {
            return ResolvedBloomFilterMode::Disabled;
        }

        let ndv_from_map = config
            .ndv_map
            .get(leaf_path)
            .or_else(|| config.ndv_map.get(top_level_name))
            .copied();

        if let Some(col_cfg) = bloom_config
            .get_column_config(leaf_path)
            .or_else(|| bloom_config.get_column_config(top_level_name))
        {
            return ResolvedBloomFilterMode::Enabled {
                fpp: col_cfg.config.fpp,
                ndv: col_cfg.config.ndv.or(ndv_from_map),
            };
        }

        if let Some(global) = bloom_config.all_enabled() {
            let effective_ndv = global.ndv.or(ndv_from_map);
            let is_nested = leaf_path != top_level_name;
            if is_nested && effective_ndv.is_none() {
                return ResolvedBloomFilterMode::Disabled;
            }
            return ResolvedBloomFilterMode::Enabled {
                fpp: global.fpp,
                ndv: effective_ndv,
            };
        }

        ResolvedBloomFilterMode::Disabled
    }

    /// Returns top-level column names that need cardinality analysis.
    pub fn columns_needing_analysis(&self) -> Vec<String> {
        let mut top_level_names: Vec<String> = self
            .leaves
            .iter()
            .filter(|(_, cfg)| {
                let needs_dict = cfg.dictionary == ResolvedDictionaryMode::Analyze;
                let needs_bloom = matches!(
                    &cfg.bloom_filter,
                    ResolvedBloomFilterMode::Enabled { ndv: None, .. }
                );
                needs_dict || needs_bloom
            })
            .filter_map(|(path, _)| path.split('.').next().map(|s| s.to_string()))
            .collect();
        top_level_names.sort();
        top_level_names.dedup();
        top_level_names
    }

    pub fn get(&self, leaf_path: &str) -> Option<&ResolvedLeafConfig> {
        self.leaves.get(leaf_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_enumerate_leaf_columns_for_nested_types() {
        // List<Struct<x: Int32>> - should enumerate to "col.list.element.x"
        let nested_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()),
            true,
        )));

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "col",
            nested_type,
            true,
        )]));

        let leaves = enumerate_leaf_columns(&schema);

        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].path, "col.list.element.x");
        assert_eq!(leaves[0].top_level, "col");
        assert_eq!(leaves[0].data_type, DataType::Int32);
    }
}
