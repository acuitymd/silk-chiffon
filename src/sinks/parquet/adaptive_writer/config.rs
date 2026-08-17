//! Configuration types for the adaptive Parquet writer.

use std::collections::HashMap;

use arrow::datatypes::{DataType, SchemaRef};

use super::encoding::{is_analyzable, is_bloom_filter_eligible, is_dictionary_eligible};
use crate::BloomFilterConfig;
use crate::sinks::parquet::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ROW_GROUP_SIZE};

#[derive(Debug, Clone)]
pub(crate) struct LeafColumn {
    /// e.g., ["col", "list", "element"]
    pub path: Vec<String>,
    pub top_level: String,
    pub data_type: DataType,
}

impl LeafColumn {
    pub fn parquet_path(&self) -> String {
        self.path.join(".")
    }

    /// e.g., ["user", "address"] matches ["user", "address", "city"]
    pub fn matches_prefix(&self, prefix: &[String]) -> bool {
        self.path.starts_with(prefix)
    }
}

/// Enumerate all leaf columns in an Arrow schema using Parquet naming conventions.
///
/// ## Arrow vs Parquet schema representation
///
/// Arrow schemas are trees -- nested types contain child fields.
/// See: <https://arrow.apache.org/blog/2022/10/08/arrow-parquet-encoding-part-2/>
///
/// ```text
/// Schema
/// ├── "tags": List
/// │   └── <field_name>: Utf8   => field name could be anything ("foo", "vals", whatever)
/// └── "user": Struct
///     ├── "name": Utf8
///     └── "age": Int32
/// ```
///
/// Parquet schemas are flat -- only leaf columns exist, identified by dot-separated paths:
///
/// ```text
/// "tags.list.element"   => col is named "tags", "list" and "element" are always literally those two strings
/// "user.name"           => col is named "user", "name" is the field name on the struct
/// "user.age"            => col is named "user", "age" is the field name on the struct
/// ```
///
/// Parquet reconstructs nesting using repetition and definition levels -- integers stored
/// alongside each value that encode list boundaries and nullability. This is the Dremel
/// algorithm from Google's paper. See: <https://parquet.apache.org/docs/file-format/nestedencoding/>
///
/// All per-column config (`WriterProperties::set_column_*`) uses these flat string paths,
/// so we need to enumerate them from the Arrow schema.
///
/// ## Parquet naming conventions
///
/// Parquet uses hardcoded strings from the format spec for nested type path segments:
///
/// - **Lists**: The Parquet spec defines list structure as `<name>.list.element`, where "list"
///   and "element" are literal strings, not placeholders. Arrow's inner field name is ignored.
///
///   ```text
///   Arrow: Field("tags", List(Field("item", Utf8)))     => Parquet path: "tags.list.element"
///   Arrow: Field("tags", List(Field("values", Utf8)))   => Parquet path: "tags.list.element"
///   Arrow: Field("tags", List(Field("whatever", Utf8))) => Parquet path: "tags.list.element"
///   ```
///
/// - **Maps**: literal `.key_value.key` and `.key_value.value` (also literal strings in the spec, not placeholders)
///
/// - **Structs**: actual field names preserved (e.g., `"user.name"`, `"user.age"`)
///
/// These compose: `Field("points", List(Struct({x: Int32, y: Int32})))` produces paths
/// `"points.list.element.x"` and `"points.list.element.y"`.
///
/// ## Why this exists
///
/// We need to resolve CLI flags like `--parquet-column-dictionary=tags.list.element`
/// before any data arrives, using only the schema. At encoding time, parquet-rs's
/// `compute_leaves` extracts the actual arrays -- the paths must match or config
/// won't apply to the right columns.
///
/// This function mirrors parquet-rs's internal path generation logic to ensure
/// consistency between config resolution (schema-only) and encoding (with data).
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
        DataType::Dictionary(_, value_type) => {
            enumerate_field(top_level, value_type, path_segments, leaves);
        }
        _ => {
            leaves.push(LeafColumn {
                path: path_segments,
                top_level: top_level.to_string(),
                data_type: data_type.clone(),
            });
        }
    }
}

/// Configuration for the adaptive Parquet writer pipeline.
///
/// The adaptive writer uses a three-task pipeline (ingestion -> encoding -> writing) with
/// optional per-row-group cardinality analysis to make dictionary encoding decisions.
#[derive(Debug, Clone)]
pub struct AdaptiveWriterConfig {
    /// Rows per row group (last row group may be smaller).
    pub max_row_group_size: usize,
    /// I/O buffer size in bytes for writing encoded data to disk.
    pub buffer_size: usize,
    /// Number of row groups that can be encoded in parallel.
    pub max_row_group_concurrency: usize,
    /// Queue capacity in batches between ingestion and row group assembly.
    pub ingestion_queue_size: usize,
    /// Queue capacity in row groups between assembly and encoding.
    pub encoding_queue_size: usize,
    /// Queue capacity in row groups between encoding and writing (backpressure).
    pub writing_queue_size: usize,
    /// Whether to omit Arrow schema metadata from the Parquet file.
    pub skip_arrow_metadata: bool,
    /// Max dictionary page size in bytes before falling back to data encoding.
    pub dictionary_page_size_limit: usize,
    /// Global default for dictionary encoding (can be overridden per-column).
    pub dictionary_enabled_default: bool,
    /// Columns to disable dictionary encoding for (column names or dot-separated paths for nested, prefix matching).
    pub user_disabled_dictionary: Vec<String>,
    /// Columns to always attempt dictionary encoding (column names or dot-separated paths for nested, prefix matching).
    pub user_enabled_dictionary_always: Vec<String>,
    /// Columns to analyze for dictionary encoding (column names or dot-separated paths for nested, prefix matching).
    /// Analysis runs per-row-group; dictionary disabled if NDV exceeds 20% of row count.
    pub user_enabled_dictionary_analyze: Vec<String>,
    /// Bloom filter configuration (which columns, FPP, NDV).
    pub bloom_filter_config: Option<BloomFilterConfig>,
    /// Pre-computed NDV (number of distinct values) per column.
    /// Used for bloom filter sizing when not specified in bloom_filter_config.
    /// Populated by cardinality analysis when using `:analyze` mode.
    /// Keys are column names, or dot-separated paths for nested columns.
    pub ndv_map: HashMap<String, u64>,
}

impl Default for AdaptiveWriterConfig {
    fn default() -> Self {
        Self {
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            buffer_size: DEFAULT_BUFFER_SIZE,
            max_row_group_concurrency: 4,
            ingestion_queue_size: 1,
            encoding_queue_size: 4,
            writing_queue_size: 4,
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

fn parse_cli_path(s: &str) -> Vec<String> {
    s.split('.').map(String::from).collect()
}

impl ResolvedColumnConfigs {
    pub fn resolve(schema: &SchemaRef, config: &AdaptiveWriterConfig) -> Self {
        let leaves = enumerate_leaf_columns(schema)
            .into_iter()
            .map(|leaf| {
                let cfg = Self::resolve_leaf(&leaf, config);
                (leaf.parquet_path(), cfg)
            })
            .collect();

        Self { leaves }
    }

    fn resolve_leaf(leaf: &LeafColumn, config: &AdaptiveWriterConfig) -> ResolvedLeafConfig {
        ResolvedLeafConfig {
            dictionary: Self::resolve_dictionary_for_leaf(leaf, config),
            bloom_filter: Self::resolve_bloom_for_leaf(leaf, config),
        }
    }

    /// Resolve dictionary encoding mode for a leaf column.
    ///
    /// Resolution order (first match wins):
    /// 1. NotEligible - type doesn't support dictionary (floats, bools, etc.)
    /// 2. Disabled - column matches `user_disabled_dictionary` prefix
    /// 3. Always - column matches `user_enabled_dictionary_always` prefix
    /// 4. Analyze - column matches `user_enabled_dictionary_analyze` prefix
    ///    (top-level analyzable types only; nested or non-analyzable types use Always)
    /// 5. Default - based on `dictionary_enabled_default`:
    ///    - true: nested/non-analyzable columns use Always, top-level analyzable uses Analyze
    ///    - false: Disabled
    ///
    /// Non-analyzable types include nested types (List, Struct, Map) and floats (high cardinality).
    fn resolve_dictionary_for_leaf(
        leaf: &LeafColumn,
        config: &AdaptiveWriterConfig,
    ) -> ResolvedDictionaryMode {
        if !is_dictionary_eligible(&leaf.data_type) {
            return ResolvedDictionaryMode::NotEligible;
        }

        if config
            .user_disabled_dictionary
            .iter()
            .map(|s| parse_cli_path(s))
            .any(|prefix| leaf.matches_prefix(&prefix))
        {
            return ResolvedDictionaryMode::Disabled;
        }

        if config
            .user_enabled_dictionary_always
            .iter()
            .map(|s| parse_cli_path(s))
            .any(|prefix| leaf.matches_prefix(&prefix))
        {
            return ResolvedDictionaryMode::Always;
        }

        if config
            .user_enabled_dictionary_analyze
            .iter()
            .map(|s| parse_cli_path(s))
            .any(|prefix| leaf.matches_prefix(&prefix))
        {
            // analyze only works on top-level analyzable columns; others use always
            if leaf.path.len() == 1 && is_analyzable(&leaf.data_type) {
                return ResolvedDictionaryMode::Analyze;
            } else {
                return ResolvedDictionaryMode::Always;
            }
        }

        let is_nested = leaf.path.len() > 1;
        let can_analyze = is_analyzable(&leaf.data_type);

        if config.dictionary_enabled_default {
            if is_nested || !can_analyze {
                ResolvedDictionaryMode::Always
            } else {
                ResolvedDictionaryMode::Analyze
            }
        } else {
            ResolvedDictionaryMode::Disabled
        }
    }

    /// Resolve bloom filter mode for a leaf column.
    ///
    /// Resolution order (first match wins):
    /// 1. Disabled - type doesn't support bloom filters
    /// 2. Disabled - no `bloom_filter_config` provided
    /// 3. Disabled - column matches `column_disabled` prefix
    /// 4. Enabled - column matches `column_enabled` prefix (uses per-column FPP/NDV, falls back to ndv_map)
    ///    (disabled if no NDV and type is non-analyzable)
    /// 5. Enabled - `all_enabled` is set (disabled if no NDV and type is nested or non-analyzable)
    /// 6. Disabled - no matching config
    ///
    /// Non-analyzable types include nested types (List, Struct, Map) and floats.
    fn resolve_bloom_for_leaf(
        leaf: &LeafColumn,
        config: &AdaptiveWriterConfig,
    ) -> ResolvedBloomFilterMode {
        if !is_bloom_filter_eligible(&leaf.data_type) {
            return ResolvedBloomFilterMode::Disabled;
        }

        let Some(bloom_config) = config.bloom_filter_config.as_ref() else {
            return ResolvedBloomFilterMode::Disabled;
        };

        let is_disabled = bloom_config
            .column_disabled()
            .iter()
            .map(|s| parse_cli_path(s))
            .any(|prefix| leaf.matches_prefix(&prefix));

        if is_disabled {
            return ResolvedBloomFilterMode::Disabled;
        }

        let parquet_path = leaf.parquet_path();
        let ndv_from_map = config
            .ndv_map
            .get(&parquet_path)
            .or_else(|| config.ndv_map.get(&leaf.top_level))
            .copied();

        let col_cfg = bloom_config
            .column_enabled()
            .iter()
            .find(|c| leaf.matches_prefix(&parse_cli_path(&c.name)));

        if let Some(col_cfg) = col_cfg {
            let effective_ndv = col_cfg.config.ndv.or(ndv_from_map);
            // if no NDV provided, column must be analyzable to get NDV at runtime
            if effective_ndv.is_none() && !is_analyzable(&leaf.data_type) {
                return ResolvedBloomFilterMode::Disabled;
            }
            return ResolvedBloomFilterMode::Enabled {
                fpp: col_cfg.config.fpp,
                ndv: effective_ndv,
            };
        }

        if let Some(global) = bloom_config.all_enabled() {
            let effective_ndv = global.ndv.or(ndv_from_map);
            let is_nested = leaf.path.len() > 1;
            let can_analyze = is_analyzable(&leaf.data_type);
            // disable if NDV unknown and we can't analyze to get it
            if effective_ndv.is_none() && (is_nested || !can_analyze) {
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
    ///
    /// A column needs analysis if any of its leaves require runtime NDV (number of distinct values):
    /// - Dictionary mode is `Analyze` (need NDV to decide if dictionary encoding is worthwhile)
    /// - Bloom filter is enabled but NDV wasn't provided (need NDV to size the filter)
    ///
    /// Analysis only works on analyzable types. Non-analyzable types (nested types like List,
    /// Struct, Map, and floats due to high cardinality) fall back to `Always` for dictionary
    /// encoding and require explicit NDV for bloom filters.
    ///
    /// Returns the top-level column name (first path segment), not the full leaf path,
    /// because analysis runs on the top-level Arrow column.
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
    use arrow::datatypes::{Field, Fields};
    use std::sync::Arc;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    #[test]
    fn test_flat_columns() {
        let s = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, false),
        ]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 3);
        assert_eq!(leaves[0].path, vec!["id"]);
        assert_eq!(leaves[0].parquet_path(), "id");
        assert_eq!(leaves[0].top_level, "id");
        assert_eq!(leaves[1].parquet_path(), "name");
        assert_eq!(leaves[2].parquet_path(), "score");
    }

    #[test]
    fn test_dictionary_unwrapped() {
        let s = schema(vec![Field::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].path, vec!["category"]);
        assert_eq!(leaves[0].data_type, DataType::Utf8);
    }

    #[test]
    fn test_plain_list() {
        let s = schema(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].path, vec!["tags", "list", "element"]);
        assert_eq!(leaves[0].parquet_path(), "tags.list.element");
        assert_eq!(leaves[0].top_level, "tags");
        assert_eq!(leaves[0].data_type, DataType::Utf8);
    }

    #[test]
    fn test_list_inner_field_name_ignored() {
        let make_list = |inner_name: &str| {
            schema(vec![Field::new(
                "col",
                DataType::List(Arc::new(Field::new(inner_name, DataType::Int32, true))),
                true,
            )])
        };

        for name in ["item", "values", "element", "foo", "whatever"] {
            let leaves = enumerate_leaf_columns(&make_list(name));
            assert_eq!(
                leaves[0].parquet_path(),
                "col.list.element",
                "inner field name '{name}' should be ignored"
            );
        }
    }

    #[test]
    fn test_large_list() {
        let s = schema(vec![Field::new(
            "big",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].parquet_path(), "big.list.element");
    }

    #[test]
    fn test_fixed_size_list() {
        let s = schema(vec![Field::new(
            "vec3",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].parquet_path(), "vec3.list.element");
    }

    #[test]
    fn test_plain_struct() {
        let s = schema(vec![Field::new(
            "user",
            DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, true),
            ])),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].path, vec!["user", "name"]);
        assert_eq!(leaves[0].parquet_path(), "user.name");
        assert_eq!(leaves[0].top_level, "user");
        assert_eq!(leaves[1].parquet_path(), "user.age");
        assert_eq!(leaves[1].top_level, "user");
    }

    #[test]
    fn test_nested_struct() {
        let s = schema(vec![Field::new(
            "outer",
            DataType::Struct(Fields::from(vec![Field::new(
                "inner",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, false),
                    Field::new("y", DataType::Int32, false),
                ])),
                true,
            )])),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].parquet_path(), "outer.inner.x");
        assert_eq!(leaves[1].parquet_path(), "outer.inner.y");
    }

    #[test]
    fn test_map() {
        let s = schema(vec![Field::new(
            "metadata",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].parquet_path(), "metadata.key_value.key");
        assert_eq!(leaves[0].top_level, "metadata");
        assert_eq!(leaves[1].parquet_path(), "metadata.key_value.value");
        assert_eq!(leaves[1].top_level, "metadata");
    }

    #[test]
    fn test_list_of_struct() {
        let s = schema(vec![Field::new(
            "points",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ])),
                true,
            ))),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].parquet_path(), "points.list.element.x");
        assert_eq!(leaves[1].parquet_path(), "points.list.element.y");
    }

    #[test]
    fn test_nested_list() {
        // List<List<Int32>> - matrix-like structure
        let s = schema(vec![Field::new(
            "matrix",
            DataType::List(Arc::new(Field::new(
                "row",
                DataType::List(Arc::new(Field::new("cell", DataType::Int32, false))),
                true,
            ))),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].parquet_path(), "matrix.list.element.list.element");
    }

    #[test]
    fn test_struct_containing_list() {
        let s = schema(vec![Field::new(
            "user",
            DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new(
                    "emails",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
            ])),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].parquet_path(), "user.name");
        assert_eq!(leaves[1].parquet_path(), "user.emails.list.element");
    }

    #[test]
    fn test_deeply_nested() {
        // List<Struct<{items: List<Struct<{value: Int32}>>}>>
        let s = schema(vec![Field::new(
            "data",
            DataType::List(Arc::new(Field::new(
                "outer",
                DataType::Struct(Fields::from(vec![Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new(
                        "inner",
                        DataType::Struct(Fields::from(vec![Field::new(
                            "value",
                            DataType::Int32,
                            false,
                        )])),
                        true,
                    ))),
                    true,
                )])),
                true,
            ))),
            true,
        )]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 1);
        assert_eq!(
            leaves[0].parquet_path(),
            "data.list.element.items.list.element.value"
        );
        assert_eq!(leaves[0].top_level, "data");
    }

    #[test]
    fn test_multiple_columns_mixed() {
        let s = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "user",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, true),
                ])),
                true,
            ),
        ]);

        let leaves = enumerate_leaf_columns(&s);

        assert_eq!(leaves.len(), 4);
        assert_eq!(leaves[0].parquet_path(), "id");
        assert_eq!(leaves[0].top_level, "id");
        assert_eq!(leaves[1].parquet_path(), "tags.list.element");
        assert_eq!(leaves[1].top_level, "tags");
        assert_eq!(leaves[2].parquet_path(), "user.name");
        assert_eq!(leaves[2].top_level, "user");
        assert_eq!(leaves[3].parquet_path(), "user.age");
        assert_eq!(leaves[3].top_level, "user");
    }

    #[test]
    fn test_matches_prefix() {
        let leaf = LeafColumn {
            path: vec![
                "user".to_string(),
                "address".to_string(),
                "city".to_string(),
            ],
            top_level: "user".to_string(),
            data_type: DataType::Utf8,
        };

        // exact match
        assert!(leaf.matches_prefix(&[
            "user".to_string(),
            "address".to_string(),
            "city".to_string()
        ]));
        // prefix match
        assert!(leaf.matches_prefix(&["user".to_string(), "address".to_string()]));
        // top-level match
        assert!(leaf.matches_prefix(&["user".to_string()]));
        // empty prefix matches everything
        assert!(leaf.matches_prefix(&[]));
        // non-matching prefix
        assert!(!leaf.matches_prefix(&["other".to_string()]));
        assert!(!leaf.matches_prefix(&["user".to_string(), "name".to_string()]));
    }

    #[test]
    fn test_parse_cli_path() {
        assert_eq!(parse_cli_path("tags"), vec!["tags"]);
        assert_eq!(parse_cli_path("user.address"), vec!["user", "address"]);
        assert_eq!(
            parse_cli_path("user.address.city"),
            vec!["user", "address", "city"]
        );
    }
}
