use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;
use parquet::{
    basic::Compression,
    file::{
        metadata::SortingColumn,
        properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion},
    },
    schema::types::ColumnPath,
};
use std::{collections::HashMap, path::Path};

use crate::{
    BloomFilterConfig, ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortDirection,
    SortSpec, utils::arrow_io::ArrowIPCReader,
};

pub struct ParquetWritePropertiesBuilder {
    compression: Compression,
    statistics: EnabledStatistics,
    writer_version: WriterVersion,
    parquet_row_group_size: usize,
    no_dictionary: bool,
    bloom_filters: BloomFilterConfig,
    sort_spec: SortSpec,
}

impl ParquetWritePropertiesBuilder {
    pub fn new(
        compression: ParquetCompression,
        statistics: ParquetStatistics,
        writer_version: ParquetWriterVersion,
        parquet_row_group_size: usize,
        no_dictionary: bool,
        bloom_filters: BloomFilterConfig,
        sort_spec: SortSpec,
    ) -> Self {
        let compression = compression.into();
        let writer_version = writer_version.into();
        let statistics = statistics.into();

        Self {
            compression,
            statistics,
            writer_version,
            parquet_row_group_size,
            no_dictionary,
            bloom_filters,
            sort_spec,
        }
    }

    pub fn build(
        &self,
        input_path: &Path,
        ndv_map: &HashMap<String, u64>,
    ) -> Result<WriterProperties> {
        let schema = ArrowIPCReader::schema_from_path(input_path)?;

        let builder = self.create_base_builder();
        let builder = self.apply_bloom_filters(builder, ndv_map)?;
        let builder = self.apply_sort_metadata(builder, &schema)?;

        Ok(builder.build())
    }

    fn create_base_builder(&self) -> WriterPropertiesBuilder {
        WriterProperties::builder()
            .set_max_row_group_size(self.parquet_row_group_size)
            .set_compression(self.compression)
            .set_writer_version(self.writer_version)
            .set_statistics_enabled(self.statistics)
            .set_dictionary_enabled(!self.no_dictionary)
    }

    fn apply_bloom_filters(
        &self,
        mut builder: WriterPropertiesBuilder,
        ndv_map: &HashMap<String, u64>,
    ) -> Result<WriterPropertiesBuilder> {
        match &self.bloom_filters {
            BloomFilterConfig::None => Ok(builder),
            BloomFilterConfig::All(bloom_all) => {
                let fpp = bloom_all.fpp;
                builder = builder
                    .set_bloom_filter_enabled(true)
                    .set_bloom_filter_fpp(fpp);

                // if user provided NDV, use it for all columns
                // otherwise use calculated NDV from ndv_map if available
                if let Some(user_ndv) = bloom_all.ndv {
                    for col_name in ndv_map.keys() {
                        let col_path = ColumnPath::from(col_name.as_str());
                        builder = builder.set_column_bloom_filter_ndv(col_path, user_ndv);
                    }
                } else {
                    for (col_name, &ndv) in ndv_map {
                        let col_path = ColumnPath::from(col_name.as_str());
                        builder = builder.set_column_bloom_filter_ndv(col_path, ndv);
                    }
                }
                Ok(builder)
            }
            BloomFilterConfig::Columns(columns) => {
                for bloom_col in columns {
                    let col_path = ColumnPath::from(bloom_col.name.as_str());
                    let fpp = bloom_col.config.fpp;

                    builder = builder
                        .set_column_bloom_filter_enabled(col_path.clone(), true)
                        .set_column_bloom_filter_fpp(col_path.clone(), fpp);

                    // use user-provided NDV if available, otherwise use calculated NDV
                    let ndv = bloom_col
                        .config
                        .ndv
                        .or_else(|| ndv_map.get(&bloom_col.name).copied());
                    if let Some(ndv) = ndv {
                        builder = builder.set_column_bloom_filter_ndv(col_path, ndv);
                    }
                }
                Ok(builder)
            }
        }
    }

    fn apply_sort_metadata(
        &self,
        builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
    ) -> Result<WriterPropertiesBuilder> {
        if self.sort_spec.is_empty() {
            return Ok(builder);
        }

        let mut sorting_columns = Vec::new();

        for sort_col in &self.sort_spec.columns {
            let column_idx = schema
                .index_of(&sort_col.name)
                .map_err(|_| anyhow!("Sort column '{}' not found in schema", sort_col.name))?;

            let descending = sort_col.direction == SortDirection::Descending;

            sorting_columns.push(SortingColumn {
                column_idx: i32::try_from(column_idx)
                    .map_err(|_| anyhow!("Column index out of range"))?,
                descending,
                nulls_first: descending,
            });
        }

        Ok(builder.set_sorting_columns(Some(sorting_columns)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AllColumnsBloomFilterConfig, ColumnBloomFilterConfig, ColumnSpecificBloomFilterConfig,
    };

    #[test]
    fn test_compression_variants() {
        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::Zstd,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.compression, Compression::ZSTD(_)));

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::Snappy,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.compression, Compression::SNAPPY));

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::Gzip,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.compression, Compression::GZIP(_)));

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::Lz4,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.compression, Compression::LZ4_RAW));

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.compression, Compression::UNCOMPRESSED));
    }

    #[test]
    fn test_writer_version() {
        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.writer_version, WriterVersion::PARQUET_1_0));

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V2,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.writer_version, WriterVersion::PARQUET_2_0));
    }

    #[test]
    fn test_statistics() {
        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.statistics, EnabledStatistics::None));

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::Chunk,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.statistics, EnabledStatistics::Chunk));

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::Page,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );
        assert!(matches!(builder.statistics, EnabledStatistics::Page));
    }

    #[test]
    fn test_create_base_builder_with_dictionary() {
        let writer_builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::Snappy,
            ParquetStatistics::Chunk,
            ParquetWriterVersion::V2,
            50000,
            false,
            BloomFilterConfig::None,
            SortSpec::default(),
        );

        let props_builder = writer_builder.create_base_builder();
        let props = props_builder.build();

        // Can't directly test all properties, but we can verify it builds successfully
        assert_eq!(props.max_row_group_size(), 50000);
    }

    #[test]
    fn test_create_base_builder_without_dictionary() {
        let writer_builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::Snappy,
            ParquetStatistics::Chunk,
            ParquetWriterVersion::V2,
            100000,
            true,
            BloomFilterConfig::None,
            SortSpec::default(),
        );

        let props_builder = writer_builder.create_base_builder();
        let props = props_builder.build();

        assert_eq!(props.max_row_group_size(), 100000);
    }

    #[test]
    fn test_apply_bloom_filters_all_columns() {
        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig {
            fpp: 0.001,
            ndv: None,
        });

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            SortSpec::default(),
        );

        let mut ndv_map = HashMap::new();
        ndv_map.insert("id".to_string(), 100);
        ndv_map.insert("name".to_string(), 200);

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_bloom_filters_specific_columns_with_ndv() {
        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "id".to_string(),
            config: ColumnBloomFilterConfig {
                fpp: 0.005,
                ndv: None,
            },
        }]);

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            SortSpec::default(),
        );

        let mut ndv_map = HashMap::new();
        ndv_map.insert("id".to_string(), 150);

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_bloom_filters_specific_columns_from_map() {
        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "id".to_string(),
            config: ColumnBloomFilterConfig {
                fpp: 0.01,
                ndv: None,
            },
        }]);

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            SortSpec::default(),
        );

        let mut ndv_map = HashMap::new();
        ndv_map.insert("id".to_string(), 300);

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_bloom_filters_without_ndv() {
        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "test_column".to_string(),
            config: ColumnBloomFilterConfig {
                fpp: 0.01,
                ndv: None,
            },
        }]);

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            SortSpec::default(),
        );

        let ndv_map = HashMap::new();

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_bloom_filters_default_fpp() {
        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig {
            fpp: 0.01,
            ndv: None,
        });

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            SortSpec::default(),
        );

        let mut ndv_map = HashMap::new();
        ndv_map.insert("id".to_string(), 100);

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_bloom_filters_mixed_config() {
        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "id".to_string(),
            config: ColumnBloomFilterConfig {
                fpp: 0.001,
                ndv: None,
            },
        }]);

        let builder = ParquetWritePropertiesBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            SortSpec::default(),
        );

        let mut ndv_map = HashMap::new();
        ndv_map.insert("id".to_string(), 500);

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }
}
