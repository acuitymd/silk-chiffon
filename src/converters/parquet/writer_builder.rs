use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;
use parquet::{
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::{
        EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
    },
    schema::types::ColumnPath,
};
use std::{collections::HashMap, path::Path};

use super::metadata::SortMetadataBuilder;
use crate::{
    BloomFilterConfig, ParquetCompression, ParquetStatistics, ParquetWriterVersion,
    utils::arrow_io::ArrowIPCReader,
};

pub struct ParquetWriterBuilder {
    compression: Compression,
    statistics: EnabledStatistics,
    writer_version: WriterVersion,
    parquet_row_group_size: usize,
    no_dictionary: bool,
    bloom_filters: BloomFilterConfig,
    sort_metadata_builder: Option<SortMetadataBuilder>,
}

impl ParquetWriterBuilder {
    pub fn new(
        compression: ParquetCompression,
        statistics: ParquetStatistics,
        writer_version: ParquetWriterVersion,
        parquet_row_group_size: usize,
        no_dictionary: bool,
        bloom_filters: BloomFilterConfig,
        sort_metadata_builder: Option<SortMetadataBuilder>,
    ) -> Self {
        let compression = match compression {
            ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP(GzipLevel::default()),
            ParquetCompression::Lz4 => Compression::LZ4_RAW,
            ParquetCompression::None => Compression::UNCOMPRESSED,
        };

        let writer_version = match writer_version {
            ParquetWriterVersion::V1 => WriterVersion::PARQUET_1_0,
            ParquetWriterVersion::V2 => WriterVersion::PARQUET_2_0,
        };

        let statistics = match statistics {
            ParquetStatistics::None => EnabledStatistics::None,
            ParquetStatistics::Chunk => EnabledStatistics::Chunk,
            ParquetStatistics::Page => EnabledStatistics::Page,
        };

        Self {
            compression,
            statistics,
            writer_version,
            parquet_row_group_size,
            no_dictionary,
            bloom_filters,
            sort_metadata_builder,
        }
    }

    pub fn build(
        &self,
        input_path: &Path,
        ndv_map: &HashMap<String, u64>,
    ) -> Result<WriterProperties> {
        let schema = ArrowIPCReader::from_path(input_path)?.schema()?;

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
                let fpp = bloom_all.fpp.unwrap_or(0.01);
                builder = builder
                    .set_bloom_filter_enabled(true)
                    .set_bloom_filter_fpp(fpp);

                for (col_name, &ndv) in ndv_map {
                    let col_path = ColumnPath::from(col_name.as_str());
                    builder = builder.set_column_bloom_filter_ndv(col_path, ndv);
                }
                Ok(builder)
            }
            BloomFilterConfig::Columns(columns) => {
                for bloom_col in columns {
                    let col_path = ColumnPath::from(bloom_col.name.as_str());
                    let fpp = bloom_col.config.fpp.unwrap_or(0.01);

                    let ndv = ndv_map.get(&bloom_col.name).copied().ok_or_else(|| {
                        anyhow!("NDV not available for column {}", bloom_col.name)
                    })?;

                    builder = builder
                        .set_column_bloom_filter_enabled(col_path.clone(), true)
                        .set_column_bloom_filter_fpp(col_path.clone(), fpp)
                        .set_column_bloom_filter_ndv(col_path, ndv);
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
        match &self.sort_metadata_builder {
            Some(sort_builder) => sort_builder.apply(builder, schema),
            None => Ok(builder),
        }
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
        let builder = ParquetWriterBuilder::new(
            ParquetCompression::Zstd,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.compression, Compression::ZSTD(_)));

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::Snappy,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.compression, Compression::SNAPPY));

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::Gzip,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.compression, Compression::GZIP(_)));

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::Lz4,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.compression, Compression::LZ4_RAW));

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.compression, Compression::UNCOMPRESSED));
    }

    #[test]
    fn test_writer_version() {
        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.writer_version, WriterVersion::PARQUET_1_0));

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V2,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.writer_version, WriterVersion::PARQUET_2_0));
    }

    #[test]
    fn test_statistics() {
        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.statistics, EnabledStatistics::None));

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::Chunk,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.statistics, EnabledStatistics::Chunk));

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::Page,
            ParquetWriterVersion::V1,
            1000,
            false,
            BloomFilterConfig::None,
            None,
        );
        assert!(matches!(builder.statistics, EnabledStatistics::Page));
    }

    #[test]
    fn test_create_base_builder_with_dictionary() {
        let writer_builder = ParquetWriterBuilder::new(
            ParquetCompression::Snappy,
            ParquetStatistics::Chunk,
            ParquetWriterVersion::V2,
            50000,
            false,
            BloomFilterConfig::None,
            None,
        );

        let props_builder = writer_builder.create_base_builder();
        let props = props_builder.build();

        // Can't directly test all properties, but we can verify it builds successfully
        assert_eq!(props.max_row_group_size(), 50000);
    }

    #[test]
    fn test_create_base_builder_without_dictionary() {
        let writer_builder = ParquetWriterBuilder::new(
            ParquetCompression::Snappy,
            ParquetStatistics::Chunk,
            ParquetWriterVersion::V2,
            100000,
            true,
            BloomFilterConfig::None,
            None,
        );

        let props_builder = writer_builder.create_base_builder();
        let props = props_builder.build();

        assert_eq!(props.max_row_group_size(), 100000);
    }

    #[test]
    fn test_apply_bloom_filters_all_columns() {
        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: Some(0.001) });

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            None,
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
            config: ColumnBloomFilterConfig { fpp: Some(0.005) },
        }]);

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            None,
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
            config: ColumnBloomFilterConfig { fpp: Some(0.01) },
        }]);

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            None,
        );

        let mut ndv_map = HashMap::new();
        ndv_map.insert("id".to_string(), 300);

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_bloom_filters_missing_ndv() {
        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "missing_column".to_string(),
            config: ColumnBloomFilterConfig { fpp: Some(0.01) },
        }]);

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            None,
        );

        let ndv_map = HashMap::new();

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("NDV not available for column missing_column")
        );
    }

    #[test]
    fn test_apply_bloom_filters_default_fpp() {
        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: None });

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            None,
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
            config: ColumnBloomFilterConfig { fpp: Some(0.001) },
        }]);

        let builder = ParquetWriterBuilder::new(
            ParquetCompression::None,
            ParquetStatistics::None,
            ParquetWriterVersion::V1,
            1000,
            false,
            bloom_config,
            None,
        );

        let mut ndv_map = HashMap::new();
        ndv_map.insert("id".to_string(), 500);

        let props_builder = WriterProperties::builder();
        let result = builder.apply_bloom_filters(props_builder, &ndv_map);

        assert!(result.is_ok());
    }
}
