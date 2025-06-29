use crate::{
    BloomFilterConfig, ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortSpec,
};
use std::path::PathBuf;

pub struct ParquetConfig {
    pub input_path: String,
    pub output_path: PathBuf,
    pub sort_spec: Option<SortSpec>,
    pub record_batch_size: usize,
    pub compression: ParquetCompression,
    pub bloom_filters: BloomFilterConfig,
    pub statistics: ParquetStatistics,
    pub parquet_row_group_size: usize,
    pub no_dictionary: bool,
    pub writer_version: ParquetWriterVersion,
    pub write_sorted_metadata: bool,
}

impl ParquetConfig {
    pub fn new(input_path: String, output_path: PathBuf) -> Self {
        Self {
            input_path,
            output_path,
            sort_spec: None,
            record_batch_size: 122_880,
            compression: ParquetCompression::None,
            bloom_filters: BloomFilterConfig::default(),
            statistics: ParquetStatistics::Page,
            parquet_row_group_size: 1_048_576,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
            write_sorted_metadata: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AllColumnsBloomFilterConfig, SortColumn, SortDirection};

    #[test]
    fn test_new_default_values() {
        let input = "input.arrow".to_string();
        let output = PathBuf::from("output.parquet");
        let config = ParquetConfig::new(input.clone(), output.clone());

        assert_eq!(config.input_path, input);
        assert_eq!(config.output_path, output);
        assert!(config.sort_spec.is_none());
        assert_eq!(config.record_batch_size, 122_880);
        assert!(matches!(config.compression, ParquetCompression::None));
        assert!(matches!(config.bloom_filters, BloomFilterConfig::None));
        assert!(matches!(config.statistics, ParquetStatistics::Page));
        assert_eq!(config.parquet_row_group_size, 1_048_576);
        assert!(!config.no_dictionary);
        assert!(matches!(config.writer_version, ParquetWriterVersion::V2));
        assert!(!config.write_sorted_metadata);
    }

    #[test]
    fn test_config_with_sort_spec() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

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

        config.sort_spec = Some(sort_spec);

        assert!(config.sort_spec.is_some());
        let spec = config.sort_spec.unwrap();
        assert_eq!(spec.columns.len(), 2);
        assert_eq!(spec.columns[0].name, "id");
        assert!(matches!(
            spec.columns[0].direction,
            SortDirection::Ascending
        ));
        assert_eq!(spec.columns[1].name, "value");
        assert!(matches!(
            spec.columns[1].direction,
            SortDirection::Descending
        ));
    }

    #[test]
    fn test_config_with_bloom_filters() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: Some(0.001) });

        config.bloom_filters = bloom_config;

        assert!(config.bloom_filters.is_configured());
        if let BloomFilterConfig::All(all_config) = &config.bloom_filters {
            assert_eq!(all_config.fpp, Some(0.001));
        } else {
            panic!("Expected BloomFilterConfig::All");
        }
    }

    #[test]
    fn test_config_compression_variants() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

        config.compression = ParquetCompression::Zstd;
        assert!(matches!(config.compression, ParquetCompression::Zstd));

        config.compression = ParquetCompression::Snappy;
        assert!(matches!(config.compression, ParquetCompression::Snappy));

        config.compression = ParquetCompression::Gzip;
        assert!(matches!(config.compression, ParquetCompression::Gzip));

        config.compression = ParquetCompression::Lz4;
        assert!(matches!(config.compression, ParquetCompression::Lz4));
    }

    #[test]
    fn test_config_statistics_variants() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

        config.statistics = ParquetStatistics::None;
        assert!(matches!(config.statistics, ParquetStatistics::None));

        config.statistics = ParquetStatistics::Chunk;
        assert!(matches!(config.statistics, ParquetStatistics::Chunk));

        config.statistics = ParquetStatistics::Page;
        assert!(matches!(config.statistics, ParquetStatistics::Page));
    }

    #[test]
    fn test_config_writer_version_variants() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

        config.writer_version = ParquetWriterVersion::V1;
        assert!(matches!(config.writer_version, ParquetWriterVersion::V1));

        config.writer_version = ParquetWriterVersion::V2;
        assert!(matches!(config.writer_version, ParquetWriterVersion::V2));
    }

    #[test]
    fn test_config_custom_batch_and_row_group_sizes() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

        config.record_batch_size = 8192;
        config.parquet_row_group_size = 524288;

        assert_eq!(config.record_batch_size, 8192);
        assert_eq!(config.parquet_row_group_size, 524288);
    }

    #[test]
    fn test_config_boolean_flags() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

        config.no_dictionary = true;
        config.write_sorted_metadata = true;

        assert!(config.no_dictionary);
        assert!(config.write_sorted_metadata);
    }

    #[test]
    fn test_config_path_handling() {
        let input = "/path/to/input.arrow".to_string();
        let output = PathBuf::from("/path/to/output.parquet");
        let config = ParquetConfig::new(input.clone(), output.clone());

        assert_eq!(config.input_path, input);
        assert_eq!(config.output_path, output);
        assert_eq!(
            config.output_path.to_str().unwrap(),
            "/path/to/output.parquet"
        );
    }

    #[test]
    fn test_config_full_configuration() {
        let mut config =
            ParquetConfig::new("input.arrow".to_string(), PathBuf::from("output.parquet"));

        config.sort_spec = Some(SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        });
        config.record_batch_size = 10000;
        config.compression = ParquetCompression::Zstd;
        config.bloom_filters =
            BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: Some(0.001) });
        config.statistics = ParquetStatistics::Chunk;
        config.parquet_row_group_size = 500000;
        config.no_dictionary = true;
        config.writer_version = ParquetWriterVersion::V1;
        config.write_sorted_metadata = true;

        assert!(config.sort_spec.is_some());
        assert_eq!(config.record_batch_size, 10000);
        assert!(matches!(config.compression, ParquetCompression::Zstd));
        assert!(config.bloom_filters.is_configured());
        assert!(matches!(config.statistics, ParquetStatistics::Chunk));
        assert_eq!(config.parquet_row_group_size, 500000);
        assert!(config.no_dictionary);
        assert!(matches!(config.writer_version, ParquetWriterVersion::V1));
        assert!(config.write_sorted_metadata);
    }
}
