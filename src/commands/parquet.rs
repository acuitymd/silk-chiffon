use crate::{
    BloomFilterConfig, ParquetArgs, converters::parquet::ParquetConverter,
    utils::filesystem::ensure_parent_dir_exists,
};
use anyhow::Result;

pub async fn run(args: ParquetArgs) -> Result<()> {
    let input_path = args.input.path().to_string_lossy().to_string();
    let output_path = args.output.path().to_path_buf();

    ensure_parent_dir_exists(&output_path).await?;

    let bloom_filters = if let Some(all_config) = args.bloom_all {
        BloomFilterConfig::All(all_config)
    } else if !args.bloom_column.is_empty() {
        BloomFilterConfig::Columns(args.bloom_column)
    } else {
        BloomFilterConfig::None
    };

    let converter = ParquetConverter::new(input_path, output_path)
        .with_sort_spec(args.sort_by)
        .with_record_batch_size(args.record_batch_size)
        .with_parquet_row_group_size(args.max_row_group_size)
        .with_compression(args.compression)
        .with_bloom_filters(bloom_filters)
        .with_statistics(args.statistics)
        .with_no_dictionary(args.no_dictionary)
        .with_writer_version(args.writer_version)
        .with_write_sorted_metadata(args.write_sorted_metadata);

    converter.convert().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_helpers::{file_helpers, test_data};
    use crate::{ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortSpec};
    use clio::{Input, OutputPath};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_run_returns_ok() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        let batch =
            test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["A", "B", "C"]);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let args = ParquetArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            sort_by: SortSpec::default(),
            compression: ParquetCompression::None,
            write_sorted_metadata: false,
            bloom_all: None,
            bloom_column: vec![],
            max_row_group_size: 1_048_576,
            statistics: ParquetStatistics::Page,
            record_batch_size: 122_880,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
        };

        assert!(run(args).await.is_ok());
    }

    #[tokio::test]
    async fn test_run_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        let batch =
            test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["A", "B", "C"]);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let args = ParquetArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            sort_by: SortSpec::default(),
            compression: ParquetCompression::Zstd,
            write_sorted_metadata: false,
            bloom_all: None,
            bloom_column: vec![],
            max_row_group_size: 1_048_576,
            statistics: ParquetStatistics::Page,
            record_batch_size: 122_880,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
        };

        assert!(run(args).await.is_ok());
    }
}
