use crate::{
    BloomFilterConfig, MergeToParquetArgs,
    converters::parquet::ParquetConverter,
    utils::{
        arrow_io::{ArrowIPCReader, MultiFileIterator},
        filesystem::ensure_parent_dir_exists,
    },
};
use anyhow::{Result, bail};
use glob::glob;

pub async fn run(args: MergeToParquetArgs) -> Result<()> {
    ensure_parent_dir_exists(args.output.path()).await?;

    let mut expanded_paths = Vec::new();
    for pattern in &args.inputs {
        for entry in glob(pattern)? {
            expanded_paths.push(entry?.to_string_lossy().to_string());
        }
    }

    expanded_paths.sort();
    expanded_paths.dedup();

    if expanded_paths.is_empty() {
        bail!("No input files found");
    }

    let iterator = if expanded_paths.len() == 1 {
        ArrowIPCReader::from_path(&expanded_paths[0])?.into_batch_iterator()?
    } else {
        Box::new(MultiFileIterator::new(
            expanded_paths,
            args.record_batch_size,
        )?)
    };

    let bloom_filters = if let Some(all_config) = args.bloom_all {
        BloomFilterConfig::All(all_config)
    } else if !args.bloom_column.is_empty() {
        BloomFilterConfig::Columns(args.bloom_column)
    } else {
        BloomFilterConfig::None
    };

    let parquet_converter =
        ParquetConverter::from_iterator(iterator, args.output.path().to_path_buf())
            .with_sort_spec(args.sort_by.clone())
            .with_query(args.query.clone())
            .with_compression(args.compression)
            .with_bloom_filters(bloom_filters)
            .with_statistics(args.statistics)
            .with_parquet_row_group_size(args.max_row_group_size)
            .with_no_dictionary(args.no_dictionary)
            .with_writer_version(args.writer_version)
            .with_write_sorted_metadata(args.write_sorted_metadata)
            .with_record_batch_size(args.record_batch_size);

    parquet_converter.convert().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortSpec,
        utils::test_helpers::{file_helpers, test_data, verify},
    };
    use clio::OutputPath;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_merge_to_parquet_two_files() {
        let temp_dir = tempdir().unwrap();
        let input1_path = temp_dir.path().join("input1.arrow");
        let input2_path = temp_dir.path().join("input2.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        let batch1 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[1, 2, 3],
            &["Alice", "Bob", "Charlie"],
        );
        let batch2 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[4, 5, 6],
            &["David", "Eve", "Frank"],
        );

        file_helpers::write_arrow_file(&input1_path, &schema, vec![batch1]).unwrap();
        file_helpers::write_arrow_file(&input2_path, &schema, vec![batch2]).unwrap();

        let args = MergeToParquetArgs {
            inputs: vec![
                input1_path.to_string_lossy().to_string(),
                input2_path.to_string_lossy().to_string(),
            ],
            output: OutputPath::new(&output_path).unwrap(),
            query: None,
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

        run(args).await.unwrap();

        let batches = verify::read_parquet_file(&output_path).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 6);
    }

    #[tokio::test]
    async fn test_merge_to_parquet_with_sorting() {
        let temp_dir = tempdir().unwrap();
        let input1_path = temp_dir.path().join("input1.arrow");
        let input2_path = temp_dir.path().join("input2.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        let batch1 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[5, 3, 1],
            &["Eve", "Charlie", "Alice"],
        );
        let batch2 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[6, 4, 2],
            &["Frank", "David", "Bob"],
        );

        file_helpers::write_arrow_file(&input1_path, &schema, vec![batch1]).unwrap();
        file_helpers::write_arrow_file(&input2_path, &schema, vec![batch2]).unwrap();

        let args = MergeToParquetArgs {
            inputs: vec![
                input1_path.to_string_lossy().to_string(),
                input2_path.to_string_lossy().to_string(),
            ],
            output: OutputPath::new(&output_path).unwrap(),
            query: None,
            sort_by: "id".parse().unwrap(),
            compression: ParquetCompression::None,
            write_sorted_metadata: true,
            bloom_all: None,
            bloom_column: vec![],
            max_row_group_size: 1_048_576,
            statistics: ParquetStatistics::Page,
            record_batch_size: 122_880,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
        };

        run(args).await.unwrap();

        let batches = verify::read_parquet_file(&output_path).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 6);

        let ids = verify::extract_column_as_i32_vec(&batches[0], "id");
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
    }
}
