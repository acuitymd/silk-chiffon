use crate::converters::split::{OutputFormat, SplitConverter};
use crate::{BloomFilterConfig, ParquetCompression, SplitToParquetArgs};
use anyhow::Result;

pub async fn run(args: SplitToParquetArgs) -> Result<()> {
    let bloom_config = if args.bloom_all.is_some() {
        BloomFilterConfig::All(args.bloom_all.unwrap())
    } else if !args.bloom_column.is_empty() {
        BloomFilterConfig::Columns(args.bloom_column)
    } else {
        BloomFilterConfig::None
    };

    let output_format = OutputFormat::Parquet {
        compression: match args.compression {
            ParquetCompression::None => None,
            other => Some(other),
        },
        statistics: args.statistics,
        max_row_group_size: args.max_row_group_size,
        writer_version: args.writer_version,
        no_dictionary: args.no_dictionary,
        bloom_filters: bloom_config,
        write_sorted_metadata: args.write_sorted_metadata,
    };

    let converter = SplitConverter::new(
        args.input.path().to_string_lossy().to_string(),
        args.by,
        args.output_template,
    )
    .with_output_format(output_format)
    .with_record_batch_size(args.record_batch_size)
    .with_sort_spec(args.sort_by)
    .with_create_dirs(args.create_dirs)
    .with_overwrite(args.overwrite)
    .with_query(args.query);

    let _created_files = converter.convert().await?;

    Ok(())
}
