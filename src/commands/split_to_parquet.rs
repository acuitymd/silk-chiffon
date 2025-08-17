use crate::converters::split::{OutputFormat, SplitConversionResult, SplitConverter};
use crate::{BloomFilterConfig, ListOutputsFormat, ParquetCompression, SplitToParquetArgs};
use anyhow::Result;
use std::path::PathBuf;

pub async fn run_with_result(args: SplitToParquetArgs) -> Result<SplitConversionResult> {
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
    .with_query(args.query)
    .with_dialect(args.dialect);

    let conversion_result = converter.convert().await?;

    match args.list_outputs {
        ListOutputsFormat::Text => {
            // grab the file paths into a vector so we can sort them, to provide a stable output order
            let mut files: Vec<PathBuf> = conversion_result
                .output_files
                .values()
                .map(|result| result.path.clone())
                .collect();
            files.sort();

            for file in files {
                println!("{}", file.as_path().display());
            }
        }
        ListOutputsFormat::Json => {
            let json = serde_json::to_string_pretty(&conversion_result)?;
            println!("{json}");
        }
        ListOutputsFormat::None => {}
    }

    Ok(conversion_result)
}

pub async fn run(args: SplitToParquetArgs) -> Result<()> {
    run_with_result(args).await?;
    Ok(())
}
