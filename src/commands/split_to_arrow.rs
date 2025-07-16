use crate::converters::split::{OutputFormat, SplitConversionResult, SplitConverter};
use crate::{ArrowCompression, ListOutputsFormat, SplitToArrowArgs};
use anyhow::Result;
use std::path::PathBuf;

pub async fn run_with_result(args: SplitToArrowArgs) -> Result<SplitConversionResult> {
    let output_format = OutputFormat::Arrow {
        compression: match args.compression {
            ArrowCompression::None => None,
            other => Some(other),
        },
        ipc_format: args.output_ipc_format,
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

    let conversion_result = converter.convert().await?;

    match args.list_outputs {
        ListOutputsFormat::Text => {
            // grab the file paths into a vector so we can sort them, to provide a stable output order
            let mut files: Vec<PathBuf> =
                conversion_result.output_files.values().cloned().collect();
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

pub async fn run(args: SplitToArrowArgs) -> Result<()> {
    run_with_result(args).await?;
    Ok(())
}
