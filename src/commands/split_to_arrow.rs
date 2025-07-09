use crate::converters::split::{OutputFormat, SplitConverter};
use crate::{ArrowCompression, SplitToArrowArgs};
use anyhow::Result;

pub async fn run(args: SplitToArrowArgs) -> Result<()> {
    let output_format = OutputFormat::Arrow {
        compression: match args.compression {
            ArrowCompression::None => None,
            other => Some(other),
        },
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
    .with_overwrite(args.overwrite);

    let _created_files = converter.convert().await?;

    Ok(())
}
