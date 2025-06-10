use anyhow::anyhow;
use arrow::ipc::{
    CompressionType,
    writer::{FileWriter, IpcWriteOptions},
};
use futures::stream::StreamExt;
use std::{
    fs::{File, create_dir_all},
    sync::Arc,
};

use crate::{ArrowArgs, ArrowCompression, SortDirection, system::get_available_memory};
use anyhow::Result;
use datafusion::{
    execution::{options::ArrowReadOptions, runtime_env::RuntimeEnvBuilder},
    logical_expr::SortExpr,
    prelude::{SessionConfig, SessionContext, col},
};

pub async fn run(args: ArrowArgs) -> Result<()> {
    let parent = args
        .output
        .path()
        .parent()
        .ok_or_else(|| anyhow!("Output directory not found"))?;

    create_dir_all(parent)?;

    let mut config = SessionConfig::new();
    let options = config.options_mut();
    options.execution.batch_size = 122_880;

    let runtime_config =
        RuntimeEnvBuilder::new().with_memory_limit(get_available_memory() as usize, 1.0);
    let runtime_env = runtime_config.build()?;
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

    let input_path = args.input.path().to_str().unwrap();

    ctx.register_arrow("input_table", input_path, ArrowReadOptions::default())
        .await?;

    let mut df = ctx.table("input_table").await?;

    if let Some(sort_by) = args.sort_by {
        let sort_exprs = sort_by
            .columns
            .iter()
            .map(|sort_column| {
                col(&sort_column.name).sort(
                    matches!(sort_column.direction, SortDirection::Ascending),
                    matches!(sort_column.direction, SortDirection::Descending),
                )
            })
            .collect::<Vec<SortExpr>>();
        df = df.sort(sort_exprs)?;
    }

    let mut stream = df.execute_stream().await?;
    let schema = stream.schema();
    let output_path = args.output.path();
    let file = File::create(output_path.to_str().unwrap())?;
    let compression = match args.compression {
        ArrowCompression::Zstd => Some(CompressionType::ZSTD),
        ArrowCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
        ArrowCompression::None => None,
    };

    let write_options = IpcWriteOptions::default().try_with_compression(compression)?;

    let mut writer = FileWriter::try_new_with_options(file, &schema, write_options)?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        writer.write(&batch)?;
    }

    writer.finish()?;

    Ok(())
}
