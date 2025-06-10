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

use crate::{ArrowArgs, ArrowCompression, SortDirection};
use anyhow::Result;
use arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::{
    execution::{options::ArrowReadOptions, runtime_env::RuntimeEnvBuilder},
    prelude::{SessionConfig, SessionContext, col},
};
use std::io::BufReader;

pub async fn run(args: ArrowArgs) -> Result<()> {
    let parent = args
        .output
        .path()
        .parent()
        .ok_or_else(|| anyhow!("Output directory not found"))?;

    create_dir_all(parent)?;

    let input_path = args.input.path().to_str().unwrap();
    let output_path = args.output.path();

    let compression = match args.compression {
        ArrowCompression::Zstd => Some(CompressionType::ZSTD),
        ArrowCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
        ArrowCompression::None => None,
    };

    let write_options = IpcWriteOptions::default().try_with_compression(compression)?;

    if args.sort_by.is_none() {
        stream_arrow_direct(input_path, output_path, write_options).await
    } else {
        stream_arrow_with_sorting(
            input_path,
            output_path,
            write_options,
            args.sort_by.unwrap(),
        )
        .await
    }
}

async fn stream_arrow_direct(
    input_path: &str,
    output_path: &std::path::Path,
    write_options: IpcWriteOptions,
) -> Result<()> {
    let input_file = File::open(input_path)?;
    let mut buf_reader = BufReader::new(input_file);

    match FileReader::try_new(&mut buf_reader, None) {
        Ok(file_reader) => {
            let schema = file_reader.schema();
            let output_file = File::create(output_path)?;
            let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

            for batch_result in file_reader {
                let batch = batch_result?;
                writer.write(&batch)?;
            }
            writer.finish()?;
        }
        Err(_) => {
            let input_file = File::open(input_path)?;
            let buf_reader = BufReader::new(input_file);
            let stream_reader = StreamReader::try_new(buf_reader, None)?;

            let schema = stream_reader.schema();
            let output_file = File::create(output_path)?;
            let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

            for batch_result in stream_reader {
                let batch = batch_result?;
                writer.write(&batch)?;
            }
            writer.finish()?;
        }
    }

    Ok(())
}

async fn stream_arrow_with_sorting(
    input_path: &str,
    output_path: &std::path::Path,
    write_options: IpcWriteOptions,
    sort_spec: crate::SortSpec,
) -> Result<()> {
    let input_file = File::open(input_path)?;
    let mut buf_reader = BufReader::new(input_file);

    let temp_file_path = match FileReader::try_new(&mut buf_reader, None) {
        Ok(_) => None,
        Err(_) => {
            let temp_path = output_path.with_extension("tmp.arrow");
            convert_stream_to_file_format(input_path, &temp_path)?;
            Some(temp_path)
        }
    };

    let datafusion_input = temp_file_path
        .as_ref()
        .map(|p| p.to_str().unwrap())
        .unwrap_or(input_path);

    let mut config = SessionConfig::new();
    let options = config.options_mut();
    options.execution.batch_size = 122_880;

    let runtime_config = RuntimeEnvBuilder::new();
    let runtime_env = runtime_config.build()?;
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

    ctx.register_arrow("input_table", datafusion_input, ArrowReadOptions::default())
        .await?;
    let df = ctx.table("input_table").await?;

    let sort_exprs = sort_spec
        .columns
        .iter()
        .map(|sort_column| {
            col(&sort_column.name).sort(
                matches!(sort_column.direction, SortDirection::Ascending),
                true, // nulls first
            )
        })
        .collect();

    let df = df.sort(sort_exprs)?;

    let mut stream = df.execute_stream().await?;
    let schema = stream.schema();
    let output_file = File::create(output_path)?;
    let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        writer.write(&batch)?;
    }

    writer.finish()?;

    if let Some(temp_path) = temp_file_path {
        let _ = std::fs::remove_file(temp_path);
    }

    Ok(())
}

fn convert_stream_to_file_format(input_path: &str, output_path: &std::path::Path) -> Result<()> {
    let input_file = File::open(input_path)?;
    let buf_reader = BufReader::new(input_file);
    let stream_reader = StreamReader::try_new(buf_reader, None)?;

    let schema = stream_reader.schema();
    let output_file = File::create(output_path)?;
    let mut writer = FileWriter::try_new(output_file, &schema)?;

    for batch_result in stream_reader {
        let batch = batch_result?;
        writer.write(&batch)?;
    }

    writer.finish()?;
    Ok(())
}
