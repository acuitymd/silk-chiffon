use arrow::{
    error::ArrowError,
    ipc::{
        CompressionType,
        writer::{FileWriter, IpcWriteOptions},
    },
};
use futures::stream::StreamExt;
use std::{fs::File, sync::Arc};

use crate::{
    ArrowArgs, ArrowCompression, SortDirection, utils::filesystem::ensure_parent_dir_exists,
};
use anyhow::{Result, anyhow};
use arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::{
    execution::{options::ArrowReadOptions, runtime_env::RuntimeEnvBuilder},
    prelude::{SessionConfig, SessionContext, col},
};
use std::io::BufReader;

pub async fn run(args: ArrowArgs) -> Result<()> {
    ensure_parent_dir_exists(args.output.path()).await?;

    let input_path = args.input.path().to_str().unwrap();
    let output_path = args.output.path();

    let compression = match args.compression {
        ArrowCompression::Zstd => Some(CompressionType::ZSTD),
        ArrowCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
        ArrowCompression::None => None,
    };

    let write_options = IpcWriteOptions::default().try_with_compression(compression)?;

    match args.sort_by {
        Some(sort_spec) => {
            stream_arrow_with_sorting(input_path, output_path, write_options, sort_spec).await
        }
        None => stream_arrow_direct(input_path, output_path, write_options).await,
    }
}

async fn stream_arrow_direct(
    input_path: &str,
    output_path: &std::path::Path,
    write_options: IpcWriteOptions,
) -> Result<()> {
    let input_file = File::open(input_path)?;

    match FileReader::try_new(input_file, None) {
        Ok(_) => {
            // The input file is in the Arrow IPC *file* format
            convert_file_to_file_format(input_path, output_path, write_options).await?;
        }
        Err(ArrowError::ParseError(_)) => {
            // Presumably the input file is in the Arrow IPC *stream* format
            convert_stream_to_file_format(input_path, output_path, write_options).await?;
        }
        Err(e) => return Err(anyhow!("Error parsing input file {:?}", e)),
    }

    Ok(())
}

async fn stream_arrow_with_sorting(
    input_path: &str,
    output_path: &std::path::Path,
    write_options: IpcWriteOptions,
    sort_spec: crate::SortSpec,
) -> Result<()> {
    let mut config = SessionConfig::new();
    let options = config.options_mut();
    options.execution.batch_size = 122_880;

    let runtime_config = RuntimeEnvBuilder::new();
    let runtime_env = runtime_config.build()?;
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

    let input_file = File::open(input_path)?;
    let mut buf_reader = BufReader::new(input_file);

    let temp_file_path = match FileReader::try_new(&mut buf_reader, None) {
        Ok(_) => {
            // The input file is in the Arrow IPC *file* format, so we can use it directly
            None
        }
        Err(ArrowError::ParseError(_)) => {
            // Presumably the input file is in the Arrow IPC *stream* format, so convert it to the file format in a temp file
            let temp_path = output_path.with_extension("tmp.arrow");
            let temp_write_options = IpcWriteOptions::default();
            convert_stream_to_file_format(input_path, &temp_path, temp_write_options).await?;
            Some(temp_path)
        }
        Err(e) => return Err(anyhow!("Error parsing input file {:?}", e)),
    };

    let datafusion_input = temp_file_path
        .as_ref()
        .map(|p| p.to_str().unwrap())
        .unwrap_or(input_path);

    ctx.register_arrow("input_table", datafusion_input, ArrowReadOptions::default())
        .await?;
    let df = ctx.table("input_table").await?;

    let sort_exprs = sort_spec
        .columns
        .iter()
        .map(|sort_column| {
            col(&sort_column.name).sort(
                matches!(sort_column.direction, SortDirection::Ascending),
                // match the behavior of postgres here, where nulls are first for descending
                // and last for ascending
                // https://www.postgresql.org/docs/current/queries-order.html#:~:text=The%20NULLS%20FIRST%20and%20NULLS%20LAST%20options%20can%20be%20used%20to%20determine%20whether%20nulls%20appear%20before%20or%20after%20non%2Dnull%20values%20in%20the%20sort%20ordering.%20By%20default%2C%20null%20values%20sort%20as%20if%20larger%20than%20any%20non%2Dnull%20value%3B%20that%20is%2C%20NULLS%20FIRST%20is%20the%20default%20for%20DESC%20order%2C%20and%20NULLS%20LAST%20otherwise
                matches!(sort_column.direction, SortDirection::Descending),
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

async fn convert_stream_to_file_format(
    input_path: &str,
    output_path: &std::path::Path,
    write_options: IpcWriteOptions,
) -> Result<()> {
    let input_file = File::open(input_path)?;
    let stream_reader = StreamReader::try_new_buffered(input_file, None)?;

    let schema = stream_reader.schema();
    let output_file = File::create(output_path)?;
    let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

    tokio::task::spawn_blocking(move || -> Result<()> {
        for batch_result in stream_reader {
            let batch = batch_result?;
            writer.write(&batch)?;
        }
        writer.finish()?;

        Ok(())
    })
    .await??;

    Ok(())
}

async fn convert_file_to_file_format(
    input_path: &str,
    output_path: &std::path::Path,
    write_options: IpcWriteOptions,
) -> Result<()> {
    let input_file = File::open(input_path)?;
    let file_reader = FileReader::try_new(input_file, None)?;

    let schema = file_reader.schema();
    let output_file = File::create(output_path)?;
    let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

    tokio::task::spawn_blocking(move || -> Result<()> {
        for batch_result in file_reader {
            let batch = batch_result?;
            writer.write(&batch)?;
        }

        writer.finish()?;

        Ok(())
    })
    .await??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use nix::unistd::Uid;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch(schema: &Arc<Schema>) -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    fn write_test_arrow_file(path: &std::path::Path, use_stream: bool) -> Result<()> {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema);
        let file = File::create(path)?;

        if use_stream {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        } else {
            let mut writer = FileWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_convert_stream_to_file_format() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, true).unwrap();

        convert_stream_to_file_format(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
        )
        .await
        .unwrap();

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let schema = file_reader.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_convert_file_to_file_format() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, false).unwrap();

        let write_options = IpcWriteOptions::default();

        convert_file_to_file_format(input_path.to_str().unwrap(), &output_path, write_options)
            .await
            .unwrap();

        assert!(output_path.exists());

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let schema = file_reader.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_stream_arrow_direct_file_format() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, false).unwrap();

        let write_options = IpcWriteOptions::default();
        stream_arrow_direct(input_path.to_str().unwrap(), &output_path, write_options)
            .await
            .unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_stream_arrow_direct_stream_format() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, true).unwrap();

        let write_options = IpcWriteOptions::default();
        stream_arrow_direct(input_path.to_str().unwrap(), &output_path, write_options)
            .await
            .unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_run_creates_parent_directory() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_dir = temp_dir.path().join("subdir");
        let output_path = output_dir.join("output.arrow");

        write_test_arrow_file(&input_path, false).unwrap();

        std::fs::create_dir_all(&output_dir).unwrap();

        let args = ArrowArgs {
            input: clio::Input::new(&input_path).unwrap(),
            output: clio::OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            compression: ArrowCompression::None,
        };

        run(args).await.unwrap();

        assert!(output_dir.exists());
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_run_with_compression() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, false).unwrap();

        let args = ArrowArgs {
            input: clio::Input::new(&input_path).unwrap(),
            output: clio::OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            compression: ArrowCompression::Zstd,
        };

        run(args).await.unwrap();
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_stream_arrow_with_sorting_file_format() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![5, 2, 4, 1, 3]);
        let name_array = StringArray::from(vec!["Eve", "Bob", "David", "Alice", "Charlie"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let file = File::create(&input_path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let sort_spec = crate::SortSpec {
            columns: vec![crate::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        stream_arrow_with_sorting(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
            sort_spec,
        )
        .await
        .unwrap();

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let mut batches = vec![];
        for batch_result in file_reader {
            batches.push(batch_result.unwrap());
        }

        assert_eq!(batches.len(), 1);
        let result_batch = &batches[0];

        let ids = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
        assert_eq!(ids.value(3), 4);
        assert_eq!(ids.value(4), 5);
    }

    #[tokio::test]
    async fn test_stream_arrow_with_sorting_stream_format() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        let value_array = Int32Array::from(vec![30, 10, 20]);
        let category_array = StringArray::from(vec!["C", "A", "B"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(value_array), Arc::new(category_array)],
        )
        .unwrap();

        let file = File::create(&input_path).unwrap();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let sort_spec = crate::SortSpec {
            columns: vec![crate::SortColumn {
                name: "value".to_string(),
                direction: SortDirection::Descending,
            }],
        };

        stream_arrow_with_sorting(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
            sort_spec,
        )
        .await
        .unwrap();

        let temp_file = output_path.with_extension("tmp.arrow");
        assert!(!temp_file.exists());

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let batch = file_reader.into_iter().next().unwrap().unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(values.value(0), 30);
        assert_eq!(values.value(1), 20);
        assert_eq!(values.value(2), 10);
    }

    #[tokio::test]
    async fn test_multi_column_sorting() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let group_array = Int32Array::from(vec![1, 2, 1, 2, 1]);
        let value_array = Int32Array::from(vec![30, 20, 10, 40, 20]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(group_array), Arc::new(value_array)],
        )
        .unwrap();

        let file = File::create(&input_path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let sort_spec = crate::SortSpec {
            columns: vec![
                crate::SortColumn {
                    name: "group".to_string(),
                    direction: SortDirection::Ascending,
                },
                crate::SortColumn {
                    name: "value".to_string(),
                    direction: SortDirection::Descending,
                },
            ],
        };

        stream_arrow_with_sorting(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
            sort_spec,
        )
        .await
        .unwrap();

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let batch = file_reader.into_iter().next().unwrap().unwrap();
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(groups.value(0), 1);
        assert_eq!(values.value(0), 30);
        assert_eq!(groups.value(1), 1);
        assert_eq!(values.value(1), 20);
        assert_eq!(groups.value(2), 1);
        assert_eq!(values.value(2), 10);

        assert_eq!(groups.value(3), 2);
        assert_eq!(values.value(3), 40);
        assert_eq!(groups.value(4), 2);
        assert_eq!(values.value(4), 20);
    }

    #[tokio::test]
    async fn test_sorting_with_nulls() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![Some(3), None, Some(1), None, Some(2)]);
        let name_array = StringArray::from(vec!["C", "null1", "A", "null2", "B"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let file = File::create(&input_path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let sort_spec = crate::SortSpec {
            columns: vec![crate::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        stream_arrow_with_sorting(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
            sort_spec,
        )
        .await
        .unwrap();

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let batch = file_reader.into_iter().next().unwrap().unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
        assert!(ids.is_null(3));
        assert!(ids.is_null(4));
    }

    #[tokio::test]
    async fn test_sorting_with_nulls_descending() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![Some(3), None, Some(1), None, Some(2)]);
        let name_array = StringArray::from(vec!["C", "null1", "A", "null2", "B"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let file = File::create(&input_path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let sort_spec = crate::SortSpec {
            columns: vec![crate::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Descending,
            }],
        };

        stream_arrow_with_sorting(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
            sort_spec,
        )
        .await
        .unwrap();

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let batch = file_reader.into_iter().next().unwrap().unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert!(ids.is_null(0));
        assert!(ids.is_null(1));
        assert_eq!(ids.value(2), 3);
        assert_eq!(ids.value(3), 2);
        assert_eq!(ids.value(4), 1);
    }

    #[tokio::test]
    async fn test_run_with_lz4_compression() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, false).unwrap();

        let args = ArrowArgs {
            input: clio::Input::new(&input_path).unwrap(),
            output: clio::OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            compression: ArrowCompression::Lz4,
        };

        run(args).await.unwrap();
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_sorting_with_compression() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, true).unwrap();

        let sort_spec = crate::SortSpec {
            columns: vec![crate::SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Descending,
            }],
        };

        let args = ArrowArgs {
            input: clio::Input::new(&input_path).unwrap(),
            output: clio::OutputPath::new(&output_path).unwrap(),
            sort_by: Some(sort_spec),
            compression: ArrowCompression::Zstd,
        };

        run(args).await.unwrap();
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_multiple_batches() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema = create_test_schema();

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["A", "B", "C"])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["D", "E"])),
            ],
        )
        .unwrap();

        let file = File::create(&input_path).unwrap();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.finish().unwrap();

        convert_stream_to_file_format(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
        )
        .await
        .unwrap();

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let batches: Vec<_> = file_reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[1].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema = create_test_schema();

        let file = File::create(&input_path).unwrap();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema).unwrap();
        writer.finish().unwrap();

        convert_stream_to_file_format(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
        )
        .await
        .unwrap();

        let file = File::open(&output_path).unwrap();
        let mut reader = BufReader::new(file);
        let file_reader = FileReader::try_new(&mut reader, None).unwrap();

        let batches: Vec<_> = file_reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_invalid_input_path() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.arrow");

        let result = stream_arrow_direct(
            "/nonexistent/file.arrow",
            &output_path,
            IpcWriteOptions::default(),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_output_directory_creation_failure() {
        if Uid::effective().is_root() {
            // Can't very well test permissions errors if we're root
            return;
        }

        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");

        write_test_arrow_file(&input_path, false).unwrap();

        let result = ensure_parent_dir_exists(Path::new("/root/no_permission")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_sort_column() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, false).unwrap();

        let sort_spec = crate::SortSpec {
            columns: vec![crate::SortColumn {
                name: "nonexistent_column".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let result = stream_arrow_with_sorting(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
            sort_spec,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_corrupted_arrow_file() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("corrupted.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        std::fs::write(&input_path, b"not an arrow file").unwrap();

        let result = convert_stream_to_file_format(
            input_path.to_str().unwrap(),
            &output_path,
            IpcWriteOptions::default(),
        )
        .await;

        assert!(result.is_err());
    }
}
