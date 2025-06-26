use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    error::ArrowError,
    ipc::{
        CompressionType,
        writer::{FileWriter, IpcWriteOptions},
    },
};
use futures::stream::StreamExt;
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::NamedTempFile;

use crate::{
    ArrowArgs, ArrowCompression, SortDirection, SortSpec,
    utils::filesystem::ensure_parent_dir_exists,
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

    let converter = ArrowConverter::new(input_path, output_path)
        .with_compression(args.compression)
        .with_sorting(args.sort_by.unwrap_or_default());

    converter.convert().await
}

pub struct ArrowConverter {
    input_path: String,
    output_path: PathBuf,
    write_options: IpcWriteOptions,
    sort_spec: SortSpec,
}

impl ArrowConverter {
    pub fn new(input_path: &str, output_path: &Path) -> Self {
        let output_path = output_path.to_path_buf();
        let write_options = IpcWriteOptions::default();
        Self {
            input_path: input_path.to_string(),
            output_path,
            write_options,
            sort_spec: SortSpec::default(),
        }
    }

    pub fn with_compression(mut self, compression: ArrowCompression) -> Self {
        let compression_type = match compression {
            ArrowCompression::Zstd => Some(CompressionType::ZSTD),
            ArrowCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
            ArrowCompression::None => None,
        };
        self.write_options = self
            .write_options
            .try_with_compression(compression_type)
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to set compression to {:?} (compression: {:?}) -- feature not enabled",
                    compression_type, compression
                );
            });
        self
    }

    pub fn with_sorting(mut self, sort_spec: SortSpec) -> Self {
        self.sort_spec = sort_spec;
        self
    }

    pub async fn convert(&self) -> Result<()> {
        if self.sort_spec.columns.is_empty() {
            self.convert_direct().await
        } else {
            self.convert_with_sorting().await
        }
    }

    async fn convert_with_sorting(&self) -> Result<()> {
        let mut config = SessionConfig::new();
        let options = config.options_mut();
        options.execution.batch_size = 122_880;

        let runtime_config = RuntimeEnvBuilder::new();
        let runtime_env = runtime_config.build()?;
        let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

        let output_path = self.output_path.clone();
        let write_options = self.write_options.clone();
        let sort_spec = self.sort_spec.clone();

        let file_format = self.as_file_format().await?;

        tokio::task::spawn(async move {
            ctx.register_arrow(
                "input_table",
                file_format.path_str(),
                ArrowReadOptions::default(),
            )
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

            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    async fn convert_direct(&self) -> Result<()> {
        let input = ArrowIPCReader::from_path(&self.input_path)?;
        match input.format() {
            ArrowIPCFormat::File => {
                let file_reader = input.file_reader()?;
                ArrowConverter::convert_arrow_reader_to_file_format(
                    file_reader,
                    &self.output_path,
                    self.write_options.clone(),
                )
                .await
            }
            ArrowIPCFormat::Stream => {
                let stream_reader = input.stream_reader()?;
                ArrowConverter::convert_arrow_reader_to_file_format(
                    stream_reader,
                    &self.output_path,
                    self.write_options.clone(),
                )
                .await
            }
        }
    }

    async fn convert_arrow_reader_to_file_format<I>(
        reader: I,
        output_path: &Path,
        write_options: IpcWriteOptions,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch, ArrowError>> + HasSchema + Send + 'static,
    {
        let output_path = output_path.to_path_buf();

        tokio::task::spawn(async move {
            let schema = reader.schema();
            let output_file = File::create(output_path)?;
            let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

            for batch_result in reader {
                writer.write(&batch_result?)?;
            }
            writer.finish()?;

            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    async fn as_file_format(&self) -> Result<ArrowIPCFileInFileFormat> {
        let input = ArrowIPCReader::from_path(&self.input_path)?;
        match input.format() {
            ArrowIPCFormat::File => Ok(ArrowIPCFileInFileFormat::Original(
                input.path().to_path_buf(),
            )),
            ArrowIPCFormat::Stream => {
                let temp_file = NamedTempFile::new()?;
                ArrowConverter::convert_arrow_reader_to_file_format(
                    input.stream_reader()?,
                    temp_file.path(),
                    IpcWriteOptions::default(),
                )
                .await?;

                Ok(ArrowIPCFileInFileFormat::Temp {
                    original_path: input.path().to_path_buf(),
                    temp_file,
                })
            }
        }
    }
}

pub enum ArrowIPCFormat {
    File,
    Stream,
}

pub struct ArrowIPCReader {
    inner: ArrowIPCReaderInner,
}

pub enum ArrowIPCReaderInner {
    File { path: PathBuf },
    Stream { path: PathBuf },
}

pub enum ArrowIPCFileInFileFormat {
    Original(PathBuf),
    Temp {
        original_path: PathBuf,
        temp_file: NamedTempFile,
    },
}

impl ArrowIPCFileInFileFormat {
    pub fn path(&self) -> &Path {
        match self {
            ArrowIPCFileInFileFormat::Original(path) => path,
            ArrowIPCFileInFileFormat::Temp {
                original_path: _,
                temp_file,
            } => temp_file.path(),
        }
    }

    pub fn path_str(&self) -> &str {
        self.path().to_str().unwrap()
    }
}

trait HasSchema {
    fn schema(&self) -> SchemaRef;
}

impl HasSchema for FileReader<BufReader<File>> {
    fn schema(&self) -> SchemaRef {
        self.schema()
    }
}

impl HasSchema for StreamReader<BufReader<File>> {
    fn schema(&self) -> SchemaRef {
        self.schema()
    }
}

impl ArrowIPCReader {
    pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        if let Ok(_) = FileReader::try_new_buffered(File::open(path)?, None) {
            return Ok(ArrowIPCReader {
                inner: ArrowIPCReaderInner::File {
                    path: path.to_path_buf(),
                },
            });
        }

        if let Ok(_) = StreamReader::try_new_buffered(File::open(path)?, None) {
            return Ok(ArrowIPCReader {
                inner: ArrowIPCReaderInner::Stream {
                    path: path.to_path_buf(),
                },
            });
        }

        Err(anyhow!("Invalid arrow file found at {}", path.display()))
    }

    pub fn path(&self) -> &std::path::Path {
        match &self.inner {
            ArrowIPCReaderInner::File { path } => path,
            ArrowIPCReaderInner::Stream { path } => path,
        }
    }

    pub fn format(&self) -> ArrowIPCFormat {
        match &self.inner {
            ArrowIPCReaderInner::File { .. } => ArrowIPCFormat::File,
            ArrowIPCReaderInner::Stream { .. } => ArrowIPCFormat::Stream,
        }
    }

    pub fn file_reader(&self) -> Result<FileReader<BufReader<File>>> {
        match &self.inner {
            ArrowIPCReaderInner::File { path } => {
                let file = File::open(path)?;
                Ok(FileReader::try_new_buffered(file, None)?)
            }
            ArrowIPCReaderInner::Stream { .. } => {
                Err(anyhow!("Cannot create FileReader for stream format"))
            }
        }
    }

    pub fn stream_reader(&self) -> Result<StreamReader<BufReader<File>>> {
        match &self.inner {
            ArrowIPCReaderInner::Stream { path } => {
                let file = File::open(path)?;
                Ok(StreamReader::try_new_buffered(file, None)?)
            }
            ArrowIPCReaderInner::File { .. } => {
                Err(anyhow!("Cannot create StreamReader for file format"))
            }
        }
    }

    pub fn schema(&self) -> Result<SchemaRef> {
        match &self.inner {
            ArrowIPCReaderInner::File { path } => {
                let file = File::open(path)?;
                let reader = FileReader::try_new_buffered(file, None)?;
                Ok(reader.schema())
            }
            ArrowIPCReaderInner::Stream { path } => {
                let file = File::open(path)?;
                let reader = StreamReader::try_new_buffered(file, None)?;
                Ok(reader.schema())
            }
        }
    }

    pub async fn as_file_format(&self) -> Result<ArrowIPCFileInFileFormat> {
        match &self.inner {
            ArrowIPCReaderInner::File { path } => {
                Ok(ArrowIPCFileInFileFormat::Original(path.clone()))
            }
            ArrowIPCReaderInner::Stream { path } => {
                let temp_file = NamedTempFile::new()?;
                let converter = ArrowConverter::new(path.to_str().unwrap(), temp_file.path());
                converter.convert().await?;
                Ok(ArrowIPCFileInFileFormat::Temp {
                    original_path: path.clone(),
                    temp_file,
                })
            }
        }
    }
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

        ArrowConverter::convert_arrow_reader_to_file_format(
            StreamReader::try_new_buffered(File::open(&input_path).unwrap(), None).unwrap(),
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

        let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path);
        converter.convert().await.unwrap();

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

        let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_stream_arrow_direct_stream_format() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        write_test_arrow_file(&input_path, true).unwrap();

        let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path);
        converter.convert().await.unwrap();

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

        let converter =
            ArrowConverter::new(input_path.to_str().unwrap(), &output_path).with_sorting(sort_spec);

        converter.convert().await.unwrap();

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

        let converter =
            ArrowConverter::new(input_path.to_str().unwrap(), &output_path).with_sorting(sort_spec);

        converter.convert().await.unwrap();

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

        let converter =
            ArrowConverter::new(input_path.to_str().unwrap(), &output_path).with_sorting(sort_spec);

        converter.convert().await.unwrap();

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

        let converter =
            ArrowConverter::new(input_path.to_str().unwrap(), &output_path).with_sorting(sort_spec);

        converter.convert().await.unwrap();

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

        let converter =
            ArrowConverter::new(input_path.to_str().unwrap(), &output_path).with_sorting(sort_spec);

        converter.convert().await.unwrap();

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

        ArrowConverter::convert_arrow_reader_to_file_format(
            StreamReader::try_new_buffered(File::open(&input_path).unwrap(), None).unwrap(),
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

        ArrowConverter::convert_arrow_reader_to_file_format(
            StreamReader::try_new_buffered(File::open(&input_path).unwrap(), None).unwrap(),
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

        let converter = ArrowConverter::new("/nonexistent/file.arrow", &output_path);
        let result = converter.convert().await;

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

        let converter =
            ArrowConverter::new(input_path.to_str().unwrap(), &output_path).with_sorting(sort_spec);

        let result = converter.convert().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_corrupted_arrow_file() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("corrupted.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        std::fs::write(&input_path, b"not an arrow file").unwrap();

        let result = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
            .convert()
            .await;

        assert!(result.is_err());
    }
}
