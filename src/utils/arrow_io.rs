use anyhow::{Result, anyhow};
use clap::ValueEnum;
use std::{
    fmt::{Display, Formatter},
    io::BufReader,
    path::{Path, PathBuf},
};

use arrow::{
    datatypes::SchemaRef,
    ipc::reader::{FileReader, StreamReader},
};
use std::fs::File;
use tempfile::NamedTempFile;

#[derive(ValueEnum, PartialEq, Clone, Debug, Default)]
pub enum ArrowIPCFormat {
    #[default]
    #[value(name = "file")]
    File,
    #[value(name = "stream")]
    Stream,
}

impl Display for ArrowIPCFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::File => "file",
            Self::Stream => "stream",
        };
        write!(f, "{s}")
    }
}

pub struct ArrowIPCReader {
    inner: ArrowIPCReaderInner,
}

pub enum ArrowIPCReaderInner {
    File { path: PathBuf },
    Stream { path: PathBuf },
}

pub enum ArrowFileSource {
    Original(PathBuf),
    Temp {
        original_path: PathBuf,
        temp_file: NamedTempFile,
    },
}

impl ArrowFileSource {
    pub fn path(&self) -> &Path {
        match self {
            ArrowFileSource::Original(path) => path,
            ArrowFileSource::Temp {
                original_path: _,
                temp_file,
            } => temp_file.path(),
        }
    }

    pub fn path_str(&self) -> &str {
        self.path().to_str().unwrap()
    }
}

pub trait HasSchema {
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

        if FileReader::try_new_buffered(File::open(path)?, None).is_ok() {
            return Ok(ArrowIPCReader {
                inner: ArrowIPCReaderInner::File {
                    path: path.to_path_buf(),
                },
            });
        }

        if StreamReader::try_new_buffered(File::open(path)?, None).is_ok() {
            return Ok(ArrowIPCReader {
                inner: ArrowIPCReaderInner::Stream {
                    path: path.to_path_buf(),
                },
            });
        }

        Err(anyhow!("Invalid arrow file found at {}", path.display()))
    }

    pub fn schema_from_path<P: AsRef<std::path::Path>>(path: P) -> Result<SchemaRef> {
        ArrowIPCReader::from_path(path)?.schema()
    }

    pub fn is_stream_format<P: AsRef<std::path::Path>>(path: P) -> bool {
        ArrowIPCReader::from_path(path).is_ok_and(|r| r.format() == ArrowIPCFormat::Stream)
    }

    pub fn is_file_format<P: AsRef<std::path::Path>>(path: P) -> bool {
        ArrowIPCReader::from_path(path).is_ok_and(|r| r.format() == ArrowIPCFormat::File)
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
}

#[cfg(test)]
mod tests {
    use crate::{
        converters::arrow::ArrowConverter,
        utils::{
            arrow_io::{ArrowFileSource, ArrowIPCFormat, ArrowIPCReader},
            test_helpers::{file_helpers, test_data, verify},
        },
    };
    use std::path::Path;
    use tempfile::tempdir;

    mod arrow_ipc_reader_tests {
        use super::*;

        #[test]
        fn test_reader_detects_file_format() {
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("file.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];
            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&file_path, &schema, vec![batch]).unwrap();

            let reader = ArrowIPCReader::from_path(&file_path).unwrap();
            match reader.format() {
                ArrowIPCFormat::File => (),
                _ => panic!("Expected file format"),
            }
        }

        #[test]
        fn test_reader_returns_path() {
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("file.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&file_path, &schema, vec![batch]).unwrap();

            let reader = ArrowIPCReader::from_path(&file_path).unwrap();
            assert_eq!(reader.path(), file_path);
        }

        #[test]
        fn test_reader_detects_stream_format() {
            let temp_dir = tempdir().unwrap();
            let stream_path = temp_dir.path().join("stream.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&stream_path, &schema, vec![batch]).unwrap();

            let reader = ArrowIPCReader::from_path(&stream_path).unwrap();
            match reader.format() {
                ArrowIPCFormat::Stream => (),
                _ => panic!("Expected stream format"),
            }
        }

        #[test]
        fn test_reader_rejects_invalid_file() {
            let temp_dir = tempdir().unwrap();
            let invalid_path = temp_dir.path().join("invalid.arrow");
            file_helpers::write_invalid_file(&invalid_path).unwrap();

            let result = ArrowIPCReader::from_path(&invalid_path);
            assert!(result.is_err());
            let err = result.err().unwrap();
            assert!(err.to_string().contains("Invalid arrow file"));
        }

        #[test]
        fn test_reader_schema_access() {
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("file.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&file_path, &schema, vec![batch]).unwrap();

            let reader = ArrowIPCReader::from_path(&file_path).unwrap();
            let read_schema = reader.schema().unwrap();
            verify::assert_schema_matches(&read_schema, &schema);
        }

        #[test]
        fn test_file_reader_creation() {
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("file.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&file_path, &schema, vec![batch]).unwrap();

            let reader = ArrowIPCReader::from_path(&file_path).unwrap();
            assert!(reader.file_reader().is_ok());
            assert!(reader.stream_reader().is_err());
        }

        #[test]
        fn test_stream_reader_creation() {
            let temp_dir = tempdir().unwrap();
            let stream_path = temp_dir.path().join("stream.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&stream_path, &schema, vec![batch]).unwrap();

            let reader = ArrowIPCReader::from_path(&stream_path).unwrap();
            assert!(reader.stream_reader().is_ok());
            assert!(reader.file_reader().is_err());
        }
    }
    mod arrow_file_source_tests {
        use super::*;

        #[tokio::test]
        async fn test_file_source_from_file_format() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), Path::new("unused"));
            let file_source = converter.as_file_format().await.unwrap();

            match file_source {
                ArrowFileSource::Original(path) => {
                    assert_eq!(path, input_path);
                }
                _ => panic!("Expected Original variant for file format input"),
            }
        }

        #[tokio::test]
        async fn test_file_source_from_stream_format() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), Path::new("unused"));
            let file_source = converter.as_file_format().await.unwrap();

            match file_source {
                ArrowFileSource::Temp {
                    original_path,
                    temp_file,
                } => {
                    assert_eq!(original_path, input_path);
                    assert!(temp_file.path().exists());

                    let reader = ArrowIPCReader::from_path(temp_file.path()).unwrap();
                    match reader.format() {
                        ArrowIPCFormat::File => (),
                        _ => panic!("Temp file should be in file format"),
                    }
                }
                _ => panic!("Expected Temp variant for stream format input"),
            }
        }

        #[tokio::test]
        async fn test_file_source_temp_cleanup() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), Path::new("unused"));
            let file_source = converter.as_file_format().await.unwrap();

            let temp_path = match &file_source {
                ArrowFileSource::Temp { temp_file, .. } => temp_file.path().to_path_buf(),
                _ => panic!("Expected Temp variant"),
            };

            assert!(temp_path.exists());
            drop(file_source); // dropping should remove the temp file
            assert!(!temp_path.exists());
        }
    }
}
