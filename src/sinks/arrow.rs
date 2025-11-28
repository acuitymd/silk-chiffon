use futures::stream::StreamExt;
use std::{
    collections::HashMap,
    fs::File,
    io::BufWriter,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use arrow::{
    array::{RecordBatch, RecordBatchWriter},
    compute::BatchCoalescer,
    datatypes::SchemaRef,
    error::ArrowError,
    ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter},
};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;

use crate::{
    ArrowCompression, ArrowIPCFormat,
    sinks::data_sink::{DataSink, SinkResult},
};

pub struct ArrowSinkOptions {
    format: ArrowIPCFormat,
    record_batch_size: usize,
    compression: ArrowCompression,
    metadata: HashMap<String, String>,
}

impl Default for ArrowSinkOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowSinkOptions {
    pub fn new() -> Self {
        Self {
            format: ArrowIPCFormat::default(),
            record_batch_size: 122_880,
            compression: ArrowCompression::None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_format(mut self, format: ArrowIPCFormat) -> Self {
        self.format = format;
        self
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.record_batch_size = record_batch_size;
        self
    }

    pub fn with_compression(mut self, compression: ArrowCompression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_metadata_value(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}

pub struct ArrowSinkInner {
    path: PathBuf,
    rows_written: u64,
    writer: Box<dyn ArrowRecordBatchWriter>,
    coalescer: BatchCoalescer,
}

pub struct ArrowSink {
    inner: Mutex<ArrowSinkInner>,
}

impl ArrowSink {
    pub fn create(path: PathBuf, schema: &SchemaRef, options: ArrowSinkOptions) -> Result<Self> {
        let file = BufWriter::new(File::create(&path)?);
        let write_options = match options.compression {
            ArrowCompression::Zstd | ArrowCompression::Lz4 => {
                IpcWriteOptions::default().try_with_compression(options.compression.into())?
            }
            ArrowCompression::None => IpcWriteOptions::default(),
        };

        let mut writer: Box<dyn ArrowRecordBatchWriter> = match options.format {
            ArrowIPCFormat::File => Box::new(FileWriter::try_new_with_options(
                file,
                schema,
                write_options,
            )?),
            ArrowIPCFormat::Stream => Box::new(StreamWriter::try_new_with_options(
                file,
                schema,
                write_options,
            )?),
        };

        for (key, value) in options.metadata {
            writer.write_metadata(&key, &value);
        }

        let coalescer = BatchCoalescer::new(Arc::clone(schema), options.record_batch_size);

        let inner = ArrowSinkInner {
            path,
            rows_written: 0,
            writer,
            coalescer,
        };

        Ok(Self {
            inner: Mutex::new(inner),
        })
    }
}

#[async_trait]
impl DataSink for ArrowSink {
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<SinkResult> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.write_batch(batch).await?;
        }

        self.finish().await
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow!("Failed to lock inner: {}", e))?;
        inner.coalescer.push_batch(batch)?;

        while let Some(completed_batch) = inner.coalescer.next_completed_batch() {
            inner.writer.write(&completed_batch)?;
            inner.rows_written += completed_batch.num_rows() as u64;
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow!("Failed to lock inner: {}", e))?;
        inner.coalescer.finish_buffered_batch()?;

        if let Some(final_batch) = inner.coalescer.next_completed_batch() {
            inner.writer.write(&final_batch)?;
            inner.rows_written += final_batch.num_rows() as u64;
        }

        inner.writer.finish()?;

        Ok(SinkResult {
            files_written: vec![inner.path.clone()],
            rows_written: inner.rows_written,
        })
    }
}

pub trait ArrowRecordBatchWriter: RecordBatchWriter + Send {
    fn finish(&mut self) -> Result<(), ArrowError>;
    fn write_metadata(&mut self, key: &str, value: &str);
}

impl ArrowRecordBatchWriter for FileWriter<BufWriter<File>> {
    fn finish(&mut self) -> Result<(), ArrowError> {
        self.finish()
    }

    fn write_metadata(&mut self, key: &str, value: &str) {
        self.write_metadata(key, value);
    }
}
impl ArrowRecordBatchWriter for StreamWriter<BufWriter<File>> {
    fn finish(&mut self) -> Result<(), ArrowError> {
        self.finish()
    }

    fn write_metadata(&mut self, _key: &str, _value: &str) {
        // NOOP for stream writer, they don't support metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sources::data_source::DataSource,
        utils::test_helpers::{file_helpers, test_data, verify},
    };
    use tempfile::tempdir;

    mod arrow_sink_tests {
        use super::*;

        #[tokio::test]
        async fn test_sink_writes_single_batch() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink =
                ArrowSink::create(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);
            assert_eq!(result.files_written.len(), 1);
            assert_eq!(result.files_written[0], output_path);

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }

        #[tokio::test]
        async fn test_sink_writes_multiple_batches() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[3, 4], &["c", "d"]);

            let mut sink =
                ArrowSink::create(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 4);

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 4);
        }

        #[tokio::test]
        async fn test_sink_coalesces_batches() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[3, 4], &["c", "d"]);
            let batch3 = test_data::create_batch_with_ids_and_names(&schema, &[5], &["e"]);

            let mut sink = ArrowSink::create(
                output_path.clone(),
                &schema,
                ArrowSinkOptions::new().with_record_batch_size(3),
            )
            .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            sink.write_batch(batch3).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 5);

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 2);
            assert_eq!(batches[0].num_rows(), 3);
            assert_eq!(batches[1].num_rows(), 2);
        }

        #[tokio::test]
        async fn test_sink_writes_stream_format() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrows");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = ArrowSink::create(
                output_path.clone(),
                &schema,
                ArrowSinkOptions::new().with_format(ArrowIPCFormat::Stream),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);

            let batches = verify::read_output_stream(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            verify::assert_id_name_batch_data_matches(&batches[0], &[1, 2, 3], &["a", "b", "c"]);
        }

        #[tokio::test]
        async fn test_sink_with_compression() {
            for compression in [ArrowCompression::Zstd, ArrowCompression::Lz4] {
                let temp_dir = tempdir().unwrap();
                let output_path = temp_dir.path().join("output.arrow");

                let schema = test_data::simple_schema();
                // need to give it enough data with enough repetition that we benefit from compression
                let batch = test_data::create_batch_with_ids_and_names(
                    &schema,
                    &[
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                        100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                    ],
                    &[
                        "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj",
                        "kkk", "lll", "mmm", "nnn", "ooo", "ppp", "qqq", "rrr", "sss", "ttt",
                        "uuu", "vvv", "www", "xxx", "yyy", "zzz", "AAA", "BBB", "CCC", "DDD",
                        "EEE", "FFF", "GGG", "HHH", "III", "JJJ", "KKK", "LLL", "MMM", "NNN",
                        "OOO", "PPP", "QQQ", "RRR", "SSS", "TTT", "UUU", "VVV", "WWW", "XXX",
                        "YYY", "ZZZ", "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh",
                        "iii", "jjj", "kkk", "lll", "mmm", "nnn", "ooo", "ppp", "qqq", "rrr",
                        "sss", "ttt", "uuu", "vvv", "www", "xxx", "yyy", "zzz", "AAA", "BBB",
                        "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III", "JJJ", "KKK", "LLL",
                        "MMM", "NNN", "OOO", "PPP", "QQQ", "RRR", "SSS", "TTT", "UUU", "VVV",
                    ],
                );

                let mut compressed_sink = ArrowSink::create(
                    output_path.clone(),
                    &schema,
                    ArrowSinkOptions::new().with_compression(compression),
                )
                .unwrap();

                compressed_sink.write_batch(batch.clone()).await.unwrap();
                let compressed_result = compressed_sink.finish().await.unwrap();
                let compressed_size = output_path.metadata().unwrap().len();

                let mut uncompressed_sink =
                    ArrowSink::create(output_path.clone(), &schema, ArrowSinkOptions::new())
                        .unwrap();
                uncompressed_sink.write_batch(batch).await.unwrap();
                let uncompressed_result = uncompressed_sink.finish().await.unwrap();
                let uncompressed_size = output_path.metadata().unwrap().len();

                assert_eq!(compressed_result.rows_written, 100);
                assert_eq!(uncompressed_result.rows_written, 100);
                assert!(compressed_size < uncompressed_size);

                let batches = verify::read_output_file(&output_path).unwrap();
                assert_eq!(batches.len(), 1);
            }
        }

        #[tokio::test]
        async fn test_sink_with_metadata() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);

            let mut metadata = HashMap::new();
            metadata.insert("key1".to_string(), "value1".to_string());
            metadata.insert("key2".to_string(), "value2".to_string());

            let mut sink = ArrowSink::create(
                output_path.clone(),
                &schema,
                ArrowSinkOptions::new().with_metadata(metadata.clone()),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            assert!(output_path.exists());

            let written_metadata = verify::read_file_metadata(&output_path).unwrap();

            assert_eq!(written_metadata, metadata);
        }

        #[tokio::test]
        async fn test_sink_empty_batches() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();

            let mut sink =
                ArrowSink::create(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 0);
            assert!(output_path.exists());

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 0);
        }

        #[tokio::test]
        async fn test_sink_write_stream() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch1 =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[4, 5], &["d", "e"]);

            file_helpers::write_arrow_file(&input_path, &schema, vec![batch1, batch2]).unwrap();

            let source = crate::sources::arrow::ArrowDataSource::new(
                input_path.to_str().unwrap().to_string(),
            );
            let stream = source.as_stream().await.unwrap();

            let mut sink =
                ArrowSink::create(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

            let result = sink.write_stream(stream).await.unwrap();

            assert_eq!(result.rows_written, 5);

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 5);
        }
    }

    mod options_builder_tests {
        use super::*;

        #[test]
        fn test_default_options() {
            let options = ArrowSinkOptions::default();
            assert_eq!(options.record_batch_size, 122_880);
            assert!(matches!(options.compression, ArrowCompression::None));
            assert!(options.metadata.is_empty());
        }

        #[test]
        fn test_builder_pattern() {
            let mut metadata = HashMap::new();
            metadata.insert("test".to_string(), "value".to_string());

            let options = ArrowSinkOptions::new()
                .with_format(ArrowIPCFormat::Stream)
                .with_record_batch_size(1000)
                .with_compression(ArrowCompression::Zstd)
                .with_metadata(metadata.clone());

            assert_eq!(options.format, ArrowIPCFormat::Stream);
            assert_eq!(options.record_batch_size, 1000);
            assert!(matches!(options.compression, ArrowCompression::Zstd));
            assert_eq!(options.metadata, metadata);
        }

        #[test]
        fn test_metadata_value() {
            let options = ArrowSinkOptions::new()
                .with_metadata_value("key1".to_string(), "val1".to_string())
                .with_metadata_value("key2".to_string(), "val2".to_string());

            assert_eq!(options.metadata.len(), 2);
            assert_eq!(options.metadata.get("key1").unwrap(), "val1");
            assert_eq!(options.metadata.get("key2").unwrap(), "val2");
        }

        #[test]
        fn test_metadata() {
            let mut metadata = HashMap::new();
            metadata.insert("key1".to_string(), "val1".to_string());
            metadata.insert("key2".to_string(), "val2".to_string());

            let options = ArrowSinkOptions::new().with_metadata(metadata.clone());

            assert_eq!(options.metadata, metadata);
        }
    }
}
