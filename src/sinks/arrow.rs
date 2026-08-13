use std::{collections::HashMap, io::Write, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::{RecordBatch, RecordBatchWriter},
    compute::BatchCoalescer,
    datatypes::SchemaRef,
    error::ArrowError,
    ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter},
};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::StreamExt;
use silk_chiffon_storage::{BlockingObjectUploadWriter, ObjectUpload, StorageHandle};
use tokio::sync::mpsc;

use crate::{
    ArrowCompression, ArrowIPCFormat,
    sinks::{
        data_sink::{DataSink, SinkCompletion},
        object_sink_task::ObjectSinkTask,
    },
    utils::memory::estimate_row_bytes,
};

#[derive(Clone)]
pub struct ArrowSinkOptions {
    format: ArrowIPCFormat,
    record_batch_size: usize,
    compression: ArrowCompression,
    metadata: HashMap<String, String>,
    queue_depth: Option<usize>,
    memory_budget: Option<usize>,
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
            queue_depth: None,
            memory_budget: None,
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

    pub fn with_queue_depth(mut self, queue_depth: usize) -> Self {
        self.queue_depth = Some(queue_depth);
        self
    }

    pub fn with_memory_budget(mut self, budget: Option<usize>) -> Self {
        self.memory_budget = budget;
        self
    }
}

struct WriterResult {
    rows_written: u64,
}

pub struct ArrowSink {
    tx: Option<mpsc::Sender<RecordBatch>>,
    task: Option<ObjectSinkTask<WriterResult>>,
}

const DEFAULT_QUEUE_DEPTH: usize = 16;

fn resolve_arrow_queue_depth(options: &ArrowSinkOptions, schema: &SchemaRef) -> usize {
    if let Some(explicit) = options.queue_depth {
        return explicit;
    }

    if let Some(budget) = options.memory_budget {
        let row_bytes = estimate_row_bytes(schema).max(1);
        let batch_bytes = options.record_batch_size.saturating_mul(row_bytes);
        let derived = budget
            .checked_div(batch_bytes)
            .unwrap_or(DEFAULT_QUEUE_DEPTH)
            .max(1);
        return derived;
    }

    DEFAULT_QUEUE_DEPTH
}

impl ArrowSink {
    pub fn create(
        handle: StorageHandle,
        schema: &SchemaRef,
        options: ArrowSinkOptions,
    ) -> Result<Self> {
        let queue_depth = resolve_arrow_queue_depth(&options, schema);
        let (tx, rx) = mpsc::channel::<RecordBatch>(queue_depth);
        let mut upload = ObjectUpload::new(handle);
        let writer = upload.blocking_writer()?;

        let schema = Arc::clone(schema);
        let task = ObjectSinkTask::spawn("Arrow writer", upload, move |_cancellation| {
            tokio::task::spawn_blocking(move || writer_task(writer, &schema, options, rx))
        });

        Ok(Self {
            tx: Some(tx),
            task: Some(task),
        })
    }
}

fn writer_task(
    writer: BlockingObjectUploadWriter,
    schema: &SchemaRef,
    options: ArrowSinkOptions,
    mut rx: mpsc::Receiver<RecordBatch>,
) -> Result<WriterResult> {
    let write_options = match options.compression {
        ArrowCompression::Zstd | ArrowCompression::Lz4 => {
            IpcWriteOptions::default().try_with_compression(options.compression.into())?
        }
        ArrowCompression::None => IpcWriteOptions::default(),
    };

    let mut writer: Box<dyn ArrowRecordBatchWriter> = match options.format {
        ArrowIPCFormat::File => Box::new(FileWriter::try_new_with_options(
            writer,
            schema,
            write_options,
        )?),
        ArrowIPCFormat::Stream => Box::new(StreamWriter::try_new_with_options(
            writer,
            schema,
            write_options,
        )?),
    };

    for (key, value) in options.metadata {
        writer.write_metadata(&key, &value);
    }

    let mut coalescer = BatchCoalescer::new(Arc::clone(schema), options.record_batch_size);
    let mut rows_written = 0u64;

    while let Some(batch) = rx.blocking_recv() {
        coalescer.push_batch(batch)?;

        while let Some(completed_batch) = coalescer.next_completed_batch() {
            writer.write(&completed_batch)?;
            rows_written += completed_batch.num_rows() as u64;
        }
    }

    // flush remaining
    coalescer.finish_buffered_batch()?;
    if let Some(final_batch) = coalescer.next_completed_batch() {
        writer.write(&final_batch)?;
        rows_written += final_batch.num_rows() as u64;
    }

    writer.finish()?;

    Ok(WriterResult { rows_written })
}

#[async_trait]
impl DataSink for ArrowSink {
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<()> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.write_batch(batch).await?;
        }

        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let tx = self.tx.as_ref().context("sink already finished")?;
        tx.send(batch).await.context("writer task died")?;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<SinkCompletion> {
        // drop sender to signal EOF
        self.tx.take();

        let (result, url) = self
            .task
            .take()
            .context("sink already finished")?
            .finish()
            .await?;

        Ok(SinkCompletion::new(url, [], result.rows_written))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.tx.take();
        match self.task.take() {
            Some(task) => task.abort().await,
            None => Ok(()),
        }
    }
}

pub trait ArrowRecordBatchWriter: RecordBatchWriter + Send {
    fn finish(&mut self) -> Result<(), ArrowError>;
    fn write_metadata(&mut self, key: &str, value: &str);
}

impl<W: Write + Send> ArrowRecordBatchWriter for FileWriter<W> {
    fn finish(&mut self) -> Result<(), ArrowError> {
        self.finish()
    }

    fn write_metadata(&mut self, key: &str, value: &str) {
        self.write_metadata(key, value);
    }
}
impl<W: Write + Send> ArrowRecordBatchWriter for StreamWriter<W> {
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
    use crate::utils::test_helpers::{test_data, verify};
    use silk_chiffon_storage::{ExistingOutput, LocationInput, OutputPreparation};
    use tempfile::tempdir;

    fn prepared_output(path: &std::path::Path) -> StorageHandle {
        let path = path.to_path_buf();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    silk_chiffon_storage::local::session()
                        .unwrap()
                        .prepare_output_target(
                            &LocationInput::parse(path.to_str().unwrap()).unwrap(),
                            &OutputPreparation::new(ExistingOutput::Allow, false),
                        )
                        .await
                        .unwrap()
                })
        })
        .join()
        .unwrap()
    }

    mod arrow_sink_tests {
        use super::*;

        #[tokio::test]
        async fn test_sink_writes_single_batch() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = ArrowSink::create(
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 3);
            assert_eq!(result.durable_locations().len(), 1);
            assert_eq!(
                result.durable_locations()[0],
                url::Url::from_file_path(&output_path).unwrap()
            );

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

            let mut sink = ArrowSink::create(
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new(),
            )
            .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 4);

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
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new().with_record_batch_size(3),
            )
            .unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            sink.write_batch(batch3).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 5);

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
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new().with_format(ArrowIPCFormat::Stream),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 3);

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
                    prepared_output(&output_path),
                    &schema,
                    ArrowSinkOptions::new().with_compression(compression),
                )
                .unwrap();

                compressed_sink.write_batch(batch.clone()).await.unwrap();
                let compressed_result = Box::new(compressed_sink).finish().await.unwrap();
                let compressed_size = output_path.metadata().unwrap().len();

                let mut uncompressed_sink = ArrowSink::create(
                    prepared_output(&output_path),
                    &schema,
                    ArrowSinkOptions::new(),
                )
                .unwrap();
                uncompressed_sink.write_batch(batch).await.unwrap();
                let uncompressed_result = Box::new(uncompressed_sink).finish().await.unwrap();
                let uncompressed_size = output_path.metadata().unwrap().len();

                assert_eq!(compressed_result.rows_written(), 100);
                assert_eq!(uncompressed_result.rows_written(), 100);
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
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new().with_metadata(metadata.clone()),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            assert!(output_path.exists());

            let written_metadata = verify::read_file_metadata(&output_path).unwrap();

            assert_eq!(written_metadata, metadata);
        }

        #[tokio::test]
        async fn test_default_file_metadata_is_empty() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink = ArrowSink::create(
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new(),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            Box::new(sink).finish().await.unwrap();

            let file = std::fs::File::open(&output_path).unwrap();
            let reader = arrow::ipc::reader::FileReader::try_new_buffered(file, None).unwrap();

            assert!(reader.custom_metadata().is_empty());
            assert!(reader.schema().metadata().is_empty());
        }

        #[tokio::test]
        async fn test_sink_empty_batches() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();

            let sink = ArrowSink::create(
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new(),
            )
            .unwrap();

            let result = Box::new(sink).finish().await.unwrap();

            assert_eq!(result.rows_written(), 0);
            assert!(output_path.exists());

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 0);
        }

        #[tokio::test]
        async fn test_sink_write_stream() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch1 =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[4, 5], &["d", "e"]);

            let ctx = datafusion::prelude::SessionContext::new();
            let provider = Arc::new(
                datafusion::datasource::MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![batch1, batch2]],
                )
                .unwrap(),
            );
            let stream = ctx
                .read_table(provider)
                .unwrap()
                .execute_stream()
                .await
                .unwrap();

            let mut sink = ArrowSink::create(
                prepared_output(&output_path),
                &schema,
                ArrowSinkOptions::new(),
            )
            .unwrap();

            sink.write_stream(stream).await.unwrap();
            let result = Box::new(sink).finish().await.unwrap();
            assert_eq!(result.rows_written(), 5);

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 5);
        }
    }

    mod options_builder_tests {
        use super::*;
        use arrow::datatypes::{DataType, Field, Schema};

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

        #[test]
        fn test_no_budget_uses_default_queue_depth() {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
            let options = ArrowSinkOptions::new();
            assert_eq!(resolve_arrow_queue_depth(&options, &schema), 16);
        }

        #[test]
        fn test_explicit_queue_depth_wins() {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
            let options = ArrowSinkOptions::new()
                .with_queue_depth(42)
                .with_memory_budget(Some(1024 * 1024 * 1024));
            assert_eq!(resolve_arrow_queue_depth(&options, &schema), 42);
        }

        #[test]
        fn test_budget_derives_queue_depth() {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
            // 4 bytes/row, batch size = 122880 rows → ~491520 bytes/batch
            // budget 10MB = 10485760 → 10485760 / 491520 = 21
            let options = ArrowSinkOptions::new().with_memory_budget(Some(10 * 1024 * 1024));
            let depth = resolve_arrow_queue_depth(&options, &schema);
            assert_eq!(depth, 21);
        }

        #[test]
        fn test_tiny_budget_clamps_to_one() {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
            let options = ArrowSinkOptions::new().with_memory_budget(Some(1));
            assert_eq!(resolve_arrow_queue_depth(&options, &schema), 1);
        }
    }
}
