//! Arrow IPC file and stream sinks backed by object storage.
//!
//! A blocking task owns the Arrow writer and sends part-sized byte chunks to a
//! depth-one channel after each complete record batch. The async uploader owns
//! `ObjectOutput`. It commits only after the Arrow writer emits its footer or
//! stream terminator. Finish joins and aborts that uploader even when the writer
//! task panics.

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
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    ArrowCompression, ArrowIPCFormat,
    sinks::{
        data_sink::{DataSink, SinkResult},
        object_writer::{DrainWriter, UploadHandle, join_object_upload, spawn_object_upload},
    },
    storage::ObjectOutput,
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
    handle: Option<JoinHandle<Result<WriterResult>>>,
    upload_handle: Option<UploadHandle>,
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
        output: ObjectOutput,
        schema: &SchemaRef,
        options: ArrowSinkOptions,
    ) -> Result<Self> {
        let queue_depth = resolve_arrow_queue_depth(&options, schema);
        let (tx, rx) = mpsc::channel::<RecordBatch>(queue_depth);
        let part_size = output.part_size();
        let (upload_sender, upload_handle) = spawn_object_upload(output);

        let schema = Arc::clone(schema);
        let handle = tokio::task::spawn_blocking(move || {
            writer_task(
                DrainWriter::new(upload_sender, part_size),
                &schema,
                options,
                rx,
            )
        });

        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
            upload_handle: Some(upload_handle),
        })
    }
}

fn writer_task(
    drain: DrainWriter,
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
            drain,
            schema,
            write_options,
        )?),
        ArrowIPCFormat::Stream => Box::new(StreamWriter::try_new_with_options(
            drain,
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
            writer.drain()?;
            rows_written += completed_batch.num_rows() as u64;
        }
    }

    coalescer.finish_buffered_batch()?;
    if let Some(final_batch) = coalescer.next_completed_batch() {
        writer.write(&final_batch)?;
        writer.drain()?;
        rows_written += final_batch.num_rows() as u64;
    }

    writer.finish()?;

    Ok(WriterResult { rows_written })
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
        let tx = self.tx.as_ref().context("sink already finished")?;
        tx.send(batch).await.context("writer task died")?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        // drop sender to signal EOF
        self.tx.take();

        let handle = self.handle.take().context("sink already finished")?;
        let upload_handle = self
            .upload_handle
            .take()
            .context("sink uploader already finished")?;
        let writer_result = handle.await;
        let upload_result = join_object_upload(upload_handle).await;

        let result = match writer_result {
            Ok(Ok(result)) => result,
            Ok(Err(primary)) => return abort_failed_writer(primary, upload_result).await,
            Err(error) => {
                let primary = anyhow::Error::new(error).context("writer task panicked");
                return abort_failed_writer(primary, upload_result).await;
            }
        };
        let mut output = upload_result?;
        let destination = output.commit().await?;

        Ok(SinkResult {
            files_written: vec![destination],
            rows_written: result.rows_written,
        })
    }
}

async fn abort_failed_writer(
    primary: anyhow::Error,
    upload_result: Result<ObjectOutput>,
) -> Result<SinkResult> {
    match upload_result {
        Ok(mut output) => {
            if let Err(cleanup) = output.abort().await {
                return Err(primary.context(format!("output cleanup also failed: {cleanup:#}")));
            }
            Err(primary)
        }
        Err(upload) => Err(upload.context(format!("encoder also failed: {primary:#}"))),
    }
}

pub trait ArrowRecordBatchWriter: RecordBatchWriter + Send {
    fn finish(&mut self) -> Result<(), ArrowError>;
    fn write_metadata(&mut self, key: &str, value: &str);
    fn drain(&mut self) -> std::io::Result<()>;
}

impl ArrowRecordBatchWriter for FileWriter<DrainWriter> {
    fn finish(&mut self) -> Result<(), ArrowError> {
        self.finish()
    }

    fn write_metadata(&mut self, key: &str, value: &str) {
        self.write_metadata(key, value);
    }

    fn drain(&mut self) -> std::io::Result<()> {
        self.get_mut().flush()
    }
}
impl ArrowRecordBatchWriter for StreamWriter<DrainWriter> {
    fn finish(&mut self) -> Result<(), ArrowError> {
        self.finish()
    }

    fn write_metadata(&mut self, _key: &str, _value: &str) {
        // NOOP for stream writer, they don't support metadata
    }

    fn drain(&mut self) -> std::io::Result<()> {
        self.get_mut().flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sources::data_source::DataSource,
        storage::{OutputPolicy, StorageConfig, StorageContext},
        utils::test_helpers::{file_helpers, test_data, verify},
    };
    use tempfile::tempdir;

    macro_rules! test_arrow_sink {
        ($path:expr, $schema:expr, $options:expr $(,)?) => {{
            let storage = StorageContext::new(StorageConfig::default()).unwrap();
            let output = storage
                .create_output(
                    $path.to_string_lossy().as_ref(),
                    OutputPolicy::new(true, true),
                )
                .await
                .unwrap();
            ArrowSink::create(output, $schema, $options)
        }};
    }

    async fn arrow_source(path: &std::path::Path) -> crate::sources::arrow::ArrowDataSource {
        let storage = crate::storage::StorageContext::new(Default::default()).unwrap();
        let input = storage
            .resolve_input(path.to_string_lossy().as_ref())
            .await
            .unwrap();
        crate::sources::arrow::ArrowDataSource::new(input)
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

            let mut sink =
                test_arrow_sink!(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

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
                test_arrow_sink!(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

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

            let mut sink = test_arrow_sink!(
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

            let mut sink = test_arrow_sink!(
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

                let mut compressed_sink = test_arrow_sink!(
                    output_path.clone(),
                    &schema,
                    ArrowSinkOptions::new().with_compression(compression),
                )
                .unwrap();

                compressed_sink.write_batch(batch.clone()).await.unwrap();
                let compressed_result = compressed_sink.finish().await.unwrap();
                let compressed_size = output_path.metadata().unwrap().len();

                let mut uncompressed_sink =
                    test_arrow_sink!(output_path.clone(), &schema, ArrowSinkOptions::new())
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

            let mut sink = test_arrow_sink!(
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
        async fn test_default_file_metadata_is_empty() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink =
                test_arrow_sink!(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

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

            let mut sink =
                test_arrow_sink!(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

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

            let source = arrow_source(&input_path).await;
            let stream = source.as_stream().await.unwrap();

            let mut sink =
                test_arrow_sink!(output_path.clone(), &schema, ArrowSinkOptions::new()).unwrap();

            let result = sink.write_stream(stream).await.unwrap();

            assert_eq!(result.rows_written, 5);

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 5);
        }

        #[tokio::test]
        async fn test_sink_round_trips_through_memory_store() {
            use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
            use std::sync::atomic::Ordering;

            let memory = InMemory::new();
            let (counting, requests) =
                crate::utils::test_helpers::object_store::CountingStore::new(memory.clone());
            let store: Arc<dyn ObjectStore> = Arc::new(counting);
            let output =
                crate::sinks::object_writer::memory_output(store, "output.arrow", 64).await;
            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);
            let mut sink = ArrowSink::create(output, &schema, ArrowSinkOptions::new()).unwrap();

            sink.write_batch(batch).await.unwrap();
            assert!(memory.head(&Path::from("output.arrow")).await.is_err());
            let result = sink.finish().await.unwrap();

            let bytes = memory
                .get(&Path::from("output.arrow"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let reader =
                arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(result.files_written, vec!["gs://memory/output.arrow"]);
            assert_eq!(batches[0].num_rows(), 3);
            assert_eq!(requests.multiparts.load(Ordering::SeqCst), 1);
            assert!(requests.parts.load(Ordering::SeqCst) > 1);
        }

        #[tokio::test]
        async fn test_encoder_failure_aborts_object_output() {
            use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

            let memory = InMemory::new();
            let store: Arc<dyn ObjectStore> = Arc::new(memory.clone());
            let output =
                crate::sinks::object_writer::memory_output(store, "broken.arrow", 64).await;
            let schema = test_data::simple_schema();
            let wrong_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("wrong", arrow::datatypes::DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                wrong_schema,
                vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let mut sink = ArrowSink::create(output, &schema, ArrowSinkOptions::new()).unwrap();

            sink.write_batch(batch).await.unwrap();
            let error = sink.finish().await.unwrap_err();

            assert!(!error.to_string().is_empty());
            assert!(memory.head(&Path::from("broken.arrow")).await.is_err());
        }

        #[tokio::test]
        async fn test_uploader_failure_stops_encoder_and_aborts_upload() {
            use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
            use std::sync::atomic::Ordering;

            let memory = InMemory::new();
            let (counting, requests) =
                crate::utils::test_helpers::object_store::CountingStore::new(memory.clone());
            requests.fail_parts.store(true, Ordering::SeqCst);
            let store: Arc<dyn ObjectStore> = Arc::new(counting);
            let output =
                crate::sinks::object_writer::memory_output(store, "failed.arrow", 64).await;
            let schema = test_data::simple_schema();
            let ids = (0..1_000).collect::<Vec<_>>();
            let names = (0..1_000).map(|_| "value").collect::<Vec<_>>();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);
            let mut sink = ArrowSink::create(output, &schema, ArrowSinkOptions::new()).unwrap();

            sink.write_batch(batch).await.unwrap();
            let error = sink.finish().await.unwrap_err();

            assert!(format!("{error:#}").contains("injected upload failure"));
            assert!(memory.head(&Path::from("failed.arrow")).await.is_err());
            assert_eq!(requests.aborts.load(Ordering::SeqCst), 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn dropping_sink_aborts_a_started_multipart_upload() {
            use futures::TryStreamExt;
            use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
            use std::sync::atomic::Ordering;

            let memory = InMemory::new();
            let (counting, requests) =
                crate::utils::test_helpers::object_store::CountingStore::new(memory.clone());
            let store: Arc<dyn ObjectStore> = Arc::new(counting);
            let output =
                crate::sinks::object_writer::memory_output(store, "dropped.arrow", 64).await;
            let schema = test_data::simple_schema();
            let ids = (0..10_000).collect::<Vec<_>>();
            let names = (0..10_000).map(|_| "value").collect::<Vec<_>>();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);
            let mut sink = ArrowSink::create(
                output,
                &schema,
                ArrowSinkOptions::new().with_record_batch_size(64),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            crate::utils::test_helpers::object_store::wait_for_count(&requests.multiparts, 1).await;
            drop(sink);
            crate::utils::test_helpers::object_store::wait_for_count(&requests.aborts, 1).await;

            assert!(memory.head(&Path::from("dropped.arrow")).await.is_err());
            assert!(
                memory
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(requests.aborts.load(Ordering::SeqCst), 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn writer_panic_explicitly_cleans_the_started_upload() {
            use futures::TryStreamExt;
            use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
            use std::sync::atomic::Ordering;

            let memory = InMemory::new();
            let (counting, requests) =
                crate::utils::test_helpers::object_store::CountingStore::new(memory.clone());
            let store: Arc<dyn ObjectStore> = Arc::new(counting);
            let output =
                crate::sinks::object_writer::memory_output(store, "panicked.arrow", 64).await;
            let schema = test_data::simple_schema();
            let ids = (0..10_000).collect::<Vec<_>>();
            let names = (0..10_000).map(|_| "value").collect::<Vec<_>>();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &ids, &names);
            let mut sink = ArrowSink::create(
                output,
                &schema,
                ArrowSinkOptions::new().with_record_batch_size(64),
            )
            .unwrap();

            sink.write_batch(batch).await.unwrap();
            crate::utils::test_helpers::object_store::wait_for_count(&requests.multiparts, 1).await;
            let detached_writer = sink
                .handle
                .replace(tokio::task::spawn_blocking(|| {
                    panic!("injected writer panic")
                }))
                .unwrap();
            drop(detached_writer);

            let error = sink.finish().await.unwrap_err();
            crate::utils::test_helpers::object_store::wait_for_count(&requests.aborts, 1).await;

            assert!(format!("{error:#}").contains("writer task panicked"));
            assert!(sink.upload_handle.is_none());
            assert!(memory.head(&Path::from("panicked.arrow")).await.is_err());
            assert!(
                memory
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(requests.aborts.load(Ordering::SeqCst), 1);
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
            // 4 bytes/row, batch size = 122880 rows, or about 491520 bytes/batch
            // budget 10MB = 10485760, so 10485760 / 491520 = 21
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
