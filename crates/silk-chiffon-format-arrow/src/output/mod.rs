use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::RecordBatch,
    compute::BatchCoalescer,
    datatypes::SchemaRef,
    ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter},
};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::StreamExt;
use silk_chiffon_core::{DataSink, SinkBinding, SinkCompletion};
use silk_chiffon_storage::{
    BlockingObjectUploadWriter, ObjectUpload, ObjectUploadTask, StorageHandle,
};
use tokio::sync::mpsc;

use crate::{
    args::{Compression, TransformArgs},
    variant::IpcVariant,
};

pub(crate) struct OutputBinding {
    variant: IpcVariant,
    record_batch_size: usize,
    compression: Compression,
    queue_depth: usize,
}

impl OutputBinding {
    pub(crate) fn new(args: &TransformArgs) -> Self {
        Self {
            variant: args.arrow_format,
            record_batch_size: args.arrow_record_batch_size,
            compression: args.arrow_compression,
            queue_depth: args.arrow_writing_queue_size,
        }
    }
}

#[async_trait]
impl SinkBinding for OutputBinding {
    async fn open_sink(
        &self,
        handle: StorageHandle,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(Sink::create(
            handle,
            &schema,
            self.variant,
            self.record_batch_size,
            self.compression,
            self.queue_depth,
        )?))
    }
}

struct WriteSummary {
    rows_written: u64,
}

pub(crate) struct Sink {
    tx: Option<mpsc::Sender<RecordBatch>>,
    task: Option<ObjectUploadTask<WriteSummary>>,
}

impl Sink {
    fn create(
        handle: StorageHandle,
        schema: &SchemaRef,
        variant: IpcVariant,
        record_batch_size: usize,
        compression: Compression,
        queue_depth: usize,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<RecordBatch>(queue_depth);
        let mut upload = ObjectUpload::new(handle);
        let writer = upload.blocking_writer()?;

        let schema = Arc::clone(schema);
        let task = ObjectUploadTask::spawn("Arrow writer", upload, move |_cancellation| {
            tokio::task::spawn_blocking(move || {
                writer_task(writer, &schema, variant, record_batch_size, compression, rx)
            })
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
    variant: IpcVariant,
    record_batch_size: usize,
    compression: Compression,
    mut rx: mpsc::Receiver<RecordBatch>,
) -> Result<WriteSummary> {
    let write_options = match compression {
        Compression::Zstd | Compression::Lz4 => {
            IpcWriteOptions::default().try_with_compression(compression.into())?
        }
        Compression::None => IpcWriteOptions::default(),
    };

    let mut writer = match variant {
        IpcVariant::File => IpcWriter::File(FileWriter::try_new_with_options(
            writer,
            schema,
            write_options,
        )?),
        IpcVariant::Stream => IpcWriter::Stream(StreamWriter::try_new_with_options(
            writer,
            schema,
            write_options,
        )?),
    };

    let mut coalescer = BatchCoalescer::new(Arc::clone(schema), record_batch_size);
    let mut rows_written = 0u64;

    while let Some(batch) = rx.blocking_recv() {
        coalescer.push_batch(batch)?;

        while let Some(completed_batch) = coalescer.next_completed_batch() {
            writer.write(&completed_batch)?;
            rows_written += completed_batch.num_rows() as u64;
        }
    }

    coalescer.finish_buffered_batch()?;
    if let Some(final_batch) = coalescer.next_completed_batch() {
        writer.write(&final_batch)?;
        rows_written += final_batch.num_rows() as u64;
    }

    writer.finish()?;

    Ok(WriteSummary { rows_written })
}

#[async_trait]
impl DataSink for Sink {
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

enum IpcWriter {
    File(FileWriter<BlockingObjectUploadWriter>),
    Stream(StreamWriter<BlockingObjectUploadWriter>),
}

impl IpcWriter {
    fn write(&mut self, batch: &RecordBatch) -> arrow::error::Result<()> {
        match self {
            Self::File(writer) => writer.write(batch),
            Self::Stream(writer) => writer.write(batch),
        }
    }

    fn finish(&mut self) -> arrow::error::Result<()> {
        match self {
            Self::File(writer) => writer.finish(),
            Self::Stream(writer) => writer.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silk_chiffon_test_support::{TestBatch, TestFile, prepared_local_output};
    use tempfile::tempdir;

    async fn write(
        variant: IpcVariant,
        compression: Compression,
        record_batch_size: usize,
    ) -> (tempfile::TempDir, std::path::PathBuf, SinkCompletion) {
        let directory = tempdir().unwrap();
        let path = directory.path().join("output.arrow");
        let batch = TestBatch::simple_with(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
        let mut sink = Sink::create(
            prepared_local_output(&path),
            &batch.schema(),
            variant,
            record_batch_size,
            compression,
            2,
        )
        .unwrap();
        sink.write_batch(batch).await.unwrap();
        let completion = Box::new(sink).finish().await.unwrap();
        (directory, path, completion)
    }

    #[tokio::test]
    async fn file_and_stream_outputs_preserve_rows_and_batch_sizing() {
        let (file_directory, file_path, file_completion) =
            write(IpcVariant::File, Compression::None, 3).await;
        let file_batches = TestFile::read_arrow(&file_path);
        assert_eq!(
            file_batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [3, 2]
        );
        assert_eq!(file_completion.rows_written(), 5);

        let (stream_directory, stream_path, stream_completion) =
            write(IpcVariant::Stream, Compression::None, 3).await;
        let stream_batches = TestFile::read_arrow_stream(&stream_path);
        assert_eq!(
            stream_batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [3, 2]
        );
        assert_eq!(stream_completion.rows_written(), 5);

        drop((file_directory, stream_directory));
    }

    #[tokio::test]
    async fn both_registered_compressions_produce_readable_files() {
        for compression in [Compression::Lz4, Compression::Zstd] {
            let (_directory, path, completion) =
                write(IpcVariant::File, compression, 122_880).await;
            assert_eq!(TestFile::read_arrow(&path)[0].num_rows(), 5);
            assert_eq!(completion.rows_written(), 5);
        }
    }

    #[tokio::test]
    async fn abort_settles_an_open_writer_without_creating_a_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("aborted.arrow");
        let batch = TestBatch::simple();
        let mut sink = Sink::create(
            prepared_local_output(&path),
            &batch.schema(),
            IpcVariant::File,
            122_880,
            Compression::None,
            1,
        )
        .unwrap();
        sink.write_batch(batch).await.unwrap();

        Box::new(sink).abort().await.unwrap();

        assert!(!path.exists());
    }
}
