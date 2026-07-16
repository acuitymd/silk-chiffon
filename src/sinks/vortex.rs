//! Vortex sink backed by a bounded object-store uploader.
//!
//! Vortex encoding writes buffers into a depth-one channel. A separate async
//! task owns `ObjectOutput`, uploads those buffers, and commits only after the
//! Vortex footer is complete.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;

use crate::{
    sinks::{
        data_sink::{DataSink, SinkResult},
        object_writer::{VortexObjectWriter, join_object_upload, spawn_object_upload},
    },
    storage::ObjectOutput,
};

#[derive(Clone, Copy)]
pub struct VortexSinkOptions {
    record_batch_size: usize,
}

impl VortexSinkOptions {
    pub fn new() -> Self {
        Self {
            record_batch_size: 122_880,
        }
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.record_batch_size = record_batch_size;
        self
    }
}

impl Default for VortexSinkOptions {
    fn default() -> Self {
        Self::new()
    }
}

struct VortexSinkInner {
    rows_written: u64,
    coalescer: BatchCoalescer,
    sender: Option<mpsc::Sender<ArrayRef>>,
}

impl VortexSinkInner {
    async fn flush_completed_batches(&mut self) -> Result<()> {
        while let Some(completed_batch) = self.coalescer.next_completed_batch() {
            let vortex_array = ArrayRef::from_arrow(completed_batch.clone(), false)?;

            self.sender
                .as_ref()
                .ok_or_else(|| anyhow!("sender already closed"))?
                .send(vortex_array)
                .await?;

            self.rows_written += completed_batch.num_rows() as u64;
        }

        Ok(())
    }

    fn finish_buffered_batch(&mut self) -> Result<()> {
        self.coalescer
            .finish_buffered_batch()
            .map_err(|e| anyhow!("failed to finish buffered batch: {e}"))
    }

    fn drop_sender(&mut self) {
        self.sender.take();
    }
}

pub struct VortexSink {
    inner: Mutex<VortexSinkInner>,
    writer_task: Option<tokio::task::JoinHandle<Result<ObjectOutput>>>,
}

impl VortexSink {
    pub fn create(
        output: ObjectOutput,
        schema: &SchemaRef,
        options: VortexSinkOptions,
    ) -> Result<Self> {
        let coalescer = BatchCoalescer::new(Arc::clone(schema), options.record_batch_size);
        let (sender, receiver) = mpsc::channel(16);

        let schema_clone = Arc::clone(schema);

        // Vortex's push writer is not Send, so the writer future stays inside
        // one task and receives arrays through a channel.
        let writer_task =
            tokio::spawn(
                async move { Self::write_vortex_file(output, schema_clone, receiver).await },
            );

        let inner = VortexSinkInner {
            rows_written: 0,
            coalescer,
            sender: Some(sender),
        };

        Ok(Self {
            inner: Mutex::new(inner),
            writer_task: Some(writer_task),
        })
    }

    async fn write_vortex_file(
        output: ObjectOutput,
        schema: SchemaRef,
        mut receiver: mpsc::Receiver<ArrayRef>,
    ) -> Result<ObjectOutput> {
        let part_size = output.part_size();
        let (upload_sender, upload_handle) = spawn_object_upload(output);
        let writer = VortexObjectWriter::new(upload_sender, part_size);

        let session = VortexSession::default();

        let dtype = DType::from_arrow(schema);

        let array_stream = ArrayStreamAdapter::new(
            dtype.clone(),
            stream::poll_fn(move |cx| receiver.poll_recv(cx).map(|opt| opt.map(Ok))),
        );

        let write_result = session
            .write_options()
            .write(writer, array_stream)
            .await
            .map_err(|e| anyhow::anyhow!("failed to write vortex file: {e}"));
        let upload_result = join_object_upload(upload_handle).await;

        if let Err(primary) = write_result {
            match upload_result {
                Ok(mut output) => {
                    if let Err(cleanup) = output.abort().await {
                        return Err(
                            primary.context(format!("output cleanup also failed: {cleanup:#}"))
                        );
                    }
                    return Err(primary);
                }
                Err(upload) => {
                    return Err(upload.context(format!("Vortex writer also failed: {primary:#}")));
                }
            }
        }

        upload_result
    }
}

#[async_trait]
impl DataSink for VortexSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.coalescer.push_batch(batch)?;
        inner.flush_completed_batches().await?;

        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        let mut inner = self.inner.lock().await;
        inner.finish_buffered_batch()?;
        inner.flush_completed_batches().await?;

        inner.drop_sender();

        let mut output = self
            .writer_task
            .take()
            .ok_or_else(|| anyhow!("writer task already finished"))?
            .await
            .map_err(|e| anyhow!("error joining writer task: {e}"))?
            .map_err(|e| anyhow!("writer task errored: {e}"))?;
        let destination = output.commit().await?;

        Ok(SinkResult {
            files_written: vec![destination],
            rows_written: inner.rows_written,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
    use std::sync::atomic::Ordering;

    use crate::{
        sources::data_source::DataSource,
        storage::{InputObject, ObjectLocation, StoreHandle, StoreKind},
    };

    #[tokio::test]
    async fn sink_round_trips_and_coalesces_through_memory_store() {
        let memory = InMemory::new();
        let (counting, requests) =
            crate::utils::test_helpers::object_store::CountingStore::new(memory.clone());
        let store: Arc<dyn ObjectStore> = Arc::new(counting);
        let output =
            crate::sinks::object_writer::memory_output(Arc::clone(&store), "output.vortex", 64)
                .await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![3, 4, 5])),
                Arc::new(StringArray::from(vec!["c", "d", "e"])),
            ],
        )
        .unwrap();
        let mut sink = VortexSink::create(
            output,
            &schema,
            VortexSinkOptions::new().with_record_batch_size(3),
        )
        .unwrap();

        sink.write_batch(first).await.unwrap();
        sink.write_batch(second).await.unwrap();
        assert!(memory.head(&Path::from("output.vortex")).await.is_err());
        let result = sink.finish().await.unwrap();

        let meta = memory.head(&Path::from("output.vortex")).await.unwrap();
        let handle = Arc::new(StoreHandle::new(
            StoreKind::Gcs {
                bucket: "memory".to_string(),
            },
            ObjectStoreUrl::parse("gs://memory").unwrap(),
            store,
        ));
        let location = ObjectLocation::new(
            "gs://memory/output.vortex".to_string(),
            "gs://memory/output.vortex".to_string(),
            Path::from("output.vortex"),
            handle,
        );
        let source =
            crate::sources::vortex::VortexDataSource::new(InputObject::new(location, meta));
        let batches = source
            .as_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(result.files_written, vec!["gs://memory/output.vortex"]);
        assert_eq!(result.rows_written, 5);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert_eq!(requests.multiparts.load(Ordering::SeqCst), 1);
        assert!(requests.parts.load(Ordering::SeqCst) > 1);
    }

    #[tokio::test]
    async fn dropping_sink_does_not_commit_a_partial_vortex_file() {
        let memory = InMemory::new();
        let store: Arc<dyn ObjectStore> = Arc::new(memory.clone());
        let output = crate::sinks::object_writer::memory_output(store, "partial.vortex", 64).await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut sink = VortexSink::create(
            output,
            &schema,
            VortexSinkOptions::new().with_record_batch_size(1),
        )
        .unwrap();
        sink.write_batch(batch).await.unwrap();

        drop(sink);
        for _ in 0..100 {
            if memory
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        assert!(memory.head(&Path::from("partial.vortex")).await.is_err());
        assert!(
            memory
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }
}
