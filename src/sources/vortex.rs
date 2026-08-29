//! Vortex metadata and scans backed by `ObjectStore`.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use tokio::sync::OnceCell;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::{VortexSessionDefault, file::OpenOptionsSessionExt};
use vortex_datafusion::v2::VortexTable;

use crate::{
    sources::data_source::{DataSource, SourceMetadata},
    storage::InputObject,
};

pub struct VortexDataSource {
    input: InputObject,
    metadata: OnceCell<SourceMetadata>,
}

impl VortexDataSource {
    pub fn new(input: InputObject) -> Self {
        Self {
            input,
            metadata: OnceCell::new(),
        }
    }

    async fn metadata(&self) -> Result<&SourceMetadata> {
        self.metadata
            .get_or_try_init(|| async {
                let session = VortexSession::default().with_tokio();
                let file = session
                    .open_options()
                    .with_file_size(self.input.metadata().size)
                    .open_object_store(
                        self.input.location().store().object_store(),
                        self.input.location().path().as_ref(),
                    )
                    .await
                    .map_err(|error| anyhow!("failed to open Vortex object: {error}"))?;
                let schema = file
                    .dtype()
                    .to_arrow_schema()
                    .map_err(|error| anyhow!("failed to convert Vortex dtype: {error}"))?;
                #[allow(clippy::cast_possible_truncation)]
                let row_count = file.row_count() as usize;
                Ok(SourceMetadata {
                    schema: Arc::new(schema),
                    row_count,
                })
            })
            .await
    }
}

#[async_trait]
impl DataSource for VortexDataSource {
    fn name(&self) -> &str {
        "vortex"
    }

    fn input(&self) -> &InputObject {
        &self.input
    }

    async fn schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.metadata().await?.schema))
    }

    async fn row_count(&self) -> Result<usize> {
        Ok(self.metadata().await?.row_count)
    }

    async fn as_table_provider(&self, _ctx: &SessionContext) -> Result<Arc<dyn TableProvider>> {
        let session = VortexSession::default().with_tokio();
        let file = session
            .open_options()
            .with_file_size(self.input.metadata().size)
            .open_object_store(
                self.input.location().store().object_store(),
                self.input.location().path().as_ref(),
            )
            .await
            .map_err(|error| anyhow!("failed to open Vortex object: {error}"))?;
        let schema = file
            .dtype()
            .to_arrow_schema()
            .map_err(|error| anyhow!("failed to convert Vortex dtype: {error}"))?;
        let data_source = file.data_source()?;

        Ok(Arc::new(VortexTable::new(
            data_source,
            session,
            Arc::new(schema),
        )))
    }

    fn supports_table_provider(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path as FsPath;

    use arrow::{
        array::{Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use object_store::{
        ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path,
    };
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;

    use crate::{
        sinks::{
            data_sink::DataSink,
            vortex::{VortexSink, VortexSinkOptions},
        },
        storage::{
            ObjectLocation, OutputPolicy, StorageConfig, StorageContext, StoreHandle, StoreKind,
        },
        utils::test_helpers::object_store::CountingStore,
    };

    async fn write_vortex_file(path: &FsPath, schema: &SchemaRef, batch: RecordBatch) {
        let storage = StorageContext::new(StorageConfig::default()).unwrap();
        let output = storage
            .create_output(
                path.to_string_lossy().as_ref(),
                OutputPolicy::new(true, true),
            )
            .await
            .unwrap();
        let mut sink = VortexSink::create(output, schema, VortexSinkOptions::new()).unwrap();
        sink.write_batch(batch).await.unwrap();
        sink.finish().await.unwrap();
    }

    async fn source() -> VortexDataSource {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.vortex");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();
        write_vortex_file(&file_path, &schema, batch).await;
        let bytes = std::fs::read(&file_path).unwrap();
        let size = u64::try_from(bytes.len()).unwrap();
        let path = Path::from("test.bin");
        let memory = InMemory::new();
        memory.put(&path, PutPayload::from(bytes)).await.unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(memory);
        let handle = Arc::new(StoreHandle::new(
            StoreKind::Gcs {
                bucket: "test".to_string(),
            },
            datafusion::execution::object_store::ObjectStoreUrl::parse("memory://test").unwrap(),
            store,
        ));
        let location = ObjectLocation::new(
            "memory://test/test.bin".to_string(),
            "memory://test/test.bin".to_string(),
            path.clone(),
            handle,
        );
        let meta = ObjectMeta {
            location: path,
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        };
        VortexDataSource::new(InputObject::new(location, meta))
    }

    #[tokio::test]
    async fn reads_dtype_and_row_count_from_object_store() {
        let source = source().await;

        assert_eq!(source.schema().await.unwrap().fields().len(), 2);
        assert_eq!(source.row_count().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn scans_object_store() {
        let source = source().await;
        let strategy =
            crate::io_strategies::input_strategy::InputStrategy::Single(Box::new(source));
        let ctx = SessionContext::new();
        let batches =
            futures::TryStreamExt::try_collect::<Vec<_>>(strategy.as_stream(&ctx).await.unwrap())
                .await
                .unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
    }

    async fn multi_segment_vortex_bytes() -> (Vec<u8>, usize) {
        const BATCH_ROWS: usize = 16_384;
        const BATCH_COUNT: usize = 8;
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("parallel.vortex");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let storage = StorageContext::new(StorageConfig::default()).unwrap();
        let output = storage
            .create_output(
                file_path.to_string_lossy().as_ref(),
                OutputPolicy::new(true, true),
            )
            .await
            .unwrap();
        let mut sink = VortexSink::create(
            output,
            &schema,
            VortexSinkOptions::new().with_record_batch_size(BATCH_ROWS),
        )
        .unwrap();
        for batch_index in 0..BATCH_COUNT {
            let start = batch_index * BATCH_ROWS;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        (start..start + BATCH_ROWS).map(|value| i32::try_from(value).unwrap()),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        (start..start + BATCH_ROWS).map(deterministic_payload),
                    )),
                ],
            )
            .unwrap();
            sink.write_batch(batch).await.unwrap();
        }
        sink.finish().await.unwrap();

        (std::fs::read(file_path).unwrap(), BATCH_ROWS * BATCH_COUNT)
    }

    #[tokio::test]
    #[ignore = "records the pinned Vortex single-file request shape"]
    async fn single_file_range_reads_are_serial_for_the_measured_fixture() {
        const REQUEST_LIMIT: usize = 2;

        let (bytes, row_count) = multi_segment_vortex_bytes().await;
        let object_size = bytes.len();
        let memory = InMemory::new();
        let path = Path::from("parallel.vortex");
        memory.put(&path, PutPayload::from(bytes)).await.unwrap();
        let (counting, requests) =
            CountingStore::new_with_range_delay(memory, std::time::Duration::from_millis(10));
        let storage = StorageContext::with_gcs_store(
            StorageConfig {
                max_requests: REQUEST_LIMIT,
                ..StorageConfig::default()
            },
            Arc::new(counting),
        );
        let input = storage
            .resolve_input("gs://test/parallel.vortex")
            .await
            .unwrap();
        let strategy = crate::io_strategies::input_strategy::InputStrategy::Single(Box::new(
            VortexDataSource::new(input),
        ));

        let config = datafusion::prelude::SessionConfig::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(0);
        let context = SessionContext::new_with_config(config);
        let batches = futures::TryStreamExt::try_collect::<Vec<_>>(
            strategy.as_stream(&context).await.unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            row_count
        );
        let maximum = requests.max_active_ranges.load(Ordering::SeqCst);
        assert_eq!(
            maximum,
            1,
            "single-file request shape changed across {} range requests for {object_size} bytes",
            requests.ranges.lock().unwrap().len()
        );
    }

    #[tokio::test]
    async fn multiple_files_read_in_parallel_within_the_store_limit() {
        const FILE_COUNT: usize = 4;
        const REQUEST_LIMIT: usize = 2;

        let (bytes, rows_per_file) = multi_segment_vortex_bytes().await;
        let memory = InMemory::new();
        for index in 0..FILE_COUNT {
            memory
                .put(
                    &Path::from(format!("parallel-{index}.vortex")),
                    PutPayload::from(bytes.clone()),
                )
                .await
                .unwrap();
        }
        let (counting, requests) =
            CountingStore::new_with_range_delay(memory, std::time::Duration::from_millis(10));
        let storage = StorageContext::with_gcs_store(
            StorageConfig {
                max_requests: REQUEST_LIMIT,
                ..StorageConfig::default()
            },
            Arc::new(counting),
        );
        let mut sources: Vec<Box<dyn DataSource>> = Vec::new();
        for index in 0..FILE_COUNT {
            let input = storage
                .resolve_input(&format!("gs://test/parallel-{index}.vortex"))
                .await
                .unwrap();
            sources.push(Box::new(VortexDataSource::new(input)));
        }
        let strategy = crate::io_strategies::input_strategy::InputStrategy::Multiple(sources);
        let context = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new()
                .with_target_partitions(4)
                .with_repartition_file_min_size(0),
        );

        let batches = futures::TryStreamExt::try_collect::<Vec<_>>(
            strategy.as_stream(&context).await.unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows_per_file * FILE_COUNT
        );
        let maximum = requests.max_active_ranges.load(Ordering::SeqCst);
        assert!(
            maximum > 1,
            "expected file-level parallel reads, got {maximum}"
        );
        assert!(maximum <= REQUEST_LIMIT, "store limit exceeded: {maximum}");
    }

    fn deterministic_payload(index: usize) -> String {
        let mut state = u64::try_from(index).unwrap() ^ 0x9e37_79b9_7f4a_7c15;
        (0..128)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                char::from(b'a' + u8::try_from(state % 26).unwrap())
            })
            .collect()
    }
}
