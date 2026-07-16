//! Arrow IPC metadata and scans backed by `ObjectStore` ranges.
//!
//! File metadata uses the trailer, footer, and record-batch message ranges.
//! Stream metadata walks message headers and skips bodies without decoding
//! arrays. The resulting schema and row count are cached together.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::read_footer_length;
use arrow::ipc::{MessageHeader, convert::fb_to_schema, root_as_footer, root_as_message};
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, datasource::file_format::arrow::ArrowFormat, prelude::SessionContext,
};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use tokio::sync::OnceCell;

use crate::{
    sources::data_source::{DataSource, SourceMetadata, object_store_table_provider},
    storage::InputObject,
};

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
const MAX_METADATA_LENGTH: usize = 64 * 1024 * 1024;
const ARROW_METADATA_FETCH_CONCURRENCY: usize = 16;

pub struct ArrowDataSource {
    input: InputObject,
    metadata: OnceCell<SourceMetadata>,
}

pub(crate) struct ArrowFileMetadata {
    pub(crate) schema: SchemaRef,
    pub(crate) batch_rows: Option<Vec<usize>>,
    pub(crate) num_batches: usize,
}

impl ArrowDataSource {
    pub fn new(input: InputObject) -> Self {
        Self {
            input,
            metadata: OnceCell::new(),
        }
    }

    async fn metadata(&self) -> Result<&SourceMetadata> {
        self.metadata
            .get_or_try_init(|| read_arrow_metadata(&self.input))
            .await
    }
}

#[async_trait]
impl DataSource for ArrowDataSource {
    fn name(&self) -> &str {
        "arrow"
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
        Ok(object_store_table_provider(
            &self.input,
            self.metadata().await?,
            Arc::new(ArrowFormat),
        ))
    }

    fn supports_table_provider(&self) -> bool {
        true
    }
}

pub(crate) async fn read_arrow_metadata(input: &InputObject) -> Result<SourceMetadata> {
    if let Some(metadata) = read_arrow_file_metadata(input, true).await? {
        return Ok(SourceMetadata {
            schema: metadata.schema,
            row_count: metadata
                .batch_rows
                .expect("row counts were requested")
                .into_iter()
                .sum(),
        });
    }
    read_stream_metadata(input, true).await
}

pub(crate) async fn read_arrow_file_metadata(
    input: &InputObject,
    count_rows: bool,
) -> Result<Option<ArrowFileMetadata>> {
    let size = input.metadata().size;
    if size < 10 {
        return Ok(None);
    }
    let trailer = input
        .location()
        .store()
        .object_store()
        .get_range(input.location().path(), size - 10..size)
        .await
        .with_context(|| {
            format!(
                "could not read Arrow trailer from '{}'",
                input.location().display()
            )
        })?;
    let trailer: [u8; 10] = trailer
        .as_ref()
        .try_into()
        .map_err(|_| anyhow!("Arrow trailer range returned an unexpected length"))?;
    let Ok(footer_len) = read_footer_length(trailer) else {
        return Ok(None);
    };

    let footer_len = u64::try_from(footer_len)?;
    let footer_start = size
        .checked_sub(10 + footer_len)
        .ok_or_else(|| anyhow!("Arrow footer extends before the start of the object"))?;
    let footer_bytes = input
        .location()
        .store()
        .object_store()
        .get_range(input.location().path(), footer_start..size - 10)
        .await
        .with_context(|| {
            format!(
                "could not read Arrow footer from '{}'",
                input.location().display()
            )
        })?;

    let (schema, ranges, num_batches) = {
        let footer = root_as_footer(&footer_bytes)
            .map_err(|error| anyhow!("could not parse Arrow footer: {error}"))?;
        let ipc_schema = footer
            .schema()
            .ok_or_else(|| anyhow!("Arrow footer does not contain a schema"))?;
        let schema = Arc::new(fb_to_schema(ipc_schema));
        let blocks = footer.recordBatches();
        let num_batches = blocks.as_ref().map_or(0, |blocks| blocks.len());
        let mut ranges = Vec::with_capacity(num_batches);
        if count_rows {
            for block in blocks.iter().flatten() {
                let offset = u64::try_from(block.offset())
                    .map_err(|_| anyhow!("Arrow record-batch offset is negative"))?;
                let metadata_len = usize::try_from(block.metaDataLength())
                    .map_err(|_| anyhow!("Arrow record-batch metadata length is negative"))?;
                if !(4..=MAX_METADATA_LENGTH).contains(&metadata_len) {
                    bail!("Arrow record-batch metadata length {metadata_len} is invalid");
                }
                let end = offset
                    .checked_add(u64::try_from(metadata_len)?)
                    .filter(|end| *end <= size)
                    .ok_or_else(|| {
                        anyhow!("Arrow record-batch metadata extends past the object")
                    })?;
                ranges.push(offset..end);
            }
        }
        (schema, ranges, num_batches)
    };

    let batch_rows = if count_rows {
        let store = input.location().store().object_store();
        let path = input.location().path();
        let display = input.location().display();
        Some(
            futures::stream::iter(ranges.into_iter().map(|range| async move {
                let start = range.start;
                let end = range.end;
                let message = store
                    .get_range(path, range)
                    .await
                    .with_context(|| {
                        format!(
                            "could not read Arrow record-batch metadata range {start}..{end} from '{display}'"
                        )
                    })?;
                record_batch_rows(&message).with_context(|| {
                    format!(
                        "could not parse Arrow record-batch metadata range {start}..{end} from '{display}'"
                    )
                })
            }))
            .buffered(ARROW_METADATA_FETCH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?,
        )
    } else {
        None
    };
    Ok(Some(ArrowFileMetadata {
        schema,
        batch_rows,
        num_batches,
    }))
}

pub(crate) async fn read_stream_metadata(
    input: &InputObject,
    count_rows: bool,
) -> Result<SourceMetadata> {
    let size = input.metadata().size;
    let store = input.location().store().object_store();
    let path = input.location().path();
    let mut offset = 0u64;
    let mut schema = None;
    let mut row_count = 0usize;

    while offset < size {
        let prefix_end = offset
            .checked_add(4)
            .filter(|end| *end <= size)
            .ok_or_else(|| anyhow!("truncated Arrow IPC stream message prefix"))?;
        let mut prefix = store.get_range(path, offset..prefix_end).await?;
        offset = prefix_end;
        if prefix.as_ref() == CONTINUATION_MARKER {
            let length_end = offset
                .checked_add(4)
                .filter(|end| *end <= size)
                .ok_or_else(|| anyhow!("truncated Arrow IPC stream metadata length"))?;
            prefix = store.get_range(path, offset..length_end).await?;
            offset = length_end;
        }

        let length_bytes: [u8; 4] = prefix
            .as_ref()
            .try_into()
            .map_err(|_| anyhow!("Arrow IPC stream length range returned an unexpected length"))?;
        let metadata_len = i32::from_le_bytes(length_bytes);
        if metadata_len == 0 {
            break;
        }
        let metadata_len = usize::try_from(metadata_len)
            .map_err(|_| anyhow!("Arrow IPC stream metadata length is negative"))?;
        if metadata_len > MAX_METADATA_LENGTH {
            bail!("Arrow IPC stream metadata length {metadata_len} exceeds 64 MiB");
        }
        let metadata_end = offset
            .checked_add(u64::try_from(metadata_len)?)
            .filter(|end| *end <= size)
            .ok_or_else(|| anyhow!("Arrow IPC stream metadata extends past the object"))?;
        let metadata = store.get_range(path, offset..metadata_end).await?;
        offset = metadata_end;

        let (message_schema, rows, body_len) = {
            let message = root_as_message(&metadata)
                .map_err(|error| anyhow!("could not parse Arrow IPC stream message: {error}"))?;
            let body_len = u64::try_from(message.bodyLength())
                .map_err(|_| anyhow!("Arrow IPC stream body length is negative"))?;
            let message_schema = message.header_as_schema().map(fb_to_schema).map(Arc::new);
            let rows = if message.header_type() == MessageHeader::RecordBatch {
                message
                    .header_as_record_batch()
                    .map(|batch| usize::try_from(batch.length()))
                    .transpose()
                    .map_err(|_| anyhow!("Arrow IPC stream record-batch length is negative"))?
                    .unwrap_or(0)
            } else {
                0
            };
            (message_schema, rows, body_len)
        };
        if schema.is_none()
            && let Some(message_schema) = message_schema
        {
            if !count_rows {
                return Ok(SourceMetadata {
                    schema: message_schema,
                    row_count: 0,
                });
            }
            schema = Some(message_schema);
        }
        if count_rows {
            row_count = row_count.saturating_add(rows);
        }
        offset = offset
            .checked_add(body_len)
            .filter(|end| *end <= size)
            .ok_or_else(|| anyhow!("Arrow IPC stream body extends past the object"))?;
    }

    let schema = schema.ok_or_else(|| {
        anyhow!(
            "could not read Arrow file or stream metadata from '{}'",
            input.location().display()
        )
    })?;
    Ok(SourceMetadata { schema, row_count })
}

fn record_batch_rows(message: &[u8]) -> Result<usize> {
    let message = if message.starts_with(&CONTINUATION_MARKER) {
        message
            .get(8..)
            .ok_or_else(|| anyhow!("truncated Arrow continuation message"))?
    } else {
        message
            .get(4..)
            .ok_or_else(|| anyhow!("truncated Arrow message"))?
    };
    let message = root_as_message(message)
        .map_err(|error| anyhow!("could not parse Arrow record-batch message: {error}"))?;
    if message.header_type() != MessageHeader::RecordBatch {
        return Ok(0);
    }
    let rows = message
        .header_as_record_batch()
        .ok_or_else(|| anyhow!("Arrow record-batch message is missing its header"))?
        .length();
    usize::try_from(rows).map_err(|_| anyhow!("Arrow record-batch length is negative"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
        ipc::writer::FileWriter,
    };
    use futures::StreamExt;
    use object_store::{
        GetRange, ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path,
    };

    use crate::{
        storage::{ObjectLocation, StoreHandle, StoreKind},
        utils::test_helpers::object_store::{CountingStore, RequestLog},
    };

    const TEST_ARROW_FILE_PATH: &str = "tests/files/people.file.arrow";
    const TEST_ARROW_STREAM_PATH: &str = "tests/files/people.stream.arrow";

    async fn memory_source(file: &str, key: &str) -> (ArrowDataSource, Arc<RequestLog>) {
        let bytes = std::fs::read(file).unwrap();
        memory_source_bytes(bytes, key, None).await
    }

    async fn memory_source_bytes(
        bytes: Vec<u8>,
        key: &str,
        range_delay: Option<Duration>,
    ) -> (ArrowDataSource, Arc<RequestLog>) {
        let size = bytes.len() as u64;
        let inner = InMemory::new();
        let path = Path::from(key);
        inner.put(&path, PutPayload::from(bytes)).await.unwrap();
        let (store, requests) = match range_delay {
            Some(delay) => CountingStore::new_with_range_delay(inner, delay),
            None => CountingStore::new(inner),
        };
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let handle = Arc::new(StoreHandle::new(
            StoreKind::Gcs {
                bucket: "test".to_string(),
            },
            datafusion::execution::object_store::ObjectStoreUrl::parse("memory://test").unwrap(),
            store,
        ));
        let location = ObjectLocation::new(
            format!("memory://test/{key}"),
            format!("memory://test/{key}"),
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
        (
            ArrowDataSource::new(InputObject::new(location, meta)),
            requests,
        )
    }

    fn many_batch_arrow_file(batch_count: usize) -> (Vec<u8>, Vec<usize>) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let expected_rows = (1..=batch_count).collect::<Vec<_>>();
        let mut bytes = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
            for rows in &expected_rows {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from_iter_values(
                        0..i32::try_from(*rows).unwrap(),
                    ))],
                )
                .unwrap();
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
        (bytes, expected_rows)
    }

    #[tokio::test]
    async fn file_metadata_uses_ranges() {
        let (source, requests) = memory_source(TEST_ARROW_FILE_PATH, "people.data").await;

        assert!(source.schema().await.unwrap().fields().len() > 1);
        assert!(source.row_count().await.unwrap() > 0);
        let ranges = requests.ranges.lock().unwrap();
        assert!(ranges.len() >= 3);
        assert!(ranges.iter().all(|range| match range {
            GetRange::Bounded(range) => range.end - range.start < source.input.metadata().size,
            GetRange::Offset(_) | GetRange::Suffix(_) => true,
        }));
        assert_eq!(
            requests.full_gets.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn file_batch_metadata_fetches_are_bounded_and_ordered() {
        let batch_count = ARROW_METADATA_FETCH_CONCURRENCY + 4;
        let (bytes, expected_rows) = many_batch_arrow_file(batch_count);
        let (source, requests) =
            memory_source_bytes(bytes, "many-batches.arrow", Some(Duration::from_millis(25))).await;

        let metadata = read_arrow_file_metadata(&source.input, true)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            metadata.batch_rows.as_deref(),
            Some(expected_rows.as_slice())
        );
        assert_eq!(requests.ranges.lock().unwrap().len(), batch_count + 2);
        assert_eq!(
            requests
                .max_active_ranges
                .load(std::sync::atomic::Ordering::SeqCst),
            ARROW_METADATA_FETCH_CONCURRENCY
        );
    }

    #[tokio::test]
    async fn stream_metadata_uses_ranges() {
        let (source, requests) = memory_source(TEST_ARROW_STREAM_PATH, "people.data").await;

        assert!(source.schema().await.unwrap().fields().len() > 1);
        assert!(source.row_count().await.unwrap() > 0);
        assert!(requests.ranges.lock().unwrap().len() > 3);
        assert_eq!(
            requests.full_gets.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn explicit_nonstandard_extension_can_be_scanned() {
        let (source, _) = memory_source(TEST_ARROW_FILE_PATH, "people.bin").await;
        let strategy =
            crate::io_strategies::input_strategy::InputStrategy::Single(Box::new(source));
        let ctx = SessionContext::new();
        let mut stream = strategy.as_stream(&ctx).await.unwrap();

        assert!(stream.next().await.unwrap().unwrap().num_rows() > 0);
    }

    #[tokio::test]
    async fn direct_stream_registers_remote_store() {
        let (source, _) = memory_source(TEST_ARROW_FILE_PATH, "people.data").await;

        let mut stream = source.as_stream().await.unwrap();

        assert!(stream.next().await.unwrap().unwrap().num_rows() > 0);
    }

    #[tokio::test]
    async fn stream_format_scans_from_object_store() {
        let (source, _) = memory_source(TEST_ARROW_STREAM_PATH, "people.data").await;
        let expected_rows = source.row_count().await.unwrap();
        let strategy =
            crate::io_strategies::input_strategy::InputStrategy::Single(Box::new(source));
        let ctx = SessionContext::new();
        let mut stream = strategy.as_stream(&ctx).await.unwrap();
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }

        assert_eq!(rows, expected_rows);
    }

    #[tokio::test]
    async fn file_scan_repartitions_into_multiple_ranges() {
        use datafusion::{physical_plan::ExecutionPlanProperties, prelude::SessionConfig};

        let (source, _) = memory_source(TEST_ARROW_FILE_PATH, "people.data").await;
        let strategy =
            crate::io_strategies::input_strategy::InputStrategy::Single(Box::new(source));
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(0);
        let ctx = SessionContext::new_with_config(config);
        let provider = strategy.as_table_provider(&ctx, None).await.unwrap();
        let plan = ctx
            .read_table(provider)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        assert!(plan.output_partitioning().partition_count() > 1);
    }
}
