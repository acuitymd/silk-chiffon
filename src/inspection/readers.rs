//! Object-store metadata readers shared by the inspectors.
//!
//! These read Arrow and Parquet metadata through ranged object-store requests.
//! The transform sources move to these same readers in a later change.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::read_footer_length;
use arrow::ipc::{MessageHeader, convert::fb_to_schema, root_as_footer, root_as_message};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::ParquetMetaData;

use crate::storage::InputObject;

#[derive(Clone)]
pub(crate) struct SourceMetadata {
    pub schema: SchemaRef,
    #[expect(
        dead_code,
        reason = "consumed by the transform sources in a later change"
    )]
    pub row_count: usize,
}

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
const MAX_METADATA_LENGTH: usize = 64 * 1024 * 1024;
const ARROW_METADATA_FETCH_CONCURRENCY: usize = 16;

pub(crate) struct ArrowFileMetadata {
    pub(crate) schema: SchemaRef,
    pub(crate) batch_rows: Option<Vec<usize>>,
    pub(crate) num_batches: usize,
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

pub(crate) async fn read_parquet_metadata(input: &InputObject) -> Result<Arc<ParquetMetaData>> {
    let location = input.location();
    let mut reader = ParquetObjectReader::new(
        Arc::clone(location.store().object_store()),
        location.path().clone(),
    )
    .with_file_size(input.metadata().size);
    Ok(reader.get_metadata(None).await?)
}
