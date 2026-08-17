use std::{collections::HashMap, io::Write, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow::{
    buffer::Buffer,
    datatypes::SchemaRef,
    ipc::reader::{FileDecoder, StreamDecoder},
};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use serde_json::{Value, json};
use silk_chiffon_core::{FormatFuture, InputDetection, InspectionMode, InspectionOutput};
use silk_chiffon_inspection_output::{
    apply_theme, dim, display_location, format_bytes, format_number, header, render_schema_fields,
    schema_json, truncate_chars,
};
use silk_chiffon_storage::InputObject;
use tabled::{
    Table, Tabled,
    settings::{Alignment, Modify, Remove, Style, object::Columns, object::Rows},
};

use crate::{
    args::InspectionArgs,
    detection,
    input::{
        MAX_IPC_SAFETY_BYTES, StreamMessageRead, block_size, read_block, read_file_layout,
        read_stream_message,
    },
    variant::IpcVariant,
};

#[derive(Debug)]
struct BatchSummary {
    index: usize,
    num_rows: usize,
}

pub(crate) struct Inspector {
    schema: SchemaRef,
    variant: IpcVariant,
    file_size: u64,
    num_rows: Option<u64>,
    num_batches: Option<usize>,
    batches: Option<Vec<BatchSummary>>,
    custom_metadata: HashMap<String, String>,
    location: String,
}

impl Inspector {
    async fn open(object: &InputObject, count_rows: bool) -> Result<Self> {
        let variant = match detection::detect(object).await? {
            InputDetection::Match(variant) => IpcVariant::parse(&variant)?,
            InputDetection::Mismatch => bail!("not an Arrow IPC input"),
            InputDetection::Malformed(error) => return Err(error),
        };
        let location = display_location(object)?;
        match variant {
            IpcVariant::File => Self::open_file(object, location, count_rows).await,
            IpcVariant::Stream => Self::open_stream(object, location, count_rows).await,
        }
    }

    async fn open_file(object: &InputObject, location: String, count_rows: bool) -> Result<Self> {
        let handle = object.handle();
        let store = handle.object_store();
        let layout_pool_capacity = usize::try_from(MAX_IPC_SAFETY_BYTES)
            .context("Arrow IPC safety bound exceeds the platform address space")?;
        let layout = read_file_layout(
            &store,
            object.metadata(),
            Arc::new(GreedyMemoryPool::new(layout_pool_capacity)),
            handle.url().as_str(),
        )
        .await?;
        let custom_metadata = layout.schema.metadata().clone();
        if !count_rows {
            return Ok(Self {
                schema: Arc::clone(&layout.schema),
                variant: IpcVariant::File,
                file_size: object.metadata().size,
                num_rows: None,
                num_batches: Some(layout.record_batches.len()),
                batches: None,
                custom_metadata,
                location,
            });
        }

        let mut decoder = FileDecoder::new(Arc::clone(&layout.schema), layout.version);
        for block in &layout.dictionaries {
            ensure_block_is_bounded(block)?;
            let bytes = read_block(&store, handle.object_path(), block).await?;
            decoder.read_dictionary(block, &Buffer::from(bytes))?;
        }
        let mut total_rows = 0_u64;
        let mut batches = Vec::with_capacity(layout.record_batches.len());
        for (index, block) in layout.record_batches.iter().enumerate() {
            ensure_block_is_bounded(block)?;
            let bytes = read_block(&store, handle.object_path(), block).await?;
            if let Some(batch) = decoder.read_record_batch(block, &Buffer::from(bytes))? {
                total_rows = total_rows
                    .checked_add(u64::try_from(batch.num_rows())?)
                    .context("Arrow row count overflow")?;
                batches.push(BatchSummary {
                    index,
                    num_rows: batch.num_rows(),
                });
            }
        }
        Ok(Self {
            schema: Arc::clone(&layout.schema),
            variant: IpcVariant::File,
            file_size: object.metadata().size,
            num_rows: Some(total_rows),
            num_batches: Some(batches.len()),
            batches: Some(batches),
            custom_metadata,
            location,
        })
    }

    async fn open_stream(object: &InputObject, location: String, count_rows: bool) -> Result<Self> {
        let handle = object.handle();
        let store = handle.object_store();
        let mut decoder = StreamDecoder::new();
        let mut offset = 0_u64;
        let mut batches = Vec::new();
        while count_rows || decoder.schema().is_none() {
            let message =
                match read_stream_message(&store, object.metadata(), offset, handle.url().as_str())
                    .await?
                {
                    StreamMessageRead::End => break,
                    StreamMessageRead::SafetyBoundExceeded => {
                        bail!("Arrow IPC message exceeds the 512 MiB inspection safety bound")
                    }
                    StreamMessageRead::Message(message) => message,
                };
            offset = message.end;
            let mut buffer = Buffer::from(message.bytes);
            while !buffer.is_empty() {
                if let Some(batch) = decoder.decode(&mut buffer)? {
                    batches.push(BatchSummary {
                        index: batches.len(),
                        num_rows: batch.num_rows(),
                    });
                }
            }
        }
        if count_rows {
            decoder.finish()?;
        }
        let schema = decoder
            .schema()
            .context("Arrow IPC stream ended before its schema")?;
        let custom_metadata = schema.metadata().clone();
        let num_rows = if count_rows {
            Some(
                batches
                    .iter()
                    .try_fold(0_u64, |total, batch| {
                        total.checked_add(u64::try_from(batch.num_rows).ok()?)
                    })
                    .context("Arrow row count overflow")?,
            )
        } else {
            None
        };
        Ok(Self {
            schema,
            variant: IpcVariant::Stream,
            file_size: object.metadata().size,
            num_rows,
            num_batches: count_rows.then_some(batches.len()),
            batches: count_rows.then_some(batches),
            custom_metadata,
            location,
        })
    }

    fn render_batches(&self, output: &mut dyn Write) -> Result<()> {
        writeln!(output)?;
        writeln!(output, "{}", header("Record Batches"))?;
        let Some(batches) = &self.batches else {
            writeln!(output, "  {}", dim("(use --row-count to read)"))?;
            return Ok(());
        };
        #[derive(Tabled)]
        struct BatchRow {
            #[tabled(rename = "Batch")]
            batch: String,
            #[tabled(rename = "Rows")]
            rows: String,
        }
        let rows = batches.iter().map(|batch| BatchRow {
            batch: batch.index.to_string(),
            rows: format_number(batch.num_rows as u64),
        });
        let mut table = Table::new(rows);
        apply_theme(&mut table);
        table.with(Modify::new(Columns::new(1..)).with(Alignment::right()));
        writeln!(output, "{table}")?;
        Ok(())
    }

    fn render_default(&self, output: &mut dyn Write) -> Result<()> {
        writeln!(output, "{}", header(&self.location))?;
        writeln!(output)?;
        #[derive(Tabled)]
        struct InfoRow {
            #[tabled(rename = "")]
            label: String,
            #[tabled(rename = "")]
            value: String,
        }
        let rows = [
            InfoRow {
                label: "Format".to_owned(),
                value: format!("Arrow IPC ({})", self.variant.display_name()),
            },
            InfoRow {
                label: "Rows".to_owned(),
                value: self
                    .num_rows
                    .map_or_else(|| dim("(use --row-count)"), format_number),
            },
            InfoRow {
                label: "Record batches".to_owned(),
                value: self
                    .num_batches
                    .map_or_else(|| dim("(use --row-count)"), |count| count.to_string()),
            },
            InfoRow {
                label: "Columns".to_owned(),
                value: self.schema.fields().len().to_string(),
            },
            InfoRow {
                label: "Size".to_owned(),
                value: format_bytes(self.file_size),
            },
        ];
        let table = Table::new(rows)
            .with(Remove::row(Rows::first()))
            .with(Style::rounded().remove_horizontals())
            .with(Modify::new(Columns::new(1..)).with(Alignment::right()))
            .to_string();
        writeln!(output, "{table}")?;
        writeln!(output)?;
        writeln!(output, "{}", header("Schema"))?;
        render_schema_fields(&self.schema, output)?;

        if !self.custom_metadata.is_empty() {
            writeln!(output)?;
            writeln!(output, "{}", header("File Metadata"))?;
            for (key, value) in &self.custom_metadata {
                let value = if value.len() > 60 {
                    format!("{}...", truncate_chars(value, 57))
                } else {
                    value.clone()
                };
                writeln!(output, "  {key}: {value}")?;
            }
        }
        if self
            .schema
            .fields()
            .iter()
            .any(|field| !field.metadata().is_empty())
        {
            writeln!(output)?;
            writeln!(output, "{}", header("Column Metadata"))?;
            for field in self.schema.fields() {
                if !field.metadata().is_empty() {
                    writeln!(output, "  {}:", field.name())?;
                    for (key, value) in field.metadata() {
                        writeln!(output, "    {key}: {value}")?;
                    }
                }
            }
        }
        Ok(())
    }

    fn to_json(&self) -> Value {
        json!({
            "format": "arrow",
            "variant": self.variant.canonical_name(),
            "file": self.location,
            "rows": self.num_rows,
            "record_batches": self.num_batches,
            "schema": schema_json(&self.schema),
            "metadata": self.custom_metadata,
        })
    }
}

fn ensure_block_is_bounded(block: &arrow::ipc::Block) -> Result<()> {
    if u64::try_from(block_size(block)?)? > MAX_IPC_SAFETY_BYTES {
        bail!("Arrow IPC message exceeds the 512 MiB inspection safety bound");
    }
    Ok(())
}

pub(crate) fn inspect<'a>(
    object: &'a InputObject,
    mode: InspectionMode,
    args: &'a InspectionArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let inspector = Inspector::open(object, args.row_count || args.batches)
            .await
            .context("Failed to open Arrow input")?;
        if mode == InspectionMode::Json {
            return Ok(InspectionOutput::Json(inspector.to_json()));
        }
        let mut output = Vec::new();
        inspector.render_default(&mut output)?;
        if args.batches {
            inspector.render_batches(&mut output)?;
        }
        Ok(InspectionOutput::Text(String::from_utf8(output)?))
    })
}

#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        sync::{Arc, OnceLock},
    };

    use arrow::ipc::writer::{FileWriter, StreamWriter};
    use bytes::Bytes;
    use clap::Command;
    use object_store::{
        GetRange, ObjectMeta, ObjectStore, ObjectStoreExt, memory::InMemory,
        path::Path as ObjectPath,
    };
    use silk_chiffon_storage::{
        LocationInput, StorageAccess, StorageBackend, StorageRegistry, StorageSession,
    };
    use silk_chiffon_test_support::{ReadProbeStore, TestBatch};

    use super::*;

    static STORE: OnceLock<Arc<ReadProbeStore>> = OnceLock::new();
    static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

    fn store() -> Arc<ReadProbeStore> {
        Arc::clone(STORE.get_or_init(|| Arc::new(ReadProbeStore::new())))
    }

    async fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

    fn create_store(
        _: &url::Url,
        _: &(),
        _: Option<&silk_chiffon_storage::RetryConfig>,
    ) -> Result<Arc<dyn ObjectStore>> {
        Ok(store())
    }

    fn session() -> StorageSession {
        let registry = StorageRegistry::builder()
            .register(
                StorageBackend::without_args()
                    .name("memory")
                    .schemes(["memory"])
                    .access(StorageAccess::ReadOnly)
                    .allow_any_location()
                    .object_store_creator(create_store)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let matches = registry
            .augment_args(Command::new("test"))
            .try_get_matches_from(["test"])
            .unwrap();
        registry.create_session(&matches).unwrap()
    }

    fn ipc_bytes(variant: IpcVariant) -> Bytes {
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        let mut bytes = Cursor::new(Vec::new());
        match variant {
            IpcVariant::File => {
                let mut writer = FileWriter::try_new(&mut bytes, &batch.schema()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            IpcVariant::Stream => {
                let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
        }
        Bytes::from(bytes.into_inner())
    }

    async fn remote_object(variant: IpcVariant) -> InputObject {
        let session = session();
        let extension = match variant {
            IpcVariant::File => "arrow",
            IpcVariant::Stream => "arrows",
        };
        let location = LocationInput::parse(format!(
            "memory://bucket/inspection-{extension}.{extension}"
        ))
        .unwrap();
        let handle = session.input_handle(&location).unwrap();
        handle
            .object_store()
            .put(handle.object_path(), ipc_bytes(variant).into())
            .await
            .unwrap();
        session.lookup_input(&location).await.unwrap()
    }

    #[tokio::test]
    async fn remote_file_and_stream_inspection_use_object_store_inputs() {
        let _guard = test_guard().await;
        store().reset_observation();
        for variant in [IpcVariant::File, IpcVariant::Stream] {
            let object = remote_object(variant).await;
            let inspector = Inspector::open(&object, true).await.unwrap();

            assert_eq!(inspector.variant, variant);
            assert_eq!(inspector.num_rows, Some(3));
            assert_eq!(inspector.num_batches, Some(1));
            assert_eq!(inspector.location, object.handle().url().to_string());
        }
    }

    #[tokio::test]
    async fn row_counts_are_skipped_until_requested() {
        let _guard = test_guard().await;
        store().reset_observation();
        let object = remote_object(IpcVariant::Stream).await;
        let inspector = Inspector::open(&object, false).await.unwrap();

        assert_eq!(inspector.num_rows, None);
        assert_eq!(inspector.num_batches, None);
        assert_eq!(inspector.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn summary_inspection_defers_batch_reads_for_both_variants() {
        let _guard = test_guard().await;
        for variant in [IpcVariant::File, IpcVariant::Stream] {
            let store = store();
            store.reset_observation();
            let object = remote_object(variant).await;

            store.reset_observation();
            Inspector::open(&object, false).await.unwrap();
            let summary_ranges = store.ranges();
            let summary_reads = summary_ranges.len();
            assert!(summary_reads > 0);
            assert!(
                summary_ranges
                    .iter()
                    .all(|range| matches!(range, GetRange::Bounded(_)))
            );

            store.reset_observation();
            Inspector::open(&object, true).await.unwrap();
            let counted_reads = store.ranges().len();
            assert!(
                counted_reads > summary_reads,
                "{variant:?} summary used {summary_reads} reads and counting used {counted_reads}"
            );
        }
    }

    #[tokio::test]
    async fn inspection_reports_object_store_read_failures() {
        let _guard = test_guard().await;
        let store = store();
        store.reset_observation();
        let object = remote_object(IpcVariant::File).await;
        store.set_fail_reads(true);

        let error = match Inspector::open(&object, false).await {
            Ok(_) => panic!("inspection should surface the object-store failure"),
            Err(error) => error,
        };

        assert!(
            format!("{error:#}").contains("controlled object-store read failure"),
            "{error:#}"
        );
        store.reset_observation();
    }

    #[tokio::test]
    async fn oversized_messages_stop_before_inspection_reads_their_payloads() {
        let _guard = test_guard().await;
        let message_size = MAX_IPC_SAFETY_BYTES + 1;
        let file_block = arrow::ipc::Block::new(
            0,
            i32::try_from(message_size / 2 + 1).unwrap(),
            i64::try_from(message_size / 2).unwrap(),
        );
        assert!(
            ensure_block_is_bounded(&file_block)
                .unwrap_err()
                .to_string()
                .contains("512 MiB inspection safety bound")
        );

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location: ObjectPath = "oversized-stream.arrow".into();
        let bytes = [
            u32::MAX.to_le_bytes(),
            u32::try_from(message_size).unwrap().to_le_bytes(),
        ]
        .concat();
        store
            .put(&location, Bytes::from(bytes).into())
            .await
            .unwrap();
        let object = ObjectMeta {
            location,
            last_modified: chrono::Utc::now(),
            size: 8 + message_size,
            e_tag: None,
            version: None,
        };

        assert!(matches!(
            read_stream_message(&store, &object, 0, "oversized-stream.arrow")
                .await
                .unwrap(),
            StreamMessageRead::SafetyBoundExceeded
        ));
    }
}
