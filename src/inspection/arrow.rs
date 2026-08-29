//! Arrow IPC inspection through object-store ranges and streams.

use std::{collections::HashMap, io::Write};

use anyhow::{Context, Result, anyhow, bail};
use arrow::datatypes::SchemaRef;
use arrow::{buffer::Buffer, ipc::reader::StreamDecoder};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use object_store::ObjectStoreExt;
use serde::Serialize;
use serde_json::{Value, json};
use tabled::{
    Table, Tabled,
    settings::{Alignment, Modify, Remove, Style, object::Columns, object::Rows},
};

use crate::{
    inspection::{
        inspectable::{
            Inspectable, format_bytes, format_number, render_schema_fields, schema_to_json,
            truncate_chars,
        },
        magic::read_magic_edges,
        style::{apply_theme, dim, header},
    },
    sources::arrow::{read_arrow_file_metadata, read_stream_metadata},
    storage::InputObject,
};

const ARROW_MAGIC: &[u8] = b"ARROW1";

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArrowVariant {
    File,
    Stream,
}

impl std::fmt::Display for ArrowVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowVariant::File => write!(f, "file"),
            ArrowVariant::Stream => write!(f, "stream"),
        }
    }
}

pub struct ArrowInspector {
    schema: SchemaRef,
    variant: ArrowVariant,
    file_size: u64,
    num_rows: Option<u64>,
    num_batches: Option<usize>,
    batch_info: Option<Vec<BatchInfo>>,
    custom_metadata: HashMap<String, String>,
    file_path: String,
}

#[derive(Debug, Serialize)]
pub struct BatchInfo {
    pub index: usize,
    pub num_rows: usize,
}

impl ArrowInspector {
    pub async fn open(input: &InputObject, count_rows: bool) -> Result<Self> {
        let edges = read_magic_edges(input).await?;
        if edges.prefix.starts_with(ARROW_MAGIC) {
            if edges.size < 10 {
                bail!(
                    "Arrow file '{}' has a truncated trailer",
                    input.location().display()
                );
            }
            if !edges.matches(ARROW_MAGIC) {
                bail!(
                    "Arrow file '{}' has an invalid or truncated trailer",
                    input.location().display()
                );
            }
            let metadata = read_arrow_file_metadata(input, count_rows)
                .await
                .with_context(|| {
                    format!(
                        "could not read Arrow file metadata from '{}'",
                        input.location().display()
                    )
                })?
                .ok_or_else(|| {
                    anyhow!(
                        "Arrow file '{}' has an invalid trailer",
                        input.location().display()
                    )
                })?;
            let batch_info = metadata.batch_rows.map(batch_info);
            let num_rows = batch_info
                .as_ref()
                .map(|batches| batches.iter().map(|batch| batch.num_rows as u64).sum());
            return Ok(Self::new(
                input,
                metadata.schema,
                ArrowVariant::File,
                num_rows,
                Some(metadata.num_batches),
                batch_info,
            ));
        }

        let (schema, batch_info) = if count_rows {
            let result = input
                .location()
                .store()
                .object_store()
                .get(input.location().path())
                .await
                .with_context(|| {
                    format!(
                        "could not read Arrow stream from '{}'",
                        input.location().display()
                    )
                })?;
            let decoded = decode_stream_chunks(result.into_stream())
                .await
                .with_context(|| {
                    format!(
                        "could not decode Arrow stream '{}'",
                        input.location().display()
                    )
                })?;
            (decoded.schema, Some(batch_info(decoded.batch_rows)))
        } else {
            let metadata = read_stream_metadata(input, false).await.with_context(|| {
                format!("'{}' is not an Arrow IPC file", input.location().display())
            })?;
            (metadata.schema, None)
        };
        let num_rows = batch_info
            .as_ref()
            .map(|batches| batches.iter().map(|batch| batch.num_rows as u64).sum());
        let num_batches = batch_info.as_ref().map(Vec::len);
        Ok(Self::new(
            input,
            schema,
            ArrowVariant::Stream,
            num_rows,
            num_batches,
            batch_info,
        ))
    }

    fn new(
        input: &InputObject,
        schema: SchemaRef,
        variant: ArrowVariant,
        num_rows: Option<u64>,
        num_batches: Option<usize>,
        batch_info: Option<Vec<BatchInfo>>,
    ) -> Self {
        let custom_metadata = schema.metadata().clone();
        Self {
            schema,
            variant,
            file_size: input.metadata().size,
            num_rows,
            num_batches,
            batch_info,
            custom_metadata,
            file_path: input.location().display().to_string(),
        }
    }

    pub fn variant(&self) -> ArrowVariant {
        self.variant
    }

    pub fn render_batches(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(out)?;
        writeln!(out, "{}", header("Record Batches"))?;

        match &self.batch_info {
            Some(batches) => {
                #[derive(Tabled)]
                struct BatchRow {
                    #[tabled(rename = "Batch")]
                    batch: String,
                    #[tabled(rename = "Rows")]
                    rows: String,
                }

                let rows: Vec<BatchRow> = batches
                    .iter()
                    .map(|b| BatchRow {
                        batch: b.index.to_string(),
                        rows: format_number(b.num_rows as u64),
                    })
                    .collect();

                let mut table = Table::new(&rows);
                apply_theme(&mut table);
                table.with(Modify::new(Columns::new(1..)).with(Alignment::right()));
                writeln!(out, "{table}")?;
            }
            None => {
                writeln!(out, "  {}", dim("(use --row-count to read)"))?;
            }
        }

        Ok(())
    }

    pub async fn detect_variant(input: &InputObject) -> Result<ArrowVariant> {
        Ok(Self::open(input, false).await?.variant())
    }
}

fn batch_info(rows: Vec<usize>) -> Vec<BatchInfo> {
    rows.into_iter()
        .enumerate()
        .map(|(index, num_rows)| BatchInfo { index, num_rows })
        .collect()
}

struct DecodedStream {
    schema: SchemaRef,
    batch_rows: Vec<usize>,
}

async fn decode_stream_chunks<S>(mut chunks: S) -> Result<DecodedStream>
where
    S: Stream<Item = object_store::Result<Bytes>> + Unpin,
{
    let mut decoder = StreamDecoder::new();
    let mut batch_rows = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let mut buffer = Buffer::from(chunk?);
        while !buffer.is_empty() {
            let remaining = buffer.len();
            if let Some(batch) = decoder.decode(&mut buffer)? {
                batch_rows.push(batch.num_rows());
            } else if buffer.len() == remaining {
                bail!("Arrow stream decoder did not consume its input");
            }
        }
    }
    decoder.finish()?;
    let schema = decoder
        .schema()
        .ok_or_else(|| anyhow!("Arrow stream does not contain a schema"))?;
    Ok(DecodedStream { schema, batch_rows })
}

impl Inspectable for ArrowInspector {
    fn format_name(&self) -> &str {
        match self.variant {
            ArrowVariant::File => "Arrow IPC (file)",
            ArrowVariant::Stream => "Arrow IPC (stream)",
        }
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn row_count(&self) -> Option<u64> {
        self.num_rows
    }

    fn custom_metadata(&self) -> Option<&HashMap<String, String>> {
        if self.custom_metadata.is_empty() {
            None
        } else {
            Some(&self.custom_metadata)
        }
    }

    fn render_default(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(out, "{}", header(&self.file_path))?;
        writeln!(out)?;

        #[derive(Tabled)]
        struct InfoRow {
            #[tabled(rename = "")]
            label: String,
            #[tabled(rename = "")]
            value: String,
        }

        let format_display = match self.variant {
            ArrowVariant::File => "Arrow IPC (file)",
            ArrowVariant::Stream => "Arrow IPC (stream)",
        };

        let rows_display = self
            .num_rows
            .map_or_else(|| dim("(use --row-count)"), format_number);

        let batches_display = self
            .num_batches
            .map_or_else(|| dim("(use --row-count)"), |n| n.to_string());

        let info_rows = vec![
            InfoRow {
                label: "Format".to_string(),
                value: format_display.to_string(),
            },
            InfoRow {
                label: "Rows".to_string(),
                value: rows_display,
            },
            InfoRow {
                label: "Record batches".to_string(),
                value: batches_display,
            },
            InfoRow {
                label: "Columns".to_string(),
                value: self.schema.fields().len().to_string(),
            },
            InfoRow {
                label: "Size".to_string(),
                value: format_bytes(self.file_size),
            },
        ];

        let info_table = Table::new(&info_rows)
            .with(Remove::row(Rows::first()))
            .with(Style::rounded().remove_horizontals())
            .with(Modify::new(Columns::new(1..)).with(Alignment::right()))
            .to_string();
        writeln!(out, "{info_table}")?;

        writeln!(out)?;
        writeln!(out, "{}", header("Schema"))?;
        render_schema_fields(&self.schema, out)?;

        if !self.custom_metadata.is_empty() {
            writeln!(out)?;
            writeln!(out, "{}", header("File Metadata"))?;
            for (k, v) in &self.custom_metadata {
                let truncated = if v.len() > 60 {
                    format!("{}...", truncate_chars(v, 57))
                } else {
                    v.clone()
                };
                writeln!(out, "  {}: {}", k, truncated)?;
            }
        }

        let has_column_meta = self
            .schema
            .fields()
            .iter()
            .any(|f| !f.metadata().is_empty());
        if has_column_meta {
            writeln!(out)?;
            writeln!(out, "{}", header("Column Metadata"))?;
            for field in self.schema.fields() {
                let field_meta = field.metadata();
                if !field_meta.is_empty() {
                    writeln!(out, "  {}:", field.name())?;
                    for (k, v) in field_meta {
                        writeln!(out, "    {}: {}", k, v)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn to_json(&self) -> Value {
        json!({
            "format": "arrow",
            "variant": self.variant,
            "file": self.file_path,
            "rows": self.num_rows,
            "record_batches": self.num_batches,
            "schema": schema_to_json(&self.schema),
            "metadata": self.custom_metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::Ordering};

    use arrow::array::{DictionaryArray, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::ipc::writer::{FileWriter, StreamWriter};
    use bytes::Bytes;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use crate::{
        storage::{InputObject, StorageConfig, StorageContext},
        utils::test_helpers::object_store::{CountingStore, RequestLog},
    };

    fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    fn file_bytes(batches: &[RecordBatch]) -> Vec<u8> {
        let mut bytes = Vec::new();
        let schema = batches[0].schema();
        let mut writer = FileWriter::try_new(&mut bytes, &schema).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    fn stream_bytes(batches: &[RecordBatch]) -> Vec<u8> {
        let mut bytes = Vec::new();
        let schema = batches[0].schema();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    async fn input(bytes: Vec<u8>, key: &str) -> (InputObject, Arc<RequestLog>) {
        let inner = InMemory::new();
        inner
            .put(&Path::from(key), PutPayload::from(bytes))
            .await
            .unwrap();
        let (store, requests) = CountingStore::new(inner);
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let storage = StorageContext::with_gcs_store(StorageConfig::default(), store);
        let input = storage
            .resolve_input(&format!("gs://inspect-tests/{key}"))
            .await
            .unwrap();
        (input, requests)
    }

    #[tokio::test]
    async fn file_schema_uses_footer_ranges() {
        let schema = simple_schema();
        let batch = create_batch(&schema);
        let bytes = file_bytes(&[batch]);
        let size = bytes.len() as u64;
        let (input, requests) = input(bytes, "people.arrow").await;

        let inspector = ArrowInspector::open(&input, false).await.unwrap();

        assert_eq!(inspector.variant(), ArrowVariant::File);
        assert_eq!(inspector.schema().as_ref(), schema.as_ref());
        assert_eq!(inspector.row_count(), None);
        assert_eq!(inspector.num_batches, Some(1));
        assert_eq!(requests.full_gets.load(Ordering::SeqCst), 0);
        assert!(requests.ranges.lock().unwrap().iter().all(|range| {
            range
                .as_range(size)
                .is_ok_and(|range| range.end - range.start < size)
        }));
    }

    #[tokio::test]
    async fn file_row_count_and_batches_use_message_ranges() {
        let schema = simple_schema();
        let first = create_batch(&schema);
        let second = create_batch(&schema);
        let (input, requests) = input(file_bytes(&[first, second]), "people.arrow").await;

        let inspector = ArrowInspector::open(&input, true).await.unwrap();

        assert_eq!(inspector.row_count(), Some(6));
        assert_eq!(inspector.num_batches, Some(2));
        assert_eq!(
            inspector
                .batch_info
                .as_ref()
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows)
                .collect::<Vec<_>>(),
            vec![3, 3]
        );
        assert_eq!(requests.full_gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stream_schema_uses_ranges_without_full_get() {
        let schema = simple_schema();
        let batch = create_batch(&schema);
        let (input, requests) = input(stream_bytes(&[batch]), "people.arrows").await;

        let inspector = ArrowInspector::open(&input, false).await.unwrap();

        assert_eq!(inspector.variant(), ArrowVariant::Stream);
        assert_eq!(inspector.schema().as_ref(), schema.as_ref());
        assert_eq!(inspector.row_count(), None);
        assert_eq!(inspector.num_batches, None);
        assert_eq!(requests.full_gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stream_rows_and_batches_decode_object_stream() {
        let schema = simple_schema();
        let first = create_batch(&schema);
        let second = create_batch(&schema);
        let (input, requests) = input(stream_bytes(&[first, second]), "people.arrows").await;

        let inspector = ArrowInspector::open(&input, true).await.unwrap();

        assert_eq!(inspector.row_count(), Some(6));
        assert_eq!(inspector.num_batches, Some(2));
        assert_eq!(requests.full_gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stream_decoder_handles_dictionaries_and_single_byte_chunks() {
        let dictionary = DictionaryArray::<Int32Type>::from_iter([Some("a"), Some("b"), Some("a")]);
        let batch = RecordBatch::try_from_iter([("category", Arc::new(dictionary) as _)]).unwrap();
        let (input, _) = input(file_bytes(std::slice::from_ref(&batch)), "dictionary.arrow").await;
        let inspector = ArrowInspector::open(&input, true).await.unwrap();
        assert_eq!(inspector.row_count(), Some(3));
        assert!(matches!(
            inspector.schema.field(0).data_type(),
            DataType::Dictionary(_, _)
        ));

        let bytes = stream_bytes(&[batch]);
        let chunks =
            futures::stream::iter(bytes.into_iter().map(|byte| Ok(Bytes::from(vec![byte]))));

        let decoded = decode_stream_chunks(chunks).await.unwrap();

        assert_eq!(decoded.schema.fields().len(), 1);
        assert_eq!(decoded.batch_rows, vec![3]);
    }

    #[tokio::test]
    async fn malformed_arrow_names_remote_location() {
        let (input, _) = input(b"ARROW1".to_vec(), "truncated.arrow").await;

        let error = ArrowInspector::open(&input, false)
            .await
            .err()
            .unwrap()
            .to_string();

        assert!(
            error.contains("gs://inspect-tests/truncated.arrow"),
            "{error}"
        );
        assert!(error.contains("trailer"), "{error}");
    }

    #[tokio::test]
    async fn json_shape_matches_local_inspection() {
        let schema = simple_schema();
        let batch = create_batch(&schema);
        let (input, _) = input(file_bytes(&[batch]), "people.arrow").await;
        let inspector = ArrowInspector::open(&input, true).await.unwrap();

        let json = inspector.to_json();

        assert_eq!(json["format"], "arrow");
        assert_eq!(json["variant"], "file");
        assert_eq!(json["file"], "gs://inspect-tests/people.arrow");
        assert_eq!(json["rows"], 3);
        assert_eq!(json["record_batches"], 1);
    }
}
