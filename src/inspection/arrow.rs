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
