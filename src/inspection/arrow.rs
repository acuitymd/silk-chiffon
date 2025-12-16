//! Arrow IPC file inspection.

use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::{FileReader, StreamReader};
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    inspectable::{
        Inspectable, format_number, render_metadata_map, render_schema_fields, schema_to_json,
    },
    style::{dim, header, label, value},
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
    pub fn open_file(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();

        let mut batch_info = Vec::new();
        let mut total_rows = 0u64;

        for (idx, batch_result) in reader.enumerate() {
            let batch = batch_result?;
            let num_rows = batch.num_rows();
            total_rows = total_rows.saturating_add(num_rows as u64);
            batch_info.push(BatchInfo {
                index: idx,
                num_rows,
            });
        }

        let custom_metadata = schema
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Self {
            schema,
            variant: ArrowVariant::File,
            num_rows: Some(total_rows),
            num_batches: Some(batch_info.len()),
            batch_info: Some(batch_info),
            custom_metadata,
            file_path: path.display().to_string(),
        })
    }

    pub fn open_stream(path: &Path, count_rows: bool) -> Result<Self> {
        let file = File::open(path)?;
        let reader = StreamReader::try_new(file, None)?;
        let schema = reader.schema();

        let custom_metadata = schema
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if count_rows {
            let mut batch_info = Vec::new();
            let mut total_rows = 0u64;

            for (idx, batch_result) in reader.enumerate() {
                let batch = batch_result?;
                let num_rows = batch.num_rows();
                total_rows = total_rows.saturating_add(num_rows as u64);
                batch_info.push(BatchInfo {
                    index: idx,
                    num_rows,
                });
            }

            Ok(Self {
                schema,
                variant: ArrowVariant::Stream,
                num_rows: Some(total_rows),
                num_batches: Some(batch_info.len()),
                batch_info: Some(batch_info),
                custom_metadata,
                file_path: path.display().to_string(),
            })
        } else {
            Ok(Self {
                schema,
                variant: ArrowVariant::Stream,
                num_rows: None,
                num_batches: None,
                batch_info: None,
                custom_metadata,
                file_path: path.display().to_string(),
            })
        }
    }

    pub fn variant(&self) -> ArrowVariant {
        self.variant
    }

    pub fn render_batches(&self, out: &mut dyn Write) -> Result<()> {
        match &self.batch_info {
            Some(batches) => {
                writeln!(
                    out,
                    "\n{} ({}):",
                    header("Record Batches"),
                    value(batches.len())
                )?;
                writeln!(out)?;

                // collapse identical consecutive batch sizes
                let mut i = 0;
                while i < batches.len() {
                    let size = batches[i].num_rows;
                    let start = i;
                    while i < batches.len() && batches[i].num_rows == size {
                        i += 1;
                    }
                    let end = i - 1;

                    if start == end {
                        writeln!(
                            out,
                            "  {} {}: {} rows",
                            label("Batch"),
                            value(start),
                            value(format_number(size as u64))
                        )?;
                    } else {
                        writeln!(
                            out,
                            "  {} {}-{}: {} rows each",
                            label("Batches"),
                            value(start),
                            value(end),
                            value(format_number(size as u64))
                        )?;
                    }
                }
            }
            None => {
                writeln!(
                    out,
                    "\n{}: {}",
                    label("Record Batches"),
                    dim("(use --row-count to read)")
                )?;
            }
        }

        Ok(())
    }

    pub fn render_metadata(&self, out: &mut dyn Write) -> Result<()> {
        render_metadata_map(out, "Custom Metadata", &self.custom_metadata)
    }
}

impl Inspectable for ArrowInspector {
    fn try_open(path: &Path) -> Result<Option<Self>> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };

        let mut magic = [0u8; 6];
        if file.read_exact(&mut magic).is_ok()
            && magic == ARROW_MAGIC
            && file.seek(SeekFrom::End(-6)).is_ok()
        {
            let mut footer_magic = [0u8; 6];
            if file.read_exact(&mut footer_magic).is_ok() && footer_magic == ARROW_MAGIC {
                return Self::open_file(path).map(Some);
            }
        }

        // stream format has no magic; try to parse header only
        match Self::open_stream(path, false) {
            Ok(inspector) => Ok(Some(inspector)),
            Err(_) => Ok(None),
        }
    }

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
        writeln!(
            out,
            "{} {}",
            header(&self.file_path),
            dim(format!("({})", self.format_name()))
        )?;
        writeln!(out)?;

        match self.num_rows {
            Some(n) => writeln!(out, "{:<15} {}", label("Rows:"), value(format_number(n)))?,
            None => writeln!(
                out,
                "{:<15} {}",
                label("Rows:"),
                dim("(use --row-count to read)")
            )?,
        }
        match self.num_batches {
            Some(n) => writeln!(out, "{:<15} {}", label("Record batches:"), value(n))?,
            None => writeln!(
                out,
                "{:<15} {}",
                label("Record batches:"),
                dim("(use --row-count to read)")
            )?,
        }

        writeln!(out)?;
        writeln!(
            out,
            "{} ({}):",
            header("Columns"),
            value(self.schema.fields().len())
        )?;
        render_schema_fields(&self.schema, out)?;

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
