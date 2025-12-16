//! Vortex file inspection.

use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use serde_json::{Value, json};
use vortex::VortexSessionDefault;
use vortex::file::{OpenOptionsSessionExt, SegmentSpec};
use vortex_array::stats::StatsSet;
use vortex_session::VortexSession;

use crate::utils::arrow_versioning::convert_schema_56_to_57;

use super::{
    inspectable::{Inspectable, format_bytes, format_number, render_schema_fields, schema_to_json},
    style::{dim, header, label, value},
};

const VORTEX_MAGIC: &[u8] = b"VTXF";

pub struct VortexInspector {
    schema: SchemaRef,
    num_rows: u64,
    file_path: String,
    file_stats: Option<Arc<[StatsSet]>>,
    segments: Arc<[SegmentSpec]>,
    field_names: Vec<String>,
}

impl VortexInspector {
    pub fn open_file(path: &Path) -> Result<Self> {
        let path_str = path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid path"))?;

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let session = VortexSession::default();
                let vortex_file = session
                    .open_options()
                    .open(path_str)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to open Vortex file: {}", e))?;

                let dtype = vortex_file.dtype();
                let arrow_schema_v56 = dtype.to_arrow_schema().map_err(|e| {
                    anyhow::anyhow!("Failed to convert Vortex DType to Arrow Schema: {}", e)
                })?;

                let schema = convert_schema_56_to_57(Arc::new(arrow_schema_v56))?;
                let num_rows = vortex_file.row_count();
                let file_stats = vortex_file.file_stats().cloned();
                let footer = vortex_file.footer();
                let segments = Arc::clone(footer.segment_map());

                // extract field names from dtype if it's a struct
                let field_names = dtype
                    .as_struct_fields_opt()
                    .map(|fields| fields.names().iter().map(|n| n.to_string()).collect())
                    .unwrap_or_default();

                Ok(Self {
                    schema,
                    num_rows,
                    file_path: path.display().to_string(),
                    file_stats,
                    segments,
                    field_names,
                })
            })
        })
    }

    pub fn render_stats(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(out, "\n{}:", header("Column Statistics"))?;

        let Some(stats) = &self.file_stats else {
            writeln!(out, "  {}", dim("(no statistics available)"))?;
            return Ok(());
        };

        if stats.is_empty() {
            writeln!(out, "  {}", dim("(no statistics available)"))?;
            return Ok(());
        }

        writeln!(out)?;

        for (idx, stat_set) in stats.iter().enumerate() {
            let field_name = self
                .field_names
                .get(idx)
                .map(|s| s.as_str())
                .unwrap_or("<unknown>");

            writeln!(out, "  {}", header(field_name))?;

            if stat_set.is_empty() {
                writeln!(out, "    {}", dim("(no stats)"))?;
            } else {
                for (stat, precision_value) in stat_set.iter() {
                    let value_str = format!("{:?}", precision_value);
                    writeln!(out, "    {}: {}", label(stat.name()), value(&value_str))?;
                }
            }
            writeln!(out)?;
        }

        Ok(())
    }

    pub fn render_layout(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(
            out,
            "\n{} ({}):",
            header("Layout Segments"),
            value(self.segments.len())
        )?;
        writeln!(out)?;

        if self.segments.is_empty() {
            writeln!(out, "  {}", dim("(no segments)"))?;
            return Ok(());
        }

        let total_size: u64 = self.segments.iter().map(|s| u64::from(s.length)).sum();
        writeln!(
            out,
            "  {}: {}",
            label("Total data size"),
            value(format_bytes(total_size))
        )?;
        writeln!(out)?;

        // find max widths for alignment
        let max_offset = self.segments.last().map(|s| s.offset).unwrap_or(0);
        let max_length = self.segments.iter().map(|s| s.length).max().unwrap_or(0);

        let offset_width = max_offset.to_string().len().max(6);
        let length_width = max_length.to_string().len().max(6);
        let idx_width = self.segments.len().to_string().len().max(3);

        writeln!(
            out,
            "  {:>idx_w$}  {:>offset_w$}  {:>length_w$}  {}",
            dim("#"),
            dim("Offset"),
            dim("Length"),
            dim("Align"),
            idx_w = idx_width,
            offset_w = offset_width,
            length_w = length_width,
        )?;

        for (i, seg) in self.segments.iter().enumerate() {
            writeln!(
                out,
                "  {:>idx_w$}  {:>offset_w$}  {:>length_w$}  {}",
                value(i),
                value(seg.offset),
                value(seg.length),
                value(*seg.alignment),
                idx_w = idx_width,
                offset_w = offset_width,
                length_w = length_width,
            )?;
        }

        Ok(())
    }
}

impl Inspectable for VortexInspector {
    fn try_open(path: &Path) -> Result<Option<Self>> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };

        let mut magic = [0u8; 4];
        if file.read_exact(&mut magic).is_err() {
            return Ok(None);
        }

        if magic == VORTEX_MAGIC {
            return Self::open_file(path).map(Some);
        }

        Ok(None)
    }

    fn format_name(&self) -> &str {
        "Vortex (file)"
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn row_count(&self) -> Option<u64> {
        Some(self.num_rows)
    }

    fn custom_metadata(&self) -> Option<&HashMap<String, String>> {
        None
    }

    fn render_default(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(
            out,
            "{} {}",
            header(&self.file_path),
            dim("(Vortex (file))")
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "{:<10} {}",
            label("Rows:"),
            value(format_number(self.num_rows))
        )?;
        writeln!(
            out,
            "{:<10} {}",
            label("Segments:"),
            value(self.segments.len())
        )?;

        let total_size: u64 = self.segments.iter().map(|s| u64::from(s.length)).sum();
        writeln!(
            out,
            "{:<10} {}",
            label("Size:"),
            value(format_bytes(total_size))
        )?;

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
        let stats_json: Option<Vec<Value>> = self.file_stats.as_ref().map(|stats| {
            stats
                .iter()
                .enumerate()
                .map(|(idx, stat_set)| {
                    let field_name = self
                        .field_names
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| format!("field_{}", idx));

                    let stat_entries: serde_json::Map<String, Value> = stat_set
                        .iter()
                        .map(|(stat, precision_value)| {
                            (
                                stat.name().to_string(),
                                json!(format!("{:?}", precision_value)),
                            )
                        })
                        .collect();

                    json!({
                        "field": field_name,
                        "stats": stat_entries,
                    })
                })
                .collect()
        });

        let total_size: u64 = self.segments.iter().map(|s| u64::from(s.length)).sum();

        json!({
            "format": "vortex",
            "variant": "file",
            "file": self.file_path,
            "rows": self.num_rows,
            "segments": self.segments.len(),
            "size": total_size,
            "schema": schema_to_json(&self.schema),
            "statistics": stats_json,
        })
    }
}
