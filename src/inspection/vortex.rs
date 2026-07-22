//! Vortex file inspection.

use std::{collections::HashMap, io::Write, sync::Arc};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use serde_json::{Value, json};
use vortex::VortexSessionDefault;
use vortex::array::stats::StatsSet;
use vortex::file::{OpenOptionsSessionExt, SegmentSpec};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use tabled::Tabled;

use crate::{
    inspection::{
        inspectable::{
            Inspectable, format_bytes, format_number, render_schema_fields, schema_to_json,
        },
        style::{dim, header, label, rounded_table, value},
    },
    storage::InputObject,
};

/// Row for segment table display.
#[derive(Tabled)]
struct SegmentRow {
    #[tabled(rename = "#")]
    index: usize,
    #[tabled(rename = "Offset")]
    offset: u64,
    #[tabled(rename = "Length")]
    length: u32,
    #[tabled(rename = "Align")]
    alignment: usize,
}

pub struct VortexInspector {
    schema: SchemaRef,
    num_rows: u64,
    file_path: String,
    file_stats: Option<Arc<[StatsSet]>>,
    segments: Arc<[SegmentSpec]>,
    field_names: Vec<String>,
}

impl VortexInspector {
    pub async fn open(input: &InputObject) -> Result<Self> {
        let session = VortexSession::default().with_tokio();
        let vortex_file = session
            .open_options()
            .with_file_size(input.metadata().size)
            .open_object_store(
                input.location().store().object_store(),
                input.location().path().as_ref(),
            )
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "could not open Vortex object '{}': {error}",
                    input.location().display()
                )
            })?;

        let dtype = vortex_file.dtype();
        let arrow_schema = dtype.to_arrow_schema().map_err(|error| {
            anyhow::anyhow!(
                "could not convert Vortex dtype from '{}': {error}",
                input.location().display()
            )
        })?;

        let schema = Arc::new(arrow_schema);
        let num_rows = vortex_file.row_count();
        let file_stats = vortex_file
            .file_stats()
            .map(|stats| Arc::clone(stats.stats_sets()));
        let footer = vortex_file.footer();
        let segments = Arc::clone(footer.segment_map());
        let field_names = dtype
            .as_struct_fields_opt()
            .map(|fields| fields.names().iter().map(ToString::to_string).collect())
            .unwrap_or_default();

        Ok(Self {
            schema,
            num_rows,
            file_path: input.location().display().to_string(),
            file_stats,
            segments,
            field_names,
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

        if self.segments.is_empty() {
            writeln!(out, "  {}", dim("(no segments)"))?;
            return Ok(());
        }

        let total_size: u64 = self.segments.iter().map(|s| u64::from(s.length)).sum();
        writeln!(
            out,
            "\n{}: {}\n",
            label("Total data size"),
            value(format_bytes(total_size))
        )?;

        let rows: Vec<SegmentRow> = self
            .segments
            .iter()
            .enumerate()
            .map(|(i, seg)| SegmentRow {
                index: i,
                offset: seg.offset,
                length: seg.length,
                alignment: *seg.alignment,
            })
            .collect();

        writeln!(out, "{}", rounded_table(rows))?;

        Ok(())
    }
}

impl Inspectable for VortexInspector {
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

        let segments_json: Vec<Value> = self
            .segments
            .iter()
            .enumerate()
            .map(|(i, seg)| {
                json!({
                    "index": i,
                    "offset": seg.offset,
                    "length": seg.length,
                    "alignment": *seg.alignment,
                })
            })
            .collect();

        json!({
            "format": "vortex",
            "variant": "file",
            "file": self.file_path,
            "rows": self.num_rows,
            "num_segments": self.segments.len(),
            "total_size": total_size,
            "segments": segments_json,
            "schema": schema_to_json(&self.schema),
            "statistics": stats_json,
        })
    }
}
