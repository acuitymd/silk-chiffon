//! Object-store-native Vortex inspection.
//!
//! Opening uses the metadata observed during input lookup, so inspection does
//! not repeat a size request. Vortex performs bounded reads to discover the
//! footer; rendering the loaded metadata performs no further object I/O.

use std::{io::Write, sync::Arc};

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use serde_json::{Value, json};
use silk_chiffon_core::{FormatFuture, InspectionMode, InspectionOutput};
use silk_chiffon_inspection_output::{
    dim, display_location, format_bytes, format_number, header, label, render_schema_fields,
    render_schema_fields_detailed, rounded_table, schema_json, value,
};
use silk_chiffon_storage::InputObject;
use tabled::Tabled;
use vortex::{
    VortexSessionDefault,
    array::stats::StatsSet,
    arrow::ToArrowType,
    file::{OpenOptionsSessionExt, SegmentSpec},
    session::VortexSession,
};

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

struct Inspector {
    schema: SchemaRef,
    num_rows: u64,
    location: String,
    file_stats: Option<Arc<[StatsSet]>>,
    segments: Arc<[SegmentSpec]>,
    field_names: Vec<String>,
}

impl Inspector {
    async fn open(object: &InputObject) -> Result<Self> {
        let handle = object.handle();
        let store = handle.object_store();
        let session = VortexSession::default();
        let file = session
            .open_options()
            .with_file_size(object.metadata().size)
            .open_object_store(&store, handle.object_path().as_ref())
            .await
            .with_context(|| format!("failed to open Vortex input {}", handle.url()))?;
        let dtype = file.dtype();
        let schema = Arc::new(
            dtype
                .to_arrow_schema()
                .context("failed to convert Vortex type to Arrow schema")?,
        );
        let file_stats = file
            .file_stats()
            .map(|stats| Arc::clone(stats.stats_sets()));
        let segments = Arc::clone(file.footer().segment_map());
        let field_names = dtype
            .as_struct_fields_opt()
            .map(|fields| fields.names().iter().map(ToString::to_string).collect())
            .unwrap_or_default();

        Ok(Self {
            schema,
            num_rows: file.row_count(),
            location: display_location(object)?,
            file_stats,
            segments,
            field_names,
        })
    }

    fn render_default(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(out, "{} {}", header(&self.location), dim("(Vortex (file))"))?;
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
        writeln!(
            out,
            "{:<10} {}",
            label("Size:"),
            value(format_bytes(self.total_segment_size()))
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

    fn render_schema(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(
            out,
            "\n{} ({} columns):",
            header("Schema"),
            value(self.schema.fields().len())
        )?;
        writeln!(out)?;
        render_schema_fields_detailed(&self.schema, out)
    }

    fn render_stats(&self, out: &mut dyn Write) -> Result<()> {
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
        for (index, stat_set) in stats.iter().enumerate() {
            let field_name = self
                .field_names
                .get(index)
                .map(String::as_str)
                .unwrap_or("<unknown>");
            writeln!(out, "  {}", header(field_name))?;
            if stat_set.is_empty() {
                writeln!(out, "    {}", dim("(no stats)"))?;
            } else {
                for (stat, precision_value) in stat_set.iter() {
                    writeln!(
                        out,
                        "    {}: {}",
                        label(stat.name()),
                        value(format!("{precision_value:?}"))
                    )?;
                }
            }
            writeln!(out)?;
        }
        Ok(())
    }

    fn render_layout(&self, out: &mut dyn Write) -> Result<()> {
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
        writeln!(
            out,
            "\n{}: {}\n",
            label("Total data size"),
            value(format_bytes(self.total_segment_size()))
        )?;
        let rows = self
            .segments
            .iter()
            .enumerate()
            .map(|(index, segment)| SegmentRow {
                index,
                offset: segment.offset,
                length: segment.length,
                alignment: *segment.alignment,
            })
            .collect::<Vec<_>>();
        writeln!(out, "{}", rounded_table(rows))?;
        Ok(())
    }

    fn total_segment_size(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| u64::from(segment.length))
            .sum()
    }

    fn to_json(&self) -> Value {
        let statistics = self.file_stats.as_ref().map(|stats| {
            stats
                .iter()
                .enumerate()
                .map(|(index, stat_set)| {
                    let field = self
                        .field_names
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| format!("field_{index}"));
                    let stats = stat_set
                        .iter()
                        .map(|(stat, precision_value)| {
                            (
                                stat.name().to_string(),
                                json!(format!("{precision_value:?}")),
                            )
                        })
                        .collect::<serde_json::Map<String, Value>>();
                    json!({ "field": field, "stats": stats })
                })
                .collect::<Vec<_>>()
        });
        let segments = self
            .segments
            .iter()
            .enumerate()
            .map(|(index, segment)| {
                json!({
                    "index": index,
                    "offset": segment.offset,
                    "length": segment.length,
                    "alignment": *segment.alignment,
                })
            })
            .collect::<Vec<_>>();

        json!({
            "format": "vortex",
            "variant": "file",
            "file": self.location,
            "rows": self.num_rows,
            "num_segments": self.segments.len(),
            "total_size": self.total_segment_size(),
            "segments": segments,
            "schema": schema_json(&self.schema),
            "statistics": statistics,
        })
    }
}

pub(crate) fn inspect<'a>(
    object: &'a InputObject,
    mode: InspectionMode,
    args: &'a crate::args::InspectionArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let inspector = Inspector::open(object).await?;
        if mode == InspectionMode::Json {
            return Ok(InspectionOutput::Json(inspector.to_json()));
        }
        let mut output = Vec::new();
        inspector.render_default(&mut output)?;
        if args.schema {
            inspector.render_schema(&mut output)?;
        }
        if args.stats {
            inspector.render_stats(&mut output)?;
        }
        if args.layout {
            inspector.render_layout(&mut output)?;
        }
        Ok(InspectionOutput::Text(String::from_utf8(output)?))
    })
}

#[cfg(test)]
mod tests {
    use clap::Command;
    use object_store::GetRange;
    use silk_chiffon_core::{InspectionMode, InspectionOutput};

    use super::*;
    use crate::test_support::{guard, object_with, store, vortex_bytes, vortex_object};

    fn binding(arguments: &[&str]) -> silk_chiffon_core::InspectionBinding {
        let definition = crate::definition();
        let matches = definition
            .augment_inspection_args(Command::new("inspect"))
            .try_get_matches_from(std::iter::once("inspect").chain(arguments.iter().copied()))
            .unwrap();
        definition.bind_inspection(&matches).unwrap()
    }

    #[tokio::test]
    async fn registered_remote_summary_uses_observed_size_and_bounded_reads() {
        let _guard = guard().await;
        let object = vortex_object().await;
        store().reset_observation();

        let output = binding(&[])
            .inspect(&object, InspectionMode::Json)
            .await
            .unwrap();
        let InspectionOutput::Json(output) = output else {
            panic!("expected JSON output");
        };

        assert_eq!(output["format"], "vortex");
        assert_eq!(output["variant"], "file");
        assert_eq!(output["rows"], 3);
        assert_eq!(output["file"], object.handle().url().as_str());
        assert_eq!(store().head_requests(), 0);
        let ranges = store().ranges();
        assert!(!ranges.is_empty());
        assert!(
            ranges
                .iter()
                .all(|range| matches!(range, GetRange::Bounded(_)))
        );
    }

    #[tokio::test]
    async fn text_details_and_json_preserve_the_registered_contract() {
        let _guard = guard().await;
        let object = vortex_object().await;

        let output = binding(&["--schema", "--stats", "--layout"])
            .inspect(&object, InspectionMode::Text)
            .await
            .unwrap();
        let InspectionOutput::Text(output) = output else {
            panic!("expected text output");
        };
        for section in [
            "Vortex (file)",
            "Rows:",
            "Columns",
            "Schema",
            "Column Statistics",
            "Layout Segments",
        ] {
            assert!(output.contains(section), "missing {section}: {output}");
        }

        let output = binding(&[])
            .inspect(&object, InspectionMode::Json)
            .await
            .unwrap();
        let InspectionOutput::Json(output) = output else {
            panic!("expected JSON output");
        };
        assert!(output["schema"].is_array());
        assert!(output["segments"].is_array());
        assert!(output["statistics"].is_array());
    }

    #[tokio::test]
    async fn empty_remote_files_report_zero_rows_without_special_cases() {
        let _guard = guard().await;
        let object = object_with(vortex_bytes(Vec::new()).await).await;

        let output = binding(&[])
            .inspect(&object, InspectionMode::Json)
            .await
            .unwrap();
        let InspectionOutput::Json(output) = output else {
            panic!("expected JSON output");
        };

        assert_eq!(output["rows"], 0);
        assert_eq!(output["file"], object.handle().url().as_str());
    }

    #[tokio::test]
    async fn rendering_loaded_footer_metadata_performs_no_object_reads() {
        let _guard = guard().await;
        let object = vortex_object().await;
        let inspector = Inspector::open(&object).await.unwrap();
        store().reset_observation();

        let mut output = Vec::new();
        inspector.render_default(&mut output).unwrap();
        inspector.render_schema(&mut output).unwrap();
        inspector.render_stats(&mut output).unwrap();
        inspector.render_layout(&mut output).unwrap();
        let _ = inspector.to_json();

        assert!(store().ranges().is_empty());
        assert_eq!(store().head_requests(), 0);
    }

    #[tokio::test]
    async fn malformed_and_storage_failures_name_the_canonical_input() {
        let _guard = guard().await;
        let malformed = object_with(b"VTXFbroken".as_slice()).await;
        let error = binding(&[])
            .inspect(&malformed, InspectionMode::Text)
            .await
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains(malformed.handle().url().as_str()),
            "{message}"
        );

        let object = vortex_object().await;
        store().reset_observation();
        store().set_fail_reads(true);
        let error = binding(&[])
            .inspect(&object, InspectionMode::Json)
            .await
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains(object.handle().url().as_str()),
            "{message}"
        );
        store().reset_observation();
    }
}
