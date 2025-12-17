//! Vortex file inspection.

use std::{collections::HashMap, fs::File, io::Write, sync::Arc};

use camino::{Utf8Path, Utf8PathBuf};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use serde_json::{Value, json};
use vortex::VortexSessionDefault;
use vortex::file::{OpenOptionsSessionExt, SegmentSpec};
use vortex_array::stats::StatsSet;
use vortex_session::VortexSession;

use crate::{
    inspection::magic::magic_bytes_match_start, utils::arrow_versioning::convert_schema_56_to_57,
};

use tabled::Tabled;

use super::{
    inspectable::{Inspectable, format_bytes, format_number, render_schema_fields, schema_to_json},
    style::{dim, header, label, rounded_table, value},
};

const VORTEX_MAGIC: &[u8] = b"VTXF";

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
    file_path: Utf8PathBuf,
    file_stats: Option<Arc<[StatsSet]>>,
    segments: Arc<[SegmentSpec]>,
    field_names: Vec<String>,
}

impl VortexInspector {
    pub fn open_file(path: &Utf8Path) -> Result<Self> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let session = VortexSession::default();
                let vortex_file = session
                    .open_options()
                    .open(path.as_str())
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
                    file_path: path.to_owned(),
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
    fn is_format(path: &Utf8Path) -> Result<bool> {
        let mut file = File::open(path)?;
        magic_bytes_match_start(&mut file, VORTEX_MAGIC)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::sinks::data_sink::DataSink;
    use crate::sinks::vortex::{VortexSink, VortexSinkOptions};

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

    fn write_vortex_file(path: &Path, schema: &SchemaRef, batch: RecordBatch) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sink =
                VortexSink::create(path.to_path_buf(), schema, VortexSinkOptions::new()).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();
        });
    }

    #[test]
    fn test_is_format_vortex_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.vortex");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);
        write_vortex_file(&std_path, &schema, batch);

        assert!(VortexInspector::is_format(path).unwrap());
    }

    #[test]
    fn test_open_vortex_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.vortex");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);
        write_vortex_file(&std_path, &schema, batch);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let inspector = rt
            .block_on(async { VortexInspector::open_file(path) })
            .unwrap();
        assert_eq!(inspector.row_count(), Some(3));
        assert_eq!(inspector.format_name(), "Vortex (file)");
    }

    #[test]
    fn test_is_format_non_vortex_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.txt");
        let path = Utf8Path::from_path(&std_path).unwrap();
        std::fs::write(path, "not a vortex file").unwrap();

        assert!(!VortexInspector::is_format(path).unwrap());
    }

    #[test]
    fn test_is_format_wrong_magic_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.vortex");
        let path = Utf8Path::from_path(&std_path).unwrap();
        // wrong magic bytes
        std::fs::write(path, b"PAR1garbage").unwrap();

        assert!(!VortexInspector::is_format(path).unwrap());
    }

    #[test]
    fn test_is_format_nonexistent_file() {
        let path = Utf8Path::new("/nonexistent/path/file.vortex");
        let result = VortexInspector::is_format(path);
        assert!(result.is_err());
    }
}
