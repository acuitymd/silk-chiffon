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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
    use tempfile::TempDir;

    use crate::{
        sinks::{
            data_sink::DataSink,
            vortex::{VortexSink, VortexSinkOptions},
        },
        storage::{InputObject, OutputPolicy, StorageConfig, StorageContext},
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

    async fn vortex_bytes() -> Vec<u8> {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.vortex");
        let schema = simple_schema();
        let storage = StorageContext::new(Default::default()).unwrap();
        let output = storage
            .create_output(
                path.to_string_lossy().as_ref(),
                OutputPolicy::new(true, true),
            )
            .await
            .unwrap();
        let mut sink = VortexSink::create(output, &schema, VortexSinkOptions::new()).unwrap();
        sink.write_batch(create_batch(&schema)).await.unwrap();
        sink.finish().await.unwrap();
        std::fs::read(path).unwrap()
    }

    async fn remote_input(bytes: Vec<u8>) -> (InputObject, Arc<RequestLog>) {
        let inner = InMemory::new();
        inner
            .put(&Path::from("nested/test.data"), PutPayload::from(bytes))
            .await
            .unwrap();
        let (store, requests) = CountingStore::new(inner);
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let storage = StorageContext::with_gcs_store(StorageConfig::default(), store);
        let input = storage
            .resolve_input("gs://inspect-tests/nested/test.data")
            .await
            .unwrap();
        (input, requests)
    }

    #[tokio::test]
    async fn opens_remote_object_and_renders_all_views() {
        let (input, requests) = remote_input(vortex_bytes().await).await;
        let inspector = VortexInspector::open(&input).await.unwrap();

        let mut default = Vec::new();
        inspector.render_default(&mut default).unwrap();
        let mut schema = Vec::new();
        inspector.render_schema(&mut schema).unwrap();
        let mut stats = Vec::new();
        inspector.render_stats(&mut stats).unwrap();
        let mut layout = Vec::new();
        inspector.render_layout(&mut layout).unwrap();

        assert!(
            String::from_utf8(default)
                .unwrap()
                .contains("gs://inspect-tests/nested/test.data")
        );
        assert!(String::from_utf8(schema).unwrap().contains("Schema"));
        assert!(
            String::from_utf8(stats)
                .unwrap()
                .contains("Column Statistics")
        );
        assert!(
            String::from_utf8(layout)
                .unwrap()
                .contains("Layout Segments")
        );
        assert_eq!(
            inspector.to_json()["file"],
            "gs://inspect-tests/nested/test.data"
        );
        assert_eq!(inspector.row_count(), Some(3));
        assert_eq!(requests.full_gets.load(Ordering::SeqCst), 0);
        assert!(!requests.ranges.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn local_and_remote_json_are_equal_except_for_location() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.vortex");
        let bytes = vortex_bytes().await;
        std::fs::write(&std_path, &bytes).unwrap();
        let storage = StorageContext::new(Default::default()).unwrap();
        let local_input = storage
            .resolve_input(std_path.to_string_lossy().as_ref())
            .await
            .unwrap();
        let local = VortexInspector::open(&local_input).await.unwrap();
        let (remote_input, _) = remote_input(bytes).await;
        let remote = VortexInspector::open(&remote_input).await.unwrap();

        let mut local_json = local.to_json();
        let mut remote_json = remote.to_json();
        local_json.as_object_mut().unwrap().remove("file");
        remote_json.as_object_mut().unwrap().remove("file");
        assert_eq!(local_json, remote_json);
    }

    #[tokio::test]
    async fn truncated_remote_object_names_the_uri() {
        let (input, _) = remote_input(b"VTXFbroken".to_vec()).await;

        let error = VortexInspector::open(&input)
            .await
            .err()
            .unwrap()
            .to_string();

        assert!(error.contains("gs://inspect-tests/nested/test.data"));
    }
}
