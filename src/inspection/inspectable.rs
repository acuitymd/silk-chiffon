//! Common trait and utilities for file inspection.

use std::{collections::HashMap, io::Write, path::Path};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use humansize::{BINARY, FormatSizeOptions, format_size};
use num_format::{Locale, ToFormattedString};
use serde::Serialize;
use serde_json::Value;
use tabled::Tabled;

use super::style::{dim, header, label, rounded_table, value};

/// Common trait for inspecting data files.
pub trait Inspectable: Send + Sync {
    /// Try to open the file as this format. Returns None if not this format.
    fn try_open(path: &Path) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Format name (e.g., "Parquet", "Arrow IPC (file)")
    fn format_name(&self) -> &str;

    /// Arrow schema
    fn schema(&self) -> &SchemaRef;

    /// Total row count (None if not available without full file read)
    fn row_count(&self) -> Option<u64>;

    /// Custom key-value metadata (if supported)
    fn custom_metadata(&self) -> Option<&HashMap<String, String>>;

    /// Render default output (summary)
    fn render_default(&self, out: &mut dyn Write) -> Result<()>;

    /// Render schema details
    fn render_schema(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(
            out,
            "\n{} ({} columns):",
            header("Schema"),
            value(self.schema().fields().len())
        )?;
        writeln!(out)?;
        render_schema_fields_detailed(self.schema(), out)?;
        Ok(())
    }

    /// Serialize to JSON
    fn to_json(&self) -> Value;

    /// Render to JSON
    fn render_to_json(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(out, "{}", serde_json::to_string(&self.to_json())?)?;
        Ok(())
    }
}

/// Format a byte size for human-readable output.
pub fn format_bytes(bytes: u64) -> String {
    format_size(bytes, FormatSizeOptions::from(BINARY).decimal_places(1))
}

/// Format a large number with thousands separators.
pub fn format_number(n: u64) -> String {
    n.to_formatted_string(&Locale::en)
}

const MAX_METADATA_DISPLAY_CHARS: usize = 100;

/// Truncate a string for display, adding char count if truncated.
pub fn truncate_for_display(value: &str) -> String {
    let char_count = value.chars().count();
    if char_count > MAX_METADATA_DISPLAY_CHARS {
        let truncated: String = value.chars().take(MAX_METADATA_DISPLAY_CHARS).collect();
        format!("{}... ({} chars total)", truncated, char_count)
    } else {
        value.to_string()
    }
}

/// Render key-value metadata with a header.
pub fn render_metadata_map(
    out: &mut dyn Write,
    header_text: &str,
    metadata: &HashMap<String, String>,
) -> Result<()> {
    writeln!(out, "\n{}:", header(header_text))?;
    if metadata.is_empty() {
        writeln!(out, "  {}", dim("(none)"))?;
    } else {
        for (k, v) in metadata {
            writeln!(out, "  {}: {}", label(k), truncate_for_display(v))?;
        }
    }
    Ok(())
}

/// Row for schema field table display.
#[derive(Tabled)]
struct SchemaFieldRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Type")]
    data_type: String,
    #[tabled(rename = "Nullable")]
    nullable: String,
}

/// Render schema fields to output.
pub fn render_schema_fields(schema: &SchemaRef, out: &mut dyn Write) -> Result<()> {
    let rows: Vec<SchemaFieldRow> = schema
        .fields()
        .iter()
        .map(|f| SchemaFieldRow {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
            nullable: if f.is_nullable() {
                "yes".to_string()
            } else {
                dim("no")
            },
        })
        .collect();

    writeln!(out, "{}", rounded_table(rows))?;
    Ok(())
}

/// Render schema fields with metadata to output.
pub fn render_schema_fields_detailed(schema: &SchemaRef, out: &mut dyn Write) -> Result<()> {
    for field in schema.fields() {
        let nullable = if field.is_nullable() {
            "nullable"
        } else {
            "not null"
        };
        writeln!(
            out,
            "  {} {}",
            header(field.name()),
            dim(format!("({})", nullable))
        )?;
        writeln!(out, "    {}: {}", label("Type"), value(field.data_type()))?;

        let meta = field.metadata();
        if !meta.is_empty() {
            writeln!(out, "    {}:", label("Metadata"))?;
            for (k, v) in meta {
                writeln!(out, "      {}: {}", dim(k), v)?;
            }
        }
    }
    Ok(())
}

/// Schema info for JSON serialization.
#[derive(Serialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

/// Convert schema to serializable form.
pub fn schema_to_json(schema: &SchemaRef) -> Vec<SchemaField> {
    schema
        .fields()
        .iter()
        .map(|f| SchemaField {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
            nullable: f.is_nullable(),
            metadata: f.metadata().clone(),
        })
        .collect()
}
