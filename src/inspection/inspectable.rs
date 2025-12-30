//! Common trait and utilities for file inspection.

use std::{collections::HashMap, io::Write};

use camino::Utf8Path;

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
    /// Check if the file is this format. Only errors on I/O issues.
    fn is_format(path: &Utf8Path) -> Result<bool>
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
        if meta.is_empty() {
            writeln!(out, "    {}: {}", label("Metadata"), dim("(none)"))?;
        } else {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_for_display_short_string() {
        let short = "hello world";
        assert_eq!(truncate_for_display(short), short);
    }

    #[test]
    fn test_truncate_for_display_exactly_max_length() {
        let exactly_100 = "a".repeat(MAX_METADATA_DISPLAY_CHARS);
        assert_eq!(truncate_for_display(&exactly_100), exactly_100);
    }

    #[test]
    fn test_truncate_for_display_over_max_length() {
        let long = "a".repeat(150);
        let result = truncate_for_display(&long);
        assert!(result.contains("..."));
        assert!(result.contains("150 chars total"));
    }

    #[test]
    fn test_truncate_for_display_unicode() {
        // 150 unicode chars (emoji are typically multi-byte but count as 1 char)
        let emojis = "🎉".repeat(150);
        let result = truncate_for_display(&emojis);
        assert!(result.contains("..."));
        assert!(result.contains("150 chars total"));
        // should truncate by char count, not byte count
        assert!(result.starts_with(&"🎉".repeat(100)));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1024), "1 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1 MiB");
        // verify fractional bytes display correctly
        assert_eq!(format_bytes(1536), "1.5 KiB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1_000_000), "1,000,000");
    }
}
