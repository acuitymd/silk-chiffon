//! Shared presentation primitives for format inspection.
//!
//! Format crates retain their own summaries and output shapes. This crate keeps
//! common terminal styling, schema presentation, and input-location display
//! consistent without imposing a universal inspector abstraction.

use std::{collections::HashMap, fmt::Display, io::Write};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use humansize::{BINARY, FormatSizeOptions, format_size};
use num_format::{Locale, ToFormattedString};
use owo_colors::{OwoColorize, Style};
use serde::Serialize;
use silk_chiffon_storage::InputObject;
use tabled::{
    Table, Tabled,
    settings::{
        Alignment, Color, Modify, Style as TableStyle,
        object::{Columns, Rows},
    },
};

const MAX_METADATA_DISPLAY_CHARS: usize = 100;

/// Formats a byte count using binary units.
pub fn format_bytes(bytes: u64) -> String {
    format_size(bytes, FormatSizeOptions::from(BINARY).decimal_places(1))
}

/// Formats an integer with thousands separators.
pub fn format_number(number: u64) -> String {
    number.to_formatted_string(&Locale::en)
}

/// Returns the prefix containing at most `max_chars` Unicode scalar values.
pub fn truncate_chars(value: &str, max_chars: usize) -> &str {
    match value.char_indices().nth(max_chars) {
        Some((index, _)) => &value[..index],
        None => value,
    }
}

/// Truncates long metadata values while retaining their original character count.
pub fn truncate_for_display(value: &str) -> String {
    let count = value.chars().count();
    if count > MAX_METADATA_DISPLAY_CHARS {
        format!(
            "{}... ({} chars total)",
            truncate_chars(value, MAX_METADATA_DISPLAY_CHARS),
            count
        )
    } else {
        value.to_owned()
    }
}

/// Returns the user-facing location for an exact input object.
pub fn display_location(object: &InputObject) -> Result<String> {
    if object.input_handle().url().scheme() == "file" {
        return object
            .input_handle()
            .local_path()?
            .into_os_string()
            .into_string()
            .map_err(|path| anyhow::anyhow!("local path is not valid UTF-8: {path:?}"));
    }
    Ok(object.input_handle().url().to_string())
}

/// Applies the standard inspection theme to a table.
pub fn apply_theme(table: &mut Table) {
    table
        .with(TableStyle::rounded())
        .modify(Rows::first(), Alignment::center())
        .modify(Rows::first(), Color::BOLD);
}

/// Creates a table with the standard inspection theme.
pub fn rounded_table<T, I>(data: I) -> Table
where
    T: Tabled,
    I: IntoIterator<Item = T>,
{
    let mut table = Table::new(data);
    apply_theme(&mut table);
    table
}

/// Styles a section heading.
pub fn header(value: impl Display) -> String {
    value.style(Style::new().bold()).to_string()
}

/// Styles a field label.
pub fn label(value: impl Display) -> String {
    value.style(Style::new().cyan()).to_string()
}

/// Styles a field value.
pub fn value(value: impl Display) -> String {
    value.style(Style::new().green()).to_string()
}

/// Styles secondary information.
pub fn dim(value: impl Display) -> String {
    value.style(Style::new().dimmed()).to_string()
}

/// Returns the conventional missing-value marker.
pub fn missing_value() -> String {
    dim("-")
}

/// Styles a boolean using the inspection glyph convention.
pub fn boolean_display(value: bool) -> String {
    if value {
        value_style("■")
    } else {
        dim("□")
    }
}

/// Styles a true value and renders false as missing.
pub fn true_or_missing_display(value: bool) -> String {
    if value {
        value_style("■")
    } else {
        missing_value()
    }
}

fn value_style(value: impl Display) -> String {
    value.style(Style::new().green()).to_string()
}

/// Styles a compression codec according to its family.
pub fn compression(codec: &str) -> String {
    match codec {
        "UNCOMPRESSED" => codec.style(Style::new().dimmed()).to_string(),
        "SNAPPY" => codec.style(Style::new().yellow()).to_string(),
        "GZIP" | "LZ4" | "LZ4_RAW" => codec.style(Style::new().blue()).to_string(),
        "LZO" => codec.style(Style::new().magenta()).to_string(),
        "BROTLI" => codec.style(Style::new().cyan()).to_string(),
        "ZSTD" => codec.style(Style::new().green()).to_string(),
        _ => codec.to_owned(),
    }
}

/// Styles an encoding name according to its family.
pub fn encoding(encoding: &str) -> String {
    match encoding {
        "RLE_DICTIONARY" | "PLAIN_DICTIONARY" => encoding.style(Style::new().yellow()).to_string(),
        "RLE" | "BIT_PACKED" => encoding.style(Style::new().blue()).to_string(),
        "BYTE_STREAM_SPLIT" => encoding.style(Style::new().cyan()).to_string(),
        value if value.starts_with("DELTA") => value.style(Style::new().magenta()).to_string(),
        _ => encoding.to_owned(),
    }
}

/// Styles an Arrow data type according to its family.
pub fn data_type(data_type: &str) -> String {
    match data_type {
        "Int8" => data_type.style(Style::new().bright_green()).to_string(),
        "Int16" => data_type.style(Style::new().green()).to_string(),
        "Int32" => data_type.style(Style::new().cyan()).to_string(),
        "Int64" => data_type.style(Style::new().bright_cyan()).to_string(),
        "UInt8" => data_type
            .style(Style::new().bright_green())
            .bold()
            .to_string(),
        "UInt16" => data_type.style(Style::new().green()).bold().to_string(),
        "UInt32" => data_type.style(Style::new().cyan()).bold().to_string(),
        "UInt64" => data_type
            .style(Style::new().bright_cyan())
            .bold()
            .to_string(),
        "Float16" => data_type.style(Style::new().bright_blue()).to_string(),
        "Float32" => data_type.style(Style::new().blue()).to_string(),
        "Float64" => data_type
            .style(Style::new().bright_blue())
            .bold()
            .to_string(),
        "Utf8" => data_type.style(Style::new().yellow()).to_string(),
        "Utf8View" => data_type.style(Style::new().bright_yellow()).to_string(),
        "LargeUtf8" => data_type.style(Style::new().yellow()).bold().to_string(),
        "Boolean" => data_type.style(Style::new().white()).bold().to_string(),
        "Date32" => data_type.style(Style::new().magenta()).to_string(),
        "Date64" => data_type.style(Style::new().bright_magenta()).to_string(),
        "Binary" => data_type.style(Style::new().red()).to_string(),
        "BinaryView" => data_type.style(Style::new().bright_red()).to_string(),
        "LargeBinary" => data_type.style(Style::new().red()).bold().to_string(),
        "Null" => data_type.style(Style::new().dimmed()).to_string(),
        value if value.starts_with("Decimal128") => value.style(Style::new().blue()).to_string(),
        value if value.starts_with("Decimal256") => {
            value.style(Style::new().bright_blue()).to_string()
        }
        value if value.starts_with("Time32") => value.style(Style::new().magenta()).to_string(),
        value if value.starts_with("Time64") => {
            value.style(Style::new().bright_magenta()).to_string()
        }
        value if value.starts_with("Timestamp") => value
            .style(Style::new().bright_magenta())
            .bold()
            .to_string(),
        value if value.starts_with("Duration") => {
            value.style(Style::new().magenta()).dimmed().to_string()
        }
        value if value.starts_with("Interval") => value
            .style(Style::new().bright_magenta())
            .dimmed()
            .to_string(),
        value if value.starts_with("FixedSizeBinary") => {
            value.style(Style::new().bright_red()).dimmed().to_string()
        }
        value if value.starts_with("List") => value.style(Style::new().white()).to_string(),
        value if value.starts_with("LargeList") => {
            value.style(Style::new().bright_white()).to_string()
        }
        value if value.starts_with("FixedSizeList") => {
            value.style(Style::new().white()).dimmed().to_string()
        }
        value if value.starts_with("Struct") => {
            value.style(Style::new().white()).bold().to_string()
        }
        value if value.starts_with("Map") => {
            value.style(Style::new().bright_white()).bold().to_string()
        }
        value if value.starts_with("Union") => {
            value.style(Style::new().white()).italic().to_string()
        }
        value if value.starts_with("Dictionary") => value
            .style(Style::new().bright_white())
            .italic()
            .to_string(),
        _ => data_type.to_owned(),
    }
}

#[derive(Tabled)]
struct SchemaFieldRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Type")]
    data_type: String,
    #[tabled(rename = "Nullable")]
    nullable: String,
}

/// Renders a compact schema table.
pub fn render_schema_fields(schema: &SchemaRef, output: &mut dyn Write) -> Result<()> {
    let rows = schema.fields().iter().map(|field| SchemaFieldRow {
        name: field.name().clone(),
        data_type: data_type(&field.data_type().to_string()),
        nullable: boolean_display(field.is_nullable()),
    });
    let mut table = Table::new(rows);
    apply_theme(&mut table);
    table.with(Modify::new(Columns::new(2..)).with(Alignment::center()));
    writeln!(output, "{table}")?;
    Ok(())
}

/// Renders schema fields and their metadata as a detail list.
pub fn render_schema_fields_detailed(schema: &SchemaRef, output: &mut dyn Write) -> Result<()> {
    for field in schema.fields() {
        let nullability = if field.is_nullable() {
            "nullable"
        } else {
            "not null"
        };
        writeln!(
            output,
            "  {} {}",
            header(field.name()),
            dim(format!("({nullability})"))
        )?;
        writeln!(
            output,
            "    {}: {}",
            label("Type"),
            data_type(&field.data_type().to_string())
        )?;
        if field.metadata().is_empty() {
            writeln!(output, "    {}: {}", label("Metadata"), dim("(none)"))?;
        } else {
            writeln!(output, "    {}:", label("Metadata"))?;
            for (key, value) in field.metadata() {
                writeln!(output, "      {}: {}", dim(key), value)?;
            }
        }
    }
    Ok(())
}

/// Renders key-value metadata under a section heading.
pub fn render_metadata_map(
    output: &mut dyn Write,
    heading: &str,
    metadata: &HashMap<String, String>,
) -> Result<()> {
    writeln!(output, "\n{}:", header(heading))?;
    if metadata.is_empty() {
        writeln!(output, "  {}", dim("(none)"))?;
    } else {
        for (key, value) in metadata {
            writeln!(output, "  {}: {}", label(key), truncate_for_display(value))?;
        }
    }
    Ok(())
}

/// A stable JSON representation of one Arrow field.
#[derive(Serialize)]
pub struct SchemaField {
    name: String,
    data_type: String,
    nullable: bool,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

/// Converts an Arrow schema into the shared JSON representation.
pub fn schema_json(schema: &SchemaRef) -> Vec<SchemaField> {
    schema
        .fields()
        .iter()
        .map(|field| SchemaField {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
            metadata: field.metadata().clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn formatting_uses_binary_sizes_and_grouped_numbers() {
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_number(1_000_000), "1,000,000");
    }

    #[test]
    fn metadata_truncation_counts_unicode_characters() {
        let value = "🎉".repeat(150);
        let rendered = truncate_for_display(&value);

        assert!(rendered.starts_with(&"🎉".repeat(100)));
        assert!(rendered.ends_with("... (150 chars total)"));
    }

    #[test]
    fn schema_text_and_json_share_field_identity() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let mut text = Vec::new();
        render_schema_fields(&schema, &mut text).unwrap();
        let text = String::from_utf8(text).unwrap();
        let json = serde_json::to_value(schema_json(&schema)).unwrap();

        assert!(text.contains("value"));
        assert_eq!(json[0]["name"], "value");
        assert_eq!(json[0]["data_type"], "Int64");
        assert_eq!(json[0]["nullable"], true);
    }
}
