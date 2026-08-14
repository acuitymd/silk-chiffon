use std::{collections::HashMap, fmt::Display, io::Write};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use humansize::{BINARY, FormatSizeOptions, format_size};
use num_format::{Locale, ToFormattedString};
use owo_colors::{OwoColorize, Style};
use serde::Serialize;
use tabled::{
    Table, Tabled,
    settings::{Alignment, Color, Modify, Style as TableStyle, object::Columns, object::Rows},
};

pub(crate) fn format_bytes(bytes: u64) -> String {
    format_size(bytes, FormatSizeOptions::from(BINARY).decimal_places(1))
}

pub(crate) fn format_number(number: u64) -> String {
    number.to_formatted_string(&Locale::en)
}

pub(crate) fn truncate_chars(value: &str, max_chars: usize) -> &str {
    match value.char_indices().nth(max_chars) {
        Some((index, _)) => &value[..index],
        None => value,
    }
}

pub(crate) fn header(value: impl Display) -> String {
    value.style(Style::new().bold()).to_string()
}

pub(crate) fn dim(value: impl Display) -> String {
    value.style(Style::new().dimmed()).to_string()
}

fn data_type(value: &str) -> String {
    match value {
        "Int32" => value.style(Style::new().cyan()).to_string(),
        "Int64" => value.style(Style::new().bright_cyan()).to_string(),
        "Utf8" => value.style(Style::new().yellow()).to_string(),
        "Boolean" => value.style(Style::new().white()).bold().to_string(),
        "Null" => value.style(Style::new().dimmed()).to_string(),
        _ => value.to_owned(),
    }
}

fn boolean_display(value: bool) -> String {
    if value {
        "■".style(Style::new().green()).to_string()
    } else {
        "□".style(Style::new().dimmed()).to_string()
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

pub(crate) fn render_schema_fields(schema: &SchemaRef, output: &mut dyn Write) -> Result<()> {
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

#[derive(Serialize)]
pub(crate) struct SchemaField {
    name: String,
    data_type: String,
    nullable: bool,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

pub(crate) fn schema_json(schema: &SchemaRef) -> Vec<SchemaField> {
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

pub(crate) fn apply_theme(table: &mut Table) {
    table
        .with(TableStyle::rounded())
        .modify(Rows::first(), Alignment::center())
        .modify(Rows::first(), Color::BOLD);
}
