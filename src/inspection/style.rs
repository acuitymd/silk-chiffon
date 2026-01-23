//! Output styling for inspect commands.

use std::fmt::Display;

use owo_colors::{OwoColorize, Style};
use tabled::{
    Table,
    settings::{Alignment, Color, Style as TableStyle, object::Rows},
};

/// Styles for output text.
pub struct Styles;

impl Styles {
    pub fn header() -> Style {
        Style::new().bold()
    }

    pub fn label() -> Style {
        Style::new().cyan()
    }

    pub fn value() -> Style {
        Style::new().green()
    }

    pub fn dim() -> Style {
        Style::new().dimmed()
    }
}

/// Format a value with label styling.
pub fn label<T: Display>(value: T) -> String {
    value.style(Styles::label()).to_string()
}

/// Format a value with value styling.
pub fn value<T: Display>(v: T) -> String {
    v.style(Styles::value()).to_string()
}

/// Format a value with header styling.
pub fn header<T: Display>(v: T) -> String {
    v.style(Styles::header()).to_string()
}

/// Format a value with dim styling.
pub fn dim<T: Display>(v: T) -> String {
    v.style(Styles::dim()).to_string()
}

/// Format a column name.
pub fn column_name<T: Display>(v: T) -> String {
    v.to_string()
}

/// Format compression type with color based on algorithm.
pub fn compression(codec: &str) -> String {
    match codec {
        "UNCOMPRESSED" => codec.style(Style::new().dimmed()).to_string(),
        "SNAPPY" => codec.style(Style::new().yellow()).to_string(),
        "GZIP" => codec.style(Style::new().blue()).to_string(),
        "LZO" => codec.style(Style::new().magenta()).to_string(),
        "BROTLI" => codec.style(Style::new().cyan()).to_string(),
        "LZ4" | "LZ4_RAW" => codec.style(Style::new().blue()).to_string(),
        "ZSTD" => codec.style(Style::new().green()).to_string(),
        _ => codec.to_string(),
    }
}

/// Format encoding type with color based on encoding family.
pub fn encoding(enc: &str) -> String {
    match enc {
        "RLE_DICTIONARY" | "PLAIN_DICTIONARY" => enc.style(Style::new().yellow()).to_string(),
        "RLE" | "BIT_PACKED" => enc.style(Style::new().blue()).to_string(),
        "BYTE_STREAM_SPLIT" => enc.style(Style::new().cyan()).to_string(),
        e if e.starts_with("DELTA") => enc.style(Style::new().magenta()).to_string(),
        _ => enc.to_string(),
    }
}

/// Format data type with color based on type family.
pub fn data_type(dt: &str) -> String {
    match dt {
        "Int8" => dt.style(Style::new().bright_green()).to_string(),
        "Int16" => dt.style(Style::new().green()).to_string(),
        "Int32" => dt.style(Style::new().cyan()).to_string(),
        "Int64" => dt.style(Style::new().bright_cyan()).to_string(),
        "UInt8" => dt.style(Style::new().bright_green()).bold().to_string(),
        "UInt16" => dt.style(Style::new().green()).bold().to_string(),
        "UInt32" => dt.style(Style::new().cyan()).bold().to_string(),
        "UInt64" => dt.style(Style::new().bright_cyan()).bold().to_string(),
        "Float16" => dt.style(Style::new().bright_blue()).to_string(),
        "Float32" => dt.style(Style::new().blue()).to_string(),
        "Float64" => dt.style(Style::new().bright_blue()).bold().to_string(),
        "Utf8" => dt.style(Style::new().yellow()).to_string(),
        "Utf8View" => dt.style(Style::new().bright_yellow()).to_string(),
        "LargeUtf8" => dt.style(Style::new().yellow()).bold().to_string(),
        "Boolean" => dt.style(Style::new().white()).bold().to_string(),
        "Date32" => dt.style(Style::new().magenta()).to_string(),
        "Date64" => dt.style(Style::new().bright_magenta()).to_string(),
        "Binary" => dt.style(Style::new().red()).to_string(),
        "BinaryView" => dt.style(Style::new().bright_red()).to_string(),
        "LargeBinary" => dt.style(Style::new().red()).bold().to_string(),
        "Null" => dt.style(Style::new().dimmed()).to_string(),
        _ if dt.starts_with("Decimal128") => dt.style(Style::new().blue()).to_string(),
        _ if dt.starts_with("Decimal256") => dt.style(Style::new().bright_blue()).to_string(),
        _ if dt.starts_with("Time32") => dt.style(Style::new().magenta()).to_string(),
        _ if dt.starts_with("Time64") => dt.style(Style::new().bright_magenta()).to_string(),
        _ if dt.starts_with("Timestamp") => {
            dt.style(Style::new().bright_magenta()).bold().to_string()
        }
        _ if dt.starts_with("Duration") => dt.style(Style::new().magenta()).dimmed().to_string(),
        _ if dt.starts_with("Interval") => {
            dt.style(Style::new().bright_magenta()).dimmed().to_string()
        }
        _ if dt.starts_with("FixedSizeBinary") => {
            dt.style(Style::new().bright_red()).dimmed().to_string()
        }
        _ if dt.starts_with("List") => dt.style(Style::new().white()).to_string(),
        _ if dt.starts_with("LargeList") => dt.style(Style::new().bright_white()).to_string(),
        _ if dt.starts_with("FixedSizeList") => dt.style(Style::new().white()).dimmed().to_string(),
        _ if dt.starts_with("Struct") => dt.style(Style::new().white()).bold().to_string(),
        _ if dt.starts_with("Map") => dt.style(Style::new().bright_white()).bold().to_string(),
        _ if dt.starts_with("Union") => dt.style(Style::new().white()).italic().to_string(),
        _ if dt.starts_with("Dictionary") => {
            dt.style(Style::new().bright_white()).italic().to_string()
        }
        _ => dt.to_string(),
    }
}

/// Filled circle for positive boolean indicators.
pub fn boolean_true() -> String {
    value("■")
}

/// Empty circle for negative boolean indicators.
pub fn boolean_false() -> String {
    dim("□")
}

/// Format a boolean for display using glyph styling.
pub fn boolean_display(b: bool) -> String {
    if b { boolean_true() } else { boolean_false() }
}

/// Format a boolean for display using glyph styling, or missing value if false.
pub fn true_or_missing_display(b: bool) -> String {
    if b { boolean_true() } else { missing_value() }
}

pub fn missing_value() -> String {
    dim("-")
}

/// Apply standard theme to a table: rounded borders with bold centered headers.
pub fn apply_theme(table: &mut Table) {
    table
        .with(TableStyle::rounded())
        .modify(Rows::first(), Alignment::center())
        .modify(Rows::first(), Color::BOLD);
}

/// Create a table with standard theme from data.
pub fn rounded_table<T, I>(data: I) -> Table
where
    T: tabled::Tabled,
    I: IntoIterator<Item = T>,
{
    let mut table = Table::new(data);
    apply_theme(&mut table);
    table
}
