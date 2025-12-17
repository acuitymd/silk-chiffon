//! Output styling for inspect commands.

use std::fmt::Display;

use owo_colors::{OwoColorize, Style};
use tabled::{
    Table,
    settings::{Alignment, Modify, Style as TableStyle, object::Rows},
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

/// Create a table with rounded borders from data.
pub fn rounded_table<T, I>(data: I) -> Table
where
    T: tabled::Tabled,
    I: IntoIterator<Item = T>,
{
    let mut table = Table::new(data);
    table
        .with(TableStyle::rounded())
        .with(Modify::new(Rows::first()).with(Alignment::center()));
    table
}
