use super::OutputFormatter;
use crate::inspect::FileInfo;
use anyhow::Result;
use arrow::datatypes::DataType;

pub struct TextFormatter;

impl OutputFormatter for TextFormatter {
    fn format(&self, info: &FileInfo) -> Result<String> {
        let mut output = String::new();

        output.push_str(&format!("File: {}\n", info.path.display()));
        output.push_str(&format!("Format: {}\n", info.format));

        if let Some(num_rows) = info.num_rows {
            output.push_str(&format!("Rows: {}\n", format_number(num_rows)));
        }

        output.push('\n');

        output.push_str(&format!(
            "Schema ({} fields):\n",
            info.schema.fields().len()
        ));

        let max_name_width = info
            .schema
            .fields()
            .iter()
            .map(|f| f.name().len())
            .max()
            .unwrap_or(0);

        for field in info.schema.fields() {
            let nullable_str = if field.is_nullable() {
                "(nullable)"
            } else {
                "(not null)"
            };
            output.push_str(&format!(
                "  {:<width$}  {}  {}\n",
                field.name(),
                format_data_type(field.data_type()),
                nullable_str,
                width = max_name_width
            ));
        }

        Ok(output)
    }
}

fn format_number(n: i64) -> String {
    if n == 0 {
        return "0".to_string();
    }

    let negative = n < 0;
    let abs_n = n.unsigned_abs();
    let s = abs_n.to_string();
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::with_capacity(s.len() + s.len() / 3 + 1);

    for (i, ch) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(*ch);
    }

    if negative {
        format!("-{}", result)
    } else {
        result
    }
}

fn format_data_type(data_type: &DataType) -> String {
    match data_type {
        DataType::List(field) => format!("List<{}>", format_data_type(field.data_type())),
        DataType::LargeList(field) => format!("LargeList<{}>", format_data_type(field.data_type())),
        DataType::Struct(fields) => {
            let field_types = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), format_data_type(f.data_type())))
                .collect::<Vec<_>>()
                .join(", ");
            format!("Struct<{}>", field_types)
        }
        DataType::Map(field, sorted) => {
            format!(
                "Map<{}, sorted={}>",
                format_data_type(field.data_type()),
                sorted
            )
        }
        DataType::Decimal128(precision, scale) => format!("Decimal128({}, {})", precision, scale),
        DataType::Decimal256(precision, scale) => format!("Decimal256({}, {})", precision, scale),
        DataType::Timestamp(unit, tz) => format!("Timestamp({:?}, {:?})", unit, tz),
        _ => format!("{:?}", data_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(123), "123");
        assert_eq!(format_number(1234), "1,234");
        assert_eq!(format_number(1234567), "1,234,567");
        assert_eq!(format_number(-1234), "-1,234");
    }
}
