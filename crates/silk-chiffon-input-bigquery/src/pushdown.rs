//! Exact DataFusion-expression translation into Storage Read row restrictions.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use datafusion::{
    common::{ScalarValue, TableReference},
    logical_expr::{Expr, Operator, TableProviderFilterPushDown},
};

pub(crate) fn support(schema: &Schema, expression: &Expr) -> TableProviderFilterPushDown {
    if translate(schema, expression).is_some() {
        TableProviderFilterPushDown::Exact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

pub(crate) fn row_restriction(
    schema: &Schema,
    filters: &[Expr],
    explicit: Option<&str>,
) -> Option<String> {
    let mut restrictions = filters
        .iter()
        .filter_map(|filter| translate(schema, filter))
        .map(|filter| format!("({filter})"))
        .collect::<Vec<_>>();
    if let Some(explicit) = explicit.filter(|value| !value.is_empty()) {
        restrictions.push(format!("({explicit})"));
    }
    (!restrictions.is_empty()).then(|| restrictions.join(" AND "))
}

fn translate(schema: &Schema, expression: &Expr) -> Option<String> {
    match expression {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            let left = translate(schema, &binary.left)?;
            let right = translate(schema, &binary.right)?;
            let operator = if binary.op == Operator::And {
                "AND"
            } else {
                "OR"
            };
            Some(format!("({left} {operator} {right})"))
        }
        Expr::BinaryExpr(binary) => {
            translate_comparison(schema, &binary.left, binary.op, &binary.right)
        }
        Expr::Not(child) => Some(format!("(NOT {})", translate(schema, child)?)),
        Expr::IsNull(child) => {
            let column = resolve_column(schema, child)?;
            Some(format!("{} IS NULL", column.sql))
        }
        Expr::IsNotNull(child) => {
            let column = resolve_column(schema, child)?;
            Some(format!("{} IS NOT NULL", column.sql))
        }
        Expr::InList(list) if !list.list.is_empty() => {
            let column = resolve_column(schema, &list.expr)?;
            let values = list
                .list
                .iter()
                .map(|value| literal(value, &column.data_type))
                .collect::<Option<Vec<_>>>()?;
            let negation = if list.negated { " NOT" } else { "" };
            Some(format!(
                "{}{negation} IN ({})",
                column.sql,
                values.join(", ")
            ))
        }
        Expr::Between(between) => {
            let column = resolve_column(schema, &between.expr)?;
            let low = literal(&between.low, &column.data_type)?;
            let high = literal(&between.high, &column.data_type)?;
            let negation = if between.negated { " NOT" } else { "" };
            Some(format!("{}{negation} BETWEEN {low} AND {high}", column.sql))
        }
        _ => None,
    }
}

fn translate_comparison(
    schema: &Schema,
    left: &Expr,
    operator: Operator,
    right: &Expr,
) -> Option<String> {
    let symbol = comparison_symbol(operator)?;
    if let Some(column) = resolve_column(schema, left) {
        return Some(format!(
            "{} {symbol} {}",
            column.sql,
            literal(right, &column.data_type)?
        ));
    }
    let column = resolve_column(schema, right)?;
    let symbol = comparison_symbol(reverse_comparison(operator)?)?;
    Some(format!(
        "{} {symbol} {}",
        column.sql,
        literal(left, &column.data_type)?
    ))
}

fn comparison_symbol(operator: Operator) -> Option<&'static str> {
    match operator {
        Operator::Eq => Some("="),
        Operator::NotEq => Some("!="),
        Operator::Lt => Some("<"),
        Operator::LtEq => Some("<="),
        Operator::Gt => Some(">"),
        Operator::GtEq => Some(">="),
        _ => None,
    }
}

fn reverse_comparison(operator: Operator) -> Option<Operator> {
    match operator {
        Operator::Eq | Operator::NotEq => Some(operator),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

struct ResolvedColumn {
    sql: String,
    data_type: DataType,
}

fn resolve_column(schema: &Schema, expression: &Expr) -> Option<ResolvedColumn> {
    let Expr::Column(column) = expression else {
        return None;
    };
    if let Ok((_, field)) = schema.column_with_name(&column.name).ok_or(()) {
        return supported_field(field).then(|| ResolvedColumn {
            sql: quote_identifier(&column.name),
            data_type: field.data_type().clone(),
        });
    }

    if let Some(TableReference::Bare { table }) = &column.relation {
        let qualified = format!("{table}.{}", column.name);
        if let Some(column) = resolve_column_path(schema, &qualified) {
            return Some(column);
        }
    }

    resolve_column_path(schema, &column.name)
}

fn resolve_column_path(schema: &Schema, path: &str) -> Option<ResolvedColumn> {
    if let Some((_, field)) = schema.column_with_name(path) {
        return supported_field(field).then(|| ResolvedColumn {
            sql: quote_identifier(path),
            data_type: field.data_type().clone(),
        });
    }
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut field = schema.field_with_name(first).ok()?;
    let mut quoted = vec![quote_identifier(first)];
    for part in parts {
        let DataType::Struct(fields) = field.data_type() else {
            return None;
        };
        field = fields.iter().find(|field| field.name() == part)?;
        quoted.push(quote_identifier(part));
    }
    supported_field(field).then(|| ResolvedColumn {
        sql: quoted.join("."),
        data_type: field.data_type().clone(),
    })
}

fn supported_field(field: &Field) -> bool {
    let unsupported_logical_type = field.metadata().iter().any(|(name, value)| {
        let name = name.to_ascii_uppercase();
        let value = value.to_ascii_uppercase();
        (name.contains("LOGICAL") || name.contains("EXTENSION"))
            && (value.contains("GEOGRAPHY") || value.contains("JSON"))
    });
    !unsupported_logical_type
        && matches!(
            field.data_type(),
            DataType::Boolean
                | DataType::Int64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Binary
                | DataType::Date32
                | DataType::Time64(TimeUnit::Microsecond)
                | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('\\', "\\\\").replace('`', "\\`"))
}

fn literal(expression: &Expr, expected: &DataType) -> Option<String> {
    let Expr::Literal(value, _) = expression else {
        return None;
    };
    match (expected, value) {
        (DataType::Boolean, ScalarValue::Boolean(Some(value))) => {
            Some(if *value { "TRUE" } else { "FALSE" }.to_owned())
        }
        (DataType::Int64, value) => integer_literal(value),
        (DataType::Float64, ScalarValue::Float64(Some(value))) if value.is_finite() => {
            Some(value.to_string())
        }
        (DataType::Utf8, ScalarValue::Utf8(Some(value)))
        | (DataType::Utf8, ScalarValue::Utf8View(Some(value)))
        | (DataType::Utf8, ScalarValue::LargeUtf8(Some(value))) => string_literal(value),
        (DataType::Binary, ScalarValue::Binary(Some(value)))
        | (DataType::Binary, ScalarValue::BinaryView(Some(value)))
        | (DataType::Binary, ScalarValue::LargeBinary(Some(value))) => Some(format!(
            "b'{}'",
            value
                .iter()
                .map(|byte| format!("\\x{byte:02x}"))
                .collect::<String>()
        )),
        (DataType::Date32, ScalarValue::Date32(Some(days))) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)?
                .checked_add_signed(Duration::days(i64::from(*days)))?;
            Some(format!("DATE '{date}'"))
        }
        (DataType::Time64(TimeUnit::Microsecond), ScalarValue::Time64Microsecond(Some(value))) => {
            time_literal(*value)
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, timezone),
            ScalarValue::TimestampMicrosecond(Some(value), literal_timezone),
        ) if equivalent_timezone(timezone.as_deref(), literal_timezone.as_deref()) => {
            timestamp_literal(*value, 1_000, timezone.as_deref())
        }
        (
            DataType::Timestamp(TimeUnit::Nanosecond, timezone),
            ScalarValue::TimestampNanosecond(Some(value), literal_timezone),
        ) if equivalent_timezone(timezone.as_deref(), literal_timezone.as_deref()) => {
            timestamp_literal(*value, 1, timezone.as_deref())
        }
        (
            DataType::Decimal128(precision, scale),
            ScalarValue::Decimal128(Some(value), literal_precision, literal_scale),
        ) if precision == literal_precision && scale == literal_scale => {
            Some(format!("NUMERIC '{}'", scaled_decimal(*value, *scale)?))
        }
        (
            DataType::Decimal256(precision, scale),
            ScalarValue::Decimal256(Some(value), literal_precision, literal_scale),
        ) if precision == literal_precision && scale == literal_scale => Some(format!(
            "BIGNUMERIC '{}'",
            scaled_decimal_string(value.to_string(), *scale)?
        )),
        _ => None,
    }
}

fn integer_literal(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(value.to_string()),
        ScalarValue::Int16(Some(value)) => Some(value.to_string()),
        ScalarValue::Int32(Some(value)) => Some(value.to_string()),
        ScalarValue::Int64(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt8(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt16(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt32(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt64(Some(value)) if i64::try_from(*value).is_ok() => {
            Some(value.to_string())
        }
        _ => None,
    }
}

fn string_literal(value: &str) -> Option<String> {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('\'');
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\'' => escaped.push_str("\\'"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => return None,
            character => escaped.push(character),
        }
    }
    escaped.push('\'');
    Some(escaped)
}

fn time_literal(micros: i64) -> Option<String> {
    if !(0..86_400_000_000).contains(&micros) {
        return None;
    }
    let seconds = u32::try_from(micros / 1_000_000).ok()?;
    let nanos = u32::try_from((micros % 1_000_000) * 1_000).ok()?;
    let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos)?;
    Some(format!("TIME '{}'", time.format("%H:%M:%S%.6f")))
}

fn timestamp_literal(value: i64, nanos_per_unit: i64, timezone: Option<&str>) -> Option<String> {
    let total_nanos = i128::from(value).checked_mul(i128::from(nanos_per_unit))?;
    let seconds = i64::try_from(total_nanos.div_euclid(1_000_000_000)).ok()?;
    let nanos = u32::try_from(total_nanos.rem_euclid(1_000_000_000)).ok()?;
    let timestamp = DateTime::<Utc>::from_timestamp(seconds, nanos)?.naive_utc();
    let formatted = timestamp.format("%Y-%m-%d %H:%M:%S%.f");
    match timezone {
        None => Some(format!("DATETIME '{formatted}'")),
        Some("UTC" | "+00:00") => Some(format!("TIMESTAMP '{formatted}+00'")),
        Some(_) => None,
    }
}

fn equivalent_timezone(expected: Option<&str>, actual: Option<&str>) -> bool {
    expected == actual
        || matches!(
            (expected, actual),
            (Some("UTC"), Some("+00:00")) | (Some("+00:00"), Some("UTC"))
        )
}

fn scaled_decimal(value: i128, scale: i8) -> Option<String> {
    scaled_decimal_string(value.to_string(), scale)
}

fn scaled_decimal_string(value: String, scale: i8) -> Option<String> {
    let scale = usize::try_from(scale).ok()?;
    if scale == 0 {
        return Some(value);
    }
    let (negative, digits) = value
        .strip_prefix('-')
        .map_or((false, value.as_str()), |digits| (true, digits));
    let mut padded = String::new();
    if digits.len() <= scale {
        padded.extend(std::iter::repeat_n('0', scale + 1 - digits.len()));
    }
    padded.push_str(digits);
    let point = padded.len() - scale;
    padded.insert(point, '.');
    if negative {
        padded.insert(0, '-');
    }
    Some(padded)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{Fields, i256};
    use datafusion::logical_expr::{Between, BinaryExpr, col, expr::InList, lit};

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("day", DataType::Date32, true),
            Field::new("clock", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new(
                "created",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            ),
            Field::new("amount", DataType::Decimal128(38, 9), true),
            Field::new("huge", DataType::Decimal256(76, 38), true),
            Field::new(
                "nested",
                DataType::Struct(Fields::from(vec![Field::new(
                    "value",
                    DataType::Int64,
                    true,
                )])),
                true,
            ),
            Field::new("odd`name", DataType::Utf8, true),
            Field::new("a.b", DataType::Int64, true),
            Field::new("geo", DataType::Utf8, true).with_metadata(HashMap::from([(
                "logical_type".to_owned(),
                "GEOGRAPHY".to_owned(),
            )])),
        ])
    }

    #[test]
    fn translates_comparisons_boolean_trees_and_null_checks_exactly() {
        let filter = col("id")
            .gt(lit(3_i64))
            .and(col("name").eq(lit("O'Reilly\\docs")))
            .or(Expr::IsNull(Box::new(col("nested.value"))));
        assert_eq!(
            support(&schema(), &filter),
            TableProviderFilterPushDown::Exact
        );
        assert_eq!(
            translate(&schema(), &filter).unwrap(),
            "((`id` > 3 AND `name` = 'O\\'Reilly\\\\docs') OR `nested`.`value` IS NULL)"
        );

        let reversed = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(5_i64)),
            Operator::Lt,
            Box::new(col("id")),
        ));
        assert_eq!(translate(&schema(), &reversed).unwrap(), "`id` > 5");
    }

    #[test]
    fn translates_in_between_decimal_and_temporal_literals() {
        let in_list = Expr::InList(InList::new(
            Box::new(col("id")),
            vec![lit(1_i64), lit(2_i64)],
            false,
        ));
        let between = Expr::Between(Between::new(
            Box::new(col("day")),
            false,
            Box::new(Expr::Literal(ScalarValue::Date32(Some(0)), None)),
            Box::new(Expr::Literal(ScalarValue::Date32(Some(1)), None)),
        ));
        assert_eq!(translate(&schema(), &in_list).unwrap(), "`id` IN (1, 2)");
        assert_eq!(
            translate(&schema(), &between).unwrap(),
            "`day` BETWEEN DATE '1970-01-01' AND DATE '1970-01-02'"
        );

        let decimal = col("amount").eq(Expr::Literal(
            ScalarValue::Decimal128(Some(-123_456_789), 38, 9),
            None,
        ));
        let huge = col("huge").eq(Expr::Literal(
            ScalarValue::Decimal256(Some(i256::from_i128(1)), 76, 38),
            None,
        ));
        assert_eq!(
            translate(&schema(), &decimal).unwrap(),
            "`amount` = NUMERIC '-0.123456789'"
        );
        assert_eq!(
            translate(&schema(), &huge).unwrap(),
            "`huge` = BIGNUMERIC '0.00000000000000000000000000000000000001'"
        );
    }

    #[test]
    fn unsupported_child_makes_the_entire_boolean_expression_unsupported() {
        let unsupported = col("id")
            .eq(lit(1_i64))
            .or((col("id") + lit(2_i64)).eq(lit(3_i64)));
        assert_eq!(
            support(&schema(), &unsupported),
            TableProviderFilterPushDown::Unsupported
        );
        assert!(translate(&schema(), &unsupported).is_none());
        assert!(
            translate(
                &schema(),
                &col("ratio").eq(Expr::Literal(ScalarValue::Float64(Some(f64::NAN)), None,)),
            )
            .is_none()
        );
        assert!(translate(&schema(), &col("geo").eq(lit("POINT(0 0)"))).is_none());
    }

    #[test]
    fn rejects_non_finite_null_and_mismatched_literals() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                translate(
                    &schema(),
                    &col("ratio").eq(Expr::Literal(ScalarValue::Float64(Some(value)), None,)),
                )
                .is_none()
            );
        }
        assert!(
            translate(
                &schema(),
                &col("id").eq(Expr::Literal(ScalarValue::Int64(None), None)),
            )
            .is_none()
        );
        assert!(
            translate(
                &schema(),
                &col("amount").eq(Expr::Literal(ScalarValue::Decimal128(Some(1), 20, 2), None,)),
            )
            .is_none()
        );
        assert!(translate(&schema(), &col("id").eq(lit("1"))).is_none());
    }

    #[test]
    fn temporal_translation_requires_exact_bounds_and_timezones() {
        let valid_time = col("clock").eq(Expr::Literal(
            ScalarValue::Time64Microsecond(Some(86_399_999_999)),
            None,
        ));
        assert_eq!(
            translate(&schema(), &valid_time).unwrap(),
            "`clock` = TIME '23:59:59.999999'"
        );
        let invalid_time = col("clock").eq(Expr::Literal(
            ScalarValue::Time64Microsecond(Some(86_400_000_000)),
            None,
        ));
        assert!(translate(&schema(), &invalid_time).is_none());

        let utc = col("created").eq(Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(0), Some(Arc::from("+00:00"))),
            None,
        ));
        assert_eq!(
            translate(&schema(), &utc).unwrap(),
            "`created` = TIMESTAMP '1970-01-01 00:00:00+00'"
        );
        let mismatched = col("created").eq(Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(0), Some(Arc::from("America/Chicago"))),
            None,
        ));
        assert!(translate(&schema(), &mismatched).is_none());
    }

    #[test]
    fn rejects_ambiguous_lists_controls_and_partially_supported_not() {
        let empty = Expr::InList(InList::new(Box::new(col("id")), Vec::new(), false));
        assert!(translate(&schema(), &empty).is_none());
        let mixed = Expr::InList(InList::new(
            Box::new(col("id")),
            vec![lit(1_i64), lit("2")],
            false,
        ));
        assert!(translate(&schema(), &mixed).is_none());
        assert!(translate(&schema(), &col("name").eq(lit("bad\u{7}value"))).is_none());

        let exact_not = Expr::Not(Box::new(col("id").eq(lit(1_i64))));
        assert_eq!(translate(&schema(), &exact_not).unwrap(), "(NOT `id` = 1)");
        let unsupported_not = Expr::Not(Box::new((col("id") + lit(1_i64)).eq(lit(2_i64))));
        assert!(translate(&schema(), &unsupported_not).is_none());
    }

    #[test]
    fn quotes_exact_top_level_and_nested_identifiers_without_ambiguity() {
        assert_eq!(
            translate(&schema(), &col("odd`name").eq(lit("value"))).unwrap(),
            "`odd\\`name` = 'value'"
        );
        assert_eq!(
            translate(&schema(), &col("a.b").eq(lit(1_i64))).unwrap(),
            "`a.b` = 1"
        );
        assert_eq!(
            translate(&schema(), &col("nested.value").eq(lit(1_i64))).unwrap(),
            "`nested`.`value` = 1"
        );
    }

    #[test]
    fn explicit_restriction_is_parenthesized_and_anded_with_exact_filters() {
        let filters = [col("id").gt(lit(10_i64))];
        assert_eq!(
            row_restriction(&schema(), &filters, Some("tenant_id = 7")).unwrap(),
            "(`id` > 10) AND (tenant_id = 7)"
        );
        assert_eq!(
            row_restriction(&schema(), &[], Some("tenant_id = 7")).unwrap(),
            "(tenant_id = 7)"
        );
    }
}
