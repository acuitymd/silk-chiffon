use std::collections::{BTreeSet, HashMap};

use anyhow::{Context, Result, anyhow, bail};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use minijinja::{AutoEscape, Environment, Value};
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, percent_encode};
use silk_chiffon_storage::{Location, LocationInput};
use url::Url;

use super::partitioner::PartitionValues;

const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

const HIVE_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'\'')
    .add(b'*')
    .add(b'/')
    .add(b':')
    .add(b'=')
    .add(b'?')
    .add(b'\\')
    .add(b'{')
    .add(b'[')
    .add(b']')
    .add(b'^');

enum OutputEnvelope {
    Bare { absolute: bool },
    Url { base: Url },
}

/// A partition output template whose routing envelope is fixed before execution.
pub struct OutputTargetTemplate {
    env: Environment<'static>,
    object_path_template: String,
    envelope: OutputEnvelope,
}

impl OutputTargetTemplate {
    pub fn new(pattern: impl Into<String>) -> Result<Self> {
        let pattern = pattern.into();
        let (envelope, object_path_template) = parse_envelope(&pattern)?;
        let protects_percent_encoding = matches!(envelope, OutputEnvelope::Url { .. });
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_name| AutoEscape::Custom("hive"));
        env.set_formatter(move |out, state, value| {
            if !matches!(state.auto_escape(), AutoEscape::Custom("hive")) {
                return minijinja::escape_formatter(out, state, value);
            }
            let formatted = if value.is_safe() {
                value.to_string()
            } else {
                hive_escape_path(&value.to_string())
            };
            if protects_percent_encoding {
                out.write_str(&formatted.replace('%', "%25"))?;
            } else {
                out.write_str(&formatted)?;
            }
            Ok(())
        });
        env.add_filter("raw", |value: &str| -> Result<Value, minijinja::Error> {
            let value = if value.is_empty() || value == HIVE_DEFAULT_PARTITION {
                HIVE_DEFAULT_PARTITION.to_owned()
            } else {
                value.to_owned()
            };
            Ok(Value::from_safe_string(value))
        });
        env.template_from_str(&object_path_template)
            .with_context(|| format!("invalid output target template {pattern:?}"))?;

        Ok(Self {
            env,
            object_path_template,
            envelope,
        })
    }

    pub fn referenced_fields(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .env
            .template_from_str(&self.object_path_template)?
            .undeclared_variables(false)
            .into_iter()
            .collect())
    }

    pub fn static_extension(&self) -> Option<&str> {
        let component = self.object_path_template.rsplit('/').next()?;
        let extension = component.rsplit_once('.')?.1;
        (!extension.is_empty()
            && !extension.contains("{{")
            && !extension.contains("{%")
            && !extension.contains("{#"))
        .then_some(extension)
    }

    pub fn require_file_number(&self) -> Result<()> {
        let mut depth = 0usize;
        let mut direct = false;
        for tag in template_tags(&self.object_path_template)? {
            match tag.kind {
                TagKind::Expression => {
                    if contains_identifier(tag.body, "file_number") {
                        if tag.body.trim() == "file_number" && depth == 0 {
                            direct = true;
                        } else {
                            bail!(
                                "nosort-evict requires file_number as a direct unconditional interpolation"
                            );
                        }
                    }
                }
                TagKind::Block => {
                    if contains_identifier(tag.body, "file_number") {
                        bail!("file_number cannot be shadowed or used in template control flow");
                    }
                    let keyword = tag.body.split_whitespace().next().unwrap_or_default();
                    if keyword.starts_with("end") {
                        depth = depth.saturating_sub(1);
                    } else if matches!(
                        keyword,
                        "if" | "for"
                            | "block"
                            | "filter"
                            | "macro"
                            | "with"
                            | "autoescape"
                            | "call"
                            | "raw"
                    ) || keyword == "set" && !tag.body.contains('=')
                    {
                        depth += 1;
                    }
                }
                TagKind::Comment => {}
            }
        }
        if !direct {
            bail!("nosort-evict output template must directly interpolate {{ file_number }}");
        }
        Ok(())
    }

    pub fn render(
        &self,
        values: &PartitionValues,
        file_number: Option<usize>,
    ) -> Result<LocationInput> {
        if values.contains_key("file_number") {
            bail!("partition field file_number is reserved for nosort-evict output templates");
        }

        let mut context = HashMap::new();
        for (column, value) in values {
            let formatter = ArrayFormatter::try_new(
                value,
                &FormatOptions::default().with_null(HIVE_DEFAULT_PARTITION),
            )?;
            context.insert(column.as_str(), Value::from(formatter.value(0).to_string()));
        }
        if let Some(file_number) = file_number {
            context.insert("file_number", Value::from(file_number));
        }

        let rendered = self
            .env
            .template_from_str(&self.object_path_template)?
            .render(context)
            .context("failed to render output target template")?;
        let object_path = match self.envelope {
            OutputEnvelope::Url { .. } => percent_decode_str(&rendered)
                .decode_utf8()
                .context("rendered output object path is not UTF-8")?
                .into_owned(),
            OutputEnvelope::Bare { .. } => rendered,
        };
        validate_object_path(&object_path)?;

        match &self.envelope {
            OutputEnvelope::Bare { absolute } => {
                let prefix = if *absolute { "/" } else { "" };
                Ok(LocationInput::Bare(format!("{prefix}{object_path}")))
            }
            OutputEnvelope::Url { base } => {
                let mut target = base.clone();
                {
                    let mut segments = target
                        .path_segments_mut()
                        .map_err(|()| anyhow!("output URL cannot contain path segments"))?;
                    segments.clear();
                    segments.extend(object_path.split('/'));
                }
                Ok(Location::parse_url(target.as_str())?.into())
            }
        }
    }
}

fn parse_envelope(pattern: &str) -> Result<(OutputEnvelope, String)> {
    if let Some(separator) = pattern.find("://") {
        if pattern.contains('#') {
            bail!("output target templates cannot contain URL fragments");
        }
        let path_start = pattern[separator + 3..]
            .find('/')
            .map(|offset| separator + 3 + offset)
            .ok_or_else(|| anyhow!("explicit output target template requires an object path"))?;
        let query_start = pattern.find('?').unwrap_or(pattern.len());
        if query_start < path_start {
            bail!("output target template query cannot appear before its object path");
        }
        let fixed_prefix = &pattern[..=path_start];
        let object_path_template = &pattern[path_start + 1..query_start];
        let fixed_query = &pattern[query_start..];
        if fixed_prefix.contains(['{', '}']) || fixed_query.contains(['{', '}']) {
            bail!("only the output object path may contain template syntax");
        }
        if object_path_template.is_empty() {
            bail!("output target template object path cannot be empty");
        }

        let masked_path = mask_template_tags(object_path_template)?;
        let candidate = format!("{fixed_prefix}{masked_path}{fixed_query}");
        let LocationInput::Url(location) = LocationInput::parse(&candidate)? else {
            bail!("explicit output target template did not retain its URL envelope");
        };
        let mut base = location.url().clone();
        base.set_path("/");
        Ok((
            OutputEnvelope::Url { base },
            object_path_template.to_owned(),
        ))
    } else {
        let absolute = pattern.starts_with('/');
        let object_path_template = pattern.strip_prefix('/').unwrap_or(pattern);
        if object_path_template.is_empty() {
            bail!("output target template object path cannot be empty");
        }
        Ok((
            OutputEnvelope::Bare { absolute },
            object_path_template.to_owned(),
        ))
    }
}

fn mask_template_tags(source: &str) -> Result<String> {
    let mut output = String::new();
    let mut cursor = 0usize;
    for tag in template_tags(source)? {
        output.push_str(&source[cursor..tag.start]);
        output.push('x');
        cursor = tag.end;
    }
    output.push_str(&source[cursor..]);
    Ok(output)
}

fn validate_object_path(path: &str) -> Result<()> {
    if path.is_empty() {
        bail!("rendered output object path cannot be empty");
    }
    for segment in path.split('/') {
        if segment.is_empty() {
            bail!("rendered output object path cannot contain empty segments");
        }
        if matches!(segment, "." | "..") {
            bail!("rendered output object path cannot contain dot segments");
        }
    }
    Ok(())
}

fn hive_escape_path(path: &str) -> String {
    if path.is_empty() {
        return HIVE_DEFAULT_PARTITION.to_owned();
    }
    percent_encode(path.as_bytes(), HIVE_ENCODE_SET).to_string()
}

#[derive(Clone, Copy)]
enum TagKind {
    Expression,
    Block,
    Comment,
}

struct TemplateTag<'a> {
    kind: TagKind,
    body: &'a str,
    start: usize,
    end: usize,
}

fn template_tags(source: &str) -> Result<Vec<TemplateTag<'_>>> {
    let mut tags = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative_start) = source[cursor..].find('{') {
        let start = cursor + relative_start;
        let remainder = &source[start..];
        let (kind, opening, closing) = if remainder.starts_with("{{") {
            (TagKind::Expression, "{{", "}}")
        } else if remainder.starts_with("{%") {
            (TagKind::Block, "{%", "%}")
        } else if remainder.starts_with("{#") {
            (TagKind::Comment, "{#", "#}")
        } else {
            cursor = start + 1;
            continue;
        };
        let body_start = start + opening.len();
        let relative_end = source[body_start..]
            .find(closing)
            .ok_or_else(|| anyhow!("unclosed output target template tag"))?;
        let body_end = body_start + relative_end;
        let end = body_end + closing.len();
        tags.push(TemplateTag {
            kind,
            body: &source[body_start..body_end],
            start,
            end,
        });
        cursor = end;
    }
    Ok(tags)
}

fn contains_identifier(source: &str, identifier: &str) -> bool {
    source.match_indices(identifier).any(|(start, _)| {
        let before = source[..start].chars().next_back();
        let after = source[start + identifier.len()..].chars().next();
        !before.is_some_and(|character| character == '_' || character.is_alphanumeric())
            && !after.is_some_and(|character| character == '_' || character.is_alphanumeric())
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
        TimestampMicrosecondArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    struct TestOutputTargetTemplate(OutputTargetTemplate);

    impl TestOutputTargetTemplate {
        fn new(pattern: String) -> Self {
            Self(OutputTargetTemplate::new(pattern).unwrap())
        }

        fn render_path(&self, values: &PartitionValues) -> String {
            bare_path(self.0.render(values, None).unwrap())
        }
    }

    fn bare_path(location: LocationInput) -> String {
        match location {
            LocationInput::Bare(path) => path,
            LocationInput::Url(location) => location.url().path().to_owned(),
        }
    }

    #[test]
    fn output_target_template_fixes_url_envelope_and_encodes_hive_bytes() {
        let template = OutputTargetTemplate::new(
            "memory://bucket/root/{{region}}.parquet?version=one".to_owned(),
        )
        .unwrap();
        let mut values = HashMap::new();
        values.insert(
            "region".to_owned(),
            Arc::new(StringArray::from(vec!["west/coast"])) as _,
        );

        let rendered = template.render(&values, None).unwrap();
        let LocationInput::Url(rendered) = rendered else {
            panic!("explicit URL template rendered as bare path");
        };
        assert_eq!(
            rendered.url().as_str(),
            "memory://bucket/root/west%252Fcoast.parquet?version=one"
        );
    }

    #[test]
    fn raw_values_may_add_valid_hierarchy_but_not_dot_segments() {
        let template =
            OutputTargetTemplate::new("out/{{region|raw}}/data.parquet".to_owned()).unwrap();
        let mut values = HashMap::new();
        values.insert(
            "region".to_owned(),
            Arc::new(StringArray::from(vec!["west/coast"])) as _,
        );
        assert_eq!(
            bare_path(template.render(&values, None).unwrap()),
            "out/west/coast/data.parquet"
        );

        values.insert(
            "region".to_owned(),
            Arc::new(StringArray::from(vec!["../escape"])) as _,
        );
        assert!(template.render(&values, None).is_err());
    }

    #[test]
    fn bare_template_treats_a_hash_as_object_path_text() {
        let template = TestOutputTargetTemplate::new("out/#/{{region}}.parquet".to_owned());
        let mut values = HashMap::new();
        values.insert(
            "region".to_owned(),
            Arc::new(StringArray::from(vec!["west"])) as _,
        );
        assert_eq!(template.render_path(&values), "out/#/west.parquet");
    }

    #[test]
    fn file_number_requires_direct_unconditional_interpolation() {
        OutputTargetTemplate::new("out/{{region}}_{{ file_number }}.parquet".to_owned())
            .unwrap()
            .require_file_number()
            .unwrap();

        let conditional = OutputTargetTemplate::new(
            "out/{% if false %}{{ file_number }}{% endif %}.parquet".to_owned(),
        )
        .unwrap();
        assert!(conditional.require_file_number().is_err());
        let transformed =
            OutputTargetTemplate::new("out/{{ file_number + 1 }}.parquet".to_owned()).unwrap();
        assert!(transformed.require_file_number().is_err());
        let captured = OutputTargetTemplate::new(
            "out/{% set ignored %}{{ file_number }}{% endset %}.parquet".to_owned(),
        )
        .unwrap();
        assert!(captured.require_file_number().is_err());
        let raw = OutputTargetTemplate::new(
            "out/{% raw %}{{ file_number }}{% endraw %}.parquet".to_owned(),
        )
        .unwrap();
        assert!(raw.require_file_number().is_err());
    }

    #[test]
    fn static_extension_requires_a_literal_terminal_extension() {
        assert_eq!(
            OutputTargetTemplate::new("out/{{region}}_{{file_number}}.parquet".to_owned())
                .unwrap()
                .static_extension(),
            Some("parquet")
        );
        assert_eq!(
            OutputTargetTemplate::new("out/{{region}}.{{ 'parquet' }}".to_owned())
                .unwrap()
                .static_extension(),
            None
        );
    }

    #[test]
    fn test_resolve_with_single_column() {
        let template = TestOutputTargetTemplate::new("output/{{year}}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "year".to_string(),
            Arc::new(Int32Array::from(vec![2024])) as _,
        );

        let result = template.render_path(&values);
        assert_eq!(result, "output/2024.parquet");
    }

    #[test]
    fn test_resolve_with_multiple_columns() {
        let template =
            TestOutputTargetTemplate::new("output/{{year}}/{{month}}/{{day}}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "year".to_string(),
            Arc::new(Int32Array::from(vec![2024])) as _,
        );
        values.insert(
            "month".to_string(),
            Arc::new(Int32Array::from(vec![11])) as _,
        );
        values.insert("day".to_string(), Arc::new(Int32Array::from(vec![22])) as _);

        let result = template.render_path(&values);
        assert_eq!(result, "output/2024/11/22.parquet");
    }

    #[test]
    fn test_resolve_with_string_column() {
        let template =
            TestOutputTargetTemplate::new("output/{{region}}/{{city}}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["us-west"])) as _,
        );
        values.insert(
            "city".to_string(),
            Arc::new(StringArray::from(vec!["seattle"])) as _,
        );

        let result = template.render_path(&values);
        assert_eq!(result, "output/us-west/seattle.parquet");
    }

    #[test]
    fn test_resolve_with_null_value() {
        let template = TestOutputTargetTemplate::new("output/{{category}}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert(
            "category".to_string(),
            Arc::new(StringArray::from(vec![None::<&str>])) as _,
        );

        let result = template.render_path(&values);
        assert_eq!(result, "output/__HIVE_DEFAULT_PARTITION__.parquet");
    }

    #[test]
    fn test_resolve_with_no_placeholders() {
        let template = TestOutputTargetTemplate::new("output/data.parquet".to_string());
        let values = HashMap::new();

        let result = template.render_path(&values);
        assert_eq!(result, "output/data.parquet");
    }

    #[test]
    fn test_resolve_with_repeated_placeholder() {
        let template = TestOutputTargetTemplate::new("output/{{id}}/{{id}}.parquet".to_string());
        let mut values = HashMap::new();
        values.insert("id".to_string(), Arc::new(Int32Array::from(vec![42])) as _);

        let result = template.render_path(&values);
        assert_eq!(result, "output/42/42.parquet");
    }

    #[test]
    fn test_all_integer_types() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // Int8
        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(Int8Array::from(vec![127])) as _);
        assert_eq!(template.render_path(&values), "output/127.parquet");

        // Int16
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Int16Array::from(vec![32767])) as _,
        );
        assert_eq!(template.render_path(&values), "output/32767.parquet");

        // Int64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Int64Array::from(vec![9223372036854775807i64])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/9223372036854775807.parquet"
        );

        // UInt8
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt8Array::from(vec![255])) as _,
        );
        assert_eq!(template.render_path(&values), "output/255.parquet");

        // UInt16
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt16Array::from(vec![65535])) as _,
        );
        assert_eq!(template.render_path(&values), "output/65535.parquet");

        // UInt32
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt32Array::from(vec![4294967295])) as _,
        );
        assert_eq!(template.render_path(&values), "output/4294967295.parquet");

        // UInt64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(UInt64Array::from(vec![18446744073709551615u64])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/18446744073709551615.parquet"
        );
    }

    #[test]
    fn test_float_types() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // Float32
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Float32Array::from(vec![1.23f32])) as _,
        );
        assert_eq!(template.render_path(&values), "output/1.23.parquet");

        // Float64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Float64Array::from(vec![4.56789])) as _,
        );
        assert_eq!(template.render_path(&values), "output/4.56789.parquet");
    }

    #[test]
    fn test_boolean_type() {
        let template = TestOutputTargetTemplate::new("output/{{flag}}.parquet".to_string());

        // true
        let mut values = HashMap::new();
        values.insert(
            "flag".to_string(),
            Arc::new(BooleanArray::from(vec![true])) as _,
        );
        assert_eq!(template.render_path(&values), "output/true.parquet");

        // false
        let mut values = HashMap::new();
        values.insert(
            "flag".to_string(),
            Arc::new(BooleanArray::from(vec![false])) as _,
        );
        assert_eq!(template.render_path(&values), "output/false.parquet");
    }

    #[test]
    fn test_string_types() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // Utf8 (already tested in test_resolve_with_string_column)

        // LargeUtf8
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(LargeStringArray::from(vec!["large-string"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/large-string.parquet");
    }

    #[test]
    fn test_date_and_time_types() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // Date32 (days since epoch) - formats as YYYY-MM-DD
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Date32Array::from(vec![19000])) as _,
        );
        assert_eq!(template.render_path(&values), "output/2022-01-08.parquet");

        // Date64 (milliseconds since epoch) - formats as ISO8601 with time (colons escaped)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Date64Array::from(vec![1640995200000i64])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/2022-01-01T00%3A00%3A00.parquet"
        );

        // TimestampNanosecond - formats as ISO8601 with colons escaped
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampNanosecondArray::from(vec![1641051045000000000i64])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/2022-01-01T15%3A30%3A45.parquet"
        );

        // TimestampMicrosecond - formats as ISO8601 with colons escaped
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampMicrosecondArray::from(vec![1641051045000000i64])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/2022-01-01T15%3A30%3A45.parquet"
        );
    }

    #[test]
    fn test_binary_types() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // Binary
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(BinaryArray::from_vec(vec![b"hello"])) as _,
        );
        let result = template.render_path(&values);
        assert!(result.starts_with("output/"));
        assert!(result.ends_with(".parquet"));

        // LargeBinary
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(LargeBinaryArray::from_vec(vec![b"world"])) as _,
        );
        let result = template.render_path(&values);
        assert!(result.starts_with("output/"));
        assert!(result.ends_with(".parquet"));
    }

    #[test]
    fn test_decimal_type() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // Decimal128 with precision 10, scale 2 (e.g., for money: 123.45)
        let mut values = HashMap::new();
        let decimal_array =
            Decimal128Array::from(vec![12345]).with_data_type(DataType::Decimal128(10, 2));
        values.insert("val".to_string(), Arc::new(decimal_array) as _);
        assert_eq!(template.render_path(&values), "output/123.45.parquet");
    }

    #[test]
    fn test_null_values_for_various_types() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // Null Int32
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Int32Array::from(vec![None])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );

        // Null Float64
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(Float64Array::from(vec![None])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );

        // Null Boolean
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(BooleanArray::from(vec![None])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );

        // Null String (already tested but included for completeness)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec![None::<&str>])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );
    }

    #[test]
    fn test_empty_string_handling() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // empty string should also use HIVE_DEFAULT_PARTITION
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec![""])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/__HIVE_DEFAULT_PARTITION__.parquet"
        );
    }

    #[test]
    fn test_list_type() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // create a list array [1, 2, 3]
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let list_array = ListArray::new(field, offsets, Arc::new(values_data), None);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(list_array) as _);

        let result = template.render_path(&values);
        // [1, 2, 3] -> %5B1, 2, 3%5D (brackets escaped, commas not)
        assert_eq!(result, "output/%5B1, 2, 3%5D.parquet");
    }

    #[test]
    fn test_struct_type() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // create a struct with fields {name: "Alice", age: 30}
        let name_array = Arc::new(StringArray::from(vec!["Alice"]));
        let age_array = Arc::new(Int32Array::from(vec![30]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                name_array as ArrayRef,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                age_array as ArrayRef,
            ),
        ]);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(struct_array) as _);

        let result = template.render_path(&values);
        // {name: Alice, age: 30} -> %7Bname%3A Alice, age%3A 30}
        assert_eq!(result, "output/%7Bname%3A Alice, age%3A 30}.parquet");
    }

    #[test]
    fn test_map_type() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // create a map {"key1": 100}
        let keys = Arc::new(StringArray::from(vec!["key1"]));
        let values_arr = Arc::new(Int32Array::from(vec![100]));

        let entry_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                keys as ArrayRef,
            ),
            (
                Arc::new(Field::new("values", DataType::Int32, false)),
                values_arr as ArrayRef,
            ),
        ]);

        let entry_offsets = OffsetBuffer::new(vec![0, 1].into());
        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("keys", DataType::Utf8, false)),
                    Arc::new(Field::new("values", DataType::Int32, false)),
                ]
                .into(),
            ),
            false,
        ));

        let map_array = MapArray::new(map_field, entry_offsets, entry_struct, None, false);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(map_array) as _);

        let result = template.render_path(&values);
        // map formatting varies, just verify it works
        assert!(result.starts_with("output/"));
        assert!(result.ends_with(".parquet"));
    }

    #[test]
    fn test_nested_list() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // create a list of lists [[1, 2], [3]]
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let inner_offsets = OffsetBuffer::new(vec![0, 2, 3].into());
        let inner_field = Arc::new(Field::new("item", DataType::Int32, false));
        let inner_list = ListArray::new(
            Arc::clone(&inner_field),
            inner_offsets,
            Arc::new(values_data),
            None,
        );

        let outer_offsets = OffsetBuffer::new(vec![0, 2].into()); // includes both inner lists
        let outer_field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::clone(&inner_field)),
            false,
        ));
        let outer_list = ListArray::new(outer_field, outer_offsets, Arc::new(inner_list), None);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(outer_list) as _);

        let result = template.render_path(&values);
        // [[1, 2], [3]] -> %5B%5B1, 2%5D, %5B3%5D%5D (commas not escaped)
        assert_eq!(result, "output/%5B%5B1, 2%5D, %5B3%5D%5D.parquet");
    }

    #[test]
    fn test_hive_escaping_special_characters() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // test forward slash
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["a/b"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/a%2Fb.parquet");

        // test colon
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["10:30"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/10%3A30.parquet");

        // test equals
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["key=value"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/key%3Dvalue.parquet");

        // test hash
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["tag#1"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/tag%231.parquet");

        // test percent
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["50%"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/50%25.parquet");

        // test question mark
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["what?"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/what%3F.parquet");

        // test backslash
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["a\\b"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/a%5Cb.parquet");

        // test asterisk
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["*.txt"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/%2A.txt.parquet");

        // test brackets and caret (note: } is not escaped, only { [ ] ^)
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["a[0]{x}^2"])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/a%5B0%5D%7Bx}%5E2.parquet"
        );
    }

    #[test]
    fn test_hive_escaping_no_escape_needed() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // simple alphanumeric
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["simple123"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/simple123.parquet");

        // with hyphens, underscores, dots
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["test-value_2.0"])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/test-value_2.0.parquet"
        );
    }

    #[test]
    fn test_hive_escaping_quotes() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // double quote
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["say \"hello\""])) as _,
        );
        assert_eq!(
            template.render_path(&values),
            "output/say %22hello%22.parquet"
        );

        // single quote
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec!["it's"])) as _,
        );
        assert_eq!(template.render_path(&values), "output/it%27s.parquet");
    }

    #[test]
    fn test_hive_escaping_struct_formatting() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // struct formatting includes special characters that need escaping
        let name_array = Arc::new(StringArray::from(vec!["Alice"]));
        let age_array = Arc::new(Int32Array::from(vec![30]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                name_array as ArrayRef,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                age_array as ArrayRef,
            ),
        ]);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(struct_array) as _);

        let result = template.render_path(&values);
        // {name: Alice, age: 30} -> %7Bname%3A Alice, age%3A 30}
        // note: only { is escaped, not } or ,
        assert_eq!(result, "output/%7Bname%3A Alice, age%3A 30}.parquet");
    }

    #[test]
    fn test_hive_escaping_list_formatting() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // list formatting includes brackets and commas
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let list_array = ListArray::new(field, offsets, Arc::new(values_data), None);

        let mut values = HashMap::new();
        values.insert("val".to_string(), Arc::new(list_array) as _);

        let result = template.render_path(&values);
        // [1, 2, 3] -> %5B1, 2, 3%5D (note: comma is not escaped)
        assert_eq!(result, "output/%5B1, 2, 3%5D.parquet");
    }

    #[test]
    fn test_hive_escaping_timestamp_formatting() {
        let template = TestOutputTargetTemplate::new("output/{{val}}.parquet".to_string());

        // timestamp includes colons
        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(TimestampNanosecondArray::from(vec![1641051045000000000i64])) as _,
        );

        let result = template.render_path(&values);
        // 2022-01-01T15:30:45 -> 2022-01-01T15%3A30%3A45
        assert_eq!(result, "output/2022-01-01T15%3A30%3A45.parquet");
    }

    #[test]
    fn test_raw_filter_bypasses_escaping() {
        let template = TestOutputTargetTemplate::new("output/{{region | raw}}.parquet".to_string());

        // forward slashes should NOT be escaped with raw filter
        let mut values = HashMap::new();
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["US/West"])) as _,
        );

        let result = template.render_path(&values);
        assert_eq!(result, "output/US/West.parquet");
    }

    #[test]
    fn test_raw_filter_with_special_characters() {
        let template =
            TestOutputTargetTemplate::new("output/{{path | raw}}/data.parquet".to_string());

        // special characters should NOT be escaped with raw filter
        let mut values = HashMap::new();
        values.insert(
            "path".to_string(),
            Arc::new(StringArray::from(vec!["2024/01/15"])) as _,
        );

        let result = template.render_path(&values);
        assert_eq!(result, "output/2024/01/15/data.parquet");
    }

    #[test]
    fn test_mixed_escaping_and_raw() {
        let template =
            TestOutputTargetTemplate::new("output/{{region}}/{{date | raw}}.parquet".to_string());

        let mut values = HashMap::new();
        values.insert(
            "region".to_string(),
            Arc::new(StringArray::from(vec!["US/West"])) as _,
        );
        values.insert(
            "date".to_string(),
            Arc::new(StringArray::from(vec!["2024:01:15"])) as _,
        );

        let result = template.render_path(&values);
        // region should be escaped, date should not
        assert_eq!(result, "output/US%2FWest/2024:01:15.parquet");
    }

    #[test]
    fn test_raw_filter_with_nulls() {
        let template =
            TestOutputTargetTemplate::new("output/{{category | raw}}.parquet".to_string());

        let mut values = HashMap::new();
        values.insert(
            "category".to_string(),
            Arc::new(StringArray::from(vec![None::<&str>])) as _,
        );

        let result = template.render_path(&values);
        // null should still become __HIVE_DEFAULT_PARTITION__ even with raw
        assert_eq!(result, "output/__HIVE_DEFAULT_PARTITION__.parquet");
    }

    #[test]
    fn test_raw_filter_with_empty_string() {
        let template = TestOutputTargetTemplate::new("output/{{val | raw}}.parquet".to_string());

        let mut values = HashMap::new();
        values.insert(
            "val".to_string(),
            Arc::new(StringArray::from(vec![""])) as _,
        );

        let result = template.render_path(&values);
        // empty string should become __HIVE_DEFAULT_PARTITION__
        assert_eq!(result, "output/__HIVE_DEFAULT_PARTITION__.parquet");
    }

    #[test]
    fn test_multiple_columns_with_raw() {
        let template = TestOutputTargetTemplate::new(
            "output/{{year}}/{{month | raw}}/{{day}}.parquet".to_string(),
        );

        let mut values = HashMap::new();
        values.insert(
            "year".to_string(),
            Arc::new(StringArray::from(vec!["2024/Q1"])) as _,
        );
        values.insert(
            "month".to_string(),
            Arc::new(StringArray::from(vec!["01/15"])) as _,
        );
        values.insert(
            "day".to_string(),
            Arc::new(StringArray::from(vec!["15:30"])) as _,
        );

        let result = template.render_path(&values);
        // year and day escaped, month raw
        assert_eq!(result, "output/2024%2FQ1/01/15/15%3A30.parquet");
    }
}
