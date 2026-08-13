//! Schema compatibility used by concrete file formats.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

/// Reports whether two schemas have the same recursive structure after metadata is removed.
///
/// File formats use this check when a logical schema has already been selected for a group of
/// files. Field order, names, nullability, and data types remain strict so the result never implies
/// coercion or reordering; only schema and field metadata are outside the compatibility contract.
pub fn schemas_match_ignoring_metadata(left: &Schema, right: &Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| stripped_field(left) == stripped_field(right))
}

fn stripped_field(field: &Field) -> Field {
    Field::new(
        field.name(),
        stripped_data_type(field.data_type()),
        field.is_nullable(),
    )
}

fn stripped_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(Arc::new(stripped_field(field))),
        DataType::ListView(field) => DataType::ListView(Arc::new(stripped_field(field))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(stripped_field(field)), *size)
        }
        DataType::LargeList(field) => DataType::LargeList(Arc::new(stripped_field(field))),
        DataType::LargeListView(field) => DataType::LargeListView(Arc::new(stripped_field(field))),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(stripped_field(field)))
                .collect(),
        ),
        DataType::Map(field, sorted) => DataType::Map(Arc::new(stripped_field(field)), *sorted),
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(stripped_data_type(key)),
            Box::new(stripped_data_type(value)),
        ),
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            Arc::new(stripped_field(run_ends)),
            Arc::new(stripped_field(values)),
        ),
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(id, field)| (id, Arc::new(stripped_field(field))))
                .collect(),
            *mode,
        ),
        other => other.clone(),
    }
}
