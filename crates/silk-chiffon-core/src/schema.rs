//! Schema compatibility used by concrete file formats.

use anyhow::{Result, bail};
use arrow::datatypes::{DataType, Field, Schema};

enum Comparison<'a> {
    Field(&'a Field, &'a Field),
    DataType(&'a DataType, &'a DataType),
}

#[derive(Clone, Copy)]
enum Nullability {
    Exact,
    ActualMayRefine,
}

/// Reports whether two schemas have the same recursive structure after metadata is removed.
///
/// File formats use this check when a logical schema has already been selected for a group of
/// files. Field order, names, nullability, and data types remain strict so the result never implies
/// coercion or reordering; only schema and field metadata are outside the compatibility contract.
pub fn schemas_match_ignoring_metadata(left: &Schema, right: &Schema) -> bool {
    schemas_have_compatible_structure(left, right, Nullability::Exact)
}

fn schemas_have_compatible_structure(
    left: &Schema,
    right: &Schema,
    nullability: Nullability,
) -> bool {
    if left.fields().len() != right.fields().len() {
        return false;
    }

    // Schemas originate in input files, so nested types must not consume the call stack.
    let mut pending = left
        .fields()
        .iter()
        .zip(right.fields())
        .map(|(left, right)| Comparison::Field(left, right))
        .collect::<Vec<_>>();

    while let Some(comparison) = pending.pop() {
        match comparison {
            Comparison::Field(left, right) => {
                let nullability_matches = match nullability {
                    Nullability::Exact => left.is_nullable() == right.is_nullable(),
                    Nullability::ActualMayRefine => left.is_nullable() || !right.is_nullable(),
                };
                if left.name() != right.name() || !nullability_matches {
                    return false;
                }
                pending.push(Comparison::DataType(left.data_type(), right.data_type()));
            }
            Comparison::DataType(left, right) => match (left, right) {
                (DataType::List(left), DataType::List(right))
                | (DataType::ListView(left), DataType::ListView(right))
                | (DataType::LargeList(left), DataType::LargeList(right))
                | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
                    pending.push(Comparison::Field(left, right));
                }
                (
                    DataType::FixedSizeList(left, left_size),
                    DataType::FixedSizeList(right, right_size),
                ) => {
                    if left_size != right_size {
                        return false;
                    }
                    pending.push(Comparison::Field(left, right));
                }
                (DataType::Struct(left), DataType::Struct(right)) => {
                    if left.len() != right.len() {
                        return false;
                    }
                    pending.extend(
                        left.iter()
                            .zip(right)
                            .map(|(left, right)| Comparison::Field(left, right)),
                    );
                }
                (DataType::Union(left, left_mode), DataType::Union(right, right_mode)) => {
                    if left_mode != right_mode || left.len() != right.len() {
                        return false;
                    }
                    for ((left_id, left), (right_id, right)) in left.iter().zip(right.iter()) {
                        if left_id != right_id {
                            return false;
                        }
                        pending.push(Comparison::Field(left, right));
                    }
                }
                (
                    DataType::Dictionary(left_key, left_value),
                    DataType::Dictionary(right_key, right_value),
                ) => {
                    pending.push(Comparison::DataType(left_key, right_key));
                    pending.push(Comparison::DataType(left_value, right_value));
                }
                (DataType::Map(left, left_sorted), DataType::Map(right, right_sorted)) => {
                    if left_sorted != right_sorted {
                        return false;
                    }
                    pending.push(Comparison::Field(left, right));
                }
                (
                    DataType::RunEndEncoded(left_ends, left_values),
                    DataType::RunEndEncoded(right_ends, right_values),
                ) => {
                    pending.push(Comparison::Field(left_ends, right_ends));
                    pending.push(Comparison::Field(left_values, right_values));
                }
                _ if left != right => return false,
                _ => {}
            },
        }
    }

    true
}

/// Rejects structural schema changes before a sink hands work to codec tasks.
///
/// Metadata may vary, and a batch may refine nullable fields to non-nullable
/// fields. Codecs still rely on field names, order, and types matching the
/// schema used to create the sink.
pub fn validate_batch_schema(expected: &Schema, actual: &Schema) -> Result<()> {
    if schemas_have_compatible_structure(expected, actual, Nullability::ActualMayRefine) {
        return Ok(());
    }
    bail!("record batch schema does not match sink schema: expected {expected:?}, got {actual:?}")
}
