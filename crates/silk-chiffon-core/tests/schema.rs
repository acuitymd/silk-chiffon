use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, UnionFields, UnionMode};
use silk_chiffon_core::schemas_match_ignoring_metadata;

fn nested_schema(child_name: &str, metadata: &str) -> Schema {
    Schema::new_with_metadata(
        vec![Field::new(
            "outer",
            DataType::List(Arc::new(
                Field::new(child_name, DataType::Utf8, true).with_metadata(HashMap::from([(
                    "child-source".to_owned(),
                    metadata.to_owned(),
                )])),
            )),
            false,
        )],
        HashMap::from([("schema-source".to_owned(), metadata.to_owned())]),
    )
}

#[test]
fn schema_comparison_ignores_schema_and_nested_field_metadata() {
    assert!(schemas_match_ignoring_metadata(
        &nested_schema("item", "left"),
        &nested_schema("item", "right"),
    ));
}

#[test]
fn schema_comparison_remains_strict_about_structure() {
    assert!(!schemas_match_ignoring_metadata(
        &nested_schema("left", "same"),
        &nested_schema("right", "same"),
    ));
}

#[test]
fn schema_comparison_ignores_metadata_inside_dictionaries() {
    let dictionary = |metadata: &[(&str, &str)]| {
        let value = DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, true).with_metadata(
                metadata
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect(),
            ),
        ));
        Schema::new(vec![Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(value)),
            true,
        )])
    };

    assert!(schemas_match_ignoring_metadata(
        &dictionary(&[("source", "left")]),
        &dictionary(&[("source", "right")]),
    ));
}

#[test]
fn schema_comparison_strips_metadata_from_every_nested_container() {
    let child = |value: &str| {
        Arc::new(
            Field::new("item", DataType::Utf8, true)
                .with_metadata(HashMap::from([("source".to_owned(), value.to_owned())])),
        )
    };
    let run_ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
    let variants = [
        (
            DataType::ListView(child("left")),
            DataType::ListView(child("right")),
        ),
        (
            DataType::FixedSizeList(child("left"), 4),
            DataType::FixedSizeList(child("right"), 4),
        ),
        (
            DataType::LargeList(child("left")),
            DataType::LargeList(child("right")),
        ),
        (
            DataType::LargeListView(child("left")),
            DataType::LargeListView(child("right")),
        ),
        (
            DataType::Map(child("left"), false),
            DataType::Map(child("right"), false),
        ),
        (
            DataType::RunEndEncoded(Arc::clone(&run_ends), child("left")),
            DataType::RunEndEncoded(Arc::clone(&run_ends), child("right")),
        ),
        (
            DataType::Union(
                UnionFields::try_new(vec![0], vec![child("left").as_ref().clone()]).unwrap(),
                UnionMode::Sparse,
            ),
            DataType::Union(
                UnionFields::try_new(vec![0], vec![child("right").as_ref().clone()]).unwrap(),
                UnionMode::Sparse,
            ),
        ),
    ];

    for (left, right) in variants {
        assert!(schemas_match_ignoring_metadata(
            &Schema::new(vec![Field::new("outer", left, true)]),
            &Schema::new(vec![Field::new("outer", right, true)]),
        ));
    }
}
