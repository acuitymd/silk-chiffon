use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::{
    common::Statistics,
    datasource::{file_format::FileFormat, listing::PartitionedFile},
    physical_expr::LexOrdering,
    physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
    },
    prelude::SessionContext,
};
use futures::{StreamExt, TryStreamExt};
use silk_chiffon_core::{CanonicalInput, file_table_provider, register_input_store};
use silk_chiffon_storage::InputObject;

pub(crate) async fn native_file_provider(
    objects: &[InputObject],
    session: &SessionContext,
    format: Arc<dyn FileFormat>,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    let representative = objects
        .iter()
        .max_by(|left, right| {
            left.metadata()
                .size
                .cmp(&right.metadata().size)
                .then_with(|| {
                    right
                        .handle()
                        .url()
                        .as_str()
                        .cmp(left.handle().url().as_str())
                })
        })
        .context("cannot build an empty file-input leaf")?;
    let representative_index = objects
        .iter()
        .position(|object| std::ptr::eq(object, representative))
        .expect("the representative came from the object slice");
    let (store_url, files) = register_input_store(session, objects)?;
    let store = session.runtime_env().object_store(&store_url)?;
    let schema = format
        .infer_schema(
            &session.state(),
            &store,
            std::slice::from_ref(&files[representative_index].object_meta),
        )
        .await
        .with_context(|| {
            format!(
                "while inferring schema from representative {}",
                representative.handle().url()
            )
        })?;
    let concurrency = session
        .state()
        .config_options()
        .execution
        .meta_fetch_concurrency;
    let file_meta = futures::stream::iter(files.into_iter().enumerate())
        .map(|(index, file)| {
            let store = Arc::clone(&store);
            let format = Arc::clone(&format);
            let schema = Arc::clone(&schema);
            let state = session.state();
            async move {
                let canonical_url = file
                    .extension::<CanonicalInput>()
                    .expect("registered input files retain their canonical URL")
                    .url()
                    .clone();
                let meta = format
                    .infer_stats_and_ordering(
                        &state,
                        &store,
                        Arc::clone(&schema),
                        &file.object_meta,
                    )
                    .await
                    .map_err(|source| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "while reading native metadata for {canonical_url}: {source}"
                        ))
                    })?;
                // Empty files never reach the physical adapter, so structural
                // validation also belongs in the metadata pass.
                let physical_schema = format
                    .infer_schema(&state, &store, std::slice::from_ref(&file.object_meta))
                    .await
                    .map_err(|source| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "while validating the schema of {canonical_url}: {source}"
                        ))
                    })?;
                if !structurally_equal(&schema, &physical_schema) {
                    return Err(datafusion::common::DataFusionError::Execution(format!(
                        "input {canonical_url} schema does not match leaf schema: expected {schema:?}, got {physical_schema:?}"
                    )));
                }
                Ok((index, file, meta))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let mut file_meta = file_meta;
    file_meta.sort_by_key(|(index, _, _)| *index);

    let files = file_meta
        .iter()
        .map(|(_, file, meta)| {
            file.clone()
                .with_statistics(Arc::new(meta.statistics.clone()))
                .with_ordering(meta.ordering.clone())
        })
        .collect::<Vec<_>>();
    let statistics = Statistics::try_merge_iter(
        file_meta.iter().map(|(_, _, meta)| &meta.statistics),
        schema.as_ref(),
    )?;
    let output_ordering = common_output_ordering(&files);

    file_table_provider(
        store_url,
        schema,
        files,
        statistics,
        output_ordering,
        format,
        Some(Arc::new(StrictPhysicalExprAdapterFactory)),
    )
    .map_err(Into::into)
}

fn common_output_ordering(files: &[PartitionedFile]) -> Vec<LexOrdering> {
    let Some(first) = files.first().and_then(|file| file.ordering.clone()) else {
        return Vec::new();
    };
    let mut common = first;
    for file in &files[1..] {
        let Some(ordering) = &file.ordering else {
            return Vec::new();
        };
        let prefix_len = common
            .iter()
            .zip(ordering.iter())
            .take_while(|(left, right)| left == right)
            .count();
        let Some(prefix) = LexOrdering::new(common[..prefix_len].to_vec()) else {
            return Vec::new();
        };
        common = prefix;
    }
    vec![common]
}

#[derive(Debug)]
struct StrictPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for StrictPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExprAdapter>> {
        if !structurally_equal(&logical_file_schema, &physical_file_schema) {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "input file schema does not match leaf schema: expected {logical_file_schema:?}, got {physical_file_schema:?}"
            )));
        }
        DefaultPhysicalExprAdapterFactory.create(logical_file_schema, physical_file_schema)
    }
}

pub(crate) fn structurally_equal(left: &SchemaRef, right: &SchemaRef) -> bool {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::Schema;
    use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
    use object_store::ObjectMeta;

    use super::*;

    #[test]
    fn structural_comparison_ignores_metadata_but_not_nested_names() {
        let nested = |name: &str, metadata: &[(&str, &str)]| {
            let child = Field::new(name, DataType::Int64, true).with_metadata(
                metadata
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect::<HashMap<_, _>>(),
            );
            Arc::new(Schema::new(vec![Field::new(
                "outer",
                DataType::Struct(vec![Arc::new(child)].into()),
                false,
            )]))
        };
        assert!(structurally_equal(
            &nested("value", &[("a", "one")]),
            &nested("value", &[("a", "two")])
        ));
        assert!(!structurally_equal(
            &nested("left", &[]),
            &nested("right", &[])
        ));
    }

    #[test]
    fn structural_comparison_ignores_metadata_inside_dictionaries() {
        let dictionary = |metadata: &[(&str, &str)]| {
            let value = DataType::List(Arc::new(
                Field::new("item", DataType::Utf8, true).with_metadata(
                    metadata
                        .iter()
                        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                        .collect(),
                ),
            ));
            Arc::new(Schema::new(vec![Field::new(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(value)),
                true,
            )]))
        };

        assert!(structurally_equal(
            &dictionary(&[("source", "left")]),
            &dictionary(&[("source", "right")])
        ));
    }

    #[test]
    fn file_ordering_uses_the_longest_prefix_declared_by_every_file() {
        let ordered_file = |columns: &[(&str, usize)]| {
            let ordering = columns
                .iter()
                .map(|(name, index)| {
                    PhysicalSortExpr::new_default(Arc::new(Column::new(name, *index)))
                })
                .collect::<Vec<_>>();
            PartitionedFile::new_from_meta(ObjectMeta {
                location: "file".into(),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: None,
                version: None,
            })
            .with_ordering(LexOrdering::new(ordering))
        };
        let files = [
            ordered_file(&[("id", 0), ("name", 1)]),
            ordered_file(&[("id", 0)]),
        ];

        let ordering = common_output_ordering(&files);

        assert_eq!(ordering.len(), 1);
        assert_eq!(ordering[0].len(), 1);
        assert_eq!(ordering[0][0].expr.to_string(), "id@0");
    }
}
