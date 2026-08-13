use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
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
use silk_chiffon_core::{
    CanonicalInput, InputLeaf, file_table_provider, schemas_match_ignoring_metadata,
};

pub(crate) async fn native_file_provider(
    leaf: &InputLeaf,
    session: &SessionContext,
    format: Arc<dyn FileFormat>,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    let store_url = leaf.object_store_url().clone();
    let files = leaf.files();
    let store = session.runtime_env().object_store(&store_url)?;
    let representative = leaf.representative();
    let representative_url = representative
        .extension::<CanonicalInput>()
        .expect("prepared input files retain their canonical URL")
        .url();
    let schema = format
        .infer_schema(
            &session.state(),
            &store,
            std::slice::from_ref(&representative.object_meta),
        )
        .await
        .with_context(|| {
            format!(
                "while inferring schema from representative {}",
                representative_url
            )
        })?;
    let concurrency = session
        .state()
        .config_options()
        .execution
        .meta_fetch_concurrency;
    let file_meta = futures::stream::iter(files.iter().cloned().enumerate())
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
                if !schemas_match_ignoring_metadata(&schema, &physical_schema) {
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
        if !schemas_match_ignoring_metadata(&logical_file_schema, &physical_file_schema) {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "input file schema does not match leaf schema: expected {logical_file_schema:?}, got {physical_file_schema:?}"
            )));
        }
        DefaultPhysicalExprAdapterFactory.create(logical_file_schema, physical_file_schema)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
    use object_store::ObjectMeta;

    use super::*;

    #[test]
    fn strict_adapter_rejects_a_structurally_different_file_schema() {
        let logical = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let physical = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));

        let error = StrictPhysicalExprAdapterFactory
            .create(logical, physical)
            .expect_err("a structurally different file schema must fail");

        assert!(error.to_string().contains("does not match leaf schema"));
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

    #[test]
    fn file_ordering_is_not_claimed_when_any_file_is_unordered() {
        let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
            "id", 0,
        )))]
        .into();
        let ordered = PartitionedFile::new("ordered", 1).with_ordering(Some(ordering));
        let unordered = PartitionedFile::new("unordered", 1);

        assert!(common_output_ordering(&[ordered, unordered]).is_empty());
    }

    #[test]
    fn file_ordering_is_not_claimed_without_a_common_prefix() {
        let ordered = |name: &str, index: usize| {
            let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
                name, index,
            )))]
            .into();
            PartitionedFile::new(name, 1).with_ordering(Some(ordering))
        };

        assert!(common_output_ordering(&[ordered("left", 0), ordered("right", 1)]).is_empty());
    }
}
