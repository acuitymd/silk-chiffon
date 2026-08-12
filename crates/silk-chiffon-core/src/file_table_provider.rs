//! A table-provider adapter for exact files resolved by the host.

use std::{fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Result, Statistics, exec_err},
    datasource::{
        file_format::FileFormat, listing::PartitionedFile, physical_plan::FileScanConfigBuilder,
        table_schema::TableSchema,
    },
    execution::object_store::ObjectStoreUrl,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::LexOrdering,
    physical_expr_adapter::PhysicalExprAdapterFactory,
    physical_plan::ExecutionPlan,
};

/// Creates a table provider for exact files whose metadata was resolved by the host.
///
/// The provider never lists or reads object metadata. Scans translate the retained files into a
/// [`datafusion::datasource::physical_plan::FileScanConfig`] and delegate plan construction to the
/// supplied format.
pub fn file_table_provider(
    object_store_url: ObjectStoreUrl,
    file_schema: SchemaRef,
    files: Vec<PartitionedFile>,
    statistics: Statistics,
    output_ordering: Vec<LexOrdering>,
    format: Arc<dyn FileFormat>,
    expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
) -> Result<Arc<dyn TableProvider>> {
    if files.is_empty() {
        return exec_err!("an exact-file table provider requires at least one file");
    }
    Ok(Arc::new(FileTableProvider {
        object_store_url,
        file_schema,
        files,
        statistics,
        output_ordering,
        format,
        expr_adapter_factory,
    }))
}

struct FileTableProvider {
    object_store_url: ObjectStoreUrl,
    file_schema: SchemaRef,
    files: Vec<PartitionedFile>,
    statistics: Statistics,
    output_ordering: Vec<LexOrdering>,
    format: Arc<dyn FileFormat>,
    expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl fmt::Debug for FileTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileTableProvider")
            .field("object_store_url", &self.object_store_url)
            .field("file_schema", &self.file_schema)
            .field("files", &self.files.len())
            .field("statistics", &self.statistics)
            .field("output_ordering", &self.output_ordering)
            .field("format", &self.format)
            .field(
                "has_expr_adapter_factory",
                &self.expr_adapter_factory.is_some(),
            )
            .finish()
    }
}

#[async_trait]
impl TableProvider for FileTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.file_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // Inexact pushdown retains a FilterExec. DataFusion's physical
        // optimizer converts that predicate and passes it through
        // FileScanConfig to the format's FileSource.
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = self
            .format
            .file_source(TableSchema::new(Arc::clone(&self.file_schema), Vec::new()));
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_group(self.files.clone().into())
            .with_statistics(self.statistics.clone())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_output_ordering(self.output_ordering.clone())
            .with_expr_adapter(self.expr_adapter_factory.clone())
            .build();
        self.format.create_physical_plan(state, config).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.statistics.clone())
    }
}
