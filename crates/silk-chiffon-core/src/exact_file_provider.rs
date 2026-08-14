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

/// Builds a table provider over an exact, nonempty collection of files.
#[derive(Default)]
pub struct ExactFileTableProviderBuilder {
    object_store_url: Option<ObjectStoreUrl>,
    schema: Option<SchemaRef>,
    files: Option<Vec<PartitionedFile>>,
    statistics: Option<Statistics>,
    output_ordering: Option<Vec<LexOrdering>>,
    format: Option<Arc<dyn FileFormat>>,
    expression_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl ExactFileTableProviderBuilder {
    /// Starts an empty provider definition.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the object-store URL registered with DataFusion.
    pub fn object_store_url(mut self, value: ObjectStoreUrl) -> Self {
        self.object_store_url = Some(value);
        self
    }

    /// Sets the logical file schema exposed by the provider.
    pub fn schema(mut self, value: SchemaRef) -> Self {
        self.schema = Some(value);
        self
    }

    /// Sets the exact file descriptors in scan order.
    pub fn files(mut self, value: Vec<PartitionedFile>) -> Self {
        self.files = Some(value);
        self
    }

    /// Sets aggregate statistics for the complete file group.
    pub fn statistics(mut self, value: Statistics) -> Self {
        self.statistics = Some(value);
        self
    }

    /// Sets orderings guaranteed by every file in the group.
    pub fn output_ordering(mut self, value: Vec<LexOrdering>) -> Self {
        self.output_ordering = Some(value);
        self
    }

    /// Sets the DataFusion file-format implementation.
    pub fn format(mut self, value: Arc<dyn FileFormat>) -> Self {
        self.format = Some(value);
        self
    }

    /// Sets the adapter used to reconcile logical and physical schemas.
    pub fn expression_adapter_factory(
        mut self,
        value: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Self {
        self.expression_adapter_factory = Some(value);
        self
    }

    /// Validates all required provider state and creates the table provider.
    pub fn build(self) -> Result<Arc<dyn TableProvider>> {
        let object_store_url = self.object_store_url.ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "exact-file provider requires an object-store URL".to_owned(),
            )
        })?;
        let schema = self.schema.ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "exact-file provider requires a schema".to_owned(),
            )
        })?;
        let files = self.files.ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "exact-file provider requires files".to_owned(),
            )
        })?;
        if files.is_empty() {
            return exec_err!("an exact-file table provider requires at least one file");
        }
        let statistics = self.statistics.ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "exact-file provider requires statistics".to_owned(),
            )
        })?;
        let output_ordering = self.output_ordering.ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "exact-file provider requires output ordering".to_owned(),
            )
        })?;
        let format = self.format.ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "exact-file provider requires a file format".to_owned(),
            )
        })?;
        Ok(Arc::new(ExactFileTableProvider {
            object_store_url,
            schema,
            files,
            statistics,
            output_ordering,
            format,
            expression_adapter_factory: self.expression_adapter_factory,
        }))
    }
}

struct ExactFileTableProvider {
    object_store_url: ObjectStoreUrl,
    schema: SchemaRef,
    files: Vec<PartitionedFile>,
    statistics: Statistics,
    output_ordering: Vec<LexOrdering>,
    format: Arc<dyn FileFormat>,
    expression_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl fmt::Debug for ExactFileTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExactFileTableProvider")
            .field("object_store_url", &self.object_store_url)
            .field("schema", &self.schema)
            .field("files", &self.files.len())
            .field("statistics", &self.statistics)
            .field("output_ordering", &self.output_ordering)
            .field("format", &self.format)
            .field(
                "has_expression_adapter_factory",
                &self.expression_adapter_factory.is_some(),
            )
            .finish()
    }
}

#[async_trait]
impl TableProvider for ExactFileTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // Inexact pushdown retains a FilterExec. DataFusion's physical
        // optimizer converts that predicate and passes it to FileSource.
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = self
            .format
            .file_source(TableSchema::new(Arc::clone(&self.schema), Vec::new()));
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_group(self.files.clone().into())
            .with_statistics(self.statistics.clone())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_output_ordering(self.output_ordering.clone())
            .with_expr_adapter(self.expression_adapter_factory.clone())
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

#[cfg(test)]
mod tests {
    use datafusion::common::Statistics;

    use super::*;

    #[test]
    fn builder_reports_each_missing_required_value() {
        let error = ExactFileTableProviderBuilder::new().build().unwrap_err();
        assert!(error.to_string().contains("object-store URL"));
    }

    #[test]
    fn builder_rejects_an_empty_file_collection() {
        let error = ExactFileTableProviderBuilder::new()
            .object_store_url(ObjectStoreUrl::local_filesystem())
            .schema(Arc::new(arrow::datatypes::Schema::empty()))
            .files(Vec::new())
            .statistics(Statistics::new_unknown(&arrow::datatypes::Schema::empty()))
            .output_ordering(Vec::new())
            .format(Arc::new(
                datafusion::datasource::file_format::arrow::ArrowFormat,
            ))
            .build()
            .unwrap_err();
        assert!(error.to_string().contains("at least one file"));
    }
}
