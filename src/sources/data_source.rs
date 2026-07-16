//! Async source metadata and object-store-backed DataFusion providers.
//!
//! Sources retain the metadata found during input resolution. Arrow and
//! Parquet providers turn that metadata into a static DataFusion file scan, so
//! provider construction does not repeat a `head` request.

use std::{fmt, sync::Arc};

use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::{
        file_format::FileFormat, listing::PartitionedFile, physical_plan::FileScanConfigBuilder,
        table_schema::TableSchema,
    },
    execution::{SendableRecordBatchStream, object_store::ObjectStoreUrl},
    logical_expr::{Expr, TableType},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use object_store::ObjectMeta;

use crate::storage::InputObject;

#[derive(Clone)]
pub(crate) struct SourceMetadata {
    pub schema: SchemaRef,
    pub row_count: usize,
}

#[async_trait]
pub trait DataSource: Send + Sync {
    fn name(&self) -> &str;

    fn input(&self) -> &InputObject;

    async fn schema(&self) -> Result<SchemaRef>;

    async fn row_count(&self) -> Result<usize>;

    async fn as_table_provider(&self, _ctx: &SessionContext) -> Result<Arc<dyn TableProvider>> {
        Err(anyhow!("as_table_provider is not implemented"))
    }

    fn supports_table_provider(&self) -> bool {
        false
    }

    /// Builds a stream in an isolated session with this source's store registered.
    async fn as_stream(&self) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::new();
        let store = self.input().location().store();
        ctx.register_object_store(store.store_url().as_ref(), Arc::clone(store.object_store()));
        self.as_stream_with_session_context(&ctx).await
    }

    /// Builds a stream using a context where the caller has registered the source store.
    async fn as_stream_with_session_context(
        &self,
        ctx: &SessionContext,
    ) -> Result<SendableRecordBatchStream> {
        let table = self.as_table_provider(ctx).await?;
        let df = ctx.read_table(table)?;
        df.execute_stream().await.map_err(|e| anyhow!(e))
    }
}

pub(crate) fn object_store_table_provider(
    input: &InputObject,
    metadata: &SourceMetadata,
    format: Arc<dyn FileFormat>,
) -> Arc<dyn TableProvider> {
    Arc::new(ObjectStoreTableProvider {
        schema: Arc::clone(&metadata.schema),
        object_meta: input.metadata().clone(),
        store_url: input.location().store().store_url().clone(),
        format,
    })
}

struct ObjectStoreTableProvider {
    schema: SchemaRef,
    object_meta: ObjectMeta,
    store_url: ObjectStoreUrl,
    format: Arc<dyn FileFormat>,
}

impl fmt::Debug for ObjectStoreTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreTableProvider")
            .field("location", &self.object_meta.location)
            .field("store_url", &self.store_url)
            .field("format", &self.format)
            .finish()
    }
}

#[async_trait]
impl TableProvider for ObjectStoreTableProvider {
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
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let table_schema = TableSchema::from_file_schema(Arc::clone(&self.schema));
        let source = self.format.file_source(table_schema);
        let config = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file(PartitionedFile::new_from_meta(self.object_meta.clone()))
            .with_limit(limit)
            .with_projection_indices(projection.cloned())?
            .build();
        self.format.create_physical_plan(state, config).await
    }
}
