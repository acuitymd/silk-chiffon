use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::{any::Any, error::Error};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow_schema as arrow_schema_v56;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::{Expr, SessionContext};
use futures::StreamExt;
use vortex::VortexSessionDefault;
use vortex::arrow::IntoArrowArray;
use vortex::file::OpenOptionsSessionExt;
use vortex_session::VortexSession;

use arrow_array::RecordBatch as RecordBatchv56;
use arrow_array::StructArray as StructArrayv56;

use crate::{sources::data_source::DataSource, utils::arrow_versioning::convert_arrow_56_to_57};

pub struct VortexDataSource {
    path: String,
}

impl VortexDataSource {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

#[async_trait]
impl DataSource for VortexDataSource {
    fn name(&self) -> &str {
        "vortex"
    }

    fn schema(&self) -> Result<SchemaRef> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let session = VortexSession::default();
                let vortex_file = session
                    .open_options()
                    .open(self.path.as_str())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to open Vortex file: {}", e))?;

                let dtype = vortex_file.dtype();
                let arrow_schema_v56 = dtype.to_arrow_schema().map_err(|e| {
                    anyhow::anyhow!("Failed to convert Vortex DType to Arrow Schema: {}", e)
                })?;

                let batch_v56 = RecordBatchv56::new_empty(Arc::new(arrow_schema_v56));
                let batch_v57 = convert_arrow_56_to_57(&batch_v56)?;

                Ok(batch_v57.schema())
            })
        })
    }

    async fn as_table_provider(&self, _ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(VortexTableProvider {
            path: self.path.clone(),
            schema: self.schema()?,
        }))
    }

    fn supports_table_provider(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct VortexTableProvider {
    path: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for VortexTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(VortexExec::new(
            self.path.clone(),
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
        )?))
    }
}

struct VortexExec {
    path: PathBuf,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl VortexExec {
    pub fn new(
        path: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self, DataFusionError> {
        let projected_schema = if let Some(ref proj) = projection {
            Arc::new(
                schema
                    .project(proj)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            )
        } else {
            Arc::clone(&schema)
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            path: PathBuf::from(path),
            schema,
            projection,
            limit,
            properties,
        })
    }
}

// world's simplest execution plan. we will absolutely switch to datafusion-vortex
// once it's updated to support datafusion 51 and arrow 57
impl ExecutionPlan for VortexExec {
    fn name(&self) -> &str {
        "VortexExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        if let Some(ref proj) = self.projection {
            Arc::new(self.schema.project(proj).expect("projection valid"))
        } else {
            Arc::clone(&self.schema)
        }
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "VortexExec cannot have children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "VortexExec only supports 1 partition".into(),
            ));
        }

        let path = self.path.clone();
        let schema = self.schema();
        let projection = self.projection.clone();
        let limit = self.limit;

        let stream = async_stream::stream! {
            let session = VortexSession::default();
            let vortex_file = session
                .open_options()
                .open(path.to_str().unwrap())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut rows_read = 0;
            let scan = vortex_file
                .scan()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let array_stream = scan
                .into_array_stream()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut array_stream = Box::pin(array_stream);

            let vortex_dtype = vortex_file.dtype();
            let arrow_schema_v56 = vortex_dtype
                .to_arrow_schema()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let arrow_data_type_v56 = arrow_schema_v56::DataType::Struct(arrow_schema_v56.fields().clone());

            while let Some(array_result) = array_stream.next().await {
                let vortex_array = array_result
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let arrow_array_v56 = vortex_array
                    .into_arrow(&arrow_data_type_v56)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let struct_array_v56 = arrow_array_v56
                    .as_any()
                    .downcast_ref::<StructArrayv56>()
                    .ok_or_else(|| DataFusionError::Internal("Expected StructArray from Vortex".into()))?;

                let batch_v56 = RecordBatchv56::from(struct_array_v56);

                let mut batch = convert_arrow_56_to_57(&batch_v56)
                    .map_err(|e| DataFusionError::External(Box::<dyn Error + Send + Sync>::from(e)))?;

                if let Some(ref proj) = projection {
                    batch = batch.project(proj)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                }

                if let Some(lim) = limit {
                    if rows_read >= lim {
                        break;
                    }
                    let remaining = lim - rows_read;
                    if batch.num_rows() > remaining {
                        batch = batch.slice(0, remaining);
                    }
                }

                rows_read += batch.num_rows();
                yield Ok(batch);

                if let Some(lim) = limit
                    && rows_read >= lim {
                        break;
                    }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for VortexExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VortexExec: path={}", self.path.display())
    }
}

impl fmt::Debug for VortexExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("VortexExec")
            .field("path", &self.path)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}
