//! DataFusion planning and execution for one transform command.

mod config;
mod error;
mod input_sources;
mod memory_pool;

use std::{
    fmt,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use camino::Utf8PathBuf;
use datafusion::{
    error::DataFusionError,
    execution::{
        TaskContext,
        memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool},
    },
    physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream, execute_stream},
    prelude::{SessionConfig, SessionContext},
};
use futures::Stream;
use sysinfo::System;
use tempfile::TempDir;

use crate::DataOperation;
use memory_pool::ReservedSpillPool;

pub use config::{QueryDialect, SpillCompression};
pub use error::{PipelineExecutionStartError, PipelinePreparationError};
pub use input_sources::InputSources;

struct PipelineConfig {
    query_dialect: QueryDialect,
    memory_limit: Option<usize>,
    target_partitions: Option<usize>,
    spill_path: Option<Utf8PathBuf>,
    spill_compression: SpillCompression,
    sort_spill_reservation_bytes: Option<usize>,
    non_spillable_reserve: Option<usize>,
    memory_pool_top_consumers: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            query_dialect: QueryDialect::default(),
            memory_limit: None,
            target_partitions: None,
            spill_path: None,
            spill_compression: SpillCompression::default(),
            sort_spill_reservation_bytes: None,
            non_spillable_reserve: None,
            memory_pool_top_consumers: 10,
        }
    }
}

/// A transform definition before its final DataFusion plan has been built.
///
/// The host creates the session first, constructs every input provider in that
/// session, and then attaches the nonempty inputs and logical operations. Output
/// behavior is intentionally absent.
#[derive(Default)]
pub struct Pipeline {
    inputs: Option<InputSources>,
    operations: Vec<Box<dyn DataOperation>>,
    config: PipelineConfig,
    spill_path: Option<TempDir>,
}

impl Pipeline {
    /// Creates an empty pipeline with default execution settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the command's nonempty logical input.
    pub fn with_inputs(mut self, inputs: InputSources) -> Self {
        self.inputs = Some(inputs);
        self
    }

    /// Appends an operation in command order.
    pub fn with_operation(mut self, operation: Box<dyn DataOperation>) -> Self {
        self.operations.push(operation);
        self
    }

    /// Selects the SQL dialect used by logical query operations.
    pub fn with_query_dialect(mut self, dialect: QueryDialect) -> Self {
        self.config.query_dialect = dialect;
        self
    }

    /// Sets the DataFusion memory-pool limit in bytes.
    pub fn with_memory_limit(mut self, memory_limit: Option<usize>) -> Self {
        self.config.memory_limit = memory_limit;
        self
    }

    /// Sets the number of DataFusion execution partitions.
    pub fn with_target_partitions(mut self, target_partitions: Option<usize>) -> Self {
        self.config.target_partitions = target_partitions;
        self
    }

    /// Selects a persistent spill directory or requests a managed temporary one.
    pub fn with_spill_path(mut self, spill_path: Option<Utf8PathBuf>) -> Self {
        self.config.spill_path = spill_path;
        self
    }

    /// Selects the compression used for DataFusion spill files.
    pub fn with_spill_compression(mut self, spill_compression: SpillCompression) -> Self {
        self.config.spill_compression = spill_compression;
        self
    }

    /// Reserves bytes for the merge phase of each sort execution.
    pub fn with_sort_spill_reservation_bytes(
        mut self,
        sort_spill_reservation_bytes: Option<usize>,
    ) -> Self {
        self.config.sort_spill_reservation_bytes = sort_spill_reservation_bytes;
        self
    }

    /// Reserves pool capacity for operators that cannot spill.
    pub fn with_non_spillable_reserve(mut self, reserve: Option<usize>) -> Self {
        self.config.non_spillable_reserve = reserve;
        self
    }

    /// Sets how many memory consumers an allocation failure reports.
    pub fn with_memory_pool_top_consumers(mut self, count: usize) -> Self {
        self.config.memory_pool_top_consumers = count;
        self
    }

    /// Creates the session shared by input-provider construction, planning, and execution.
    pub fn create_session_context(
        &mut self,
    ) -> std::result::Result<SessionContext, PipelinePreparationError> {
        self.create_session_context_inner()
            .map_err(PipelinePreparationError::new)
    }

    fn create_session_context_inner(&mut self) -> Result<SessionContext> {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.map_string_types_to_utf8view = false;
        config.options_mut().sql_parser.dialect = self.config.query_dialect.into();
        config.options_mut().execution.spill_compression = self.config.spill_compression.into();
        if let Some(reservation) = self.config.sort_spill_reservation_bytes {
            config.options_mut().execution.sort_spill_reservation_bytes = reservation;
        }
        if let Some(target_partitions) = self.config.target_partitions {
            config = config.with_target_partitions(target_partitions);
        }

        let memory_limit = self
            .config
            .memory_limit
            .unwrap_or_else(default_memory_limit);
        let spill_path = if let Some(path) = &self.config.spill_path {
            path.clone()
        } else {
            let directory = tempfile::Builder::new()
                .prefix("silk-chiffon-spill-")
                .tempdir()?;
            let path = directory.path().to_path_buf();
            self.spill_path = Some(directory);
            path.try_into()?
        };
        let top_n = match self.config.memory_pool_top_consumers {
            0 => NonZeroUsize::MAX,
            count => NonZeroUsize::new(count).expect("nonzero by match"),
        };
        let pool: Arc<dyn MemoryPool> = match self.config.non_spillable_reserve {
            Some(reserve) => Arc::new(TrackConsumersPool::new(
                ReservedSpillPool::new(memory_limit, reserve),
                top_n,
            )),
            None => Arc::new(TrackConsumersPool::new(
                FairSpillPool::new(memory_limit),
                top_n,
            )),
        };
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::default()
            .with_temp_file_path(&spill_path)
            .with_memory_pool(pool)
            .build()?;
        Ok(SessionContext::new_with_config_rt(
            config,
            Arc::new(runtime),
        ))
    }

    /// Builds, validates, and retains the final physical plan.
    pub async fn prepare(
        mut self,
        mut session: SessionContext,
    ) -> std::result::Result<PreparedPipeline, PipelinePreparationError> {
        self.prepare_inner(&mut session)
            .await
            .map_err(PipelinePreparationError::new)
            .map(|plan| PreparedPipeline {
                plan,
                session,
                sort_spill_reservation_bytes: self.config.sort_spill_reservation_bytes,
                spill_path: self.spill_path,
            })
    }

    async fn prepare_inner(
        &mut self,
        session: &mut SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inputs = self
            .inputs
            .take()
            .ok_or_else(|| anyhow!("no inputs provided"))?;
        let mut data_frame = inputs.data_frame();
        for operation in &self.operations {
            data_frame = operation.apply(session, data_frame).await?;
        }
        let plan = data_frame.create_physical_plan().await?;
        if plan.properties().boundedness.is_unbounded() {
            anyhow::bail!("current outputs require a bounded final plan");
        }
        Ok(plan)
    }
}

/// A validated physical plan awaiting execution.
pub struct PreparedPipeline {
    plan: Arc<dyn ExecutionPlan>,
    session: SessionContext,
    sort_spill_reservation_bytes: Option<usize>,
    spill_path: Option<TempDir>,
}

impl PreparedPipeline {
    /// Returns the session shared by planning, binding, and execution.
    pub fn session(&self) -> &SessionContext {
        &self.session
    }

    /// Returns the schema of the exact validated physical plan.
    pub fn output_schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    /// Returns the validated physical plan for final-plan resource tuning.
    pub fn execution_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }

    /// Replaces the sort reservation after final-plan statistics are evaluated.
    pub fn with_sort_spill_reservation_bytes(mut self, reservation: Option<usize>) -> Self {
        self.sort_spill_reservation_bytes = reservation;
        self
    }

    /// Starts the retained plan and transfers all polling keepalives to the
    /// returned stream.
    pub fn begin_execution(
        self,
    ) -> std::result::Result<PipelineExecution, PipelineExecutionStartError> {
        let mut task_context = self.session.task_ctx();
        if let Some(reservation) = self.sort_spill_reservation_bytes {
            let config = task_context
                .session_config()
                .clone()
                .with_sort_spill_reservation_bytes(reservation);
            task_context = Arc::new(TaskContext::new(
                task_context.task_id(),
                task_context.session_id(),
                config,
                task_context.scalar_functions().clone(),
                task_context.higher_order_functions().clone(),
                task_context.aggregate_functions().clone(),
                task_context.window_functions().clone(),
                task_context.runtime_env(),
            ));
        }
        let inner = execute_stream(Arc::clone(&self.plan), task_context)
            .map_err(|source| PipelineExecutionStartError { source })?;
        Ok(PipelineExecution {
            inner,
            _session: self.session,
            _spill_path: self.spill_path,
        })
    }
}

/// The running final plan together with every resource required while it is polled.
pub struct PipelineExecution {
    inner: SendableRecordBatchStream,
    _session: SessionContext,
    _spill_path: Option<TempDir>,
}

impl PipelineExecution {
    /// Boxes this complete execution without detaching the inner stream from
    /// its keepalives.
    pub fn into_sendable_stream(self) -> SendableRecordBatchStream {
        Box::pin(self)
    }
}

impl Stream for PipelineExecution {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(context)
    }
}

impl RecordBatchStream for PipelineExecution {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use super::*;

    #[test]
    fn new_and_default_sessions_use_the_same_memory_diagnostics() {
        fn memory_pool_description(mut pipeline: Pipeline) -> String {
            let session = pipeline.create_session_context().unwrap();
            session.runtime_env().memory_pool.to_string()
        }

        let from_new = memory_pool_description(Pipeline::new());
        let from_default = memory_pool_description(Pipeline::default());

        assert_eq!(from_new, from_default);
        assert!(from_default.contains("num_of_top_consumers: 10"));
    }

    #[test]
    fn boxed_execution_retains_and_releases_its_spill_directory() {
        let spill_path = tempfile::tempdir().unwrap();
        let directory = spill_path.path().to_path_buf();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::empty(),
        ));
        let execution = PipelineExecution {
            inner,
            _session: SessionContext::new(),
            _spill_path: Some(spill_path),
        };

        let stream = execution.into_sendable_stream();
        assert!(directory.exists());
        drop(stream);
        assert!(!directory.exists());
    }
}

impl fmt::Debug for PipelineExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineExecution")
            .finish_non_exhaustive()
    }
}

fn default_memory_limit() -> usize {
    total_memory() * 4 / 5
}

#[allow(clippy::cast_possible_truncation)]
fn total_memory() -> usize {
    #[cfg(target_os = "linux")]
    if let Some(limit) = cgroup_total_memory() {
        return limit;
    }
    System::new_all().total_memory() as usize
}

#[cfg(target_os = "linux")]
#[allow(clippy::cast_possible_truncation)]
fn cgroup_total_memory() -> Option<usize> {
    let v2 = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|content| parse_memory_max(&content))
        .map(|value| value as usize);
    v2.or_else(|| {
        std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
            .ok()
            .and_then(|content| parse_cgroup_v1_limit(&content))
            .map(|value| value as usize)
    })
}

#[cfg(target_os = "linux")]
fn parse_memory_max(content: &str) -> Option<u64> {
    let content = content.trim();
    (content != "max").then(|| content.parse().ok()).flatten()
}

#[cfg(target_os = "linux")]
fn parse_cgroup_v1_limit(content: &str) -> Option<u64> {
    const ONE_PETABYTE: u64 = 1_125_899_906_842_624;
    content
        .trim()
        .parse()
        .ok()
        .filter(|limit| *limit < ONE_PETABYTE)
}
