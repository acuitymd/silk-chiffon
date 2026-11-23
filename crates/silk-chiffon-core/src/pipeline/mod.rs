//! DataFusion planning and execution for one transform command.

mod config;
mod error;
mod input;
mod memory_pool;
mod sort;

use std::{
    fmt,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use camino::Utf8PathBuf;
use datafusion::{
    error::DataFusionError,
    execution::{
        TaskContext,
        memory_pool::{MemoryPool, TrackConsumersPool},
    },
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, RecordBatchStream, SendableRecordBatchStream,
        execute_stream,
    },
    prelude::{DataFrame, SessionConfig, SessionContext, ident},
};
use futures::Stream;
use sysinfo::System;
use tempfile::TempDir;

use memory_pool::ReservedSpillPool;
use sort::{ParallelSortExec, ParallelSortOptions};

use crate::{NullPlacement, SortColumn, SortDirection};

pub use config::{QueryDialect, SortEngine, SpillCompression};
pub use error::{PipelineExecutionStartError, PipelinePreparationError};
pub use input::union_input_providers_by_name;

struct PipelineConfig {
    query_dialect: QueryDialect,
    memory_limit: Option<usize>,
    target_partitions: Option<usize>,
    spill_path: Option<Utf8PathBuf>,
    spill_compression: SpillCompression,
    max_spill_size: Option<usize>,
    max_spill_merge_fan_in: Option<usize>,
    sort_spill_reservation_bytes: Option<usize>,
    non_spillable_reserve: Option<usize>,
    memory_pool_top_consumers: usize,
    sort: Vec<SortColumn>,
    sort_engine: SortEngine,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            query_dialect: QueryDialect::default(),
            memory_limit: None,
            target_partitions: None,
            spill_path: None,
            spill_compression: SpillCompression::default(),
            max_spill_size: None,
            max_spill_merge_fan_in: None,
            sort_spill_reservation_bytes: None,
            non_spillable_reserve: None,
            memory_pool_top_consumers: 10,
            sort: Vec::new(),
            sort_engine: SortEngine::default(),
        }
    }
}

/// A transform definition before its final DataFusion plan has been built.
///
/// The host creates the session first, constructs every input provider in that
/// session, and applies command-owned transformations before passing the final
/// logical frame here. Output behavior is intentionally absent.
#[derive(Default)]
pub struct Pipeline {
    config: PipelineConfig,
    spill_path: Option<TempDir>,
}

impl Pipeline {
    /// Creates an empty pipeline with default execution settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects the SQL dialect used by logical query operations.
    pub fn with_query_dialect(mut self, dialect: QueryDialect) -> Self {
        self.config.query_dialect = dialect;
        self
    }

    /// Sets the DataFusion memory-pool limit in bytes. The pool never grants
    /// more than this. Without one, the limit is 80% of the machine's memory.
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

    /// Caps the total bytes of spill files live at once, replacing DataFusion's
    /// 100 GiB default. Execution fails when a spill write would exceed the cap.
    pub fn with_max_spill_size(mut self, max_spill_size: Option<usize>) -> Self {
        self.config.max_spill_size = max_spill_size;
        self
    }

    /// Caps how many spill files one sort or aggregation merge pass reads, replacing
    /// DataFusion's unlimited default. Each file in a pass holds non-spillable memory, so
    /// the cap bounds merge memory at the cost of extra passes through disk.
    ///
    /// It applies to DataFusion's sort and aggregations. The parallel sort sizes its
    /// own merges from the memory pool and the open-file limit, and does not read it.
    pub fn with_max_spill_merge_fan_in(mut self, fan_in: Option<usize>) -> Self {
        self.config.max_spill_merge_fan_in = fan_in;
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

    /// Reserves pool capacity for operators that cannot spill. Spillable
    /// operators, such as sorts, never hold it, so a reader or a merge that
    /// cannot spill finds it free. Without one, the reserve is a fifth of the
    /// pool.
    pub fn with_non_spillable_reserve(mut self, reserve: Option<usize>) -> Self {
        self.config.non_spillable_reserve = reserve;
        self
    }

    /// Sets how many memory consumers an allocation failure reports.
    pub fn with_memory_pool_top_consumers(mut self, count: usize) -> Self {
        self.config.memory_pool_top_consumers = count;
        self
    }

    /// Sorts the final plan's output by these columns. Empty means unsorted.
    pub fn with_sort(mut self, columns: Vec<SortColumn>) -> Self {
        self.config.sort = columns;
        self
    }

    /// Selects the operator that performs [`Self::with_sort`].
    pub fn with_sort_engine(mut self, engine: SortEngine) -> Self {
        self.config.sort_engine = engine;
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

        // a pool of no bytes refuses every request, which is the caller's to
        // see as an error, where ReservedSpillPool would panic
        let memory_limit = self
            .config
            .memory_limit
            .unwrap_or_else(default_memory_limit)
            .max(1);
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
        let reserve = self
            .config
            .non_spillable_reserve
            .unwrap_or(memory_limit / DEFAULT_RESERVE_DIVISOR);
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            ReservedSpillPool::new(memory_limit, reserve),
            top_n,
        ));
        let mut runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::default()
            .with_temp_file_path(&spill_path)
            .with_memory_pool(pool);
        if let Some(max_spill_size) = self.config.max_spill_size {
            runtime = runtime.with_max_temp_directory_size(max_spill_size as u64);
        }
        if let Some(fan_in) = self.config.max_spill_merge_fan_in {
            runtime = runtime.with_max_spill_merge_fan_in(fan_in);
        }
        let runtime = runtime.build()?;
        Ok(SessionContext::new_with_config_rt(
            config,
            Arc::new(runtime),
        ))
    }

    /// Builds, validates, and retains the final physical plan.
    pub async fn prepare(
        mut self,
        input: DataFrame,
        session: SessionContext,
    ) -> std::result::Result<PreparedPipeline, PipelinePreparationError> {
        self.prepare_inner(input, &session)
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
        data_frame: DataFrame,
        session: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.plan_with_sort(data_frame, session).await?;
        if plan.properties().boundedness.is_unbounded() {
            anyhow::bail!("current outputs require a bounded final plan");
        }
        Ok(plan)
    }

    async fn plan_with_sort(
        &self,
        data_frame: DataFrame,
        session: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let columns = &self.config.sort;
        if columns.is_empty() {
            return Ok(data_frame.create_physical_plan().await?);
        }
        // the names and key types are checked before planning, since planning
        // twice can do remote work twice, such as opening a read session
        let logical_schema = Arc::new(data_frame.schema().as_arrow().clone());
        if self.config.sort_engine == SortEngine::Parallel
            && physical_ordering(&logical_schema, columns)
                .is_some_and(|ordering| ParallelSortExec::supports(&logical_schema, &ordering))
        {
            let unsorted = data_frame.clone().create_physical_plan().await?;
            // an input that is already in order needs no sort, and DataFusion's
            // planner is what knows how to keep that order
            if let Some(ordering) = physical_ordering(&unsorted.schema(), columns)
                && !unsorted
                    .equivalence_properties()
                    .ordering_satisfy(ordering.clone())?
                && ParallelSortExec::supports(&unsorted.schema(), &ordering)
            {
                let parallelism = self
                    .config
                    .target_partitions
                    .unwrap_or_else(|| session.state().config().target_partitions());
                return Ok(Arc::new(ParallelSortExec::try_new(
                    unsorted,
                    ordering,
                    ParallelSortOptions::new(parallelism),
                )?));
            }
        }
        Ok(sort_data_frame(data_frame, columns)?
            .create_physical_plan()
            .await?)
    }
}

fn sort_data_frame(data_frame: DataFrame, columns: &[SortColumn]) -> Result<DataFrame> {
    Ok(data_frame.sort(
        columns
            .iter()
            .map(|column| {
                // the name as it is: `col` would fold its case and split it at
                // dots, where the parallel sort takes the name exactly
                ident(column.name()).sort(
                    column.direction() == SortDirection::Ascending,
                    column.null_placement() == NullPlacement::First,
                )
            })
            .collect(),
    )?)
}

/// The sort as an ordering over the columns of `schema`. `None` when a column
/// is not in it under that exact name, which leaves resolving the name to
/// DataFusion's planner.
fn physical_ordering(schema: &SchemaRef, columns: &[SortColumn]) -> Option<LexOrdering> {
    let expressions = columns
        .iter()
        .map(|column| {
            let expression = Column::new_with_schema(column.name(), schema).ok()?;
            Some(PhysicalSortExpr::new(
                Arc::new(expression),
                arrow::compute::SortOptions {
                    descending: column.direction() == SortDirection::Descending,
                    nulls_first: column.null_placement() == NullPlacement::First,
                },
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    LexOrdering::new(expressions)
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
///
/// DataFusion defines dropping the execution stream as cancellation. Sources therefore bind their
/// tasks and channels to the inner stream instead of relying on a separate pipeline token.
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

impl fmt::Debug for PipelineExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineExecution")
            .finish_non_exhaustive()
    }
}

/// The pool's share kept for consumers that cannot spill when no reserve is
/// given: a fifth. Input readers and merges that cannot spill then find room
/// however full the sorts keep the rest.
const DEFAULT_RESERVE_DIVISOR: usize = 5;

/// 80% of the machine's memory.
fn default_memory_limit() -> usize {
    total_memory() * 4 / 5
}

#[allow(clippy::cast_possible_truncation)]
fn total_memory() -> usize {
    #[cfg(target_os = "linux")]
    if let Some(limit) = cgroup_total_memory() {
        return limit;
    }
    System::new_all().map_or(0, |system| system.total_memory() as usize)
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

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::TryStreamExt;

    use super::*;

    /// Sorts one nullable column through a whole pipeline and returns the plan's
    /// top operator with the sorted values.
    async fn sorted_ids(engine: SortEngine, column: SortColumn) -> (String, Vec<Option<i32>>) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![
                Some(3),
                None,
                Some(1),
                None,
                Some(2),
            ]))],
        )
        .unwrap();
        let mut pipeline = Pipeline::new()
            .with_sort(vec![column])
            .with_sort_engine(engine);
        let session = pipeline.create_session_context().unwrap();
        let input = session.read_batch(batch).unwrap();
        let prepared = pipeline.prepare(input, session).await.unwrap();
        let operator = prepared.execution_plan().name().to_owned();
        let batches: Vec<RecordBatch> = prepared
            .begin_execution()
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        (operator, ids)
    }

    /// A column whose name has capitals is sorted by both engines, where
    /// DataFusion's would fold the name to lower case.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_sort_engine_takes_a_column_name_as_it_is() {
        for engine in [SortEngine::Parallel, SortEngine::DataFusion] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "UserId",
                DataType::Int32,
                true,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![Some(2), None, Some(1)]))],
            )
            .unwrap();
            let mut pipeline = Pipeline::new()
                .with_sort(vec![SortColumn::new(
                    "UserId",
                    SortDirection::Ascending,
                    NullPlacement::Last,
                )])
                .with_sort_engine(engine);
            let session = pipeline.create_session_context().unwrap();
            let input = session.read_batch(batch).unwrap();
            let prepared = pipeline.prepare(input, session).await.unwrap();
            let batches: Vec<RecordBatch> = prepared
                .begin_execution()
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let ids: Vec<Option<i32>> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(ids, vec![Some(1), Some(2), None], "{engine}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_sort_engine_honors_direction_and_null_placement() {
        for (engine, operator) in [
            (SortEngine::Parallel, "ParallelSortExec"),
            (SortEngine::DataFusion, "SortExec"),
        ] {
            let (planned, ascending) = sorted_ids(
                engine,
                SortColumn::new("id", SortDirection::Ascending, NullPlacement::First),
            )
            .await;
            assert_eq!(planned, operator);
            assert_eq!(ascending, vec![None, None, Some(1), Some(2), Some(3)]);

            let (_, descending) = sorted_ids(
                engine,
                SortColumn::new("id", SortDirection::Descending, NullPlacement::Last),
            )
            .await;
            assert_eq!(descending, vec![Some(3), Some(2), Some(1), None, None]);
        }
    }

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
    fn a_pool_of_no_bytes_refuses_memory_instead_of_panicking() {
        let session = Pipeline::new()
            .with_memory_limit(Some(0))
            .create_session_context()
            .unwrap();
        let pool = &session.runtime_env().memory_pool;
        let reader =
            datafusion::execution::memory_pool::MemoryConsumer::new("reader").register(pool);
        assert!(reader.try_grow(1024).is_err());
    }

    #[test]
    fn spill_settings_reach_the_disk_manager_together() {
        let mut pipeline = Pipeline::new()
            .with_max_spill_size(Some(1 << 30))
            .with_max_spill_merge_fan_in(Some(2));
        let session = pipeline.create_session_context().unwrap();
        let disk_manager = &session.runtime_env().disk_manager;

        assert_eq!(disk_manager.max_spill_merge_fan_in(), 2);
        assert_eq!(disk_manager.max_temp_directory_size(), 1 << 30);
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
