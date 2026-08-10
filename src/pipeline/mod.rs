//! DataFusion planning and execution for one transform command.

mod memory_pool;

use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use camino::Utf8PathBuf;
use datafusion::{
    execution::{
        TaskContext,
        memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool},
    },
    physical_plan::{ExecutionPlan, execute_stream},
    prelude::{SessionConfig, SessionContext},
};
use silk_chiffon_storage::StorageHandle;
use tempfile::TempDir;

use memory_pool::ReservedSpillPool;

use crate::utils::memory::total_memory;
use crate::{
    ListOutputsFormat, QueryDialect, SpillCompression,
    io_strategies::{
        OutputFileInfo,
        input_sources::InputSources,
        output_strategy::{OutputStrategy, SinkOpenerFn},
        path_template::PathTemplate,
    },
    operations::data_operation::DataOperation,
};

/// Settings used to construct a transform command's DataFusion session.
pub struct PipelineConfig {
    pub working_directory: Option<String>,
    pub query_dialect: QueryDialect,
    pub memory_limit: Option<usize>,
    pub target_partitions: Option<usize>,
    pub spill_path: Option<Utf8PathBuf>,
    pub spill_compression: SpillCompression,
    pub sort_spill_reservation_bytes: Option<usize>,
    pub non_spillable_reserve: Option<usize>,
    pub memory_pool_top_consumers: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            working_directory: None,
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

#[derive(Default)]
/// A transform definition before its final DataFusion plan has been built.
///
/// The host creates the session first, constructs every source with that session, then attaches
/// sources and logical operations here. [`Self::prepare`] builds and validates the completed
/// physical plan. Output configuration belongs to [`PreparedPipeline`] because formats bind sinks
/// only after that validation succeeds.
pub struct Pipeline {
    inputs: Option<InputSources>,
    operations: Vec<Box<dyn DataOperation>>,
    storage_handles: Vec<StorageHandle>,
    config: PipelineConfig,
    /// temp directory for spilling when memory_limit is set - kept alive until Pipeline drops
    spill_path: Option<TempDir>,
}

impl Pipeline {
    /// Creates an empty transform definition with default execution settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the command's nonempty logical input.
    pub fn with_inputs(mut self, inputs: InputSources) -> Self {
        self.inputs = Some(inputs);
        self
    }

    /// Appends an operation to the logical plan in command order.
    pub fn with_operation(mut self, operation: Box<dyn DataOperation>) -> Self {
        self.operations.push(operation);

        self
    }

    /// Registers an input's object store when the DataFusion session is created or prepared.
    pub fn with_storage_handle(mut self, handle: StorageHandle) -> Self {
        self.storage_handles.push(handle);
        self
    }

    pub fn with_working_directory(mut self, working_directory: String) -> Self {
        self.config.working_directory = Some(working_directory);
        self
    }

    pub fn with_query_dialect(mut self, dialect: QueryDialect) -> Self {
        self.config.query_dialect = dialect;
        self
    }

    pub fn with_memory_limit(mut self, memory_limit: Option<usize>) -> Self {
        self.config.memory_limit = memory_limit;
        self
    }

    pub fn with_target_partitions(mut self, target_partitions: Option<usize>) -> Self {
        self.config.target_partitions = target_partitions;
        self
    }

    pub fn with_spill_path(mut self, spill_path: Option<Utf8PathBuf>) -> Self {
        self.config.spill_path = spill_path;
        self
    }

    pub fn with_spill_compression(mut self, spill_compression: SpillCompression) -> Self {
        self.config.spill_compression = spill_compression;
        self
    }

    pub fn with_sort_spill_reservation_bytes(
        mut self,
        sort_spill_reservation_bytes: Option<usize>,
    ) -> Self {
        self.config.sort_spill_reservation_bytes = sort_spill_reservation_bytes;
        self
    }

    pub fn with_non_spillable_reserve(mut self, reserve: Option<usize>) -> Self {
        self.config.non_spillable_reserve = reserve;
        self
    }

    pub fn with_memory_pool_top_consumers(mut self, n: usize) -> Self {
        self.config.memory_pool_top_consumers = n;
        self
    }

    /// Builds and validates the final physical plan in the source's DataFusion session.
    ///
    /// Every logical operation is attached before the plan is built. DataFusion's final physical
    /// boundedness property is authoritative; an unbounded plan is rejected because the available
    /// sinks require completion.
    pub async fn prepare(mut self, mut session: SessionContext) -> Result<PreparedPipeline> {
        for handle in &self.storage_handles {
            session
                .runtime_env()
                .register_object_store(handle.store_url(), handle.object_store());
        }
        let inputs = self
            .inputs
            .take()
            .ok_or_else(|| anyhow!("No input sources provided"))?;
        let provider = inputs.table_provider(&session).await?;
        let mut data_frame = session.read_table(provider)?;
        for operation in &self.operations {
            data_frame = operation.apply(&mut session, data_frame).await?;
        }
        let plan = data_frame.create_physical_plan().await?;
        if plan.properties().boundedness.is_unbounded() {
            anyhow::bail!("current output formats require a bounded input plan");
        }
        Ok(PreparedPipeline {
            inputs,
            output_strategy: None,
            plan,
            session,
            sort_spill_reservation_bytes: self.config.sort_spill_reservation_bytes,
            spill_path: self.spill_path,
        })
    }

    /// Creates the DataFusion session that source construction and execution must share.
    pub fn create_session_context(&mut self) -> Result<SessionContext> {
        let mut cfg = SessionConfig::new();

        // DuckDB doesn't like joining Datatype::Utf8View to Datatype::Utf8, so we disable
        // the automatic mapping of all string types to Datatype::Utf8View.
        // https://datafusion.apache.org/library-user-guide/upgrading.html#new-map-string-types-to-utf8view-configuration-option
        cfg.options_mut().sql_parser.map_string_types_to_utf8view = false;

        cfg.options_mut().sql_parser.dialect = self.config.query_dialect.into();
        cfg.options_mut().execution.spill_compression = self.config.spill_compression.into();

        if let Some(reservation) = self.config.sort_spill_reservation_bytes {
            cfg.options_mut().execution.sort_spill_reservation_bytes = reservation;
        }

        if let Some(target_partitions) = self.config.target_partitions {
            cfg = cfg.with_target_partitions(target_partitions);
        }

        let memory_limit = self
            .config
            .memory_limit
            .unwrap_or_else(default_memory_limit);

        // use user-provided spill path or create a temp one
        let spill_path = if let Some(ref user_path) = self.config.spill_path {
            user_path.clone()
        } else {
            let spill_path = tempfile::Builder::new()
                .prefix("silk-chiffon-spill-")
                .tempdir()?;
            let path = spill_path.path().to_path_buf();
            self.spill_path = Some(spill_path);
            path.try_into()?
        };

        let top_n = match self.config.memory_pool_top_consumers {
            0 => NonZeroUsize::MAX,
            n => NonZeroUsize::new(n).expect("nonzero by match"),
        };

        let pool: Arc<dyn MemoryPool> = match self.config.non_spillable_reserve {
            Some(reserve) => {
                let inner = ReservedSpillPool::new(memory_limit, reserve);
                Arc::new(TrackConsumersPool::new(inner, top_n))
            }
            None => {
                let inner = FairSpillPool::new(memory_limit);
                Arc::new(TrackConsumersPool::new(inner, top_n))
            }
        };

        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::default()
            .with_temp_file_path(&spill_path)
            .with_memory_pool(pool)
            .build()?;

        let context = SessionContext::new_with_config_rt(cfg, std::sync::Arc::new(runtime));
        for handle in &self.storage_handles {
            context
                .runtime_env()
                .register_object_store(handle.store_url(), handle.object_store());
        }

        Ok(context)
    }
}

/// A validated physical plan awaiting output configuration and execution.
///
/// The retained plan is the exact plan whose boundedness was checked. Callers may inspect source
/// metadata or measure replayable input before binding a sink, then configure output and execute
/// this plan without rebuilding it.
pub struct PreparedPipeline {
    inputs: InputSources,
    output_strategy: Option<OutputStrategy>,
    plan: Arc<dyn ExecutionPlan>,
    session: SessionContext,
    sort_spill_reservation_bytes: Option<usize>,
    spill_path: Option<TempDir>,
}

impl PreparedPipeline {
    /// Returns the sources retained for replayability and cardinality decisions.
    pub fn inputs(&self) -> &InputSources {
        &self.inputs
    }

    /// Returns the session shared by source providers, planning, row measurement, and execution.
    pub fn session(&self) -> &SessionContext {
        &self.session
    }

    /// Overrides the sort spill reservation in the task context used for this plan.
    pub fn with_sort_spill_reservation_bytes(mut self, reservation: Option<usize>) -> Self {
        self.sort_spill_reservation_bytes = reservation;
        self
    }

    /// Configures one output sink for the complete result stream.
    pub fn with_output_strategy_with_single_sink(
        mut self,
        path: String,
        sink_opener: SinkOpenerFn,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::Single {
            path,
            sink_opener,
            exclude_columns,
            create_dirs,
            overwrite,
        });
        self
    }

    /// Configures partitioned output that writes one partition at a time after sorting.
    #[allow(clippy::too_many_arguments)]
    pub fn with_single_writer_partitioned_sink(
        mut self,
        columns: Vec<String>,
        template: PathTemplate,
        sink_opener: SinkOpenerFn,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
        list_outputs: ListOutputsFormat,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::PartitionedSingleWriter {
            columns,
            template: Box::new(template),
            sink_opener,
            exclude_columns,
            create_dirs,
            overwrite,
            list_outputs,
        });
        self
    }

    /// Configures partitioned output that keeps every partition writer open.
    #[allow(clippy::too_many_arguments)]
    pub fn with_multi_writer_partitioned_sink(
        mut self,
        columns: Vec<String>,
        template: PathTemplate,
        sink_opener: SinkOpenerFn,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
        list_outputs: ListOutputsFormat,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::PartitionedMultiWriter {
            columns,
            template: Box::new(template),
            sink_opener,
            exclude_columns,
            create_dirs,
            overwrite,
            list_outputs,
        });
        self
    }

    /// Configures partitioned output that evicts writers above a fixed open-file limit.
    #[allow(clippy::too_many_arguments)]
    pub fn with_evict_writer_partitioned_sink(
        mut self,
        columns: Vec<String>,
        template: PathTemplate,
        sink_opener: SinkOpenerFn,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
        list_outputs: ListOutputsFormat,
        max_open: NonZeroUsize,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::PartitionedEvictWriter {
            columns,
            template: Box::new(template),
            sink_opener,
            exclude_columns,
            create_dirs,
            overwrite,
            list_outputs,
            max_open,
        });
        self
    }

    /// Registers an output handle's object store in the prepared session.
    pub fn with_storage_handle(self, handle: &StorageHandle) -> Self {
        self.session
            .runtime_env()
            .register_object_store(handle.store_url(), handle.object_store());
        self
    }

    /// Executes the retained physical plan and completes its configured outputs.
    pub async fn execute(mut self) -> Result<Vec<OutputFileInfo>> {
        let output_strategy = self
            .output_strategy
            .as_mut()
            .ok_or_else(|| anyhow!("No output strategy provided"))?;
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
        let stream = execute_stream(Arc::clone(&self.plan), task_context)?;
        let files = output_strategy.write_stream(stream).await?;
        drop(self.spill_path.take());
        Ok(files)
    }
}

/// Returns 80% of total system memory as the default memory limit for DataFusion operations.
/// Uses container-aware detection (cgroups) when running in Docker/Kubernetes.
fn default_memory_limit() -> usize {
    total_memory() * 4 / 5
}

/// Parse a human-readable byte size string (e.g., "512MB", "2GB", "1GiB") into bytes.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_byte_size(s: &str) -> Result<usize> {
    s.parse::<ByteSize>()
        .map(|bs| bs.as_u64() as usize)
        .map_err(|_| {
            anyhow!(
                "invalid byte size '{}': expected format like '512MB', '2GB', or '1GiB'",
                s
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_byte_size_decimal_units() {
        // KB = 1000 bytes (decimal)
        assert_eq!(parse_byte_size("1KB").unwrap(), 1000);
        assert_eq!(parse_byte_size("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_byte_size("1GB").unwrap(), 1_000_000_000);
        assert_eq!(parse_byte_size("2GB").unwrap(), 2_000_000_000);
    }

    #[test]
    fn test_parse_byte_size_binary_units() {
        // KiB = 1024 bytes (binary)
        assert_eq!(parse_byte_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_byte_size("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("512MiB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_parse_byte_size_bare_bytes() {
        assert_eq!(parse_byte_size("1024").unwrap(), 1024);
        assert_eq!(parse_byte_size("33554432").unwrap(), 33554432); // 32MB default buffer
    }

    #[test]
    fn test_parse_byte_size_with_spaces() {
        assert_eq!(parse_byte_size("512 MB").unwrap(), 512_000_000);
        assert_eq!(parse_byte_size("1 GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_byte_size_case_insensitive() {
        assert_eq!(parse_byte_size("1mb").unwrap(), 1_000_000);
        assert_eq!(parse_byte_size("1Mb").unwrap(), 1_000_000);
        assert_eq!(parse_byte_size("1mib").unwrap(), 1024 * 1024);
    }

    #[test]
    fn test_parse_byte_size_invalid() {
        assert!(parse_byte_size("invalid").is_err());
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("MB").is_err());
    }
}
