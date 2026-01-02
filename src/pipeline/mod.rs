use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use datafusion::{
    execution::memory_pool::FairSpillPool,
    prelude::{SessionConfig, SessionContext},
};

use crate::{
    ListOutputsFormat, QueryDialect,
    io_strategies::{
        OutputFileInfo,
        input_strategy::InputStrategy,
        output_strategy::{OutputStrategy, SinkFactory},
        path_template::PathTemplate,
    },
    operations::data_operation::DataOperation,
    sources::data_source::DataSource,
};

#[derive(Default)]
pub struct PipelineConfig {
    pub working_directory: Option<String>,
    pub query_dialect: QueryDialect,
    pub memory_limit: Option<usize>,
    pub target_partitions: Option<usize>,
}

#[derive(Default)]
pub struct Pipeline {
    input_strategy: Option<InputStrategy>,
    operations: Vec<Box<dyn DataOperation>>,
    output_strategy: Option<OutputStrategy>,
    config: PipelineConfig,
}

impl Pipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_input_strategy_with_single_source(mut self, source: Box<dyn DataSource>) -> Self {
        self.input_strategy = Some(InputStrategy::Single(source));

        self
    }

    pub fn with_input_strategy_with_multiple_sources(
        mut self,
        sources: Vec<Box<dyn DataSource>>,
    ) -> Self {
        self.input_strategy = Some(InputStrategy::Multiple(sources));

        self
    }

    pub fn with_operation(mut self, operation: Box<dyn DataOperation>) -> Self {
        self.operations.push(operation);

        self
    }

    pub fn with_output_strategy_with_single_sink(
        mut self,
        path: String,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::Single {
            path,
            sink_factory,
            exclude_columns,
            create_dirs,
            overwrite,
        });

        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_output_strategy_with_high_cardinality_partitioned_sink(
        mut self,
        columns: Vec<String>,
        template: PathTemplate,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
        list_outputs: ListOutputsFormat,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::PartitionedHighCardinality {
            columns,
            template: Box::new(template),
            sink_factory,
            exclude_columns,
            create_dirs,
            overwrite,
            list_outputs,
        });

        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_output_strategy_with_low_cardinality_partitioned_sink(
        mut self,
        columns: Vec<String>,
        template: PathTemplate,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
        list_outputs: ListOutputsFormat,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::PartitionedLowCardinality {
            columns,
            template: Box::new(template),
            sink_factory,
            exclude_columns,
            create_dirs,
            overwrite,
            list_outputs,
        });

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

    pub async fn execute(&mut self) -> Result<Vec<OutputFileInfo>> {
        let mut ctx = self.build_session_context()?;
        self.execute_with_session_context(&mut ctx).await
    }

    pub async fn execute_with_session_context(
        &mut self,
        ctx: &mut SessionContext,
    ) -> Result<Vec<OutputFileInfo>> {
        let input_strategy = self
            .input_strategy
            .as_ref()
            .ok_or_else(|| anyhow!("No input strategy provided"))?;

        let output_strategy = self
            .output_strategy
            .as_mut()
            .ok_or_else(|| anyhow!("No output strategy provided"))?;

        if self.operations.is_empty() {
            let stream = input_strategy.as_stream(ctx).await?;
            let files = output_strategy.write_stream(stream).await?;
            return Ok(files);
        }

        let table_provider = input_strategy
            .as_table_provider(ctx, self.config.working_directory.clone())
            .await?;

        let mut df = ctx.read_table(table_provider)?;

        for operation in &self.operations {
            df = operation.apply(ctx, df).await?;
        }

        let files = output_strategy.write(df).await?;

        Ok(files)
    }

    pub fn build_session_context(&self) -> Result<SessionContext> {
        let mut cfg = SessionConfig::new();

        // DuckDB doesn't like joining Datatype::Utf8View to Datatype::Utf8, so we disable
        // the automatic mapping of all string types to Datatype::Utf8View.
        // https://datafusion.apache.org/library-user-guide/upgrading.html#new-map-string-types-to-utf8view-configuration-option
        cfg.options_mut().sql_parser.map_string_types_to_utf8view = false;

        cfg.options_mut().sql_parser.dialect = self.config.query_dialect.into();

        if let Some(target_partitions) = self.config.target_partitions {
            cfg = cfg.with_target_partitions(target_partitions);
        }

        if let Some(bytes) = self.config.memory_limit {
            // use FairSpillPool which allows spilling to disk when memory is exceeded
            let pool = FairSpillPool::new(bytes);
            let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::default()
                .with_memory_pool(std::sync::Arc::new(pool))
                .build()?;
            return Ok(SessionContext::new_with_config_rt(
                cfg,
                std::sync::Arc::new(runtime),
            ));
        }

        Ok(SessionContext::new_with_config(cfg))
    }
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
