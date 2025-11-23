use anyhow::{Result, anyhow};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::{
    QueryDialect,
    io_strategies::{
        input_strategy::InputStrategy,
        output_strategy::{OutputStrategy, SinkFactory},
        path_template::PathTemplate,
    },
    operations::data_operation::DataOperation,
    sinks::data_sink::DataSink,
    sources::data_source::DataSource,
};

#[derive(Default)]
pub struct PipelineConfig {
    pub working_directory: Option<String>,
    pub query_dialect: QueryDialect,
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

    pub fn with_output_strategy_with_single_sink(mut self, sink: Box<dyn DataSink>) -> Self {
        self.output_strategy = Some(OutputStrategy::Single(sink));

        self
    }

    pub fn with_output_strategy_with_partitioned_sink(
        mut self,
        columns: Vec<String>,
        template: PathTemplate,
        sink_factory: SinkFactory,
        exclude_partition_columns: bool,
    ) -> Self {
        self.output_strategy = Some(OutputStrategy::Partitioned {
            columns,
            template: Box::new(template),
            sink_factory,
            exclude_partition_columns,
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

    pub async fn execute(&mut self) -> Result<()> {
        let mut ctx = self.build_session_context();
        self.execute_with_session_context(&mut ctx).await
    }

    pub async fn execute_with_session_context(&mut self, ctx: &mut SessionContext) -> Result<()> {
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
            output_strategy.write_stream(stream).await?;
            return Ok(());
        }

        let table_provider = input_strategy
            .as_table_provider(ctx, self.config.working_directory.clone())
            .await?;

        let mut df = ctx.read_table(table_provider)?;

        for operation in &self.operations {
            df = operation.apply(df).await?;
        }

        output_strategy.write(df).await?;

        Ok(())
    }

    pub fn build_session_context(&self) -> SessionContext {
        let mut cfg = SessionConfig::new();

        // DuckDB doesn't like joining Datatype::Utf8View to Datatype::Utf8, so we disable
        // the automatic mapping of all string types to Datatype::Utf8View.
        // https://datafusion.apache.org/library-user-guide/upgrading.html#new-map-string-types-to-utf8view-configuration-option
        cfg.options_mut().sql_parser.map_string_types_to_utf8view = false;

        cfg.options_mut().sql_parser.dialect = self.config.query_dialect.into();

        SessionContext::new_with_config(cfg)
    }
}
