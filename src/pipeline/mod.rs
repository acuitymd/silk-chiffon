use std::path::PathBuf;

use anyhow::Result;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::{
    QueryDialect,
    io_strategies::{input_strategy::InputStrategy, output_strategy::OutputStrategy},
    operations::data_operation::DataOperation,
};

#[derive(Default)]
pub struct PipelineConfig {
    pub temp_dir: Option<PathBuf>,
}

pub struct Pipeline {
    input_strategy: InputStrategy,
    operations: Vec<Box<dyn DataOperation>>,
    output_strategy: OutputStrategy,
    config: PipelineConfig,
}

impl Pipeline {
    pub fn new(
        input_strategy: InputStrategy,
        operations: Vec<Box<dyn DataOperation>>,
        output_strategy: OutputStrategy,
    ) -> Self {
        Self {
            input_strategy,
            operations,
            output_strategy,
            config: PipelineConfig::default(),
        }
    }

    pub fn with_temp_dir(mut self, temp_dir: PathBuf) -> Self {
        self.config.temp_dir = Some(temp_dir);
        self
    }

    pub async fn execute(&mut self) -> Result<()> {
        let mut ctx = SessionContext::new();
        self.execute_with_session_context(&mut ctx).await
    }

    pub async fn execute_with_session_context(&mut self, ctx: &mut SessionContext) -> Result<()> {
        let table_provider = self.input_strategy.as_table_provider(ctx).await?;

        let mut df = ctx.read_table(table_provider)?;

        for operation in &self.operations {
            df = operation.apply(df).await?;
        }

        self.output_strategy.write(df).await?;

        Ok(())
    }

    pub fn build_session_context(&self, dialect: QueryDialect) -> SessionContext {
        let mut cfg = SessionConfig::new();

        // DuckDB doesn't like joining Datatype::Utf8View to Datatype::Utf8, so we disable
        // the automatic mapping of all string types to Datatype::Utf8View.
        // https://datafusion.apache.org/library-user-guide/upgrading.html#new-map-string-types-to-utf8view-configuration-option
        cfg.options_mut().sql_parser.map_string_types_to_utf8view = false;

        cfg.options_mut().sql_parser.dialect = dialect.to_string();

        SessionContext::new_with_config(cfg)
    }
}
