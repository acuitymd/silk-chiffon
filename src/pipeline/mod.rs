use std::path::PathBuf;

use anyhow::Result;
use datafusion::prelude::SessionContext;

use crate::{
    io_strategies::{input_strategy::InputStrategy, output_strategy::OutputStrategy},
    operations::data_operation::DataOperation,
};

pub struct PipelineConfig {
    pub temp_dir: Option<PathBuf>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self { temp_dir: None }
    }
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
}
