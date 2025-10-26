use anyhow::Result;
use datafusion::prelude::SessionContext;

use crate::{
    io_strategies::{input_strategy::InputStrategy, output_strategy::OutputStrategy},
    operations::data_operation::DataOperation,
};

pub struct Pipeline {
    input_strategy: InputStrategy,
    operations: Vec<Box<dyn DataOperation>>,
    output_strategy: OutputStrategy,
}

impl Pipeline {
    fn new(
        input_strategy: InputStrategy,
        operations: Vec<Box<dyn DataOperation>>,
        output_strategy: OutputStrategy,
    ) -> Self {
        Self {
            input_strategy,
            operations,
            output_strategy,
        }
    }

    async fn execute(&mut self) -> Result<()> {
        let ctx = SessionContext::new();
        let table_provider = self.input_strategy.as_table_provider().await?;

        let mut df = ctx.read_table(table_provider)?;

        for operation in &self.operations {
            df = operation.apply(df).await?;
        }

        self.output_strategy.write(df).await?;

        Ok(())
    }
}
