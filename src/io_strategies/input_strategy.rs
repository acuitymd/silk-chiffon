use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::{
    catalog::TableProvider,
    prelude::{DataFrame, SessionContext},
};

use crate::sources::data_source::DataSource;

pub enum InputStrategy {
    Single(Box<dyn DataSource>),
    Multiple(Vec<Box<dyn DataSource>>),
}

impl InputStrategy {
    pub async fn as_table_provider(
        &self,
        ctx: &mut SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
        match self {
            InputStrategy::Single(source) => source.as_table_provider(ctx).await,
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    return Err(anyhow!("No sources provided"));
                }

                let mut providers: Vec<Arc<dyn TableProvider>> = Vec::new();

                for source in sources {
                    providers.push(source.as_table_provider(ctx).await?);
                }

                let mut df: DataFrame = ctx.read_empty()?;

                for provider in providers {
                    df = df.union(ctx.read_table(provider.clone())?)?;
                }

                Ok(df.into_view())
            }
        }
    }
}
