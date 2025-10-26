use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::{
    catalog::TableProvider,
    prelude::{DataFrame, SessionContext},
};
use futures::future::join_all;

use crate::sources::data_source::DataSource;

pub enum InputStrategy {
    Single(Box<dyn DataSource>),
    Multiple(Vec<Box<dyn DataSource>>),
}

impl InputStrategy {
    pub async fn as_table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        match self {
            InputStrategy::Single(source) => source.as_table_provider().await,
            InputStrategy::Multiple(sources) => {
                if sources.len() == 0 {
                    return Err(anyhow!("No sources provided"));
                }

                let futures = sources
                    .iter()
                    .map(async |source| source.as_table_provider().await)
                    .collect::<Vec<_>>();

                let providers = join_all(futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<Arc<dyn TableProvider>>>>()?;

                let ctx = SessionContext::new();

                let mut df: DataFrame = ctx.read_empty()?;

                for provider in providers {
                    df = df.union(ctx.read_table(provider.clone())?)?;
                }

                Ok(df.into_view())
            }
        }
    }
}
