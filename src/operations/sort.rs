use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext, col};

use crate::{SortColumn, SortDirection, operations::data_operation::DataOperation};

pub struct SortOperation {
    columns: Vec<SortColumn>,
}

impl SortOperation {
    pub fn new(columns: Vec<SortColumn>) -> Self {
        Self { columns }
    }
}

#[async_trait]
impl DataOperation for SortOperation {
    async fn apply(&self, _ctx: &mut SessionContext, df: DataFrame) -> Result<DataFrame> {
        Ok(df.sort(
            self.columns
                .iter()
                .map(|c| col(&c.name).sort(c.direction == SortDirection::Ascending, false))
                .collect(),
        )?)
    }
}
