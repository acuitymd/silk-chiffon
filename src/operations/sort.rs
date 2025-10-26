use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, col};

use crate::operations::data_operation::DataOperation;

pub struct SortOperation {
    columns: Vec<String>,
}

impl SortOperation {
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }
}

#[async_trait]
impl DataOperation for SortOperation {
    async fn apply(&self, df: DataFrame) -> Result<DataFrame> {
        Ok(df.sort(
            self.columns
                .iter()
                .map(|c| col(c).sort(true, false))
                .collect(),
        )?)
    }
}
