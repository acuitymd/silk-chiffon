use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;

use crate::operations::data_operation::DataOperation;

pub struct IdentityOperation;

impl IdentityOperation {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataOperation for IdentityOperation {
    async fn apply(&self, df: DataFrame) -> Result<DataFrame> {
        Ok(df)
    }
}
