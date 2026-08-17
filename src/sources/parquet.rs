use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    catalog::TableProvider, datasource::file_format::parquet::ParquetFormat,
    prelude::SessionContext,
};
use silk_chiffon_core::InputLeaf;

use crate::sources::file::native_file_provider;

pub(crate) async fn create_provider(
    leaf: &InputLeaf,
    session: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    native_file_provider(leaf, session, Arc::new(ParquetFormat::new())).await
}
