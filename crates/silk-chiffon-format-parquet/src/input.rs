//! Native DataFusion input-provider creation.
//!
//! Core owns exact-file metadata preparation and structural validation. This
//! module contributes only Parquet's native `FileFormat`, preserving one
//! shared implementation for the same lifecycle across native formats.

use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    catalog::TableProvider, datasource::file_format::parquet::ParquetFormat,
    prelude::SessionContext,
};
use silk_chiffon_core::FileInputGroup;

pub(crate) async fn create_provider(
    group: &FileInputGroup,
    session: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    group
        .create_table_provider(session, Arc::new(ParquetFormat::new()))
        .await
}
