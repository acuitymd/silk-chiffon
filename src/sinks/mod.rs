pub mod arrow;
pub mod data_sink;
pub mod parquet;
pub mod vortex;

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use url::Url;

pub(crate) async fn completed_file_url(path: &Path) -> Result<Url> {
    let absolute_path = tokio::fs::canonicalize(path)
        .await
        .with_context(|| format!("failed to resolve output path: {}", path.display()))?;
    Url::from_file_path(&absolute_path).map_err(|()| {
        anyhow!(
            "failed to convert output path to file URL: {}",
            absolute_path.display()
        )
    })
}
