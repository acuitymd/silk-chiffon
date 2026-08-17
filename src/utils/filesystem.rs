use anyhow::{Context, Result, anyhow};
use std::path::Path;
use tokio::fs::create_dir_all;

pub async fn ensure_parent_dir_exists(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("No parent directory for path {}", path.display()))?;

    create_dir_all(parent)
        .await
        .with_context(|| format!("Failed to create parent directory for {}", path.display()))?;

    Ok(())
}
