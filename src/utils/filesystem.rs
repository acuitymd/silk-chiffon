use anyhow::{Result, anyhow};
use std::path::Path;
use tokio::fs::create_dir_all;

pub async fn ensure_parent_dir_exists(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Output directory not found"))?;
    create_dir_all(parent).await?;
    Ok(())
}
