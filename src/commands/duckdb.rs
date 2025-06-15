use crate::{DuckDbArgs, utils::filesystem::ensure_parent_dir_exists};
use anyhow::{Context, Result};
use duckdb::Connection;
use tokio::fs::remove_file;

// use the arrow command to load the arrow file and sort it out

pub async fn run(args: DuckDbArgs) -> Result<()> {
    ensure_parent_dir_exists(args.output.path()).await?;

    let _input_path = args.input.path().to_str().ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to get input path from {}",
            args.input.path().display()
        )
    })?;

    let output_path = args.output.path();

    let _conn = create_duckdb_connection(output_path, args.truncate).await?;

    // convert to parquet first, then load into duckdb

    Ok(())
}

async fn create_duckdb_connection(
    output_path: &std::path::Path,
    truncate: bool,
) -> Result<Connection> {
    if truncate {
        remove_file(output_path).await.with_context(|| {
            format!(
                "Failed to truncate DuckDB database at {}",
                output_path.display()
            )
        })?;
    }

    let conn = Connection::open(output_path).with_context(|| {
        format!(
            "Failed to create DuckDB connection to {}",
            output_path.display()
        )
    })?;

    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clio::{Input, OutputPath};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_run_returns_ok() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.db");

        std::fs::write(&input_path, b"dummy").unwrap();

        let args = DuckDbArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            truncate: false,
        };

        assert!(run(args).await.is_ok());
    }
}
