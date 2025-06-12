use crate::DuckDbArgs;
use anyhow::Result;

pub fn run(_args: DuckDbArgs) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clio::{Input, OutputPath};
    use tempfile::TempDir;

    #[test]
    fn test_run_returns_ok() {
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

        assert!(run(args).is_ok());
    }
}
