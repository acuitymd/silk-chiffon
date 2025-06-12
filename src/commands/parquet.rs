use crate::ParquetArgs;
use anyhow::Result;

pub fn run(_args: ParquetArgs) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ParquetCompression, ParquetWriterVersion};
    use clio::{Input, OutputPath};
    use tempfile::TempDir;

    #[test]
    fn test_run_returns_ok() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        std::fs::write(&input_path, b"dummy").unwrap();

        let args = ParquetArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            compression: ParquetCompression::None,
            write_sorted_metadata: false,
            bloom_all: None,
            bloom_column: vec![],
            max_row_group_size: 122_880,
            no_stats: false,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
        };

        assert!(run(args).is_ok());
    }

    #[test]
    fn test_run_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        std::fs::write(&input_path, b"dummy").unwrap();

        let args = ParquetArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            compression: ParquetCompression::Zstd,
            write_sorted_metadata: false,
            bloom_all: None,
            bloom_column: vec![],
            max_row_group_size: 122_880,
            no_stats: false,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
        };

        assert!(run(args).is_ok());
    }
}
