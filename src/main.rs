use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::{fmt::Display, str::FromStr};

pub mod commands;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Convert Arrow data to Parquet format.
    Parquet(ParquetArgs),
    /// Convert Arrow data to DuckDB format.
    Duckdb(DuckDbArgs),
    /// Convert any Arrow IPC file to an Arrow IPC file in the file (not stream) format.
    Arrow(ArrowArgs),
}

#[derive(Args, Debug)]
pub struct ParquetArgs {
    /// Input Arrow IPC file.
    #[arg(value_parser = clap::value_parser!(clio::Input).exists().is_file())]
    input: clio::Input,

    /// Output Parquet file path.
    #[arg(value_parser)]
    output: clio::OutputPath,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long)]
    sort_by: Option<SortSpec>,

    /// The compression algorithm to use.
    #[arg(short, long, default_value_t = ParquetCompression::None)]
    compression: ParquetCompression,

    /// Embed metadata indicating that the file's data is sorted.
    ///
    /// Requires --sort-by to be set.
    #[arg(long, default_value_t = false, requires = "sort_by")]
    write_sorted_metadata: bool,

    /// Enable bloom filters for all columns with optional custom settings.
    /// Mutually exclusive with --bloom-column.
    ///
    /// Formats:
    ///   --bloom-all                 - Use defaults for both FPP and NDV
    ///   --bloom-all "fpp=VALUE"     - Custom FPP, auto-estimate NDV
    ///   --bloom-all "ndv=VALUE"     - Default FPP, custom NDV  
    ///   --bloom-all "fpp=VALUE:ndv=VALUE" - Both custom (order doesn't matter)
    ///
    /// Examples:
    ///   --bloom-all                      # All defaults
    ///   --bloom-all "fpp=0.001"          # Custom FPP only
    ///   --bloom-all "ndv=1000000"        # Custom NDV only
    ///   --bloom-all "fpp=0.01:ndv=1000000" # Both custom
    ///   --bloom-all "ndv=1000000:fpp=0.01" # Same, different order
    #[arg(
        long,
        value_name = "[fpp=VALUE][:ndv=VALUE]",
        conflicts_with = "bloom_column",
        verbatim_doc_comment
    )]
    bloom_all: Option<String>,

    /// Enable bloom filter for specific columns with optional custom settings.
    /// Can be specified multiple times. Mutually exclusive with --bloom-all.
    ///
    /// Formats:
    ///   COLUMN                     - Use defaults for both FPP and NDV
    ///   COLUMN:fpp=VALUE           - Custom FPP, auto-estimate NDV
    ///   COLUMN:ndv=VALUE           - Default FPP, custom NDV  
    ///   COLUMN:fpp=VALUE:ndv=VALUE - Both custom (order doesn't matter)
    ///
    /// Examples:
    ///   --bloom-column "user_id"                    # All defaults
    ///   --bloom-column "user_id:fpp=0.001"          # Custom FPP only
    ///   --bloom-column "user_id:ndv=1000000"        # Custom NDV only
    ///   --bloom-column "user_id:fpp=0.01:ndv=1000000" # Both custom
    ///   --bloom-column "user_id:ndv=1000000:fpp=0.01" # Same, different order
    #[arg(
        long,
        value_name = "COLUMN[:fpp=VALUE][:ndv=VALUE]",
        conflicts_with = "bloom_all",
        verbatim_doc_comment
    )]
    bloom_column: Vec<String>,

    /// The maximum number of rows per Parquet row group.
    #[arg(long, default_value_t = 122_880)]
    max_row_group_size: usize,

    /// Disable writing statistics (min/max/null count) for columns.
    #[arg(long, default_value_t = false)]
    no_stats: bool,

    /// Disable dictionary encoding for columns.
    #[arg(long, default_value_t = false)]
    no_dictionary: bool,

    /// Set writer version.
    #[arg(long, default_value_t = ParquetWriterVersion::V2)]
    writer_version: ParquetWriterVersion,
}

#[derive(Args, Debug)]
pub struct DuckDbArgs {
    /// Input Arrow IPC file.
    #[arg(value_parser = clap::value_parser!(clio::Input).exists().is_file())]
    input: clio::Input,

    /// Output DuckDB database file path.
    #[arg(value_parser)]
    output: clio::OutputPath,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long)]
    sort_by: Option<SortSpec>,

    /// Truncate the database before writing.
    ///
    /// By default, the database is appended to.
    #[arg(long, default_value_t = false)]
    truncate: bool,
}

#[derive(Args, Debug)]
pub struct ArrowArgs {
    /// Input Arrow IPC file.
    #[arg(value_parser = clap::value_parser!(clio::Input).exists().is_file())]
    input: clio::Input,

    /// Output Arrow IPC file path.
    #[arg(value_parser)]
    output: clio::OutputPath,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long)]
    sort_by: Option<SortSpec>,

    /// The IPC compression to use for the output.
    #[arg(long, default_value_t = ArrowCompression::None)]
    compression: ArrowCompression,
}

#[derive(ValueEnum, Clone, Debug, Default)]
pub enum ParquetCompression {
    #[value(name = "zstd")]
    Zstd,
    #[value(name = "snappy")]
    Snappy,
    #[value(name = "gzip")]
    Gzip,
    #[value(name = "lz4")]
    Lz4,
    #[default]
    #[value(name = "none")]
    None,
}

impl std::fmt::Display for ParquetCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Zstd => "zstd",
            Self::Snappy => "snappy",
            Self::Gzip => "gzip",
            Self::Lz4 => "lz4",
            Self::None => "none",
        };
        write!(f, "{}", s)
    }
}

#[derive(ValueEnum, Clone, Debug, Default)]
pub enum ParquetWriterVersion {
    #[value(name = "v1")]
    V1,
    #[default]
    #[value(name = "v2")]
    V2,
}

impl std::fmt::Display for ParquetWriterVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        };
        write!(f, "{}", s)
    }
}

#[derive(ValueEnum, Clone, Debug, Default)]
pub enum ArrowCompression {
    #[value(name = "zstd")]
    Zstd,
    #[value(name = "lz4")]
    Lz4,
    #[default]
    #[value(name = "none")]
    None,
}

impl std::fmt::Display for ArrowCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
            Self::None => "none",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone)]
pub struct SortColumn {
    pub name: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub columns: Vec<SortColumn>,
}

impl FromStr for SortSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut columns = Vec::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let (name, descending) = if let Some((col, direction)) = part.split_once(':') {
                let direction = direction.trim().to_lowercase();
                match direction.as_str() {
                    "desc" | "descending" => (col.trim(), true),
                    "asc" | "ascending" => (col.trim(), false),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Invalid sort direction '{}'. Use 'asc' or 'desc'",
                            direction
                        ));
                    }
                }
            } else {
                (part, false) // default to ascending
            };

            columns.push(SortColumn {
                name: name.to_string(),
                direction: if descending {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                },
            });
        }

        Ok(SortSpec { columns })
    }
}

impl Display for SortSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self
            .columns
            .iter()
            .map(|col| match col.direction {
                SortDirection::Descending => format!("{}:desc", col.name),
                SortDirection::Ascending => col.name.clone(),
            })
            .collect();
        write!(f, "{}", parts.join(","))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Parquet(args) => commands::parquet::run(args)?,
        Commands::Duckdb(args) => commands::duckdb::run(args)?,
        Commands::Arrow(args) => commands::arrow::run(args).await?,
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_spec_parsing() {
        // single column with default sort
        let spec = SortSpec::from_str("col1").unwrap();
        assert_eq!(spec.columns.len(), 1);
        assert_eq!(spec.columns[0].name, "col1");
        assert_eq!(spec.columns[0].direction, SortDirection::Ascending);

        // single column with explicit ascending
        let spec = SortSpec::from_str("col1:asc").unwrap();
        assert_eq!(spec.columns.len(), 1);
        assert_eq!(spec.columns[0].name, "col1");
        assert_eq!(spec.columns[0].direction, SortDirection::Ascending);

        // single column with explicit descending
        let spec = SortSpec::from_str("col1:desc").unwrap();
        assert_eq!(spec.columns.len(), 1);
        assert_eq!(spec.columns[0].name, "col1");
        assert_eq!(spec.columns[0].direction, SortDirection::Descending);

        // multiple columns
        let spec = SortSpec::from_str("col1,col2:desc,col3:asc").unwrap();
        assert_eq!(spec.columns.len(), 3);
        assert_eq!(spec.columns[0].name, "col1");
        assert_eq!(spec.columns[0].direction, SortDirection::Ascending);
        assert_eq!(spec.columns[1].name, "col2");
        assert_eq!(spec.columns[1].direction, SortDirection::Descending);
        assert_eq!(spec.columns[2].name, "col3");
        assert_eq!(spec.columns[2].direction, SortDirection::Ascending);

        // make sure it's tolerant of extra whitespace
        let spec = SortSpec::from_str("col1 , col2:desc , col3").unwrap();
        assert_eq!(spec.columns.len(), 3);
        assert_eq!(spec.columns[0].name, "col1");
        assert_eq!(spec.columns[1].name, "col2");
        assert_eq!(spec.columns[2].name, "col3");

        // make sure it's tolerant of empty parts
        let spec = SortSpec::from_str("col1,,col2").unwrap();
        assert_eq!(spec.columns.len(), 2);
        assert_eq!(spec.columns[0].name, "col1");
        assert_eq!(spec.columns[1].name, "col2");
    }

    #[test]
    fn test_sort_spec_invalid_direction() {
        let result = SortSpec::from_str("col1:invalid");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid sort direction")
        );
    }

    #[test]
    fn test_sort_spec_display() {
        let spec = SortSpec {
            columns: vec![
                SortColumn {
                    name: "col1".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "col2".to_string(),
                    direction: SortDirection::Descending,
                },
                SortColumn {
                    name: "col3".to_string(),
                    direction: SortDirection::Ascending,
                },
            ],
        };
        assert_eq!(spec.to_string(), "col1,col2:desc,col3");
    }

    #[test]
    fn test_parquet_compression_display() {
        assert_eq!(ParquetCompression::Zstd.to_string(), "zstd");
        assert_eq!(ParquetCompression::Snappy.to_string(), "snappy");
        assert_eq!(ParquetCompression::Gzip.to_string(), "gzip");
        assert_eq!(ParquetCompression::Lz4.to_string(), "lz4");
        assert_eq!(ParquetCompression::None.to_string(), "none");
    }

    #[test]
    fn test_parquet_writer_version_display() {
        assert_eq!(ParquetWriterVersion::V1.to_string(), "v1");
        assert_eq!(ParquetWriterVersion::V2.to_string(), "v2");
    }

    #[test]
    fn test_arrow_compression_display() {
        assert_eq!(ArrowCompression::Zstd.to_string(), "zstd");
        assert_eq!(ArrowCompression::Lz4.to_string(), "lz4");
        assert_eq!(ArrowCompression::None.to_string(), "none");
    }
}
