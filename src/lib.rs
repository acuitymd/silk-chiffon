pub mod commands;
pub mod converters;
pub mod utils;

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Convert Arrow format to Parquet format.
    ///
    /// Input formats:
    ///   - Arrow IPC stream format
    ///   - Arrow IPC file format
    ///
    /// Output formats:
    ///   - Parquet file format
    #[command(verbatim_doc_comment)]
    Parquet(ParquetArgs),
    /// Convert Arrow format to DuckDB format.
    ///
    /// Input formats:
    ///   - Arrow IPC stream format
    ///   - Arrow IPC file format
    ///
    /// Output formats:
    ///   - DuckDB database file format
    #[command(verbatim_doc_comment)]
    Duckdb(DuckDbArgs),
    /// Convert Arrow format to Arrow format.
    ///
    /// Input formats:
    ///   - Arrow IPC stream format
    ///   - Arrow IPC file format
    ///
    /// Output formats:
    ///   - Arrow IPC file format
    #[command(verbatim_doc_comment)]
    Arrow(ArrowArgs),
}

#[derive(Args, Debug)]
pub struct ParquetArgs {
    /// Input Arrow IPC file.
    #[arg(value_parser = clap::value_parser!(clio::Input).exists().is_file())]
    pub input: clio::Input,

    /// Output Parquet file path.
    #[arg(value_parser)]
    pub output: clio::OutputPath,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long, default_value_t = SortSpec::default())]
    pub sort_by: SortSpec,

    /// The compression algorithm to use.
    #[arg(short, long, default_value_t = ParquetCompression::None)]
    pub compression: ParquetCompression,

    /// Embed metadata indicating that the file's data is sorted.
    ///
    /// Requires --sort-by to be set.
    #[arg(long, default_value_t = false, requires = "sort_by")]
    pub write_sorted_metadata: bool,

    /// Enable bloom filters for all columns with optional custom settings.
    /// Mutually exclusive with --bloom-column.
    ///
    /// Formats:
    ///   --bloom-all             # Use default FPP (0.01)
    ///   --bloom-all "fpp=VALUE" # Custom FPP
    ///
    /// Examples:
    ///   --bloom-all             # Use default FPP (0.01)
    ///   --bloom-all "fpp=0.001" # Custom FPP (0.001)
    #[arg(
        long,
        value_name = "[fpp=VALUE]",
        conflicts_with = "bloom_column",
        num_args = 0..=1,
        default_missing_value = "",
        verbatim_doc_comment
    )]
    pub bloom_all: Option<AllColumnsBloomFilterConfig>,

    /// Enable bloom filter for specific columns with optional custom settings.
    /// Can be specified multiple times. Mutually exclusive with --bloom-all.
    ///
    /// Formats:
    ///   COLUMN           # Use default FPP (0.01)
    ///   COLUMN:fpp=VALUE # Custom FPP
    ///
    /// Examples:
    ///   --bloom-column "user_id"            # Use default FPP (0.01)
    ///   --bloom-column "user_id:fpp=0.001"  # Custom FPP (0.001)
    ///   --bloom-column "user_id" --bloom-column "name:fpp=0.05" # Multiple columns
    #[arg(
        long,
        value_name = "COLUMN[:fpp=VALUE]",
        conflicts_with = "bloom_all",
        verbatim_doc_comment
    )]
    pub bloom_column: Vec<ColumnSpecificBloomFilterConfig>,

    /// The maximum number of rows per Parquet row group.
    #[arg(long, default_value_t = 1_048_576)]
    pub max_row_group_size: usize,

    /// Control column statistics level.
    #[arg(long, default_value_t = ParquetStatistics::Page)]
    pub statistics: ParquetStatistics,

    /// Size of Arrow record batches written to the output file.
    #[arg(long, default_value_t = 122_880)]
    pub record_batch_size: usize,

    /// Disable dictionary encoding for columns.
    #[arg(long, default_value_t = false)]
    pub no_dictionary: bool,

    /// Set writer version.
    #[arg(long, default_value_t = ParquetWriterVersion::V2)]
    pub writer_version: ParquetWriterVersion,
}

#[derive(Args, Debug)]
pub struct DuckDbArgs {
    /// Input Arrow IPC file.
    #[arg(value_parser = clap::value_parser!(clio::Input).exists().is_file())]
    pub input: clio::Input,

    /// Output DuckDB database file path.
    #[arg(value_parser)]
    pub output: clio::OutputPath,

    /// Name of the table to create.
    #[arg(short, long)]
    pub table_name: String,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long, default_value_t = SortSpec::default())]
    pub sort_by: SortSpec,

    /// Truncate the database file before writing (removes entire file).
    #[arg(long, default_value_t = false)]
    pub truncate: bool,

    /// Drop the table if it already exists before creating.
    #[arg(long, default_value_t = false, conflicts_with = "truncate")]
    pub drop_table: bool,
}

#[derive(Args, Debug)]
pub struct ArrowArgs {
    /// Input Arrow IPC file.
    #[arg(value_parser = clap::value_parser!(clio::Input).exists().is_file())]
    pub input: clio::Input,

    /// Output Arrow IPC file path.
    #[arg(value_parser)]
    pub output: clio::OutputPath,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long)]
    pub sort_by: Option<SortSpec>,

    /// The IPC compression to use for the output.
    #[arg(long, default_value_t = ArrowCompression::None)]
    pub compression: ArrowCompression,

    /// Size of Arrow record batches written to the output file.
    #[arg(long, default_value_t = 122_880)]
    pub record_batch_size: usize,
}

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
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

impl Display for ParquetCompression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
pub enum ParquetStatistics {
    #[value(name = "none")]
    None,
    #[value(name = "chunk")]
    Chunk,
    #[default]
    #[value(name = "page")]
    Page,
}

impl Display for ParquetStatistics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => "none",
            Self::Chunk => "chunk",
            Self::Page => "page",
        };
        write!(f, "{}", s)
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
pub enum ParquetWriterVersion {
    #[value(name = "v1")]
    V1,
    #[default]
    #[value(name = "v2")]
    V2,
}

impl Display for ParquetWriterVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl Display for ArrowCompression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

#[derive(Debug, Clone, Default)]
pub struct SortSpec {
    pub columns: Vec<SortColumn>,
}

impl SortSpec {
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn is_configured(&self) -> bool {
        !self.is_empty()
    }
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

pub const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.01;

#[derive(Debug, Clone)]
pub struct AllColumnsBloomFilterConfig {
    pub fpp: f64,
}

impl FromStr for AllColumnsBloomFilterConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Ok(AllColumnsBloomFilterConfig {
                fpp: DEFAULT_BLOOM_FILTER_FPP,
            });
        }

        let mut fpp = None;

        let parts = s
            .split(':')
            .map(|p| p.trim())
            .filter(|p| !p.is_empty())
            .collect::<Vec<&str>>();

        if parts.len() > 1 {
            return Err(anyhow!("Invalid bloom filter specification: {}", s));
        }

        for part in parts {
            if part.is_empty() {
                return Err(anyhow!("Invalid bloom filter specification: {}", s));
            }

            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim();
                let value = value.trim();

                match key {
                    "fpp" => {
                        if fpp.is_some() {
                            return Err(anyhow!(
                                "Invalid bloom filter specification, fpp is set twice: {}",
                                s
                            ));
                        }

                        fpp = Some(value.parse::<f64>().map_err(|e| {
                            anyhow::anyhow!("Invalid fpp value '{}': {}", value, e)
                        })?);
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unknown parameter '{}'. Valid parameter is 'fpp'",
                            key
                        ));
                    }
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Invalid parameter format '{}'. Expected 'key=value'",
                    part
                ));
            }
        }

        Ok(AllColumnsBloomFilterConfig {
            fpp: fpp.unwrap_or(DEFAULT_BLOOM_FILTER_FPP),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnBloomFilterConfig {
    pub fpp: f64,
}

impl FromStr for ColumnBloomFilterConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut fpp = None;

        let parts = s
            .split(':')
            .map(|p| p.trim())
            .filter(|p| !p.is_empty())
            .collect::<Vec<&str>>();

        if parts.len() > 1 {
            return Err(anyhow!("Invalid bloom filter specification: {}", s));
        }

        for part in parts {
            if part.is_empty() {
                return Err(anyhow!("Invalid bloom filter specification: {}", s));
            }

            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim();
                let value = value.trim();

                match key {
                    "fpp" => {
                        if fpp.is_some() {
                            return Err(anyhow!(
                                "Invalid bloom filter specification, fpp is set twice: {}",
                                s
                            ));
                        }

                        fpp = Some(value.parse::<f64>().map_err(|e| {
                            anyhow::anyhow!("Invalid fpp value '{}': {}", value, e)
                        })?);
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unknown parameter '{}'. Valid parameter is 'fpp'",
                            key
                        ));
                    }
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Invalid parameter format '{}'. Expected 'key=value'",
                    part
                ));
            }
        }

        Ok(ColumnBloomFilterConfig {
            fpp: fpp.unwrap_or(DEFAULT_BLOOM_FILTER_FPP),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSpecificBloomFilterConfig {
    pub name: String,
    pub config: ColumnBloomFilterConfig,
}

impl FromStr for ColumnSpecificBloomFilterConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((column_name, rest)) = s.split_once(':') {
            let column_name = column_name.trim();

            if column_name.is_empty() {
                return Err(anyhow!(
                    "Invalid bloom filter specification, column name is empty: {}",
                    s
                ));
            }

            Ok(ColumnSpecificBloomFilterConfig {
                name: column_name.to_string(),
                config: ColumnBloomFilterConfig::from_str(rest)?,
            })
        } else {
            let column_name = s.trim();
            if column_name.is_empty() {
                return Err(anyhow!(
                    "Invalid bloom filter specification: empty column name"
                ));
            }
            Ok(ColumnSpecificBloomFilterConfig {
                name: column_name.to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: DEFAULT_BLOOM_FILTER_FPP,
                },
            })
        }
    }
}

#[derive(Debug, Clone, Default)]
pub enum BloomFilterConfig {
    #[default]
    None,
    All(AllColumnsBloomFilterConfig),
    Columns(Vec<ColumnSpecificBloomFilterConfig>),
}

impl BloomFilterConfig {
    pub fn is_configured(&self) -> bool {
        !matches!(self, BloomFilterConfig::None)
    }
}

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
fn _silk_chiffon(m: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register_module(m)?;
    Ok(())
}
