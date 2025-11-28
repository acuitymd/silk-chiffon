pub mod commands;
pub mod io_strategies;
pub mod operations;
pub mod pipeline;
pub mod sinks;
pub mod sources;
pub mod utils;

use crate::utils::collections::{uniq, uniq_by};
use anyhow::{Result, anyhow};
use arrow::ipc::CompressionType;
use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::config::Dialect;
use parquet::{
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::{EnabledStatistics, WriterVersion},
};
use std::{
    fmt::{self, Formatter},
    str::FromStr,
};
use strum_macros::Display;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Transform data between formats with optional filtering, sorting, merging, and partitioning.
    ///
    /// Examples:
    ///   # Simple conversion
    ///   silk-chiffon transform --from input.arrow --to output.parquet
    ///
    ///   # Merge multiple files
    ///   silk-chiffon transform --from-many file1.arrow --from-many file2.arrow --to merged.parquet
    ///
    ///   # Partition into multiple files
    ///   silk-chiffon transform --from input.arrow --to-many "{{region}}.parquet" --by region
    ///
    ///   # Merge and partition with glob
    ///   silk-chiffon transform --from-many '*.arrow' --to-many "{{year}}/{{month}}.parquet" --by year,month
    #[command(verbatim_doc_comment)]
    Transform(TransformCommand),
}

#[derive(ValueEnum, Clone, Copy, Debug, Default, Display)]
#[value(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ListOutputsFormat {
    #[default]
    None,
    Text,
    Json,
}

#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Display)]
#[value(rename_all = "lowercase")]
pub enum QueryDialect {
    #[default]
    DuckDb,
    Generic,
    MySQL,
    PostgreSQL,
    Hive,
    SQLite,
    Snowflake,
    Redshift,
    MsSQL,
    ClickHouse,
    BigQuery,
    ANSI,
    Databricks,
}

impl From<QueryDialect> for Dialect {
    fn from(dialect: QueryDialect) -> Self {
        match dialect {
            QueryDialect::DuckDb => Dialect::DuckDB,
            QueryDialect::Generic => Dialect::Generic,
            QueryDialect::MySQL => Dialect::MySQL,
            QueryDialect::PostgreSQL => Dialect::PostgreSQL,
            QueryDialect::Hive => Dialect::Hive,
            QueryDialect::SQLite => Dialect::SQLite,
            QueryDialect::Snowflake => Dialect::Snowflake,
            QueryDialect::Redshift => Dialect::Redshift,
            QueryDialect::MsSQL => Dialect::MsSQL,
            QueryDialect::ClickHouse => Dialect::ClickHouse,
            QueryDialect::BigQuery => Dialect::BigQuery,
            QueryDialect::Databricks => Dialect::Databricks,
            QueryDialect::ANSI => Dialect::Ansi,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
#[value(rename_all = "lowercase")]
pub enum ParquetCompression {
    Zstd,
    Snappy,
    Gzip,
    Lz4,
    #[default]
    None,
}

impl From<ParquetCompression> for Compression {
    fn from(compression: ParquetCompression) -> Self {
        match compression {
            ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP(GzipLevel::default()),
            ParquetCompression::Lz4 => Compression::LZ4_RAW,
            ParquetCompression::None => Compression::UNCOMPRESSED,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
#[value(rename_all = "lowercase")]
pub enum ParquetStatistics {
    None,
    Chunk,
    #[default]
    Page,
}

impl From<ParquetStatistics> for EnabledStatistics {
    fn from(statistics: ParquetStatistics) -> Self {
        match statistics {
            ParquetStatistics::None => EnabledStatistics::None,
            ParquetStatistics::Chunk => EnabledStatistics::Chunk,
            ParquetStatistics::Page => EnabledStatistics::Page,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
#[value(rename_all = "lowercase")]
pub enum ParquetWriterVersion {
    V1,
    #[default]
    V2,
}

impl From<ParquetWriterVersion> for WriterVersion {
    fn from(writer_version: ParquetWriterVersion) -> Self {
        match writer_version {
            ParquetWriterVersion::V1 => WriterVersion::PARQUET_1_0,
            ParquetWriterVersion::V2 => WriterVersion::PARQUET_2_0,
        }
    }
}

impl From<ParquetWriterVersion> for i32 {
    fn from(writer_version: ParquetWriterVersion) -> Self {
        match writer_version {
            ParquetWriterVersion::V1 => 1,
            ParquetWriterVersion::V2 => 2,
        }
    }
}

#[derive(ValueEnum, Clone, Debug, Default, Copy)]
#[value(rename_all = "lowercase")]
pub enum ArrowCompression {
    Zstd,
    Lz4,
    #[default]
    None,
}

impl From<ArrowCompression> for Option<CompressionType> {
    fn from(compression: ArrowCompression) -> Self {
        match compression {
            ArrowCompression::Zstd => Some(CompressionType::ZSTD),
            ArrowCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
            ArrowCompression::None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SortColumn {
    pub name: String,
    pub direction: SortDirection,
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
#[value(rename_all = "lowercase")]
pub enum SortDirection {
    #[value(name = "asc")]
    Ascending,
    #[value(name = "desc")]
    Descending,
}

#[derive(Debug, Clone, Default)]
pub struct SortSpec {
    pub columns: Vec<SortColumn>,
}

impl From<Vec<String>> for SortSpec {
    fn from(names: Vec<String>) -> Self {
        Self {
            columns: uniq(&names)
                .iter()
                .map(|name| SortColumn {
                    name: name.clone(),
                    direction: SortDirection::Ascending,
                })
                .collect(),
        }
    }
}

impl SortSpec {
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn is_configured(&self) -> bool {
        !self.is_empty()
    }

    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|c| c.name == column_name)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn without_columns_named(&self, column_names: &[String]) -> Self {
        Self {
            columns: self
                .columns
                .iter()
                .filter(|c| !column_names.contains(&c.name))
                .cloned()
                .collect(),
        }
    }

    pub fn extend(&mut self, other: &Self) {
        self.columns.extend(other.columns.iter().cloned());
        self.columns = uniq_by(&self.columns, |c| &c.name);
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

        Ok(SortSpec {
            columns: uniq_by(&columns, |c| &c.name),
        })
    }
}

impl fmt::Display for SortSpec {
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
    pub ndv: Option<u64>,
}

impl FromStr for AllColumnsBloomFilterConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Ok(AllColumnsBloomFilterConfig {
                fpp: DEFAULT_BLOOM_FILTER_FPP,
                ndv: None,
            });
        }

        let mut fpp = None;
        let mut ndv = None;

        let parts = s
            .split(',')
            .map(|p| p.trim())
            .filter(|p| !p.is_empty())
            .collect::<Vec<&str>>();

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
                    "ndv" => {
                        if ndv.is_some() {
                            return Err(anyhow!(
                                "Invalid bloom filter specification, ndv is set twice: {}",
                                s
                            ));
                        }

                        ndv = Some(value.parse::<u64>().map_err(|e| {
                            anyhow::anyhow!("Invalid ndv value '{}': {}", value, e)
                        })?);
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unknown parameter '{}'. Valid parameters are 'fpp' and 'ndv'",
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
            ndv,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnBloomFilterConfig {
    pub fpp: f64,
    pub ndv: Option<u64>,
}

impl FromStr for ColumnBloomFilterConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut fpp = None;
        let mut ndv = None;

        let parts = s
            .split(',')
            .map(|p| p.trim())
            .filter(|p| !p.is_empty())
            .collect::<Vec<&str>>();

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
                    "ndv" => {
                        if ndv.is_some() {
                            return Err(anyhow!(
                                "Invalid bloom filter specification, ndv is set twice: {}",
                                s
                            ));
                        }

                        ndv = Some(value.parse::<u64>().map_err(|e| {
                            anyhow::anyhow!("Invalid ndv value '{}': {}", value, e)
                        })?);
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unknown parameter '{}'. Valid parameters are 'fpp' and 'ndv'",
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
            ndv,
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
                    ndv: None,
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

#[derive(Args, Debug)]
pub struct TransformCommand {
    /// Single input file path.
    #[arg(long, conflicts_with_all = ["from_many"], required_unless_present = "from_many")]
    pub from: Option<String>,

    /// Multiple input file paths (supports glob patterns). Can be specified multiple times.
    #[arg(long, conflicts_with = "from", required_unless_present = "from")]
    pub from_many: Vec<String>,

    /// Single output file path.
    #[arg(long, conflicts_with_all = ["to_many", "by"], required_unless_present = "to_many")]
    pub to: Option<String>,

    /// Output path template for partitioning (e.g., "{{region}}.parquet"). Requires --by.
    #[arg(
        long,
        conflicts_with = "to",
        requires = "by",
        required_unless_present = "to"
    )]
    pub to_many: Option<String>,

    /// Column(s) to partition by (comma-separated for multi-column partitioning).
    #[arg(long, short, requires = "to_many")]
    pub by: Option<String>,

    /// Names of columns to exclude from the output.
    #[arg(long, short = 'e')]
    pub exclude_columns: Vec<String>,

    /// List the output files after creation (only with --to-many).
    #[arg(long, short = 'l', value_enum, requires = "to_many")]
    pub list_outputs: Option<ListOutputsFormat>,

    /// Create directories as needed.
    #[arg(long, default_value_t = true)]
    pub create_dirs: bool,

    /// Overwrite existing files.
    #[arg(long)]
    pub overwrite: bool,

    /// SQL query to apply to the data. The input data is available as table 'data'.
    ///
    /// Examples:
    ///   --query "SELECT * FROM data WHERE status = 'active'"
    ///   --query "SELECT id, name, amount FROM data"
    ///   --query "SELECT region, SUM(amount) FROM data GROUP BY region"
    ///   --query "SELECT *, amount * 1.1 as adjusted FROM data"
    #[arg(short, long, verbatim_doc_comment)]
    pub query: Option<String>,

    /// The query dialect to use.
    #[arg(short, long, default_value_t, value_enum)]
    pub dialect: QueryDialect,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long)]
    pub sort_by: Option<SortSpec>,

    /// Override input format detection.
    #[arg(long, value_enum)]
    pub input_format: Option<DataFormat>,

    /// Override output format detection.
    #[arg(long, value_enum)]
    pub output_format: Option<DataFormat>,

    /// Arrow IPC compression codec.
    #[arg(long, value_enum)]
    pub arrow_compression: Option<ArrowCompression>,

    /// Arrow IPC format (file or stream).
    #[arg(long, value_enum)]
    pub arrow_format: Option<ArrowIPCFormat>,

    /// Arrow record batch size.
    #[arg(long)]
    pub arrow_record_batch_size: Option<usize>,

    /// Parquet compression codec.
    #[arg(long, value_enum)]
    pub parquet_compression: Option<ParquetCompression>,

    /// Enable bloom filters for all columns with optional custom settings.
    ///
    /// Formats:
    ///   --parquet-bloom-all                       # Use defaults (fpp=0.01, auto NDV)
    ///   --parquet-bloom-all "fpp=VALUE"           # Custom FPP
    ///   --parquet-bloom-all "ndv=VALUE"           # Custom NDV
    ///   --parquet-bloom-all "fpp=VALUE,ndv=VALUE" # Custom FPP and NDV
    ///
    /// Examples:
    ///   --parquet-bloom-all                     # Use defaults
    ///   --parquet-bloom-all "fpp=0.001"         # Custom FPP
    ///   --parquet-bloom-all "ndv=10000"         # Custom NDV (10k distinct values)
    ///   --parquet-bloom-all "fpp=0.001,ndv=10000" # Custom FPP and NDV
    #[arg(
        long,
        value_name = "[fpp=VALUE][,ndv=VALUE]",
        conflicts_with = "parquet_bloom_column",
        num_args = 0..=1,
        default_missing_value = "",
        verbatim_doc_comment
    )]
    pub parquet_bloom_all: Option<AllColumnsBloomFilterConfig>,

    /// Enable bloom filter for specific columns with optional custom settings.
    ///
    /// Formats:
    ///   COLUMN                     # Use defaults (fpp=0.01, auto NDV)
    ///   COLUMN:fpp=VALUE           # Custom FPP
    ///   COLUMN:ndv=VALUE           # Custom NDV
    ///   COLUMN:fpp=VALUE,ndv=VALUE # Custom FPP and NDV
    ///
    /// Examples:
    ///   --parquet-bloom-column "user_id"                   # Use defaults
    ///   --parquet-bloom-column "user_id:fpp=0.001"         # Custom FPP
    ///   --parquet-bloom-column "user_id:ndv=50000"         # Custom NDV (50k distinct values)
    ///   --parquet-bloom-column "user_id:fpp=0.001,ndv=50000" # Custom FPP and NDV
    #[arg(
        long,
        value_name = "COLUMN[:fpp=VALUE][,ndv=VALUE]",
        conflicts_with = "parquet_bloom_all",
        verbatim_doc_comment
    )]
    pub parquet_bloom_column: Vec<ColumnSpecificBloomFilterConfig>,

    /// Maximum number of rows per Parquet row group.
    #[arg(long)]
    pub parquet_row_group_size: Option<usize>,

    /// Parquet column statistics level.
    #[arg(long, value_enum)]
    pub parquet_statistics: Option<ParquetStatistics>,

    /// Parquet writer version.
    #[arg(long, value_enum)]
    pub parquet_writer_version: Option<ParquetWriterVersion>,

    /// Disable dictionary encoding for Parquet columns.
    #[arg(long)]
    pub parquet_no_dictionary: bool,

    /// Embed metadata indicating that the file's data is sorted.
    ///
    /// Requires --sort-by to be set.
    #[arg(long, default_value_t = false, requires = "sort_by")]
    pub parquet_sorted_metadata: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "lowercase")]
pub enum DataFormat {
    Arrow,
    Parquet,
}

#[derive(ValueEnum, PartialEq, Clone, Debug, Default)]
pub enum ArrowIPCFormat {
    #[default]
    #[value(name = "file")]
    File,
    #[value(name = "stream")]
    Stream,
}

impl fmt::Display for ArrowIPCFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::File => "file",
            Self::Stream => "stream",
        };
        write!(f, "{s}")
    }
}

impl FromStr for ArrowIPCFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "file" => Ok(ArrowIPCFormat::File),
            "stream" => Ok(ArrowIPCFormat::Stream),
            _ => Err(anyhow!(
                "Invalid Arrow IPC format: {}. Valid options: file, stream",
                s
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_enum_from_str() {
        assert_eq!(
            QueryDialect::from_str("duckdb", true),
            Ok(QueryDialect::DuckDb)
        );
        assert_eq!(
            QueryDialect::from_str("generic", true),
            Ok(QueryDialect::Generic)
        );
        assert_eq!(
            QueryDialect::from_str("mysql", true),
            Ok(QueryDialect::MySQL)
        );
        assert_eq!(QueryDialect::from_str("hive", true), Ok(QueryDialect::Hive));
        assert_eq!(
            QueryDialect::from_str("sqlite", true),
            Ok(QueryDialect::SQLite)
        );
    }
}
