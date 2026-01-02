pub mod commands;
pub mod inspection;
pub mod io_strategies;
pub mod operations;
pub mod pipeline;
pub mod sinks;
pub mod sources;
pub mod utils;

use crate::utils::collections::{uniq, uniq_by};
use anyhow::{Result, anyhow};
use arrow::ipc::CompressionType;
use camino::Utf8PathBuf;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, builder::ValueHint};
use clap_complete::Shell;
use datafusion::config::Dialect;
use parquet::{
    basic::{Compression, Encoding, GzipLevel, ZstdLevel},
    file::properties::{EnabledStatistics, WriterVersion},
};
use std::{
    fmt::{self, Formatter},
    io::{self, IsTerminal},
    str::FromStr,
};
use strum_macros::Display;

/// Parse a usize that must be at least 1.
fn parse_at_least_one(s: &str) -> Result<usize, String> {
    let n: usize = s.parse().map_err(|e| format!("{e}"))?;
    if n == 0 {
        Err("value must be at least 1".into())
    } else {
        Ok(n)
    }
}

/// Parse a human-readable byte size (e.g., "512MB", "2GB") that must be greater than 0.
#[allow(clippy::cast_possible_truncation)]
fn parse_nonzero_byte_size(s: &str) -> Result<usize, String> {
    let bytes = s
        .parse::<bytesize::ByteSize>()
        .map_err(|_| {
            format!("invalid byte size '{s}': expected format like '512MB', '2GB', or '1GiB'")
        })?
        .as_u64() as usize;
    if bytes == 0 {
        Err("value must be greater than 0".into())
    } else {
        Ok(bytes)
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Maximum worker threads for the tokio async runtime.
    ///
    /// Controls the thread pool size for async operations including I/O and DataFusion
    /// query execution. Both DataFusion and parquet encoding use tokio's thread pools
    /// (async pool for queries, blocking pool for CPU-bound work like encoding).
    /// Defaults to the number of CPU cores.
    #[arg(long, short = 't', global = true, value_parser = parse_at_least_one)]
    pub threads: Option<usize>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
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

    /// Inspect file metadata and structure.
    ///
    /// Examples:
    ///   # Identify format
    ///   silk-chiffon inspect identify data.parquet
    ///
    ///   # Inspect Parquet file
    ///   silk-chiffon inspect parquet data.parquet --stats --row-groups
    ///
    ///   # Inspect Arrow file
    ///   silk-chiffon inspect arrow data.arrow --schema --batches
    #[command(verbatim_doc_comment)]
    Inspect(InspectCommand),

    /// Generate shell completions for your shell.
    ///
    /// To add completions for your current shell session only:
    ///   zsh:  eval "$(silk-chiffon completions zsh)"
    ///   bash: eval "$(silk-chiffon completions bash)"
    ///   fish: silk-chiffon completions fish | source
    ///
    /// To persist completions across sessions:
    ///   zsh:  echo 'eval "$(silk-chiffon completions zsh)"' >> ~/.zshrc
    ///   bash: echo 'eval "$(silk-chiffon completions bash)"' >> ~/.bashrc
    ///   fish: silk-chiffon completions fish > ~/.config/fish/completions/silk-chiffon.fish
    #[command(verbatim_doc_comment)]
    Completions {
        /// Shell to generate completions for
        shell: Shell,
    },
}

impl Commands {
    pub fn generate_completions(shell: Shell) {
        clap_complete::generate(
            shell,
            &mut Cli::command(),
            "silk-chiffon",
            &mut std::io::stdout(),
        );
    }
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

#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
#[value(rename_all = "kebab-case")]
pub enum ParquetEncoding {
    #[default]
    Plain,
    Rle,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    ByteStreamSplit,
}

impl ParquetEncoding {
    /// Returns true if this encoding requires parquet writer version 2 for compatibility.
    pub fn requires_v2(&self) -> bool {
        matches!(
            self,
            ParquetEncoding::DeltaBinaryPacked
                | ParquetEncoding::DeltaLengthByteArray
                | ParquetEncoding::DeltaByteArray
                | ParquetEncoding::ByteStreamSplit
        )
    }

    /// Validates that this encoding is compatible with the given Arrow data type.
    /// Returns an error message if incompatible, None if compatible.
    pub fn validate_for_type(&self, data_type: &arrow::datatypes::DataType) -> Option<String> {
        use arrow::datatypes::DataType;

        match self {
            ParquetEncoding::Plain => None, // works for all types

            ParquetEncoding::Rle => {
                // RLE only works for boolean
                if matches!(data_type, DataType::Boolean) {
                    None
                } else {
                    Some(format!(
                        "RLE encoding only supports Boolean, got {}",
                        data_type
                    ))
                }
            }

            ParquetEncoding::DeltaBinaryPacked => {
                // only for integer types
                if matches!(
                    data_type,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Date32
                        | DataType::Date64
                        | DataType::Time32(_)
                        | DataType::Time64(_)
                        | DataType::Timestamp(_, _)
                        | DataType::Duration(_)
                ) {
                    None
                } else {
                    Some(format!(
                        "DELTA_BINARY_PACKED encoding only supports integer types, got {}",
                        data_type
                    ))
                }
            }

            ParquetEncoding::DeltaLengthByteArray | ParquetEncoding::DeltaByteArray => {
                // only for byte array types
                if matches!(
                    data_type,
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::Utf8View
                        | DataType::BinaryView
                ) {
                    None
                } else {
                    Some(format!(
                        "{} encoding only supports byte array types (Utf8, Binary, etc.), got {}",
                        self, data_type
                    ))
                }
            }

            ParquetEncoding::ByteStreamSplit => {
                // for fixed-width types: floats and integers
                if matches!(
                    data_type,
                    DataType::Float16
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::FixedSizeBinary(_)
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                ) {
                    None
                } else {
                    Some(format!(
                        "BYTE_STREAM_SPLIT encoding only supports fixed-width types (floats, integers, decimals), got {}",
                        data_type
                    ))
                }
            }
        }
    }
}

impl From<ParquetEncoding> for Encoding {
    fn from(encoding: ParquetEncoding) -> Self {
        match encoding {
            ParquetEncoding::Plain => Encoding::PLAIN,
            ParquetEncoding::Rle => Encoding::RLE,
            ParquetEncoding::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
            ParquetEncoding::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            ParquetEncoding::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
            ParquetEncoding::ByteStreamSplit => Encoding::BYTE_STREAM_SPLIT,
        }
    }
}

impl fmt::Display for ParquetEncoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            ParquetEncoding::Plain => "plain",
            ParquetEncoding::Rle => "rle",
            ParquetEncoding::DeltaBinaryPacked => "delta-binary-packed",
            ParquetEncoding::DeltaLengthByteArray => "delta-length-byte-array",
            ParquetEncoding::DeltaByteArray => "delta-byte-array",
            ParquetEncoding::ByteStreamSplit => "byte-stream-split",
        };
        write!(f, "{s}")
    }
}

/// Per-column encoding configuration, parsed from "column=encoding" format.
#[derive(Debug, Clone)]
pub struct ColumnEncodingConfig {
    pub name: String,
    pub encoding: ParquetEncoding,
}

impl FromStr for ColumnEncodingConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, encoding_str) = s.split_once('=').ok_or_else(|| {
            anyhow!(
                "Invalid column encoding format '{}'. Expected 'column=encoding' (e.g., 'id=delta-binary-packed')",
                s
            )
        })?;

        let name = name.trim();
        if name.is_empty() {
            return Err(anyhow!("Column name cannot be empty in '{}'", s));
        }

        let encoding_str = encoding_str.trim();
        let encoding = ParquetEncoding::from_str(encoding_str, true).map_err(|_| {
            anyhow!(
                "Invalid encoding '{}'. Valid options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array, byte-stream-split",
                encoding_str
            )
        })?;

        Ok(ColumnEncodingConfig {
            name: name.to_string(),
            encoding,
        })
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
    //
    // ─── Input/Output ──────────────────────────────────────────────────────────────────
    //
    /// Single input file path.
    #[arg(
        long,
        conflicts_with_all = ["from_many"],
        required_unless_present = "from_many",
        help_heading = "Input/Output"
    )]
    pub from: Option<String>,

    /// Multiple input file paths (supports glob patterns). Can be specified multiple times.
    #[arg(
        long,
        conflicts_with = "from",
        required_unless_present = "from",
        help_heading = "Input/Output"
    )]
    pub from_many: Vec<String>,

    /// Override input format detection.
    #[arg(long, value_enum, help_heading = "Input/Output")]
    pub input_format: Option<DataFormat>,

    /// Override output format detection.
    #[arg(long, value_enum, help_heading = "Input/Output")]
    pub output_format: Option<DataFormat>,

    /// Single output file path.
    #[arg(
        long,
        conflicts_with_all = ["to_many", "by"],
        required_unless_present = "to_many",
        help_heading = "Input/Output"
    )]
    pub to: Option<String>,

    /// Output path template for partitioning (e.g., "{{region}}.parquet"). Requires --by.
    #[arg(
        long,
        conflicts_with = "to",
        requires = "by",
        required_unless_present = "to",
        help_heading = "Input/Output"
    )]
    pub to_many: Option<String>,

    //
    // ─── Transformations ───────────────────────────────────────────────────────────────
    //
    /// The query dialect to use.
    #[arg(
        short,
        long,
        default_value_t,
        value_enum,
        help_heading = "Transformations"
    )]
    pub dialect: QueryDialect,

    /// Names of columns to exclude from the output.
    #[arg(long, short = 'e', help_heading = "Transformations")]
    pub exclude_columns: Vec<String>,

    /// SQL query to apply to the data. The input data is available as table 'data'.
    ///
    /// Examples:
    ///   --query "SELECT * FROM data WHERE status = 'active'"
    ///   --query "SELECT id, name, amount FROM data"
    ///   --query "SELECT region, SUM(amount) FROM data GROUP BY region"
    ///   --query "SELECT *, amount * 1.1 as adjusted FROM data"
    #[arg(short, long, verbatim_doc_comment, help_heading = "Transformations")]
    pub query: Option<String>,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like "col_a,col_b:desc,col_c".
    #[arg(short, long, help_heading = "Transformations")]
    pub sort_by: Option<SortSpec>,

    //
    // ─── Execution ─────────────────────────────────────────────────────────────────────
    //
    /// Memory limit for query execution (e.g., "512MB", "2GB").
    ///
    /// Limits memory used by DataFusion for buffering operators (sort, group by,
    /// aggregation). When exceeded, operators spill to disk. Only tracks large
    /// allocations, not streaming data. Supports suffixes: B, KB, MB, GB, TB
    /// (or KiB, MiB, GiB, TiB for binary). Default: unlimited.
    #[arg(long, help_heading = "Execution", value_parser = parse_nonzero_byte_size)]
    pub memory_limit: Option<usize>,

    /// Number of partitions for query execution parallelism.
    ///
    /// Controls how DataFusion partitions data during queries (aggregations, joins, sorts).
    /// Higher values increase parallelism but use more memory. These tasks run on the
    /// tokio thread pool (--threads). Defaults to CPU cores.
    #[arg(long, help_heading = "Execution", value_parser = parse_at_least_one)]
    pub target_partitions: Option<usize>,

    //
    // ─── Partitioning ──────────────────────────────────────────────────────────────────
    //
    /// Column(s) to partition by (comma-separated for multi-column partitioning).
    #[arg(long, short, requires = "to_many", help_heading = "Partitioning")]
    pub by: Option<String>,

    /// List the output files after creation (only with --to-many).
    #[arg(
        long,
        short = 'l',
        value_enum,
        requires = "to_many",
        help_heading = "Partitioning"
    )]
    pub list_outputs: Option<ListOutputsFormat>,

    /// Write output file listing to a file instead of stdout.
    #[arg(long, requires = "list_outputs", help_heading = "Partitioning")]
    pub list_outputs_file: Option<Utf8PathBuf>,

    //
    // ─── Output Behavior ──────────────────────────────────────────────────────────────
    //
    /// Create directories as needed.
    #[arg(long, default_value_t = true, help_heading = "Output Behavior")]
    pub create_dirs: bool,

    /// Overwrite existing files.
    #[arg(long, help_heading = "Output Behavior")]
    pub overwrite: bool,

    //
    // ─── Arrow Options ─────────────────────────────────────────────────────────────────
    //
    /// Arrow IPC compression codec.
    #[arg(long, value_enum, help_heading = "Arrow Options")]
    pub arrow_compression: Option<ArrowCompression>,

    /// Arrow IPC format (file or stream).
    #[arg(long, value_enum, help_heading = "Arrow Options")]
    pub arrow_format: Option<ArrowIPCFormat>,

    /// Arrow record batch size.
    #[arg(long, help_heading = "Arrow Options")]
    pub arrow_record_batch_size: Option<usize>,

    //
    // ─── Parquet Options ───────────────────────────────────────────────────────────────
    //
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
        verbatim_doc_comment,
        help_heading = "Parquet Options"
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
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_bloom_column: Vec<ColumnSpecificBloomFilterConfig>,

    /// I/O buffer size for Parquet writing (e.g., "32MB", "64MB", "1GB").
    ///
    /// Controls the size of the buffer used when writing encoded data to disk.
    /// Supports suffixes: B, KB, MB, GB, TB (or KiB, MiB, GiB, TiB for binary).
    /// Default: 32MB.
    #[arg(long, help_heading = "Parquet Options", value_parser = parse_nonzero_byte_size)]
    pub parquet_buffer_size: Option<usize>,

    /// Enable dictionary encoding for specific columns. Can be specified multiple times.
    ///
    /// Overrides --parquet-no-dictionary for the named columns, enabling dictionary encoding
    /// even when it's globally disabled.
    ///
    /// Useful when most columns have high cardinality (dictionary disabled globally) but
    /// a few columns have low cardinality and would benefit from dictionary encoding.
    #[arg(
        long,
        value_name = "COLUMN",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_column_dictionary: Vec<String>,

    /// Set data page encoding for specific columns. Can be specified multiple times.
    ///
    /// Overrides --parquet-encoding for the named column. See --parquet-encoding for
    /// how this interacts with dictionary encoding.
    ///
    /// Format: COLUMN=ENCODING
    ///
    /// Options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array,
    /// byte-stream-split
    ///
    /// Examples:
    ///   --parquet-column-encoding id=delta-binary-packed
    ///   --parquet-column-encoding name=delta-byte-array
    ///   --parquet-column-encoding price=byte-stream-split
    #[arg(
        long,
        value_name = "COLUMN=ENCODING",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_column_encoding: Vec<ColumnEncodingConfig>,

    /// Disable dictionary encoding for specific columns. Can be specified multiple times.
    ///
    /// Overrides the default (dictionary enabled) for the named columns.
    ///
    /// Useful for high-cardinality columns like UUIDs or timestamps where dictionary
    /// encoding adds overhead without compression benefit.
    #[arg(
        long,
        value_name = "COLUMN",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_column_no_dictionary: Vec<String>,

    /// Parquet compression codec.
    #[arg(long, value_enum, help_heading = "Parquet Options")]
    pub parquet_compression: Option<ParquetCompression>,

    /// Data page encoding for Parquet columns.
    ///
    /// This encoding is used for column data pages. Its role depends on dictionary encoding:
    /// - Dictionary enabled (default): this is the fallback encoding, used when the dictionary
    ///   becomes too large or is inefficient for the data.
    /// - Dictionary disabled: this is the primary encoding for all data.
    ///
    /// If not specified, the writer automatically selects an encoding based on column type
    /// and writer version. With Parquet v2: integers use delta-binary-packed, strings use
    /// delta-byte-array, booleans use rle. With Parquet v1: everything uses plain.
    ///
    /// Options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array,
    /// byte-stream-split
    #[arg(
        long,
        value_enum,
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_encoding: Option<ParquetEncoding>,

    /// Disable dictionary encoding globally for all Parquet columns.
    ///
    /// Dictionary encoding builds a dictionary of unique values and stores references to it,
    /// which is very effective for low-cardinality columns (few unique values). When disabled,
    /// columns use their data page encoding directly.
    ///
    /// Default: dictionary encoding is enabled.
    ///
    /// Use --parquet-column-dictionary or --parquet-column-no-dictionary to override
    /// this setting for specific columns.
    #[arg(long, verbatim_doc_comment, help_heading = "Parquet Options")]
    pub parquet_no_dictionary: bool,

    /// Maximum row groups to encode in parallel.
    ///
    /// Controls how many row groups can be encoded concurrently. Column encoding within
    /// each row group runs on tokio's blocking thread pool via spawn_blocking.
    /// Defaults to the number of CPU cores.
    #[arg(long, help_heading = "Parquet Options", value_parser = parse_at_least_one)]
    pub parquet_parallelism: Option<usize>,

    /// Maximum number of rows per Parquet row group.
    #[arg(long, help_heading = "Parquet Options")]
    pub parquet_row_group_size: Option<usize>,

    /// Embed metadata indicating that the file's data is sorted.
    ///
    /// Requires --sort-by to be set.
    #[arg(
        long,
        default_value_t = false,
        requires = "sort_by",
        help_heading = "Parquet Options"
    )]
    pub parquet_sorted_metadata: bool,

    /// Parquet column statistics level.
    #[arg(long, value_enum, help_heading = "Parquet Options")]
    pub parquet_statistics: Option<ParquetStatistics>,

    /// Parquet writer version.
    #[arg(long, value_enum, help_heading = "Parquet Options")]
    pub parquet_writer_version: Option<ParquetWriterVersion>,

    //
    // ─── Vortex Options ────────────────────────────────────────────────────────────────
    //
    /// Vortex record batch size.
    #[arg(long, help_heading = "Vortex Options")]
    pub vortex_record_batch_size: Option<usize>,
}

#[derive(Args, Debug)]
pub struct InspectCommand {
    #[command(subcommand)]
    pub command: InspectSubcommand,
}

#[derive(Subcommand, Debug)]
pub enum InspectSubcommand {
    /// Detect file format
    Identify(InspectIdentifyArgs),
    /// Inspect a Parquet file
    Parquet(InspectParquetArgs),
    /// Inspect an Arrow IPC file
    Arrow(InspectArrowArgs),
    /// Inspect a Vortex file
    Vortex(InspectVortexArgs),
}

#[derive(Args, Debug)]
pub struct InspectIdentifyArgs {
    /// Path to the file to identify
    #[arg(value_hint = ValueHint::FilePath)]
    pub file: Utf8PathBuf,
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    pub format: OutputFormat,
}

#[derive(Args, Debug)]
pub struct InspectParquetArgs {
    /// Path to the Parquet file
    #[arg(value_hint = ValueHint::FilePath)]
    pub file: Utf8PathBuf,
    /// Show full schema details
    #[arg(long)]
    pub schema: bool,
    /// Show column statistics and encoding details
    #[arg(long)]
    pub stats: bool,
    /// Show detailed encoding breakdown (dictionary vs data pages)
    #[arg(long)]
    pub encodings: bool,
    /// Show per-row-group details
    #[arg(long)]
    pub row_groups: bool,
    /// Show file-level key-value metadata
    #[arg(long)]
    pub metadata: bool,
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    pub format: OutputFormat,
}

#[derive(Args, Debug)]
pub struct InspectArrowArgs {
    /// Path to the Arrow IPC file
    #[arg(value_hint = ValueHint::FilePath)]
    pub file: Utf8PathBuf,
    /// Show full schema details
    #[arg(long)]
    pub schema: bool,
    /// Show per-record-batch details
    #[arg(long)]
    pub batches: bool,
    /// Show custom metadata (file format only)
    #[arg(long)]
    pub metadata: bool,
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    pub format: OutputFormat,
    /// Count total rows (slow! requires reading entire file)
    #[arg(long)]
    pub row_count: bool,
}

#[derive(Args, Debug)]
pub struct InspectVortexArgs {
    /// Path to the Vortex file
    #[arg(value_hint = ValueHint::FilePath)]
    pub file: Utf8PathBuf,
    /// Show full schema details
    #[arg(long)]
    pub schema: bool,
    /// Show per-column statistics
    #[arg(long)]
    pub stats: bool,
    /// Show layout structure
    #[arg(long)]
    pub layout: bool,
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    pub format: OutputFormat,
}

/// Output format for inspect commands
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[value(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Auto-detect: JSON if stdout is not a TTY, otherwise text
    #[default]
    Auto,
    /// Human-readable text output
    Text,
    /// JSON output
    Json,
}

impl OutputFormat {
    pub fn resolves_to_json(&self) -> bool {
        match self {
            OutputFormat::Auto => !io::stdout().is_terminal(),
            OutputFormat::Text => false,
            OutputFormat::Json => true,
        }
    }

    pub fn resolves_to_text(&self) -> bool {
        match self {
            OutputFormat::Auto => io::stdout().is_terminal(),
            OutputFormat::Text => true,
            OutputFormat::Json => false,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "lowercase")]
pub enum DataFormat {
    Arrow,
    Parquet,
    Vortex,
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

    mod parquet_encoding_tests {
        use super::*;

        #[test]
        fn test_parquet_encoding_from_str() {
            assert_eq!(
                ParquetEncoding::from_str("plain", true),
                Ok(ParquetEncoding::Plain)
            );
            assert_eq!(
                ParquetEncoding::from_str("rle", true),
                Ok(ParquetEncoding::Rle)
            );
            assert_eq!(
                ParquetEncoding::from_str("delta-binary-packed", true),
                Ok(ParquetEncoding::DeltaBinaryPacked)
            );
            assert_eq!(
                ParquetEncoding::from_str("delta-length-byte-array", true),
                Ok(ParquetEncoding::DeltaLengthByteArray)
            );
            assert_eq!(
                ParquetEncoding::from_str("delta-byte-array", true),
                Ok(ParquetEncoding::DeltaByteArray)
            );
            assert_eq!(
                ParquetEncoding::from_str("byte-stream-split", true),
                Ok(ParquetEncoding::ByteStreamSplit)
            );
        }

        #[test]
        fn test_parquet_encoding_requires_v2() {
            assert!(!ParquetEncoding::Plain.requires_v2());
            assert!(!ParquetEncoding::Rle.requires_v2());
            assert!(ParquetEncoding::DeltaBinaryPacked.requires_v2());
            assert!(ParquetEncoding::DeltaLengthByteArray.requires_v2());
            assert!(ParquetEncoding::DeltaByteArray.requires_v2());
            assert!(ParquetEncoding::ByteStreamSplit.requires_v2());
        }

        #[test]
        fn test_parquet_encoding_display() {
            assert_eq!(format!("{}", ParquetEncoding::Plain), "plain");
            assert_eq!(format!("{}", ParquetEncoding::Rle), "rle");
            assert_eq!(
                format!("{}", ParquetEncoding::DeltaBinaryPacked),
                "delta-binary-packed"
            );
            assert_eq!(
                format!("{}", ParquetEncoding::DeltaLengthByteArray),
                "delta-length-byte-array"
            );
            assert_eq!(
                format!("{}", ParquetEncoding::DeltaByteArray),
                "delta-byte-array"
            );
            assert_eq!(
                format!("{}", ParquetEncoding::ByteStreamSplit),
                "byte-stream-split"
            );
        }
    }

    mod column_encoding_config_tests {
        use super::*;

        #[test]
        fn test_parse_column_encoding_config() {
            let config: ColumnEncodingConfig = "id=delta-binary-packed".parse().unwrap();
            assert_eq!(config.name, "id");
            assert_eq!(config.encoding, ParquetEncoding::DeltaBinaryPacked);
        }

        #[test]
        fn test_parse_column_encoding_config_with_spaces() {
            let config: ColumnEncodingConfig = "  name  =  delta-byte-array  ".parse().unwrap();
            assert_eq!(config.name, "name");
            assert_eq!(config.encoding, ParquetEncoding::DeltaByteArray);
        }

        #[test]
        fn test_parse_column_encoding_config_all_encodings() {
            let test_cases = [
                ("col=plain", ParquetEncoding::Plain),
                ("col=rle", ParquetEncoding::Rle),
                (
                    "col=delta-binary-packed",
                    ParquetEncoding::DeltaBinaryPacked,
                ),
                (
                    "col=delta-length-byte-array",
                    ParquetEncoding::DeltaLengthByteArray,
                ),
                ("col=delta-byte-array", ParquetEncoding::DeltaByteArray),
                ("col=byte-stream-split", ParquetEncoding::ByteStreamSplit),
            ];

            for (input, expected_encoding) in test_cases {
                let config: ColumnEncodingConfig = input.parse().unwrap();
                assert_eq!(
                    config.encoding, expected_encoding,
                    "Failed for input: {}",
                    input
                );
            }
        }

        #[test]
        fn test_parse_column_encoding_config_missing_equals() {
            let result: Result<ColumnEncodingConfig, _> = "id".parse();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("column=encoding"));
        }

        #[test]
        fn test_parse_column_encoding_config_empty_column_name() {
            let result: Result<ColumnEncodingConfig, _> = "=plain".parse();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("empty"));
        }

        #[test]
        fn test_parse_column_encoding_config_invalid_encoding() {
            let result: Result<ColumnEncodingConfig, _> = "id=invalid".parse();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("Invalid encoding"));
        }
    }
}
