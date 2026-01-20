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
pub fn parse_at_least_one(s: &str) -> Result<usize> {
    let n: usize = s.parse().map_err(anyhow::Error::new)?;

    if n == 0 {
        anyhow::bail!("value must be at least 1");
    } else {
        Ok(n)
    }
}

/// Parse a human-readable byte size (e.g., "512MB", "2GB") that must be greater than 0.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_nonzero_byte_size(s: &str) -> Result<usize> {
    let bytes = s
        .parse::<bytesize::ByteSize>()
        .map_err(|_| {
            anyhow!("invalid byte size '{s}': expected format like '512MB', '2GB', or '1GiB'")
        })?
        .as_u64() as usize;
    if bytes == 0 {
        anyhow::bail!("value must be greater than 0");
    } else {
        Ok(bytes)
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
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

/// Strategy for writing partitioned output files.
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Display)]
#[value(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum PartitionStrategy {
    /// Sort by partition columns first, then write one file at a time.
    /// Uses minimal file handles but requires sorting the entire dataset.
    /// Best for high-cardinality partition columns, or when partition columns are highly fragmented.
    #[default]
    SortSingle,
    /// Keep a file handle open per partition, write rows directly.
    /// No sorting required, preserves input order within each partition.
    /// Best for low-cardinality partition columns with low fragmentation.
    NosortMulti,
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
    #[default]
    Chunk,
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

#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
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

/// Parquet encoder strategy.
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
#[value(rename_all = "lowercase")]
pub enum ParquetEncoder {
    /// Sequential single-threaded encoder using Arrow's built-in writer.
    Sequential,
    /// Parallel encoder using rayon for concurrent column encoding.
    #[default]
    Parallel,
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

/// Bloom filter configuration with granular control.
///
/// Resolution order:
/// 1. Column-specific enables (`column_enabled`) take highest precedence
/// 2. Column-specific disables (`column_disabled`) take second precedence
/// 3. Global setting (`all_enabled`) applies as default for unspecified columns
#[derive(Debug, Clone, Default)]
pub struct BloomFilterConfig {
    /// Global setting: Some(config) = enabled for all, None = disabled for all
    all_enabled: Option<AllColumnsBloomFilterConfig>,
    /// Columns explicitly enabled with config (overrides all_enabled)
    column_enabled: Vec<ColumnSpecificBloomFilterConfig>,
    /// Columns explicitly disabled (overrides all_enabled, but not column_enabled)
    column_disabled: Vec<String>,
}

impl BloomFilterConfig {
    pub fn try_new(
        all_enabled: Option<AllColumnsBloomFilterConfig>,
        column_enabled: Vec<ColumnSpecificBloomFilterConfig>,
        column_disabled: Vec<String>,
    ) -> Result<Self> {
        let config = Self {
            all_enabled,
            column_enabled,
            column_disabled,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn is_configured(&self) -> bool {
        self.all_enabled.is_some() || !self.column_enabled.is_empty()
    }

    pub fn all_enabled(&self) -> Option<&AllColumnsBloomFilterConfig> {
        self.all_enabled.as_ref()
    }

    pub fn column_enabled(&self) -> &[ColumnSpecificBloomFilterConfig] {
        &self.column_enabled
    }

    pub fn column_disabled(&self) -> &[String] {
        &self.column_disabled
    }

    pub fn is_column_enabled(&self, col_name: &str) -> bool {
        self.column_enabled.iter().any(|c| c.name == col_name)
    }

    pub fn is_column_disabled(&self, col_name: &str) -> bool {
        self.column_disabled.iter().any(|c| c == col_name)
    }

    pub fn get_column_config(&self, col_name: &str) -> Option<&ColumnSpecificBloomFilterConfig> {
        self.column_enabled.iter().find(|c| c.name == col_name)
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen_enabled = std::collections::HashSet::new();
        for config in &self.column_enabled {
            if !seen_enabled.insert(&config.name) {
                anyhow::bail!(
                    "column '{}' specified multiple times as enabled",
                    config.name
                );
            }
        }

        let mut seen_disabled = std::collections::HashSet::new();
        for name in &self.column_disabled {
            if !seen_disabled.insert(name) {
                anyhow::bail!("column '{}' specified multiple times as disabled", name);
            }
        }

        for enabled in &self.column_enabled {
            if self.column_disabled.contains(&enabled.name) {
                anyhow::bail!(
                    "column '{}' specified as both enabled and disabled",
                    enabled.name
                );
            }
        }
        Ok(())
    }

    pub fn builder() -> BloomFilterConfigBuilder {
        BloomFilterConfigBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct BloomFilterConfigBuilder {
    all_enabled: Option<AllColumnsBloomFilterConfig>,
    column_enabled: Vec<ColumnSpecificBloomFilterConfig>,
    column_disabled: Vec<String>,
}

impl BloomFilterConfigBuilder {
    pub fn all_enabled(mut self, config: AllColumnsBloomFilterConfig) -> Self {
        self.all_enabled = Some(config);
        self
    }

    pub fn all_disabled(mut self) -> Self {
        self.all_enabled = None;
        self
    }

    pub fn enable_column(mut self, config: ColumnSpecificBloomFilterConfig) -> Self {
        self.column_enabled.push(config);
        self
    }

    pub fn disable_column(mut self, name: impl Into<String>) -> Self {
        self.column_disabled.push(name.into());
        self
    }

    pub fn build(self) -> Result<BloomFilterConfig> {
        let config = BloomFilterConfig {
            all_enabled: self.all_enabled,
            column_enabled: self.column_enabled,
            column_disabled: self.column_disabled,
        };
        config.validate()?;
        Ok(config)
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

    /// Preserve the row order from the input file in the output.
    ///
    /// By default, DataFusion reads files using multiple partitions for parallelism,
    /// which can interleave rows. This flag forces single-partition reading to maintain
    /// the original row order. Only valid for single-file-to-single-file transforms
    /// without queries or sorting.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with_all = ["query", "sort_by", "to_many", "from_many"],
        help_heading = "Execution"
    )]
    pub preserve_input_order: bool,

    /// Number of partitions for query execution parallelism.
    ///
    /// Controls how DataFusion partitions data during queries (aggregations, joins, sorts).
    /// Higher values increase parallelism but use more memory. These tasks run on the
    /// tokio thread pool (--threads). Defaults to CPU cores.
    #[arg(long, help_heading = "Execution", value_parser = parse_at_least_one)]
    pub target_partitions: Option<usize>,

    /// Maximum worker threads for the tokio async runtime.
    ///
    /// Controls the thread pool size for async operations including I/O and DataFusion
    /// query execution. Defaults to the number of CPU cores.
    #[arg(long, short = 't', help_heading = "Execution", value_parser = parse_at_least_one)]
    pub threads: Option<usize>,

    //
    // ─── Partitioning ──────────────────────────────────────────────────────────────────
    //
    /// Column(s) to partition by (comma-separated for multi-column partitioning).
    /// Partition output by column values. Only primitive types (integers, floats,
    /// strings, dates, etc.) are supported. Complex types (arrays, structs, maps)
    /// will error.
    #[arg(long, short, requires = "to_many", help_heading = "Partitioning")]
    pub by: Option<String>,

    /// Partitioning strategy for writing output files.
    #[arg(
        long,
        value_enum,
        default_value_t,
        requires = "by",
        help_heading = "Partitioning"
    )]
    pub partition_strategy: PartitionStrategy,

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
    /// Bloom filters are enabled for all columns by default; use this to customize settings.
    ///
    /// Formats:
    ///   --parquet-bloom-all                       # Use defaults (fpp=0.01, auto NDV)
    ///   --parquet-bloom-all "fpp=VALUE"           # Custom FPP
    ///   --parquet-bloom-all "ndv=VALUE"           # Custom NDV
    ///   --parquet-bloom-all "fpp=VALUE,ndv=VALUE" # Custom FPP and NDV
    ///
    /// Can be combined with --parquet-no-bloom-column to exclude specific columns.
    ///
    /// Examples:
    ///   --parquet-bloom-all                     # Use defaults
    ///   --parquet-bloom-all "fpp=0.001"         # Custom FPP
    ///   --parquet-bloom-all "ndv=10000"         # Custom NDV (10k distinct values)
    ///   --parquet-bloom-all --parquet-no-bloom-column user_id  # All except user_id
    #[arg(
        long,
        value_name = "[fpp=VALUE][,ndv=VALUE]",
        conflicts_with = "parquet_no_bloom_all",
        num_args = 0..=1,
        default_missing_value = "",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_bloom_all: Option<AllColumnsBloomFilterConfig>,

    /// Disable bloom filters for all columns (default is enabled for all columns).
    ///
    /// Can be combined with --parquet-bloom-column to enable for specific columns only.
    ///
    /// Examples:
    ///   --parquet-no-bloom-all --parquet-bloom-column user_id  # Only user_id has bloom filter
    #[arg(
        long,
        conflicts_with = "parquet_bloom_all",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_no_bloom_all: bool,

    /// Customize bloom filters for specific columns with optional custom settings.
    ///
    /// Overrides --parquet-no-bloom-all for the specified columns.
    /// Use with --parquet-no-bloom-all to enable only specific columns.
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
    ///   --parquet-bloom-column "user_id:ndv=50000"         # Custom NDV
    #[arg(
        long,
        value_name = "COLUMN[:fpp=VALUE][,ndv=VALUE]",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_bloom_column: Vec<ColumnSpecificBloomFilterConfig>,

    /// Disable bloom filter for specific columns (repeatable).
    ///
    /// Overrides --parquet-bloom-all for the specified columns.
    ///
    /// Examples:
    ///   --parquet-bloom-all --parquet-no-bloom-column user_id  # All columns except user_id
    ///   --parquet-no-bloom-column user_id --parquet-no-bloom-column session_id
    #[arg(
        long,
        value_name = "COLUMN",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub parquet_no_bloom_column: Vec<String>,

    /// Channel buffer size for batches sent to row group encoder tasks.
    ///
    /// Controls how many batches can be buffered between the coordinator and each
    /// row group encoder. Higher values reduce backpressure but use more memory.
    /// Defaults to 16.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_at_least_one)]
    pub parquet_batch_channel_size: Option<usize>,

    /// I/O buffer size for Parquet writing (e.g., "32MB", "64MB", "1GB").
    ///
    /// Controls the size of the buffer used when writing encoded data to disk.
    /// Supports suffixes: B, KB, MB, GB, TB (or KiB, MiB, GiB, TiB for binary).
    /// Default: 32MB.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_nonzero_byte_size)]
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

    /// Number of threads for CPU-bound parquet column encoding.
    ///
    /// Controls the rayon thread pool size for encoding columns within row groups.
    /// Column encoding is CPU-intensive and benefits from parallelism.
    /// Defaults to the number of CPU cores.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_at_least_one)]
    pub parquet_column_encoding_threads: Option<usize>,

    /// Channel buffer size for encoded row groups sent to the writer task.
    ///
    /// Controls how many encoded row groups can be buffered before writing to disk.
    /// Higher values allow more encoding to proceed while I/O is in progress.
    /// Defaults to 4.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_at_least_one)]
    pub parquet_encoded_channel_size: Option<usize>,

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

    /// Parquet encoder strategy.
    ///
    /// Controls whether to use the sequential (single-threaded) or parallel
    /// (multi-threaded) encoder. Parallel is faster for large files but has
    /// higher memory overhead.
    #[arg(
        long,
        value_enum,
        default_value_t,
        help_heading = "Parquet Tuning Options"
    )]
    pub parquet_encoder: ParquetEncoder,

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

    /// Number of threads for blocking parquet I/O operations.
    ///
    /// Controls the rayon thread pool size for file writes during parquet output.
    /// Typically needs fewer threads than encoding since I/O is less CPU-intensive.
    /// Defaults to 1.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_at_least_one)]
    pub parquet_io_threads: Option<usize>,

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

    /// Maximum number of row groups that can be encoding concurrently.
    ///
    /// Controls how many row groups can be actively encoding at once. Higher values
    /// increase parallelism but use more memory. Each row group encodes its columns
    /// in parallel using --parquet-column-encoding-threads.
    /// Defaults to 4.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_at_least_one)]
    pub parquet_row_group_concurrency: Option<usize>,

    /// Maximum number of rows per Parquet row group.
    #[arg(long, help_heading = "Parquet Options", value_parser = parse_at_least_one)]
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

    /// Maximum data page size in bytes.
    ///
    /// Controls the maximum size of each data page within a column chunk.
    /// Larger pages reduce overhead but increase granularity of reads.
    /// Default: 100MB (DuckDB MAX_UNCOMPRESSED_PAGE_SIZE).
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_nonzero_byte_size)]
    pub parquet_data_page_size: Option<usize>,

    /// Maximum rows per data page.
    ///
    /// Controls the maximum number of rows in each data page within a column chunk.
    /// Default: unlimited (one page per row group for optimal DuckDB compatibility).
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_at_least_one)]
    pub parquet_data_page_row_limit: Option<usize>,

    /// Maximum dictionary page size in bytes.
    ///
    /// Controls the maximum size of dictionary pages. When a dictionary exceeds this
    /// size, the writer falls back to the data page encoding for remaining values.
    /// Default: 1GB (DuckDB MAX_UNCOMPRESSED_DICT_PAGE_SIZE).
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_nonzero_byte_size)]
    pub parquet_dictionary_page_size: Option<usize>,

    /// Internal write batch size.
    ///
    /// Controls how many rows are processed at once when writing data pages.
    /// Larger values improve throughput but use more memory.
    /// Default: 8192.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_at_least_one)]
    pub parquet_write_batch_size: Option<usize>,

    /// Enable offset index writing.
    ///
    /// Offset indexes store the position of each data page within column chunks,
    /// enabling faster page-level seeks. Only useful when there are multiple data
    /// pages per column chunk.
    /// Default: disabled (not needed with one page per row group).
    #[arg(long, help_heading = "Parquet Options")]
    pub parquet_offset_index: bool,

    /// Embed Arrow schema in file metadata.
    ///
    /// Stores the original Arrow schema in the Parquet file's key-value metadata.
    /// This enables exact schema round-tripping but adds overhead.
    /// Default: disabled (not needed for most use cases).
    #[arg(long, help_heading = "Parquet Options")]
    pub parquet_arrow_metadata: bool,

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
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    pub format: OutputFormat,
    /// Row group to display details for (default: 0)
    #[arg(long, short = 'g', default_value = "0")]
    pub row_group: usize,
    /// Show page details for columns (comma-separated, or omit value for all columns)
    #[arg(long, short = 'p', num_args = 0..=1, default_missing_value = "")]
    pub pages: Option<String>,
}

#[derive(Args, Debug)]
pub struct InspectArrowArgs {
    /// Path to the Arrow IPC file
    #[arg(value_hint = ValueHint::FilePath)]
    pub file: Utf8PathBuf,
    /// Show per-record-batch details
    #[arg(long)]
    pub batches: bool,
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    pub format: OutputFormat,
    /// Count total rows (requires reading entire file)
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

    mod bloom_filter_config_builder_tests {
        use super::*;

        #[test]
        fn test_builder_default() {
            let config = BloomFilterConfig::builder().build().unwrap();
            assert!(config.all_enabled().is_none());
            assert!(config.column_enabled().is_empty());
            assert!(config.column_disabled().is_empty());
        }

        #[test]
        fn test_builder_all_enabled() {
            let config = BloomFilterConfig::builder()
                .all_enabled(AllColumnsBloomFilterConfig {
                    fpp: 0.01,
                    ndv: None,
                })
                .build()
                .unwrap();
            let all_enabled = config.all_enabled().expect("expected all-enabled config");
            assert_eq!(all_enabled.fpp, 0.01);
        }

        #[test]
        fn test_builder_enable_column() {
            let config = BloomFilterConfig::builder()
                .enable_column(ColumnSpecificBloomFilterConfig {
                    name: "user_id".to_string(),
                    config: ColumnBloomFilterConfig {
                        fpp: 0.05,
                        ndv: Some(1000),
                    },
                })
                .build()
                .unwrap();
            assert_eq!(config.column_enabled().len(), 1);
            assert_eq!(config.column_enabled()[0].name, "user_id");
        }

        #[test]
        fn test_builder_disable_column() {
            let config = BloomFilterConfig::builder()
                .disable_column("status")
                .build()
                .unwrap();
            assert_eq!(config.column_disabled().len(), 1);
            assert_eq!(config.column_disabled()[0], "status");
        }

        #[test]
        fn test_builder_rejects_conflict() {
            let result = BloomFilterConfig::builder()
                .enable_column(ColumnSpecificBloomFilterConfig {
                    name: "user_id".to_string(),
                    config: ColumnBloomFilterConfig {
                        fpp: 0.05,
                        ndv: None,
                    },
                })
                .disable_column("user_id")
                .build();
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("column 'user_id' specified as both enabled and disabled")
            );
        }

        #[test]
        fn test_builder_combined() {
            let config = BloomFilterConfig::builder()
                .all_enabled(AllColumnsBloomFilterConfig {
                    fpp: 0.01,
                    ndv: None,
                })
                .enable_column(ColumnSpecificBloomFilterConfig {
                    name: "user_id".to_string(),
                    config: ColumnBloomFilterConfig {
                        fpp: 0.001,
                        ndv: Some(100_000),
                    },
                })
                .disable_column("status")
                .build()
                .unwrap();

            assert!(config.all_enabled().is_some());
            assert_eq!(config.column_enabled().len(), 1);
            assert_eq!(config.column_disabled().len(), 1);
        }
    }

    mod cli_validation_tests {
        use super::*;
        use clap::Parser;

        #[test]
        fn test_preserve_input_order_conflicts_with_query() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.parquet",
                "--to",
                "output.parquet",
                "--preserve-input-order",
                "--query",
                "SELECT * FROM data",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("query"));
        }

        #[test]
        fn test_preserve_input_order_conflicts_with_sort_by() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.parquet",
                "--to",
                "output.parquet",
                "--preserve-input-order",
                "--sort-by",
                "id",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("sort-by"));
        }

        #[test]
        fn test_preserve_input_order_conflicts_with_to_many() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.parquet",
                "--to-many",
                "output_{id}.parquet",
                "--preserve-input-order",
                "--by",
                "id",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("to-many"));
        }

        #[test]
        fn test_preserve_input_order_conflicts_with_from_many() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from-many",
                "*.parquet",
                "--to",
                "output.parquet",
                "--preserve-input-order",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("from-many"));
        }
    }
}
