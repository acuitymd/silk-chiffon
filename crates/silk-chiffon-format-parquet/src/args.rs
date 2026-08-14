//! Typed command settings and validated Parquet policies.
//!
//! These types stay private because the registered format definition is the
//! contract with the host. Keeping parsing beside the codec prevents the
//! executable from acquiring a second Parquet configuration model.

use std::{
    fmt::{self, Formatter},
    str::FromStr,
};

use anyhow::{Result, anyhow};
use clap::{Args, ValueEnum};
use parquet::{
    basic::{
        BrotliLevel, Compression as CompressionCodec, Encoding as EncodingCodec, GzipLevel,
        ZstdLevel,
    },
    file::properties::{EnabledStatistics, WriterVersion as FileWriterVersion},
};

fn parse_positive(value: &str) -> Result<usize> {
    let value: usize = value.parse()?;
    if value == 0 {
        anyhow::bail!("value must be at least 1");
    }
    Ok(value)
}

#[allow(clippy::cast_possible_truncation)]
fn parse_byte_size(value: &str) -> Result<usize> {
    let bytes = value
        .parse::<bytesize::ByteSize>()
        .map_err(|_| {
            anyhow!("invalid byte size '{value}': expected format like '512MB', '2GB', or '1GiB'")
        })?
        .as_u64() as usize;
    if bytes == 0 {
        anyhow::bail!("value must be greater than 0");
    }
    Ok(bytes)
}

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
#[value(rename_all = "lowercase")]
pub(crate) enum Compression {
    Zstd,
    Snappy,
    Gzip,
    Brotli,
    Lz4,
    #[default]
    None,
}

impl Compression {
    pub fn to_compression(self, level: Option<i32>) -> Result<CompressionCodec> {
        match self {
            Self::Zstd => {
                let lvl = match level {
                    Some(l) => ZstdLevel::try_new(l)?,
                    None => ZstdLevel::default(),
                };
                Ok(CompressionCodec::ZSTD(lvl))
            }
            Self::Gzip => {
                let lvl = match level {
                    Some(l) => GzipLevel::try_new(
                        u32::try_from(l)
                            .map_err(|_| anyhow!("gzip compression level must be non-negative"))?,
                    )?,
                    None => GzipLevel::default(),
                };
                Ok(CompressionCodec::GZIP(lvl))
            }
            Self::Brotli => {
                let lvl =
                    match level {
                        Some(l) => BrotliLevel::try_new(u32::try_from(l).map_err(|_| {
                            anyhow!("brotli compression level must be non-negative")
                        })?)?,
                        None => BrotliLevel::default(),
                    };
                Ok(CompressionCodec::BROTLI(lvl))
            }
            Self::Snappy | Self::Lz4 | Self::None => {
                if level.is_some() {
                    anyhow::bail!(
                        "compression level is not supported for {}",
                        match self {
                            Self::Snappy => "snappy",
                            Self::Lz4 => "lz4",
                            Self::None => "uncompressed",
                            _ => unreachable!(),
                        }
                    );
                }
                Ok(match self {
                    Self::Snappy => CompressionCodec::SNAPPY,
                    Self::Lz4 => CompressionCodec::LZ4_RAW,
                    Self::None => CompressionCodec::UNCOMPRESSED,
                    _ => unreachable!(),
                })
            }
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
#[value(rename_all = "lowercase")]
pub(crate) enum Statistics {
    None,
    #[default]
    Chunk,
    Page,
}

impl From<Statistics> for EnabledStatistics {
    fn from(statistics: Statistics) -> Self {
        match statistics {
            Statistics::None => EnabledStatistics::None,
            Statistics::Chunk => EnabledStatistics::Chunk,
            Statistics::Page => EnabledStatistics::Page,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[value(rename_all = "lowercase")]
pub(crate) enum WriterVersion {
    V1,
    #[default]
    V2,
}

impl From<WriterVersion> for FileWriterVersion {
    fn from(writer_version: WriterVersion) -> Self {
        match writer_version {
            WriterVersion::V1 => FileWriterVersion::PARQUET_1_0,
            WriterVersion::V2 => FileWriterVersion::PARQUET_2_0,
        }
    }
}

impl From<WriterVersion> for i32 {
    fn from(writer_version: WriterVersion) -> Self {
        match writer_version {
            WriterVersion::V1 => 1,
            WriterVersion::V2 => 2,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Encoding {
    #[default]
    Plain,
    Rle,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    ByteStreamSplit,
}

impl Encoding {
    /// Validates that this encoding is compatible with the given Arrow data type.
    /// Returns an error message if incompatible, None if compatible.
    pub fn validate_for_type(&self, data_type: &arrow::datatypes::DataType) -> Option<String> {
        use arrow::datatypes::DataType;

        match self {
            Encoding::Plain => None, // works for all types

            Encoding::Rle => {
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

            Encoding::DeltaBinaryPacked => {
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

            Encoding::DeltaLengthByteArray | Encoding::DeltaByteArray => {
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

            Encoding::ByteStreamSplit => {
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

impl From<Encoding> for EncodingCodec {
    fn from(encoding: Encoding) -> Self {
        match encoding {
            Encoding::Plain => EncodingCodec::PLAIN,
            Encoding::Rle => EncodingCodec::RLE,
            Encoding::DeltaBinaryPacked => EncodingCodec::DELTA_BINARY_PACKED,
            Encoding::DeltaLengthByteArray => EncodingCodec::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DeltaByteArray => EncodingCodec::DELTA_BYTE_ARRAY,
            Encoding::ByteStreamSplit => EncodingCodec::BYTE_STREAM_SPLIT,
        }
    }
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            Encoding::Plain => "plain",
            Encoding::Rle => "rle",
            Encoding::DeltaBinaryPacked => "delta-binary-packed",
            Encoding::DeltaLengthByteArray => "delta-length-byte-array",
            Encoding::DeltaByteArray => "delta-byte-array",
            Encoding::ByteStreamSplit => "byte-stream-split",
        };
        write!(f, "{s}")
    }
}

/// Per-column encoding configuration, parsed from "column=encoding" format.
#[derive(Debug, Clone)]
pub(crate) struct ColumnEncoding {
    pub name: String,
    pub encoding: Encoding,
}

impl FromStr for ColumnEncoding {
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
        let encoding = Encoding::from_str(encoding_str, true).map_err(|_| {
            anyhow!(
                "Invalid encoding '{}'. Valid options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array, byte-stream-split",
                encoding_str
            )
        })?;

        Ok(ColumnEncoding {
            name: name.to_string(),
            encoding,
        })
    }
}

/// Dictionary mode for per-column dictionary configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DictionaryMode {
    /// Always attempt dictionary encoding; parquet-rs handles overflow at write time.
    Always,
    /// Analyze cardinality per row group and decide whether to use dictionary.
    /// Falls back to Always for non-analyzable types (nested types, floats).
    #[default]
    Analyze,
}

/// Per-column dictionary configuration, parsed from "column:mode" format.
///
/// Modes:
/// - `col:always` - always attempt dictionary encoding
/// - `col:analyze` - use cardinality analysis to decide
#[derive(Debug, Clone)]
pub(crate) struct DictionaryPolicy {
    pub name: String,
    pub mode: DictionaryMode,
}

impl FromStr for DictionaryPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(anyhow!("Column name cannot be empty"));
        }

        let (name, mode) = if let Some((name, mode_str)) = s.split_once(':') {
            let name = name.trim();
            if name.is_empty() {
                return Err(anyhow!("Column name cannot be empty in '{}'", s));
            }
            let mode = match mode_str.trim().to_lowercase().as_str() {
                "always" => DictionaryMode::Always,
                "analyze" => DictionaryMode::Analyze,
                other => {
                    return Err(anyhow!(
                        "Invalid dictionary mode '{}'. Valid options: always, analyze",
                        other
                    ));
                }
            };
            (name.to_string(), mode)
        } else {
            return Err(anyhow!(
                "Missing dictionary mode for '{}'. Format: COLUMN:MODE where MODE is 'always' or 'analyze'",
                s
            ));
        };

        Ok(DictionaryPolicy { name, mode })
    }
}

pub(crate) const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.01;

/// Parsed bloom filter parameters (fpp and optional ndv).
#[derive(Debug)]
struct BloomFilterParams {
    fpp: Option<f64>,
    ndv: Option<u64>,
}

fn parse_bloom_filter_params(s: &str) -> Result<BloomFilterParams> {
    let mut fpp = None;
    let mut ndv = None;

    let parts = s
        .split(',')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect::<Vec<&str>>();

    for part in parts {
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

                    let parsed = value
                        .parse::<f64>()
                        .map_err(|e| anyhow::anyhow!("Invalid fpp value '{}': {}", value, e))?;

                    if !parsed.is_finite() || parsed <= 0.0 || parsed >= 1.0 {
                        return Err(anyhow::anyhow!(
                            "Invalid fpp value '{}': must be in range (0.0, 1.0)",
                            value
                        ));
                    }

                    fpp = Some(parsed);
                }
                "ndv" => {
                    if ndv.is_some() {
                        return Err(anyhow!(
                            "Invalid bloom filter specification, ndv is set twice: {}",
                            s
                        ));
                    }

                    let parsed = value
                        .parse::<u64>()
                        .map_err(|e| anyhow::anyhow!("Invalid ndv value '{}': {}", value, e))?;

                    if parsed == 0 {
                        return Err(anyhow::anyhow!(
                            "Invalid ndv value '{}': must be greater than 0",
                            value
                        ));
                    }

                    ndv = Some(parsed);
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

    Ok(BloomFilterParams { fpp, ndv })
}

#[derive(Debug, Clone)]
pub(crate) struct DefaultBloomFilterPolicy {
    pub fpp: f64,
    pub ndv: Option<u64>,
}

impl FromStr for DefaultBloomFilterPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Ok(Self {
                fpp: DEFAULT_BLOOM_FILTER_FPP,
                ndv: None,
            });
        }

        let params = parse_bloom_filter_params(s)?;
        Ok(Self {
            fpp: params.fpp.unwrap_or(DEFAULT_BLOOM_FILTER_FPP),
            ndv: params.ndv,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BloomFilterSettings {
    pub fpp: f64,
    pub ndv: Option<u64>,
}

impl FromStr for BloomFilterSettings {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let params = parse_bloom_filter_params(s)?;
        Ok(Self {
            fpp: params.fpp.unwrap_or(DEFAULT_BLOOM_FILTER_FPP),
            ndv: params.ndv,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnBloomFilterPolicy {
    pub name: String,
    pub config: BloomFilterSettings,
}

impl FromStr for ColumnBloomFilterPolicy {
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

            Ok(ColumnBloomFilterPolicy {
                name: column_name.to_string(),
                config: BloomFilterSettings::from_str(rest)?,
            })
        } else {
            let column_name = s.trim();
            if column_name.is_empty() {
                return Err(anyhow!(
                    "Invalid bloom filter specification: empty column name"
                ));
            }
            Ok(ColumnBloomFilterPolicy {
                name: column_name.to_string(),
                config: BloomFilterSettings {
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
pub(crate) struct BloomFilterPolicy {
    /// Global setting: Some(config) = enabled for all, None = disabled for all
    all_enabled: Option<DefaultBloomFilterPolicy>,
    /// Columns explicitly enabled with config (overrides all_enabled)
    column_enabled: Vec<ColumnBloomFilterPolicy>,
    /// Columns explicitly disabled (overrides all_enabled, but not column_enabled)
    column_disabled: Vec<String>,
}

impl BloomFilterPolicy {
    pub fn try_new(
        all_enabled: Option<DefaultBloomFilterPolicy>,
        column_enabled: Vec<ColumnBloomFilterPolicy>,
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

    pub fn all_enabled(&self) -> Option<&DefaultBloomFilterPolicy> {
        self.all_enabled.as_ref()
    }

    pub fn column_enabled(&self) -> &[ColumnBloomFilterPolicy] {
        &self.column_enabled
    }

    pub fn column_disabled(&self) -> &[String] {
        &self.column_disabled
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
}
#[derive(Args, Clone, Debug)]
#[group(id = "parquet_transform")]
pub(crate) struct TransformArgs {
    /// Enable bloom filters for columns (default behavior).
    ///
    /// DICTIONARY/BLOOM INTERACTION:
    /// Bloom filters are coupled to dictionary encoding decisions:
    ///   - Columns that KEEP dictionary encoding -> bloom filter enabled (using analyzed NDV)
    ///   - Columns where dictionary is DISABLED (high cardinality) -> bloom filter disabled
    ///   - This coupling exists because both features degrade for high-cardinality data
    ///
    /// To force bloom filters ON regardless of dictionary decisions, specify explicit NDV:
    ///
    ///     --parquet-bloom-column "high_card_col:ndv=100000"
    ///
    /// NESTED TYPES (structs, lists, maps):
    /// Bloom filters apply to leaf columns within nested structures. The column path uses
    /// dot notation (e.g., "struct_col.field" or "list_col.element").
    ///
    /// Formats:
    ///
    ///     --parquet-bloom-all                       # Use defaults (fpp=0.01, auto NDV)
    ///     --parquet-bloom-all "fpp=VALUE"           # Custom false positive probability
    ///     --parquet-bloom-all "ndv=VALUE"           # Force bloom on with explicit NDV
    ///     --parquet-bloom-all "fpp=VALUE,ndv=VALUE" # Both custom
    ///
    /// CONFLICTS: Cannot be used with --parquet-bloom-all-off.
    ///
    /// Examples:
    ///
    ///     --parquet-bloom-all  # Bloom for low-cardinality columns
    ///     --parquet-bloom-all "fpp=0.001"  # Tighter false positive rate
    ///     --parquet-bloom-all "ndv=10000"  # Force bloom on all columns
    ///     --parquet-bloom-all --parquet-bloom-column-off user_id  # Exclude user_id
    #[arg(
        long,
        value_name = "[fpp=VALUE][,ndv=VALUE]",
        conflicts_with = "parquet_bloom_all_off",
        num_args = 0..=1,
        default_missing_value = "",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_bloom_all: Option<DefaultBloomFilterPolicy>,

    /// Disable bloom filters for all columns.
    ///
    /// Use with --parquet-bloom-column to enable bloom filters for specific columns only.
    ///
    /// CONFLICTS: Cannot be used with --parquet-bloom-all.
    ///
    /// Examples:
    ///
    ///     --parquet-bloom-all-off                                 # No bloom filters
    ///     --parquet-bloom-all-off --parquet-bloom-column user_id  # Only user_id
    #[arg(
        long = "parquet-bloom-all-off",
        conflicts_with = "parquet_bloom_all",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_bloom_all_off: bool,

    /// Enable or customize bloom filters for specific columns.
    ///
    /// Overrides --parquet-bloom-all-off for the specified columns.
    ///
    /// Without explicit NDV, the bloom filter depends on the dictionary decision.
    /// See `--parquet-bloom-all`.
    /// WITH explicit NDV: bloom filter is FORCED ON regardless of dictionary encoding.
    ///
    /// Use explicit NDV to enable bloom filters on high-cardinality columns that won't
    /// use dictionary encoding (e.g., UUIDs, timestamps).
    ///
    /// NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This matches
    /// all leaf columns under that path, so you don't need to know Parquet internal naming.
    ///
    /// Formats:
    ///
    ///     COLUMN                     # Depends on dictionary decision
    ///     COLUMN:fpp=VALUE           # Custom false positive probability
    ///     COLUMN:ndv=VALUE           # Force bloom ON with explicit NDV
    ///     COLUMN:fpp=VALUE,ndv=VALUE # Both custom
    ///
    /// CONFLICTS: Cannot specify same column in both --parquet-bloom-column and
    /// --parquet-bloom-column-off.
    ///
    /// Examples:
    ///
    ///     --parquet-bloom-column "region"                    # If region keeps dictionary
    ///     --parquet-bloom-column "user.address"              # All leaves under user.address
    ///     --parquet-bloom-column "user_id:ndv=1000000"
    ///     # Force bloom on a high-cardinality column
    ///     --parquet-bloom-column "user_id:fpp=0.001"         # Tighter FPP
    #[arg(
        long,
        value_name = "COLUMN[:fpp=VALUE][,ndv=VALUE]",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_bloom_column: Vec<ColumnBloomFilterPolicy>,

    /// Disable bloom filter for specific columns (repeatable).
    ///
    /// Overrides --parquet-bloom-all for the specified columns.
    ///
    /// NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This disables
    /// bloom filters for all leaf columns under that path.
    ///
    /// CONFLICTS: Cannot specify same column in both --parquet-bloom-column and
    /// --parquet-bloom-column-off.
    ///
    /// Examples:
    ///
    ///     --parquet-bloom-all --parquet-bloom-column-off user_id  # All except user_id
    ///     --parquet-bloom-column-off "user.address"  # Disable all user.address leaves
    ///     --parquet-bloom-column-off col1 --parquet-bloom-column-off col2  # Disable multiple
    #[arg(
        long = "parquet-bloom-column-off",
        value_name = "COLUMN",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_bloom_column_off: Vec<String>,

    /// Encoding buffer size for Parquet writing (e.g., "32MB", "64MB", "1GB").
    ///
    /// Controls the bytes buffered between Parquet encoding and the object upload.
    /// Supports suffixes: B, KB, MB, GB, TB (or KiB, MiB, GiB, TiB for binary).
    /// Default: 32MB.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_byte_size)]
    pub(crate) parquet_buffer_size: Option<usize>,

    /// Disable dictionary encoding globally for all Parquet columns.
    ///
    /// Dictionary encoding builds a dictionary of unique values and stores references to it,
    /// which is effective for low-cardinality columns (few unique values). When disabled,
    /// columns use their data page encoding directly (see --parquet-encoding).
    ///
    /// DEFAULT BEHAVIOR (without this flag):
    ///   - Most primitive columns use "analyze" mode. Cardinality analysis
    ///     decides per row group.
    ///     whether to use dictionary (disabled if >20% distinct values)
    ///   - Floats use "always" mode: high cardinality makes analysis unhelpful
    ///   - Nested columns (structs, lists, maps) use "always" mode: dictionary encoding is
    ///     always attempted (parquet-rs handles overflow gracefully)
    ///
    /// BLOOM FILTER IMPACT: Disabling dictionary also disables bloom filters for affected
    /// columns (unless explicit NDV is provided via --parquet-bloom-column).
    ///
    /// Use --parquet-dictionary-column to re-enable for specific columns.
    #[arg(
        long = "parquet-dictionary-all-off",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_dictionary_all_off: bool,

    /// Enable dictionary encoding for specific columns. Can be specified multiple times.
    ///
    /// Overrides --parquet-dictionary-all-off for the named columns.
    ///
    /// Format: COLUMN:MODE where MODE is:
    ///   - analyze: Cardinality analysis decides per-row-group (disabled if >20% distinct)
    ///   - always: Always attempt dictionary; parquet-rs handles overflow gracefully
    ///
    /// NON-ANALYZABLE TYPES:
    /// Cardinality analysis only works on certain types. Non-analyzable types (nested types
    /// like structs/lists/maps, and floats due to high cardinality) automatically use "always"
    /// mode even if you specify "analyze". Use dot notation for nested paths
    /// (e.g., "user.address").
    /// This enables dictionary for all leaf columns under that path.
    ///
    /// BLOOM FILTER INTERACTION:
    /// The cardinality analysis from "analyze" mode also provides NDV for bloom filter sizing.
    /// When dictionary is disabled (high cardinality), bloom filters are also disabled unless
    /// you provide explicit NDV via --parquet-bloom-column.
    ///
    /// CONFLICTS: Cannot specify same column in both --parquet-dictionary-column and
    /// --parquet-dictionary-column-off.
    ///
    /// Examples:
    ///
    ///     --parquet-dictionary-column region:analyze       # Let analysis decide
    ///     --parquet-dictionary-column region:always        # Force dictionary on
    ///     --parquet-dictionary-column "user.address:always"   # All user.address leaves
    #[arg(
        long = "parquet-dictionary-column",
        value_name = "COLUMN:MODE",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_dictionary_column: Vec<DictionaryPolicy>,

    /// Set data page encoding for specific columns. Can be specified multiple times.
    ///
    /// Overrides --parquet-encoding and automatic encoding selection for the named column.
    ///
    /// DICTIONARY INTERACTION:
    ///   - Dictionary ENABLED: this encoding is used when dictionary overflows
    ///   - Dictionary DISABLED: this encoding is used for all data
    ///
    /// NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This sets
    /// encoding for all leaf columns under that path.
    ///
    /// Format: COLUMN=ENCODING
    ///
    /// Options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array,
    /// byte-stream-split
    ///
    /// Examples:
    ///
    ///     --parquet-column-encoding id=delta-binary-packed
    ///     # Efficient for sorted integers
    ///     --parquet-column-encoding name=delta-byte-array       # Efficient for strings
    ///     --parquet-column-encoding price=byte-stream-split     # Efficient for floats
    #[arg(
        long,
        value_name = "COLUMN=ENCODING",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_column_encoding: Vec<ColumnEncoding>,

    /// Number of threads for CPU-bound parquet column encoding.
    ///
    /// Controls the rayon thread pool size for encoding columns within row groups.
    /// Column encoding is CPU-intensive and benefits from parallelism.
    ///
    /// Default: auto-detected based on workload. With sorting (--sort-by or --by): 25%
    /// of available CPU cores. Without sorting: 75% of available CPU cores.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_positive)]
    pub(crate) parquet_column_encoding_threads: Option<usize>,

    /// Queue size for record batches waiting to be assembled into row groups.
    ///
    /// Controls backpressure between the data source and ingestion stage. Higher
    /// values allow the source to stay ahead of row group assembly.
    #[arg(long, help_heading = "Parquet Tuning Options", default_value = "1", value_parser = parse_positive)]
    pub(crate) parquet_ingestion_queue_size: usize,

    /// Queue size for row groups waiting to be encoded.
    ///
    /// Controls backpressure between ingestion and encoding stages. Higher values
    /// allow more row groups to be assembled while encoders are busy.
    #[arg(long, help_heading = "Parquet Tuning Options", default_value = "4", value_parser = parse_positive)]
    pub(crate) parquet_encoding_queue_size: usize,

    /// Queue size for encoded row groups waiting to be serialized to the output.
    ///
    /// Controls backpressure between encoding and I/O stages. Higher values allow
    /// more encoding to proceed while I/O is in progress.
    #[arg(long, help_heading = "Parquet Tuning Options", default_value = "4", value_parser = parse_positive)]
    pub(crate) parquet_writing_queue_size: usize,

    /// Disable dictionary encoding for specific columns. Can be specified multiple times.
    ///
    /// Overrides the default (dictionary enabled with analysis) for the named columns.
    ///
    /// BLOOM FILTER IMPACT: Disabling dictionary also disables bloom filters for the column
    /// (unless you provide explicit NDV via --parquet-bloom-column).
    ///
    /// NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This disables
    /// dictionary for all leaf columns under that path.
    ///
    /// CONFLICTS: Cannot specify same column in both --parquet-dictionary-column and
    /// --parquet-dictionary-column-off.
    ///
    /// Useful for high-cardinality columns like UUIDs or timestamps where dictionary
    /// encoding adds overhead without compression benefit.
    #[arg(
        long = "parquet-dictionary-column-off",
        value_name = "COLUMN",
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_dictionary_column_off: Vec<String>,

    /// Parquet compression codec.
    #[arg(long, value_enum, default_value_t = Compression::default(), help_heading = "Parquet Options")]
    pub(crate) parquet_compression: Compression,

    /// Compression level for the chosen codec.
    ///
    /// Only applies to gzip (0-9, default 6), brotli (0-11, default 1),
    /// and zstd (1-22, default 1). Not supported for snappy, lz4, or none.
    #[arg(long, help_heading = "Parquet Options")]
    pub(crate) parquet_compression_level: Option<i32>,

    /// Data page encoding for Parquet columns.
    ///
    /// This encoding is used for column data pages. Its role depends on dictionary encoding:
    ///   - Dictionary ENABLED: this is the fallback encoding when dictionary overflows
    ///   - Dictionary DISABLED: this is the primary encoding for all data
    ///
    /// AUTOMATIC ENCODING (when not specified):
    /// The writer automatically selects encodings based on writer version:
    ///
    ///   V1 (--parquet-writer-version=v1):
    ///     All columns use PLAIN encoding for maximum compatibility with older readers.
    ///
    ///   V2 (default):
    ///     Optimized encodings are selected based on column type:
    ///       - Integers -> delta-binary-packed (good for sorted/sequential data)
    ///       - Floats -> byte-stream-split (better compression for floats)
    ///       - Strings/Binary -> delta-length-byte-array
    ///       - Booleans -> plain
    ///
    /// Use this flag to override automatic selection globally, or --parquet-column-encoding
    /// for specific columns.
    ///
    /// Options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array,
    /// byte-stream-split
    #[arg(
        long,
        value_enum,
        verbatim_doc_comment,
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_encoding: Option<Encoding>,

    /// Number of threads for blocking Parquet writer operations.
    ///
    /// Controls the runtime used to serialize Parquet row groups into the object upload.
    /// Typically needs fewer threads than column encoding.
    /// Defaults to 1.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_positive)]
    pub(crate) parquet_writing_threads: Option<usize>,

    /// Maximum number of row groups that can be encoding concurrently.
    ///
    /// Controls how many row groups can be actively encoding at once. Higher values
    /// increase parallelism but use more memory. Each row group encodes its columns
    /// in parallel using --parquet-column-encoding-threads.
    /// Defaults to 4.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_positive)]
    pub(crate) parquet_row_group_concurrency: Option<usize>,

    /// Maximum number of rows per Parquet row group.
    #[arg(long, help_heading = "Parquet Options", value_parser = parse_positive)]
    pub(crate) parquet_row_group_size: Option<usize>,

    /// Embed metadata indicating that the file's data is sorted.
    ///
    /// Requires --sort-by to be set.
    #[arg(
        long,
        default_value_t = false,
        requires = "sort_by",
        help_heading = "Parquet Options"
    )]
    pub(crate) parquet_sorted_metadata: bool,

    /// Parquet column statistics level.
    #[arg(long, value_enum, default_value_t = Statistics::default(), help_heading = "Parquet Options")]
    pub(crate) parquet_statistics: Statistics,

    /// Parquet writer version.
    #[arg(long, value_enum, default_value_t = WriterVersion::default(), help_heading = "Parquet Options")]
    pub(crate) parquet_writer_version: WriterVersion,

    /// Maximum data page size in bytes.
    ///
    /// Controls the maximum size of each data page within a column chunk.
    /// Larger pages reduce overhead but increase granularity of reads.
    /// Default: 100MB (DuckDB MAX_UNCOMPRESSED_PAGE_SIZE).
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_byte_size)]
    pub(crate) parquet_data_page_size: Option<usize>,

    /// Maximum rows per data page.
    ///
    /// Controls the maximum number of rows in each data page within a column chunk.
    /// Default: unlimited (one page per row group for optimal DuckDB compatibility).
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_positive)]
    pub(crate) parquet_data_page_row_limit: Option<usize>,

    /// Maximum dictionary page size in bytes.
    ///
    /// Controls the maximum size of dictionary pages. When a dictionary exceeds this
    /// size, the writer falls back to the data page encoding for remaining values.
    /// Default: 1GB (DuckDB MAX_UNCOMPRESSED_DICT_PAGE_SIZE).
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_byte_size)]
    pub(crate) parquet_dictionary_page_size: Option<usize>,

    /// Internal write batch size.
    ///
    /// Controls how many rows are processed at once when writing data pages.
    /// Larger values improve throughput but use more memory.
    /// Default: 8192.
    #[arg(long, help_heading = "Parquet Tuning Options", value_parser = parse_positive)]
    pub(crate) parquet_write_batch_size: Option<usize>,

    /// Enable offset index writing.
    ///
    /// Offset indexes store the position of each data page within column chunks,
    /// enabling faster page-level seeks. Only useful when there are multiple data
    /// pages per column chunk.
    /// Default: disabled (not needed with one page per row group).
    #[arg(long, help_heading = "Parquet Options")]
    pub(crate) parquet_offset_index: bool,

    /// Write statistics to page headers.
    ///
    /// Embeds min/max statistics in each data page header. This is redundant with
    /// column index statistics and can increase file size. Plus it's generally
    /// not used by query engines.
    ///
    /// Only use if you know what you're doing.
    /// Default: disabled.
    #[arg(long, help_heading = "Parquet Options")]
    pub(crate) parquet_page_header_statistics: bool,

    /// Embed Arrow schema in file metadata.
    ///
    /// Stores the original Arrow schema in the Parquet file's key-value metadata.
    /// This enables exact schema round-tripping but adds overhead.
    /// Default: disabled (not needed for most use cases).
    #[arg(long, help_heading = "Parquet Options")]
    pub(crate) parquet_arrow_metadata: bool,
}

#[derive(Args, Clone, Debug)]
#[group(id = "parquet_inspection")]
pub(crate) struct InspectionArgs {
    /// Row group to display details for (default: 0)
    #[arg(long, short = 'g', default_value = "0")]
    pub(crate) row_group: usize,
    /// Show page details for columns (comma-separated, or omit value for all columns)
    #[arg(long, short = 'p', num_args = 0..=1, default_missing_value = "")]
    pub(crate) pages: Option<String>,
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn byte_sizes_and_positive_counts_reject_zero_and_malformed_values() {
        assert_eq!(parse_positive("7").unwrap(), 7);
        assert!(
            parse_positive("0")
                .unwrap_err()
                .to_string()
                .contains("at least 1")
        );
        assert_eq!(parse_byte_size("2KiB").unwrap(), 2048);
        for value in ["0B", "nope", "-1MB"] {
            assert!(parse_byte_size(value).is_err(), "{value} should fail");
        }
    }

    #[test]
    fn compression_levels_are_validated_per_codec() {
        assert_eq!(
            Compression::Zstd.to_compression(Some(3)).unwrap(),
            CompressionCodec::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
        assert!(Compression::Gzip.to_compression(Some(-1)).is_err());
        assert!(Compression::Brotli.to_compression(Some(-1)).is_err());
        for compression in [Compression::Snappy, Compression::Lz4, Compression::None] {
            assert!(compression.to_compression(Some(1)).is_err());
            assert!(compression.to_compression(None).is_ok());
        }
    }

    #[test]
    fn column_encoding_requires_a_named_column_and_known_encoding() {
        let parsed = "value=delta-byte-array".parse::<ColumnEncoding>().unwrap();
        assert_eq!(parsed.name, "value");
        assert_eq!(parsed.encoding, Encoding::DeltaByteArray);
        for value in ["value", "=plain", "value=unknown"] {
            assert!(
                value.parse::<ColumnEncoding>().is_err(),
                "{value} should fail"
            );
        }
    }

    #[test]
    fn encoding_compatibility_covers_variable_and_fixed_width_types() {
        assert!(
            Encoding::Plain
                .validate_for_type(&DataType::List(std::sync::Arc::new(
                    arrow::datatypes::Field::new("item", DataType::Utf8, true,)
                ),))
                .is_none()
        );
        assert!(
            Encoding::Rle
                .validate_for_type(&DataType::Boolean)
                .is_none()
        );
        assert!(Encoding::Rle.validate_for_type(&DataType::Int32).is_some());
        assert!(
            Encoding::DeltaBinaryPacked
                .validate_for_type(&DataType::Int32)
                .is_none()
        );
        assert!(
            Encoding::DeltaBinaryPacked
                .validate_for_type(&DataType::Utf8)
                .is_some()
        );
        assert!(
            Encoding::DeltaLengthByteArray
                .validate_for_type(&DataType::Utf8)
                .is_none()
        );
        assert!(
            Encoding::DeltaLengthByteArray
                .validate_for_type(&DataType::Float64)
                .is_some()
        );
        assert!(
            Encoding::ByteStreamSplit
                .validate_for_type(&DataType::Float64)
                .is_none()
        );
        assert!(
            Encoding::ByteStreamSplit
                .validate_for_type(&DataType::Utf8)
                .is_some()
        );
    }

    #[test]
    fn dictionary_policy_requires_an_explicit_supported_mode() {
        let always = " value : ALWAYS ".parse::<DictionaryPolicy>().unwrap();
        assert_eq!(always.name, "value");
        assert_eq!(always.mode, DictionaryMode::Always);
        for value in ["", "value", ":always", "value:sometimes"] {
            assert!(
                value.parse::<DictionaryPolicy>().is_err(),
                "{value} should fail"
            );
        }
    }

    #[test]
    fn bloom_parameters_reject_duplicates_unknowns_and_invalid_ranges() {
        let parsed = "fpp=0.05, ndv=42".parse::<BloomFilterSettings>().unwrap();
        assert_eq!(parsed.fpp, 0.05);
        assert_eq!(parsed.ndv, Some(42));
        for value in [
            "fpp=0",
            "fpp=1",
            "fpp=NaN",
            "ndv=0",
            "fpp=0.1,fpp=0.2",
            "ndv=1,ndv=2",
            "unknown=1",
            "fpp",
        ] {
            assert!(
                value.parse::<BloomFilterSettings>().is_err(),
                "{value} should fail"
            );
        }
    }

    #[test]
    fn column_bloom_policy_supports_defaults_and_rejects_empty_names() {
        let defaulted = "value".parse::<ColumnBloomFilterPolicy>().unwrap();
        assert_eq!(defaulted.name, "value");
        assert_eq!(defaulted.config.fpp, DEFAULT_BLOOM_FILTER_FPP);
        let explicit = "value:fpp=0.2,ndv=7"
            .parse::<ColumnBloomFilterPolicy>()
            .unwrap();
        assert_eq!(explicit.config.ndv, Some(7));
        for value in ["", ":fpp=0.1"] {
            assert!(
                value.parse::<ColumnBloomFilterPolicy>().is_err(),
                "{value} should fail"
            );
        }
    }
}
