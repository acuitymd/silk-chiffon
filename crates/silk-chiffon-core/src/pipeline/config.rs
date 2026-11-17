use std::fmt;

use clap::ValueEnum;
use datafusion::config::Dialect;
use strum_macros::Display;

/// SQL parser dialect used while applying query operations.
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

/// Compression codec for spilled intermediate data.
#[derive(ValueEnum, Clone, Copy, Debug, Default)]
#[value(rename_all = "lowercase")]
pub enum SpillCompression {
    None,
    #[default]
    Lz4,
    Zstd,
}

impl fmt::Display for SpillCompression {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        })
    }
}

impl From<SpillCompression> for datafusion::config::SpillCompression {
    fn from(compression: SpillCompression) -> Self {
        match compression {
            SpillCompression::None => datafusion::config::SpillCompression::Uncompressed,
            SpillCompression::Lz4 => datafusion::config::SpillCompression::Lz4Frame,
            SpillCompression::Zstd => datafusion::config::SpillCompression::Zstd,
        }
    }
}
