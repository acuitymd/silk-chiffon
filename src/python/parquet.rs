#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::common::{
    PySortColumn, create_input, create_output, parse_sort_spec, run_async_command,
};
use crate::{
    AllColumnsBloomFilterConfig, BloomFilterConfig, ColumnBloomFilterConfig,
    ColumnSpecificBloomFilterConfig, DEFAULT_BLOOM_FILTER_FPP, ParquetArgs, ParquetCompression,
    ParquetStatistics, ParquetWriterVersion, commands,
};

#[derive(FromPyObject)]
pub enum PyBloomFilterAll {
    #[pyo3(transparent)]
    Bool(bool),
    #[pyo3(transparent)]
    Fpp(f64),
    #[pyo3(transparent)]
    Dict(HashMap<String, f64>),
}

#[derive(FromPyObject)]
pub enum PyBloomFilterColumn {
    #[pyo3(transparent)]
    Name(String),
    #[pyo3(transparent)]
    Config(HashMap<String, f64>),
}

#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_path,
    *,
    query = None,
    sort_by = None,
    compression = "none",
    write_sorted_metadata = false,
    bloom_filter_all = None,
    bloom_filter_columns = None,
    max_row_group_size = 1_048_576,
    statistics = "page",
    record_batch_size = 122_880,
    enable_dictionary = true,
    writer_version = "v2"
))]
#[allow(clippy::too_many_arguments)]
pub fn arrow_to_parquet(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    query: Option<String>,
    sort_by: Option<Vec<PySortColumn>>,
    compression: &str,
    write_sorted_metadata: bool,
    bloom_filter_all: Option<PyBloomFilterAll>,
    bloom_filter_columns: Option<Vec<PyBloomFilterColumn>>,
    max_row_group_size: usize,
    statistics: &str,
    record_batch_size: usize,
    enable_dictionary: bool,
    writer_version: &str,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?.unwrap_or_default();

    let bloom_config = match (bloom_filter_all, bloom_filter_columns) {
        (Some(all_config), None) => match all_config {
            PyBloomFilterAll::Bool(true) => BloomFilterConfig::All(AllColumnsBloomFilterConfig {
                fpp: DEFAULT_BLOOM_FILTER_FPP,
            }),
            PyBloomFilterAll::Bool(false) => BloomFilterConfig::None,
            PyBloomFilterAll::Fpp(fpp) => {
                BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp })
            }
            PyBloomFilterAll::Dict(dict) => {
                let fpp = dict.get("fpp").copied().unwrap_or(DEFAULT_BLOOM_FILTER_FPP);
                BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp })
            }
        },
        (None, Some(columns)) => {
            let configs = columns
                .into_iter()
                .map(|col| match col {
                    PyBloomFilterColumn::Name(name) => ColumnSpecificBloomFilterConfig {
                        name,
                        config: ColumnBloomFilterConfig {
                            fpp: DEFAULT_BLOOM_FILTER_FPP,
                        },
                    },
                    PyBloomFilterColumn::Config(dict) => {
                        if let Some((column_name, fpp_value)) = dict.iter().next() {
                            ColumnSpecificBloomFilterConfig {
                                name: column_name.clone(),
                                config: ColumnBloomFilterConfig { fpp: *fpp_value },
                            }
                        } else {
                            ColumnSpecificBloomFilterConfig {
                                name: String::new(),
                                config: ColumnBloomFilterConfig {
                                    fpp: DEFAULT_BLOOM_FILTER_FPP,
                                },
                            }
                        }
                    }
                })
                .collect();
            BloomFilterConfig::Columns(configs)
        }
        (None, None) => BloomFilterConfig::None,
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!(
                "Cannot specify both bloom_filter_all and bloom_filter_columns",
            ));
        }
    };

    let compression = match compression {
        "zstd" => ParquetCompression::Zstd,
        "snappy" => ParquetCompression::Snappy,
        "gzip" => ParquetCompression::Gzip,
        "lz4" => ParquetCompression::Lz4,
        "none" => ParquetCompression::None,
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid compression: {}. Valid options: zstd, snappy, gzip, lz4, none",
                compression
            ));
        }
    };

    let statistics = match statistics {
        "none" => ParquetStatistics::None,
        "chunk" => ParquetStatistics::Chunk,
        "page" => ParquetStatistics::Page,
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid statistics: {}. Valid options: none, chunk, page",
                statistics
            ));
        }
    };

    let writer_version = match writer_version {
        "v1" => ParquetWriterVersion::V1,
        "v2" => ParquetWriterVersion::V2,
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid writer version: {}. Valid options: v1, v2",
                writer_version
            ));
        }
    };

    let (bloom_all, bloom_column) = match bloom_config {
        BloomFilterConfig::None => (None, Vec::new()),
        BloomFilterConfig::All(config) => (Some(config), Vec::new()),
        BloomFilterConfig::Columns(columns) => (None, columns),
    };

    let args = ParquetArgs {
        input: create_input(&input_path)?,
        output: create_output(&output_path)?,
        query,
        sort_by: sort_spec,
        compression,
        write_sorted_metadata,
        bloom_all,
        bloom_column,
        max_row_group_size,
        statistics,
        record_batch_size,
        no_dictionary: !enable_dictionary,
        writer_version,
    };

    run_async_command(py, || commands::parquet::run(args))
}
