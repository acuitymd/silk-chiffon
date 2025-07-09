#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;
use std::collections::HashMap;

use super::common::{PySortColumn, create_input, parse_sort_spec, run_async_command};
use crate::{
    AllColumnsBloomFilterConfig, BloomFilterConfig, ColumnBloomFilterConfig,
    ColumnSpecificBloomFilterConfig, DEFAULT_BLOOM_FILTER_FPP, ParquetCompression,
    ParquetStatistics, ParquetWriterVersion, SplitToParquetArgs, commands,
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
    output_template,
    split_column,
    *,
    sort_by = None,
    compression = "none",
    create_dirs = true,
    overwrite = false,
    record_batch_size = 122_880,
    write_sorted_metadata = false,
    bloom_filter_all = None,
    bloom_filter_columns = None,
    max_row_group_size = 1_048_576,
    statistics = "page",
    enable_dictionary = true,
    writer_version = "v2"
))]
#[allow(clippy::too_many_arguments)]
pub fn split_to_parquet(
    py: Python<'_>,
    input_path: String,
    output_template: String,
    split_column: String,
    sort_by: Option<Vec<PySortColumn>>,
    compression: &str,
    create_dirs: bool,
    overwrite: bool,
    record_batch_size: usize,
    write_sorted_metadata: bool,
    bloom_filter_all: Option<PyBloomFilterAll>,
    bloom_filter_columns: Option<Vec<PyBloomFilterColumn>>,
    max_row_group_size: usize,
    statistics: &str,
    enable_dictionary: bool,
    writer_version: &str,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?;
    let compression = compression.parse::<ParquetCompression>()?;

    let bloom_config = match (bloom_filter_all, bloom_filter_columns) {
        (Some(all_config), None) => match all_config {
            PyBloomFilterAll::Bool(true) => BloomFilterConfig::All(AllColumnsBloomFilterConfig {
                fpp: DEFAULT_BLOOM_FILTER_FPP,
            }),
            PyBloomFilterAll::Bool(false) => BloomFilterConfig::None,
            PyBloomFilterAll::Fpp(fpp) => {
                BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp })
            }
            PyBloomFilterAll::Dict(config) => {
                let columns = config
                    .into_iter()
                    .map(|(name, fpp)| ColumnSpecificBloomFilterConfig {
                        name,
                        config: ColumnBloomFilterConfig { fpp },
                    })
                    .collect();
                BloomFilterConfig::Columns(columns)
            }
        },
        (None, Some(columns)) => {
            let column_configs = columns
                .into_iter()
                .map(|col| match col {
                    PyBloomFilterColumn::Name(name) => ColumnSpecificBloomFilterConfig {
                        name,
                        config: ColumnBloomFilterConfig {
                            fpp: DEFAULT_BLOOM_FILTER_FPP,
                        },
                    },
                    PyBloomFilterColumn::Config(config) => {
                        let (name, fpp) = config.into_iter().next().unwrap();
                        ColumnSpecificBloomFilterConfig {
                            name,
                            config: ColumnBloomFilterConfig { fpp },
                        }
                    }
                })
                .collect();
            BloomFilterConfig::Columns(column_configs)
        }
        (None, None) => BloomFilterConfig::None,
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!(
                "Cannot specify both bloom_filter_all and bloom_filter_columns",
            ));
        }
    };

    let (bloom_all, bloom_column) = match bloom_config {
        BloomFilterConfig::None => (None, Vec::new()),
        BloomFilterConfig::All(config) => (Some(config), Vec::new()),
        BloomFilterConfig::Columns(columns) => (None, columns),
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

    let args = SplitToParquetArgs {
        input: create_input(&input_path)?,
        by: split_column,
        output_template,
        record_batch_size,
        sort_by: sort_spec,
        create_dirs,
        overwrite,
        compression,
        statistics,
        max_row_group_size,
        writer_version,
        no_dictionary: !enable_dictionary,
        write_sorted_metadata,
        bloom_all,
        bloom_column,
    };

    run_async_command(py, || commands::split_to_parquet::run(args))
}
