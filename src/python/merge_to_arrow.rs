#![allow(unsafe_op_in_unsafe_fn)]

use clap::ValueEnum;
use pyo3::prelude::*;

use super::common::{PySortColumn, create_output, parse_sort_spec, run_async_command};
use crate::{
    ArrowCompression, MergeToArrowArgs, QueryDialect, commands, utils::arrow_io::ArrowIPCFormat,
};

#[pyfunction]
#[pyo3(signature = (
    input_paths,
    output_path,
    *,
    query = None,
    dialect = "duckdb",
    sort_by = None,
    compression = "none",
    record_batch_size = 122_880,
    output_ipc_format = "file"
))]
#[allow(clippy::too_many_arguments)]
pub fn merge_to_arrow(
    py: Python<'_>,
    input_paths: Vec<String>,
    output_path: String,
    query: Option<String>,
    dialect: &str,
    sort_by: Option<Vec<PySortColumn>>,
    compression: &str,
    record_batch_size: usize,
    output_ipc_format: &str,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?.unwrap_or_default();
    let compression =
        ArrowCompression::from_str(compression, true).map_err(|e| anyhow::anyhow!(e))?;
    let output_ipc_format =
        ArrowIPCFormat::from_str(output_ipc_format, true).map_err(|e| anyhow::anyhow!(e))?;
    let dialect = QueryDialect::from_str(dialect, true).map_err(|e| anyhow::anyhow!(e))?;

    let args = MergeToArrowArgs {
        inputs: input_paths,
        output: create_output(&output_path)?,
        query,
        dialect,
        sort_by: Some(sort_spec),
        compression,
        record_batch_size,
        output_ipc_format,
    };

    run_async_command(py, || commands::merge_to_arrow::run(args))
}
