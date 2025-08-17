#![allow(unsafe_op_in_unsafe_fn)]

use clap::ValueEnum;
use pyo3::prelude::*;

use super::common::{PySortColumn, create_output, parse_sort_spec, run_async_command};
use crate::{MergeToDuckdbArgs, QueryDialect, commands};

#[pyfunction]
#[pyo3(signature = (
    input_paths,
    output_path,
    table_name,
    *,
    query = None,
    dialect = "duckdb",
    sort_by = None,
    truncate = false,
    drop_table = false,
    record_batch_size = 122_880
))]
#[allow(clippy::too_many_arguments)]
pub fn merge_to_duckdb(
    py: Python<'_>,
    input_paths: Vec<String>,
    output_path: String,
    table_name: String,
    query: Option<String>,
    dialect: &str,
    sort_by: Option<Vec<PySortColumn>>,
    truncate: bool,
    drop_table: bool,
    record_batch_size: usize,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?.unwrap_or_default();
    let dialect = QueryDialect::from_str(dialect, true).map_err(|e| anyhow::anyhow!(e))?;

    let args = MergeToDuckdbArgs {
        inputs: input_paths,
        output: create_output(&output_path)?,
        table_name,
        query,
        dialect,
        sort_by: Some(sort_spec),
        truncate,
        drop_table,
        record_batch_size,
    };

    run_async_command(py, || commands::merge_to_duckdb::run(args))
}
