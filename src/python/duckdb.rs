#![allow(unsafe_op_in_unsafe_fn)]

use clap::ValueEnum;
use pyo3::prelude::*;

use super::common::{
    PySortColumn, create_input, create_output, parse_sort_spec, run_async_command,
};
use crate::{DuckDbArgs, QueryDialect, commands};

#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_path,
    table_name,
    *,
    query = None,
    dialect = "duckdb",
    sort_by = None,
    truncate = false,
    drop_table = false
))]
#[allow(clippy::too_many_arguments)]
pub fn arrow_to_duckdb(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    table_name: String,
    query: Option<String>,
    dialect: &str,
    sort_by: Option<Vec<PySortColumn>>,
    truncate: bool,
    drop_table: bool,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?.unwrap_or_default();
    let dialect = QueryDialect::from_str(dialect, true).map_err(|e| anyhow::anyhow!(e))?;

    let args = DuckDbArgs {
        input: create_input(&input_path)?,
        output: create_output(&output_path)?,
        table_name,
        query,
        dialect,
        sort_by: Some(sort_spec),
        truncate,
        drop_table,
    };

    run_async_command(py, || commands::duckdb::run(args))
}
