#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;

use super::common::{PySortColumn, create_output, parse_sort_spec, run_async_command};
use crate::{MergeToDuckdbArgs, commands};

#[pyfunction]
#[pyo3(signature = (
    input_paths,
    output_path,
    table_name,
    *,
    query = None,
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
    sort_by: Option<Vec<PySortColumn>>,
    truncate: bool,
    drop_table: bool,
    record_batch_size: usize,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?.unwrap_or_default();

    let args = MergeToDuckdbArgs {
        inputs: input_paths,
        output: create_output(&output_path)?,
        table_name,
        query,
        sort_by: sort_spec,
        truncate,
        drop_table,
        record_batch_size,
    };

    run_async_command(py, || commands::merge_to_duckdb::run(args))
}
