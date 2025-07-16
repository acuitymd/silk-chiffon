#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;

use super::common::{
    PySortColumn, create_input, create_output, parse_sort_spec, run_async_command,
};
use crate::{ArrowArgs, ArrowCompression, commands, utils::arrow_io::ArrowIPCFormat};

#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_path,
    *,
    query = None,
    sort_by = None,
    compression = "none",
    record_batch_size = 122_880,
    output_ipc_format = "file"
))]
#[allow(clippy::too_many_arguments)]
pub fn arrow_to_arrow(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    query: Option<String>,
    sort_by: Option<Vec<PySortColumn>>,
    compression: &str,
    record_batch_size: usize,
    output_ipc_format: &str,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?;
    let compression = compression.parse::<ArrowCompression>()?;
    let output_ipc_format = output_ipc_format.parse::<ArrowIPCFormat>()?;

    let args = ArrowArgs {
        input: create_input(&input_path)?,
        output: create_output(&output_path)?,
        query,
        sort_by: sort_spec,
        compression,
        record_batch_size,
        output_ipc_format,
    };

    run_async_command(py, || commands::arrow::run(args))
}
