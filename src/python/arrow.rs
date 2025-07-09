#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;

use super::common::{
    PySortColumn, create_input, create_output, parse_sort_spec, run_async_command,
};
use crate::{ArrowArgs, ArrowCompression, commands};

#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_path,
    *,
    sort_by = None,
    compression = "none",
    record_batch_size = 122_880
))]
pub fn arrow_to_arrow(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    sort_by: Option<Vec<PySortColumn>>,
    compression: &str,
    record_batch_size: usize,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?;
    let compression = compression.parse::<ArrowCompression>()?;

    let args = ArrowArgs {
        input: create_input(&input_path)?,
        output: create_output(&output_path)?,
        sort_by: sort_spec,
        compression,
        record_batch_size,
    };

    run_async_command(py, || commands::arrow::run(args))
}
