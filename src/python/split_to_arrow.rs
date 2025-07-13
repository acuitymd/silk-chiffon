#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;

use super::common::{PySortColumn, create_input, parse_sort_spec, run_async_command};
use crate::{
    ArrowCompression, ListOutputsFormat, SplitToArrowArgs, commands,
    utils::arrow_io::ArrowIPCFormat,
};

#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_template,
    split_column,
    *,
    query = None,
    sort_by = None,
    compression = "none",
    create_dirs = true,
    overwrite = false,
    record_batch_size = 122_880,
))]
#[allow(clippy::too_many_arguments)]
pub fn split_to_arrow(
    py: Python<'_>,
    input_path: String,
    output_template: String,
    split_column: String,
    query: Option<String>,
    sort_by: Option<Vec<PySortColumn>>,
    compression: &str,
    create_dirs: bool,
    overwrite: bool,
    record_batch_size: usize,
) -> anyhow::Result<()> {
    let sort_spec = parse_sort_spec(sort_by)?;
    let compression = compression.parse::<ArrowCompression>()?;

    let args = SplitToArrowArgs {
        input: create_input(&input_path)?,
        by: split_column,
        output_template,
        query,
        record_batch_size,
        sort_by: sort_spec,
        create_dirs,
        overwrite,
        compression,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
    };

    run_async_command(py, || commands::split_to_arrow::run(args))
}
