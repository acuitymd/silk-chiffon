#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::common::{PySortColumn, create_input, parse_sort_spec};
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
    list_outputs = "none",
    output_ipc_format = "file",
    exclude_columns = []
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
    list_outputs: &str,
    output_ipc_format: &str,
    exclude_columns: Vec<String>,
) -> anyhow::Result<Py<PyDict>> {
    let sort_spec = parse_sort_spec(sort_by)?;
    let compression = compression.parse::<ArrowCompression>()?;
    let list_outputs = list_outputs.parse::<ListOutputsFormat>()?;
    let output_ipc_format = output_ipc_format.parse::<ArrowIPCFormat>()?;

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
        list_outputs,
        output_ipc_format,
        exclude_columns,
    };

    let result = py.allow_threads(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(commands::split_to_arrow::run_with_result(args))
    })?;

    let py_dict = PyDict::new(py);

    for (key, path) in result.output_files {
        py_dict.set_item(key, path.to_string_lossy().to_string())?;
    }

    Ok(py_dict.into())
}
