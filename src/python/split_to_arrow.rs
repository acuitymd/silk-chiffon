#![allow(unsafe_op_in_unsafe_fn)]

use clap::ValueEnum;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::common::{PySortColumn, create_input, parse_sort_spec};
use crate::{
    ArrowCompression, ListOutputsFormat, QueryDialect, SplitToArrowArgs, commands,
    utils::arrow_io::ArrowIPCFormat,
};

#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_template,
    split_column,
    *,
    query = None,
    dialect = "duckdb",
    sort_by = None,
    compression = "none",
    create_dirs = true,
    overwrite = false,
    record_batch_size = 122_880,
    list_outputs = "none",
    output_ipc_format = "file",
    exclude_columns = vec![]
))]
#[allow(clippy::too_many_arguments)]
pub fn split_to_arrow(
    py: Python<'_>,
    input_path: String,
    output_template: String,
    split_column: String,
    query: Option<String>,
    dialect: &str,
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
    let compression =
        ArrowCompression::from_str(compression, true).map_err(|e| anyhow::anyhow!(e))?;
    let list_outputs =
        ListOutputsFormat::from_str(list_outputs, true).map_err(|e| anyhow::anyhow!(e))?;
    let output_ipc_format =
        ArrowIPCFormat::from_str(output_ipc_format, true).map_err(|e| anyhow::anyhow!(e))?;
    let dialect = QueryDialect::from_str(dialect, true).map_err(|e: String| anyhow::anyhow!(e))?;

    let args = SplitToArrowArgs {
        input: create_input(&input_path)?,
        by: split_column,
        output_template,
        query,
        dialect,
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

    for (key, result_info) in result.output_files {
        let info_dict = PyDict::new(py);
        info_dict.set_item("path", result_info.path.to_string_lossy().to_string())?;
        info_dict.set_item("row_count", result_info.row_count)?;
        py_dict.set_item(key, info_dict)?;
    }

    Ok(py_dict.into())
}
