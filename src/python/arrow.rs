use pyo3::prelude::*;

use crate::{ArrowArgs, ArrowCompression, SortColumn, SortDirection, SortSpec, commands};

#[derive(FromPyObject)]
enum PySortColumn {
    #[pyo3(transparent)]
    Name(String),
    #[pyo3(transparent)]
    NameAndDirection((String, String)),
}

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
) -> PyResult<()> {
    let sort_spec = if let Some(cols) = sort_by {
        let columns =
            cols.into_iter()
                .map(|col| match col {
                    PySortColumn::Name(name) => Ok(SortColumn {
                        name,
                        direction: SortDirection::Ascending,
                    }),
                    PySortColumn::NameAndDirection((name, dir)) => {
                        let direction = match dir.as_str() {
                            "asc" => SortDirection::Ascending,
                            "desc" => SortDirection::Descending,
                            _ => {
                                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                    format!("Invalid sort direction: {}. Use 'asc' or 'desc'", dir),
                                ));
                            }
                        };
                        Ok(SortColumn { name, direction })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
        Some(SortSpec { columns })
    } else {
        None
    };

    let compression = match compression {
        "zstd" => ArrowCompression::Zstd,
        "lz4" => ArrowCompression::Lz4,
        "none" => ArrowCompression::None,
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid compression: {}. Valid options: zstd, lz4, none",
                compression
            )));
        }
    };

    let args = ArrowArgs {
        input: clio::Input::new(&input_path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Failed to open input file: {}",
                e
            ))
        })?,
        output: clio::OutputPath::new(&output_path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Failed to open output file: {}",
                e
            ))
        })?,
        sort_by: sort_spec,
        compression,
        record_batch_size,
    };

    py.allow_threads(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(commands::arrow::run(args))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    })
}
