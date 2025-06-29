use pyo3::prelude::*;

use crate::{DuckDbArgs, SortColumn, SortDirection, SortSpec, commands};

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
    table_name,
    *,
    sort_by = None,
    truncate = false,
    drop_table = false
))]
pub fn arrow_to_duckdb(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    table_name: String,
    sort_by: Option<Vec<PySortColumn>>,
    truncate: bool,
    drop_table: bool,
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
        SortSpec { columns }
    } else {
        SortSpec::default()
    };

    let args = DuckDbArgs {
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
        table_name,
        sort_by: sort_spec,
        truncate,
        drop_table,
    };

    py.allow_threads(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(commands::duckdb::run(args))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    })
}
