use pyo3::prelude::*;

mod arrow;
mod common;
mod duckdb;
mod parquet;
mod split_to_arrow;
mod split_to_parquet;

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(arrow::arrow_to_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(parquet::arrow_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(duckdb::arrow_to_duckdb, m)?)?;
    m.add_function(wrap_pyfunction!(split_to_arrow::split_to_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(split_to_parquet::split_to_parquet, m)?)?;
    Ok(())
}
