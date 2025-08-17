use crate::{SortColumn, SortDirection, SortSpec};
use clap::ValueEnum;
use pyo3::prelude::*;

#[derive(FromPyObject)]
pub enum PySortColumn {
    #[pyo3(transparent)]
    Name(String),
    #[pyo3(transparent)]
    NameAndDirection((String, String)),
}

pub fn parse_sort_spec(sort_by: Option<Vec<PySortColumn>>) -> anyhow::Result<Option<SortSpec>> {
    if let Some(cols) = sort_by {
        let columns = cols
            .into_iter()
            .map(|col| -> anyhow::Result<SortColumn> {
                match col {
                    PySortColumn::Name(name) => Ok(SortColumn {
                        name,
                        direction: SortDirection::Ascending,
                    }),
                    PySortColumn::NameAndDirection((name, dir)) => Ok(SortColumn {
                        name,
                        direction: SortDirection::from_str(&dir, true)
                            .map_err(|e| anyhow::anyhow!(e))?,
                    }),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(SortSpec { columns }))
    } else {
        Ok(None)
    }
}

pub fn create_input(path: &str) -> anyhow::Result<clio::Input> {
    clio::Input::new(path).map_err(|e| anyhow::anyhow!("Failed to open input file: {}", e))
}

pub fn create_output(path: &str) -> anyhow::Result<clio::OutputPath> {
    clio::OutputPath::new(path).map_err(|e| anyhow::anyhow!("Failed to open output file: {}", e))
}

pub fn run_async_command<F, Fut>(py: Python<'_>, f: F) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut + Send,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
{
    py.allow_threads(|| tokio::runtime::Runtime::new().unwrap().block_on(f()))
}
