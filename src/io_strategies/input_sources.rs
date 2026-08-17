//! Nonempty command input collections and their combined source properties.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use datafusion::{catalog::TableProvider, prelude::SessionContext};

use crate::sources::data_source::{DataSource, Replayability, RowCount, RowCountCapability};

/// A command's nonempty collection of input sources.
///
/// Keeping the first source separate makes the nonempty invariant part of the type. A single
/// source exposes its provider directly. Multiple sources become a DataFusion union in the same
/// session used to plan the command.
pub struct InputSources {
    first: Box<dyn DataSource>,
    rest: Vec<Box<dyn DataSource>>,
}

impl InputSources {
    /// Starts the collection with its required first source.
    pub fn new(first: Box<dyn DataSource>) -> Self {
        Self {
            first,
            rest: Vec::new(),
        }
    }

    /// Appends another source in input order.
    pub fn push(&mut self, source: Box<dyn DataSource>) {
        self.rest.push(source);
    }

    /// Iterates over every source in input order.
    pub fn iter(&self) -> impl Iterator<Item = &dyn DataSource> {
        std::iter::once(self.first.as_ref()).chain(self.rest.iter().map(Box::as_ref))
    }

    /// Creates the provider for the complete logical input.
    ///
    /// Multiple inputs use DataFusion's union semantics rather than a Silk Chiffon stream wrapper.
    pub async fn table_provider(&self, session: &SessionContext) -> Result<Arc<dyn TableProvider>> {
        let first = self.first.table_provider().await?;
        if self.rest.is_empty() {
            return Ok(first);
        }

        let mut table = session.read_table(first)?;
        for source in &self.rest {
            table = table.union(session.read_table(source.table_provider().await?)?)?;
        }
        Ok(table.into_view())
    }

    /// Returns [`Replayability::Replayable`] only when every input is replayable.
    pub fn replayability(&self) -> Replayability {
        if self
            .iter()
            .all(|source| source.replayability() == Replayability::Replayable)
        {
            Replayability::Replayable
        } else {
            Replayability::SinglePass
        }
    }

    /// Returns combined row-count behavior only when every input provides it.
    ///
    /// The combined operation returns an estimate if any component count is estimated and returns
    /// unknown if any component count is unknown.
    pub fn row_count_capability(&self) -> Option<&dyn RowCountCapability> {
        self.iter()
            .all(|source| source.row_count_capability().is_some())
            .then_some(self)
    }
}

#[async_trait]
impl RowCountCapability for InputSources {
    async fn row_count(&self) -> Result<RowCount> {
        let mut total = 0_u64;
        let mut estimated = false;
        for source in self.iter() {
            let capability = source
                .row_count_capability()
                .ok_or_else(|| anyhow!("source does not provide row-count metadata"))?;
            match capability.row_count().await? {
                RowCount::Exact(count) => {
                    total = total
                        .checked_add(count)
                        .ok_or_else(|| anyhow!("combined row count exceeds u64"))?;
                }
                RowCount::Estimated(count) => {
                    total = total
                        .checked_add(count)
                        .ok_or_else(|| anyhow!("combined row count exceeds u64"))?;
                    estimated = true;
                }
                RowCount::Unknown => return Ok(RowCount::Unknown),
            }
        }
        Ok(if estimated {
            RowCount::Estimated(total)
        } else {
            RowCount::Exact(total)
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use datafusion::datasource::empty::EmptyTable;

    use super::*;

    struct TestSource {
        replayability: Replayability,
        count: Option<RowCount>,
    }

    #[async_trait]
    impl DataSource for TestSource {
        fn name(&self) -> &str {
            "test"
        }

        fn replayability(&self) -> Replayability {
            self.replayability
        }

        fn row_count_capability(&self) -> Option<&dyn RowCountCapability> {
            self.count.map(|_| self as &dyn RowCountCapability)
        }

        async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
            Ok(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))))
        }
    }

    #[async_trait]
    impl RowCountCapability for TestSource {
        async fn row_count(&self) -> Result<RowCount> {
            Ok(self.count.expect("capability is present"))
        }
    }

    fn source(replayability: Replayability, count: Option<RowCount>) -> Box<dyn DataSource> {
        Box::new(TestSource {
            replayability,
            count,
        })
    }

    #[test]
    fn replayability_requires_every_source_to_be_replayable() {
        let mut inputs =
            InputSources::new(source(Replayability::Replayable, Some(RowCount::Exact(2))));
        inputs.push(source(Replayability::SinglePass, None));
        assert_eq!(inputs.replayability(), Replayability::SinglePass);

        let mut inputs =
            InputSources::new(source(Replayability::Replayable, Some(RowCount::Exact(2))));
        inputs.push(source(Replayability::Replayable, Some(RowCount::Exact(3))));
        assert_eq!(inputs.replayability(), Replayability::Replayable);
    }

    #[tokio::test]
    async fn row_count_requires_every_source_capability() {
        let mut mixed =
            InputSources::new(source(Replayability::Replayable, Some(RowCount::Exact(2))));
        mixed.push(source(Replayability::Replayable, None));
        assert!(mixed.row_count_capability().is_none());

        let mut complete =
            InputSources::new(source(Replayability::Replayable, Some(RowCount::Exact(2))));
        complete.push(source(
            Replayability::Replayable,
            Some(RowCount::Estimated(3)),
        ));
        let count = complete
            .row_count_capability()
            .expect("all sources provide row counts")
            .row_count()
            .await
            .unwrap();
        assert_eq!(count, RowCount::Estimated(5));
    }
}
