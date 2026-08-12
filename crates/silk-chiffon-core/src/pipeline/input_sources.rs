//! A command's retained lazy input frame.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::{catalog::TableProvider, dataframe::DataFrame, prelude::SessionContext};

/// The nonempty, lazy base frame for one transform command.
///
/// Each provider is a leaf dataset. Schemas are aligned only at this boundary with DataFusion's
/// `union_by_name`; cloning the returned frame rebuilds logical consumers without executing input.
#[derive(Clone)]
pub struct InputSources {
    data_frame: DataFrame,
}

impl InputSources {
    /// Builds the base frame from leaf providers in operand order.
    pub fn try_new(
        session: &SessionContext,
        providers: Vec<Arc<dyn TableProvider>>,
    ) -> Result<Self> {
        let mut providers = providers.into_iter();
        let first = providers
            .next()
            .ok_or_else(|| anyhow!("no input providers supplied"))?;
        let mut data_frame = session.read_table(first)?;
        for provider in providers {
            data_frame = data_frame.union_by_name(session.read_table(provider)?)?;
        }
        Ok(Self { data_frame })
    }

    /// Returns a clone of the retained lazy base frame.
    pub fn data_frame(&self) -> DataFrame {
        self.data_frame.clone()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;

    use super::*;

    #[test]
    fn union_by_name_aligns_leaf_schemas_in_operand_order() {
        let session = SessionContext::new();
        let first = Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
            "left",
            DataType::Int64,
            false,
        )])))) as Arc<dyn TableProvider>;
        let second = Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
            "right",
            DataType::Utf8,
            false,
        )])))) as Arc<dyn TableProvider>;

        let inputs = InputSources::try_new(&session, vec![first, second]).unwrap();
        let names = inputs
            .data_frame()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(names, ["left", "right"]);
    }

    #[test]
    fn empty_provider_collection_is_rejected() {
        let error = match InputSources::try_new(&SessionContext::new(), Vec::new()) {
            Ok(_) => panic!("empty providers must fail"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "no input providers supplied");
    }
}
