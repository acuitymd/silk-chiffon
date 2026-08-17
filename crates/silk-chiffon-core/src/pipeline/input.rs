//! Composition of file and service input providers.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::{catalog::TableProvider, dataframe::DataFrame, prelude::SessionContext};

/// Builds one nonempty lazy frame by aligning provider schemas by name.
pub fn union_input_providers_by_name(
    session: &SessionContext,
    providers: Vec<Arc<dyn TableProvider>>,
) -> Result<DataFrame> {
    let mut providers = providers.into_iter();
    let first = providers
        .next()
        .ok_or_else(|| anyhow!("no input providers supplied"))?;
    let mut data_frame = session.read_table(first)?;
    for provider in providers {
        data_frame = data_frame.union_by_name(session.read_table(provider)?)?;
    }
    Ok(data_frame)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;

    use super::*;

    #[test]
    fn union_by_name_aligns_provider_schemas_in_operand_order() {
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

        let input = union_input_providers_by_name(&session, vec![first, second]).unwrap();
        let names = input
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(names, ["left", "right"]);
    }

    #[test]
    fn empty_provider_collection_is_rejected() {
        let error = union_input_providers_by_name(&SessionContext::new(), Vec::new()).unwrap_err();
        assert_eq!(error.to_string(), "no input providers supplied");
    }
}
