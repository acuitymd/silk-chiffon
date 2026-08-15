use anyhow::Result;
use datafusion::prelude::{DataFrame, col};
use silk_chiffon_core::{NullPlacement, SortColumn, SortDirection};

pub(super) fn apply_sort(input: DataFrame, columns: &[SortColumn]) -> Result<DataFrame> {
    Ok(input.sort(
        columns
            .iter()
            .map(|column| {
                col(column.name()).sort(
                    column.direction() == SortDirection::Ascending,
                    column.null_placement() == NullPlacement::First,
                )
            })
            .collect(),
    )?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn sorted_values(column: SortColumn) -> Int32Array {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![
                Some(3),
                None,
                Some(1),
                None,
                Some(2),
            ]))],
        )
        .unwrap();
        let session = SessionContext::new();
        let result = apply_sort(session.read_batch(batch).unwrap(), &[column])
            .unwrap()
            .collect()
            .await
            .unwrap();
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn explicit_null_placement_is_preserved_for_both_directions() {
        let ascending = sorted_values(SortColumn::new(
            "id",
            SortDirection::Ascending,
            NullPlacement::First,
        ))
        .await;
        assert_eq!(
            ascending.iter().collect::<Vec<_>>(),
            vec![None, None, Some(1), Some(2), Some(3)]
        );

        let descending = sorted_values(SortColumn::new(
            "id",
            SortDirection::Descending,
            NullPlacement::Last,
        ))
        .await;
        assert_eq!(
            descending.iter().collect::<Vec<_>>(),
            vec![Some(3), Some(2), Some(1), None, None]
        );
    }
}
