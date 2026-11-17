//! Final-plan sort memory tuning.

use datafusion::physical_plan::{ExecutionPlan, sorts::sort::SortExec};

const MIN_SORT_SPILL_RESERVATION: usize = 10 * 1024 * 1024;

/// Tunes every physical sort from its immediate input statistics.
pub(super) fn sort_spill_reservation_from_plan(
    plan: &std::sync::Arc<dyn ExecutionPlan>,
    memory_per_partition: usize,
    batch_size: usize,
) -> Option<usize> {
    let mut reservations = Vec::new();
    let mut pending = vec![std::sync::Arc::clone(plan)];
    while let Some(plan) = pending.pop() {
        if let Some(sort) = plan.downcast_ref::<SortExec>() {
            let input = sort.input();
            let partition_count = input.properties().partitioning.partition_count().max(1);
            let maximum = memory_per_partition / 2;
            let fallback = (maximum >= MIN_SORT_SPILL_RESERVATION)
                .then(|| (memory_per_partition / 10).clamp(MIN_SORT_SPILL_RESERVATION, maximum));
            for partition in 0..partition_count {
                let reservation = input
                    .partition_statistics(Some(partition))
                    .ok()
                    .and_then(|statistics| {
                        let rows = *statistics.num_rows.get_value()?;
                        let bytes = *statistics.total_byte_size.get_value()?;
                        let average_row_bytes = bytes.checked_div(rows.max(1))?;
                        estimate_sort_spill_reservation(
                            average_row_bytes,
                            bytes,
                            memory_per_partition,
                            batch_size,
                        )
                    })
                    .or(fallback);
                reservations.push(reservation);
            }
        }
        pending.extend(plan.children().into_iter().cloned());
    }
    (!reservations.is_empty())
        .then(|| reservations.into_iter().flatten().max())
        .flatten()
}

fn estimate_sort_spill_reservation(
    average_row_bytes: usize,
    total_in_memory_bytes: usize,
    memory_per_partition: usize,
    batch_size: usize,
) -> Option<usize> {
    if average_row_bytes == 0 || memory_per_partition == 0 {
        return None;
    }
    let maximum = memory_per_partition / 2;
    if maximum < MIN_SORT_SPILL_RESERVATION {
        return None;
    }
    let spill_files = total_in_memory_bytes
        .checked_div(memory_per_partition)
        .unwrap_or(1)
        .max(1);
    let reservation = spill_files.saturating_mul(batch_size.saturating_mul(average_row_bytes));
    Some(reservation.clamp(MIN_SORT_SPILL_RESERVATION, maximum))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        common::{ColumnStatistics, Statistics, stats::Precision},
        physical_expr::{Partitioning, PhysicalSortExpr, expressions::Column},
        physical_plan::{
            ExecutionPlan, repartition::RepartitionExec, sorts::sort::SortExec,
            test::exec::StatisticsExec, union::UnionExec,
        },
    };

    use super::*;

    fn sort_with_statistics(statistics: Statistics) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let input = Arc::new(StatisticsExec::new(statistics, schema));
        let input =
            Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2)).unwrap());
        Arc::new(SortExec::new(
            [PhysicalSortExpr::new_default(Arc::new(Column::new(
                "value", 0,
            )))]
            .into(),
            input,
        ))
    }

    #[test]
    fn known_statistics_drive_the_reservation() {
        let plan = sort_with_statistics(Statistics {
            num_rows: Precision::Inexact(50_000_000),
            total_byte_size: Precision::Inexact(10_000_000_000),
            column_statistics: vec![ColumnStatistics::new_unknown()],
        });
        assert_eq!(
            sort_spill_reservation_from_plan(&plan, 500_000_000, 8192),
            Some(10 * 8192 * 200)
        );
    }

    #[test]
    fn unknown_statistics_use_the_bounded_fallback() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let plan = sort_with_statistics(Statistics::new_unknown(&schema));
        assert_eq!(
            sort_spill_reservation_from_plan(&plan, 200_000_000, 8192),
            Some(20_000_000)
        );
    }

    #[test]
    fn one_unknown_sort_does_not_hide_a_larger_known_reservation() {
        let known = sort_with_statistics(Statistics {
            num_rows: Precision::Inexact(200_000_000),
            total_byte_size: Precision::Inexact(40_000_000_000),
            column_statistics: vec![ColumnStatistics::new_unknown()],
        });
        let unknown = sort_with_statistics(Statistics::new_unknown(&known.schema()));
        let plan = UnionExec::try_new(vec![known, unknown]).unwrap();
        assert_eq!(
            sort_spill_reservation_from_plan(&plan, 500_000_000, 8192),
            Some(40 * 8192 * 200)
        );
    }
}
