//! Container-aware memory detection and sort memory estimation.
//!
//! Uses cgroup limits when running in a container (Linux), falls back to system
//! memory on macOS or when not containerized.

use arrow::datatypes::{DataType, Schema};
use futures::StreamExt;
use sysinfo::System;

use crate::io_strategies::input_strategy::InputStrategy;

/// Returns total memory in bytes, respecting container cgroup limits.
///
/// In containerized environments (Docker, Kubernetes), this returns the cgroup memory
/// limit. On macOS, bare metal Linux, or when cgroup limits aren't set, falls back to
/// system total memory.
#[allow(clippy::cast_possible_truncation)]
pub fn total_memory() -> usize {
    #[cfg(target_os = "linux")]
    if let Some(limit) = cgroup_total_memory() {
        return limit;
    }

    let sys = System::new_all();
    sys.total_memory() as usize
}

/// Returns available memory in bytes, respecting container cgroup limits.
///
/// In containerized environments (Docker, Kubernetes), this returns the available
/// memory within the cgroup constraints. On macOS, bare metal Linux, or when cgroup
/// limits aren't set, falls back to system available memory.
#[allow(clippy::cast_possible_truncation)]
pub fn available_memory() -> usize {
    #[cfg(target_os = "linux")]
    if let Some(available) = cgroup_available_memory() {
        return available;
    }

    let sys = System::new_all();
    let available = sys.available_memory() as usize;

    if available > 0 {
        available
    } else {
        sys.total_memory() as usize / 2
    }
}

/// Returns cgroup memory limit. Tries v2 first, then v1.
#[cfg(target_os = "linux")]
#[allow(clippy::cast_possible_truncation)]
fn cgroup_total_memory() -> Option<usize> {
    use std::fs;

    let v2 = fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|c| parse_memory_max(&c))
        .map(|v| v as usize);
    if v2.is_some() {
        return v2;
    }

    fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        .ok()
        .and_then(|c| parse_cgroup_v1_limit(&c))
        .map(|v| v as usize)
}

/// Reads cgroup memory limit and calculates available memory.
///
/// Tries cgroup v2 first, then v1. Returns None if not in a cgroup or unlimited.
#[cfg(target_os = "linux")]
fn cgroup_available_memory() -> Option<usize> {
    cgroup_v2_available().or_else(cgroup_v1_available)
}

#[cfg(target_os = "linux")]
#[allow(clippy::cast_possible_truncation)]
fn cgroup_v2_available() -> Option<usize> {
    use std::fs;
    let max_content = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let memory_max = parse_memory_max(&max_content)?;
    let stat_content = fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    let net_used = parse_memory_stat_net_used(&stat_content)?;
    Some(memory_max.saturating_sub(net_used) as usize)
}

#[cfg(target_os = "linux")]
#[allow(clippy::cast_possible_truncation)]
fn cgroup_v1_available() -> Option<usize> {
    use std::fs;
    let limit = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
    let limit = parse_cgroup_v1_limit(&limit)?;
    let usage = fs::read_to_string("/sys/fs/cgroup/memory/memory.usage_in_bytes").ok()?;
    let usage: u64 = usage.trim().parse().ok()?;
    Some(limit.saturating_sub(usage) as usize)
}

/// Parses cgroup v1 limit. Returns None if unlimited (value >= 1 PB).
#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_v1_limit(content: &str) -> Option<u64> {
    const ONE_PETABYTE: u64 = 1_125_899_906_842_624;
    let limit: u64 = content.trim().parse().ok()?;
    // v1 uses a huge number for unlimited (near PAGE_COUNTER_MAX)
    if limit >= ONE_PETABYTE {
        return None;
    }
    Some(limit)
}

/// Parses memory.max content. Returns None if "max" (unlimited) or unparseable.
#[cfg(any(target_os = "linux", test))]
fn parse_memory_max(content: &str) -> Option<u64> {
    let trimmed = content.trim();
    if trimmed == "max" {
        return None;
    }
    trimmed.parse().ok()
}

/// Parses memory.stat and calculates net used memory.
///
/// net_used = anon + file + kernel + kernel_stack + pagetables + percpu + slab_unreclaimable - slab_reclaimable
#[cfg(any(target_os = "linux", test))]
fn parse_memory_stat_net_used(content: &str) -> Option<u64> {
    let mut anon = 0u64;
    let mut file = 0u64;
    let mut kernel = 0u64;
    let mut kernel_stack = 0u64;
    let mut pagetables = 0u64;
    let mut percpu = 0u64;
    let mut slab_reclaimable = 0u64;
    let mut slab_unreclaimable = 0u64;
    let mut found_any = false;

    for line in content.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            continue;
        };
        let Some(value_str) = parts.next() else {
            continue;
        };
        let Ok(value) = value_str.parse::<u64>() else {
            continue;
        };

        found_any = true;
        match key {
            "anon" => anon = value,
            "file" => file = value,
            "kernel" => kernel = value,
            "kernel_stack" => kernel_stack = value,
            "pagetables" => pagetables = value,
            "percpu" => percpu = value,
            "slab_reclaimable" => slab_reclaimable = value,
            "slab_unreclaimable" => slab_unreclaimable = value,
            _ => {}
        }
    }

    if !found_any {
        return None;
    }

    let total = anon + file + kernel + kernel_stack + pagetables + percpu + slab_unreclaimable;
    Some(total.saturating_sub(slab_reclaimable))
}

/// Sample actual rows from an `InputStrategy` to measure average in-memory Arrow row size.
///
/// Creates a throwaway `SessionContext` and streams batches until `target_rows` rows are
/// read (or EOF). Returns the average bytes per row based on
/// `RecordBatch::get_array_memory_size()`, which reflects the real in-memory footprint
/// including variable-width columns like strings and lists.
pub async fn sample_avg_row_bytes(
    input_strategy: &InputStrategy,
    target_rows: usize,
) -> anyhow::Result<usize> {
    let mut ctx = datafusion::prelude::SessionContext::new();
    let mut stream = input_strategy.as_stream(&mut ctx).await?;

    let mut total_bytes: usize = 0;
    let mut total_rows: usize = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_bytes += batch.get_array_memory_size();
        total_rows += batch.num_rows();
        if total_rows >= target_rows {
            break;
        }
    }

    if total_rows == 0 {
        return Ok(0);
    }
    Ok(total_bytes / total_rows)
}

const MIN_SORT_SPILL_RESERVATION: usize = 10 * 1024 * 1024; // 10MB (DataFusion default)

/// Estimate `sort_spill_reservation_bytes` from sampled row size and input characteristics.
///
/// The merge phase holds one batch per spill file in memory simultaneously.
/// We estimate spill file count from total in-memory input size vs per-partition memory
/// budget, then multiply by `batch_size * avg_row_bytes` to get the reservation.
///
/// `total_in_memory_bytes` should be computed from metadata row counts * avg_row_bytes,
/// NOT from on-disk file sizes (which would underestimate for compressed formats).
///
/// Clamped to [10MB, memory_per_partition / 2] so there's always room for the sorter
/// to accumulate batches before spilling. Returns `None` when input is insufficient
/// to produce a useful estimate (zero row bytes, zero budget, or budget too tight).
pub fn estimate_sort_spill_reservation(
    avg_row_bytes: usize,
    total_in_memory_bytes: usize,
    memory_per_partition: usize,
    batch_size: usize,
) -> Option<usize> {
    if avg_row_bytes == 0 || memory_per_partition == 0 {
        return None;
    }

    let max_reservation = memory_per_partition / 2;
    if max_reservation < MIN_SORT_SPILL_RESERVATION {
        return None;
    }

    let estimated_spill_files = total_in_memory_bytes
        .checked_div(memory_per_partition)
        .unwrap_or(1)
        .max(1);
    let merge_batch_bytes = batch_size.saturating_mul(avg_row_bytes);
    let reservation = estimated_spill_files.saturating_mul(merge_batch_bytes);

    Some(reservation.clamp(MIN_SORT_SPILL_RESERVATION, max_reservation))
}

/// Returns the exact byte size for fixed-width Arrow types, or `None` for variable-width types.
#[allow(clippy::cast_sign_loss)]
pub fn estimate_fixed_type_bytes(dt: &DataType) -> Option<usize> {
    match dt {
        DataType::Boolean => Some(1),
        DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Some(8),
        DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => Some(4),
        DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => Some(8),
        DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => Some(16),
        DataType::Time32(_) => Some(4),
        DataType::Decimal128(_, _) => Some(16),
        DataType::Decimal256(_, _) => Some(32),
        DataType::FixedSizeBinary(n) => Some((*n).max(0) as usize),
        DataType::FixedSizeList(f, n) => {
            estimate_fixed_type_bytes(f.data_type()).map(|size| size * (*n).max(0) as usize)
        }
        _ => None,
    }
}

/// Estimate bytes per row for a given Arrow schema.
///
/// Fixed-width types use their exact byte size. Variable-width types (Utf8, Binary, List, etc.)
/// use a conservative 16-byte estimate per value.
pub fn estimate_row_bytes(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|f| estimate_fixed_type_bytes(f.data_type()).unwrap_or(16))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_total_memory_returns_nonzero() {
        let mem = total_memory();
        assert!(mem > 0, "total memory should be > 0");
    }

    #[test]
    fn test_available_memory_returns_nonzero() {
        let mem = available_memory();
        assert!(mem > 0, "available memory should be > 0");
    }

    #[test]
    fn test_total_at_least_available() {
        let total = total_memory();
        let available = available_memory();
        assert!(
            total >= available,
            "total ({total}) should be >= available ({available})"
        );
    }

    #[test]
    fn test_parse_memory_max_with_limit() {
        assert_eq!(parse_memory_max("1073741824"), Some(1073741824));
        assert_eq!(parse_memory_max("1073741824\n"), Some(1073741824));
        assert_eq!(parse_memory_max("  1073741824  \n"), Some(1073741824));
    }

    #[test]
    fn test_parse_memory_max_unlimited() {
        assert_eq!(parse_memory_max("max"), None);
        assert_eq!(parse_memory_max("max\n"), None);
        assert_eq!(parse_memory_max("  max  "), None);
    }

    #[test]
    fn test_parse_memory_max_invalid() {
        assert_eq!(parse_memory_max(""), None);
        assert_eq!(parse_memory_max("not_a_number"), None);
        assert_eq!(parse_memory_max("-1"), None);
    }

    #[test]
    fn test_parse_memory_stat_basic() {
        let content = "anon 1000
file 2000
kernel 500
kernel_stack 100
pagetables 200
percpu 50
slab_reclaimable 300
slab_unreclaimable 150
other_field 999";

        let net_used = parse_memory_stat_net_used(content).unwrap();
        // 1000 + 2000 + 500 + 100 + 200 + 50 + 150 - 300 = 3700
        assert_eq!(net_used, 3700);
    }

    #[test]
    fn test_parse_memory_stat_missing_fields() {
        let content = "anon 1000\nfile 2000";
        let net_used = parse_memory_stat_net_used(content).unwrap();
        assert_eq!(net_used, 3000);
    }

    #[test]
    fn test_parse_memory_stat_empty() {
        assert_eq!(parse_memory_stat_net_used(""), None);
        assert_eq!(parse_memory_stat_net_used("   \n  \n  "), None);
    }

    #[test]
    fn test_parse_memory_stat_reclaimable_larger_than_used() {
        // edge case: slab_reclaimable > total (saturating_sub prevents underflow)
        let content = "anon 100\nslab_reclaimable 1000";
        let net_used = parse_memory_stat_net_used(content).unwrap();
        assert_eq!(net_used, 0);
    }

    #[test]
    fn test_parse_memory_stat_realistic() {
        let content = "anon 6011367424
file 4726673408
kernel 110718976
kernel_stack 6029312
pagetables 56123392
sec_pagetables 0
percpu 2448
sock 8192
vmalloc 65536
shmem 5246976
file_mapped 5242880
file_dirty 81920
file_writeback 0
swapcached 0
anon_thp 706740224
file_thp 0
shmem_thp 0
inactive_anon 6035795968
active_anon 16384
inactive_file 655872000
active_file 4065554432
unevictable 0
slab_reclaimable 44485560
slab_unreclaimable 3884784
slab 48370344";

        let net_used = parse_memory_stat_net_used(content).unwrap();
        let expected: u64 =
            6011367424 + 4726673408 + 110718976 + 6029312 + 56123392 + 2448 + 3884784 - 44485560;
        assert_eq!(net_used, expected);
    }

    #[test]
    fn test_parse_memory_stat_ignores_malformed_lines() {
        let content = "anon 1000
malformed_no_value
file 2000
also_bad
kernel 500";
        let net_used = parse_memory_stat_net_used(content).unwrap();
        assert_eq!(net_used, 3500);
    }

    #[test]
    fn test_parse_cgroup_v1_limit() {
        assert_eq!(parse_cgroup_v1_limit("1073741824\n"), Some(1073741824));
        assert_eq!(parse_cgroup_v1_limit("8589934592"), Some(8589934592)); // 8 GB
    }

    #[test]
    fn test_parse_cgroup_v1_limit_unlimited() {
        assert_eq!(parse_cgroup_v1_limit("9223372036854771712"), None);
        assert_eq!(parse_cgroup_v1_limit("9223372036854775807"), None);
    }

    #[test]
    fn test_parse_cgroup_v1_limit_invalid() {
        assert_eq!(parse_cgroup_v1_limit(""), None);
        assert_eq!(parse_cgroup_v1_limit("max"), None);
    }

    #[test]
    fn test_estimate_row_bytes_fixed_width() {
        use arrow::datatypes::Field;
        // 3 × Int32 + 1 × Int16 + 1 × Date32 = 12 + 2 + 4 = 18
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int16, false),
            Field::new("e", DataType::Date32, false),
        ]);
        assert_eq!(estimate_row_bytes(&schema), 18);
    }

    #[test]
    fn test_estimate_row_bytes_variable_width() {
        use arrow::datatypes::Field;
        // Utf8 = 16 (estimate), Int32 = 4
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        assert_eq!(estimate_row_bytes(&schema), 20);
    }

    #[test]
    fn test_estimate_row_bytes_empty_schema() {
        let schema = Schema::empty();
        assert_eq!(estimate_row_bytes(&schema), 0);
    }

    #[test]
    fn test_sort_spill_reservation_hits_floor() {
        // small input that would compute below 10MB
        let reservation = estimate_sort_spill_reservation(
            100,         // 100 bytes/row
            1_000_000,   // 1MB input
            100_000_000, // 100MB per partition
            8192,        // batch size
        );
        assert_eq!(reservation, Some(10 * 1024 * 1024)); // 10MB floor
    }

    #[test]
    fn test_sort_spill_reservation_scales_up() {
        // large input: 10GB input, 500MB per partition = ~20 spill files
        // 20 * 8192 * 200 = ~32MB
        let reservation = estimate_sort_spill_reservation(
            200,            // 200 bytes/row
            10_000_000_000, // 10GB input
            500_000_000,    // 500MB per partition
            8192,           // batch size
        )
        .unwrap();
        assert_eq!(reservation, 20 * 8192 * 200);
        assert!(reservation > 10 * 1024 * 1024); // above floor
        assert!(reservation < 250_000_000); // below ceiling (250MB)
    }

    #[test]
    fn test_sort_spill_reservation_hits_ceiling() {
        // huge input with wide rows: would exceed half of per-partition budget
        let memory_per_partition = 100_000_000; // 100MB
        let reservation = estimate_sort_spill_reservation(
            2000,            // 2KB/row (wide rows)
            100_000_000_000, // 100GB input
            memory_per_partition,
            8192,
        );
        assert_eq!(reservation, Some(memory_per_partition / 2)); // ceiling
    }

    #[test]
    fn test_sort_spill_reservation_zero_row_bytes() {
        let reservation = estimate_sort_spill_reservation(0, 1_000_000, 100_000_000, 8192);
        assert_eq!(reservation, None);
    }

    #[test]
    fn test_sort_spill_reservation_zero_memory_per_partition() {
        let reservation = estimate_sort_spill_reservation(100, 1_000_000, 0, 8192);
        assert_eq!(reservation, None);
    }

    #[test]
    fn test_sort_spill_reservation_tight_budget() {
        // memory_per_partition < 20MB means max_reservation < MIN_SORT_SPILL_RESERVATION
        let reservation = estimate_sort_spill_reservation(
            200,            // 200 bytes/row
            10_000_000_000, // 10GB input
            10_000_000,     // 10MB per partition
            8192,
        );
        assert_eq!(reservation, None);
    }

    #[tokio::test]
    async fn test_sample_avg_row_bytes_single_arrow() {
        use crate::sources::arrow::ArrowDataSource;
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.arrow");
        let batch = TestBatch::simple_with(&[1, 2, 3], &["hello", "world", "foo"]);
        TestFile::write_arrow_batch(&path, &batch);

        let source: Box<dyn crate::sources::data_source::DataSource> =
            Box::new(ArrowDataSource::new(path.to_string_lossy().to_string()));
        let strategy = InputStrategy::Single(source);

        let avg = sample_avg_row_bytes(&strategy, 100).await.unwrap();
        // i32 (4 bytes) + variable-length strings -- should be non-trivial
        assert!(avg > 0, "avg_row_bytes should be > 0, got {avg}");
    }

    #[tokio::test]
    async fn test_sample_avg_row_bytes_multiple_files() {
        use crate::sources::arrow::ArrowDataSource;
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();
        let path1 = dir.path().join("a.arrow");
        let path2 = dir.path().join("b.arrow");
        let batch1 = TestBatch::simple_with(&[1, 2], &["aa", "bb"]);
        let batch2 = TestBatch::simple_with(&[3, 4], &["cc", "dd"]);
        TestFile::write_arrow_batch(&path1, &batch1);
        TestFile::write_arrow_batch(&path2, &batch2);

        let sources: Vec<Box<dyn crate::sources::data_source::DataSource>> = vec![
            Box::new(ArrowDataSource::new(path1.to_string_lossy().to_string())),
            Box::new(ArrowDataSource::new(path2.to_string_lossy().to_string())),
        ];
        let strategy = InputStrategy::Multiple(sources);

        let avg = sample_avg_row_bytes(&strategy, 100).await.unwrap();
        assert!(avg > 0);
    }

    #[tokio::test]
    async fn test_sample_avg_row_bytes_parquet() {
        use crate::sources::parquet::ParquetDataSource;
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let batch = TestBatch::builder()
            .column_i32("id", &[1, 2, 3, 4, 5])
            .column_string("name", &["alpha", "bravo", "charlie", "delta", "echo"])
            .column_f64("score", &[1.0, 2.0, 3.0, 4.0, 5.0])
            .build();
        TestFile::write_parquet_batch(&path, &batch);

        let source: Box<dyn crate::sources::data_source::DataSource> =
            Box::new(ParquetDataSource::new(path.to_string_lossy().to_string()));
        let strategy = InputStrategy::Single(source);

        let avg = sample_avg_row_bytes(&strategy, 100).await.unwrap();
        assert!(avg > 0);
    }

    #[tokio::test]
    async fn test_sample_avg_row_bytes_respects_target_rows() {
        use crate::sources::arrow::ArrowDataSource;
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();

        // write 5 files with 100 rows each = 500 rows total
        let mut sources: Vec<Box<dyn crate::sources::data_source::DataSource>> = Vec::new();
        for i in 0..5 {
            let path = dir.path().join(format!("file_{i}.arrow"));
            let ids: Vec<i32> = (0..100).collect();
            let names: Vec<&str> = (0..100).map(|_| "test_value").collect();
            let batch = TestBatch::simple_with(&ids, &names);
            TestFile::write_arrow_batch(&path, &batch);
            sources.push(Box::new(ArrowDataSource::new(
                path.to_string_lossy().to_string(),
            )));
        }

        let strategy = InputStrategy::Multiple(sources);

        // target only 50 rows -- should still produce a valid average
        let avg = sample_avg_row_bytes(&strategy, 50).await.unwrap();
        assert!(avg > 0);
    }

    #[tokio::test]
    async fn test_sample_avg_row_bytes_wide_vs_narrow() {
        use crate::sources::arrow::ArrowDataSource;
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();

        // narrow: single i32 column
        let narrow_path = dir.path().join("narrow.arrow");
        let narrow_batch = TestBatch::builder().column_i32("x", &[1, 2, 3]).build();
        TestFile::write_arrow_batch(&narrow_path, &narrow_batch);

        let narrow_strategy = InputStrategy::Single(Box::new(ArrowDataSource::new(
            narrow_path.to_string_lossy().to_string(),
        )));
        let narrow_avg = sample_avg_row_bytes(&narrow_strategy, 100).await.unwrap();

        // wide: many columns including strings
        let wide_path = dir.path().join("wide.arrow");
        let wide_batch = TestBatch::builder()
            .column_i32("a", &[1, 2, 3])
            .column_i64("b", &[100, 200, 300])
            .column_f64("c", &[1.1, 2.2, 3.3])
            .column_string("d", &["a long string value", "another one here", "third"])
            .column_string("e", &["more text data", "for testing", "purposes"])
            .build();
        TestFile::write_arrow_batch(&wide_path, &wide_batch);

        let wide_strategy = InputStrategy::Single(Box::new(ArrowDataSource::new(
            wide_path.to_string_lossy().to_string(),
        )));
        let wide_avg = sample_avg_row_bytes(&wide_strategy, 100).await.unwrap();

        assert!(
            wide_avg > narrow_avg,
            "wide schema ({wide_avg} bytes/row) should be larger than narrow ({narrow_avg} bytes/row)"
        );
    }
}
