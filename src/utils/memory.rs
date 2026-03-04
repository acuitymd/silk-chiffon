//! Memory utilities: container-aware detection, budget planning, and sort spill estimation.
//!
//! Uses cgroup limits when running in a container (Linux), falls back to system
//! memory on macOS or when not containerized. Also provides [`BudgetPlan`] for
//! splitting a memory budget between DataFusion and sinks, and helpers for
//! computing sort spill reservations based on arrow-row encoding overhead.

use std::collections::HashMap;
use std::fmt;

use arrow::datatypes::{DataType, Schema};
use sysinfo::System;

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
/// use 32 bytes per value (offset (4 or 8 bytes depending on type) + average data). This is a
/// schema-only fallback — prefer sampled estimates from `row_size()` when data is available.
pub fn estimate_row_bytes(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|f| estimate_fixed_type_bytes(f.data_type()).unwrap_or(32))
        .sum()
}

/// Compute the row encoding overhead for arrow-row's RowConverter.
///
/// For multi-column sorts, DataFusion converts sort columns to a normalized row format
/// using arrow-row's RowConverter. This encoding is tracked by the memory pool during
/// the merge phase. The encoding size depends on column types:
///
/// - Fixed-width: 1 null byte + type_width (e.g. int32 → 5 bytes)
/// - Boolean: 2 bytes (1 null byte + 1 data byte)
/// - Variable-width: padded block encoding (see arrow-row's variable.rs)
///
/// Returns `(data_bytes_per_row, encoding_bytes_per_row)`. `data_bytes` covers the
/// full schema (the merge phase holds complete record batches), while `encoding_bytes`
/// covers only the sort columns (only those are converted to row format).
/// `avg_variable_sizes` provides sampled average byte sizes for variable-width columns;
/// any variable-width column not in the map defaults to 32 bytes.
pub fn sort_merge_row_overhead(
    schema: &Schema,
    sort_columns: &[String],
    avg_variable_sizes: &HashMap<String, usize>,
    row_bytes: usize,
) -> (usize, usize) {
    let data_bytes = row_bytes;

    // per-row offset in the Rows struct: one usize per row
    let mut encoding_bytes: usize = std::mem::size_of::<usize>();

    for col_name in sort_columns {
        let Some(field) = schema.field_with_name(col_name).ok() else {
            continue;
        };
        let dt = field.data_type();
        if let DataType::Boolean = dt {
            encoding_bytes += 2; // 1 null byte + 1 data byte
        } else if let Some(type_width) = estimate_fixed_type_bytes(dt) {
            encoding_bytes += 1 + type_width;
        } else {
            // variable-width: use arrow-row's padded block encoding formula
            let avg_len = avg_variable_sizes.get(col_name).copied().unwrap_or(32);
            encoding_bytes += arrow_row_variable_encoded_len(avg_len);
        }
    }

    (data_bytes, encoding_bytes)
}

/// Replicate arrow-row's padded_length formula for variable-width encoding.
/// See arrow-row/src/variable.rs: `padded_length(Some(len))`.
/// Validated against arrow-row 54.x (arrow crate ^57.2.0). Re-verify on major upgrades.
fn arrow_row_variable_encoded_len(value_len: usize) -> usize {
    const BLOCK_SIZE: usize = 32;
    const MINI_BLOCK_COUNT: usize = 4;
    const MINI_BLOCK_SIZE: usize = BLOCK_SIZE / MINI_BLOCK_COUNT; // 8

    if value_len <= BLOCK_SIZE {
        1 + value_len.div_ceil(MINI_BLOCK_SIZE) * (MINI_BLOCK_SIZE + 1)
    } else {
        MINI_BLOCK_COUNT + value_len.div_ceil(BLOCK_SIZE) * (BLOCK_SIZE + 1)
    }
}

/// Compute the sort_spill_reservation_bytes needed for a sort with the given
/// data and encoding overhead. Returns 0 if encoding adds no overhead.
///
/// During DataFusion's sort-and-spill merge phase, total tracked memory is:
///   batches × (1 + encoding_per_row / data_per_row)
/// The reservation must hold back enough pool space so the merge doesn't fill it.
///   reservation ≥ pool × encoding / (data + encoding)
/// We add 10% safety margin on top.
pub fn compute_sort_spill_reservation(
    pool_size: usize,
    data_bytes: usize,
    encoding_bytes: usize,
) -> usize {
    if data_bytes == 0 || encoding_bytes == 0 {
        return 0;
    }
    let total = data_bytes + encoding_bytes;
    // fraction = encoding / total, with 10% safety margin
    let reservation =
        (pool_size as u128 * encoding_bytes as u128 * 110 / (total as u128 * 100)) as usize;
    // don't reserve more than 80% of pool — need room for actual sort data
    reservation.min(pool_size * 80 / 100)
}

/// Encoding overhead factor (120% = 1.2×) for parquet encoding slots.
/// Encoding temporarily holds uncompressed + partially encoded data per slot.
pub const ENCODING_OVERHEAD_PCT: usize = 120;

/// Minimum pool memory per partition for productive sort work. Each partition's
/// ExternalSorter needs enough memory to accumulate data before spilling, and
/// the ExternalSorterMerge needs memory for the merge phase. During
/// sort_and_spill, the in-memory merge temporarily holds ~all accumulated data
/// (one batch per input stream), so each partition effectively needs its
/// full share of pool memory. 256MB ensures reasonable batch accumulation
/// before spilling.
const MIN_POOL_PER_PARTITION: usize = 256 * 1024 * 1024; // 256MB

/// Estimate the maximum number of sort partitions the DataFusion memory pool can sustain.
///
/// With GreedyMemoryPool, all consumers share the pool freely. The constraint is
/// that N partitions running concurrently will each accumulate ~pool/N data before
/// spilling. During the in-memory sort-merge-spill phase, each partition's merge
/// temporarily holds ~pool/N data. We cap partitions so each gets at least
/// MIN_POOL_PER_PARTITION of working memory.
pub fn estimate_max_sort_partitions(df_pool: usize) -> usize {
    (df_pool / MIN_POOL_PER_PARTITION).max(1)
}

/// What DataFusion will be doing — determines how much memory headroom it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    Passthrough,
    Active,
}

impl WorkloadKind {
    pub fn new(has_sort: bool, has_query: bool) -> Self {
        if has_sort || has_query {
            Self::Active
        } else {
            Self::Passthrough
        }
    }

    fn needs_df_headroom(self) -> bool {
        matches!(self, Self::Active)
    }

    fn df_floor(self, row_bytes: usize) -> usize {
        if self.needs_df_headroom() {
            // 10MB base for DF's per-partition spill reservation +
            // SORT_HEADROOM (10MB) for merge-phase temp buffers +
            // 4 batches worth of row data for actual sort working set
            10 * 1024 * 1024 + SORT_HEADROOM + 4 * ASSUMED_BATCH_SIZE * row_bytes
        } else {
            2 * ASSUMED_BATCH_SIZE * row_bytes + PASSTHROUGH_OVERHEAD
        }
    }
}

/// How the total memory budget is split between DataFusion and sinks.
///
/// Uses needs-based allocation: compute what the sink actually needs from the
/// schema and output format, give it that, give DataFusion all the remainder.
/// DataFusion has the least predictable memory usage (sort spill, query complexity)
/// so it gets breathing room rather than a fixed percentage.
///
/// Overhead reserve varies by workload: Active workloads need 15% because DataFusion's
/// in-memory sort merge temporarily holds ~100% of pool (one batch per input stream
/// for all in-memory batches), leaving non-pool memory (tokio, IPC spill writer,
/// OS buffers) to fit within the overhead. Passthrough workloads use 5%.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetPlan {
    pub total: usize,
    pub overhead_reserve: usize,
    pub datafusion_pool: usize,
    pub sink_budget: usize,
}

const OVERHEAD_RESERVE_PCT: usize = 5;
// sort workloads need more headroom: the in-memory sort merge phase uses ~100%
// of pool, so non-pool memory (tokio, IPC writer, OS) must fit in overhead.
const SORT_OVERHEAD_RESERVE_PCT: usize = 15;

// heuristic headroom constants — conservative estimates, not actual DF config values.
// tuned to avoid OOM in practice; see budget tests for validation.

const SORT_HEADROOM: usize = 10 * 1024 * 1024; // 10MB
const ASSUMED_BATCH_SIZE: usize = 8192;
const PASSTHROUGH_OVERHEAD: usize = 2 * 1024 * 1024; // 2MB
const MIN_SINK_ABSOLUTE: usize = 1024 * 1024; // 1MB

impl BudgetPlan {
    /// Split `total` bytes between overhead reserve, DataFusion, and sinks.
    ///
    /// Needs-based: sink gets `sink_needs`, DataFusion gets everything else
    /// (floored at a schema-aware minimum). When both floors exceed usable
    /// budget, that's intentional overcommit — the system may have swap space.
    pub fn new(total: usize, workload: WorkloadKind, row_bytes: usize, sink_needs: usize) -> Self {
        let overhead_pct = if workload.needs_df_headroom() {
            SORT_OVERHEAD_RESERVE_PCT
        } else {
            OVERHEAD_RESERVE_PCT
        };
        let overhead_reserve = total * overhead_pct / 100;
        let usable = total.saturating_sub(overhead_reserve);

        let df_floor = workload.df_floor(row_bytes);

        // give sink what it needs, DF gets the rest (floored)
        let datafusion_pool = usable.saturating_sub(sink_needs).max(df_floor);

        // if DF floor consumed more than expected, sink gets whatever's left
        let sink_budget = usable
            .saturating_sub(datafusion_pool)
            .max(MIN_SINK_ABSOLUTE);

        Self {
            total,
            overhead_reserve,
            datafusion_pool,
            sink_budget,
        }
    }
}

impl fmt::Display for BudgetPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use humansize::{BINARY, format_size};
        write!(
            f,
            "total={}, overhead={}, datafusion={}, sinks={}",
            format_size(self.total, BINARY),
            format_size(self.overhead_reserve, BINARY),
            format_size(self.datafusion_pool, BINARY),
            format_size(self.sink_budget, BINARY),
        )?;
        let committed = self.overhead_reserve + self.datafusion_pool + self.sink_budget;
        if committed > self.total {
            write!(f, " (overcommit)")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sinks::arrow::ArrowSinkOptions;
    use crate::sinks::parquet::ParquetSinkOptions;

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
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        assert_eq!(estimate_row_bytes(&schema), 36);
    }

    #[test]
    fn test_estimate_row_bytes_empty_schema() {
        let schema = Schema::empty();
        assert_eq!(estimate_row_bytes(&schema), 0);
    }

    #[test]
    fn test_budget_plan_generous_narrow_sort() {
        // 500MB, narrow schema (36 bytes/row), sort -- with default queues (no
        // user overrides), estimate uses 1 slot (concurrency only) so sink needs ~75MB
        let total = 500 * 1024 * 1024;
        let row_bytes = 36;
        let sink_needs = ParquetSinkOptions::new().estimate_sink_needs(row_bytes);
        let plan = BudgetPlan::new(total, WorkloadKind::Active, row_bytes, sink_needs);
        let overhead = total * 15 / 100;
        let usable = total - overhead;
        assert_eq!(plan.overhead_reserve, overhead);
        // sink gets exactly what it needs, DF gets the rest
        assert_eq!(plan.sink_budget, sink_needs);
        assert_eq!(plan.datafusion_pool, usable - sink_needs);
        assert_eq!(
            plan.overhead_reserve + plan.datafusion_pool + plan.sink_budget,
            total
        );
    }

    #[test]
    fn test_budget_plan_generous_wide_sort() {
        // 500MB, wide schema (260 bytes/row), sort -- with 1-slot estimate,
        // sink needs ~344MB which fits in usable (~425MB), DF gets the rest
        let total = 500 * 1024 * 1024;
        let row_bytes = 260;
        let sink_needs = ParquetSinkOptions::new().estimate_sink_needs(row_bytes);
        let plan = BudgetPlan::new(total, WorkloadKind::Active, row_bytes, sink_needs);
        let usable = total - plan.overhead_reserve;
        // sink fits within usable, so DF gets usable - sink_needs
        assert_eq!(plan.datafusion_pool, usable - sink_needs);
        assert_eq!(plan.sink_budget, sink_needs);
    }

    #[test]
    fn test_budget_plan_tiny_sort_overcommit() {
        // 5MB total with sort: DF floor exceeds usable, sink gets absolute minimum
        let total = 5 * 1024 * 1024;
        let row_bytes = 36;
        let sink_needs = ParquetSinkOptions::new().estimate_sink_needs(row_bytes);
        let plan = BudgetPlan::new(total, WorkloadKind::Active, row_bytes, sink_needs);
        let df_floor = WorkloadKind::Active.df_floor(row_bytes);
        assert_eq!(plan.datafusion_pool, df_floor);
        assert_eq!(plan.sink_budget, MIN_SINK_ABSOLUTE);
        assert!(
            plan.overhead_reserve + plan.datafusion_pool + plan.sink_budget > total,
            "tiny budget with sort should overcommit due to floors"
        );
    }

    #[test]
    fn test_budget_plan_tiny_no_sort() {
        let total = 1024 * 1024;
        let row_bytes = 36;
        let sink_needs = ParquetSinkOptions::new().estimate_sink_needs(row_bytes);
        let plan = BudgetPlan::new(total, WorkloadKind::Passthrough, row_bytes, sink_needs);
        let df_floor = WorkloadKind::Passthrough.df_floor(row_bytes);
        assert_eq!(plan.datafusion_pool, df_floor);
        assert_eq!(plan.sink_budget, MIN_SINK_ABSOLUTE);
    }

    #[test]
    fn test_budget_plan_zero() {
        let plan = BudgetPlan::new(0, WorkloadKind::Active, 36, 100 * 1024 * 1024);
        assert_eq!(plan.overhead_reserve, 0);
        let df_floor = WorkloadKind::Active.df_floor(36);
        assert_eq!(plan.datafusion_pool, df_floor);
        assert_eq!(plan.sink_budget, MIN_SINK_ABSOLUTE);
    }

    #[test]
    fn test_budget_plan_passthrough_uses_small_overhead() {
        let total = 200 * 1024 * 1024;
        let row_bytes = 36;
        let sink_needs = 20 * 1024 * 1024;
        let plan = BudgetPlan::new(total, WorkloadKind::Passthrough, row_bytes, sink_needs);
        // passthrough uses 5% overhead
        assert_eq!(plan.overhead_reserve, total * 5 / 100);
        assert_eq!(
            plan.overhead_reserve + plan.datafusion_pool + plan.sink_budget,
            total
        );
    }

    #[test]
    fn test_budget_plan_sort_uses_large_overhead() {
        // sort workloads get 15% overhead for the merge phase headroom
        let total = 1024 * 1024 * 1024; // 1GB
        let row_bytes = 36;
        let sink_needs = 100 * 1024 * 1024;
        let plan = BudgetPlan::new(total, WorkloadKind::Active, row_bytes, sink_needs);
        assert_eq!(plan.overhead_reserve, total * 15 / 100);
    }

    #[test]
    fn test_budget_plan_arrow_sink() {
        let total = 200 * 1024 * 1024;
        let row_bytes = 36;
        let sink_needs = ArrowSinkOptions::new().estimate_sink_needs(row_bytes);
        let plan = BudgetPlan::new(total, WorkloadKind::Passthrough, row_bytes, sink_needs);
        // arrow sink needs are tiny (~4.4MB), DF gets almost everything
        assert!(plan.datafusion_pool > 180 * 1024 * 1024);
        assert_eq!(plan.sink_budget, sink_needs);
    }

    #[test]
    fn test_budget_plan_display() {
        let plan = BudgetPlan::new(
            1024 * 1024 * 1024,
            WorkloadKind::Active,
            36,
            100 * 1024 * 1024,
        );
        let display = format!("{plan}");
        assert!(display.contains("total="));
        assert!(display.contains("overhead="));
        assert!(display.contains("datafusion="));
        assert!(display.contains("sinks="));
    }

    #[test]
    fn test_estimate_arrow_sink_needs() {
        let needs = ArrowSinkOptions::new().estimate_sink_needs(36);
        assert_eq!(needs, 36 * crate::sinks::DEFAULT_RECORD_BATCH_SIZE);
    }

    fn assert_estimate_within_4x(batch: &arrow::array::RecordBatch) {
        let estimated = estimate_row_bytes(&batch.schema());
        let actual = batch.get_array_memory_size() / batch.num_rows();
        assert!(
            estimated <= actual * 4,
            "estimate too high: estimated={estimated} actual={actual}"
        );
        assert!(
            estimated * 4 >= actual,
            "estimate too low: estimated={estimated} actual={actual}"
        );
    }

    #[test]
    fn test_estimate_vs_actual_narrow_schema() {
        assert_estimate_within_4x(&crate::utils::test_data::TestBatch::narrow(10_000));
    }

    #[test]
    fn test_estimate_vs_actual_wide_schema() {
        assert_estimate_within_4x(&crate::utils::test_data::TestBatch::wide(10_000));
    }

    #[test]
    fn test_estimate_accuracy_fixed_only() {
        use arrow::datatypes::Field;
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Float64, false),
        ]);
        assert_eq!(estimate_row_bytes(&schema), 20);
    }

    // -- estimate_max_sort_partitions tests --
    // formula: pool / 256MB (minimum per partition)

    #[test]
    fn test_sort_partitions_small_pool() {
        // 500MB pool → 500/256 = 1
        let pool = 500 * 1024 * 1024;
        let max = estimate_max_sort_partitions(pool);
        assert_eq!(max, 1);
    }

    #[test]
    fn test_sort_partitions_medium_pool() {
        // 1GB pool → 1024/256 = 4
        let pool = 1024 * 1024 * 1024;
        let max = estimate_max_sort_partitions(pool);
        assert_eq!(max, 4);
    }

    #[test]
    fn test_sort_partitions_always_at_least_one() {
        let max = estimate_max_sort_partitions(1024);
        assert_eq!(max, 1);
    }

    #[test]
    fn test_sort_partitions_large_pool() {
        // 27GB pool → 27*1024/256 = 108
        let pool = 27 * 1024 * 1024 * 1024;
        let max = estimate_max_sort_partitions(pool);
        assert_eq!(max, 108);
    }

    #[test]
    fn test_sort_partitions_2gb_pool() {
        let pool = 2 * 1024 * 1024 * 1024; // 2GB
        let max = estimate_max_sort_partitions(pool);
        assert_eq!(max, 8);
    }

    // -- arrow_row_variable_encoded_len tests --

    #[test]
    fn test_arrow_row_encoded_len_zero() {
        // 0 bytes: 1 header + ceil(0/8) * 9 = 1
        assert_eq!(arrow_row_variable_encoded_len(0), 1);
    }

    #[test]
    fn test_arrow_row_encoded_len_small() {
        // 8 bytes: 1 header + ceil(8/8) * 9 = 10
        assert_eq!(arrow_row_variable_encoded_len(8), 10);
    }

    #[test]
    fn test_arrow_row_encoded_len_boundary() {
        // 32 bytes (exactly BLOCK_SIZE): mini-block path
        // 1 + ceil(32/8) * 9 = 1 + 4*9 = 37
        assert_eq!(arrow_row_variable_encoded_len(32), 37);
    }

    #[test]
    fn test_arrow_row_encoded_len_above_boundary() {
        // 33 bytes: full-block path
        // 4 + ceil(33/32) * 33 = 4 + 2*33 = 70
        assert_eq!(arrow_row_variable_encoded_len(33), 70);
    }

    #[test]
    fn test_arrow_row_encoded_len_large() {
        // 64 bytes: 4 + ceil(64/32) * 33 = 4 + 2*33 = 70
        assert_eq!(arrow_row_variable_encoded_len(64), 70);
    }

    #[test]
    fn test_arrow_row_encoded_len_matches_actual_library() {
        use arrow::array::StringArray;
        use arrow::row::{RowConverter, SortField};
        use std::sync::Arc;

        // test several value lengths spanning both mini-block and full-block paths
        for &len in &[0usize, 5, 8, 10, 16, 32, 33, 50, 64, 100, 200] {
            let value: String = "x".repeat(len);
            let array = StringArray::from(vec![Some(value.as_str())]);
            let mut converter =
                RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
            let rows = converter
                .convert_columns(&[Arc::new(array)])
                .unwrap();

            let actual_encoded_len = rows.row(0).as_ref().len();
            let predicted = arrow_row_variable_encoded_len(len);

            assert_eq!(
                actual_encoded_len, predicted,
                "mismatch for value_len={len}: actual={actual_encoded_len}, predicted={predicted}"
            );
        }
    }

    // -- sort_merge_row_overhead tests --

    #[test]
    fn test_sort_merge_overhead_fixed_only() {
        use arrow::datatypes::Field;
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let (data, encoding) = sort_merge_row_overhead(&schema, &["a".into()], &HashMap::new(), 12);
        assert_eq!(data, 12); // 4 + 8
        // encoding: 8 (usize offset) + 1 (null) + 4 (i32) = 13
        assert_eq!(encoding, 13);
    }

    #[test]
    fn test_sort_merge_overhead_boolean() {
        use arrow::datatypes::Field;
        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, false)]);
        let (_, encoding) = sort_merge_row_overhead(&schema, &["flag".into()], &HashMap::new(), 1);
        // 8 (usize) + 2 (bool: 1 null + 1 data) = 10
        assert_eq!(encoding, 10);
    }

    #[test]
    fn test_sort_merge_overhead_variable_with_sizes() {
        use arrow::datatypes::Field;
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let mut sizes = HashMap::new();
        sizes.insert("name".to_string(), 10usize);
        let (data, encoding) = sort_merge_row_overhead(&schema, &["name".into()], &sizes, 14);
        assert_eq!(data, 14); // 4 (Int32) + 10 (sampled Utf8)
        // encoding: 8 (usize) + arrow_row_variable_encoded_len(10)
        // 10 bytes: 1 + ceil(10/8) * 9 = 1 + 2*9 = 19
        assert_eq!(encoding, 8 + 19);
    }

    // -- compute_sort_spill_reservation tests --

    #[test]
    fn test_spill_reservation_zero_inputs() {
        assert_eq!(compute_sort_spill_reservation(1_000_000, 0, 100), 0);
        assert_eq!(compute_sort_spill_reservation(1_000_000, 100, 0), 0);
        assert_eq!(compute_sort_spill_reservation(0, 100, 50), 0);
    }

    #[test]
    fn test_spill_reservation_normal() {
        // pool=1GB, data=100, encoding=50 → fraction = 50/150 = 1/3 × 1.1 = 36.67%
        let pool = 1024 * 1024 * 1024;
        let reservation = compute_sort_spill_reservation(pool, 100, 50);
        let expected = (pool as u128 * 50 * 110 / (150 * 100)) as usize;
        assert_eq!(reservation, expected);
        assert!(reservation < pool * 80 / 100);
    }

    #[test]
    fn test_spill_reservation_caps_at_80_pct() {
        // encoding >> data → fraction approaches 100%, should cap at 80%
        let pool = 100 * 1024 * 1024;
        let reservation = compute_sort_spill_reservation(pool, 1, 10000);
        assert_eq!(reservation, pool * 80 / 100);
    }
}
