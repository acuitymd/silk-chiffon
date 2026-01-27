//! Container-aware memory detection.
//!
//! Uses cgroup limits when running in a container (Linux), falls back to system
//! memory on macOS or when not containerized.

use sysinfo::System;

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

/// Reads cgroup v2 memory limit and calculates available memory.
///
/// Returns None if not running in a cgroup or if there's no memory limit.
#[cfg(target_os = "linux")]
#[allow(clippy::cast_possible_truncation)]
fn cgroup_available_memory() -> Option<usize> {
    use std::fs;

    const MEMORY_MAX: &str = "/sys/fs/cgroup/memory.max";
    const MEMORY_STAT: &str = "/sys/fs/cgroup/memory.stat";

    let max_content = fs::read_to_string(MEMORY_MAX).ok()?;
    let memory_max = parse_memory_max(&max_content)?;

    let stat_content = fs::read_to_string(MEMORY_STAT).ok()?;
    let net_used = parse_memory_stat_net_used(&stat_content)?;

    Some(memory_max.saturating_sub(net_used) as usize)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_memory_returns_nonzero() {
        let mem = available_memory();
        assert!(mem > 0, "available memory should be > 0");
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
}
