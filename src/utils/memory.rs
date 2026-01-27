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
    // try cgroup detection on Linux (supports both v1 and v2)
    #[cfg(target_os = "linux")]
    if let Ok(available) = cgroup_memory::memory_available() {
        return available;
    }

    // fall back to system memory (macOS, bare metal, or no cgroup limits)
    System::new_all().available_memory() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_memory_returns_nonzero() {
        let mem = available_memory();
        assert!(mem > 0, "available memory should be > 0");
    }
}
