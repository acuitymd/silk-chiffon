use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

pub fn get_available_memory() -> u64 {
    let mut system = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
    );
    system.refresh_memory();
    system.available_memory()
}

pub fn get_available_cpu_cores() -> usize {
    let mut system =
        System::new_with_specifics(RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()));
    system.refresh_cpu_list(CpuRefreshKind::everything());
    system.cpus().len()
}
