use anyhow::Result;
use mimalloc::MiMalloc;
use silk_chiffon::{Cli, Command};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    #[cfg(unix)]
    raise_open_file_limit();

    let cli = Cli::parse();

    if let Command::Completions { shell } = &cli.command {
        Command::generate_completions(*shell);
        return Ok(());
    }

    let thread_budget = cli.command.runtime_worker_threads();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    builder.worker_threads(thread_budget);
    let runtime = builder.build()?;

    runtime.block_on(cli.command.execute())
}

/// Raises the soft limit on open files to the hard limit, leaving it as it is
/// where that fails.
///
/// The parallel sort keeps a quarter of the soft limit in spill files, up to
/// 4,096, and merges its smallest flushes early to stay under that. The usual
/// soft limit of 1,024 makes that 256, which a sort of 24 threads reaches at
/// its tenth flush, while the hard limit on most hosts is far higher.
#[cfg(unix)]
fn raise_open_file_limit() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    // what macOS takes at most when the hard limit is unlimited: OPEN_MAX
    const MACOS_MOST: u64 = 10_240;

    let limit = getrlimit(Resource::Nofile);
    let raised = |current| Rlimit {
        current,
        maximum: limit.maximum,
    };
    if limit.current != limit.maximum
        && setrlimit(Resource::Nofile, raised(limit.maximum)).is_err()
        && limit.current.is_some_and(|current| current < MACOS_MOST)
    {
        let _ = setrlimit(Resource::Nofile, raised(Some(MACOS_MOST)));
    }
}
