use anyhow::Result;
use mimalloc::MiMalloc;
use silk_chiffon::{Cli, Command, commands, default_thread_budget};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Command::Completions { shell } = &cli.command {
        Command::generate_completions(*shell);
        return Ok(());
    }

    let thread_budget = match &cli.command {
        Command::Transform(args) => args
            .thread_budget
            .as_ref()
            .map(|spec| spec.resolve())
            .unwrap_or_else(default_thread_budget),
        _ => std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4),
    };

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    builder.worker_threads(thread_budget);
    let runtime = builder.build()?;

    runtime.block_on(async {
        match cli.command {
            Command::Transform(args) => commands::transform::run(args).await?,
            Command::Detect(args) => commands::detect::run(args).await?,
            Command::Inspect(args) => commands::inspect::run(args).await?,
            Command::Completions { .. } => unreachable!(),
        };
        Ok(())
    })
}
