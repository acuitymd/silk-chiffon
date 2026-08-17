use anyhow::Result;
use mimalloc::MiMalloc;
use silk_chiffon::{Cli, Command, commands};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
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
