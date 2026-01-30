use anyhow::Result;
use clap::Parser;
use silk_chiffon::{Cli, Commands, commands, default_thread_budget};

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Commands::Completions { shell } = &cli.command {
        Commands::generate_completions(*shell);
        return Ok(());
    }

    let thread_budget = match &cli.command {
        Commands::Transform(args) => args
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
            Commands::Transform(args) => commands::transform::run(args).await?,
            Commands::Inspect(args) => commands::inspect::run(args.command).await?,
            Commands::Completions { .. } => unreachable!(),
        };
        Ok(())
    })
}
