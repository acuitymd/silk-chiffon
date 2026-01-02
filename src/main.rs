use anyhow::Result;
use clap::Parser;
use silk_chiffon::{Cli, Commands, commands};

fn main() -> Result<()> {
    let cli = Cli::parse();

    // handle completions before anything else since it writes to stdout
    if let Commands::Completions { shell } = &cli.command {
        Commands::generate_completions(*shell);
        return Ok(());
    }

    // handle thread limit next so that it's set before any other operations
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = cli.threads {
        if threads == 0 {
            anyhow::bail!("--threads must be at least 1");
        }
        builder.worker_threads(threads);
    }
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
