use anyhow::Result;
use clap::Parser;
use silk_chiffon::{Cli, Commands, commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // handle completions before anything else since it writes to stdout
    if let Commands::Completions { shell } = &cli.command {
        Commands::generate_completions(*shell);
        return Ok(());
    }

    match cli.command {
        Commands::Transform(args) => commands::transform::run(args).await?,
        Commands::Inspect(args) => commands::inspect::run(args.command).await?,
        Commands::Completions { .. } => unreachable!(),
    };
    Ok(())
}
