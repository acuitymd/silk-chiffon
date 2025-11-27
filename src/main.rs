use anyhow::Result;
use clap::Parser;
use silk_chiffon::{Cli, Commands, commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Transform(args) => commands::transform::run(args).await?,
    };
    Ok(())
}
