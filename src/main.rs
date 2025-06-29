use anyhow::Result;
use clap::Parser;
use daisy::{commands, Cli};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        daisy::Commands::Parquet(args) => commands::parquet::run(args).await?,
        daisy::Commands::Duckdb(args) => commands::duckdb::run(args).await?,
        daisy::Commands::Arrow(args) => commands::arrow::run(args).await?,
    };
    Ok(())
}