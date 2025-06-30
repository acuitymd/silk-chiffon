use anyhow::Result;
use clap::Parser;
use chiffon::{Cli, commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        chiffon::Commands::Parquet(args) => commands::parquet::run(args).await?,
        chiffon::Commands::Duckdb(args) => commands::duckdb::run(args).await?,
        chiffon::Commands::Arrow(args) => commands::arrow::run(args).await?,
    };
    Ok(())
}
