use anyhow::Result;
use clap::Parser;
use silk_chiffon::{Cli, commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        silk_chiffon::Commands::Parquet(args) => commands::parquet::run(args).await?,
        silk_chiffon::Commands::Duckdb(args) => commands::duckdb::run(args).await?,
        silk_chiffon::Commands::Arrow(args) => commands::arrow::run(args).await?,
        silk_chiffon::Commands::SplitToArrow(args) => commands::split_to_arrow::run(args).await?,
        silk_chiffon::Commands::SplitToParquet(args) => {
            commands::split_to_parquet::run(args).await?
        }
    };
    Ok(())
}
