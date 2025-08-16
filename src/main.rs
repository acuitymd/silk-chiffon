use anyhow::Result;
use clap::Parser;
use silk_chiffon::{Cli, Commands, commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Parquet(args) => commands::parquet::run(args).await?,
        Commands::Duckdb(args) => commands::duckdb::run(args).await?,
        Commands::Arrow(args) => commands::arrow::run(args).await?,
        Commands::SplitToArrow(args) => commands::split_to_arrow::run(args).await?,
        Commands::SplitToParquet(args) => commands::split_to_parquet::run(args).await?,
        Commands::MergeToArrow(args) => commands::merge_to_arrow::run(args).await?,
        Commands::MergeToParquet(args) => commands::merge_to_parquet::run(args).await?,
        Commands::MergeToDuckdb(args) => commands::merge_to_duckdb::run(args).await?,
    };
    Ok(())
}
