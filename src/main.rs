use anyhow::Result;
use clap::Parser;
use silk_chiffon::{Cli, Commands, commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::ArrowToParquet(args) => commands::arrow_to_parquet::run(args).await?,
        Commands::ArrowToDuckdb(args) => commands::arrow_to_duckdb::run(args).await?,
        Commands::ArrowToArrow(args) => commands::arrow_to_arrow::run(args).await?,
        Commands::PartitionArrowToArrow(args) => {
            commands::partition_arrow_to_arrow::run(args).await?
        }
        Commands::PartitionArrowToParquet(args) => {
            commands::partition_arrow_to_parquet::run(args).await?
        }
        Commands::MergeArrowToArrow(args) => commands::merge_arrow_to_arrow::run(args).await?,
        Commands::MergeArrowToParquet(args) => commands::merge_arrow_to_parquet::run(args).await?,
        Commands::MergeArrowToDuckdb(args) => commands::merge_arrow_to_duckdb::run(args).await?,
    };
    Ok(())
}
