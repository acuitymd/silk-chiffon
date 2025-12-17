//! Inspect command for examining file metadata and structure.

use std::io::{self, Write};

use anyhow::{Result, anyhow};

use crate::{
    InspectArrowArgs, InspectIdentifyArgs, InspectParquetArgs, InspectSubcommand,
    InspectVortexArgs,
    inspection::{
        arrow::ArrowInspector, detect_format, inspectable::Inspectable, parquet::ParquetInspector,
        vortex::VortexInspector,
    },
};

pub async fn run(command: InspectSubcommand) -> Result<()> {
    match &command {
        InspectSubcommand::Identify(args) => run_identify(args),
        InspectSubcommand::Parquet(args) => run_parquet(args),
        InspectSubcommand::Arrow(args) => run_arrow(args),
        InspectSubcommand::Vortex(args) => run_vortex(args),
    }
}

fn run_identify(args: &InspectIdentifyArgs) -> Result<()> {
    let format = detect_format(&args.file)?;

    if args.format.resolves_to_json() {
        println!("{}", serde_json::to_string(&format.to_json())?);
    } else {
        println!("{}", format);
    }

    Ok(())
}

fn run_parquet(args: &InspectParquetArgs) -> Result<()> {
    let inspector = ParquetInspector::open(&args.file)
        .map_err(|e| anyhow!("Failed to open Parquet file: {}", e))?;

    let mut out = io::stdout();

    if args.format.resolves_to_json() {
        inspector.render_to_json(&mut out)?;
        return Ok(());
    }

    inspector.render_default(&mut out)?;

    if args.schema {
        inspector.render_schema(&mut out)?;
    }

    if args.stats {
        inspector.render_stats(&mut out)?;
    }

    if args.row_groups {
        inspector.render_row_groups(&mut out, args.stats)?;
    }

    if args.metadata {
        inspector.render_metadata(&mut out)?;
    }

    out.flush()?;
    Ok(())
}

fn run_arrow(args: &InspectArrowArgs) -> Result<()> {
    let inspector = ArrowInspector::open(&args.file, args.row_count)
        .map_err(|e| anyhow!("Failed to open Arrow file: {}", e))?;

    let mut out = io::stdout();

    if args.format.resolves_to_json() {
        inspector.render_to_json(&mut out)?;
        return Ok(());
    }

    inspector.render_default(&mut out)?;

    if args.schema {
        inspector.render_schema(&mut out)?;
    }

    if args.batches {
        inspector.render_batches(&mut out)?;
    }

    if args.metadata {
        inspector.render_metadata(&mut out)?;
    }

    out.flush()?;
    Ok(())
}

fn run_vortex(args: &InspectVortexArgs) -> Result<()> {
    let inspector = VortexInspector::open_file(&args.file)
        .map_err(|e| anyhow!("Failed to open Vortex file: {}", e))?;

    let mut out = io::stdout();

    if args.format.resolves_to_json() {
        inspector.render_to_json(&mut out)?;
        return Ok(());
    }

    inspector.render_default(&mut out)?;

    if args.schema {
        inspector.render_schema(&mut out)?;
    }

    if args.stats {
        inspector.render_stats(&mut out)?;
    }

    if args.layout {
        inspector.render_layout(&mut out)?;
    }

    out.flush()?;
    Ok(())
}
