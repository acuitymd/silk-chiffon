//! Inspect command for examining file metadata and structure.

use std::{
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{Result, anyhow};
use camino::{Utf8Path, Utf8PathBuf};
use silk_chiffon_storage::{LocationInput, StorageResolver};

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
    let file = resolve_local_path(&args.file)?;
    let format = detect_format(&file)?;

    if args.format.resolves_to_json() {
        println!("{}", serde_json::to_string(&format.to_json())?);
    } else {
        println!("{}", format);
    }

    Ok(())
}

fn run_parquet(args: &InspectParquetArgs) -> Result<()> {
    let file = resolve_local_path(&args.file)?;
    let inspector =
        ParquetInspector::open(&file).map_err(|e| anyhow!("Failed to open Parquet file: {}", e))?;

    let mut out = io::stdout();

    let columns_filter: Option<Vec<&str>> = args.pages.as_ref().and_then(|cols| {
        if cols.is_empty() {
            None
        } else {
            Some(cols.split(',').map(|s| s.trim()).collect())
        }
    });

    if args.format.resolves_to_json() {
        if args.pages.is_some() {
            let json = inspector.to_json_with_pages(columns_filter.as_deref());
            writeln!(out, "{}", serde_json::to_string(&json)?)?;
        } else {
            inspector.render_to_json(&mut out)?;
        }
    } else {
        inspector.render_with_row_group(&mut out, args.row_group)?;

        if args.pages.is_some() {
            inspector.render_pages(&mut out, args.row_group, columns_filter.as_deref())?;
        }
    }

    out.flush()?;
    Ok(())
}

fn run_arrow(args: &InspectArrowArgs) -> Result<()> {
    let file = resolve_local_path(&args.file)?;
    let inspector = ArrowInspector::open(&file, args.row_count || args.batches)
        .map_err(|e| anyhow!("Failed to open Arrow file: {}", e))?;

    let mut out = io::stdout();

    if args.format.resolves_to_json() {
        inspector.render_to_json(&mut out)?;
        return Ok(());
    }

    inspector.render_default(&mut out)?;

    if args.batches {
        inspector.render_batches(&mut out)?;
    }

    out.flush()?;
    Ok(())
}

fn run_vortex(args: &InspectVortexArgs) -> Result<()> {
    let file = resolve_local_path(&args.file)?;
    let inspector = VortexInspector::open_file(&file)
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

fn resolve_local_path(input: &Utf8Path) -> Result<Utf8PathBuf> {
    let location = LocationInput::parse(input.as_str())?;
    let resolved = StorageResolver::local()?.resolve_input(&location)?;
    Utf8PathBuf::from_path_buf(resolved.local_path()?)
        .map_err(|path: PathBuf| anyhow!("Local path is not valid UTF-8: {}", path.display()))
}
