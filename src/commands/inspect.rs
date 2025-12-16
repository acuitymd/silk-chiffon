//! Inspect command for examining file metadata and structure.

use std::fs::File;
use std::io::{self, IsTerminal, Read, Seek, SeekFrom, Write};

use anyhow::{Result, anyhow};

use crate::{
    InspectArrowArgs, InspectIdentifyArgs, InspectParquetArgs, InspectSubcommand,
    InspectVortexArgs, OutputFormat,
    inspection::{
        arrow::ArrowInspector, detect_format, inspectable::Inspectable, parquet::ParquetInspector,
        vortex::VortexInspector,
    },
};

/// Resolve Auto format based on TTY detection
fn resolve_format(format: OutputFormat) -> OutputFormat {
    match format {
        OutputFormat::Auto => {
            if io::stdout().is_terminal() {
                OutputFormat::Text
            } else {
                OutputFormat::Json
            }
        }
        other => other,
    }
}

const ARROW_MAGIC: &[u8] = b"ARROW1";

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

    match resolve_format(args.format) {
        OutputFormat::Json => println!("{}", serde_json::to_string(&format.to_json())?),
        _ => println!("{}", format),
    }

    Ok(())
}

fn run_parquet(args: &InspectParquetArgs) -> Result<()> {
    let inspector = ParquetInspector::open(&args.file)
        .map_err(|e| anyhow!("Failed to open Parquet file: {}", e))?;

    if matches!(resolve_format(args.format), OutputFormat::Json) {
        let json = inspector.to_json();
        println!("{}", serde_json::to_string(&json)?);
        return Ok(());
    }

    let mut out = io::stdout();

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
    let mut file = File::open(&args.file).map_err(|e| anyhow!("Failed to open file: {}", e))?;

    let is_file_format = {
        let mut magic = [0u8; 6];
        if file.read_exact(&mut magic).is_ok() && magic == ARROW_MAGIC {
            if file.seek(SeekFrom::End(-6)).is_ok() {
                let mut footer_magic = [0u8; 6];
                file.read_exact(&mut footer_magic).is_ok() && footer_magic == ARROW_MAGIC
            } else {
                false
            }
        } else {
            false
        }
    };

    let inspector = if is_file_format {
        ArrowInspector::open_file(&args.file)
            .map_err(|e| anyhow!("Failed to open Arrow file: {}", e))?
    } else {
        let count_rows = args.row_count || args.batches;
        ArrowInspector::open_stream(&args.file, count_rows)
            .map_err(|e| anyhow!("Failed to open Arrow stream: {}", e))?
    };

    if matches!(resolve_format(args.format), OutputFormat::Json) {
        let json = inspector.to_json();
        println!("{}", serde_json::to_string(&json)?);
        return Ok(());
    }

    let mut out = io::stdout();

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

    if matches!(resolve_format(args.format), OutputFormat::Json) {
        let json = inspector.to_json();
        println!("{}", serde_json::to_string(&json)?);
        return Ok(());
    }

    let mut out = io::stdout();

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
