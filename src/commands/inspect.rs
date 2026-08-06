//! Inspect command for examining file metadata and structure.

use std::io::{self, Write};

use anyhow::{Result, anyhow};
use silk_chiffon_core::{FormatRegistry, IdentifiedFormat, InspectionOutput};
use silk_chiffon_storage::{LocationInput, StorageHandle};

use crate::{
    InspectCommand, InspectSubcommand, OutputFormat,
    inspection::style::{dim, value},
};

pub async fn run(command: InspectCommand) -> Result<()> {
    let (command, inspection, storage, formats) = command.into_parts();
    match command {
        InspectSubcommand::Identify(args) => {
            let location = LocationInput::parse(args.file.as_str())?;
            let handle = storage.input_handle(&location)?;
            run_identify(&handle, args.format, &formats).await
        }
        InspectSubcommand::Parquet(args) => run_inspection(&args.file, inspection, &storage).await,
        InspectSubcommand::Arrow(args) => run_inspection(&args.file, inspection, &storage).await,
        InspectSubcommand::Vortex(args) => run_inspection(&args.file, inspection, &storage).await,
    }
}

async fn run_identify(
    handle: &StorageHandle,
    output_format: OutputFormat,
    formats: &FormatRegistry,
) -> Result<()> {
    let mut identified = None;
    for registration in formats.identifiers() {
        if let Some(result) = registration.identify(handle).await? {
            identified = Some(result);
            break;
        }
    }

    if output_format.resolves_to_json() {
        let output = match &identified {
            Some(result) => {
                let mut object = serde_json::Map::new();
                object.insert("format".to_owned(), result.format().into());
                if let Some(variant) = result.variant() {
                    object.insert("variant".to_owned(), variant.into());
                }
                serde_json::Value::Object(object)
            }
            None => serde_json::json!({ "format": "unknown" }),
        };
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!("{}", identification_text(identified.as_ref()));
    }

    Ok(())
}

async fn run_inspection(
    file: &camino::Utf8Path,
    inspection: Option<silk_chiffon_core::ConfiguredInspection>,
    storage: &silk_chiffon_storage::StorageSession,
) -> Result<()> {
    let location = LocationInput::parse(file.as_str())?;
    let handle = storage.input_handle(&location)?;
    let output = inspection
        .ok_or_else(|| anyhow!("inspection capability is unavailable"))?
        .inspect(&handle)
        .await?;
    let mut stdout = io::stdout().lock();
    match output {
        InspectionOutput::Text(text) => stdout.write_all(text.as_bytes())?,
        InspectionOutput::Json(json) => writeln!(stdout, "{}", serde_json::to_string(&json)?)?,
    }
    stdout.flush()?;
    Ok(())
}

fn identification_text(identified: Option<&IdentifiedFormat>) -> String {
    let Some(identified) = identified else {
        return dim("Unknown");
    };
    let name = if identified.format() == "arrow" {
        "Arrow IPC".to_owned()
    } else {
        title_case(identified.format())
    };
    match identified.variant() {
        Some(variant) => format!("{} {}", value(name), dim(format!("({variant})"))),
        None => value(name),
    }
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}
