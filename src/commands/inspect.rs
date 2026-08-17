//! Inspect command for examining format-specific metadata.

use std::io::{self, Write};

use anyhow::Result;
use silk_chiffon_core::InspectionOutput;
use silk_chiffon_storage::LocationInput;

use crate::InspectCommand;

pub async fn run(command: InspectCommand) -> Result<()> {
    let (file, mode, inspection, storage) = command.into_parts();
    let location = LocationInput::parse(file.as_str())?;
    let handle = storage.input_handle(&location)?;
    let output = inspection.inspect(&handle, mode).await?;
    let mut stdout = io::stdout().lock();
    match output {
        InspectionOutput::Text(text) => stdout.write_all(text.as_bytes())?,
        InspectionOutput::Json(json) => writeln!(stdout, "{}", serde_json::to_string(&json)?)?,
    }
    stdout.flush()?;
    Ok(())
}
