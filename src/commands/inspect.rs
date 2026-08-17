//! Inspect command for examining format-specific metadata.

use std::io::{self, Write};

use crate::InspectCommand;
use anyhow::Result;
use silk_chiffon_core::InspectionOutput;

pub(crate) async fn run(command: InspectCommand) -> Result<()> {
    let (input, mode, inspection, storage) = command.into_parts();
    let object = storage.lookup_input(&input).await?;
    let output = inspection.inspect(&object, mode).await?;
    let mut stdout = io::stdout().lock();
    match output {
        InspectionOutput::Text(text) => stdout.write_all(text.as_bytes())?,
        InspectionOutput::Json(json) => writeln!(stdout, "{}", serde_json::to_string(&json)?)?,
    }
    stdout.flush()?;
    Ok(())
}
