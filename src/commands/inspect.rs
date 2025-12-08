use crate::InspectCommand;
use crate::inspect::{
    FileInspector,
    arrow_ipc::ArrowIpcInspector,
    output::{OutputFormatter, text::TextFormatter},
};
use anyhow::{Context, Result};

pub async fn execute(cmd: &InspectCommand) -> Result<()> {
    let inspector = ArrowIpcInspector;
    let file_info = inspector
        .inspect(&cmd.file)
        .context("failed to inspect file")?;

    let formatter = TextFormatter;
    let output = formatter.format(&file_info)?;
    println!("{}", output);

    Ok(())
}
