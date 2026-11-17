//! Typed command state for Vortex operations.

use anyhow::Result;
use clap::Args;
use vortex::{VortexSessionDefault, io::session::RuntimeSessionExt, session::VortexSession};

fn parse_positive(value: &str) -> Result<usize> {
    let value: usize = value.parse()?;
    if value == 0 {
        anyhow::bail!("value must be at least 1");
    }
    Ok(value)
}

#[derive(Args, Clone, Debug)]
#[group(id = "vortex_transform")]
pub(crate) struct TransformState {
    /// Vortex record batch size.
    #[arg(
        long,
        value_parser = parse_positive,
        help_heading = "Vortex Options"
    )]
    pub(crate) vortex_record_batch_size: Option<usize>,

    // The session's runtime adapter discovers the active Tokio runtime when
    // work executes, so binding this state does not capture the host runtime.
    #[arg(skip = VortexSession::default().with_tokio())]
    session: VortexSession,
}

impl TransformState {
    pub(crate) fn session(&self) -> &VortexSession {
        &self.session
    }
}

#[derive(Args, Clone, Debug)]
#[group(id = "vortex_inspection")]
pub(crate) struct InspectionArgs {
    /// Show full schema details.
    #[arg(long)]
    pub(crate) schema: bool,
    /// Show per-column statistics.
    #[arg(long)]
    pub(crate) stats: bool,
    /// Show layout structure.
    #[arg(long)]
    pub(crate) layout: bool,
}

#[cfg(test)]
mod tests {
    use clap::{Args, Command};

    use super::*;

    #[test]
    fn record_batch_size_rejects_zero_during_cli_parsing() {
        let command = TransformState::augment_args(Command::new("test"));
        let error = command
            .try_get_matches_from(["test", "--vortex-record-batch-size", "0"])
            .unwrap_err();
        assert!(error.to_string().contains("at least 1"));
    }

    #[test]
    fn operation_groups_have_explicit_ids() {
        let transform = TransformState::augment_args(Command::new("test"));
        assert!(
            transform
                .get_groups()
                .any(|group| group.get_id() == "vortex_transform")
        );
        let inspection = InspectionArgs::augment_args(Command::new("test"));
        assert!(
            inspection
                .get_groups()
                .any(|group| group.get_id() == "vortex_inspection")
        );
    }
}
