//! Arrow IPC format definition for Silk Chiffon.
//!
//! The crate owns Arrow IPC file and stream detection, DataFusion input scanning, output encoding,
//! inspection, and their CLI settings. Hosts compose that behavior through [`definition`] rather
//! than depending on the private codec types or their command-scoped state.

mod args;
mod detection;
mod input;
mod inspection;
mod output;
mod variant;

use std::sync::Arc;

use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_core::{
    FileInputGroup, FormatDefinition, FormatFuture, InspectionDefinition, SinkBinding,
    SinkBindingConfig, TransformDefinition,
};

use args::{InspectionArgs, TransformArgs};

/// Returns the immutable Arrow IPC definition registered by a host application.
///
/// Each registry owns the same definition-time metadata and typed functions. Parsed settings,
/// provider state, and output bindings are still created independently for each host command.
pub fn definition() -> FormatDefinition {
    FormatDefinition::builder("arrow", "Arrow IPC")
        .extensions(["arrow", "arrows"])
        .detector(detection::detect)
        .detection_priority(1)
        .transform(
            TransformDefinition::with_args::<TransformArgs>()
                .input_provider(create_provider)
                .sink(bind_output)
                .build(),
        )
        .inspection(InspectionDefinition::with_args::<InspectionArgs>(
            inspection::inspect,
        ))
        .build()
}

fn create_provider<'a>(
    group: &'a FileInputGroup,
    session: &'a SessionContext,
    _args: &'a TransformArgs,
) -> FormatFuture<'a, Arc<dyn TableProvider>> {
    Box::pin(input::create_provider(group, session))
}

fn bind_output<'a>(
    _config: &'a SinkBindingConfig,
    args: &'a TransformArgs,
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    Box::pin(async move { Ok(Box::new(output::OutputBinding::new(args)) as Box<dyn SinkBinding>) })
}

#[cfg(test)]
mod tests {
    use clap::Command;

    use super::*;

    #[test]
    fn definition_exposes_the_complete_registered_contract() {
        let definition = definition();
        assert_eq!(definition.name(), "arrow");
        assert_eq!(definition.display_name(), "Arrow IPC");
        assert_eq!(definition.extensions(), ["arrow", "arrows"]);
        assert!(definition.has_detector());
        assert!(definition.has_input_provider());
        assert!(definition.has_sink());
        assert!(definition.has_inspector());

        let registry = silk_chiffon_core::FormatRegistry::builder()
            .register(definition)
            .build()
            .unwrap();
        let help = registry
            .augment_transform_args(Command::new("test"))
            .render_long_help()
            .to_string();
        for argument in [
            "--arrow-compression",
            "--arrow-format",
            "--arrow-record-batch-size",
            "--arrow-writing-queue-size",
        ] {
            assert!(help.contains(argument), "missing {argument}");
        }
    }
}
