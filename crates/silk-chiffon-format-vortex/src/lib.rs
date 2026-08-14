//! Vortex format definition for Silk Chiffon.
//!
//! This crate owns Vortex detection, native DataFusion input preparation,
//! output encoding, inspection, and their CLI settings. Hosts compose that
//! behavior through [`definition`] rather than depending on codec internals.
//!
//! One bound transform retains a Vortex session shared by every Vortex input
//! leaf and output sink in that command. DataFusion's command session remains
//! independently responsible for stores, planning, and execution.

mod args;
mod detection;
mod input;
mod inspection;
mod output;
#[cfg(test)]
mod test_support;

use std::sync::Arc;

use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_core::{
    FormatDefinition, FormatFuture, InputLeaf, InspectionDefinition, SinkBinding,
    SinkBindingConfig, TransformDefinition,
};

use args::{InspectionArgs, TransformState};

/// Returns the immutable Vortex definition registered by a host application.
pub fn definition() -> FormatDefinition {
    FormatDefinition::builder("vortex", "Vortex")
        .extensions(["vortex"])
        .detector(detection::detect)
        .detection_priority(2)
        .transform(
            TransformDefinition::with_args::<TransformState>()
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
    leaf: &'a InputLeaf,
    session: &'a SessionContext,
    state: &'a TransformState,
) -> FormatFuture<'a, Arc<dyn TableProvider>> {
    Box::pin(input::create_provider(leaf, session, state.session()))
}

fn bind_output<'a>(
    config: &'a SinkBindingConfig,
    state: &'a TransformState,
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    Box::pin(output::bind(config, state))
}

#[cfg(test)]
mod tests {
    use clap::{Arg, Command};

    use super::*;

    #[test]
    fn definition_exposes_the_complete_registered_contract() {
        let definition = definition();
        assert_eq!(definition.name(), "vortex");
        assert_eq!(definition.display_name(), "Vortex");
        assert_eq!(definition.extensions(), ["vortex"]);
        assert!(definition.has_detector());
        assert!(definition.has_input_provider());
        assert!(definition.has_sink());
        assert!(definition.has_inspector());

        let registry = silk_chiffon_core::FormatRegistry::builder()
            .register(definition)
            .build()
            .unwrap();
        let help = registry
            .augment_transform_args(Command::new("test").arg(Arg::new("sort_by").long("sort-by")))
            .render_long_help()
            .to_string();
        assert!(help.contains("--vortex-record-batch-size"));
    }
}
