//! Parquet format definition for Silk Chiffon.
//!
//! This crate owns Parquet detection, DataFusion input preparation, output
//! encoding, inspection, and their CLI settings. Hosts compose the behavior
//! through [`definition`] rather than depending on codec internals.
//!
//! Binding a definition parses one command invocation's private settings.
//! Input creation delegates shared exact-file metadata work to
//! `FileInputGroup`, while output binding creates the command-scoped encoding and
//! writing runtimes shared by its sinks. Inspection reads the resolved input
//! object directly, so local and remote backends follow the same path.

mod args;
mod detection;
mod input;
mod inspection;
mod output;

use std::sync::Arc;

use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_core::{
    FileInputGroup, FormatDefinition, FormatFuture, InspectionDefinition, SinkBinding,
    SinkBindingConfig, TransformDefinition,
};

pub(crate) use args::{
    BloomFilterPolicy, ColumnEncoding, Compression, DEFAULT_BLOOM_FILTER_FPP,
    DefaultBloomFilterPolicy, DictionaryMode, DictionaryPolicy, Encoding, Statistics,
    WriterVersion,
};
#[cfg(test)]
pub(crate) use args::{BloomFilterSettings, ColumnBloomFilterPolicy};
use args::{InspectionArgs, TransformArgs};

/// Returns the immutable Parquet definition registered by a host application.
pub fn definition() -> FormatDefinition {
    FormatDefinition::builder("parquet", "Parquet")
        .extensions(["parquet"])
        .detector(detection::detect)
        .detection_priority(0)
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
    config: &'a SinkBindingConfig,
    args: &'a TransformArgs,
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    Box::pin(output::bind(config, args))
}

#[cfg(test)]
mod tests {
    use clap::{Arg, Command};

    use super::*;

    #[test]
    fn definition_exposes_the_complete_registered_contract() {
        let definition = definition();
        assert_eq!(definition.name(), "parquet");
        assert_eq!(definition.display_name(), "Parquet");
        assert_eq!(definition.extensions(), ["parquet"]);
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
        for argument in [
            "--parquet-compression",
            "--parquet-row-group-size",
            "--parquet-writing-threads",
        ] {
            assert!(help.contains(argument), "missing {argument}");
        }
        assert!(!help.contains("--parquet-io-threads"));
    }
}
