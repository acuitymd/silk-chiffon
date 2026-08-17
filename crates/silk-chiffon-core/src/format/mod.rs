//! Extensible data-format definitions and their command-scoped bindings.
//!
//! A [`FormatDefinition`] is immutable metadata and functionality contributed by one format crate.
//! Detection, input-provider creation, sink binding, and inspection are independent capabilities.
//! The [`FormatRegistry`] validates definitions and indexes names, aliases, extensions, and
//! detection order without depending on concrete formats.
//!
//! Format crates may contribute ordinary [`clap::Args`] types to transform and inspection
//! commands. The private binding layer keeps each parsed argument type paired with the functions
//! that accept it. This lets one registry hold formats with unrelated settings types while each
//! format retains a strongly typed contract. Callers never store settings as [`std::any::Any`] or
//! downcast them.
//!
//! Binding happens once per command invocation. A [`TransformBinding`] couples one format's parsed
//! transform settings to its input-provider and sink functions. A [`SinkBinding`] may then retain
//! shared resources while opening multiple output sinks.

mod binding;
mod definition;
mod registry;

pub use definition::{
    DetectedFormat, FormatDefinition, FormatDefinitionBuilder, FormatFuture, FormatOperation,
    FormatOperationError, InputDetection, InputDetectorFn, InputProviderFn, InputVariant,
    InspectionBinding, InspectionDefinition, InspectionMode, InspectorFn, OpenSinkMode,
    OutputOrderingColumn, SinkBinderFn, SinkBindingConfig, SortDirection, TransformBinding,
    TransformBindings, TransformDefinition, TransformDefinitionBuilder,
};
pub use registry::{FormatRegistry, FormatRegistryBuilder, FormatRegistryError};
