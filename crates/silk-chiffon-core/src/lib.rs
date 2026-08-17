//! DataFusion-facing contracts used to compose Silk Chiffon.
//!
//! The crate separates four extension roles:
//!
//! - file formats contribute [`FormatDefinition`] values;
//! - non-file inputs contribute [`ServiceInputDefinition`] values;
//! - non-file outputs contribute [`ServiceOutputDefinition`] values;
//! - the host builds and executes a [`Pipeline`] from their table providers and sinks.
//!
//! Concrete formats, storage backends, and cloud connectors live in other crates. Their command
//! settings stay typed through definition and binding, then are erased behind private traits so a
//! host can compose unrelated implementations. See the repository's
//! [extension guide](https://github.com/acuitymd/silk-chiffon/blob/main/docs/extending.md) for the
//! complete lifecycles and examples.

mod data_sink;
mod exact_file_provider;
mod file_input;
mod format;
mod inspection;
mod pipeline;
mod schema;
mod service_input;
mod service_output;

pub use data_sink::{DataSink, SinkBinding, SinkCompletion};
pub use exact_file_provider::ExactFileTableProviderBuilder;
pub use file_input::{CanonicalInputUrl, FileInputGroup};
pub use format::{
    DetectedFormat, FormatDefinition, FormatDefinitionBuilder, FormatFuture, FormatInputVariant,
    FormatOperation, FormatOperationError, FormatRegistry, FormatRegistryBuilder,
    FormatRegistryError, InputDetection, InputDetectorFn, InputProviderFn, InspectionBinding,
    InspectionDefinition, InspectorFn, NullPlacement, OpenSinkMode, PresentationMode, SinkBinderFn,
    SinkBindingConfig, SortColumn, SortDirection, TransformBinding, TransformBindings,
    TransformDefinition, TransformDefinitionBuilder,
};
pub use inspection::InspectionOutput;
pub use pipeline::{
    Pipeline, PipelineExecution, PipelineExecutionStartError, PipelinePreparationError,
    PreparedPipeline, QueryDialect, SpillCompression, union_input_providers_by_name,
};
pub use schema::{schemas_match_ignoring_metadata, validate_batch_schema};
pub use service_input::{
    ServiceInputBinding, ServiceInputDefinition, ServiceInputDefinitionBuildError,
    ServiceInputDefinitionBuilder, ServiceInputProviderError, ServiceInputProviderFn,
};
pub use service_output::{
    ServiceOutputBinding, ServiceOutputConsumerFn, ServiceOutputConsumptionError,
    ServiceOutputDefinition, ServiceOutputDefinitionBuildError, ServiceOutputDefinitionBuilder,
};
