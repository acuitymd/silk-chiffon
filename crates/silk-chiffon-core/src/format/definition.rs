//! Immutable format definitions and command-scoped bindings.
//!
//! Definitions retain format metadata, CLI parsers, and typed functions. Binding parses one
//! command invocation's arguments and keeps those values paired with the functions that accept
//! them. See the [`super`] module for the complete lifecycle.

use std::{
    collections::HashMap, fmt, future::Future, marker::PhantomData, num::NonZeroUsize, pin::Pin,
    sync::Arc,
};

use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_storage::{InputObject, StorageHandle};
use thiserror::Error;

use super::binding;
use crate::{InspectionOutput, SinkBinding};

/// A boxed future returned by an asynchronous format function.
pub type FormatFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Examines an input and reports format-specific match details.
///
/// The registry supplies the canonical format name, so detector functions do not repeat it.
pub type InputDetectorFn = for<'a> fn(&'a InputObject) -> FormatFuture<'a, InputDetection>;

/// Creates one leaf provider from exact, homogeneous input objects and typed settings.
///
/// The format variant was identified once before grouping and is not rediscovered during schema
/// inference or scanning.
pub type InputProviderFn<T> = for<'a> fn(
    &'a [InputObject],
    &'a InputVariant,
    &'a SessionContext,
    &'a T,
) -> FormatFuture<'a, Arc<dyn TableProvider>>;

/// Creates command-scoped sink state from typed transform settings.
///
/// The returned [`SinkBinding`] can retain resources shared by every output sink opened during the
/// command.
pub type SinkBinderFn<T> =
    for<'a> fn(&'a SinkBindingConfig, &'a T) -> FormatFuture<'a, Box<dyn SinkBinding>>;

/// Inspects one input using typed inspection settings and the host-selected output mode.
pub type InspectorFn<T> =
    for<'a> fn(&'a StorageHandle, InspectionMode, &'a T) -> FormatFuture<'a, InspectionOutput>;

/// Host-owned execution settings used to bind a format's output behavior.
///
/// These values are known only after the final input plan and command-wide budgets have been
/// determined. They are passed once when the format creates its [`SinkBinding`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkBindingConfig {
    thread_budget: NonZeroUsize,
    sink_concurrency: SinkConcurrency,
    output_ordering: Vec<OutputOrderingColumn>,
}

impl SinkBindingConfig {
    /// Creates the format-neutral context supplied to a sink binder.
    pub fn new(
        thread_budget: NonZeroUsize,
        sink_concurrency: SinkConcurrency,
        output_ordering: Vec<OutputOrderingColumn>,
    ) -> Self {
        Self {
            thread_budget,
            sink_concurrency,
            output_ordering,
        }
    }

    /// Returns the command's thread budget for format-owned output work.
    pub const fn thread_budget(&self) -> NonZeroUsize {
        self.thread_budget
    }

    /// Returns whether the host may keep multiple output sinks open simultaneously.
    pub const fn sink_concurrency(&self) -> SinkConcurrency {
        self.sink_concurrency
    }

    /// Returns the order guaranteed within each output sink's input stream.
    pub fn output_ordering(&self) -> &[OutputOrderingColumn] {
        &self.output_ordering
    }
}

/// Whether an output strategy keeps one or several sinks open at a time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkConcurrency {
    /// The host keeps at most one output sink open.
    Sequential,
    /// The host may keep several output sinks open simultaneously.
    Concurrent,
}

/// One column in the order produced within each output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputOrderingColumn {
    name: String,
    direction: SortDirection,
}

impl OutputOrderingColumn {
    /// Describes one column in the order supplied to each output sink.
    pub fn new(name: impl Into<String>, direction: SortDirection) -> Self {
        Self {
            name: name.into(),
            direction,
        }
    }

    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the column's sort direction.
    pub const fn direction(&self) -> SortDirection {
        self.direction
    }
}

/// The direction of one column in an output ordering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    /// Values increase within the output.
    Ascending,
    /// Values decrease within the output.
    Descending,
}

/// The output representation selected by the host for an inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InspectionMode {
    /// Human-readable text selected by the host CLI.
    Text,
    /// Structured JSON selected by the host CLI.
    Json,
}

/// A format-specific container variant identified before leaf construction.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct InputVariant {
    name: Option<String>,
}

impl InputVariant {
    /// Describes a format with no more specific container variant.
    pub fn new() -> Self {
        Self::default()
    }

    /// Describes a recognized container variant.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
        }
    }

    /// Returns the container variant name, when the format distinguishes one.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

/// The bounded result of asking one format detector to identify an object.
#[derive(Debug)]
pub enum InputDetection {
    /// The object is not this format.
    Mismatch,
    /// The object is this format and has the supplied container variant.
    Match(InputVariant),
    /// The object is recognizably this format but is structurally malformed.
    Malformed(anyhow::Error),
}

/// A detection result paired with its definition's canonical name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetectedFormat {
    format: &'static str,
    variant: InputVariant,
}

impl DetectedFormat {
    /// Returns the canonical registered format name.
    pub fn format(&self) -> &'static str {
        self.format
    }

    /// Returns the format-specific variant reported by its detector.
    pub fn variant(&self) -> Option<&str> {
        self.variant.name()
    }

    /// Returns the bound container variant.
    pub fn input_variant(&self) -> &InputVariant {
        &self.variant
    }
}

/// A format capability that may be omitted from a definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormatOperation {
    /// Recognizing an input from its contents.
    Detection,
    /// Producing format-specific metadata output.
    Inspection,
    /// Creating a DataFusion input provider.
    InputProviderCreation,
    /// Creating command-scoped output state.
    SinkBinding,
}

impl fmt::Display for FormatOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Detection => "detection",
            Self::Inspection => "inspection",
            Self::InputProviderCreation => "input provider creation",
            Self::SinkBinding => "sink binding",
        })
    }
}

/// A missing or failed operation attributed to its format definition.
#[derive(Debug, Error)]
pub enum FormatOperationError {
    #[error("format {format} does not support {operation}")]
    Unsupported {
        format: &'static str,
        operation: FormatOperation,
    },
    #[error("{operation} failed for format {format}: {source}")]
    Failed {
        format: &'static str,
        operation: FormatOperation,
        #[source]
        source: anyhow::Error,
    },
    #[error("malformed {format} input {input}: {source}")]
    MalformedInput {
        format: &'static str,
        input: String,
        #[source]
        source: anyhow::Error,
    },
}

/// A format's transform CLI settings and optional input and output capabilities.
///
/// Input-provider creation and sink binding share the same parsed argument type so a format can
/// expose one coherent transform configuration. Either capability may be omitted.
#[derive(Clone)]
pub struct TransformDefinition {
    pub(super) definition: Arc<dyn binding::ErasedTransformDefinition>,
}

impl TransformDefinition {
    /// Starts a transform definition whose functions receive parsed `T` settings.
    pub fn with_args<T>() -> TransformDefinitionBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        TransformDefinitionBuilder {
            args: binding::ArgsParser::for_args(),
            input_provider: None,
            sink: None,
            settings: PhantomData,
        }
    }

    /// Starts a transform definition for a format with no transform-specific arguments.
    pub fn without_args() -> TransformDefinitionBuilder<()> {
        TransformDefinitionBuilder {
            args: binding::ArgsParser::unit(),
            input_provider: None,
            sink: None,
            settings: PhantomData,
        }
    }
}

/// Builds transform capabilities that share one concrete argument type.
///
/// Calling [`Self::build`] preserves whichever capabilities were supplied; transform definitions
/// may be input-only, sink-only, both, or neither.
pub struct TransformDefinitionBuilder<T> {
    args: binding::ArgsParser<T>,
    input_provider: Option<InputProviderFn<T>>,
    sink: Option<SinkBinderFn<T>>,
    settings: PhantomData<fn() -> T>,
}

impl<T> TransformDefinitionBuilder<T>
where
    T: Send + Sync + 'static,
{
    /// Adds the function that creates one homogeneous input provider.
    pub fn input_provider(mut self, input_provider: InputProviderFn<T>) -> Self {
        self.input_provider = Some(input_provider);
        self
    }

    /// Adds the function that creates command-scoped sink state.
    pub fn sink(mut self, sink: SinkBinderFn<T>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Completes the transform definition and erases its settings type as one typed unit.
    pub fn build(self) -> TransformDefinition {
        TransformDefinition {
            definition: Arc::new(binding::TypedTransformDefinition::new(
                self.args,
                self.input_provider,
                self.sink,
            )),
        }
    }
}

/// A format's inspection CLI settings and inspection function.
#[derive(Clone)]
pub struct InspectionDefinition {
    definition: Arc<dyn binding::ErasedInspectionDefinition>,
}

impl InspectionDefinition {
    /// Creates an inspection definition whose function receives parsed `T` settings.
    pub fn with_args<T>(inspector: InspectorFn<T>) -> Self
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        Self {
            definition: Arc::new(binding::TypedInspectionDefinition::new(
                binding::ArgsParser::for_args(),
                inspector,
            )),
        }
    }

    /// Creates an inspection definition with no format-specific arguments.
    pub fn without_args(inspector: InspectorFn<()>) -> Self {
        Self {
            definition: Arc::new(binding::TypedInspectionDefinition::new(
                binding::ArgsParser::unit(),
                inspector,
            )),
        }
    }
}

/// Immutable metadata and independently optional capabilities for one data format.
///
/// A format crate constructs this value and a host adds it to a [`super::FormatRegistry`]. The
/// definition exists before any command is parsed and contains no invocation-specific settings.
#[derive(Clone)]
pub struct FormatDefinition {
    pub(super) name: &'static str,
    pub(super) aliases: Vec<&'static str>,
    pub(super) extensions: Vec<&'static str>,
    pub(super) detection_priority: usize,
    pub(super) detector: Option<InputDetectorFn>,
    pub(super) transform: Option<TransformDefinition>,
    inspection: Option<InspectionDefinition>,
}

impl FormatDefinition {
    /// Starts a definition with its canonical registry name.
    pub fn builder(name: &'static str) -> FormatDefinitionBuilder {
        FormatDefinitionBuilder {
            definition: Self {
                name,
                aliases: Vec::new(),
                extensions: Vec::new(),
                detection_priority: usize::MAX,
                detector: None,
                transform: None,
                inspection: None,
            },
        }
    }

    /// Returns the canonical registry name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns alternate names accepted anywhere the registry accepts a format name.
    pub fn aliases(&self) -> &[&'static str] {
        &self.aliases
    }

    /// Returns filename extensions owned by this format, without requiring a leading dot.
    pub fn extensions(&self) -> &[&'static str] {
        &self.extensions
    }

    /// Reports whether the format can recognize inputs from their contents.
    pub fn has_detector(&self) -> bool {
        self.detector.is_some()
    }

    /// Reports whether the format can create input providers.
    pub fn has_input_provider(&self) -> bool {
        self.transform
            .as_ref()
            .is_some_and(|transform| transform.definition.has_input_provider())
    }

    /// Reports whether the format can bind output sinks.
    pub fn has_sink(&self) -> bool {
        self.transform
            .as_ref()
            .is_some_and(|transform| transform.definition.has_sink())
    }

    /// Reports whether the format can produce inspection output.
    pub fn has_inspector(&self) -> bool {
        self.inspection.is_some()
    }

    /// Runs this definition's detector and attaches its canonical format name.
    pub async fn detect(
        &self,
        object: &InputObject,
    ) -> Result<Option<DetectedFormat>, FormatOperationError> {
        let detector = self.detector.ok_or(FormatOperationError::Unsupported {
            format: self.name,
            operation: FormatOperation::Detection,
        })?;
        match detector(object)
            .await
            .map_err(|source| FormatOperationError::Failed {
                format: self.name,
                operation: FormatOperation::Detection,
                source,
            })? {
            InputDetection::Mismatch => Ok(None),
            InputDetection::Match(variant) => Ok(Some(DetectedFormat {
                format: self.name,
                variant,
            })),
            InputDetection::Malformed(source) => Err(FormatOperationError::MalformedInput {
                format: self.name,
                input: object.handle().url().to_string(),
                source,
            }),
        }
    }

    /// Adds this format's inspection arguments to a host-owned Clap command.
    pub fn augment_inspection_args(&self, command: Command) -> Command {
        match &self.inspection {
            Some(inspection) => inspection.definition.augment(command),
            None => command,
        }
    }

    /// Parses this format's inspection arguments for one command invocation.
    pub fn bind_inspection(&self, matches: &ArgMatches) -> Result<InspectionBinding, clap::Error> {
        let binding = self
            .inspection
            .as_ref()
            .map(|inspection| inspection.definition.bind(matches))
            .transpose()?;
        Ok(InspectionBinding {
            format: self.name,
            binding,
        })
    }
}

/// One format's inspection function bound to one invocation's parsed arguments.
pub struct InspectionBinding {
    format: &'static str,
    binding: Option<Arc<dyn binding::ErasedInspectionBinding>>,
}

impl InspectionBinding {
    /// Returns the canonical format name.
    pub fn format(&self) -> &'static str {
        self.format
    }

    /// Inspects one input using the arguments retained by this binding.
    pub async fn inspect(
        &self,
        handle: &StorageHandle,
        mode: InspectionMode,
    ) -> Result<InspectionOutput, FormatOperationError> {
        let binding = self
            .binding
            .as_ref()
            .ok_or(FormatOperationError::Unsupported {
                format: self.format,
                operation: FormatOperation::Inspection,
            })?;
        binding.inspect(self.format, handle, mode).await
    }
}

/// Builds one immutable format definition.
pub struct FormatDefinitionBuilder {
    definition: FormatDefinition,
}

impl FormatDefinitionBuilder {
    /// Adds alternate names for explicit format selection.
    pub fn aliases(mut self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        self.definition.aliases.extend(aliases);
        self
    }

    /// Claims filename extensions for input and output format selection.
    pub fn extensions(mut self, extensions: impl IntoIterator<Item = &'static str>) -> Self {
        self.definition.extensions.extend(extensions);
        self
    }

    /// Adds content-based detection and makes the format eligible for registry detection.
    pub fn detector(mut self, detector: InputDetectorFn) -> Self {
        self.definition.detector = Some(detector);
        self
    }

    /// Sets the detector's order relative to other registered formats.
    ///
    /// Lower values run first. Formats with equal priorities retain registration order.
    pub fn detection_priority(mut self, priority: usize) -> Self {
        self.definition.detection_priority = priority;
        self
    }

    /// Adds transform CLI settings and input-provider or sink capabilities.
    pub fn transform(mut self, transform: TransformDefinition) -> Self {
        self.definition.transform = Some(transform);
        self
    }

    /// Adds format-specific inspection CLI settings and behavior.
    pub fn inspection(mut self, inspection: InspectionDefinition) -> Self {
        self.definition.inspection = Some(inspection);
        self
    }

    /// Completes the definition without performing cross-format validation.
    ///
    /// [`super::FormatRegistryBuilder::build`] validates conflicts after all definitions have been
    /// registered.
    pub fn build(self) -> FormatDefinition {
        self.definition
    }
}

/// One format's input-provider and sink functions bound to one invocation's transform arguments.
pub struct TransformBinding {
    pub(super) format: &'static str,
    pub(super) detector: Option<InputDetectorFn>,
    pub(super) binding: Arc<dyn binding::ErasedTransformBinding>,
}

impl TransformBinding {
    /// Returns the canonical format name.
    pub fn format(&self) -> &'static str {
        self.format
    }

    /// Reports whether this binding can create input providers.
    pub fn has_input_provider(&self) -> bool {
        self.binding.has_input_provider()
    }

    /// Reports whether this binding can recognize inputs from their contents.
    pub fn has_detector(&self) -> bool {
        self.detector.is_some()
    }

    /// Reports whether this binding can create command-scoped sink state.
    pub fn has_sink(&self) -> bool {
        self.binding.has_sink()
    }

    /// Runs this binding's detector and retains the already-bound transform settings.
    pub async fn detect(
        &self,
        object: &InputObject,
    ) -> Result<Option<InputVariant>, FormatOperationError> {
        let detector = self.detector.ok_or(FormatOperationError::Unsupported {
            format: self.format,
            operation: FormatOperation::Detection,
        })?;
        match detector(object)
            .await
            .map_err(|source| FormatOperationError::Failed {
                format: self.format,
                operation: FormatOperation::Detection,
                source,
            })? {
            InputDetection::Mismatch => Ok(None),
            InputDetection::Match(variant) => Ok(Some(variant)),
            InputDetection::Malformed(source) => Err(FormatOperationError::MalformedInput {
                format: self.format,
                input: object.handle().url().to_string(),
                source,
            }),
        }
    }

    /// Creates one homogeneous input provider using this binding's parsed settings.
    pub async fn create_input_provider(
        &self,
        objects: &[InputObject],
        variant: &InputVariant,
        session: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>, FormatOperationError> {
        self.binding
            .create_input_provider(self.format, objects, variant, session)
            .await
    }

    /// Creates command-scoped sink state using this binding's parsed settings.
    pub async fn bind_sink(
        &self,
        context: &SinkBindingConfig,
    ) -> Result<Box<dyn SinkBinding>, FormatOperationError> {
        self.binding.bind_sink(self.format, context).await
    }
}

/// Transform bindings and lookup indexes for one command invocation.
///
/// A [`super::FormatRegistry`] creates this collection after the host has parsed its composed Clap
/// command. Every entry retains its own concrete settings internally.
pub struct TransformBindings {
    pub(super) bindings: Vec<TransformBinding>,
    pub(super) names: HashMap<String, usize>,
    pub(super) extensions: HashMap<String, usize>,
    pub(super) detection_order: Vec<usize>,
}

impl TransformBindings {
    /// Iterates over formats that contributed transform settings or capabilities.
    pub fn formats(&self) -> impl Iterator<Item = &TransformBinding> {
        self.bindings.iter()
    }

    /// Looks up a binding by canonical name or alias, ignoring ASCII case.
    pub fn get(&self, name_or_alias: &str) -> Option<&TransformBinding> {
        self.names
            .get(&name_or_alias.to_ascii_lowercase())
            .map(|index| &self.bindings[*index])
    }

    /// Looks up a binding by filename extension, with or without a leading dot.
    pub fn by_extension(&self, extension: &str) -> Option<&TransformBinding> {
        self.extensions
            .get(&extension.trim_start_matches('.').to_ascii_lowercase())
            .map(|index| &self.bindings[*index])
    }

    /// Detects an input, trying its extension owner before the normal detector order.
    pub async fn detect(
        &self,
        object: &InputObject,
    ) -> Result<Option<(&TransformBinding, InputVariant)>, FormatOperationError> {
        let preferred = object
            .handle()
            .object_path()
            .extension()
            .and_then(|extension| {
                self.extensions
                    .get(&extension.to_ascii_lowercase())
                    .copied()
            });
        if let Some(index) = preferred
            && self.bindings[index].detector.is_some()
            && self.bindings[index].has_input_provider()
            && let Some(variant) = self.bindings[index].detect(object).await?
        {
            return Ok(Some((&self.bindings[index], variant)));
        }
        for &index in &self.detection_order {
            if Some(index) == preferred {
                continue;
            }
            if let Some(variant) = self.bindings[index].detect(object).await? {
                return Ok(Some((&self.bindings[index], variant)));
            }
        }
        Ok(None)
    }
}
