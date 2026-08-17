//! Data-format registration contracts.
//!
//! A format contributes ordinary [`clap::Args`] types for each CLI scope it supports. Format-owned long options follow the `--{format}-...` convention, such as `--parquet-row-group-size`. Shared or global arguments may remain unprefixed. Registry construction rejects colliding transform argument IDs, long names, and short names.
//!
//! Parsed arguments remain bound to their format callbacks, so callers cannot pair one format's settings with another format. Each callback returns a boxed `Send` future. Identification has no CLI settings. Source and sink callbacks share one transform argument type, while inspection may register a different argument type.
//!
//! The sink callback creates one command-scoped [`DataSinkFactory`] that can retain state across every output sink.

mod binding;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
};

use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use silk_chiffon_storage::StorageHandle;
use thiserror::Error;

use crate::{DataSinkFactory, DataSource, InspectionOutput};

/// A `Send` future returned by a format callback.
pub type FormatFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Identifies a matching format without naming that format centrally.
pub type Identifier = for<'a> fn(&'a StorageHandle) -> FormatFuture<'a, Option<Identification>>;

/// Creates a source from transform settings registered as `T`.
pub type SourceFactory<T> =
    for<'a> fn(&'a StorageHandle, &'a T) -> FormatFuture<'a, Box<dyn DataSource>>;

/// Creates a command-scoped sink factory from transform settings registered as `T`.
pub type SinkFactory<T> =
    for<'a> fn(&'a SinkFactoryContext, &'a T) -> FormatFuture<'a, Box<dyn DataSinkFactory>>;

/// Produces inspection output from inspection settings registered as `T`.
pub type Inspector<T> = for<'a> fn(&'a StorageHandle, &'a T) -> FormatFuture<'a, InspectionOutput>;

/// Format-neutral execution settings needed when configuring output sinks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkFactoryContext {
    thread_budget: NonZeroUsize,
    pipeline_sorts: bool,
    output_ordering: Vec<OutputSortColumn>,
}

impl SinkFactoryContext {
    pub fn new(
        thread_budget: NonZeroUsize,
        pipeline_sorts: bool,
        output_ordering: Vec<OutputSortColumn>,
    ) -> Self {
        Self {
            thread_budget,
            pipeline_sorts,
            output_ordering,
        }
    }

    pub const fn thread_budget(&self) -> NonZeroUsize {
        self.thread_budget
    }

    pub const fn pipeline_sorts(&self) -> bool {
        self.pipeline_sorts
    }

    pub fn output_ordering(&self) -> &[OutputSortColumn] {
        &self.output_ordering
    }
}

/// One column in the order produced within each output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputSortColumn {
    name: String,
    direction: SortDirection,
}

impl OutputSortColumn {
    pub fn new(name: impl Into<String>, direction: SortDirection) -> Self {
        Self {
            name: name.into(),
            direction,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn direction(&self) -> SortDirection {
        self.direction
    }
}

/// The direction of one column in an output ordering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Format-specific details found by an identifier.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Identification {
    variant: Option<String>,
}

impl Identification {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_variant(variant: impl Into<String>) -> Self {
        Self {
            variant: Some(variant.into()),
        }
    }

    pub fn variant(&self) -> Option<&str> {
        self.variant.as_deref()
    }
}

/// An identification result paired with its registration's canonical name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentifiedFormat {
    format: &'static str,
    variant: Option<String>,
}

impl IdentifiedFormat {
    pub fn format(&self) -> &'static str {
        self.format
    }

    pub fn variant(&self) -> Option<&str> {
        self.variant.as_deref()
    }
}

/// One independently invocable format capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormatCapability {
    Identification,
    Inspection,
    Source,
    Sink,
}

impl fmt::Display for FormatCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Identification => "identification",
            Self::Inspection => "inspection",
            Self::Source => "source",
            Self::Sink => "sink",
        })
    }
}

/// A format invocation failure detected at the registration boundary.
#[derive(Debug, Error)]
pub enum FormatInvocationError {
    #[error("{capability} capability is unavailable for format: {format}")]
    CapabilityUnavailable {
        format: &'static str,
        capability: FormatCapability,
    },
    #[error("{capability} capability failed for format {format}: {source}")]
    CallbackFailed {
        format: &'static str,
        capability: FormatCapability,
        #[source]
        source: anyhow::Error,
    },
}

/// A format's transform CLI contribution and optional source and sink factories.
#[derive(Clone)]
pub struct FormatTransform {
    binder: Arc<dyn binding::BindTransform>,
}

impl FormatTransform {
    pub fn with_args<T>() -> FormatTransformBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        FormatTransformBuilder {
            args: binding::ArgsParser::for_args(),
            source: None,
            sink: None,
            settings: PhantomData,
        }
    }

    pub fn without_args() -> FormatTransformBuilder<()> {
        FormatTransformBuilder {
            args: binding::ArgsParser::unit(),
            source: None,
            sink: None,
            settings: PhantomData,
        }
    }
}

/// Builds transform capabilities that share one concrete argument type.
pub struct FormatTransformBuilder<T> {
    args: binding::ArgsParser<T>,
    source: Option<SourceFactory<T>>,
    sink: Option<SinkFactory<T>>,
    settings: PhantomData<fn() -> T>,
}

impl<T> FormatTransformBuilder<T>
where
    T: Send + Sync + 'static,
{
    pub fn source(mut self, source: SourceFactory<T>) -> Self {
        self.source = Some(source);
        self
    }

    pub fn sink(mut self, sink: SinkFactory<T>) -> Self {
        self.sink = Some(sink);
        self
    }

    pub fn build(self) -> FormatTransform {
        FormatTransform {
            binder: Arc::new(binding::TransformDefinition::new(
                self.args,
                self.source,
                self.sink,
            )),
        }
    }
}

/// A format's inspection CLI contribution and typed callback.
#[derive(Clone)]
pub struct FormatInspection {
    binder: Arc<dyn binding::BindInspection>,
}

impl FormatInspection {
    pub fn with_args<T>(inspector: Inspector<T>) -> Self
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        Self {
            binder: Arc::new(binding::InspectionDefinition::new(
                binding::ArgsParser::for_args(),
                inspector,
            )),
        }
    }

    pub fn without_args(inspector: Inspector<()>) -> Self {
        Self {
            binder: Arc::new(binding::InspectionDefinition::new(
                binding::ArgsParser::unit(),
                inspector,
            )),
        }
    }
}

/// Declares one format's names and independently optional capabilities.
#[derive(Clone)]
pub struct FormatRegistration {
    name: &'static str,
    aliases: Vec<&'static str>,
    extensions: Vec<&'static str>,
    identifier_priority: usize,
    identifier: Option<Identifier>,
    transform: Option<FormatTransform>,
    inspection: Option<FormatInspection>,
}

impl FormatRegistration {
    pub fn builder(name: &'static str) -> FormatRegistrationBuilder {
        FormatRegistrationBuilder {
            registration: Self {
                name,
                aliases: Vec::new(),
                extensions: Vec::new(),
                identifier_priority: usize::MAX,
                identifier: None,
                transform: None,
                inspection: None,
            },
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn aliases(&self) -> &[&'static str] {
        &self.aliases
    }

    pub fn extensions(&self) -> &[&'static str] {
        &self.extensions
    }

    pub fn has_identifier(&self) -> bool {
        self.identifier.is_some()
    }

    pub fn has_source(&self) -> bool {
        self.transform
            .as_ref()
            .is_some_and(|transform| transform.binder.has_source())
    }

    pub fn has_sink(&self) -> bool {
        self.transform
            .as_ref()
            .is_some_and(|transform| transform.binder.has_sink())
    }

    pub fn has_inspector(&self) -> bool {
        self.inspection.is_some()
    }

    pub async fn identify(
        &self,
        handle: &StorageHandle,
    ) -> Result<Option<IdentifiedFormat>, FormatInvocationError> {
        let identifier = self
            .identifier
            .ok_or(FormatInvocationError::CapabilityUnavailable {
                format: self.name,
                capability: FormatCapability::Identification,
            })?;
        let identification =
            identifier(handle)
                .await
                .map_err(|source| FormatInvocationError::CallbackFailed {
                    format: self.name,
                    capability: FormatCapability::Identification,
                    source,
                })?;
        Ok(identification.map(|identification| IdentifiedFormat {
            format: self.name,
            variant: identification.variant,
        }))
    }

    pub fn augment_inspection_args(&self, command: Command) -> Command {
        match &self.inspection {
            Some(inspection) => inspection.binder.augment(command),
            None => command,
        }
    }

    pub fn bind_inspection_args(
        &self,
        matches: &ArgMatches,
    ) -> Result<ConfiguredInspection, clap::Error> {
        let callbacks = self
            .inspection
            .as_ref()
            .map(|inspection| inspection.binder.bind(matches))
            .transpose()?;
        Ok(ConfiguredInspection {
            format: self.name,
            callbacks,
        })
    }
}

/// One format's inspection callback bound to its parsed CLI arguments.
pub struct ConfiguredInspection {
    format: &'static str,
    callbacks: Option<Arc<dyn binding::InvokeInspection>>,
}

impl ConfiguredInspection {
    pub fn format(&self) -> &'static str {
        self.format
    }

    pub async fn inspect(
        &self,
        handle: &StorageHandle,
    ) -> Result<InspectionOutput, FormatInvocationError> {
        let callbacks =
            self.callbacks
                .as_ref()
                .ok_or(FormatInvocationError::CapabilityUnavailable {
                    format: self.format,
                    capability: FormatCapability::Inspection,
                })?;
        callbacks.inspect(self.format, handle).await
    }
}

pub struct FormatRegistrationBuilder {
    registration: FormatRegistration,
}

impl FormatRegistrationBuilder {
    pub fn aliases(mut self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        self.registration.aliases.extend(aliases);
        self
    }

    pub fn extensions(mut self, extensions: impl IntoIterator<Item = &'static str>) -> Self {
        self.registration.extensions.extend(extensions);
        self
    }

    pub fn identifier(mut self, identifier: Identifier) -> Self {
        self.registration.identifier = Some(identifier);
        self
    }

    pub fn identifier_priority(mut self, priority: usize) -> Self {
        self.registration.identifier_priority = priority;
        self
    }

    pub fn transform(mut self, transform: FormatTransform) -> Self {
        self.registration.transform = Some(transform);
        self
    }

    pub fn inspection(mut self, inspection: FormatInspection) -> Self {
        self.registration.inspection = Some(inspection);
        self
    }

    pub fn build(self) -> FormatRegistration {
        self.registration
    }
}

#[derive(Debug, Error)]
pub enum FormatRegistryError {
    #[error("duplicate format name: {0}")]
    DuplicateName(String),
    #[error("duplicate format alias: {0}")]
    DuplicateAlias(String),
    #[error("duplicate format extension: {0}")]
    DuplicateExtension(String),
    #[error("duplicate format CLI argument: {0}")]
    DuplicateCliArgument(String),
}

pub struct FormatRegistryBuilder {
    registrations: Vec<FormatRegistration>,
}

impl FormatRegistryBuilder {
    pub fn register(mut self, registration: FormatRegistration) -> Self {
        self.registrations.push(registration);
        self
    }

    pub fn build(self) -> Result<FormatRegistry, FormatRegistryError> {
        FormatRegistry::from_registrations(self.registrations)
    }
}

pub struct FormatRegistry {
    registrations: Vec<FormatRegistration>,
    names: HashMap<String, usize>,
    extensions: HashMap<String, usize>,
    identifier_order: Vec<usize>,
}

impl FormatRegistry {
    pub fn builder() -> FormatRegistryBuilder {
        FormatRegistryBuilder {
            registrations: Vec::new(),
        }
    }

    fn from_registrations(
        registrations: Vec<FormatRegistration>,
    ) -> Result<Self, FormatRegistryError> {
        let mut names = HashMap::new();
        let mut extensions = HashMap::new();
        let mut cli_arguments = HashSet::new();

        for (index, registration) in registrations.iter().enumerate() {
            let name = registration.name.to_ascii_lowercase();
            if names.insert(name.clone(), index).is_some() {
                return Err(FormatRegistryError::DuplicateName(name));
            }

            for alias in &registration.aliases {
                let alias = alias.to_ascii_lowercase();
                if names.insert(alias.clone(), index).is_some() {
                    return Err(FormatRegistryError::DuplicateAlias(alias));
                }
            }

            for extension in &registration.extensions {
                let extension = extension.trim_start_matches('.').to_ascii_lowercase();
                if extensions.insert(extension.clone(), index).is_some() {
                    return Err(FormatRegistryError::DuplicateExtension(extension));
                }
            }

            if let Some(transform) = &registration.transform {
                for (key, argument) in transform.binder.argument_keys() {
                    if !cli_arguments.insert(key) {
                        return Err(FormatRegistryError::DuplicateCliArgument(argument));
                    }
                }
            }
        }

        let mut identifier_order = registrations
            .iter()
            .enumerate()
            .filter_map(|(index, registration)| registration.identifier.map(|_| index))
            .collect::<Vec<_>>();
        identifier_order.sort_by_key(|index| (registrations[*index].identifier_priority, *index));

        Ok(Self {
            registrations,
            names,
            extensions,
            identifier_order,
        })
    }

    pub fn registrations(&self) -> impl Iterator<Item = &FormatRegistration> {
        self.registrations.iter()
    }

    pub fn identifiers(&self) -> impl Iterator<Item = &FormatRegistration> {
        self.identifier_order
            .iter()
            .map(|index| &self.registrations[*index])
    }

    pub fn get(&self, name_or_alias: &str) -> Option<&FormatRegistration> {
        self.names
            .get(&name_or_alias.to_ascii_lowercase())
            .map(|index| &self.registrations[*index])
    }

    pub fn by_extension(&self, extension: &str) -> Option<&FormatRegistration> {
        self.extensions
            .get(&extension.trim_start_matches('.').to_ascii_lowercase())
            .map(|index| &self.registrations[*index])
    }

    pub fn augment_transform_args(&self, mut command: Command) -> Command {
        for registration in &self.registrations {
            if let Some(transform) = &registration.transform {
                command = transform.binder.augment(command);
            }
        }
        command
    }

    pub fn bind_transform_args(
        &self,
        matches: &ArgMatches,
    ) -> Result<ConfiguredFormats, clap::Error> {
        let mut formats = Vec::with_capacity(self.registrations.len());
        for registration in &self.registrations {
            let callbacks = registration
                .transform
                .as_ref()
                .map(|transform| transform.binder.bind(matches))
                .transpose()?;
            formats.push(ConfiguredFormat {
                format: registration.name,
                callbacks,
            });
        }
        Ok(ConfiguredFormats {
            formats,
            names: self.names.clone(),
            extensions: self.extensions.clone(),
        })
    }
}

/// One format's source and sink callbacks bound to their shared CLI arguments.
pub struct ConfiguredFormat {
    format: &'static str,
    callbacks: Option<Arc<dyn binding::InvokeTransform>>,
}

impl ConfiguredFormat {
    pub fn format(&self) -> &'static str {
        self.format
    }

    pub fn has_source(&self) -> bool {
        self.callbacks
            .as_ref()
            .is_some_and(|callbacks| callbacks.has_source())
    }

    pub fn has_sink(&self) -> bool {
        self.callbacks
            .as_ref()
            .is_some_and(|callbacks| callbacks.has_sink())
    }

    pub async fn create_source(
        &self,
        handle: &StorageHandle,
    ) -> Result<Box<dyn DataSource>, FormatInvocationError> {
        let callbacks =
            self.callbacks
                .as_ref()
                .ok_or(FormatInvocationError::CapabilityUnavailable {
                    format: self.format,
                    capability: FormatCapability::Source,
                })?;
        callbacks.create_source(self.format, handle).await
    }

    pub async fn create_sink_factory(
        &self,
        context: &SinkFactoryContext,
    ) -> Result<Box<dyn DataSinkFactory>, FormatInvocationError> {
        let callbacks =
            self.callbacks
                .as_ref()
                .ok_or(FormatInvocationError::CapabilityUnavailable {
                    format: self.format,
                    capability: FormatCapability::Sink,
                })?;
        callbacks.create_sink_factory(self.format, context).await
    }
}

/// CLI-bound source and sink callbacks for every registered format.
pub struct ConfiguredFormats {
    formats: Vec<ConfiguredFormat>,
    names: HashMap<String, usize>,
    extensions: HashMap<String, usize>,
}

impl ConfiguredFormats {
    pub fn formats(&self) -> impl Iterator<Item = &ConfiguredFormat> {
        self.formats.iter()
    }

    pub fn get(&self, name_or_alias: &str) -> Option<&ConfiguredFormat> {
        self.names
            .get(&name_or_alias.to_ascii_lowercase())
            .map(|index| &self.formats[*index])
    }

    pub fn by_extension(&self, extension: &str) -> Option<&ConfiguredFormat> {
        self.extensions
            .get(&extension.trim_start_matches('.').to_ascii_lowercase())
            .map(|index| &self.formats[*index])
    }
}
