//! Data-format registration contracts.
//!
//! A format contributes ordinary [`clap::Args`] types for each CLI scope it supports. Format-owned long options follow the `--{format}-...` convention, such as `--parquet-row-group-size`. Shared or global arguments may remain unprefixed. Registry construction rejects colliding transform argument IDs, long names, and short names.
//!
//! Each runtime callback returns a boxed `Send` future and receives the concrete argument type registered for its scope. Identification has no CLI settings. Source and sink callbacks share one transform argument type, while inspection may register a different argument type.
//!
//! The sink callback creates one command-scoped [`DataSinkFactory`] that can retain state across every output sink.

mod erased;

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
use silk_chiffon_storage::ResolvedLocation;
use thiserror::Error;

use crate::{DataSinkFactory, DataSource, InspectionOutput};

/// A `Send` future returned by a format callback.
pub type FormatFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Identifies a matching format without naming that format centrally.
pub type Identifier = for<'a> fn(&'a ResolvedLocation) -> FormatFuture<'a, Option<Identification>>;

/// Creates a source from transform settings registered as `T`.
pub type SourceFactory<T> =
    for<'a> fn(&'a ResolvedLocation, &'a T) -> FormatFuture<'a, Box<dyn DataSource>>;

/// Creates a command-scoped sink factory from transform settings registered as `T`.
pub type SinkFactory<T> =
    for<'a> fn(&'a SinkFactoryContext, &'a T) -> FormatFuture<'a, Box<dyn DataSinkFactory>>;

/// Produces inspection output from inspection settings registered as `T`.
pub type Inspector<T> =
    for<'a> fn(&'a ResolvedLocation, &'a T) -> FormatFuture<'a, InspectionOutput>;

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
pub enum FormatRuntimeError {
    #[error("runtime settings are unavailable for format: {format}")]
    MissingSettings { format: &'static str },
    #[error("runtime settings have the wrong type for format: {format}")]
    SettingsTypeMismatch { format: &'static str },
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
    cli: erased::CliContribution,
    runtime: Arc<dyn erased::TransformRuntime>,
}

impl FormatTransform {
    pub fn with_args<T>() -> FormatTransformBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        FormatTransformBuilder {
            cli: erased::CliContribution::for_args::<T>(),
            source: None,
            sink: None,
            settings: PhantomData,
        }
    }

    pub fn without_args() -> FormatTransformBuilder<()> {
        FormatTransformBuilder {
            cli: erased::CliContribution::unit(),
            source: None,
            sink: None,
            settings: PhantomData,
        }
    }
}

/// Builds transform capabilities that share one concrete argument type.
pub struct FormatTransformBuilder<T> {
    cli: erased::CliContribution,
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
            cli: self.cli,
            runtime: Arc::new(erased::TypedTransform::new(self.source, self.sink)),
        }
    }
}

/// A format's inspection CLI contribution and typed callback.
#[derive(Clone)]
pub struct FormatInspection {
    cli: erased::CliContribution,
    runtime: Arc<dyn erased::InspectionRuntime>,
}

impl FormatInspection {
    pub fn with_args<T>(inspector: Inspector<T>) -> Self
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        Self {
            cli: erased::CliContribution::for_args::<T>(),
            runtime: Arc::new(erased::TypedInspection::new(inspector)),
        }
    }

    pub fn without_args(inspector: Inspector<()>) -> Self {
        Self {
            cli: erased::CliContribution::unit(),
            runtime: Arc::new(erased::TypedInspection::new(inspector)),
        }
    }
}

/// Declares one format's names and independently optional runtime capabilities.
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
            .is_some_and(|transform| transform.runtime.has_source())
    }

    pub fn has_sink(&self) -> bool {
        self.transform
            .as_ref()
            .is_some_and(|transform| transform.runtime.has_sink())
    }

    pub fn has_inspector(&self) -> bool {
        self.inspection.is_some()
    }

    pub async fn identify(
        &self,
        location: &ResolvedLocation,
    ) -> Result<Option<IdentifiedFormat>, FormatRuntimeError> {
        let identifier = self
            .identifier
            .ok_or(FormatRuntimeError::CapabilityUnavailable {
                format: self.name,
                capability: FormatCapability::Identification,
            })?;
        let identification =
            identifier(location)
                .await
                .map_err(|source| FormatRuntimeError::CallbackFailed {
                    format: self.name,
                    capability: FormatCapability::Identification,
                    source,
                })?;
        Ok(identification.map(|identification| IdentifiedFormat {
            format: self.name,
            variant: identification.variant,
        }))
    }

    pub async fn create_source(
        &self,
        location: &ResolvedLocation,
        settings: &FormatRuntimeSettings,
    ) -> Result<Box<dyn DataSource>, FormatRuntimeError> {
        let transform =
            self.transform
                .as_ref()
                .ok_or(FormatRuntimeError::CapabilityUnavailable {
                    format: self.name,
                    capability: FormatCapability::Source,
                })?;
        if !transform.runtime.has_source() {
            return Err(FormatRuntimeError::CapabilityUnavailable {
                format: self.name,
                capability: FormatCapability::Source,
            });
        }
        let settings = settings
            .settings
            .get(&self.name.to_ascii_lowercase())
            .ok_or(FormatRuntimeError::MissingSettings { format: self.name })?;
        transform
            .runtime
            .create_source(self.name, location, settings)
            .await
    }

    pub async fn create_sink_factory(
        &self,
        context: &SinkFactoryContext,
        settings: &FormatRuntimeSettings,
    ) -> Result<Box<dyn DataSinkFactory>, FormatRuntimeError> {
        let transform =
            self.transform
                .as_ref()
                .ok_or(FormatRuntimeError::CapabilityUnavailable {
                    format: self.name,
                    capability: FormatCapability::Sink,
                })?;
        if !transform.runtime.has_sink() {
            return Err(FormatRuntimeError::CapabilityUnavailable {
                format: self.name,
                capability: FormatCapability::Sink,
            });
        }
        let settings = settings
            .settings
            .get(&self.name.to_ascii_lowercase())
            .ok_or(FormatRuntimeError::MissingSettings { format: self.name })?;
        transform
            .runtime
            .create_sink_factory(self.name, context, settings)
            .await
    }

    pub fn augment_inspection_args(&self, command: Command) -> Command {
        match &self.inspection {
            Some(inspection) => inspection.cli.augment(command),
            None => command,
        }
    }

    pub fn parse_inspection_cli(
        &self,
        matches: &ArgMatches,
    ) -> Result<FormatInspectionSettings, clap::Error> {
        let settings = match &self.inspection {
            Some(inspection) => inspection.cli.parse(matches)?,
            None => erased::Settings::unit(),
        };
        Ok(FormatInspectionSettings {
            format: self.name,
            settings,
        })
    }

    pub async fn inspect(
        &self,
        location: &ResolvedLocation,
        settings: &FormatInspectionSettings,
    ) -> Result<InspectionOutput, FormatRuntimeError> {
        let inspection =
            self.inspection
                .as_ref()
                .ok_or(FormatRuntimeError::CapabilityUnavailable {
                    format: self.name,
                    capability: FormatCapability::Inspection,
                })?;
        if settings.format != self.name {
            return Err(FormatRuntimeError::SettingsTypeMismatch { format: self.name });
        }
        inspection
            .runtime
            .inspect(self.name, location, &settings.settings)
            .await
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
                for (key, argument) in transform.cli.argument_keys() {
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
                command = transform.cli.augment(command);
            }
        }
        command
    }

    pub fn parse_transform_cli(
        &self,
        matches: &ArgMatches,
    ) -> Result<FormatRuntimeSettings, clap::Error> {
        let mut settings = HashMap::new();
        for registration in &self.registrations {
            if let Some(transform) = &registration.transform {
                settings.insert(
                    registration.name.to_ascii_lowercase(),
                    transform.cli.parse(matches)?,
                );
            }
        }
        Ok(FormatRuntimeSettings { settings })
    }
}

/// Parsed transform settings keyed by canonical format name.
pub struct FormatRuntimeSettings {
    settings: HashMap<String, erased::Settings>,
}

/// Parsed settings for one format's inspection CLI scope.
pub struct FormatInspectionSettings {
    format: &'static str,
    settings: erased::Settings,
}
