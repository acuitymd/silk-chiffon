//! Public contracts for service-backed command outputs.
//!
//! A connector crate contributes an immutable [`ServiceOutputDefinition`] with its name, claimed
//! schemes, typed Clap command state, and consumer operation. The host adds that state to its
//! command and binds it once after parsing. The resulting [`ServiceOutputBinding`] consumes the
//! final DataFusion record-batch stream into one exact target. The consumer must drain the stream
//! and finish its writer or service operation before it returns.
//!
//! Each connector keeps its command-state type through parsing and binding. The private `binding`
//! module erases the complete typed definition or binding behind a trait object, allowing
//! connectors with different command-state types to coexist without storing `Any` values or
//! downcasting state.

mod binding;

use std::{collections::HashSet, fmt, sync::Arc};

use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::future::BoxFuture;
use thiserror::Error;

/// Consumes one final result stream into an exact service target.
///
/// The returned future must drain the stream and finish the target before it resolves.
pub type ServiceOutputConsumerFn<T> =
    for<'a> fn(&'a str, SendableRecordBatchStream, &'a T) -> BoxFuture<'a, Result<()>>;

/// Immutable metadata and typed consumption behavior contributed by one service output.
#[derive(Clone)]
pub struct ServiceOutputDefinition {
    name: &'static str,
    schemes: Arc<[&'static str]>,
    definition: Arc<dyn binding::ErasedServiceOutputDefinition>,
}

impl fmt::Debug for ServiceOutputDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceOutputDefinition")
            .field("name", &self.name)
            .field("schemes", &self.schemes)
            .finish_non_exhaustive()
    }
}

impl ServiceOutputDefinition {
    /// Starts a definition whose consumer receives command state parsed as `T`.
    pub fn with_args<T>(consumer: ServiceOutputConsumerFn<T>) -> ServiceOutputDefinitionBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        ServiceOutputDefinitionBuilder::new(binding::ArgsParser::for_args(), consumer)
    }

    /// Starts a definition with no service-specific command state.
    pub fn without_args(
        consumer: ServiceOutputConsumerFn<()>,
    ) -> ServiceOutputDefinitionBuilder<()> {
        ServiceOutputDefinitionBuilder::new(binding::ArgsParser::<()>::unit(), consumer)
    }

    /// Returns the canonical name used in assembly diagnostics.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the exact URL schemes claimed by this output definition.
    pub fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    /// Adds this definition's typed command state to the host command.
    pub fn augment_args(&self, command: Command) -> Command {
        self.definition.augment_args(command)
    }

    /// Binds this definition's typed command state for one parsed command.
    pub fn bind(&self, matches: &ArgMatches) -> Result<ServiceOutputBinding, clap::Error> {
        Ok(ServiceOutputBinding {
            name: self.name,
            binding: self.definition.bind(matches)?,
        })
    }
}

/// Builds one service-output definition while preserving its concrete command-state type.
pub struct ServiceOutputDefinitionBuilder<T> {
    name: Option<&'static str>,
    schemes: Vec<&'static str>,
    args: binding::ArgsParser<T>,
    consumer: ServiceOutputConsumerFn<T>,
}

impl<T> ServiceOutputDefinitionBuilder<T>
where
    T: Send + Sync + 'static,
{
    fn new(args: binding::ArgsParser<T>, consumer: ServiceOutputConsumerFn<T>) -> Self {
        Self {
            name: None,
            schemes: Vec::new(),
            args,
            consumer,
        }
    }

    /// Sets the canonical name used for identity and diagnostics.
    pub fn name(mut self, name: &'static str) -> Self {
        self.name = Some(name);
        self
    }

    /// Replaces the exact URL schemes claimed by this definition.
    pub fn schemes(mut self, schemes: impl IntoIterator<Item = &'static str>) -> Self {
        self.schemes = schemes.into_iter().collect();
        self
    }

    /// Validates and erases the complete typed definition.
    pub fn build(self) -> Result<ServiceOutputDefinition, ServiceOutputDefinitionBuildError> {
        let name = self
            .name
            .ok_or(ServiceOutputDefinitionBuildError::MissingName)?;
        if !valid_name(name) {
            return Err(ServiceOutputDefinitionBuildError::InvalidName { name });
        }
        validate_schemes(&self.schemes)?;
        Ok(ServiceOutputDefinition {
            name,
            schemes: Arc::from(self.schemes),
            definition: Arc::new(binding::TypedServiceOutputDefinition::new(
                self.args,
                self.consumer,
            )),
        })
    }
}

/// Invalid immutable service-output definition.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ServiceOutputDefinitionBuildError {
    #[error("service output definition requires a name")]
    MissingName,
    #[error("invalid service output name {name:?}")]
    InvalidName { name: &'static str },
    #[error("service output definition requires at least one scheme")]
    MissingSchemes,
    #[error("invalid service output scheme {scheme:?}")]
    InvalidScheme { scheme: &'static str },
    #[error("duplicate service output scheme {scheme:?}")]
    DuplicateScheme { scheme: &'static str },
}

/// Command-scoped service-output behavior with its typed state already bound.
pub struct ServiceOutputBinding {
    name: &'static str,
    binding: Box<dyn binding::ErasedServiceOutputBinding>,
}

impl ServiceOutputBinding {
    /// Returns the definition name used to attribute failures.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Consumes the complete final stream into one raw exact target.
    pub async fn consume(
        &self,
        target: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<(), ServiceOutputConsumptionError> {
        self.binding
            .consume(target, stream)
            .await
            .map_err(|source| ServiceOutputConsumptionError {
                service: self.name,
                target: target.to_owned(),
                source,
            })
    }
}

/// Failure while one bound service output consumes a stream into its exact target.
#[derive(Debug, Error)]
#[error("service output {service:?} failed to consume {target:?}: {source}")]
pub struct ServiceOutputConsumptionError {
    service: &'static str,
    target: String,
    #[source]
    source: anyhow::Error,
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('a'..='z'))
        && chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
}

fn validate_schemes(schemes: &[&'static str]) -> Result<(), ServiceOutputDefinitionBuildError> {
    if schemes.is_empty() {
        return Err(ServiceOutputDefinitionBuildError::MissingSchemes);
    }
    let mut seen = HashSet::new();
    for &scheme in schemes {
        if !valid_scheme(scheme) {
            return Err(ServiceOutputDefinitionBuildError::InvalidScheme { scheme });
        }
        if !seen.insert(scheme) {
            return Err(ServiceOutputDefinitionBuildError::DuplicateScheme { scheme });
        }
    }
    Ok(())
}

fn valid_scheme(scheme: &str) -> bool {
    let mut chars = scheme.chars();
    matches!(chars.next(), Some('a'..='z'))
        && chars.all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '+' | '-' | '.')
        })
}
