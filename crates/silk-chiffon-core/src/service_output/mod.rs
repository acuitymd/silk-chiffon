//! Public contracts for service-backed command outputs.
//!
//! A connector crate contributes an immutable [`ServiceOutputDefinition`] with its name, claimed
//! schemes, typed Clap settings, and write operation. The host adds those settings to its command
//! and binds them once after parsing. The resulting [`ServiceOutputBinding`] writes one exact
//! target from the final DataFusion record-batch stream. The write operation must drain the stream
//! and finish its writer or service operation before it returns.
//!
//! Each connector keeps its settings type through parsing and binding. The private `binding`
//! module erases the complete typed definition or binding behind a trait object, allowing
//! connectors with different settings types to coexist without storing `Any` values or
//! downcasting settings.

mod binding;

use std::{collections::HashSet, fmt, sync::Arc};

use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::future::BoxFuture;
use thiserror::Error;

/// Writes one final result stream directly to an exact service target.
///
/// The returned future must drain the stream and finish the target before it resolves.
pub type ServiceOutputWriteFn<T> =
    for<'a> fn(&'a str, SendableRecordBatchStream, &'a T) -> BoxFuture<'a, Result<()>>;

/// Immutable metadata and typed write behavior contributed by one service output.
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
    /// Starts a definition whose write operation receives parsed `T` settings.
    pub fn with_args<T>(write: ServiceOutputWriteFn<T>) -> ServiceOutputDefinitionBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        ServiceOutputDefinitionBuilder::new(binding::ArgsParser::for_args(), write)
    }

    /// Starts a definition with no service-specific settings.
    pub fn without_args(write: ServiceOutputWriteFn<()>) -> ServiceOutputDefinitionBuilder<()> {
        ServiceOutputDefinitionBuilder::new(binding::ArgsParser::<()>::unit(), write)
    }

    /// Returns the canonical name used in assembly diagnostics.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the exact URL schemes claimed by this output definition.
    pub fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    /// Adds this definition's typed settings to the host command.
    pub fn augment_args(&self, command: Command) -> Command {
        self.definition.augment_args(command)
    }

    /// Binds this definition's typed settings for one parsed command.
    pub fn bind(&self, matches: &ArgMatches) -> Result<ServiceOutputBinding, clap::Error> {
        Ok(ServiceOutputBinding {
            name: self.name,
            binding: self.definition.bind(matches)?,
        })
    }
}

/// Builds one service-output definition while preserving its concrete settings type.
pub struct ServiceOutputDefinitionBuilder<T> {
    name: Option<&'static str>,
    schemes: Vec<&'static str>,
    args: binding::ArgsParser<T>,
    write: ServiceOutputWriteFn<T>,
}

impl<T> ServiceOutputDefinitionBuilder<T>
where
    T: Send + Sync + 'static,
{
    fn new(args: binding::ArgsParser<T>, write: ServiceOutputWriteFn<T>) -> Self {
        Self {
            name: None,
            schemes: Vec::new(),
            args,
            write,
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
                self.args, self.write,
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

/// Command-scoped service-output behavior with its typed settings already bound.
pub struct ServiceOutputBinding {
    name: &'static str,
    binding: Box<dyn binding::ErasedServiceOutputBinding>,
}

impl ServiceOutputBinding {
    /// Returns the definition name used to attribute failures.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Writes the complete final stream to one raw exact target.
    pub async fn write(
        &self,
        target: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<(), ServiceOutputWriteError> {
        self.binding
            .write(target, stream)
            .await
            .map_err(|source| ServiceOutputWriteError {
                service: self.name,
                target: target.to_owned(),
                source,
            })
    }
}

/// Failure while one bound service output writes its exact target.
#[derive(Debug, Error)]
#[error("service output {service:?} failed to write {target:?}: {source}")]
pub struct ServiceOutputWriteError {
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
