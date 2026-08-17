//! Public contracts for service-backed command inputs.
//!
//! A connector crate contributes an immutable [`ServiceInputDefinition`] with its name, claimed
//! schemes, typed Clap settings, and source creator. The host adds those settings to its command
//! and binds them once after parsing. The resulting [`ServiceInputBinding`] creates a
//! [`DataSource`] from one raw exact reference and the command's shared DataFusion session.
//!
//! Each connector keeps its settings type through parsing and binding. The private `binding`
//! module erases the complete typed definition or binding behind a trait object, allowing
//! connectors with different settings types to coexist without storing `Any` values or
//! downcasting settings.

mod binding;

use std::{collections::HashSet, fmt, sync::Arc};

use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use thiserror::Error;

use crate::DataSource;

/// Creates one logical input from a raw exact reference, the shared session,
/// and typed settings.
pub type ServiceInputCreatorFn<T> =
    for<'a> fn(&'a str, &'a SessionContext, &'a T) -> BoxFuture<'a, Result<Box<dyn DataSource>>>;

/// Immutable metadata and typed creation behavior contributed by one service input.
#[derive(Clone)]
pub struct ServiceInputDefinition {
    name: &'static str,
    schemes: Arc<[&'static str]>,
    definition: Arc<dyn binding::ErasedServiceInputDefinition>,
}

impl fmt::Debug for ServiceInputDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceInputDefinition")
            .field("name", &self.name)
            .field("schemes", &self.schemes)
            .finish_non_exhaustive()
    }
}

impl ServiceInputDefinition {
    /// Starts a definition whose creator receives parsed `T` settings.
    pub fn with_args<T>(creator: ServiceInputCreatorFn<T>) -> ServiceInputDefinitionBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        ServiceInputDefinitionBuilder::new(binding::ArgsParser::for_args(), creator)
    }

    /// Starts a definition with no service-specific settings.
    pub fn without_args(creator: ServiceInputCreatorFn<()>) -> ServiceInputDefinitionBuilder<()> {
        ServiceInputDefinitionBuilder::new(binding::ArgsParser::<()>::unit(), creator)
    }

    /// Returns the canonical name used in assembly diagnostics.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the exact URL schemes claimed by this input definition.
    pub fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    /// Adds this definition's typed settings to the host command.
    pub fn augment_args(&self, command: Command) -> Command {
        self.definition.augment_args(command)
    }

    /// Binds this definition's typed settings for one parsed command.
    pub fn bind(&self, matches: &ArgMatches) -> Result<ServiceInputBinding, clap::Error> {
        Ok(ServiceInputBinding {
            name: self.name,
            binding: self.definition.bind(matches)?,
        })
    }
}

/// Builds one service-input definition while preserving its concrete settings type.
pub struct ServiceInputDefinitionBuilder<T> {
    name: Option<&'static str>,
    schemes: Vec<&'static str>,
    args: binding::ArgsParser<T>,
    creator: ServiceInputCreatorFn<T>,
}

impl<T> ServiceInputDefinitionBuilder<T>
where
    T: Send + Sync + 'static,
{
    fn new(args: binding::ArgsParser<T>, creator: ServiceInputCreatorFn<T>) -> Self {
        Self {
            name: None,
            schemes: Vec::new(),
            args,
            creator,
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
    pub fn build(self) -> Result<ServiceInputDefinition, ServiceInputDefinitionBuildError> {
        let name = self
            .name
            .ok_or(ServiceInputDefinitionBuildError::MissingName)?;
        if !valid_name(name) {
            return Err(ServiceInputDefinitionBuildError::InvalidName { name });
        }
        validate_schemes(&self.schemes)?;
        Ok(ServiceInputDefinition {
            name,
            schemes: Arc::from(self.schemes),
            definition: Arc::new(binding::TypedServiceInputDefinition::new(
                self.args,
                self.creator,
            )),
        })
    }
}

/// Invalid immutable service-input definition.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ServiceInputDefinitionBuildError {
    #[error("service input definition requires a name")]
    MissingName,
    #[error("invalid service input name {name:?}")]
    InvalidName { name: &'static str },
    #[error("service input definition requires at least one scheme")]
    MissingSchemes,
    #[error("invalid service input scheme {scheme:?}")]
    InvalidScheme { scheme: &'static str },
    #[error("duplicate service input scheme {scheme:?}")]
    DuplicateScheme { scheme: &'static str },
}

/// Command-scoped service-input behavior with its typed settings already bound.
pub struct ServiceInputBinding {
    name: &'static str,
    binding: Box<dyn binding::ErasedServiceInputBinding>,
}

impl ServiceInputBinding {
    /// Returns the definition name used to attribute failures.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Creates one source from a raw exact reference in the shared session.
    pub async fn create_source(
        &self,
        reference: &str,
        session: &SessionContext,
    ) -> Result<Box<dyn DataSource>, ServiceInputCreationError> {
        self.binding
            .create_source(reference, session)
            .await
            .map_err(|source| ServiceInputCreationError {
                service: self.name,
                reference: reference.to_owned(),
                source,
            })
    }
}

/// Failure while one bound service input creates its logical source.
#[derive(Debug, Error)]
#[error("service input {service:?} failed to create source for {reference:?}: {source}")]
pub struct ServiceInputCreationError {
    service: &'static str,
    reference: String,
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

fn validate_schemes(schemes: &[&'static str]) -> Result<(), ServiceInputDefinitionBuildError> {
    if schemes.is_empty() {
        return Err(ServiceInputDefinitionBuildError::MissingSchemes);
    }
    let mut seen = HashSet::new();
    for &scheme in schemes {
        if !valid_scheme(scheme) {
            return Err(ServiceInputDefinitionBuildError::InvalidScheme { scheme });
        }
        if !seen.insert(scheme) {
            return Err(ServiceInputDefinitionBuildError::DuplicateScheme { scheme });
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
