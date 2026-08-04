//! Private type erasure for heterogeneous provider settings.
//!
//! Enabled registrations keep each Clap argument type `T` paired with a
//! [`super::ProviderResolver<T>`]. Disabled registrations retain only `T`'s parser. At command
//! binding time this module parses `T` for enabled providers and erases only the behavior needed by
//! the registry. Neither provider authors nor resolution callers need `Any`, downcasting, or a
//! shared untyped settings representation.

use std::sync::Arc;

use clap::{ArgMatches, Args, Command, FromArgMatches};

use object_store::RetryConfig;

use super::{ProviderResolution, ProviderResolver};
use crate::Location;

#[derive(Clone, Copy)]
/// Function pointers for augmenting a command with `T` and parsing `T` from its matches.
///
/// Storing these operations keeps the registration builder independent of a particular Clap
/// command while preserving the concrete settings type.
pub(super) struct ArgsParser<T> {
    augment: fn(Command) -> Command,
    parse: fn(&ArgMatches) -> Result<T, clap::Error>,
}

impl<T> ArgsParser<T> {
    /// Captures the Clap operations implemented by an ordinary argument struct.
    pub(super) fn for_args() -> Self
    where
        T: Args + FromArgMatches,
    {
        Self {
            augment: T::augment_args,
            parse: T::from_arg_matches,
        }
    }

    /// Adds `T`'s arguments to the command used for registry-wide parsing.
    pub(super) fn augment(&self, command: Command) -> Command {
        (self.augment)(command)
    }

    /// Parses `T` when the registry binds one command's matches.
    pub(super) fn parse(&self, matches: &ArgMatches) -> Result<T, clap::Error> {
        (self.parse)(matches)
    }

    /// Returns collision keys for every Clap ID and primary long and short option contributed by
    /// `T`.
    pub(super) fn argument_keys(&self) -> Vec<(String, String)> {
        let command = self.augment(Command::new("provider"));
        let mut keys = Vec::new();
        for argument in command.get_arguments() {
            let id = argument.get_id().as_str().to_owned();
            keys.push((format!("id:{id}"), id.clone()));
            if let Some(long) = argument.get_long() {
                keys.push((format!("long:{long}"), id.clone()));
            }
            if let Some(short) = argument.get_short() {
                keys.push((format!("short:{short}"), id.clone()));
            }
        }
        keys
    }
}

impl ArgsParser<()> {
    /// Creates a no-op parser for a provider without command-line settings.
    pub(super) fn unit() -> Self {
        Self {
            augment: |command| command,
            parse: |_| Ok(()),
        }
    }
}

/// The argument behavior retained for both enabled and disabled registrations.
///
/// Disabled providers implement only this layer so they can preserve their CLI surface without
/// exposing resolution behavior.
pub(super) trait RegisterProviderArguments: Send + Sync {
    /// Adds the provider's arguments to a command.
    fn augment(&self, command: Command) -> Command;

    /// Returns the provider's Clap collision keys.
    fn argument_keys(&self) -> Vec<(String, String)>;
}

/// An enabled registration that can bind parsed settings into a resolver.
pub(super) trait BindProvider: RegisterProviderArguments {
    /// Parses the provider settings and erases the resulting resolver behavior.
    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ResolveProvider>, clap::Error>;
}

/// Resolution behavior after a provider's concrete settings type has been erased.
pub(super) trait ResolveProvider: Send + Sync {
    /// Invokes the provider callback with its bound settings.
    fn resolve(
        &self,
        location: &Location,
        retry: Option<&RetryConfig>,
    ) -> anyhow::Result<ProviderResolution>;
}

/// An enabled provider before command arguments have been bound.
///
/// `T` remains concrete here so the resolver cannot become detached from the settings type it
/// expects.
pub(super) struct ProviderDefinition<T> {
    args: ArgsParser<T>,
    resolver: ProviderResolver<T>,
}

impl<T> ProviderDefinition<T> {
    /// Pairs a settings parser with the resolver that consumes those settings.
    pub(super) fn new(args: ArgsParser<T>, resolver: ProviderResolver<T>) -> Self {
        Self { args, resolver }
    }
}

impl<T> RegisterProviderArguments for ProviderDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn augment(&self, command: Command) -> Command {
        self.args.augment(command)
    }

    fn argument_keys(&self) -> Vec<(String, String)> {
        self.args.argument_keys()
    }
}

impl<T> BindProvider for ProviderDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ResolveProvider>, clap::Error> {
        Ok(Arc::new(BoundProvider {
            settings: self.args.parse(matches)?,
            resolver: self.resolver,
        }))
    }
}

/// One provider's parsed settings paired with its typed resolver.
struct BoundProvider<T> {
    settings: T,
    resolver: ProviderResolver<T>,
}

impl<T> ResolveProvider for BoundProvider<T>
where
    T: Send + Sync + 'static,
{
    fn resolve(
        &self,
        location: &Location,
        retry: Option<&RetryConfig>,
    ) -> anyhow::Result<ProviderResolution> {
        (self.resolver)(location, &self.settings, retry)
    }
}

/// The argument contribution retained by a disabled provider.
pub(super) struct ProviderArguments<T> {
    args: ArgsParser<T>,
}

impl<T> ProviderArguments<T> {
    /// Keeps a disabled provider's arguments available for help and collision checks.
    pub(super) fn new(args: ArgsParser<T>) -> Self {
        Self { args }
    }
}

impl<T> RegisterProviderArguments for ProviderArguments<T>
where
    T: Send + Sync + 'static,
{
    fn augment(&self, command: Command) -> Command {
        self.args.augment(command)
    }

    fn argument_keys(&self) -> Vec<(String, String)> {
        self.args.argument_keys()
    }
}
