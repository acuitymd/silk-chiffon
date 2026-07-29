use std::sync::Arc;

use clap::{ArgMatches, Args, Command, FromArgMatches};

use object_store::RetryConfig;

use super::{ProviderResolution, ProviderResolver};
use crate::Location;

#[derive(Clone, Copy)]
pub(super) struct ArgsParser<T> {
    augment: fn(Command) -> Command,
    parse: fn(&ArgMatches) -> Result<T, clap::Error>,
}

impl<T> ArgsParser<T> {
    pub(super) fn for_args() -> Self
    where
        T: Args + FromArgMatches,
    {
        Self {
            augment: T::augment_args,
            parse: T::from_arg_matches,
        }
    }

    pub(super) fn augment(&self, command: Command) -> Command {
        (self.augment)(command)
    }

    pub(super) fn parse(&self, matches: &ArgMatches) -> Result<T, clap::Error> {
        (self.parse)(matches)
    }

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
    pub(super) fn unit() -> Self {
        Self {
            augment: |command| command,
            parse: |_| Ok(()),
        }
    }
}

pub(super) trait RegisterProviderArguments: Send + Sync {
    fn augment(&self, command: Command) -> Command;

    fn argument_keys(&self) -> Vec<(String, String)>;
}

pub(super) trait BindProvider: RegisterProviderArguments {
    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ResolveProvider>, clap::Error>;
}

pub(super) trait ResolveProvider: Send + Sync {
    fn resolve(
        &self,
        location: &Location,
        retry: Option<&RetryConfig>,
    ) -> anyhow::Result<ProviderResolution>;
}

pub(super) struct ProviderDefinition<T> {
    args: ArgsParser<T>,
    resolver: ProviderResolver<T>,
}

impl<T> ProviderDefinition<T> {
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

pub(super) struct ProviderArguments<T> {
    args: ArgsParser<T>,
}

impl<T> ProviderArguments<T> {
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
