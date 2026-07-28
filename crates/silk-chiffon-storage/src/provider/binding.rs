use std::sync::Arc;

use clap::{ArgMatches, Args, Command, FromArgMatches};

use super::{ProviderResolution, ProviderResolver, StorageDirection};
use crate::{Location, RetryConfiguration, StorageError};

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

pub(super) trait BindProvider: Send + Sync {
    fn has_input(&self) -> bool;

    fn has_output(&self) -> bool;

    fn augment(&self, command: Command) -> Command;

    fn argument_keys(&self) -> Vec<(String, String)>;

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ResolveProvider>, clap::Error>;
}

pub(super) trait ResolveProvider: Send + Sync {
    fn has_input(&self) -> bool;

    fn has_output(&self) -> bool;

    fn resolve(
        &self,
        provider: &'static str,
        direction: StorageDirection,
        location: &Location,
        retry: Option<&RetryConfiguration>,
    ) -> Result<ProviderResolution, StorageError>;
}

pub(super) struct ProviderDefinition<T> {
    args: ArgsParser<T>,
    input: Option<ProviderResolver<T>>,
    output: Option<ProviderResolver<T>>,
}

impl<T> ProviderDefinition<T> {
    pub(super) fn new(
        args: ArgsParser<T>,
        input: Option<ProviderResolver<T>>,
        output: Option<ProviderResolver<T>>,
    ) -> Self {
        Self {
            args,
            input,
            output,
        }
    }
}

impl<T> BindProvider for ProviderDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn has_input(&self) -> bool {
        self.input.is_some()
    }

    fn has_output(&self) -> bool {
        self.output.is_some()
    }

    fn augment(&self, command: Command) -> Command {
        self.args.augment(command)
    }

    fn argument_keys(&self) -> Vec<(String, String)> {
        self.args.argument_keys()
    }

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ResolveProvider>, clap::Error> {
        Ok(Arc::new(BoundProvider {
            settings: self.args.parse(matches)?,
            input: self.input,
            output: self.output,
        }))
    }
}

struct BoundProvider<T> {
    settings: T,
    input: Option<ProviderResolver<T>>,
    output: Option<ProviderResolver<T>>,
}

impl<T> ResolveProvider for BoundProvider<T>
where
    T: Send + Sync + 'static,
{
    fn has_input(&self) -> bool {
        self.input.is_some()
    }

    fn has_output(&self) -> bool {
        self.output.is_some()
    }

    fn resolve(
        &self,
        provider: &'static str,
        direction: StorageDirection,
        location: &Location,
        retry: Option<&RetryConfiguration>,
    ) -> Result<ProviderResolution, StorageError> {
        let resolver = match direction {
            StorageDirection::Input => self.input,
            StorageDirection::Output => self.output,
        }
        .ok_or(StorageError::DirectionUnsupported {
            provider,
            direction,
        })?;

        resolver(location, &self.settings, retry)
    }
}
