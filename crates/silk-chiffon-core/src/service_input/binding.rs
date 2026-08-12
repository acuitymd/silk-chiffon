//! Private type erasure for service-input definitions and command bindings.
//!
//! `TypedServiceInputDefinition<T>` keeps one connector's Clap settings as `T` until command
//! binding. It then parses `T` once and stores it beside that connector's provider function. Only
//! the complete definition and binding become trait objects, so independently typed connectors
//! can share one application collection without `Any` values or downcasts.

use std::{marker::PhantomData, sync::Arc};

use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use futures::future::BoxFuture;

use super::ServiceInputProviderFn;

pub(super) trait ErasedServiceInputDefinition: Send + Sync {
    fn augment_args(&self, command: Command) -> Command;
    fn bind(&self, matches: &ArgMatches)
    -> Result<Box<dyn ErasedServiceInputBinding>, clap::Error>;
}

pub(super) trait ErasedServiceInputBinding: Send + Sync {
    fn create_input_provider<'a>(
        &'a self,
        reference: &'a str,
        session: &'a SessionContext,
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>>;
}

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

    pub(super) fn unit() -> ArgsParser<()> {
        ArgsParser {
            augment: |command| command,
            parse: |_| Ok(()),
        }
    }
}

pub(super) struct TypedServiceInputDefinition<T> {
    args: ArgsParser<T>,
    provider: ServiceInputProviderFn<T>,
    settings: PhantomData<fn() -> T>,
}

impl<T> TypedServiceInputDefinition<T> {
    pub(super) fn new(args: ArgsParser<T>, provider: ServiceInputProviderFn<T>) -> Self {
        Self {
            args,
            provider,
            settings: PhantomData,
        }
    }
}

impl<T> ErasedServiceInputDefinition for TypedServiceInputDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn augment_args(&self, command: Command) -> Command {
        (self.args.augment)(command)
    }

    fn bind(
        &self,
        matches: &ArgMatches,
    ) -> Result<Box<dyn ErasedServiceInputBinding>, clap::Error> {
        Ok(Box::new(TypedServiceInputBinding {
            settings: Arc::new((self.args.parse)(matches)?),
            provider: self.provider,
        }))
    }
}

struct TypedServiceInputBinding<T> {
    settings: Arc<T>,
    provider: ServiceInputProviderFn<T>,
}

impl<T> ErasedServiceInputBinding for TypedServiceInputBinding<T>
where
    T: Send + Sync + 'static,
{
    fn create_input_provider<'a>(
        &'a self,
        reference: &'a str,
        session: &'a SessionContext,
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
        (self.provider)(reference, session, &self.settings)
    }
}
