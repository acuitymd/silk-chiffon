//! Private type erasure for service-output definitions and command bindings.
//!
//! `TypedServiceOutputDefinition<T>` keeps one connector's Clap settings as `T` until command
//! binding. It then parses `T` once and stores it beside that connector's write operation. Only
//! the complete definition and binding become trait objects, so independently typed connectors
//! can share one application collection without `Any` values or downcasts.

use std::{marker::PhantomData, sync::Arc};

use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::future::BoxFuture;

use super::ServiceOutputWriteFn;

pub(super) trait ErasedServiceOutputDefinition: Send + Sync {
    fn augment_args(&self, command: Command) -> Command;
    fn bind(
        &self,
        matches: &ArgMatches,
    ) -> Result<Box<dyn ErasedServiceOutputBinding>, clap::Error>;
}

pub(super) trait ErasedServiceOutputBinding: Send + Sync {
    fn write<'a>(
        &'a self,
        target: &'a str,
        stream: SendableRecordBatchStream,
    ) -> BoxFuture<'a, Result<()>>;
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

pub(super) struct TypedServiceOutputDefinition<T> {
    args: ArgsParser<T>,
    write: ServiceOutputWriteFn<T>,
    settings: PhantomData<fn() -> T>,
}

impl<T> TypedServiceOutputDefinition<T> {
    pub(super) fn new(args: ArgsParser<T>, write: ServiceOutputWriteFn<T>) -> Self {
        Self {
            args,
            write,
            settings: PhantomData,
        }
    }
}

impl<T> ErasedServiceOutputDefinition for TypedServiceOutputDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn augment_args(&self, command: Command) -> Command {
        (self.args.augment)(command)
    }

    fn bind(
        &self,
        matches: &ArgMatches,
    ) -> Result<Box<dyn ErasedServiceOutputBinding>, clap::Error> {
        Ok(Box::new(TypedServiceOutputBinding {
            settings: Arc::new((self.args.parse)(matches)?),
            write: self.write,
        }))
    }
}

struct TypedServiceOutputBinding<T> {
    settings: Arc<T>,
    write: ServiceOutputWriteFn<T>,
}

impl<T> ErasedServiceOutputBinding for TypedServiceOutputBinding<T>
where
    T: Send + Sync + 'static,
{
    fn write<'a>(
        &'a self,
        target: &'a str,
        stream: SendableRecordBatchStream,
    ) -> BoxFuture<'a, Result<()>> {
        (self.write)(target, stream, &self.settings)
    }
}
