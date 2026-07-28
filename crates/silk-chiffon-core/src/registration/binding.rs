use std::{future::Future, pin::Pin, sync::Arc};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use silk_chiffon_storage::ResolvedLocation;

use super::{
    FormatCapability, FormatInvocationError, Inspector, SinkFactory, SinkFactoryContext,
    SourceFactory,
};
use crate::{DataSinkFactory, DataSource, InspectionOutput};

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
        let command = self.augment(Command::new("format"));
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

type CallbackFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, FormatInvocationError>> + Send + 'a>>;

pub(super) trait BindTransform: Send + Sync {
    fn has_source(&self) -> bool;

    fn has_sink(&self) -> bool;

    fn augment(&self, command: Command) -> Command;

    fn argument_keys(&self) -> Vec<(String, String)>;

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn InvokeTransform>, clap::Error>;
}

pub(super) trait InvokeTransform: Send + Sync {
    fn has_source(&self) -> bool;

    fn has_sink(&self) -> bool;

    fn create_source<'a>(
        &'a self,
        format: &'static str,
        location: &'a ResolvedLocation,
    ) -> CallbackFuture<'a, Box<dyn DataSource>>;

    fn create_sink_factory<'a>(
        &'a self,
        format: &'static str,
        context: &'a SinkFactoryContext,
    ) -> CallbackFuture<'a, Box<dyn DataSinkFactory>>;
}

pub(super) struct TransformDefinition<T> {
    args: ArgsParser<T>,
    source: Option<SourceFactory<T>>,
    sink: Option<SinkFactory<T>>,
}

impl<T> TransformDefinition<T> {
    pub(super) fn new(
        args: ArgsParser<T>,
        source: Option<SourceFactory<T>>,
        sink: Option<SinkFactory<T>>,
    ) -> Self {
        Self { args, source, sink }
    }
}

impl<T> BindTransform for TransformDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn has_source(&self) -> bool {
        self.source.is_some()
    }

    fn has_sink(&self) -> bool {
        self.sink.is_some()
    }

    fn augment(&self, command: Command) -> Command {
        self.args.augment(command)
    }

    fn argument_keys(&self) -> Vec<(String, String)> {
        self.args.argument_keys()
    }

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn InvokeTransform>, clap::Error> {
        Ok(Arc::new(BoundTransform {
            settings: self.args.parse(matches)?,
            source: self.source,
            sink: self.sink,
        }))
    }
}

struct BoundTransform<T> {
    settings: T,
    source: Option<SourceFactory<T>>,
    sink: Option<SinkFactory<T>>,
}

impl<T> InvokeTransform for BoundTransform<T>
where
    T: Send + Sync + 'static,
{
    fn has_source(&self) -> bool {
        self.source.is_some()
    }

    fn has_sink(&self) -> bool {
        self.sink.is_some()
    }

    fn create_source<'a>(
        &'a self,
        format: &'static str,
        location: &'a ResolvedLocation,
    ) -> CallbackFuture<'a, Box<dyn DataSource>> {
        let Some(source) = self.source else {
            return Box::pin(async move {
                Err(FormatInvocationError::CapabilityUnavailable {
                    format,
                    capability: FormatCapability::Source,
                })
            });
        };

        Box::pin(async move {
            source(location, &self.settings).await.map_err(|source| {
                FormatInvocationError::CallbackFailed {
                    format,
                    capability: FormatCapability::Source,
                    source,
                }
            })
        })
    }

    fn create_sink_factory<'a>(
        &'a self,
        format: &'static str,
        context: &'a SinkFactoryContext,
    ) -> CallbackFuture<'a, Box<dyn DataSinkFactory>> {
        let Some(sink) = self.sink else {
            return Box::pin(async move {
                Err(FormatInvocationError::CapabilityUnavailable {
                    format,
                    capability: FormatCapability::Sink,
                })
            });
        };

        Box::pin(async move {
            sink(context, &self.settings).await.map_err(|source| {
                FormatInvocationError::CallbackFailed {
                    format,
                    capability: FormatCapability::Sink,
                    source,
                }
            })
        })
    }
}

pub(super) trait BindInspection: Send + Sync {
    fn augment(&self, command: Command) -> Command;

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn InvokeInspection>, clap::Error>;
}

pub(super) trait InvokeInspection: Send + Sync {
    fn inspect<'a>(
        &'a self,
        format: &'static str,
        location: &'a ResolvedLocation,
    ) -> CallbackFuture<'a, InspectionOutput>;
}

pub(super) struct InspectionDefinition<T> {
    args: ArgsParser<T>,
    inspector: Inspector<T>,
}

impl<T> InspectionDefinition<T> {
    pub(super) fn new(args: ArgsParser<T>, inspector: Inspector<T>) -> Self {
        Self { args, inspector }
    }
}

impl<T> BindInspection for InspectionDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn augment(&self, command: Command) -> Command {
        self.args.augment(command)
    }

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn InvokeInspection>, clap::Error> {
        Ok(Arc::new(BoundInspection {
            settings: self.args.parse(matches)?,
            inspector: self.inspector,
        }))
    }
}

struct BoundInspection<T> {
    settings: T,
    inspector: Inspector<T>,
}

impl<T> InvokeInspection for BoundInspection<T>
where
    T: Send + Sync + 'static,
{
    fn inspect<'a>(
        &'a self,
        format: &'static str,
        location: &'a ResolvedLocation,
    ) -> CallbackFuture<'a, InspectionOutput> {
        Box::pin(async move {
            (self.inspector)(location, &self.settings)
                .await
                .map_err(|source| FormatInvocationError::CallbackFailed {
                    format,
                    capability: FormatCapability::Inspection,
                    source,
                })
        })
    }
}
