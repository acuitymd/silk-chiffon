use std::any::Any;
use std::{future::Future, pin::Pin};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use silk_chiffon_storage::ResolvedLocation;

use super::{
    FormatCapability, FormatRuntimeError, Inspector, SinkFactory, SinkFactoryContext, SourceFactory,
};
use crate::{DataSinkFactory, DataSource, InspectionOutput};

pub(super) struct Settings(Box<dyn Any + Send + Sync>);

impl Settings {
    pub(super) fn new<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self(Box::new(value))
    }

    pub(super) fn unit() -> Self {
        Self::new(())
    }

    fn typed<T: 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

type CliParser = fn(&ArgMatches) -> Result<Settings, clap::Error>;

#[derive(Clone, Copy)]
pub(super) struct CliContribution {
    augment: fn(Command) -> Command,
    parse: CliParser,
}

impl CliContribution {
    pub(super) fn for_args<T>() -> Self
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        Self {
            augment: T::augment_args,
            parse: |matches| T::from_arg_matches(matches).map(Settings::new),
        }
    }

    pub(super) fn unit() -> Self {
        Self {
            augment: |command| command,
            parse: |_| Ok(Settings::unit()),
        }
    }

    pub(super) fn augment(self, command: Command) -> Command {
        (self.augment)(command)
    }

    pub(super) fn parse(self, matches: &ArgMatches) -> Result<Settings, clap::Error> {
        (self.parse)(matches)
    }

    pub(super) fn argument_keys(self) -> Vec<(String, String)> {
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

type RuntimeFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, FormatRuntimeError>> + Send + 'a>>;

pub(super) trait TransformRuntime: Send + Sync {
    fn has_source(&self) -> bool;

    fn has_sink(&self) -> bool;

    fn create_source<'a>(
        &'a self,
        format: &'static str,
        location: &'a ResolvedLocation,
        settings: &'a Settings,
    ) -> RuntimeFuture<'a, Box<dyn DataSource>>;

    fn create_sink_factory<'a>(
        &'a self,
        format: &'static str,
        context: &'a SinkFactoryContext,
        settings: &'a Settings,
    ) -> RuntimeFuture<'a, Box<dyn DataSinkFactory>>;
}

pub(super) struct TypedTransform<T> {
    source: Option<SourceFactory<T>>,
    sink: Option<SinkFactory<T>>,
}

impl<T> TypedTransform<T> {
    pub(super) fn new(source: Option<SourceFactory<T>>, sink: Option<SinkFactory<T>>) -> Self {
        Self { source, sink }
    }
}

impl<T> TransformRuntime for TypedTransform<T>
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
        settings: &'a Settings,
    ) -> RuntimeFuture<'a, Box<dyn DataSource>> {
        let Some(settings) = settings.typed::<T>() else {
            return Box::pin(
                async move { Err(FormatRuntimeError::SettingsTypeMismatch { format }) },
            );
        };
        let Some(source) = self.source else {
            return Box::pin(async move {
                Err(FormatRuntimeError::CapabilityUnavailable {
                    format,
                    capability: FormatCapability::Source,
                })
            });
        };

        Box::pin(async move {
            source(location, settings)
                .await
                .map_err(|source| FormatRuntimeError::CallbackFailed {
                    format,
                    capability: FormatCapability::Source,
                    source,
                })
        })
    }

    fn create_sink_factory<'a>(
        &'a self,
        format: &'static str,
        context: &'a SinkFactoryContext,
        settings: &'a Settings,
    ) -> RuntimeFuture<'a, Box<dyn DataSinkFactory>> {
        let Some(settings) = settings.typed::<T>() else {
            return Box::pin(
                async move { Err(FormatRuntimeError::SettingsTypeMismatch { format }) },
            );
        };
        let Some(sink) = self.sink else {
            return Box::pin(async move {
                Err(FormatRuntimeError::CapabilityUnavailable {
                    format,
                    capability: FormatCapability::Sink,
                })
            });
        };

        Box::pin(async move {
            sink(context, settings)
                .await
                .map_err(|source| FormatRuntimeError::CallbackFailed {
                    format,
                    capability: FormatCapability::Sink,
                    source,
                })
        })
    }
}

pub(super) trait InspectionRuntime: Send + Sync {
    fn inspect<'a>(
        &'a self,
        format: &'static str,
        location: &'a ResolvedLocation,
        settings: &'a Settings,
    ) -> RuntimeFuture<'a, InspectionOutput>;
}

pub(super) struct TypedInspection<T> {
    inspector: Inspector<T>,
}

impl<T> TypedInspection<T> {
    pub(super) fn new(inspector: Inspector<T>) -> Self {
        Self { inspector }
    }
}

impl<T> InspectionRuntime for TypedInspection<T>
where
    T: Send + Sync + 'static,
{
    fn inspect<'a>(
        &'a self,
        format: &'static str,
        location: &'a ResolvedLocation,
        settings: &'a Settings,
    ) -> RuntimeFuture<'a, InspectionOutput> {
        let Some(settings) = settings.typed::<T>() else {
            return Box::pin(
                async move { Err(FormatRuntimeError::SettingsTypeMismatch { format }) },
            );
        };
        let inspector = self.inspector;

        Box::pin(async move {
            inspector(location, settings).await.map_err(|source| {
                FormatRuntimeError::CallbackFailed {
                    format,
                    capability: FormatCapability::Inspection,
                    source,
                }
            })
        })
    }
}
