//! Private type-erasure boundary for format definitions and command bindings.
//!
//! A host must store formats contributed by unrelated crates in one registry, even though each
//! format may define a different command-state type constructed from Clap matches. Erasing only
//! that state as [`std::any::Any`] would make the relationship between a state value and the
//! functions that accept it a runtime convention. A mistaken downcast or index could pair the
//! wrong values.
//!
//! This module erases a complete typed definition instead. [`TypedTransformDefinition<T>`] retains
//! the parser and functions that use `T`. Binding parses `T` and produces a
//! [`TypedTransformBinding<T>`], which retains that value with the same functions for one command
//! invocation. The registry sees only the behavior traits, while format crates keep strong types
//! within their own code and across their contract with the host.
//!
//! Inspection uses the same boundary with a separate settings type because its CLI scope and
//! lifecycle are independent of transform.

use std::{future::Future, pin::Pin, sync::Arc};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_storage::InputObject;

use super::{
    FormatOperation, FormatOperationError, InputProviderFn, InspectorFn, PresentationMode,
    SinkBinderFn, SinkBindingConfig,
};
use crate::{FileInputGroup, InspectionOutput, SinkBinding};

/// The two Clap operations that must stay paired for one concrete command-value type.
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

/// An erased operation result that still borrows its typed command binding.
type BindingFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, FormatOperationError>> + Send + 'a>>;

/// Definition-time transform behavior stored by the registry.
pub(super) trait ErasedTransformDefinition: Send + Sync {
    fn has_input_provider(&self) -> bool;

    fn has_sink(&self) -> bool;

    fn augment(&self, command: Command) -> Command;

    fn argument_keys(&self) -> Vec<(String, String)>;

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ErasedTransformBinding>, clap::Error>;
}

/// Invocation-time transform behavior after one command-state value has been constructed.
pub(super) trait ErasedTransformBinding: Send + Sync {
    fn has_input_provider(&self) -> bool;

    fn has_sink(&self) -> bool;

    fn create_input_provider<'a>(
        &'a self,
        format: &'static str,
        group: &'a FileInputGroup,
        session: &'a SessionContext,
    ) -> BindingFuture<'a, Arc<dyn TableProvider>>;

    fn bind_sink<'a>(
        &'a self,
        format: &'static str,
        context: &'a SinkBindingConfig,
    ) -> BindingFuture<'a, Box<dyn SinkBinding>>;
}

/// A definition whose parser and input-provider or sink functions share state type `T`.
pub(super) struct TypedTransformDefinition<T> {
    args: ArgsParser<T>,
    input_provider: Option<InputProviderFn<T>>,
    sink: Option<SinkBinderFn<T>>,
}

impl<T> TypedTransformDefinition<T> {
    pub(super) fn new(
        args: ArgsParser<T>,
        input_provider: Option<InputProviderFn<T>>,
        sink: Option<SinkBinderFn<T>>,
    ) -> Self {
        Self {
            args,
            input_provider,
            sink,
        }
    }
}

impl<T> ErasedTransformDefinition for TypedTransformDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn has_input_provider(&self) -> bool {
        self.input_provider.is_some()
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

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ErasedTransformBinding>, clap::Error> {
        Ok(Arc::new(TypedTransformBinding {
            state: self.args.parse(matches)?,
            input_provider: self.input_provider,
            sink: self.sink,
        }))
    }
}

/// One command-state `T` retained with the input-provider and sink functions that accept it.
struct TypedTransformBinding<T> {
    state: T,
    input_provider: Option<InputProviderFn<T>>,
    sink: Option<SinkBinderFn<T>>,
}

impl<T> ErasedTransformBinding for TypedTransformBinding<T>
where
    T: Send + Sync + 'static,
{
    fn has_input_provider(&self) -> bool {
        self.input_provider.is_some()
    }

    fn has_sink(&self) -> bool {
        self.sink.is_some()
    }

    fn create_input_provider<'a>(
        &'a self,
        format: &'static str,
        group: &'a FileInputGroup,
        session: &'a SessionContext,
    ) -> BindingFuture<'a, Arc<dyn TableProvider>> {
        let Some(input_provider) = self.input_provider else {
            return Box::pin(async move {
                Err(FormatOperationError::Unsupported {
                    format,
                    operation: FormatOperation::InputProviderCreation,
                })
            });
        };

        Box::pin(async move {
            input_provider(group, session, &self.state)
                .await
                .map_err(|source| FormatOperationError::Failed {
                    format,
                    operation: FormatOperation::InputProviderCreation,
                    source,
                })
        })
    }

    fn bind_sink<'a>(
        &'a self,
        format: &'static str,
        context: &'a SinkBindingConfig,
    ) -> BindingFuture<'a, Box<dyn SinkBinding>> {
        let Some(sink) = self.sink else {
            return Box::pin(async move {
                Err(FormatOperationError::Unsupported {
                    format,
                    operation: FormatOperation::SinkBinding,
                })
            });
        };

        Box::pin(async move {
            sink(context, &self.state)
                .await
                .map_err(|source| FormatOperationError::Failed {
                    format,
                    operation: FormatOperation::SinkBinding,
                    source,
                })
        })
    }
}

/// Definition-time inspection behavior stored by the registry.
pub(super) trait ErasedInspectionDefinition: Send + Sync {
    fn augment(&self, command: Command) -> Command;

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ErasedInspectionBinding>, clap::Error>;
}

/// Invocation-time inspection behavior after one settings value has been parsed.
pub(super) trait ErasedInspectionBinding: Send + Sync {
    fn inspect<'a>(
        &'a self,
        format: &'static str,
        object: &'a InputObject,
        mode: PresentationMode,
    ) -> BindingFuture<'a, InspectionOutput>;
}

/// An inspection parser paired with the function that accepts its settings type `T`.
pub(super) struct TypedInspectionDefinition<T> {
    args: ArgsParser<T>,
    inspector: InspectorFn<T>,
}

impl<T> TypedInspectionDefinition<T> {
    pub(super) fn new(args: ArgsParser<T>, inspector: InspectorFn<T>) -> Self {
        Self { args, inspector }
    }
}

impl<T> ErasedInspectionDefinition for TypedInspectionDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn augment(&self, command: Command) -> Command {
        self.args.augment(command)
    }

    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ErasedInspectionBinding>, clap::Error> {
        Ok(Arc::new(TypedInspectionBinding {
            settings: self.args.parse(matches)?,
            inspector: self.inspector,
        }))
    }
}

/// One parsed inspection settings value retained with its matching function.
struct TypedInspectionBinding<T> {
    settings: T,
    inspector: InspectorFn<T>,
}

impl<T> ErasedInspectionBinding for TypedInspectionBinding<T>
where
    T: Send + Sync + 'static,
{
    fn inspect<'a>(
        &'a self,
        format: &'static str,
        object: &'a InputObject,
        mode: PresentationMode,
    ) -> BindingFuture<'a, InspectionOutput> {
        Box::pin(async move {
            (self.inspector)(object, mode, &self.settings)
                .await
                .map_err(|source| FormatOperationError::Failed {
                    format,
                    operation: FormatOperation::Inspection,
                    source,
                })
        })
    }
}
