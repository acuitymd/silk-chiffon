//! Preserves provider-specific settings types across a runtime-selected provider registry.
//!
//! # The problem
//!
//! A storage provider registration claims URL schemes and supplies a resolver callback that turns
//! a matching [`Location`] into a provider-specific object path and client factory. It may also
//! claim schemeless input and supply a callback that maps the raw text into a [`Location`]. Provider
//! code can live in another crate, so each provider must be free to define its own Clap arguments
//! and receive the parsed values in either callback.
//!
//! The public registration API represents that contract with a concrete settings type `T`.
//! [`super::StorageProviderRegistration::with_args`] selects `T` and accepts only a
//! [`super::ProviderResolver<T>`], while
//! [`super::StorageProviderRegistrationBuilder::bare_locations`] accepts only a
//! [`super::BareLocationMapper<T>`]. Rust therefore checks that both callbacks receive the same
//! type produced by the provider's argument parser.
//!
//! The registry cannot use one `T` for the whole collection. If [`super::StorageRegistry`] had a
//! `T` parameter, choosing `ProviderAArgs` would prevent that registry from also storing a provider
//! whose resolver needs `ProviderBArgs`. The registry must store both, augment one command with all
//! of their arguments, parse them from one [`ArgMatches`], and later select a provider by URL
//! scheme:
//!
//! ```text
//! ProviderDefinition<ProviderAArgs> --+
//!                                      +--> one provider registry
//! ProviderDefinition<ProviderBArgs> --+
//! ```
//!
//! One possible design would store every settings value as [`Any`](std::any::Any), Rust's container
//! for values whose concrete type is checked at runtime. Each resolver would downcast that value by
//! asking whether it contains the expected `T`. `Any` can be wrapped safely, but the downcast still
//! depends on a runtime invariant: the selected resolver and the erased settings must come from the
//! same registration. The compiler cannot check that relationship after the two values have been
//! erased independently. A central settings enum would retain static checking but would require
//! this crate to know every provider's type, preventing other crates from extending the registry
//! independently.
//!
//! # The entities and their responsibilities
//!
//! **Provider** is a role, not a public Rust type. One provider consists of a name, the URL schemes
//! it claims, optional command-line settings, resolution behavior, and optionally a claim on
//! schemeless input. Several types represent that provider at different points in its lifecycle.
//!
//! The public entities divide ownership between provider code and the host executable:
//!
//! - **Provider code**, which may live in a separate crate, defines its settings type `T`, its
//!   [`super::ProviderResolver<T>`], an optional [`super::BareLocationMapper<T>`], and a function
//!   that returns a [`super::StorageProviderRegistration`].
//! - The **host executable** chooses which registrations to include, constructs a
//!   [`super::StorageRegistry`], lets that registry augment its Clap command, and passes the parsed
//!   matches back to the registry.
//! - [`ArgMatches`] is Clap's result from parsing the arguments contributed by the host and every
//!   provider registration.
//! - [`crate::LocationInput`] is the provider-neutral syntax accepted by
//!   [`super::StorageResolver`]. It is either an explicit [`Location`] or raw schemeless text.
//! - [`Location`] is a canonical URL ready for provider resolution. It contains no provider
//!   settings, object-store client, or provider-specific object path.
//! - [`super::BareLocationMapper<T>`] is the optional callback contract for schemeless input. The
//!   registry supplies the raw text and parsed `&T`; provider code assigns its own meaning and
//!   returns a [`Location`] using one of that provider's schemes.
//! - [`super::ProviderResolver<T>`] is the callback contract. `StorageResolver` supplies the
//!   location, the parsed `&T`, and an optional retry configuration. Provider code returns a
//!   [`super::ProviderResolution`].
//! - [`super::StorageAccess`] is the input and output capability declared by provider code.
//!   `StorageResolver` checks it before invoking the provider callback.
//! - [`crate::RetryConfig`] contains shared retry settings parsed by `StorageRegistry`. A callback
//!   receives it only if its registration opted into shared retries, and provider code decides how
//!   to apply it to the object-store client.
//! - [`super::StorageProviderRegistration`] is an unbound provider description. It records the
//!   provider's identity, schemes, command-line behavior, capabilities, and callbacks. It does not
//!   contain settings parsed for a particular command.
//! - [`super::StorageRegistry`] owns a set of those descriptions. During construction it checks
//!   provider names, URL schemes, the unique schemeless-input claim, and indexed argument keys for
//!   collisions. It also builds the routing indexes and contributes every provider's arguments to
//!   the host's Clap command. It can be reused to bind more than one set of command-line matches.
//! - [`super::StorageResolver`] is the runtime state bound to one set of command-line matches. It
//!   owns every registered provider's parsed settings, any shared retry configuration, and the
//!   client cache. It selects a provider when asked to resolve a [`crate::LocationInput`].
//! - [`super::ProviderResolution`] is one callback's answer for one location. It contains the
//!   provider-specific object path and a factory that can create the corresponding object-store
//!   client after a cache miss.
//!
//! This private module supplies the adapters stored inside those public entities:
//!
//! - [`ArgsParser<T>`] carries the Clap operations that add `T`'s arguments and reconstruct `T`
//!   from matches.
//! - [`ProviderDefinition<T>`] is a provider before binding. It keeps `ArgsParser<T>` next to the
//!   callbacks that accept `&T`.
//! - [`BoundProvider<T>`] is that provider after binding. It keeps the parsed `T` next to the same
//!   mapper and resolver callbacks.
//! - [`RegisterProviderArguments`], [`BindProvider`], and [`ResolveProvider`] are the dynamic
//!   interfaces used during registry validation and command composition, binding, and resolution.
//!
//! ## What binding means
//!
//! **Binding** turns a reusable registry description into runtime state for one parsed command.
//! [`super::StorageRegistry::bind_args`] reconstructs each registered provider's `T` from the same
//! [`ArgMatches`], attaches each value to its matching callback, and returns a
//! [`super::StorageResolver`]. Binding does not select a provider, map schemeless input, resolve a
//! [`Location`], or construct an object-store client. Those actions happen later when the host
//! calls the resolver.
//!
//! # From registration to resolution
//!
//! Provider code creates a registration while its settings type is still visible:
//!
//! ```text
//! with_args::<ProviderArgs>("provider", "provider", access, resolve)
//!     .build()
//!
//! resolve: fn(&Location, &ProviderArgs, Option<&RetryConfig>) -> ...
//! ```
//!
//! The complete sequence assigns one actor to each step:
//!
//! 1. The provider crate constructs the registration shown above and gives it to the host.
//! 2. The host adds the registration to `StorageRegistry`, then uses `augment_args` to build the
//!    command it will parse.
//! 3. Clap produces `ArgMatches`, and the host passes those matches to `bind_args`.
//! 4. `StorageRegistry` parses every registered provider's settings and returns `StorageResolver`.
//! 5. The host gives a `LocationInput` to `StorageResolver`. An explicit URL selects its provider
//!    by scheme. Schemless input selects the one provider that claimed the bare-location route,
//!    whose mapper first turns the raw text into a `Location`.
//! 6. `StorageResolver` invokes the selected provider's resolver with the `ProviderArgs` parsed in
//!    step 4.
//!
//! All provider code is already linked into the host executable. The runtime behavior here is
//! selection among explicit registrations, not loading crates or dynamic libraries after the
//! process starts. All registered provider arguments share one flat Clap command. Clap therefore
//! enforces required arguments from every registration when it creates [`ArgMatches`], even if the
//! command later resolves a location for only one provider. A provider that is not available in a
//! build must be omitted from the registry so its schemes and arguments are absent together.
//!
//! # The typed boundary
//!
//! This module keeps each provider in its own generic type until the parser and resolver have been
//! joined. It then stores the provider behind traits that expose the next operation, rather than
//! converting its settings into an untyped value:
//!
//! ```text
//! ArgsParser<T> + BareLocationMapper<T> + ProviderResolver<T>
//!                              |
//!                              v
//!                     ProviderDefinition<T>
//!                              |
//!                       dyn BindProvider
//!                              | bind(matches)
//!                              v
//!                       BoundProvider<T>
//!                              |
//!                      dyn ResolveProvider
//!                              | map/resolve(location)
//!                              v
//!            callbacks receive the same parsed &T
//! ```
//!
//! `dyn BindProvider` and `dyn ResolveProvider` are **trait objects**. Before binding, the registry
//! sees a provider as `dyn BindProvider`. After binding, the resolver sees it as
//! `dyn ResolveProvider`. In each state the caller knows which methods it can use, but the concrete
//! implementing type is hidden. Hiding `T` this way is **type erasure**: the caller loses the name
//! of the settings type without converting the settings into an untyped value. Calling a
//! trait-object method uses **dynamic dispatch**, which selects the concrete implementation at
//! runtime.
//!
//! That concrete implementation still knows `T`. [`ProviderDefinition<T>`] parses `T` and
//! constructs a [`BoundProvider<T>`]. The bound provider later passes `&T` to the matching mapper
//! or resolver without a cast. This gives provider crates concrete types on their side of the API,
//! preserves the typed contract between the provider and host crates, and lets the registry perform
//! runtime selection without knowing those types.
//!
//! The traits divide that process by lifecycle:
//!
//! 1. [`RegisterProviderArguments`] exposes CLI arguments for command augmentation and keys for
//!    registry collision checks.
//! 2. [`BindProvider`] parses a provider's `T` once after Clap has produced matches.
//! 3. [`ResolveProvider`] invokes the typed mapper or resolver after the settings type is hidden
//!    from the registry.
//!
use std::sync::Arc;

use clap::{ArgMatches, Args, Command, FromArgMatches};

use object_store::RetryConfig;

use super::{BareLocationMapper, ProviderResolution, ProviderResolver};
use crate::Location;

#[derive(Clone, Copy)]
/// The two Clap operations the registry needs for one concrete settings type.
///
/// Clap exposes argument definitions by augmenting a [`Command`], then reconstructs `T` from the
/// resulting [`ArgMatches`]. Keeping both operations lets a registration carry that type-specific
/// behavior without storing a command or a settings value.
pub(super) struct ArgsParser<T> {
    augment: fn(Command) -> Command,
    parse: fn(&ArgMatches) -> Result<T, clap::Error>,
}

impl<T> ArgsParser<T> {
    /// Captures the Clap operations implemented by the argument struct `T`.
    pub(super) fn for_args() -> Self
    where
        T: Args + FromArgMatches,
    {
        Self {
            augment: T::augment_args,
            parse: T::from_arg_matches,
        }
    }

    /// Adds the arguments declared by `T` to `command`.
    ///
    /// The registry uses the same operation to compose its public command and to inspect the
    /// provider's argument keys before composition.
    pub(super) fn augment(&self, command: Command) -> Command {
        (self.augment)(command)
    }

    /// Constructs `T` from matches produced by the augmented command.
    pub(super) fn parse(&self, matches: &ArgMatches) -> Result<T, clap::Error> {
        (self.parse)(matches)
    }

    /// Extracts each argument and group ID plus each primary long and short option name.
    ///
    /// [`Args`] exposes this metadata through a [`Command`], so this method augments a scratch
    /// command and converts its arguments into registry collision keys. Aliases are absent because
    /// the registry does not index them.
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
        for group in command.get_groups() {
            let id = group.get_id().as_str().to_owned();
            keys.push((format!("id:{id}"), id.clone()));
        }
        keys
    }
}

impl ArgsParser<()> {
    /// Uses `()` as the settings type for a provider with no command-line arguments.
    ///
    /// The no-op operations let settings-free providers follow the same typed registration and
    /// binding path as providers with argument structs.
    pub(super) fn unit() -> Self {
        Self {
            augment: |command| command,
            parse: |_| Ok(()),
        }
    }
}

/// The runtime interface shared by registry validation and command composition.
///
/// Registrations use this interface for command composition and collision checks before binding.
pub(super) trait RegisterProviderArguments: Send + Sync {
    /// Adds this provider's arguments to the command being composed.
    fn augment(&self, command: Command) -> Command;

    /// Returns the keys used to detect collisions with other storage arguments.
    fn argument_keys(&self) -> Vec<(String, String)>;
}

/// The runtime interface for turning command matches into a bound provider.
///
/// The concrete implementation still knows `T`. It parses `T`, pairs the value with its matching
/// typed callbacks, and erases the group as a [`ResolveProvider`]. The registry can therefore bind
/// providers with different settings types without recovering those types by downcasting.
pub(super) trait BindProvider: RegisterProviderArguments {
    /// Parses this provider's settings once and returns a resolver that owns them.
    fn bind(&self, matches: &ArgMatches) -> Result<Arc<dyn ResolveProvider>, clap::Error>;
}

/// The runtime interface used after a provider's settings have been bound.
///
/// A [`BoundProvider<T>`] implements this interface while retaining `T` internally. Dynamic
/// dispatch hides the type from [`super::StorageResolver`], but the provider callbacks still
/// receive `&T`.
pub(super) trait ResolveProvider: Send + Sync {
    /// Maps schemeless input with this provider's bound settings when it claimed that route.
    fn map_bare_location(&self, input: &str) -> Option<anyhow::Result<Location>>;

    /// Invokes the typed provider callback with its bound settings and retry configuration.
    fn resolve(
        &self,
        location: &Location,
        retry: Option<&RetryConfig>,
    ) -> anyhow::Result<ProviderResolution>;
}

/// A provider before command arguments have been bound.
///
/// The parser, mapper, and resolver share the same `T`, which makes a mismatched group a
/// compile-time error. This value is erased as [`BindProvider`] only after that relationship has
/// been established.
pub(super) struct ProviderDefinition<T> {
    args: ArgsParser<T>,
    bare_location_mapper: Option<BareLocationMapper<T>>,
    resolver: ProviderResolver<T>,
}

impl<T> ProviderDefinition<T> {
    /// Pairs a settings parser with the typed callbacks that consume its output.
    pub(super) fn new(
        args: ArgsParser<T>,
        bare_location_mapper: Option<BareLocationMapper<T>>,
        resolver: ProviderResolver<T>,
    ) -> Self {
        Self {
            args,
            bare_location_mapper,
            resolver,
        }
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
            bare_location_mapper: self.bare_location_mapper,
            resolver: self.resolver,
        }))
    }
}

/// A provider after binding, with its concrete settings still intact.
///
/// [`BindProvider::bind`] returns this value as an `Arc<dyn ResolveProvider>`. Dynamic dispatch
/// enters this implementation, where `T` is known and can be passed to the matching mapper or
/// resolver without a cast.
struct BoundProvider<T> {
    settings: T,
    bare_location_mapper: Option<BareLocationMapper<T>>,
    resolver: ProviderResolver<T>,
}

impl<T> ResolveProvider for BoundProvider<T>
where
    T: Send + Sync + 'static,
{
    fn map_bare_location(&self, input: &str) -> Option<anyhow::Result<Location>> {
        self.bare_location_mapper
            .map(|mapper| mapper(input, &self.settings))
    }

    fn resolve(
        &self,
        location: &Location,
        retry: Option<&RetryConfig>,
    ) -> anyhow::Result<ProviderResolution> {
        (self.resolver)(location, &self.settings, retry)
    }
}
