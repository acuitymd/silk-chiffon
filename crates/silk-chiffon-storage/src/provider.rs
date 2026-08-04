//! Typed storage-provider registration and command-scoped resolution.
//!
//! Each enabled registration binds one concrete Clap argument type to its resolver. A disabled
//! registration retains its argument type and a diagnostic. The registry can hold both without
//! separating settings from the resolver that understands them.
//!
//! Provider setup has two stages. [`StorageRegistry`] first collects registrations, validates their
//! names, schemes, and CLI arguments, and augments a Clap command. [`StorageRegistry::bind_args`]
//! then parses each enabled provider's settings once and produces a [`StorageResolver`] whose
//! clones share those settings and a command-scoped client cache.

mod binding;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use object_store::{ObjectStore, RetryConfig, path::Path as ObjectPath};
use thiserror::Error;
use url::Url;

use crate::{Location, ResolvedLocation, RetryArgs, RetryConfigurationError, StorageError};

/// Resolves one provider location using settings registered as `T`.
///
/// The registry invokes this function after it has selected the location's scheme and enforced the
/// provider's [`StorageAccess`]. `settings` is the value parsed once by
/// [`StorageRegistry::bind_args`]. `retry` is present only when this registration opted into
/// [`StorageProviderRegistrationBuilder::shared_retries`].
///
/// The resolver returns the per-location object path and a lazy store factory. It should validate
/// any provider-owned URL rules before returning. Callback and factory errors are wrapped in
/// [`StorageError::ProviderResolution`] with the provider name and requested direction.
///
/// This alias is a function pointer, so it cannot capture state. A returned `'static` factory must
/// own or clone anything it needs from `settings` or `retry`.
pub type ProviderResolver<T> = fn(
    location: &Location,
    settings: &T,
    retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution>;

/// A provider's object path and lazy client factory.
///
/// [`ProviderResolver`] runs for every location because the object path can change.
/// [`StorageResolver`] invokes the returned factory only when its command-scoped cache has no
/// client for the URL origin.
pub struct ProviderResolution {
    store_factory: Box<dyn FnOnce() -> anyhow::Result<Arc<dyn ObjectStore>> + Send>,
    path: ObjectPath,
}

impl ProviderResolution {
    /// Creates a resolution whose client is constructed only after a cache miss.
    ///
    /// `path` must use the namespace expected by the store returned from `factory`. Factory
    /// failures receive provider and direction context from [`StorageResolver`]. Stores are cached
    /// by scheme, host, and port, so the factory must create an equivalent client for every path
    /// and query under the same origin.
    pub fn from_factory(
        path: ObjectPath,
        factory: impl FnOnce() -> anyhow::Result<Arc<dyn ObjectStore>> + Send + 'static,
    ) -> Self {
        Self {
            store_factory: Box::new(factory),
            path,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Whether a location will be read as an input or written as an output.
pub enum StorageDirection {
    /// Resolve the location for reading.
    Input,
    /// Resolve the location for writing.
    Output,
}

impl fmt::Display for StorageDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Input => "input",
            Self::Output => "output",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// The input and output directions supported by a provider.
///
/// [`StorageResolver`] enforces this declaration before invoking the provider callback.
pub enum StorageAccess {
    /// The provider resolves inputs only.
    ReadOnly,
    /// The provider resolves outputs only.
    WriteOnly,
    /// The provider resolves both inputs and outputs.
    ReadWrite,
}

impl StorageAccess {
    const fn supports(self, direction: StorageDirection) -> bool {
        matches!(
            (self, direction),
            (Self::ReadOnly | Self::ReadWrite, StorageDirection::Input)
                | (Self::WriteOnly | Self::ReadWrite, StorageDirection::Output)
        )
    }
}

#[derive(Clone)]
enum ProviderRegistration {
    Enabled {
        access: StorageAccess,
        binder: Arc<dyn binding::BindProvider>,
    },
    Disabled {
        diagnostic: &'static str,
        arguments: Arc<dyn binding::RegisterProviderArguments>,
    },
}

/// One provider's identity, argument contribution, and enabled or disabled behavior.
///
/// Registrations are immutable descriptions that can be cloned into more than one registry. Use
/// [`Self::with_args`] or [`Self::without_args`] to start a registration and finish it with
/// [`StorageProviderRegistrationBuilder::enabled`] or
/// [`StorageProviderRegistrationBuilder::disabled`].
///
/// A provider registration follows this order:
///
/// 1. Choose whether the provider has a settings type.
/// 2. Add every URL scheme that selects it.
/// 3. Opt into shared retries if its client supports them.
/// 4. Finish with an access declaration and resolver, or with a disabled diagnostic.
#[derive(Clone)]
pub struct StorageProviderRegistration {
    name: &'static str,
    schemes: Vec<&'static str>,
    provider: ProviderRegistration,
    uses_shared_retries: bool,
}

impl StorageProviderRegistration {
    /// Starts a registration whose settings are parsed from the command line as `T`.
    ///
    /// The registry adds `T`'s provider-specific Clap arguments to its command. An enabled
    /// registration parses `T` once during [`StorageRegistry::bind_args`]. A disabled registration
    /// retains the arguments without constructing `T` during binding.
    pub fn with_args<T>(name: &'static str) -> StorageProviderRegistrationBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        StorageProviderRegistrationBuilder {
            name,
            schemes: Vec::new(),
            args: binding::ArgsParser::for_args(),
            uses_shared_retries: false,
            settings: PhantomData,
        }
    }

    /// Starts a registration that contributes no provider-specific CLI arguments.
    pub fn without_args(name: &'static str) -> StorageProviderRegistrationBuilder<()> {
        StorageProviderRegistrationBuilder {
            name,
            schemes: Vec::new(),
            args: binding::ArgsParser::unit(),
            uses_shared_retries: false,
            settings: PhantomData,
        }
    }

    /// Returns the provider name used in diagnostics and duplicate detection.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the URL schemes claimed by this provider.
    pub fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    /// Returns whether this enabled registration accepts input resolution.
    ///
    /// Disabled registrations return `false` regardless of their intended capability.
    pub fn has_input(&self) -> bool {
        matches!(&self.provider, ProviderRegistration::Enabled { access, .. } if access.supports(StorageDirection::Input))
    }

    /// Returns whether this enabled registration accepts output resolution.
    ///
    /// Disabled registrations return `false` regardless of their intended capability.
    pub fn has_output(&self) -> bool {
        matches!(&self.provider, ProviderRegistration::Enabled { access, .. } if access.supports(StorageDirection::Output))
    }

    /// Returns whether this registration requests the registry's shared retry settings.
    pub const fn uses_shared_retries(&self) -> bool {
        self.uses_shared_retries
    }

    fn augment_args(&self, command: Command) -> Command {
        match &self.provider {
            ProviderRegistration::Enabled { binder, .. } => binder.augment(command),
            ProviderRegistration::Disabled { arguments, .. } => arguments.augment(command),
        }
    }

    fn argument_keys(&self) -> Vec<(String, String)> {
        match &self.provider {
            ProviderRegistration::Enabled { binder, .. } => binder.argument_keys(),
            ProviderRegistration::Disabled { arguments, .. } => arguments.argument_keys(),
        }
    }
}

/// Builds one typed storage-provider registration.
///
/// The type parameter keeps a provider's Clap settings paired with the resolver that accepts them.
/// Finishing the builder erases that concrete type behind the registry's private behavior
/// interface.
pub struct StorageProviderRegistrationBuilder<T> {
    name: &'static str,
    schemes: Vec<&'static str>,
    args: binding::ArgsParser<T>,
    uses_shared_retries: bool,
    settings: PhantomData<fn() -> T>,
}

impl<T> StorageProviderRegistrationBuilder<T>
where
    T: Send + Sync + 'static,
{
    /// Adds the URL schemes that select this provider.
    ///
    /// Scheme lookup and duplicate detection are ASCII case-insensitive. Duplicate schemes are
    /// reported when the containing registry is built. A registration without a scheme remains
    /// visible through iteration but cannot be selected for resolution.
    pub fn schemes(mut self, schemes: impl IntoIterator<Item = &'static str>) -> Self {
        self.schemes.extend(schemes);
        self
    }

    /// Passes the registry's shared retry configuration to this provider's resolver.
    ///
    /// One shared argument group is contributed when at least one registration opts in. Providers
    /// that do not opt in receive `None` even when the group exists for another provider. The
    /// provider must apply the received configuration to its object-store client.
    pub fn shared_retries(mut self) -> Self {
        self.uses_shared_retries = true;
        self
    }

    /// Finishes an enabled registration with its access declaration and typed resolver.
    ///
    /// The access declaration is enforced before `resolver` runs. The same callback serves every
    /// allowed direction and does not receive the direction as an argument.
    pub fn enabled(
        self,
        access: StorageAccess,
        resolver: ProviderResolver<T>,
    ) -> StorageProviderRegistration {
        StorageProviderRegistration {
            name: self.name,
            schemes: self.schemes,
            provider: ProviderRegistration::Enabled {
                access,
                binder: Arc::new(binding::ProviderDefinition::new(self.args, resolver)),
            },
            uses_shared_retries: self.uses_shared_retries,
        }
    }

    /// Finishes a disabled registration that reserves its name, schemes, and CLI arguments.
    ///
    /// Disabled registrations keep command help and scheme diagnostics stable across feature sets.
    /// Resolution returns [`StorageError::ProviderDisabled`] with `diagnostic` without invoking a
    /// provider callback.
    pub fn disabled(self, diagnostic: &'static str) -> StorageProviderRegistration {
        StorageProviderRegistration {
            name: self.name,
            schemes: self.schemes,
            provider: ProviderRegistration::Disabled {
                diagnostic,
                arguments: Arc::new(binding::ProviderArguments::new(self.args)),
            },
            uses_shared_retries: self.uses_shared_retries,
        }
    }
}

/// Errors that make a set of provider registrations ambiguous.
#[derive(Debug, Error)]
pub enum StorageRegistryError {
    /// Two registrations use the same provider name, ignoring ASCII case.
    #[error("duplicate storage provider name: {0}")]
    DuplicateName(String),
    /// Two registrations claim the same URL scheme, ignoring ASCII case.
    #[error("duplicate storage scheme: {0}")]
    DuplicateScheme(String),
    /// Provider or retry arguments reuse a Clap ID or primary long or short option.
    #[error("duplicate storage CLI argument: {0}")]
    DuplicateCliArgument(String),
}

/// Collects provider registrations before validating and indexing them.
pub struct StorageRegistryBuilder {
    registrations: Vec<StorageProviderRegistration>,
}

impl StorageRegistryBuilder {
    /// Adds a provider registration in iteration and CLI augmentation order.
    ///
    /// Validation is deferred to [`Self::build`] so collisions can be checked across the complete
    /// set.
    pub fn register(mut self, registration: StorageProviderRegistration) -> Self {
        self.registrations.push(registration);
        self
    }

    /// Validates all registrations and builds their scheme index.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRegistryError`] for duplicate provider names, schemes, or Clap identifiers.
    /// Provider names and schemes are compared without ASCII case. Clap IDs and primary long and
    /// short option spellings are compared exactly. Aliases are not indexed.
    pub fn build(self) -> Result<StorageRegistry, StorageRegistryError> {
        StorageRegistry::from_registrations(self.registrations)
    }
}

/// Provider definitions, scheme lookup, and CLI composition for one executable.
///
/// A registry is configuration, not parsed command state. It can augment commands and bind
/// independent argument sets. Each call to [`Self::bind_args`] creates a resolver with its own
/// parsed settings, retry configuration, and store cache.
pub struct StorageRegistry {
    registrations: Vec<StorageProviderRegistration>,
    schemes: HashMap<String, usize>,
    uses_shared_retries: bool,
}

impl StorageRegistry {
    /// Starts an empty provider registry.
    pub fn builder() -> StorageRegistryBuilder {
        StorageRegistryBuilder {
            registrations: Vec::new(),
        }
    }

    fn from_registrations(
        registrations: Vec<StorageProviderRegistration>,
    ) -> Result<Self, StorageRegistryError> {
        let mut names = HashMap::new();
        let mut schemes = HashMap::new();
        let mut cli_arguments = HashSet::new();
        let uses_shared_retries = registrations
            .iter()
            .any(StorageProviderRegistration::uses_shared_retries);

        if uses_shared_retries {
            for (key, _) in argument_keys::<RetryArgs>("storage retries") {
                cli_arguments.insert(key);
            }
        }

        for (index, registration) in registrations.iter().enumerate() {
            let name = registration.name.to_ascii_lowercase();
            if names.insert(name.clone(), index).is_some() {
                return Err(StorageRegistryError::DuplicateName(name));
            }

            for scheme in &registration.schemes {
                let scheme = scheme.to_ascii_lowercase();
                if schemes.insert(scheme.clone(), index).is_some() {
                    return Err(StorageRegistryError::DuplicateScheme(scheme));
                }
            }

            for (key, argument) in registration.argument_keys() {
                if !cli_arguments.insert(key) {
                    return Err(StorageRegistryError::DuplicateCliArgument(argument));
                }
            }
        }

        Ok(Self {
            registrations,
            schemes,
            uses_shared_retries,
        })
    }

    /// Iterates registrations in the order they were added to the builder.
    pub fn registrations(&self) -> impl Iterator<Item = &StorageProviderRegistration> {
        self.registrations.iter()
    }

    /// Finds the registration for a URL scheme using ASCII case-insensitive lookup.
    pub fn by_scheme(&self, scheme: &str) -> Option<&StorageProviderRegistration> {
        self.schemes
            .get(&scheme.to_ascii_lowercase())
            .map(|index| &self.registrations[*index])
    }

    /// Adds shared retry arguments and every provider's Clap arguments.
    ///
    /// The returned command should be used to produce the [`ArgMatches`] later passed to
    /// [`Self::bind_args`]. Disabled providers still contribute their arguments. Registry
    /// validation does not inspect arguments already present on `command`, so those IDs and option
    /// spellings must not collide with storage arguments.
    pub fn augment_args(&self, mut command: Command) -> Command {
        if self.uses_shared_retries {
            command = RetryArgs::augment_args(command);
        }
        for registration in &self.registrations {
            command = registration.augment_args(command);
        }
        command
    }

    /// Binds one command's parsed arguments into a resolver and a fresh client cache.
    ///
    /// Each enabled provider's concrete settings type is parsed once. Clones of the returned
    /// resolver share those settings and its cache, while a later call to `bind_args` creates
    /// independent parsed settings, retry configuration, and cache.
    ///
    /// # Errors
    ///
    /// Returns [`StorageResolverBuildError`] when provider arguments cannot be parsed or shared
    /// retry settings are invalid.
    pub fn bind_args(
        &self,
        matches: &ArgMatches,
    ) -> Result<StorageResolver, StorageResolverBuildError> {
        let retry = if self.uses_shared_retries {
            Some(RetryArgs::from_arg_matches(matches)?.into_retry_config()?)
        } else {
            None
        };

        let mut providers = Vec::with_capacity(self.registrations.len());
        for registration in &self.registrations {
            providers.push(match &registration.provider {
                ProviderRegistration::Enabled { access, binder } => BoundProvider::Enabled {
                    name: registration.name,
                    access: *access,
                    resolver: binder.bind(matches)?,
                    uses_shared_retries: registration.uses_shared_retries,
                },
                ProviderRegistration::Disabled { diagnostic, .. } => BoundProvider::Disabled {
                    name: registration.name,
                    diagnostic,
                },
            });
        }

        Ok(StorageResolver {
            providers: Arc::new(providers),
            schemes: Arc::new(self.schemes.clone()),
            retry,
            stores: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

enum BoundProvider {
    Enabled {
        name: &'static str,
        access: StorageAccess,
        resolver: Arc<dyn binding::ResolveProvider>,
        uses_shared_retries: bool,
    },
    Disabled {
        name: &'static str,
        diagnostic: &'static str,
    },
}

/// Provider resolvers, retry settings, and cached clients bound to one command.
///
/// Cloning a resolver shares the bound provider settings and client cache. Stores are keyed by URL
/// origin, so paths and queries under the same scheme, host, and port reuse one [`ObjectStore`]
/// client.
#[derive(Clone)]
pub struct StorageResolver {
    providers: Arc<Vec<BoundProvider>>,
    schemes: Arc<HashMap<String, usize>>,
    retry: Option<RetryConfig>,
    stores: Arc<Mutex<HashMap<Url, Arc<dyn ObjectStore>>>>,
}

impl StorageResolver {
    /// Builds a resolver containing the default local provider registration.
    ///
    /// Without the `local` Cargo feature, construction still succeeds and local resolution returns
    /// [`StorageError::ProviderDisabled`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageResolverBuildError`] if the built-in registration cannot be validated or
    /// its default command arguments cannot be bound.
    pub fn local() -> Result<Self, StorageResolverBuildError> {
        let registry = StorageRegistry::builder()
            .register(crate::local::registration())
            .build()?;
        let command = registry.augment_args(Command::new("storage"));
        let matches = command.try_get_matches_from(["storage"])?;
        registry.bind_args(&matches)
    }

    /// Returns the command's validated shared retry configuration.
    ///
    /// This is `None` when no registration requested shared retries.
    pub fn retry_configuration(&self) -> Option<&RetryConfig> {
        self.retry.as_ref()
    }

    /// Resolves a location for reading.
    ///
    /// Scheme selection and read access are checked before the provider callback runs. The resolver
    /// layer issues no metadata request. Provider callbacks and factories execute as part of
    /// resolution. Use [`crate::validate_input`] when the caller requires an explicit existence
    /// check after resolution.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when the scheme is unavailable, the provider does not support
    /// input, or provider resolution or client construction fails.
    pub fn resolve_input(&self, location: &Location) -> Result<ResolvedLocation, StorageError> {
        self.resolve_direction(location, StorageDirection::Input)
    }

    /// Resolves a location for writing.
    ///
    /// Scheme selection and write access are checked before the provider callback runs. The
    /// resolver layer applies no overwrite policy. Provider callbacks and factories execute as
    /// part of resolution. Use [`crate::preflight_output`] when the caller needs an explicit check
    /// after resolution.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when the scheme is unavailable, the provider does not support
    /// output, or provider resolution or client construction fails.
    pub fn resolve_output(&self, location: &Location) -> Result<ResolvedLocation, StorageError> {
        self.resolve_direction(location, StorageDirection::Output)
    }

    fn resolve_direction(
        &self,
        location: &Location,
        direction: StorageDirection,
    ) -> Result<ResolvedLocation, StorageError> {
        let scheme = location.url().scheme();
        let provider = self
            .schemes
            .get(scheme)
            .map(|index| &self.providers[*index])
            .ok_or_else(|| StorageError::UnsupportedScheme(scheme.to_owned()))?;

        let (provider_name, access, resolver, uses_shared_retries) = match provider {
            BoundProvider::Enabled {
                name,
                access,
                resolver,
                uses_shared_retries,
            } => (*name, *access, resolver, *uses_shared_retries),
            BoundProvider::Disabled { name, diagnostic } => {
                return Err(StorageError::ProviderDisabled {
                    provider: name,
                    diagnostic,
                });
            }
        };
        if !access.supports(direction) {
            return Err(StorageError::DirectionUnsupported {
                provider: provider_name,
                direction,
            });
        }

        let retry = if uses_shared_retries {
            self.retry.as_ref()
        } else {
            None
        };
        let resolution = resolver.resolve(location, retry).map_err(|source| {
            StorageError::ProviderResolution {
                provider: provider_name,
                direction,
                source,
            }
        })?;

        let store_url = store_url(location.url());
        let mut stores = self
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Holding the lock through construction prevents duplicate clients for one origin.
        let store = match stores.entry(store_url.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => Arc::clone(entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let store = (resolution.store_factory)().map_err(|source| {
                    StorageError::ProviderResolution {
                        provider: provider_name,
                        direction,
                        source,
                    }
                })?;
                Arc::clone(entry.insert(store))
            }
        };

        Ok(ResolvedLocation {
            url: location.url().clone(),
            store,
            path: resolution.path,
            store_url,
        })
    }
}

/// Errors produced while validating a registry or binding command arguments.
#[derive(Debug, Error)]
pub enum StorageResolverBuildError {
    /// The provider registrations are ambiguous.
    #[error(transparent)]
    Registry(#[from] StorageRegistryError),
    /// Clap cannot read storage settings from the supplied matches.
    #[error(transparent)]
    Arguments(#[from] clap::Error),
    /// The shared retry arguments form an invalid configuration.
    #[error(transparent)]
    Retry(#[from] RetryConfigurationError),
}

/// Returns collision keys for each Clap ID and primary long and short option contributed by `T`.
fn argument_keys<T>(name: &'static str) -> Vec<(String, String)>
where
    T: Args,
{
    let command = T::augment_args(Command::new(name));
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

/// Reduces an exact location URL to the origin used for client caching and DataFusion registration.
fn store_url(url: &Url) -> Url {
    let mut store_url = url.clone();
    store_url.set_path("/");
    store_url.set_query(None);
    store_url.set_fragment(None);
    store_url
}
