//! Typed storage-provider registration and command-scoped resolution.
//!
//! Each registration binds one concrete Clap argument type to its resolver and optional
//! bare-location mapper. The registry can hold different settings types without separating them
//! from the callbacks that understand them.
//!
//! Provider setup has two stages. [`StorageRegistry`] first collects registrations, validates their
//! names, schemes, and CLI arguments, and augments a Clap command. [`StorageRegistry::bind_args`]
//! then parses each registered provider's settings once and produces a [`StorageResolver`] whose
//! clones share those settings and a command-scoped client cache.

mod binding;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex},
};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use object_store::{ObjectStore, RetryConfig, path::Path as ObjectPath};
use thiserror::Error;
use url::Url;

use crate::{
    Location, LocationInput, ResolvedLocation, RetryArgs, RetryConfigurationError, StorageError,
};

/// Converts schemeless input into a canonical location using settings registered as `T`.
///
/// A registration opts into the registry's exclusive bare-location route by supplying this
/// callback. The returned location must use a scheme claimed by the same registration.
pub type BareLocationMapper<T> = fn(input: &str, settings: &T) -> anyhow::Result<Location>;

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
/// client for the store root.
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
    /// and query under the same store root.
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

/// One available provider's identity, argument contribution, and resolution behavior.
///
/// Registrations are immutable descriptions that can be cloned into more than one registry. Use
/// [`Self::with_args`] or [`Self::without_args`] to supply the required provider contract, add any
/// optional behavior through the returned builder, and finish with
/// [`StorageProviderRegistrationBuilder::build`].
///
/// A provider registration follows this order:
///
/// 1. Supply its settings type, name, primary URL scheme, access, and resolver.
/// 2. Add any additional URL schemes that select it.
/// 3. Opt into shared retries if its client supports them.
/// 4. Optionally claim schemeless input with a typed mapper.
/// 5. Build the complete registration.
#[derive(Clone)]
pub struct StorageProviderRegistration {
    name: &'static str,
    schemes: Vec<&'static str>,
    access: StorageAccess,
    binder: Arc<dyn binding::BindProvider>,
    handles_bare_locations: bool,
    uses_shared_retries: bool,
}

impl StorageProviderRegistration {
    /// Starts a registration whose settings are parsed from the command line as `T`.
    ///
    /// `scheme` is the provider's required primary route. `access` is enforced before `resolver`
    /// runs. The registry adds `T`'s provider-specific Clap arguments to its command and parses `T`
    /// once during [`StorageRegistry::bind_args`].
    pub fn with_args<T>(
        name: &'static str,
        scheme: &'static str,
        access: StorageAccess,
        resolver: ProviderResolver<T>,
    ) -> StorageProviderRegistrationBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        StorageProviderRegistrationBuilder {
            name,
            schemes: vec![scheme],
            access,
            resolver,
            args: binding::ArgsParser::for_args(),
            bare_location_mapper: None,
            uses_shared_retries: false,
        }
    }

    /// Starts a registration that contributes no provider-specific CLI arguments.
    ///
    /// `scheme` is the provider's required primary route. `access` is enforced before `resolver`
    /// runs.
    pub fn without_args(
        name: &'static str,
        scheme: &'static str,
        access: StorageAccess,
        resolver: ProviderResolver<()>,
    ) -> StorageProviderRegistrationBuilder<()> {
        StorageProviderRegistrationBuilder {
            name,
            schemes: vec![scheme],
            access,
            resolver,
            args: binding::ArgsParser::unit(),
            bare_location_mapper: None,
            uses_shared_retries: false,
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

    /// Returns whether this registration accepts input resolution.
    pub fn has_input(&self) -> bool {
        self.access.supports(StorageDirection::Input)
    }

    /// Returns whether this registration accepts output resolution.
    pub fn has_output(&self) -> bool {
        self.access.supports(StorageDirection::Output)
    }

    /// Returns whether this provider interprets schemeless input.
    pub const fn handles_bare_locations(&self) -> bool {
        self.handles_bare_locations
    }

    /// Returns whether this registration requests the registry's shared retry settings.
    pub const fn uses_shared_retries(&self) -> bool {
        self.uses_shared_retries
    }

    fn augment_args(&self, command: Command) -> Command {
        self.binder.augment(command)
    }

    fn argument_keys(&self) -> Vec<(String, String)> {
        self.binder.argument_keys()
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
    access: StorageAccess,
    resolver: ProviderResolver<T>,
    args: binding::ArgsParser<T>,
    bare_location_mapper: Option<BareLocationMapper<T>>,
    uses_shared_retries: bool,
}

impl<T> StorageProviderRegistrationBuilder<T>
where
    T: Send + Sync + 'static,
{
    /// Adds URL schemes beyond the primary scheme supplied at construction.
    ///
    /// Scheme lookup and duplicate detection are ASCII case-insensitive. Duplicate schemes are
    /// reported when the containing registry is built.
    pub fn additional_schemes(mut self, schemes: impl IntoIterator<Item = &'static str>) -> Self {
        self.schemes.extend(schemes);
        self
    }

    /// Claims schemeless input and maps it into one of this provider's registered schemes.
    ///
    /// Registry construction rejects more than one claimant. The mapper receives the raw input
    /// unchanged after argument binding and may interpret it using the provider's typed settings.
    pub fn bare_locations(mut self, mapper: BareLocationMapper<T>) -> Self {
        self.bare_location_mapper = Some(mapper);
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

    /// Finishes the registration and erases its settings type for registry storage.
    ///
    /// The access declaration is enforced before the resolver runs. The same callback serves every
    /// allowed direction and does not receive the direction as an argument.
    pub fn build(self) -> StorageProviderRegistration {
        StorageProviderRegistration {
            name: self.name,
            schemes: self.schemes,
            access: self.access,
            handles_bare_locations: self.bare_location_mapper.is_some(),
            binder: Arc::new(binding::ProviderDefinition::new(
                self.args,
                self.bare_location_mapper,
                self.resolver,
            )),
            uses_shared_retries: self.uses_shared_retries,
        }
    }
}

/// Errors that make a set of provider registrations invalid or ambiguous.
#[derive(Debug, Error)]
pub enum StorageRegistryError {
    /// Two registrations use the same provider name, ignoring ASCII case.
    #[error("duplicate storage provider name: {0}")]
    DuplicateName(String),
    /// A URL scheme is claimed more than once, ignoring ASCII case.
    #[error("duplicate storage scheme: {0}")]
    DuplicateScheme(String),
    /// Provider or retry arguments reuse a Clap argument or group ID, or a primary option name.
    #[error("duplicate storage CLI argument: {0}")]
    DuplicateCliArgument(String),
    /// More than one provider claims schemeless input.
    #[error("storage providers {first} and {second} both claim bare locations")]
    DuplicateBareLocationProvider {
        /// The first provider claiming bare locations.
        first: &'static str,
        /// The later provider with the conflicting claim.
        second: &'static str,
    },
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
    /// Returns [`StorageRegistryError`] for duplicate provider names, URL schemes, bare-location
    /// claims, or Clap identifiers. Provider names and schemes are compared without ASCII case.
    /// Clap IDs and primary long and short option spellings are compared exactly. Clap option
    /// aliases are not indexed.
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
    bare_location_provider: Option<usize>,
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
        let mut bare_location_provider = None;
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

            if registration.handles_bare_locations
                && let Some(first) = bare_location_provider.replace(index)
            {
                return Err(StorageRegistryError::DuplicateBareLocationProvider {
                    first: registrations[first].name,
                    second: registration.name,
                });
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
            bare_location_provider,
            uses_shared_retries,
        })
    }

    /// Iterates available providers in the order they were registered.
    ///
    /// Omitted feature-gated providers do not have placeholder registrations.
    pub fn registrations(&self) -> impl Iterator<Item = &StorageProviderRegistration> {
        self.registrations.iter()
    }

    /// Finds the registration for a URL scheme using ASCII case-insensitive lookup.
    ///
    /// `None` means no available provider owns the scheme.
    pub fn by_scheme(&self, scheme: &str) -> Option<&StorageProviderRegistration> {
        self.schemes
            .get(&scheme.to_ascii_lowercase())
            .map(|index| &self.registrations[*index])
    }

    /// Returns the provider that interprets schemeless input, if one is registered.
    ///
    /// When this is `None`, bound resolvers reject [`LocationInput::Bare`].
    pub fn bare_location_provider(&self) -> Option<&StorageProviderRegistration> {
        self.bare_location_provider
            .map(|index| &self.registrations[index])
    }

    /// Adds shared retry arguments and every provider's Clap arguments.
    ///
    /// The returned command should be used to produce the `ArgMatches` later passed to
    /// [`Self::bind_args`]. Registry validation does not inspect arguments already present on
    /// `command`, so those IDs and option spellings must not collide with storage arguments.
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
    /// Each registered provider's concrete settings type is parsed once. Clones of the returned
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
            providers.push(BoundProvider {
                name: registration.name,
                access: registration.access,
                resolver: registration.binder.bind(matches)?,
                uses_shared_retries: registration.uses_shared_retries,
            });
        }

        Ok(StorageResolver {
            providers: Arc::new(providers),
            schemes: Arc::new(self.schemes.clone()),
            bare_location_provider: self.bare_location_provider,
            retry,
            stores: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

struct BoundProvider {
    name: &'static str,
    access: StorageAccess,
    resolver: Arc<dyn binding::ResolveProvider>,
    uses_shared_retries: bool,
}

/// Provider resolvers, retry settings, and cached clients bound to one command.
///
/// Cloning a resolver shares the bound provider settings and client cache. Stores are keyed by a
/// root URL derived from the scheme, host, and port, so paths and queries under that root reuse one
/// `ObjectStore` client.
#[derive(Clone)]
pub struct StorageResolver {
    providers: Arc<Vec<BoundProvider>>,
    schemes: Arc<HashMap<String, usize>>,
    bare_location_provider: Option<usize>,
    retry: Option<RetryConfig>,
    stores: Arc<Mutex<HashMap<Url, Arc<dyn ObjectStore>>>>,
}

impl StorageResolver {
    /// Builds a resolver containing the local provider registration.
    ///
    /// # Errors
    ///
    /// Returns [`StorageResolverBuildError`] if the built-in registration cannot be validated or
    /// its default command arguments cannot be bound.
    #[cfg(feature = "local")]
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
    /// An explicit URL selects its provider by scheme. Schemless input selects the registry's one
    /// bare-location provider, which maps the raw text into one of its URL schemes. Read access is
    /// checked before either provider callback runs. The resolver layer issues no metadata request.
    /// The returned factory executes only on a cache miss. Use [`crate::validate_input`] when the
    /// caller requires an explicit existence check after resolution.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when no provider owns the route, the selected provider does not
    /// support input, bare-location mapping is invalid, or provider resolution or client
    /// construction fails.
    pub fn resolve_input(
        &self,
        location: &LocationInput,
    ) -> Result<ResolvedLocation, StorageError> {
        self.resolve_direction(location, StorageDirection::Input)
    }

    /// Resolves a location for writing.
    ///
    /// An explicit URL selects its provider by scheme. Schemless input selects the registry's one
    /// bare-location provider, which maps the raw text into one of its URL schemes. Write access is
    /// checked before either provider callback runs. The resolver layer applies no overwrite
    /// policy, and the returned factory executes only on a cache miss. Use
    /// [`crate::preflight_output`] when the caller needs an explicit check after resolution.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when no provider owns the route, the selected provider does not
    /// support output, bare-location mapping is invalid, or provider resolution or client
    /// construction fails.
    pub fn resolve_output(
        &self,
        location: &LocationInput,
    ) -> Result<ResolvedLocation, StorageError> {
        self.resolve_direction(location, StorageDirection::Output)
    }

    fn resolve_direction(
        &self,
        input: &LocationInput,
        direction: StorageDirection,
    ) -> Result<ResolvedLocation, StorageError> {
        let provider_index = match input {
            LocationInput::Url(location) => {
                let scheme = location.url().scheme();
                *self
                    .schemes
                    .get(scheme)
                    .ok_or_else(|| StorageError::UnsupportedScheme(scheme.to_owned()))?
            }
            LocationInput::Bare(input) => self
                .bare_location_provider
                .ok_or_else(|| StorageError::UnsupportedBareLocation(input.clone()))?,
        };
        let provider = &self.providers[provider_index];
        if !provider.access.supports(direction) {
            return Err(StorageError::DirectionUnsupported {
                provider: provider.name,
                direction,
            });
        }

        let mapped_location = match input {
            LocationInput::Url(_) => None,
            LocationInput::Bare(input) => Some(
                provider
                    .resolver
                    .map_bare_location(input)
                    .expect("the indexed bare-location provider must have a mapper")
                    .map_err(|source| StorageError::ProviderResolution {
                        provider: provider.name,
                        direction,
                        source,
                    })?,
            ),
        };
        let location = match (input, mapped_location.as_ref()) {
            (LocationInput::Url(location), _) => location,
            (LocationInput::Bare(_), Some(location)) => location,
            (LocationInput::Bare(_), None) => unreachable!(),
        };
        let scheme = location.url().scheme();
        if self.schemes.get(scheme) != Some(&provider_index) {
            return Err(StorageError::BareLocationSchemeMismatch {
                provider: provider.name,
                scheme: scheme.to_owned(),
            });
        }

        let retry = if provider.uses_shared_retries {
            self.retry.as_ref()
        } else {
            None
        };
        let resolution = provider
            .resolver
            .resolve(location, retry)
            .map_err(|source| StorageError::ProviderResolution {
                provider: provider.name,
                direction,
                source,
            })?;

        let store_url = store_url(location.url());
        let mut stores = self
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Holding the lock through construction prevents duplicate clients for one store root.
        let store = match stores.entry(store_url.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => Arc::clone(entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let store = (resolution.store_factory)().map_err(|source| {
                    StorageError::ProviderResolution {
                        provider: provider.name,
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

/// Returns collision keys for each Clap argument or group ID and primary option contributed by `T`.
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
    for group in command.get_groups() {
        let id = group.get_id().as_str().to_owned();
        keys.push((format!("id:{id}"), id.clone()));
    }
    keys
}

/// Reduces an exact location URL to the root URL used for caching and DataFusion registration.
fn store_url(url: &Url) -> Url {
    let mut store_url = url.clone();
    store_url.set_path("/");
    store_url.set_query(None);
    store_url.set_fragment(None);
    store_url
}
