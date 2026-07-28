//! Typed storage-provider registration and command-scoped resolution.
//!
//! Each registration binds one concrete Clap argument type to its callbacks. The registry can therefore hold heterogeneous providers without separating settings from the callback that understands them.

mod binding;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    marker::PhantomData,
    path::Path as FilePath,
    sync::{Arc, Mutex},
};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use object_store::{ObjectStore, path::Path as ObjectPath};
use thiserror::Error;
use url::Url;

use crate::{
    Location, ResolvedLocation, RetryArgs, RetryConfiguration, RetryConfigurationError,
    StorageError, StoreCacheKey,
};

/// Resolves one provider location using settings registered as `T`.
pub type ProviderResolver<T> = fn(
    location: &Location,
    settings: &T,
    retry: Option<&RetryConfiguration>,
) -> Result<ProviderResolution, StorageError>;

/// A provider's object path, lazy client factory, and cache configuration.
///
/// Every setting that can change client behavior belongs in the cache configuration.
pub struct ProviderResolution {
    store_url: Url,
    store_factory: Box<dyn FnOnce() -> Result<Arc<dyn ObjectStore>, StorageError> + Send>,
    path: ObjectPath,
    configuration: BTreeMap<String, String>,
}

impl ProviderResolution {
    /// Creates a resolution whose client is constructed only after a cache miss.
    pub fn from_factory(
        store_url: Url,
        path: ObjectPath,
        factory: impl FnOnce() -> Result<Arc<dyn ObjectStore>, StorageError> + Send + 'static,
    ) -> Self {
        Self {
            store_url,
            store_factory: Box::new(factory),
            path,
            configuration: BTreeMap::new(),
        }
    }

    /// Adds one effective provider setting to the store-cache identity.
    pub fn with_configuration(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configuration.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageDirection {
    Input,
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

#[derive(Clone)]
/// One provider's identity, argument contribution, and typed resolver callbacks.
pub struct StorageProviderRegistration {
    name: &'static str,
    aliases: Vec<&'static str>,
    schemes: Vec<&'static str>,
    binder: Arc<dyn binding::BindProvider>,
    feature_disabled_diagnostic: Option<&'static str>,
    uses_shared_retries: bool,
}

impl StorageProviderRegistration {
    pub fn with_args<T>(name: &'static str) -> StorageProviderRegistrationBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        StorageProviderRegistrationBuilder {
            name,
            aliases: Vec::new(),
            schemes: Vec::new(),
            args: binding::ArgsParser::for_args(),
            input: None,
            output: None,
            feature_disabled_diagnostic: None,
            uses_shared_retries: false,
            settings: PhantomData,
        }
    }

    pub fn without_args(name: &'static str) -> StorageProviderRegistrationBuilder<()> {
        StorageProviderRegistrationBuilder {
            name,
            aliases: Vec::new(),
            schemes: Vec::new(),
            args: binding::ArgsParser::unit(),
            input: None,
            output: None,
            feature_disabled_diagnostic: None,
            uses_shared_retries: false,
            settings: PhantomData,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn aliases(&self) -> &[&'static str] {
        &self.aliases
    }

    pub fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    pub fn has_input(&self) -> bool {
        self.binder.has_input()
    }

    pub fn has_output(&self) -> bool {
        self.binder.has_output()
    }

    pub const fn uses_shared_retries(&self) -> bool {
        self.uses_shared_retries
    }
}

pub struct StorageProviderRegistrationBuilder<T> {
    name: &'static str,
    aliases: Vec<&'static str>,
    schemes: Vec<&'static str>,
    args: binding::ArgsParser<T>,
    input: Option<ProviderResolver<T>>,
    output: Option<ProviderResolver<T>>,
    feature_disabled_diagnostic: Option<&'static str>,
    uses_shared_retries: bool,
    settings: PhantomData<fn() -> T>,
}

impl<T> StorageProviderRegistrationBuilder<T>
where
    T: Send + Sync + 'static,
{
    pub fn aliases(mut self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        self.aliases.extend(aliases);
        self
    }

    pub fn schemes(mut self, schemes: impl IntoIterator<Item = &'static str>) -> Self {
        self.schemes.extend(schemes);
        self
    }

    pub fn input(mut self, resolver: ProviderResolver<T>) -> Self {
        self.input = Some(resolver);
        self
    }

    pub fn output(mut self, resolver: ProviderResolver<T>) -> Self {
        self.output = Some(resolver);
        self
    }

    /// Supplies the error guidance used when this registration has no callbacks.
    pub fn feature_disabled_diagnostic(mut self, diagnostic: &'static str) -> Self {
        self.feature_disabled_diagnostic = Some(diagnostic);
        self
    }

    /// Passes the registry's shared retry configuration to this provider's callbacks.
    pub fn shared_retries(mut self) -> Self {
        self.uses_shared_retries = true;
        self
    }

    pub fn build(self) -> StorageProviderRegistration {
        StorageProviderRegistration {
            name: self.name,
            aliases: self.aliases,
            schemes: self.schemes,
            binder: Arc::new(binding::ProviderDefinition::new(
                self.args,
                self.input,
                self.output,
            )),
            feature_disabled_diagnostic: self.feature_disabled_diagnostic,
            uses_shared_retries: self.uses_shared_retries,
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageRegistryError {
    #[error("duplicate storage provider name: {0}")]
    DuplicateName(String),
    #[error("duplicate storage provider alias: {0}")]
    DuplicateAlias(String),
    #[error("duplicate storage scheme: {0}")]
    DuplicateScheme(String),
    #[error("duplicate storage CLI argument: {0}")]
    DuplicateCliArgument(String),
}

pub struct StorageRegistryBuilder {
    registrations: Vec<StorageProviderRegistration>,
}

impl StorageRegistryBuilder {
    pub fn register(mut self, registration: StorageProviderRegistration) -> Self {
        self.registrations.push(registration);
        self
    }

    pub fn build(self) -> Result<StorageRegistry, StorageRegistryError> {
        StorageRegistry::from_registrations(self.registrations)
    }
}

/// Provider definitions, scheme lookup, and CLI composition for one executable.
pub struct StorageRegistry {
    registrations: Vec<StorageProviderRegistration>,
    names: HashMap<String, usize>,
    schemes: HashMap<String, usize>,
    uses_shared_retries: bool,
}

impl StorageRegistry {
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

            for alias in &registration.aliases {
                let alias = alias.to_ascii_lowercase();
                if names.insert(alias.clone(), index).is_some() {
                    return Err(StorageRegistryError::DuplicateAlias(alias));
                }
            }

            for scheme in &registration.schemes {
                let scheme = scheme.to_ascii_lowercase();
                if schemes.insert(scheme.clone(), index).is_some() {
                    return Err(StorageRegistryError::DuplicateScheme(scheme));
                }
            }

            for (key, argument) in registration.binder.argument_keys() {
                if !cli_arguments.insert(key) {
                    return Err(StorageRegistryError::DuplicateCliArgument(argument));
                }
            }
        }

        Ok(Self {
            registrations,
            names,
            schemes,
            uses_shared_retries,
        })
    }

    pub fn registrations(&self) -> impl Iterator<Item = &StorageProviderRegistration> {
        self.registrations.iter()
    }

    pub fn get(&self, name_or_alias: &str) -> Option<&StorageProviderRegistration> {
        self.names
            .get(&name_or_alias.to_ascii_lowercase())
            .map(|index| &self.registrations[*index])
    }

    pub fn by_scheme(&self, scheme: &str) -> Option<&StorageProviderRegistration> {
        self.schemes
            .get(&scheme.to_ascii_lowercase())
            .map(|index| &self.registrations[*index])
    }

    /// Parses a bare local path or URL whose scheme belongs to this registry.
    pub fn parse_location(
        &self,
        input: impl AsRef<str>,
        working_directory: impl AsRef<FilePath>,
    ) -> Result<Location, StorageError> {
        Location::parse_registered(input, working_directory, |scheme| {
            self.schemes.contains_key(scheme)
        })
    }

    /// Adds shared retry arguments and every provider's ordinary Clap arguments.
    pub fn augment_args(&self, mut command: Command) -> Command {
        if self.uses_shared_retries {
            command = RetryArgs::augment_args(command);
        }
        for registration in &self.registrations {
            command = registration.binder.augment(command);
        }
        command
    }

    /// Binds one command's parsed arguments into a resolver and a fresh client cache.
    pub fn bind_args(
        &self,
        matches: &ArgMatches,
    ) -> Result<StorageResolver, StorageResolverBuildError> {
        let retry = if self.uses_shared_retries {
            Some(RetryConfiguration::try_from(RetryArgs::from_arg_matches(
                matches,
            )?)?)
        } else {
            None
        };

        let mut providers = Vec::with_capacity(self.registrations.len());
        for registration in &self.registrations {
            providers.push(BoundProvider {
                name: registration.name,
                callbacks: registration.binder.bind(matches)?,
                feature_disabled_diagnostic: registration.feature_disabled_diagnostic,
                uses_shared_retries: registration.uses_shared_retries,
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

struct BoundProvider {
    name: &'static str,
    callbacks: Arc<dyn binding::ResolveProvider>,
    feature_disabled_diagnostic: Option<&'static str>,
    uses_shared_retries: bool,
}

#[derive(Clone)]
/// Provider callbacks, retry settings, and cached clients bound to one command.
pub struct StorageResolver {
    providers: Arc<Vec<BoundProvider>>,
    schemes: Arc<HashMap<String, usize>>,
    retry: Option<RetryConfiguration>,
    stores: Arc<Mutex<HashMap<StoreCacheKey, Arc<dyn ObjectStore>>>>,
}

impl StorageResolver {
    /// Builds a resolver containing the default local provider registration.
    pub fn new() -> Result<Self, StorageResolverBuildError> {
        let registry = StorageRegistry::builder()
            .register(crate::local::registration())
            .build()?;
        let command = registry.augment_args(Command::new("storage"));
        let matches = command.try_get_matches_from(["storage"])?;
        registry.bind_args(&matches)
    }

    pub fn retry_configuration(&self) -> Option<&RetryConfiguration> {
        self.retry.as_ref()
    }

    pub fn resolve_input(&self, location: &Location) -> Result<ResolvedLocation, StorageError> {
        self.resolve_direction(location, StorageDirection::Input)
    }

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

        let has_direction = match direction {
            StorageDirection::Input => provider.callbacks.has_input(),
            StorageDirection::Output => provider.callbacks.has_output(),
        };
        let provider_enabled = provider.callbacks.has_input() || provider.callbacks.has_output();
        if !has_direction
            && !provider_enabled
            && let Some(diagnostic) = provider.feature_disabled_diagnostic
        {
            return Err(StorageError::ProviderDisabled {
                provider: provider.name,
                diagnostic,
            });
        }

        let retry = if provider.uses_shared_retries {
            self.retry.as_ref()
        } else {
            None
        };
        let mut resolution =
            provider
                .callbacks
                .resolve(provider.name, direction, location, retry)?;
        if let Some(retry) = retry {
            retry.append_cache_configuration(&mut resolution.configuration);
        }

        let cache_key = StoreCacheKey::new(
            location.url().scheme(),
            authority(location.url()),
            resolution.configuration,
        );
        let mut stores = self
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Construction stays under the cache lock so concurrent resolutions create one client.
        let store = match stores.entry(cache_key.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => Arc::clone(entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                Arc::clone(entry.insert((resolution.store_factory)()?))
            }
        };

        Ok(ResolvedLocation {
            url: location.url().clone(),
            store,
            path: resolution.path,
            store_url: resolution.store_url,
            cache_key,
        })
    }
}

#[derive(Debug, Error)]
pub enum StorageResolverBuildError {
    #[error(transparent)]
    Registry(#[from] StorageRegistryError),
    #[error(transparent)]
    Arguments(#[from] clap::Error),
    #[error(transparent)]
    Retry(#[from] RetryConfigurationError),
}

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

fn authority(url: &Url) -> String {
    match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_owned(),
        (None, _) => String::new(),
    }
}
