//! Typed storage-provider registration and command-scoped resolution.
//!
//! Each registration binds one concrete Clap argument type to its resolver. The registry can hold heterogeneous providers without separating settings from the resolver that understands them.

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
pub type ProviderResolver<T> = fn(
    location: &Location,
    settings: &T,
    retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution>;

/// A provider's object path and lazy client factory.
pub struct ProviderResolution {
    store_factory: Box<dyn FnOnce() -> anyhow::Result<Arc<dyn ObjectStore>> + Send>,
    path: ObjectPath,
}

impl ProviderResolution {
    /// Creates a resolution whose client is constructed only after a cache miss.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageAccess {
    ReadOnly,
    WriteOnly,
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

#[derive(Clone)]
/// One provider's identity, argument contribution, access, and typed resolver.
pub struct StorageProviderRegistration {
    name: &'static str,
    schemes: Vec<&'static str>,
    provider: ProviderRegistration,
    uses_shared_retries: bool,
}

impl StorageProviderRegistration {
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

    pub fn without_args(name: &'static str) -> StorageProviderRegistrationBuilder<()> {
        StorageProviderRegistrationBuilder {
            name,
            schemes: Vec::new(),
            args: binding::ArgsParser::unit(),
            uses_shared_retries: false,
            settings: PhantomData,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    pub fn has_input(&self) -> bool {
        matches!(&self.provider, ProviderRegistration::Enabled { access, .. } if access.supports(StorageDirection::Input))
    }

    pub fn has_output(&self) -> bool {
        matches!(&self.provider, ProviderRegistration::Enabled { access, .. } if access.supports(StorageDirection::Output))
    }

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
    pub fn schemes(mut self, schemes: impl IntoIterator<Item = &'static str>) -> Self {
        self.schemes.extend(schemes);
        self
    }

    /// Passes the registry's shared retry configuration to this provider's resolver.
    pub fn shared_retries(mut self) -> Self {
        self.uses_shared_retries = true;
        self
    }

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

#[derive(Debug, Error)]
pub enum StorageRegistryError {
    #[error("duplicate storage provider name: {0}")]
    DuplicateName(String),
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

    pub fn registrations(&self) -> impl Iterator<Item = &StorageProviderRegistration> {
        self.registrations.iter()
    }

    pub fn by_scheme(&self, scheme: &str) -> Option<&StorageProviderRegistration> {
        self.schemes
            .get(&scheme.to_ascii_lowercase())
            .map(|index| &self.registrations[*index])
    }

    /// Adds shared retry arguments and every provider's ordinary Clap arguments.
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

#[derive(Clone)]
/// Provider resolvers, retry settings, and cached clients bound to one command.
pub struct StorageResolver {
    providers: Arc<Vec<BoundProvider>>,
    schemes: Arc<HashMap<String, usize>>,
    retry: Option<RetryConfig>,
    stores: Arc<Mutex<HashMap<Url, Arc<dyn ObjectStore>>>>,
}

impl StorageResolver {
    /// Builds a resolver containing the default local provider registration.
    pub fn local() -> Result<Self, StorageResolverBuildError> {
        let registry = StorageRegistry::builder()
            .register(crate::local::registration())
            .build()?;
        let command = registry.augment_args(Command::new("storage"));
        let matches = command.try_get_matches_from(["storage"])?;
        registry.bind_args(&matches)
    }

    pub fn retry_configuration(&self) -> Option<&RetryConfig> {
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

fn store_url(url: &Url) -> Url {
    let mut store_url = url.clone();
    store_url.set_path("/");
    store_url.set_query(None);
    store_url.set_fragment(None);
    store_url
}
