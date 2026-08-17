//! Immutable storage backend definitions and their typed callback contracts.
//!
//! A [`StorageBackend`] describes one storage implementation before any command has been parsed.
//! It owns the backend's registry identity, URL schemes, access declaration, Clap behavior, and
//! callbacks. [`StorageBackendBuilder`] keeps the callback settings type `T` intact until
//! [`StorageBackendBuilder::build`] has paired every parser and callback that uses it.

mod binding;

use std::{collections::HashSet, fmt, future::Future, pin::Pin, sync::Arc};

use clap::{ArgMatches, Args, Command, FromArgMatches};
use object_store::{ObjectStore, RetryConfig};
use thiserror::Error;
use url::Url;

use crate::{Location, LocationPattern, OutputPreparation, StorageHandle};

pub(crate) use binding::BackendBinding;
use binding::{BackendDefinition, TypedBackendDefinition};

/// Converts schemeless input into a canonical location using settings parsed as `T`.
///
/// Registering a backend with this callback claims the registry's one bare-location route. The
/// returned [`Location`] must use a scheme claimed by the same backend.
pub type BareLocationMapper<T> = fn(input: &str, settings: &T) -> anyhow::Result<Location>;

/// Converts schemeless pattern input into a canonical location pattern using settings parsed as
/// `T`.
///
/// This callback is optional and requires the same backend to claim bare exact locations. The
/// returned [`LocationPattern`] must use a scheme claimed by the backend.
pub type BarePatternMapper<T> = fn(input: &str, settings: &T) -> anyhow::Result<LocationPattern>;

/// Applies backend-specific validation to one routed canonical location using settings parsed as
/// `T`.
///
/// Object paths are derived generically from decoded URL paths. A backend may use this callback
/// for its authority, query, and other location-specific rules.
pub type LocationValidator<T> = fn(location: &Location, settings: &T) -> anyhow::Result<()>;

fn accept_any_location<T>(_location: &Location, _settings: &T) -> anyhow::Result<()> {
    Ok(())
}

/// Creates an object-store client for one session cache entry.
///
/// The session derives `store_url` from the canonical location and calls this creator only on a
/// cache miss. This URL is the scheme and authority root used as the session cache key: its path
/// is `/`, and it has no query or fragment. `retry` is present only when the backend opted into
/// [`StorageBackendBuilder::shared_retries`].
pub type ObjectStoreCreatorFn<T> = fn(
    store_url: &Url,
    settings: &T,
    retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>>;

/// Performs backend-specific work before a prepared output handle is opened by a format.
pub type PrepareOutputTargetFn<T> =
    for<'a> fn(
        &'a StorageHandle,
        &'a OutputPreparation,
        &'a T,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

fn prepare_output_target_without_backend_work<'a, T>(
    _handle: &'a StorageHandle,
    _preparation: &'a OutputPreparation,
    _settings: &'a T,
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async { Ok(()) })
}

/// Whether a handle will be used for input or output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageDirection {
    /// The handle will be used for reading.
    Input,
    /// The handle will be used for writing.
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

/// The input and output directions accepted by a storage backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageAccess {
    /// The backend accepts input handles only.
    ReadOnly,
    /// The backend accepts output handles only.
    WriteOnly,
    /// The backend accepts both input and output handles.
    ReadWrite,
}

impl StorageAccess {
    pub(super) const fn supports(self, direction: StorageDirection) -> bool {
        matches!(
            (self, direction),
            (Self::ReadOnly | Self::ReadWrite, StorageDirection::Input)
                | (Self::WriteOnly | Self::ReadWrite, StorageDirection::Output)
        )
    }
}

/// One complete, immutable storage backend definition.
///
/// A backend is available only when the host registers this value in a
/// [`StorageRegistry`](crate::StorageRegistry). Omitted feature-gated backends claim no schemes,
/// add no CLI arguments, and appear in no registry introspection.
pub struct StorageBackend {
    definition: Box<dyn BackendDefinition>,
}

impl fmt::Debug for StorageBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageBackend")
            .field("name", &self.name())
            .field("schemes", &self.schemes())
            .field("claims_bare_locations", &self.claims_bare_locations())
            .field("uses_shared_retries", &self.uses_shared_retries())
            .finish_non_exhaustive()
    }
}

impl StorageBackend {
    /// Starts a backend builder whose settings are parsed from the host's Clap matches as `T`.
    ///
    /// The builder stores `T`'s argument augmentation and parsing functions next to callbacks that
    /// accept `&T`. Calling
    /// [`StorageRegistry::create_session`](crate::StorageRegistry::create_session) parses one `T`,
    /// which that session reuses for every callback on this backend.
    pub fn with_args<T>() -> StorageBackendBuilder<T>
    where
        T: Args + FromArgMatches + Send + Sync + 'static,
    {
        StorageBackendBuilder::new(T::augment_args, T::from_arg_matches)
    }

    /// Starts a backend builder that has no backend-specific CLI settings.
    pub fn without_args() -> StorageBackendBuilder<()> {
        StorageBackendBuilder::new(|command| command, |_| Ok(()))
    }

    /// Returns the canonical name used for registry identity and diagnostics.
    pub fn name(&self) -> &'static str {
        self.definition.name()
    }

    /// Returns every canonical URL scheme claimed by this backend.
    pub fn schemes(&self) -> &[&'static str] {
        self.definition.schemes()
    }

    /// Returns whether this backend accepts the requested access direction.
    pub fn supports(&self, direction: StorageDirection) -> bool {
        self.definition.supports(direction)
    }

    /// Returns whether this backend claims the registry's exclusive bare-location route.
    pub fn claims_bare_locations(&self) -> bool {
        self.definition.claims_bare_locations()
    }

    /// Returns whether this backend consumes the registry's shared retry configuration.
    pub fn uses_shared_retries(&self) -> bool {
        self.definition.uses_shared_retries()
    }

    pub(crate) fn augment_args(&self, command: Command) -> Command {
        self.definition.augment_args(command)
    }

    pub(crate) fn argument_keys(&self) -> &[CliArgumentKey] {
        self.definition.argument_keys()
    }

    pub(crate) fn bind(
        &self,
        matches: &ArgMatches,
    ) -> Result<Box<dyn BackendBinding>, clap::Error> {
        self.definition.bind(matches)
    }
}

/// Builds one typed storage backend definition.
///
/// The builder allows definition fields to be supplied in any order. [`Self::build`] validates
/// the complete definition before erasing `T` behind the private backend interface.
pub struct StorageBackendBuilder<T> {
    name: Option<&'static str>,
    schemes: Vec<&'static str>,
    access: Option<StorageAccess>,
    bare_location_mapper: Option<BareLocationMapper<T>>,
    bare_pattern_mapper: Option<BarePatternMapper<T>>,
    location_validator: Option<LocationValidator<T>>,
    object_store_creator: Option<ObjectStoreCreatorFn<T>>,
    prepare_output_target: PrepareOutputTargetFn<T>,
    uses_shared_retries: bool,
    augment_args: fn(Command) -> Command,
    parse_args: fn(&ArgMatches) -> Result<T, clap::Error>,
}

impl<T> StorageBackendBuilder<T> {
    fn new(
        augment_args: fn(Command) -> Command,
        parse_args: fn(&ArgMatches) -> Result<T, clap::Error>,
    ) -> Self {
        Self {
            name: None,
            schemes: Vec::new(),
            access: None,
            bare_location_mapper: None,
            bare_pattern_mapper: None,
            location_validator: None,
            object_store_creator: None,
            prepare_output_target: prepare_output_target_without_backend_work::<T>,
            uses_shared_retries: false,
            augment_args,
            parse_args,
        }
    }

    /// Sets the backend's canonical registry name.
    ///
    /// Names use lowercase ASCII letters, digits, and hyphens and must start with a letter.
    pub fn name(mut self, name: &'static str) -> Self {
        self.name = Some(name);
        self
    }

    /// Replaces the complete set of canonical URL schemes claimed by this backend.
    ///
    /// Each scheme uses lowercase URL-scheme syntax. At least one scheme is required.
    pub fn schemes(mut self, schemes: impl IntoIterator<Item = &'static str>) -> Self {
        self.schemes = schemes.into_iter().collect();
        self
    }

    /// Sets the input and output directions accepted by this backend.
    pub fn access(mut self, access: StorageAccess) -> Self {
        self.access = Some(access);
        self
    }

    /// Claims schemeless input and supplies the callback that assigns it a canonical URL.
    pub fn bare_location_mapper(mut self, mapper: BareLocationMapper<T>) -> Self {
        self.bare_location_mapper = Some(mapper);
        self
    }

    /// Maps schemeless patterns after this backend has claimed the bare-location route.
    pub fn bare_pattern_mapper(mut self, mapper: BarePatternMapper<T>) -> Self {
        self.bare_pattern_mapper = Some(mapper);
        self
    }

    /// Sets the callback that validates routed canonical locations.
    pub fn location_validator(mut self, validator: LocationValidator<T>) -> Self {
        self.location_validator = Some(validator);
        self
    }

    /// Explicitly accepts every routed location that passed core syntax validation.
    ///
    /// Use this when a backend has no authority, query, or other location-specific rules.
    pub fn allow_any_location(mut self) -> Self {
        self.location_validator = Some(accept_any_location::<T>);
        self
    }

    /// Sets the callback that creates object stores for session cache misses.
    pub fn object_store_creator(mut self, creator: ObjectStoreCreatorFn<T>) -> Self {
        self.object_store_creator = Some(creator);
        self
    }

    /// Sets the backend callback run after generic output policy and before sink opening.
    pub fn prepare_output_target(mut self, callback: PrepareOutputTargetFn<T>) -> Self {
        self.prepare_output_target = callback;
        self
    }

    /// Makes the registry's shared retry configuration available to this backend's store creator.
    pub fn shared_retries(mut self) -> Self {
        self.uses_shared_retries = true;
        self
    }

    /// Validates and erases the complete backend definition.
    ///
    /// # Errors
    ///
    /// Returns [`StorageBackendBuildError`] when a required field is absent, a name or scheme is
    /// noncanonical, or the backend contributes a duplicate route or CLI key.
    pub fn build(self) -> Result<StorageBackend, StorageBackendBuildError>
    where
        T: Send + Sync + 'static,
    {
        let name = self.name.ok_or(StorageBackendBuildError::MissingName)?;
        if !valid_backend_name(name) {
            return Err(StorageBackendBuildError::InvalidName { name });
        }
        if self.schemes.is_empty() {
            return Err(StorageBackendBuildError::MissingSchemes);
        }

        let mut seen_schemes = HashSet::new();
        for &scheme in &self.schemes {
            if !valid_scheme(scheme) {
                return Err(StorageBackendBuildError::InvalidScheme { scheme });
            }
            if !seen_schemes.insert(scheme) {
                return Err(StorageBackendBuildError::DuplicateScheme { scheme });
            }
        }

        let cli_argument_keys = argument_keys(name, self.augment_args);
        let mut seen_argument_keys = HashSet::new();
        for key in &cli_argument_keys {
            if !seen_argument_keys.insert(key.clone()) {
                return Err(StorageBackendBuildError::DuplicateCliArgument {
                    argument: key.to_string(),
                });
            }
        }

        let access = self.access.ok_or(StorageBackendBuildError::MissingAccess)?;
        if self.bare_pattern_mapper.is_some() && self.bare_location_mapper.is_none() {
            return Err(StorageBackendBuildError::BarePatternMapperWithoutBareLocationMapper);
        }
        let location_validator = self
            .location_validator
            .ok_or(StorageBackendBuildError::MissingLocationValidator)?;
        let object_store_creator = self
            .object_store_creator
            .ok_or(StorageBackendBuildError::MissingObjectStoreCreator)?;

        Ok(StorageBackend {
            definition: Box::new(TypedBackendDefinition {
                name,
                schemes: self.schemes.into_boxed_slice(),
                access,
                bare_location_mapper: self.bare_location_mapper,
                bare_pattern_mapper: self.bare_pattern_mapper,
                location_validator,
                object_store_creator,
                prepare_output_target: self.prepare_output_target,
                uses_shared_retries: self.uses_shared_retries,
                cli_argument_keys: cli_argument_keys.into_boxed_slice(),
                augment_args: self.augment_args,
                parse_args: self.parse_args,
            }),
        })
    }
}

/// Errors that make one storage backend definition incomplete or ambiguous.
#[derive(Debug, Error)]
pub enum StorageBackendBuildError {
    #[error("storage backend name is required")]
    MissingName,
    #[error("invalid storage backend name: {name}")]
    InvalidName { name: &'static str },
    #[error("storage backend must claim at least one URL scheme")]
    MissingSchemes,
    #[error("invalid storage backend URL scheme: {scheme}")]
    InvalidScheme { scheme: &'static str },
    #[error("duplicate storage backend URL scheme: {scheme}")]
    DuplicateScheme { scheme: &'static str },
    #[error("storage backend defines CLI argument more than once: {argument}")]
    DuplicateCliArgument { argument: String },
    #[error("storage backend access is required")]
    MissingAccess,
    #[error("storage backend location validator is required")]
    MissingLocationValidator,
    #[error("storage backend cannot map bare patterns without mapping bare locations")]
    BarePatternMapperWithoutBareLocationMapper,
    #[error("storage backend object-store creator is required")]
    MissingObjectStoreCreator,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum CliArgumentKey {
    Id(String),
    Long(String),
    Short(char),
}

impl fmt::Display for CliArgumentKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(id) => write!(formatter, "Clap ID {id:?}"),
            Self::Long(long) => write!(formatter, "--{long}"),
            Self::Short(short) => write!(formatter, "-{short}"),
        }
    }
}

pub(crate) fn argument_keys(
    name: &'static str,
    augment_args: fn(Command) -> Command,
) -> Vec<CliArgumentKey> {
    let command = augment_args(Command::new(name));
    let mut keys = Vec::new();
    for argument in command.get_arguments() {
        keys.push(CliArgumentKey::Id(argument.get_id().as_str().to_owned()));
        if let Some(long) = argument.get_long() {
            keys.push(CliArgumentKey::Long(long.to_owned()));
        }
        if let Some(aliases) = argument.get_all_aliases() {
            keys.extend(
                aliases
                    .into_iter()
                    .map(|alias| CliArgumentKey::Long(alias.to_owned())),
            );
        }
        if let Some(short) = argument.get_short() {
            keys.push(CliArgumentKey::Short(short));
        }
        if let Some(aliases) = argument.get_all_short_aliases() {
            keys.extend(aliases.into_iter().map(CliArgumentKey::Short));
        }
    }
    for group in command.get_groups() {
        keys.push(CliArgumentKey::Id(group.get_id().as_str().to_owned()));
    }
    keys
}

fn valid_backend_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_scheme(scheme: &str) -> bool {
    let mut bytes = scheme.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'+' | b'.' | b'-')
        })
}
