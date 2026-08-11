//! Strongly typed backend settings behind one runtime-selected collection.
//!
//! A backend crate owns a Clap argument type `T`, the functions that add and parse those arguments,
//! and callbacks that accept `&T`. It packages them as a [`StorageBackend`](super::StorageBackend).
//! The host executable owns the complete [`Command`], registers those definitions in a
//! [`StorageRegistry`](crate::StorageRegistry), parses the augmented command, and passes the
//! resulting [`ArgMatches`] into
//! [`StorageRegistry::create_session`](crate::StorageRegistry::create_session).
//!
//! Erasing each parsed settings value as `Any` would separate it from the callbacks that expect its
//! concrete type. Every invocation would then rely on a runtime downcast and on the caller choosing
//! the matching callback. This module instead erases a complete typed backend through behavior
//! traits. The concrete implementation retains `T`, so the compiler checks the parser and callback
//! contract before the backend enters the shared collection.
//!
//! # Definitions and bindings
//!
//! A **backend definition** exists before one command invocation. [`BackendDefinition`] exposes
//! immutable metadata, Clap augmentation, collision keys, and the operation that parses settings.
//! [`TypedBackendDefinition<T>`] retains the concrete parser and callbacks that use `T`.
//!
//! A **backend binding** exists for one storage session. Creating a session asks every definition
//! to parse its own `T` from the host's matches. [`TypedBackendBinding<T>`] owns that parsed value
//! and invokes the matching callbacks with `&T`; [`BackendBinding`] exposes those operations to
//! the session without exposing `T`.
//!
//! ```text
//! TypedBackendDefinition<T>
//!     |
//!     | Box<dyn BackendDefinition>
//!     v
//! StorageBackend --> StorageRegistry
//!                         |
//!                         | create_session(&ArgMatches)
//!                         v
//! TypedBackendBinding<T>
//!     |
//!     | Box<dyn BackendBinding>
//!     v
//! StorageSession
//! ```
//!
//! `BackendDefinition` lets the registry invoke definition-time behavior through dynamic dispatch.
//! After session creation, routing selects a `BackendBinding`, which invokes that backend's typed
//! callbacks. The backend crate continues to use ordinary Rust types on both sides of its contract.

use clap::{ArgMatches, Command};
use object_store::{ObjectStore, RetryConfig};
use url::Url;

use super::{
    BareLocationMapper, BarePatternMapper, CliArgumentKey, LocationValidator, ObjectStoreCreatorFn,
    StorageAccess, StorageDirection,
};
use crate::{Location, LocationPattern};

/// Definition-time behavior shared by storage backends with different settings types.
pub(super) trait BackendDefinition: Send + Sync {
    fn name(&self) -> &'static str;

    fn schemes(&self) -> &[&'static str];

    fn supports(&self, direction: StorageDirection) -> bool;

    fn claims_bare_locations(&self) -> bool;

    fn uses_shared_retries(&self) -> bool;

    fn augment_args(&self, command: Command) -> Command;

    fn argument_keys(&self) -> &[CliArgumentKey];

    fn bind(&self, matches: &ArgMatches) -> Result<Box<dyn BackendBinding>, clap::Error>;
}

/// Session-time behavior shared by bound backends with different settings types.
pub(crate) trait BackendBinding: Send + Sync {
    fn name(&self) -> &'static str;

    fn supports(&self, direction: StorageDirection) -> bool;

    fn uses_shared_retries(&self) -> bool;

    fn map_bare_location(&self, input: &str) -> Option<anyhow::Result<Location>>;

    fn map_bare_pattern(&self, input: &str) -> Option<anyhow::Result<LocationPattern>>;

    fn validate_location(&self, location: &Location) -> anyhow::Result<()>;

    fn create_object_store(
        &self,
        store_url: &Url,
        retry: Option<&RetryConfig>,
    ) -> anyhow::Result<std::sync::Arc<dyn ObjectStore>>;
}

/// One complete backend before command arguments have been parsed.
pub(super) struct TypedBackendDefinition<T> {
    pub(super) name: &'static str,
    pub(super) schemes: Box<[&'static str]>,
    pub(super) access: StorageAccess,
    pub(super) bare_location_mapper: Option<BareLocationMapper<T>>,
    pub(super) bare_pattern_mapper: Option<BarePatternMapper<T>>,
    pub(super) location_validator: LocationValidator<T>,
    pub(super) object_store_creator: ObjectStoreCreatorFn<T>,
    pub(super) uses_shared_retries: bool,
    pub(super) cli_argument_keys: Box<[CliArgumentKey]>,
    pub(super) augment_args: fn(Command) -> Command,
    pub(super) parse_args: fn(&ArgMatches) -> Result<T, clap::Error>,
}

impl<T> BackendDefinition for TypedBackendDefinition<T>
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    fn supports(&self, direction: StorageDirection) -> bool {
        self.access.supports(direction)
    }

    fn claims_bare_locations(&self) -> bool {
        self.bare_location_mapper.is_some()
    }

    fn uses_shared_retries(&self) -> bool {
        self.uses_shared_retries
    }

    fn augment_args(&self, command: Command) -> Command {
        (self.augment_args)(command)
    }

    fn argument_keys(&self) -> &[CliArgumentKey] {
        &self.cli_argument_keys
    }

    fn bind(&self, matches: &ArgMatches) -> Result<Box<dyn BackendBinding>, clap::Error> {
        Ok(Box::new(TypedBackendBinding {
            name: self.name,
            access: self.access,
            settings: (self.parse_args)(matches)?,
            bare_location_mapper: self.bare_location_mapper,
            bare_pattern_mapper: self.bare_pattern_mapper,
            location_validator: self.location_validator,
            object_store_creator: self.object_store_creator,
            uses_shared_retries: self.uses_shared_retries,
        }))
    }
}

/// One backend after its settings have been parsed for a storage session.
struct TypedBackendBinding<T> {
    name: &'static str,
    access: StorageAccess,
    settings: T,
    bare_location_mapper: Option<BareLocationMapper<T>>,
    bare_pattern_mapper: Option<BarePatternMapper<T>>,
    location_validator: LocationValidator<T>,
    object_store_creator: ObjectStoreCreatorFn<T>,
    uses_shared_retries: bool,
}

impl<T> BackendBinding for TypedBackendBinding<T>
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn supports(&self, direction: StorageDirection) -> bool {
        self.access.supports(direction)
    }

    fn uses_shared_retries(&self) -> bool {
        self.uses_shared_retries
    }

    fn map_bare_location(&self, input: &str) -> Option<anyhow::Result<Location>> {
        self.bare_location_mapper
            .map(|mapper| mapper(input, &self.settings))
    }

    fn map_bare_pattern(&self, input: &str) -> Option<anyhow::Result<LocationPattern>> {
        self.bare_pattern_mapper
            .map(|mapper| mapper(input, &self.settings))
    }

    fn validate_location(&self, location: &Location) -> anyhow::Result<()> {
        (self.location_validator)(location, &self.settings)
    }

    fn create_object_store(
        &self,
        store_url: &Url,
        retry: Option<&RetryConfig>,
    ) -> anyhow::Result<std::sync::Arc<dyn ObjectStore>> {
        (self.object_store_creator)(store_url, &self.settings, retry)
    }
}
