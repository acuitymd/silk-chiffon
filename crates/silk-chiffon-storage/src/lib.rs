//! Typed storage backends, command-scoped sessions, and object-store handles.
//!
//! A [`StorageBackend`] defines one storage implementation. It keeps that backend's metadata, Clap
//! argument parser, and typed callbacks together. Registering the definition in a
//! [`StorageRegistry`] makes the backend available to sessions. The registry validates and indexes
//! its fixed backend set, then contributes their arguments to a host-owned Clap command.
//!
//! [`StorageRegistry::create_session`] binds the host's parsed arguments to one command invocation.
//! The resulting [`StorageSession`] owns each backend's typed settings, shared retry
//! configuration, routing indexes, and object-store cache. Calling
//! [`StorageSession::input_handle`] produces an unobserved read handle, while
//! [`StorageSession::prepare_output_target`] claims a write target and applies its preparation
//! policy before returning a [`PreparedOutputTarget`].
//!
//! # First handle
//!
//! With the `local` feature, an explicit file URL follows the complete backend-to-handle flow:
//!
//! ```no_run
//! use clap::Command;
//! use silk_chiffon_storage::{LocationInput, StorageRegistry, local};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # #[cfg(feature = "local")]
//! # {
//! let registry = StorageRegistry::builder()
//!     .register(local::backend()?)
//!     .build()?;
//! let command = registry.augment_args(Command::new("storage-example"));
//! let matches = command.try_get_matches_from(["storage-example"])?;
//! let storage = registry.create_session(&matches)?;
//!
//! let location = LocationInput::parse("file:///tmp/input.parquet")?;
//! let handle = storage.input_handle(&location)?;
//! # let _ = handle;
//! # }
//! # Ok(())
//! # }
//! ```

#[cfg(not(unix))]
compile_error!("silk-chiffon-storage supports Unix targets only");

mod backend;
#[cfg(any(feature = "gcs", feature = "s3"))]
mod cloud;
mod error;
#[cfg(feature = "gcs")]
pub mod gcs;
mod handle;
mod input;
pub mod local;
mod location;
mod output;
mod pattern;
mod registry;
mod retry;
#[cfg(feature = "s3")]
pub mod s3;
mod session;
mod upload;

pub use backend::{
    BareLocationMapper, BarePatternMapper, LocationValidator, ObjectStoreCreatorFn,
    PrepareOutputTargetFn, StorageAccess, StorageBackend, StorageBackendBuildError,
    StorageBackendBuilder, StorageDirection,
};
pub use error::StorageError;
pub use handle::{InputHandle, OutputTarget, PreparedOutputTarget};
pub use input::InputObject;
pub use location::{Location, LocationInput};
pub use object_store::RetryConfig;
pub use output::{ExistingOutput, OutputPreparation};
pub use pattern::LocationPattern;
pub use registry::{StorageRegistry, StorageRegistryBuilder, StorageRegistryError};
pub use retry::RetryConfigurationError;
pub use session::{StorageSession, StorageSessionCreationError};
pub use upload::{BlockingObjectUploadWriter, ObjectUpload, ObjectUploadError, ObjectUploadTask};
