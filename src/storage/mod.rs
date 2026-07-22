//! Local object-store locations and clients.
//!
//! Accepted locations use bare local paths or `file://` URLs. Remote schemes
//! (`gs://`) are added with the Google Cloud Storage backend in a later change.

mod config;
mod gcs;
mod glob;
mod input;
mod location;
mod output;
mod registry;

pub use config::{StorageArgs, StorageConfig};
pub use input::InputObject;
pub use location::{ObjectLocation, StoreHandle, StoreKind};
pub use output::{ObjectOutput, OutputPolicy};
pub use registry::StorageContext;
