//! Local and Google Cloud Storage locations and clients.
//!
//! Accepted locations use bare local paths, `file://` URLs, or `gs://` URLs.
//! Google clients are created on the first location resolved for a bucket, so
//! local commands don't read Application Default Credentials. Each cache key
//! includes the bucket, endpoint, authentication mode, credential source, and
//! HTTP client settings. Credential values and bearer tokens are omitted from
//! diagnostics.

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
