//! Errors from location parsing, backend routing, handle creation, and storage checks.

use std::path::PathBuf;

use thiserror::Error;
use url::Url;

use crate::StorageDirection;

/// Errors produced while parsing, creating, or checking a storage handle.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage location cannot be empty")]
    EmptyLocation,
    /// The colon comes before the first path separator, but the prefix is not a valid URL scheme.
    #[error("ambiguous storage location: {0}")]
    AmbiguousLocation(String),
    #[error("bare storage locations are unsupported: {0}")]
    UnsupportedBareLocation(String),
    #[error("storage URL requires an explicit scheme: {0}")]
    UrlSchemeRequired(String),
    #[error("unsupported storage scheme: {0}")]
    UnsupportedScheme(String),
    #[error("storage URL must use the exact lowercase {scheme}:// form: {input}")]
    NonCanonicalStorageUrl { scheme: String, input: String },
    #[error("storage backend {backend} mapped a bare location to unclaimed scheme: {scheme}")]
    BareLocationSchemeMismatch {
        backend: &'static str,
        scheme: String,
    },
    #[error("storage backend {backend} does not support {direction} access")]
    DirectionUnsupported {
        backend: &'static str,
        direction: StorageDirection,
    },
    #[error("storage backend {backend} failed to map bare location {bare_location:?}: {source}")]
    BareLocationMapping {
        backend: &'static str,
        bare_location: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("storage backend {backend} failed to map {location} to an object path: {source}")]
    ObjectPathMapping {
        backend: &'static str,
        location: Url,
        #[source]
        source: anyhow::Error,
    },
    #[error("storage backend {backend} failed to create an object store for {store_url}: {source}")]
    ObjectStoreCreation {
        backend: &'static str,
        store_url: Url,
        #[source]
        source: anyhow::Error,
    },
    #[error("local file URL must use the exact lowercase file:/// form: {0}")]
    NonCanonicalFileUrl(String),
    #[error("invalid storage URL {input}: {source}")]
    InvalidUrl {
        input: String,
        source: url::ParseError,
    },
    #[error("storage URLs cannot contain fragments: {0}")]
    FragmentNotSupported(String),
    #[error("storage URLs cannot contain user information: {0}")]
    UserInfoNotSupported(String),
    #[error("invalid percent encoding in storage URL: {0}")]
    InvalidPercentEncoding(String),
    /// URL parsing would encode or normalize the supplied path.
    #[error("storage URL path is not canonical: {0}")]
    NonCanonicalUrlPath(String),
    /// A filesystem path cannot become a `file:` URL, or a handle URL cannot become a local path.
    #[error("filesystem path cannot be represented as a local file URL: {0}")]
    InvalidFilePath(PathBuf),
    /// An operation against an existing upstream object store failed.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("output already exists: {0}")]
    OutputAlreadyExists(Url),
}
