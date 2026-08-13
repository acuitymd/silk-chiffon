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
    #[error("bare storage patterns are unsupported: {0}")]
    UnsupportedBarePattern(String),
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
    #[error("storage backend {backend} mapped a bare pattern to unclaimed scheme: {scheme}")]
    BarePatternSchemeMismatch {
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
    #[error("storage backend {backend} failed to map bare pattern {bare_pattern:?}: {source}")]
    BarePatternMapping {
        backend: &'static str,
        bare_pattern: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("storage backend {backend} rejected location {location}: {source}")]
    LocationValidation {
        backend: &'static str,
        location: Url,
        #[source]
        source: anyhow::Error,
    },
    #[error("storage location has an invalid object path {location}: {source}")]
    InvalidObjectPath {
        location: Url,
        source: Box<object_store::path::Error>,
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
    #[error("invalid storage location pattern {input:?}: {source}")]
    InvalidLocationPattern {
        input: String,
        source: glob::PatternError,
    },
    #[error("storage pattern syntax is allowed only in the URL path: {0}")]
    PatternOutsideUrlPath(String),
    /// URL parsing would encode or normalize the supplied path.
    #[error("storage URL path is not canonical: {0}")]
    NonCanonicalUrlPath(String),
    /// A filesystem path cannot become a `file:` URL, or a handle URL cannot become a local path.
    #[error("filesystem path cannot be represented as a local file URL: {0}")]
    InvalidFilePath(PathBuf),
    /// An operation against an existing upstream object store failed.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("failed to read metadata for storage pattern {pattern:?}: {source}")]
    PatternMetadata {
        pattern: String,
        source: object_store::Error,
    },
    #[error("failed to list storage pattern {pattern:?}: {source}")]
    PatternListing {
        pattern: String,
        source: object_store::Error,
    },
    #[error("output target already exists: {target}")]
    OutputTargetAlreadyExists { target: Url },
    #[error("output target is already claimed by this storage session: {target}")]
    OutputTargetAlreadyClaimed { target: Url },
    #[error("storage backend {backend} failed to prepare output target {target}: {source}")]
    OutputTargetPreparation {
        backend: &'static str,
        target: Url,
        #[source]
        source: anyhow::Error,
    },
}
