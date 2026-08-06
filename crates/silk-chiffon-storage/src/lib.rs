//! Storage locations and registered `object_store` providers for Silk Chiffon.
//!
//! [`LocationInput`] distinguishes an explicit canonical URL from schemeless text without
//! assigning meaning to that text. A [`StorageRegistry`] composes providers and their
//! provider-specific Clap arguments, then binds one command's parsed settings into a
//! [`StorageResolver`]. One provider may claim schemeless input and map it to a [`Location`] using
//! its bound settings.
//!
//! Resolution selects a provider, enforces its declared [`StorageAccess`], and translates the URL
//! into an object path. It reuses one client per store root (scheme, host, and port) and does not
//! impose existence or overwrite policy. Call [`validate_input`] and [`preflight_output`] when a
//! command needs those checks.

use std::{path::PathBuf, sync::Arc};

use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use thiserror::Error;
use url::Url;

pub mod local;
mod provider;
mod retry;

pub use object_store::RetryConfig;
pub use provider::{
    BareLocationMapper, ProviderResolution, ProviderResolver, StorageAccess, StorageDirection,
    StorageProviderRegistration, StorageProviderRegistrationBuilder, StorageRegistry,
    StorageRegistryBuilder, StorageRegistryError, StorageResolver, StorageResolverBuildError,
};
pub use retry::{RetryArgs, RetryConfigurationError};

/// Errors produced while parsing, resolving, or checking a storage location.
#[derive(Debug, Error)]
pub enum StorageError {
    /// The input location is empty.
    #[error("storage location cannot be empty")]
    EmptyLocation,
    /// The colon comes before the first path separator, but the prefix is not a valid URL scheme.
    #[error("ambiguous storage location: {0}")]
    AmbiguousLocation(String),
    /// No provider is registered to interpret schemeless input.
    #[error("bare storage locations are unsupported: {0}")]
    UnsupportedBareLocation(String),
    #[error("storage URL requires an explicit scheme: {0}")]
    UrlSchemeRequired(String),
    /// No provider is registered for the location's URL scheme.
    #[error("unsupported storage scheme: {0}")]
    UnsupportedScheme(String),
    /// A storage URL does not use the required lowercase scheme and `://` separator.
    #[error("storage URL must use the exact lowercase {scheme}:// form: {input}")]
    NonCanonicalStorageUrl {
        /// The lowercase scheme spelling required by the parser.
        scheme: String,
        /// The rejected URL source text.
        input: String,
    },
    /// A bare-location mapper returned a scheme owned by another provider or by no provider.
    #[error(
        "storage provider {provider} mapped a bare location to scheme it does not own: {scheme}"
    )]
    BareLocationSchemeMismatch {
        /// The provider selected to interpret the bare location.
        provider: &'static str,
        /// The scheme returned by that provider's mapper.
        scheme: String,
    },
    /// The provider does not support the requested input or output direction.
    #[error("{direction} resolution is unsupported for storage provider: {provider}")]
    DirectionUnsupported {
        /// The registered provider name.
        provider: &'static str,
        /// The direction rejected by the provider's access declaration.
        direction: StorageDirection,
    },
    /// The provider callback or its lazy store factory failed.
    #[error("storage provider {provider} failed to resolve {direction}: {source}")]
    ProviderResolution {
        /// The registered provider name.
        provider: &'static str,
        /// The direction being resolved when the provider failed.
        direction: StorageDirection,
        /// The provider-specific failure.
        #[source]
        source: anyhow::Error,
    },
    /// A local file URL does not use the exact lowercase `file:///` form.
    #[error("local file URL must use the exact lowercase file:/// form: {0}")]
    NonCanonicalFileUrl(String),
    /// The URL parser rejected the supplied source text.
    #[error("invalid storage URL {input}: {source}")]
    InvalidUrl {
        /// The rejected URL source text.
        input: String,
        /// The URL parser's error.
        source: url::ParseError,
    },
    /// A storage URL contains a fragment.
    #[error("storage URLs cannot contain fragments: {0}")]
    FragmentNotSupported(String),
    /// A storage URL contains embedded user information.
    #[error("storage URLs cannot contain user information: {0}")]
    UserInfoNotSupported(String),
    /// A percent escape is incomplete or contains non-hexadecimal digits.
    #[error("invalid percent encoding in storage URL: {0}")]
    InvalidPercentEncoding(String),
    /// URL parsing would encode or normalize the supplied path.
    #[error("storage URL path is not canonical: {0}")]
    NonCanonicalUrlPath(String),
    /// A filesystem path cannot become a `file:` URL, or a resolved URL cannot become a local path.
    #[error("filesystem path cannot be represented as a local file URL: {0}")]
    InvalidFilePath(PathBuf),
    /// An operation against the upstream object store failed.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    /// Output preflight found an existing object while overwrite was disabled.
    #[error("output already exists: {0}")]
    OutputAlreadyExists(Url),
}

/// One parsed storage location with a canonical URL representation.
///
/// A location contains syntax only. Parsing does not require a registered provider and does not
/// contact the target store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Location {
    url: Url,
}

/// Raw location syntax before schemeless input has been assigned to a provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocationInput {
    /// A canonical URL with an explicit scheme.
    Url(Location),
    /// Input without a URL scheme, preserved exactly for the provider that claims it.
    Bare(String),
}

impl LocationInput {
    /// Classifies raw input without assigning meaning to schemeless text.
    ///
    /// Explicit URLs are parsed and checked for canonical spelling. Input with no scheme-like
    /// prefix is preserved exactly in [`Self::Bare`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when the input is empty or explicit URL syntax is malformed,
    /// ambiguous, or noncanonical.
    pub fn parse(input: impl AsRef<str>) -> Result<Self, StorageError> {
        let input = input.as_ref();
        if input.is_empty() {
            return Err(StorageError::EmptyLocation);
        }

        match scheme_like_prefix(input)? {
            Some(_) => Location::parse_url(input).map(Self::Url),
            None => Ok(Self::Bare(input.to_owned())),
        }
    }
}

impl From<Location> for LocationInput {
    fn from(location: Location) -> Self {
        Self::Url(location)
    }
}

impl Location {
    /// Parses a canonical storage URL with an explicit scheme.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when the input is empty, lacks a scheme, or is malformed,
    /// ambiguous, or noncanonical.
    pub fn parse_url(input: impl AsRef<str>) -> Result<Self, StorageError> {
        let input = input.as_ref();
        if input.is_empty() {
            return Err(StorageError::EmptyLocation);
        }

        let url = if let Some(raw_path) = input.strip_prefix("file:///") {
            if raw_path.starts_with('/') {
                return Err(StorageError::NonCanonicalFileUrl(input.to_owned()));
            }
            parse_file_url(input, raw_path)?
        } else {
            match scheme_like_prefix(input)? {
                Some(scheme) if scheme.eq_ignore_ascii_case("file") => {
                    return Err(StorageError::NonCanonicalFileUrl(input.to_owned()));
                }
                Some(scheme) => parse_storage_url(input, scheme)?,
                None => return Err(StorageError::UrlSchemeRequired(input.to_owned())),
            }
        };

        Ok(Self { url })
    }

    #[cfg(feature = "local-bare-paths")]
    pub(crate) fn from_file_path(path: impl AsRef<std::path::Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let url = Url::from_file_path(path)
            .map_err(|()| StorageError::InvalidFilePath(path.to_path_buf()))?;
        Ok(Self { url })
    }

    /// Returns the canonical URL, including any query supplied in URL source text.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

fn parse_storage_url(input: &str, scheme: &str) -> Result<Url, StorageError> {
    let normalized_scheme = scheme.to_ascii_lowercase();
    if scheme != normalized_scheme || !input.starts_with(&format!("{scheme}://")) {
        return Err(StorageError::NonCanonicalStorageUrl {
            scheme: normalized_scheme,
            input: input.to_owned(),
        });
    }
    if !has_valid_percent_encoding(input) {
        return Err(StorageError::InvalidPercentEncoding(input.to_owned()));
    }
    if input.contains('#') {
        return Err(StorageError::FragmentNotSupported(input.to_owned()));
    }

    let remainder = &input[scheme.len() + 3..];
    let authority_and_path = remainder
        .split_once('?')
        .map_or(remainder, |(before_query, _)| before_query);
    let raw_path = authority_and_path
        .split_once('/')
        .map_or("", |(_, raw_path)| raw_path);

    let url = Url::parse(input).map_err(|source| StorageError::InvalidUrl {
        input: input.to_owned(),
        source,
    })?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(StorageError::UserInfoNotSupported(input.to_owned()));
    }
    validate_canonical_url_path(input, raw_path, &url)?;
    Ok(url)
}

fn scheme_like_prefix(input: &str) -> Result<Option<&str>, StorageError> {
    let Some(colon) = input.find(':') else {
        return Ok(None);
    };
    let first_separator = input.find(['/', '\\']).unwrap_or(usize::MAX);
    if colon > first_separator {
        return Ok(None);
    }

    let scheme = &input[..colon];
    let mut characters = scheme.chars();
    let is_scheme = characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        });

    if !is_scheme {
        return Err(StorageError::AmbiguousLocation(input.to_owned()));
    }
    Ok(Some(scheme))
}

fn parse_file_url(input: &str, raw_path: &str) -> Result<Url, StorageError> {
    if !has_valid_percent_encoding(input) {
        return Err(StorageError::InvalidPercentEncoding(input.to_owned()));
    }
    if raw_path.contains('#') {
        return Err(StorageError::FragmentNotSupported(input.to_owned()));
    }
    let raw_path = raw_path
        .split_once('?')
        .map_or(raw_path, |(before_query, _)| before_query);

    let url = Url::parse(input).map_err(|source| StorageError::InvalidUrl {
        input: input.to_owned(),
        source,
    })?;
    validate_canonical_url_path(input, raw_path, &url)?;
    url.to_file_path()
        .map_err(|()| StorageError::InvalidFilePath(PathBuf::from(input)))?;

    Ok(url)
}

fn validate_canonical_url_path(input: &str, raw_path: &str, url: &Url) -> Result<(), StorageError> {
    if url.path().strip_prefix('/').unwrap_or(url.path()) != raw_path {
        return Err(StorageError::NonCanonicalUrlPath(input.to_owned()));
    }
    Ok(())
}

fn has_valid_percent_encoding(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len()
            || !bytes[index + 1].is_ascii_hexdigit()
            || !bytes[index + 2].is_ascii_hexdigit()
        {
            return false;
        }
        index += 3;
    }
    true
}

/// An exact location paired with its upstream object-store client and object path.
///
/// [`Self::url`] preserves the canonical exact-location URL, including its query.
/// [`Self::store_url`] is the resolver's cache key and the URL callers use to register
/// [`Self::store`] with DataFusion.
#[derive(Clone)]
pub struct ResolvedLocation {
    /// The canonical URL for the exact object, including its path and query.
    pub url: Url,
    /// The provider's client for the store root.
    pub store: Arc<dyn ObjectStore>,
    /// The provider-specific path passed to operations on [`Self::store`].
    pub path: ObjectPath,
    store_url: Url,
}

impl std::fmt::Debug for ResolvedLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedLocation")
            .field("url", &self.url)
            .field("store", &self.store)
            .field("path", &self.path)
            .field("store_url", &self.store_url)
            .finish()
    }
}

impl ResolvedLocation {
    /// Converts a resolved `file:` URL back into a filesystem path.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidFilePath`] when this location is not a representable local
    /// file URL.
    pub fn local_path(&self) -> Result<PathBuf, StorageError> {
        if self.url.scheme() != "file" {
            return Err(StorageError::InvalidFilePath(PathBuf::from(
                self.url.as_str(),
            )));
        }
        self.url
            .to_file_path()
            .map_err(|()| StorageError::InvalidFilePath(PathBuf::from(self.url.as_str())))
    }

    /// Returns the root URL used to cache this store and register it with DataFusion.
    ///
    /// The returned URL retains the scheme, host, and port and has `/` as its path with no query or
    /// fragment. This crate exposes the URL. The caller performs any DataFusion registration.
    pub fn store_url(&self) -> &Url {
        &self.store_url
    }
}

/// Requires the resolved input object to exist and returns its metadata.
///
/// This invokes `ObjectStoreExt::head` once. The provider may retry the underlying request.
/// Resolution itself deliberately omits this policy so callers can resolve locations that they
/// intend to create.
///
/// # Errors
///
/// Returns [`StorageError::ObjectStore`] when the object is absent or the metadata request
/// otherwise fails.
pub async fn validate_input(location: &ResolvedLocation) -> Result<ObjectMeta, StorageError> {
    Ok(location.store.head(&location.path).await?)
}

/// Checks whether a resolved output may be created under the caller's overwrite policy.
///
/// When `overwrite` is `true`, this returns without contacting storage. Otherwise it invokes
/// `ObjectStoreExt::head` once, accepts a not-found response, and rejects an existing object. The
/// provider may retry the underlying request. The check is advisory and does not reserve the
/// destination against another writer.
///
/// # Errors
///
/// Returns [`StorageError::OutputAlreadyExists`] for an existing object or
/// [`StorageError::ObjectStore`] when the metadata request fails for another reason.
pub async fn preflight_output(
    location: &ResolvedLocation,
    overwrite: bool,
) -> Result<(), StorageError> {
    if overwrite {
        return Ok(());
    }

    match location.store.head(&location.path).await {
        Ok(_) => Err(StorageError::OutputAlreadyExists(location.url.clone())),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
