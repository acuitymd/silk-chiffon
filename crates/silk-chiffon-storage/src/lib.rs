//! Strict locations and registered `object_store` providers for Silk Chiffon.
//!
//! A [`StorageRegistry`] composes ordinary Clap argument structs and binds their concrete values into a command-scoped [`StorageResolver`]. Each provider declares its access separately from its location resolver. The resolver caches one client per URL origin so direct I/O and DataFusion can share the same upstream store.

use std::{
    path::{Path as FilePath, PathBuf},
    sync::Arc,
};

use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use thiserror::Error;
use url::Url;

pub mod local;
mod provider;
mod retry;

pub use object_store::RetryConfig;
pub use provider::{
    ProviderResolution, ProviderResolver, StorageAccess, StorageDirection,
    StorageProviderRegistration, StorageProviderRegistrationBuilder, StorageRegistry,
    StorageRegistryBuilder, StorageRegistryError, StorageResolver, StorageResolverBuildError,
};
pub use retry::{RetryArgs, RetryConfigurationError};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage location cannot be empty")]
    EmptyLocation,
    #[error("working directory must be absolute: {0}")]
    RelativeWorkingDirectory(PathBuf),
    #[error("ambiguous storage location: {0}")]
    AmbiguousLocation(String),
    #[error("unsupported storage scheme: {0}")]
    UnsupportedScheme(String),
    #[error("storage URL must use the exact lowercase {scheme}:// form: {input}")]
    NonCanonicalStorageUrl { scheme: String, input: String },
    #[error("storage provider {provider} is disabled: {diagnostic}")]
    ProviderDisabled {
        provider: &'static str,
        diagnostic: &'static str,
    },
    #[error("{direction} resolution is unsupported for storage provider: {provider}")]
    DirectionUnsupported {
        provider: &'static str,
        direction: StorageDirection,
    },
    #[error("storage provider {provider} failed to resolve {direction}: {source}")]
    ProviderResolution {
        provider: &'static str,
        direction: StorageDirection,
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
    #[error("storage URL path contains a character that must be percent-encoded: {0}")]
    UnencodedUrlPath(String),
    #[error("filesystem path cannot be represented as a local file URL: {0}")]
    InvalidFilePath(PathBuf),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("output already exists: {0}")]
    OutputAlreadyExists(Url),
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// One parsed storage location with a canonical URL representation.
pub struct Location {
    url: Url,
}

impl Location {
    /// Parses a bare local path or canonical storage URL against an absolute working directory.
    ///
    /// Bare paths retain filesystem characters. URL paths must encode characters disallowed by URL syntax.
    pub fn parse(
        input: impl AsRef<str>,
        working_directory: impl AsRef<FilePath>,
    ) -> Result<Self, StorageError> {
        let input = input.as_ref();
        if input.is_empty() {
            return Err(StorageError::EmptyLocation);
        }

        let working_directory = working_directory.as_ref();
        if !working_directory.is_absolute() {
            return Err(StorageError::RelativeWorkingDirectory(
                working_directory.to_path_buf(),
            ));
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
                None => {
                    let path = FilePath::new(input);
                    let absolute = if path.is_absolute() {
                        path.to_path_buf()
                    } else {
                        working_directory.join(path)
                    };
                    Url::from_file_path(&absolute)
                        .map_err(|()| StorageError::InvalidFilePath(absolute))?
                }
            }
        };

        Ok(Self { url })
    }

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
    validate_url_path_encoding(input, raw_path, &url)?;
    Ok(url)
}

fn scheme_like_prefix(input: &str) -> Result<Option<&str>, StorageError> {
    if FilePath::new(input).is_absolute() {
        return Ok(None);
    }

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
    validate_url_path_encoding(input, raw_path, &url)?;
    url.to_file_path()
        .map_err(|()| StorageError::InvalidFilePath(PathBuf::from(input)))?;

    Ok(url)
}

fn validate_url_path_encoding(input: &str, raw_path: &str, url: &Url) -> Result<(), StorageError> {
    if url.path().strip_prefix('/').unwrap_or(url.path()) != raw_path {
        return Err(StorageError::UnencodedUrlPath(input.to_owned()));
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

#[derive(Clone)]
/// An exact location paired with its upstream object-store client and object path.
pub struct ResolvedLocation {
    pub url: Url,
    pub store: Arc<dyn ObjectStore>,
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
    pub fn local_path(&self) -> Result<PathBuf, StorageError> {
        self.url
            .to_file_path()
            .map_err(|()| StorageError::InvalidFilePath(PathBuf::from(self.url.as_str())))
    }

    pub fn store_url(&self) -> &Url {
        &self.store_url
    }
}

pub async fn validate_input(location: &ResolvedLocation) -> Result<ObjectMeta, StorageError> {
    Ok(location.store.head(&location.path).await?)
}

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
