//! Canonical URL syntax and backend-neutral raw location input.
//!
//! [`LocationInput`] separates explicit URL syntax from schemeless text. Parsing does not consult a
//! [`StorageRegistry`](crate::StorageRegistry), choose a backend, or assign filesystem meaning to a
//! bare string. A [`Location`] contains only a canonical URL.

use std::path::PathBuf;

use url::Url;

use crate::StorageError;

/// One parsed storage location with a canonical URL representation.
///
/// A location contains syntax only. Parsing does not require a registered backend and does not
/// contact the target store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Location {
    url: Url,
}

/// Raw location syntax before schemeless input has been assigned to a backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocationInput {
    /// A canonical URL with an explicit scheme.
    Url(Location),
    /// Input without a URL scheme, preserved exactly for the backend that claims it.
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
    let authority = authority_and_path
        .split_once('/')
        .map_or(authority_and_path, |(authority, _)| authority);
    if authority.contains('@') {
        return Err(StorageError::UserInfoNotSupported(input.to_owned()));
    }
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
    if url.as_str() != input {
        return Err(StorageError::NonCanonicalStorageUrl {
            scheme: normalized_scheme,
            input: input.to_owned(),
        });
    }
    Ok(url)
}

fn scheme_like_prefix(input: &str) -> Result<Option<&str>, StorageError> {
    if cfg!(windows) && has_windows_drive_root(input) {
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

fn has_windows_drive_root(input: &str) -> bool {
    let bytes = input.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphabetic)
        && bytes.get(1) == Some(&b':')
        && (bytes.get(2) == Some(&b'\\')
            || (bytes.get(2) == Some(&b'/') && bytes.get(3) != Some(&b'/')))
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
    if url.as_str() != input {
        return Err(StorageError::NonCanonicalFileUrl(input.to_owned()));
    }
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
