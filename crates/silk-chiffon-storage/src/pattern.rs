//! Storage location syntax that may select more than one exact object.

use glob::Pattern;

use crate::{Location, LocationInput, StorageError, location::scheme_like_prefix};

/// One parsed exact location or object-path glob.
///
/// Parsing validates syntax only. Backend routing, object-store access, and expansion happen in a
/// [`StorageSession`](crate::StorageSession).
#[derive(Clone, Debug)]
pub struct LocationPattern {
    pub(crate) input: PatternInput,
}

#[derive(Clone, Debug)]
pub(crate) enum PatternInput {
    Exact(LocationInput),
    Bare {
        source: String,
    },
    Url {
        source: String,
        location: Location,
        matcher: Pattern,
        literal_prefix: String,
    },
}

impl LocationPattern {
    /// Parses a location that may contain object-path glob syntax.
    ///
    /// A raw `?` is a one-character wildcard. In an explicit URL, `??` begins the query copied to
    /// each matched exact URL; percent-encoded metacharacters are literals.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when the input is empty, ambiguous, noncanonical, contains glob
    /// syntax outside the URL path, or contains an invalid glob.
    pub fn parse(input: impl AsRef<str>) -> Result<Self, StorageError> {
        let input = input.as_ref();
        if input.is_empty() {
            return Err(StorageError::EmptyLocation);
        }

        if glob_before_url_separator(input) {
            return Err(StorageError::PatternOutsideUrlPath(input.to_owned()));
        }

        match scheme_like_prefix(input)? {
            Some(_) => Self::parse_url(input),
            None if has_active_glob(input) => {
                validate_recursive_globs(input, input)?;
                Pattern::new(input).map_err(|source| StorageError::InvalidLocationPattern {
                    input: input.to_owned(),
                    source,
                })?;
                Ok(Self {
                    input: PatternInput::Bare {
                        source: input.to_owned(),
                    },
                })
            }
            None => Ok(Self {
                input: PatternInput::Exact(LocationInput::parse(input)?),
            }),
        }
    }

    /// Parses an explicit URL that may contain object-path glob syntax.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when the input is not an explicit canonical URL or its glob syntax
    /// is invalid or appears outside the path.
    pub fn parse_url(input: impl AsRef<str>) -> Result<Self, StorageError> {
        let input = input.as_ref();
        if input.is_empty() {
            return Err(StorageError::EmptyLocation);
        }
        if glob_before_url_separator(input) {
            return Err(StorageError::PatternOutsideUrlPath(input.to_owned()));
        }

        let scheme = scheme_like_prefix(input)?
            .ok_or_else(|| StorageError::UrlSchemeRequired(input.to_owned()))?;
        let after_scheme = input.strip_prefix(&format!("{scheme}://")).ok_or_else(|| {
            StorageError::NonCanonicalStorageUrl {
                scheme: scheme.to_ascii_lowercase(),
                input: input.to_owned(),
            }
        })?;
        let path_start = after_scheme.find('/');
        let authority = path_start.map_or(after_scheme, |index| &after_scheme[..index]);
        if authority.bytes().any(|byte| matches!(byte, b'*' | b'?'))
            || ((authority.contains('[') || authority.contains(']'))
                && url::Url::parse(&format!("{scheme}://{authority}/")).is_err())
        {
            return Err(StorageError::PatternOutsideUrlPath(input.to_owned()));
        }

        let (before_query, query) = match input.split_once("??") {
            Some((before_query, query)) => (before_query, Some(query)),
            None => (input, None),
        };
        let path_start = scheme.len() + 3 + path_start.unwrap_or(after_scheme.len());
        let raw_path = before_query
            .get(path_start..)
            .unwrap_or("")
            .strip_prefix('/')
            .unwrap_or("");
        if raw_path.contains('#') || query.is_some_and(|query| query.contains('#')) {
            return Err(StorageError::FragmentNotSupported(input.to_owned()));
        }

        validate_recursive_globs(raw_path, input)?;
        let canonical = canonical_pattern_url(before_query, path_start, query);
        let location = Location::parse_url(&canonical)?;
        if !has_active_glob(raw_path) {
            return Ok(Self {
                input: PatternInput::Exact(LocationInput::Url(location)),
            });
        }

        let matcher_source = decoded_matcher(raw_path, input)?;
        let literal_prefix = decoded_literal_prefix(raw_path, input)?;
        let matcher = Pattern::new(&matcher_source).map_err(|source| {
            StorageError::InvalidLocationPattern {
                input: input.to_owned(),
                source,
            }
        })?;
        Ok(Self {
            input: PatternInput::Url {
                source: input.to_owned(),
                location,
                matcher,
                literal_prefix,
            },
        })
    }

    #[cfg(feature = "local-bare-paths")]
    pub(crate) fn from_file_path_pattern(
        path: &std::path::Path,
        source: &str,
    ) -> Result<Self, StorageError> {
        let path_source = path
            .to_str()
            .ok_or_else(|| StorageError::InvalidFilePath(path.to_path_buf()))?;
        validate_recursive_globs(path_source, source)?;
        let url = url::Url::from_file_path(path)
            .map_err(|()| StorageError::InvalidFilePath(path.to_path_buf()))?;
        let location = Location::parse_url(url.as_str())?;
        let matcher_source = object_store::path::Path::from_url_path(location.url().path())
            .map_err(|object_path_source| StorageError::InvalidObjectPath {
                location: location.url().clone(),
                source: Box::new(object_path_source),
            })?
            .as_ref()
            .to_owned();
        let matcher = Pattern::new(&matcher_source).map_err(|pattern_source| {
            StorageError::InvalidLocationPattern {
                input: source.to_owned(),
                source: pattern_source,
            }
        })?;
        let literal_prefix = path_source
            .strip_prefix('/')
            .unwrap_or(path_source)
            .split('/')
            .take_while(|segment| !has_active_glob(segment))
            .collect::<Vec<_>>()
            .join("/");
        Ok(Self {
            input: PatternInput::Url {
                source: source.to_owned(),
                location,
                matcher,
                literal_prefix,
            },
        })
    }
}

fn glob_before_url_separator(input: &str) -> bool {
    input
        .find("://")
        .is_some_and(|separator| has_active_glob(&input[..separator]))
}

fn has_active_glob(input: &str) -> bool {
    input.bytes().any(|byte| matches!(byte, b'*' | b'?' | b'['))
}

fn validate_recursive_globs(path: &str, input: &str) -> Result<(), StorageError> {
    if path
        .split('/')
        .any(|segment| segment.contains("**") && segment != "**")
    {
        return Err(StorageError::InvalidLocationPattern {
            input: input.to_owned(),
            source: glob::PatternError {
                pos: path.find("**").unwrap_or_default(),
                msg: "recursive ** must occupy a complete path segment",
            },
        });
    }
    Ok(())
}

fn canonical_pattern_url(before_query: &str, path_start: usize, query: Option<&str>) -> String {
    let mut canonical =
        String::with_capacity(before_query.len() + query.map_or(0, |q| q.len() + 1));
    canonical.push_str(&before_query[..path_start]);
    for byte in before_query[path_start..].bytes() {
        match byte {
            b'*' => canonical.push_str("%2A"),
            b'?' => canonical.push_str("%3F"),
            b'[' => canonical.push_str("%5B"),
            b']' => canonical.push_str("%5D"),
            _ => canonical.push(char::from(byte)),
        }
    }
    if let Some(query) = query {
        canonical.push('?');
        canonical.push_str(query);
    }
    canonical
}

fn decoded_matcher(raw_path: &str, input: &str) -> Result<String, StorageError> {
    let mut matcher = String::with_capacity(raw_path.len());
    let bytes = raw_path.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let start = index;
            while index + 2 < bytes.len()
                && bytes[index] == b'%'
                && bytes[index + 1].is_ascii_hexdigit()
                && bytes[index + 2].is_ascii_hexdigit()
            {
                index += 3;
            }
            if start == index {
                return Err(StorageError::InvalidPercentEncoding(input.to_owned()));
            }
            let mut decoded = Vec::with_capacity((index - start) / 3);
            for encoded in bytes[start..index].chunks_exact(3) {
                decoded.push(hex_value(encoded[1]) * 16 + hex_value(encoded[2]));
            }
            let decoded = std::str::from_utf8(&decoded)
                .map_err(|_| StorageError::InvalidPercentEncoding(input.to_owned()))?;
            matcher.push_str(&Pattern::escape(decoded));
            continue;
        }

        let byte = bytes[index];
        matcher.push(char::from(byte));
        index += 1;
    }
    Ok(matcher)
}

fn decoded_literal_prefix(raw_path: &str, input: &str) -> Result<String, StorageError> {
    let prefix = raw_path
        .split('/')
        .take_while(|segment| !has_active_glob(segment))
        .collect::<Vec<_>>()
        .join("/");
    decoded_matcher(&prefix, input).map(|prefix| unescape_matcher_literals(&prefix))
}

fn unescape_matcher_literals(input: &str) -> String {
    let mut literal = String::with_capacity(input.len());
    let characters = input.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < characters.len() {
        if index + 2 < characters.len()
            && characters[index] == '['
            && characters[index + 2] == ']'
            && matches!(characters[index + 1], '*' | '?' | '[' | ']')
        {
            literal.push(characters[index + 1]);
            index += 3;
        } else {
            literal.push(characters[index]);
            index += 1;
        }
    }
    literal
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => unreachable!("caller validates hexadecimal digits"),
    }
}
