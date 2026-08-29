//! Object-store glob parsing and prefix selection.
//!
//! Patterns are matched against complete object keys with filesystem-style `/`
//! separator rules. Listing starts at the longest complete literal component
//! before a glob expression. The same matching rules apply to local paths and
//! GCS keys, where `/` remains part of the key rather than proof of a directory.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use glob::{MatchOptions, Pattern};
use object_store::path::Path;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode, utf8_percent_encode};
use url::Url;

use super::{ObjectLocation, StorageContext, StoreKind, location::has_glob_syntax};

const GCS_KEY_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'#')
    .add(b'%')
    .add(b'*')
    .add(b'?')
    .add(b'[')
    .add(b']');

pub(crate) enum ParsedInput {
    Exact(ObjectLocation),
    Glob(ParsedGlob),
}

pub(crate) struct ParsedGlob {
    original: String,
    location: ObjectLocation,
    matcher: Pattern,
    list_prefix: Path,
}

impl ParsedInput {
    pub(crate) fn parse(storage: &StorageContext, value: &str) -> Result<Self> {
        let location = storage.resolve_pattern(value)?;
        let PatternKey {
            matcher,
            has_syntax,
        } = pattern_key(value, &location)?;
        if !has_syntax {
            return Ok(Self::Exact(location));
        }

        let compiled = Pattern::new(&matcher)
            .with_context(|| format!("invalid input glob pattern '{value}'"))?;
        let list_prefix = literal_listing_prefix(&matcher);
        Ok(Self::Glob(ParsedGlob {
            original: value.to_string(),
            location,
            matcher: compiled,
            list_prefix,
        }))
    }
}

impl ParsedGlob {
    pub(crate) fn original(&self) -> &str {
        &self.original
    }

    pub(crate) fn location(&self) -> &ObjectLocation {
        &self.location
    }

    pub(crate) fn list_prefix(&self) -> &Path {
        &self.list_prefix
    }

    pub(crate) fn matches(&self, path: &Path) -> bool {
        self.matcher.matches_with(
            path.as_ref(),
            MatchOptions {
                case_sensitive: true,
                require_literal_separator: true,
                require_literal_leading_dot: false,
            },
        )
    }

    pub(crate) fn concrete_location(&self, path: Path) -> Result<ObjectLocation> {
        let display = match self.location.store().kind() {
            StoreKind::Gcs { bucket } => {
                let key = path
                    .parts()
                    .map(|part| utf8_percent_encode(part.as_ref(), GCS_KEY_ENCODE_SET).to_string())
                    .collect::<Vec<_>>()
                    .join("/");
                format!("gs://{bucket}/{key}")
            }
            StoreKind::Local => self.local_display(&path)?,
        };
        Ok(ObjectLocation::new(
            display.clone(),
            display,
            path,
            Arc::clone(self.location.store()),
        ))
    }

    fn local_display(&self, path: &Path) -> Result<String> {
        let absolute = PathBuf::from("/").join(path.as_ref());
        if self.original.to_ascii_lowercase().starts_with("file://") {
            return Url::from_file_path(&absolute)
                .map(String::from)
                .map_err(|()| {
                    anyhow::anyhow!("could not render local input '{}': invalid path", path)
                });
        }
        if std::path::Path::new(&self.original).is_absolute() {
            return Ok(absolute.to_string_lossy().into_owned());
        }

        let current = std::env::current_dir().context("could not read the current directory")?;
        Ok(absolute
            .strip_prefix(current)
            .unwrap_or(&absolute)
            .to_string_lossy()
            .into_owned())
    }
}

struct PatternKey {
    matcher: String,
    has_syntax: bool,
}

fn pattern_key(value: &str, location: &ObjectLocation) -> Result<PatternKey> {
    match location.store().kind() {
        StoreKind::Gcs { .. } => {
            let (_, remainder) = value
                .split_once("://")
                .expect("resolved GCS locations contain a scheme");
            let (_, encoded_key) = remainder
                .split_once('/')
                .expect("resolved GCS locations contain an object key");
            Ok(PatternKey {
                matcher: decode_url_pattern(encoded_key, value)?,
                has_syntax: has_glob_syntax(encoded_key, true),
            })
        }
        StoreKind::Local if value.to_ascii_lowercase().starts_with("file://") => {
            let url = Url::parse(value).with_context(|| format!("invalid file URL '{value}'"))?;
            Ok(PatternKey {
                matcher: decode_url_pattern(url.path().trim_start_matches('/'), value)?,
                has_syntax: has_glob_syntax(url.path(), true),
            })
        }
        StoreKind::Local => Ok(PatternKey {
            matcher: location.path().as_ref().to_string(),
            has_syntax: has_glob_syntax(value, false),
        }),
    }
}

fn decode_url_pattern(encoded: &str, original: &str) -> Result<String> {
    let mut protected = Vec::with_capacity(encoded.len());
    let bytes = encoded.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
            && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit)
        {
            let byte = u8::from_str_radix(&encoded[index + 1..index + 3], 16)
                .expect("validated hexadecimal percent escape");
            if matches!(byte, b'*' | b'?' | b'[' | b']') {
                protected
                    .extend_from_slice(Pattern::escape(&char::from(byte).to_string()).as_bytes());
                index += 3;
                continue;
            }
        }
        protected.push(bytes[index]);
        index += 1;
    }

    percent_decode(&protected)
        .decode_utf8()
        .with_context(|| format!("input glob pattern is not UTF-8: '{original}'"))
        .map(|value| value.into_owned())
}

fn literal_listing_prefix(matcher: &str) -> Path {
    let components = matcher.split('/').collect::<Vec<_>>();
    let mut literal = Vec::new();
    for component in &components {
        let Some(component) = literal_component(component) else {
            break;
        };
        literal.push(component);
    }

    // `list` omits an object exactly equal to its prefix, so a fully literalized
    // escape pattern must list its parent component.
    if literal.len() == components.len() {
        literal.pop();
    }
    if literal.is_empty() {
        Path::ROOT
    } else {
        Path::parse(literal.join("/")).expect("literal prefix came from a parsed object path")
    }
}

fn literal_component(component: &str) -> Option<String> {
    let mut literal = String::with_capacity(component.len());
    let mut chars = component.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '*' | '?' => return None,
            '[' => {
                let first = chars.next()?;
                let second = chars.next()?;
                let escaped = match (first, second) {
                    ('*', ']') => '*',
                    ('?', ']') => '?',
                    ('[', ']') => '[',
                    (']', ']') => ']',
                    _ => return None,
                };
                literal.push(escaped);
            }
            character => literal.push(character),
        }
    }
    Some(literal)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::{
        StorageConfig,
        gcs::{GcsEnvironment, GcsStoreFactory},
    };
    use object_store::{ObjectStore, memory::InMemory};

    #[derive(Debug)]
    struct MemoryFactory;

    impl GcsStoreFactory for MemoryFactory {
        fn build(
            &self,
            _bucket: &str,
            _environment: &GcsEnvironment,
        ) -> Result<Arc<dyn ObjectStore>> {
            Ok(Arc::new(InMemory::new()))
        }
    }

    fn storage() -> StorageContext {
        StorageContext::with_parts(
            StorageConfig::default(),
            GcsEnvironment::from_pairs([("GOOGLE_SKIP_SIGNATURE", "true")]).unwrap(),
            Arc::new(MemoryFactory),
        )
        .unwrap()
    }

    fn parsed(pattern: &str) -> ParsedGlob {
        match ParsedInput::parse(&storage(), pattern).unwrap() {
            ParsedInput::Exact(_) => panic!("expected glob"),
            ParsedInput::Glob(pattern) => pattern,
        }
    }

    #[test]
    fn selects_longest_complete_literal_prefix() {
        let pattern = parsed("gs://bucket/data/year=2025/part-*.parquet");

        assert_eq!(pattern.list_prefix().as_ref(), "data/year=2025");
    }

    #[test]
    fn supports_glob_expressions_and_recursive_patterns() {
        let cases = [
            ("gs://bucket/data/part-*.parquet", "data/part-12.parquet"),
            ("gs://bucket/data/part-?.parquet", "data/part-1.parquet"),
            ("gs://bucket/data/part-[12].parquet", "data/part-2.parquet"),
            ("gs://bucket/data/**/part.parquet", "data/a/b/part.parquet"),
        ];

        for (pattern, value) in cases {
            assert!(parsed(pattern).matches(&Path::from(value)), "{pattern}");
        }
    }

    #[test]
    fn escaped_tokens_are_literal_prefix_components() {
        let pattern = parsed("gs://bucket/data/literal[*]/part-?.parquet");

        assert_eq!(pattern.list_prefix().as_ref(), "data/literal*");
        assert!(pattern.matches(&Path::parse("data/literal*/part-1.parquet").unwrap()));
    }

    #[test]
    fn percent_escaped_tokens_stay_literal() {
        assert!(matches!(
            ParsedInput::parse(&storage(), "gs://bucket/data/literal%2A.parquet").unwrap(),
            ParsedInput::Exact(_)
        ));

        let pattern = parsed("gs://bucket/data/literal%2A/part-?.parquet");
        assert_eq!(pattern.list_prefix().as_ref(), "data/literal*");
        assert!(pattern.matches(&Path::parse("data/literal*/part-1.parquet").unwrap()));
    }

    #[test]
    fn star_does_not_cross_object_key_separators() {
        let pattern = parsed("gs://bucket/data/*.parquet");

        assert!(pattern.matches(&Path::from("data/a.parquet")));
        assert!(!pattern.matches(&Path::from("data/nested/a.parquet")));
    }
}
