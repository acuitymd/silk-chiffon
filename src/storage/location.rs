//! Parsed object locations shared by sources, sinks, and inspectors.

use std::{fmt, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::{ObjectStore, path::Path};
use percent_encoding::percent_decode_str;
use url::Url;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum StoreKind {
    Local,
    Gcs { bucket: String },
}

pub struct StoreHandle {
    kind: StoreKind,
    store_url: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
}

impl StoreHandle {
    pub(crate) fn new(
        kind: StoreKind,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            kind,
            store_url,
            store,
        }
    }

    #[must_use]
    pub fn kind(&self) -> &StoreKind {
        &self.kind
    }

    #[must_use]
    pub fn store_url(&self) -> &ObjectStoreUrl {
        &self.store_url
    }

    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
}

impl fmt::Debug for StoreHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreHandle")
            .field("kind", &self.kind)
            .field("store_url", &self.store_url)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ObjectLocation {
    original: String,
    display: String,
    path: Path,
    store: Arc<StoreHandle>,
}

impl ObjectLocation {
    pub(crate) fn new(
        original: String,
        display: String,
        path: Path,
        store: Arc<StoreHandle>,
    ) -> Self {
        Self {
            original,
            display,
            path,
            store,
        }
    }

    #[must_use]
    pub fn original(&self) -> &str {
        &self.original
    }

    #[must_use]
    pub fn display(&self) -> &str {
        &self.display
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn store(&self) -> &Arc<StoreHandle> {
        &self.store
    }

    #[must_use]
    pub fn same_object(&self, other: &Self) -> bool {
        self.store.store_url() == other.store.store_url() && self.path == other.path
    }
}

impl fmt::Debug for ObjectLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectLocation")
            .field("original", &self.original)
            .field("display", &self.display)
            .field("path", &self.path)
            .field("kind", self.store.kind())
            .finish()
    }
}

#[derive(Debug)]
pub(crate) enum ParsedLocation {
    Local {
        original: String,
        display: String,
        path: Path,
    },
    Gcs {
        original: String,
        display: String,
        bucket: String,
        path: Path,
    },
}

impl ParsedLocation {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        if value.is_empty() {
            bail!("object location cannot be empty");
        }

        let Some((scheme, remainder)) = value.split_once("://") else {
            return Self::local_path(value, value);
        };

        match scheme.to_ascii_lowercase().as_str() {
            "file" => Self::file_url(value),
            "gs" => Self::gcs_url(value, remainder),
            unsupported => bail!(
                "unsupported object-store scheme '{unsupported}'; supported schemes are file and gs"
            ),
        }
    }

    pub(crate) fn parse_pattern(value: &str) -> Result<Self> {
        if value.is_empty() {
            bail!("object location cannot be empty");
        }

        let Some((scheme, remainder)) = value.split_once("://") else {
            return Self::local_pattern(value, value, split_local_pattern(value, value)?);
        };

        match scheme.to_ascii_lowercase().as_str() {
            "file" => Self::file_url_pattern(value),
            "gs" => Self::gcs_url(value, remainder),
            unsupported => bail!(
                "unsupported object-store scheme '{unsupported}'; supported schemes are file and gs"
            ),
        }
    }

    fn local_path(original: &str, display: &str) -> Result<Self> {
        let absolute = std::path::absolute(original)
            .with_context(|| format!("could not resolve local path '{display}'"))?;
        let absolute = lower_existing_ancestors(&absolute, display)?;
        let path = Path::from_absolute_path(&absolute)
            .with_context(|| format!("invalid local path '{display}'"))?;

        Ok(Self::Local {
            original: original.to_string(),
            display: display.to_string(),
            path,
        })
    }

    fn local_pattern(original: &str, display: &str, parts: Option<PatternPath>) -> Result<Self> {
        let Some(parts) = parts else {
            return Self::local_path(original, display);
        };
        let absolute_prefix = std::path::absolute(&parts.literal_prefix)
            .with_context(|| format!("could not resolve local glob prefix '{display}'"))?;
        let mut lowered = lower_existing_ancestors(&absolute_prefix, display)?;
        lowered.push(parts.glob_suffix);
        let path = Path::from_absolute_path(&lowered)
            .with_context(|| format!("invalid local glob pattern '{display}'"))?;

        Ok(Self::Local {
            original: original.to_string(),
            display: display.to_string(),
            path,
        })
    }

    fn file_url(value: &str) -> Result<Self> {
        let url = Url::parse(value).with_context(|| format!("invalid file URL '{value}'"))?;
        if url.host_str().is_some_and(|host| host != "localhost") {
            bail!("file URL must not contain a remote host: '{value}'");
        }
        if url.query().is_some() || url.fragment().is_some() {
            bail!("file URL query and fragment characters must be percent-escaped: '{value}'");
        }

        let filesystem_path = url
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("file URL is not an absolute local path: '{value}'"))?;
        let filesystem_path = lower_existing_ancestors(&filesystem_path, value)?;
        let path = Path::from_absolute_path(filesystem_path)
            .with_context(|| format!("invalid file URL path '{value}'"))?;

        Ok(Self::Local {
            original: value.to_string(),
            display: value.to_string(),
            path,
        })
    }

    fn file_url_pattern(value: &str) -> Result<Self> {
        let url = Url::parse(value).with_context(|| format!("invalid file URL '{value}'"))?;
        if url.host_str().is_some_and(|host| host != "localhost") {
            bail!("file URL must not contain a remote host: '{value}'");
        }
        if url.query().is_some() || url.fragment().is_some() {
            bail!("file URL query and fragment characters must be percent-escaped: '{value}'");
        }

        let parts = split_file_url_pattern(value)?;
        if parts.is_none() {
            return Self::file_url(value);
        }
        Self::local_pattern(value, value, parts)
    }

    fn gcs_url(value: &str, remainder: &str) -> Result<Self> {
        let Some((bucket, encoded_key)) = remainder.split_once('/') else {
            bail!("GCS location requires an object key after the bucket: '{value}'");
        };
        if bucket.is_empty() {
            bail!("GCS location requires a bucket: '{value}'");
        }
        if encoded_key.is_empty() {
            bail!("GCS location requires an object key after the bucket: '{value}'");
        }

        let key = percent_decode_str(encoded_key)
            .decode_utf8()
            .with_context(|| format!("GCS object key is not UTF-8: '{value}'"))?;
        let path = Path::parse(key.as_ref())
            .with_context(|| format!("invalid GCS object key in '{value}'"))?;
        if path.is_root() {
            bail!("GCS location requires an object key after the bucket: '{value}'");
        }

        Ok(Self::Gcs {
            original: value.to_string(),
            display: value.to_string(),
            bucket: bucket.to_string(),
            path,
        })
    }
}

struct PatternPath {
    literal_prefix: PathBuf,
    glob_suffix: PathBuf,
}

fn split_local_pattern(value: &str, display: &str) -> Result<Option<PatternPath>> {
    use std::path::Component;

    let mut literal_prefix = PathBuf::new();
    let mut glob_suffix = PathBuf::new();
    let mut found_glob = false;
    for component in std::path::Path::new(value).components() {
        if found_glob {
            if component == Component::ParentDir {
                bail!(
                    "local glob pattern cannot contain '..' after its first glob component: '{display}'"
                );
            }
            glob_suffix.push(component.as_os_str());
            continue;
        }

        let is_glob = match component {
            Component::Normal(value) => has_glob_syntax(&value.to_string_lossy(), false),
            _ => false,
        };
        if is_glob {
            found_glob = true;
            glob_suffix.push(component.as_os_str());
        } else {
            literal_prefix.push(component.as_os_str());
        }
    }

    Ok(found_glob.then_some(PatternPath {
        literal_prefix,
        glob_suffix,
    }))
}

fn split_file_url_pattern(value: &str) -> Result<Option<PatternPath>> {
    let (_, remainder) = value
        .split_once("://")
        .expect("validated file URLs contain a scheme");
    let encoded_path = if remainder.starts_with('/') {
        remainder
    } else {
        let (_, path) = remainder
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("file URL is not an absolute local path: '{value}'"))?;
        path
    };

    let mut literal_prefix = PathBuf::from("/");
    let mut glob_suffix = PathBuf::new();
    let mut found_glob = false;
    for encoded in encoded_path
        .split('/')
        .filter(|component| !component.is_empty())
    {
        let decoded = percent_decode_str(encoded)
            .decode_utf8()
            .with_context(|| format!("file URL path is not UTF-8: '{value}'"))?;
        if found_glob {
            if decoded == ".." {
                bail!(
                    "local glob pattern cannot contain '..' after its first glob component: '{value}'"
                );
            }
            glob_suffix.push(decoded.as_ref());
        } else if has_glob_syntax(encoded, true) {
            found_glob = true;
            glob_suffix.push(decoded.as_ref());
        } else {
            literal_prefix.push(decoded.as_ref());
        }
    }

    Ok(found_glob.then_some(PatternPath {
        literal_prefix,
        glob_suffix,
    }))
}

pub(crate) fn has_glob_syntax(value: &str, percent_encoded: bool) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if percent_encoded
            && bytes[index] == b'%'
            && bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
            && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit)
        {
            index += 3;
            continue;
        }
        if matches!(bytes[index], b'*' | b'?' | b'[') {
            return true;
        }
        index += 1;
    }
    false
}

fn lower_existing_ancestors(path: &std::path::Path, display: &str) -> Result<PathBuf> {
    for ancestor in path.ancestors() {
        match std::fs::canonicalize(ancestor) {
            Ok(mut canonical) => {
                let remainder = path
                    .strip_prefix(ancestor)
                    .expect("ancestor came from the path");
                if remainder
                    .components()
                    .any(|component| component == std::path::Component::ParentDir)
                {
                    bail!(
                        "local path cannot traverse '..' after a missing path component: '{display}'"
                    );
                }
                canonical.push(remainder);
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("could not resolve local path '{display}'"));
            }
        }
    }

    bail!("could not find an existing ancestor for local path '{display}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_relative_local_path() {
        let input = "data/input.parquet";
        let ParsedLocation::Local { display, path, .. } = ParsedLocation::parse(input).unwrap()
        else {
            panic!("expected local location");
        };

        let expected = std::path::absolute(input).unwrap();
        assert_eq!(
            (display, path),
            (
                input.to_string(),
                Path::from_absolute_path(expected).unwrap()
            )
        );
    }

    #[test]
    fn parses_absolute_local_path() {
        let input = std::env::temp_dir().join("silk-chiffon-location.parquet");
        let input = input.to_str().unwrap();
        let ParsedLocation::Local { display, path, .. } = ParsedLocation::parse(input).unwrap()
        else {
            panic!("expected local location");
        };

        assert_eq!(display, input);
        let expected = std::fs::canonicalize(std::path::Path::new(input).parent().unwrap())
            .unwrap()
            .join(std::path::Path::new(input).file_name().unwrap());
        assert_eq!(path, Path::from_absolute_path(expected).unwrap());
    }

    #[test]
    fn lowers_nonexistent_local_output_beneath_an_existing_ancestor() {
        let temp = tempfile::TempDir::new().unwrap();
        let output = temp.path().join("missing/output.parquet");
        let ParsedLocation::Local { path, .. } =
            ParsedLocation::parse(output.to_str().unwrap()).unwrap()
        else {
            panic!("expected local location");
        };
        let expected = std::fs::canonicalize(temp.path())
            .unwrap()
            .join("missing/output.parquet");

        assert_eq!(path, Path::from_absolute_path(expected).unwrap());
    }

    #[test]
    fn parses_file_url() {
        let ParsedLocation::Local { display, path, .. } =
            ParsedLocation::parse("file:///tmp/input%20file.parquet").unwrap()
        else {
            panic!("expected local location");
        };

        assert_eq!(display, "file:///tmp/input%20file.parquet");
        let expected = std::fs::canonicalize("/tmp")
            .unwrap()
            .join("input file.parquet");
        assert_eq!(path, Path::from_absolute_path(expected).unwrap());
    }

    #[test]
    fn parses_gcs_url_without_losing_key_characters() {
        let input = "gs://data-bucket/folder/a b#c?.parquet";
        let ParsedLocation::Gcs {
            display,
            bucket,
            path,
            ..
        } = ParsedLocation::parse(input).unwrap()
        else {
            panic!("expected GCS location");
        };

        assert_eq!(display, input);
        assert_eq!(bucket, "data-bucket");
        assert_eq!(path.as_ref(), "folder/a b#c?.parquet");
    }

    #[test]
    fn decodes_percent_escaped_gcs_key() {
        let ParsedLocation::Gcs { path, .. } =
            ParsedLocation::parse("gs://data-bucket/folder/a%20b%23c%3F.parquet").unwrap()
        else {
            panic!("expected GCS location");
        };

        assert_eq!(path.as_ref(), "folder/a b#c?.parquet");
    }

    #[test]
    fn rejects_gcs_url_without_bucket_or_key() {
        for input in ["gs:///key.parquet", "gs://data-bucket", "gs://data-bucket/"] {
            let error = ParsedLocation::parse(input).unwrap_err().to_string();
            assert!(
                error.contains("bucket") || error.contains("object key"),
                "{error}"
            );
        }
    }

    #[test]
    fn rejects_unsupported_schemes() {
        for input in [
            "gcs://bucket/key",
            "s3://bucket/key",
            "az://bucket/key",
            "http://example.com/key",
        ] {
            let error = ParsedLocation::parse(input).unwrap_err().to_string();
            assert!(error.contains("file") && error.contains("gs"), "{error}");
        }
    }
}
