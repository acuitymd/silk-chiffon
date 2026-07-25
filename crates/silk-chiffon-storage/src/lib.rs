use std::{
    collections::BTreeMap,
    path::{Path as FilePath, PathBuf},
    sync::Arc,
};

#[cfg(feature = "local")]
use datafusion_execution::runtime_env::RuntimeEnv;
#[cfg(feature = "local")]
use object_store::local::LocalFileSystem;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
#[cfg(feature = "local")]
use std::{collections::HashMap, sync::Mutex};
use thiserror::Error;
use url::Url;

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
    #[error("storage support is disabled for scheme: {0}")]
    SchemeDisabled(String),
    #[error("local file URL must use the exact lowercase file:/// form: {0}")]
    NonCanonicalFileUrl(String),
    #[error("invalid storage URL {input}: {source}")]
    InvalidUrl {
        input: String,
        source: url::ParseError,
    },
    #[error("storage URLs cannot contain query strings: {0}")]
    QueryNotSupported(String),
    #[error("storage URLs cannot contain fragments: {0}")]
    FragmentNotSupported(String),
    #[error("invalid percent encoding in storage URL: {0}")]
    InvalidPercentEncoding(String),
    #[error("filesystem path cannot be represented as a local file URL: {0}")]
    InvalidFilePath(PathBuf),
    #[error("invalid object path: {0}")]
    InvalidObjectPath(#[from] object_store::path::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("output already exists: {0}")]
    OutputAlreadyExists(Url),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Location {
    url: Url,
}

impl Location {
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
                Some(scheme) => {
                    return Err(StorageError::UnsupportedScheme(scheme.to_ascii_lowercase()));
                }
                None => {}
            }

            let path = FilePath::new(input);
            let absolute = if path.is_absolute() {
                path.to_path_buf()
            } else {
                working_directory.join(path)
            };
            Url::from_file_path(&absolute).map_err(|()| StorageError::InvalidFilePath(absolute))?
        };

        ObjectPath::from_url_path(url.path())?;
        Ok(Self { url })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }
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
    if raw_path.contains('?') {
        return Err(StorageError::QueryNotSupported(input.to_owned()));
    }
    if raw_path.contains('#') {
        return Err(StorageError::FragmentNotSupported(input.to_owned()));
    }

    ObjectPath::from_url_path(raw_path)?;

    let url = Url::parse(input).map_err(|source| StorageError::InvalidUrl {
        input: input.to_owned(),
        source,
    })?;
    url.to_file_path()
        .map_err(|()| StorageError::InvalidFilePath(PathBuf::from(input)))?;

    Ok(url)
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StoreCacheKey {
    scheme: String,
    authority: String,
    configuration: BTreeMap<String, String>,
}

impl StoreCacheKey {
    #[cfg(feature = "local")]
    fn local() -> Self {
        Self {
            scheme: "file".to_owned(),
            authority: String::new(),
            configuration: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedLocation {
    pub url: Url,
    pub store: Arc<dyn ObjectStore>,
    pub path: ObjectPath,
    store_url: Url,
    cache_key: StoreCacheKey,
}

impl std::fmt::Debug for ResolvedLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedLocation")
            .field("url", &self.url)
            .field("store", &self.store)
            .field("path", &self.path)
            .field("store_url", &self.store_url)
            .field("cache_key", &self.cache_key)
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

    pub fn cache_key(&self) -> &StoreCacheKey {
        &self.cache_key
    }

    #[cfg(feature = "local")]
    pub fn register_with_datafusion(&self, runtime: &RuntimeEnv) {
        runtime.register_object_store(&self.store_url, Arc::clone(&self.store));
    }
}

#[derive(Clone, Default)]
pub struct StorageResolver {
    #[cfg(feature = "local")]
    stores: Arc<Mutex<HashMap<StoreCacheKey, Arc<dyn ObjectStore>>>>,
}

impl StorageResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn resolve(&self, location: &Location) -> Result<ResolvedLocation, StorageError> {
        #[cfg(not(feature = "local"))]
        {
            let _ = location;
            Err(StorageError::SchemeDisabled("file".to_owned()))
        }

        #[cfg(feature = "local")]
        {
            let path = ObjectPath::from_url_path(location.url.path())?;
            let cache_key = StoreCacheKey::local();
            let mut stores = self
                .stores
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let store = Arc::clone(
                stores
                    .entry(cache_key.clone())
                    .or_insert_with(|| Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>),
            );
            let mut store_url = location.url.clone();
            store_url.set_path("/");

            Ok(ResolvedLocation {
                url: location.url.clone(),
                store,
                path,
                store_url,
                cache_key,
            })
        }
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
