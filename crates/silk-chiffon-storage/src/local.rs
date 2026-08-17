//! Built-in storage backend for canonical `file:///` locations.
//!
//! The `local` Cargo feature exposes [`backend`] for explicit `file:` URLs. The separate
//! `local-bare-paths` feature also makes this backend interpret schemeless input as a filesystem
//! path.

#[cfg(feature = "local")]
use std::sync::Arc;

#[cfg(feature = "local")]
use object_store::{ObjectStore, local::LocalFileSystem};

#[cfg(feature = "local-bare-paths")]
use crate::{Location, LocationPattern};
#[cfg(feature = "local")]
use crate::{
    OutputPreparation, OutputTarget, StorageAccess, StorageBackend, StorageBackendBuildError,
};

/// Builds the built-in local backend definition for canonical `file:///` locations.
///
/// With `local-bare-paths`, the same definition also claims schemeless input and maps relative
/// paths against the process working directory.
///
/// # Errors
///
/// Returns [`StorageBackendBuildError`] if the built-in definition violates backend invariants.
#[cfg(feature = "local")]
pub fn backend() -> Result<StorageBackend, StorageBackendBuildError> {
    let builder = StorageBackend::without_args()
        .name("local")
        .schemes(["file"])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(create_object_store)
        .prepare_output_target(prepare_output_target);

    #[cfg(feature = "local-bare-paths")]
    let builder = builder
        .bare_location_mapper(map_bare_location)
        .bare_pattern_mapper(map_bare_pattern);

    builder.build()
}

#[cfg(feature = "local")]
fn create_object_store(
    _store_url: &url::Url,
    _settings: &(),
    _retry: Option<&crate::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new()))
}

#[cfg(feature = "local")]
fn prepare_output_target<'a>(
    target: &'a OutputTarget,
    preparation: &'a OutputPreparation,
    _settings: &'a (),
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let path = target.local_path()?;
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        if preparation.create_parent_directories() {
            tokio::fs::create_dir_all(parent).await?;
        } else {
            let metadata = tokio::fs::metadata(parent).await?;
            anyhow::ensure!(
                metadata.is_dir(),
                "output parent is not a directory: {}",
                parent.display()
            );
        }
        Ok(())
    })
}

#[cfg(feature = "local-bare-paths")]
fn map_bare_location(input: &str, _settings: &()) -> anyhow::Result<Location> {
    let path = std::path::Path::new(input);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(Location::from_file_path(absolute)?)
}

#[cfg(feature = "local-bare-paths")]
fn map_bare_pattern(input: &str, _settings: &()) -> anyhow::Result<LocationPattern> {
    map_bare_pattern_from(input, &std::env::current_dir()?)
}

#[cfg(feature = "local-bare-paths")]
fn map_bare_pattern_from(
    input: &str,
    working_directory: &std::path::Path,
) -> anyhow::Result<LocationPattern> {
    let pattern = std::path::Path::new(input);
    let (literal_base, relative_pattern) = if pattern.is_absolute() {
        let root = std::path::Path::new("/");
        (root, pattern.strip_prefix(root)?)
    } else {
        (working_directory, pattern)
    };
    Ok(LocationPattern::from_file_path_pattern(
        literal_base,
        relative_pattern,
        input,
    )?)
}

#[cfg(all(test, feature = "local-bare-paths"))]
mod tests {
    fn local_storage_session() -> crate::StorageSession {
        use clap::Command;

        let registry = crate::StorageRegistry::builder()
            .register(super::backend().unwrap())
            .build()
            .unwrap();
        let command = registry.augment_args(Command::new("storage-test"));
        let matches = command.try_get_matches_from(["storage-test"]).unwrap();
        registry.create_session(&matches).unwrap()
    }

    #[tokio::test]
    async fn bare_patterns_treat_the_working_directory_as_literal() {
        let temporary = tempfile::tempdir().unwrap();
        let decoy_directory = temporary.path().join("la");
        std::fs::create_dir(&decoy_directory).unwrap();
        std::fs::write(decoy_directory.join("decoy.arrow"), b"decoy").unwrap();

        for directory_name in ["[literal]*?", "[unterminated", "**", "%2A"] {
            let working_directory = temporary.path().join(directory_name);
            std::fs::create_dir(&working_directory).unwrap();
            let input = working_directory.join("one.arrow");
            std::fs::write(&input, b"test").unwrap();

            let pattern = super::map_bare_pattern_from("*.arrow", &working_directory).unwrap();
            let matches = local_storage_session()
                .expand_input_pattern(&pattern)
                .await
                .unwrap();

            assert_eq!(matches.len(), 1, "working directory {directory_name:?}");
            assert_eq!(matches[0].input_handle().local_path().unwrap(), input);
        }
    }

    #[tokio::test]
    async fn bare_patterns_resolve_literal_parent_segments_before_globs() {
        let temporary = tempfile::tempdir().unwrap();
        let working_directory = temporary.path().join("work");
        let input_directory = temporary.path().join("data");
        std::fs::create_dir(&working_directory).unwrap();
        std::fs::create_dir(&input_directory).unwrap();
        let input = input_directory.join("one.arrow");
        std::fs::write(&input, b"test").unwrap();

        let pattern = super::map_bare_pattern_from("../data/*.arrow", &working_directory).unwrap();
        let matches = local_storage_session()
            .expand_input_pattern(&pattern)
            .await
            .unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].input_handle().local_path().unwrap(), input);
    }
}
