use std::{path::Path, sync::Arc};

use bytes::Bytes;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use silk_chiffon_storage::{
    Location, StorageError, StorageResolver, preflight_output, validate_input,
};
use tempfile::TempDir;

fn location(input: &str, working_directory: &Path) -> Result<Location, StorageError> {
    Location::parse(input, working_directory)
}

#[test]
fn relative_path_becomes_an_absolute_file_url() -> Result<(), StorageError> {
    let working_directory = TempDir::new().unwrap();
    let location = location("nested/data.parquet", working_directory.path())?;
    let expected = working_directory.path().join("nested/data.parquet");

    assert_eq!(location.url().scheme(), "file");
    assert_eq!(location.url().to_file_path().unwrap(), expected);
    Ok(())
}

#[test]
fn absolute_path_becomes_a_file_url_without_requiring_the_path_to_exist() -> Result<(), StorageError>
{
    let working_directory = TempDir::new().unwrap();
    let absent = working_directory.path().join("absent/data.parquet");
    let location = location(absent.to_str().unwrap(), working_directory.path())?;

    assert_eq!(location.url().to_file_path().unwrap(), absent);
    Ok(())
}

#[test]
fn canonical_file_urls_map_absolute_paths_to_store_keys() -> Result<(), Box<dyn std::error::Error>>
{
    for (input, filesystem_path, object_path) in [
        (
            "file:///tmp/data.parquet",
            "/tmp/data.parquet",
            "tmp/data.parquet",
        ),
        (
            "file:///tmp/data%20set.parquet",
            "/tmp/data set.parquet",
            "tmp/data set.parquet",
        ),
    ] {
        let location = location(input, Path::new("/work"))?;
        let resolved = StorageResolver::new()?.resolve_input(&location)?;

        assert_eq!(location.url().as_str(), input);
        assert_eq!(
            location.url().to_file_path().unwrap(),
            Path::new(filesystem_path)
        );
        assert_eq!(resolved.url, *location.url());
        assert_eq!(resolved.local_path()?, Path::new(filesystem_path));
        assert_eq!(resolved.store_url().as_str(), "file:///");
        assert_eq!(resolved.path.as_ref(), object_path);
    }

    Ok(())
}

#[test]
fn noncanonical_local_file_urls_are_rejected() {
    let working_directory = TempDir::new().unwrap();

    for invalid in [
        "file:relative",
        "file:/tmp/object",
        "file://localhost/path",
        "file://server/path",
        "file://[",
        "FILE:///tmp/object",
        "file:////tmp/object",
    ] {
        assert!(
            matches!(
                Location::parse(invalid, working_directory.path()),
                Err(StorageError::NonCanonicalFileUrl(_))
            ),
            "{invalid:?} should be rejected as a noncanonical local file URL"
        );
    }
}

#[test]
fn strict_parser_rejects_unsupported_or_ambiguous_locations() {
    let working_directory = TempDir::new().unwrap();

    for invalid in [
        "",
        "s3://bucket/object",
        "relative:object",
        "file:///tmp/object?version=1",
        "file:///tmp/object#fragment",
        "file:///tmp/../object",
        "file:///tmp/./object",
        "file:///tmp/%2E%2E/object",
        "file:///tmp/%ZZ",
        "bad\0path",
    ] {
        assert!(
            Location::parse(invalid, working_directory.path()).is_err(),
            "{invalid:?} should be rejected"
        );
    }
}

#[test]
fn equivalent_locations_share_the_cached_store() {
    let working_directory = TempDir::new().unwrap();
    let relative = location("data.parquet", working_directory.path()).unwrap();
    let file_url = location(relative.url().as_str(), working_directory.path()).unwrap();
    let resolver = StorageResolver::new().unwrap();

    let first = resolver.resolve_input(&relative).unwrap();
    let second = resolver.resolve_input(&file_url).unwrap();

    assert!(Arc::ptr_eq(&first.store, &second.store));
}

#[test]
fn resolution_preserves_the_upstream_object_path() {
    let working_directory = TempDir::new().unwrap();
    let location = location("nested/data%20set.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::new()
        .unwrap()
        .resolve_input(&location)
        .unwrap();

    assert_eq!(
        resolved.path,
        object_store::path::Path::from_absolute_path(
            working_directory.path().join("nested/data%20set.parquet")
        )
        .unwrap()
    );
}

#[tokio::test]
async fn absent_object_resolution_is_separate_from_input_validation() {
    let working_directory = TempDir::new().unwrap();
    let location = location("absent.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::new()
        .unwrap()
        .resolve_input(&location)
        .unwrap();

    assert!(validate_input(&resolved).await.is_err());
}

#[tokio::test]
async fn absent_output_passes_preflight() {
    let working_directory = TempDir::new().unwrap();
    let location = location("absent.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::new()
        .unwrap()
        .resolve_output(&location)
        .unwrap();

    preflight_output(&resolved, false).await.unwrap();
}

#[tokio::test]
async fn existing_output_requires_overwrite() {
    let working_directory = TempDir::new().unwrap();
    let location = location("existing.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::new()
        .unwrap()
        .resolve_output(&location)
        .unwrap();
    resolved
        .store
        .put(&resolved.path, Bytes::from_static(b"existing").into())
        .await
        .unwrap();

    assert!(preflight_output(&resolved, false).await.is_err());
    preflight_output(&resolved, true).await.unwrap();
}

#[tokio::test]
async fn local_store_supports_object_operations() {
    let working_directory = TempDir::new().unwrap();
    let location = location("nested/data.bin", working_directory.path()).unwrap();
    let resolved = StorageResolver::new()
        .unwrap()
        .resolve_output(&location)
        .unwrap();

    resolved
        .store
        .put(&resolved.path, Bytes::from_static(b"abcdef").into())
        .await
        .unwrap();
    assert_eq!(
        resolved
            .store
            .get(&resolved.path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::from_static(b"abcdef")
    );
    assert_eq!(
        resolved
            .store
            .get_range(&resolved.path, 1..4)
            .await
            .unwrap(),
        Bytes::from_static(b"bcd")
    );

    let listed = resolved
        .store
        .list(resolved.path.parent().as_ref())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].location, resolved.path);

    resolved.store.delete(&resolved.path).await.unwrap();
    assert!(resolved.store.head(&resolved.path).await.is_err());
}

#[tokio::test]
async fn datafusion_uses_the_same_store_for_local_scans() {
    let working_directory = TempDir::new().unwrap();
    let location = location("data.csv", working_directory.path()).unwrap();
    let resolved = StorageResolver::new()
        .unwrap()
        .resolve_input(&location)
        .unwrap();
    resolved
        .store
        .put(
            &resolved.path,
            Bytes::from_static(b"id,name\n1,alice\n2,bob\n").into(),
        )
        .await
        .unwrap();

    let context = SessionContext::new();
    resolved.register_with_datafusion(context.runtime_env().as_ref());

    let registered = context
        .runtime_env()
        .object_store_registry
        .get_store(&resolved.url)
        .unwrap();
    assert!(Arc::ptr_eq(&resolved.store, &registered));

    let batches = context
        .read_csv(resolved.url.as_str(), CsvReadOptions::new())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
}
