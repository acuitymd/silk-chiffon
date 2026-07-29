use std::path::Path;

#[cfg(feature = "local")]
use bytes::Bytes;
#[cfg(feature = "local")]
use futures::TryStreamExt;
#[cfg(feature = "local")]
use object_store::ObjectStoreExt;
use silk_chiffon_storage::{Location, StorageError, StorageResolver};
#[cfg(feature = "local")]
use silk_chiffon_storage::{preflight_output, validate_input};
#[cfg(feature = "local")]
use std::sync::Arc;
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
fn bare_paths_accept_characters_that_require_encoding_in_urls() -> Result<(), StorageError> {
    let working_directory = TempDir::new().unwrap();
    for input in [
        "data set.parquet",
        "snapshot?#100%.parquet",
        "literal%20name.parquet",
        "résumé.parquet",
    ] {
        let location = location(input, working_directory.path())?;
        let expected = working_directory.path().join(input);
        assert_eq!(location.url().to_file_path().unwrap(), expected);
        #[cfg(feature = "local")]
        {
            let resolved = StorageResolver::local().unwrap().resolve_input(&location)?;
            assert_eq!(resolved.local_path()?, expected);
            assert_eq!(
                resolved.path,
                object_store::path::Path::from_absolute_path(&expected).unwrap()
            );
        }
    }

    Ok(())
}

#[test]
#[cfg(feature = "local")]
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
        (
            "file:///tmp/r%C3%A9sum%C3%A9.parquet",
            "/tmp/résumé.parquet",
            "tmp/résumé.parquet",
        ),
    ] {
        let location = location(input, Path::new("/work"))?;
        let resolved = StorageResolver::local()?.resolve_input(&location)?;

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
fn url_paths_require_percent_encoding() {
    for input in [
        "file:///tmp/data set.parquet",
        "file:///tmp/résumé.parquet",
        "s3://bucket/data set.parquet",
        "s3://bucket/résumé.parquet",
    ] {
        assert!(matches!(
            Location::parse(input, Path::new("/work")),
            Err(StorageError::UnencodedUrlPath(rejected)) if rejected == input
        ));
    }
}

#[test]
fn canonical_provider_urls_parse_before_scheme_resolution() {
    let location = Location::parse("s3://bucket/object", Path::new("/work")).unwrap();

    assert_eq!(location.url().as_str(), "s3://bucket/object");
    assert!(matches!(
        StorageResolver::local()
            .unwrap()
            .resolve_input(&location),
        Err(StorageError::UnsupportedScheme(scheme)) if scheme == "s3"
    ));
}

#[test]
#[cfg(feature = "local")]
fn object_store_path_validation_happens_during_resolution() {
    let location = Location::parse("bad\0path", Path::new("/work")).unwrap();

    assert!(matches!(
        StorageResolver::local().unwrap().resolve_input(&location),
        Err(StorageError::ProviderResolution {
            provider: "local",
            direction: silk_chiffon_storage::StorageDirection::Input,
            source,
        }) if source.downcast_ref::<object_store::path::Error>().is_some()
    ));
}

#[test]
fn noncanonical_storage_urls_are_rejected() {
    for input in ["s3:/bucket/object", "S3://bucket/object"] {
        assert!(matches!(
            Location::parse(input, Path::new("/work")),
            Err(StorageError::NonCanonicalStorageUrl { scheme, input: rejected })
                if scheme == "s3" && rejected == input
        ));
    }
}

#[test]
fn storage_urls_preserve_queries() {
    for (input, path, query) in [
        (
            "s3://bucket/object?version=1&mode=active",
            "/object",
            "version=1&mode=active",
        ),
        ("file:///tmp/object?version=1", "/tmp/object", "version=1"),
    ] {
        let location = Location::parse(input, Path::new("/work")).unwrap();

        assert_eq!(location.url().as_str(), input);
        assert_eq!(location.url().path(), path);
        assert_eq!(location.url().query(), Some(query));
    }

    #[cfg(feature = "local")]
    {
        let file = Location::parse("file:///tmp/object?version=1", Path::new("/work")).unwrap();
        let resolved = StorageResolver::local()
            .unwrap()
            .resolve_input(&file)
            .unwrap();
        assert_eq!(resolved.url.query(), Some("version=1"));
        assert_eq!(resolved.path.as_ref(), "tmp/object");
        assert_eq!(resolved.local_path().unwrap(), Path::new("/tmp/object"));
        assert_eq!(resolved.store_url().as_str(), "file:///");
    }
}

#[test]
fn storage_urls_reject_fragments_user_information_and_noncanonical_paths() {
    for input in [
        "s3://bucket/object#fragment",
        "s3://user:password@bucket/object",
        "s3://bucket/a/../object",
        "s3://bucket/a/./object",
        "s3://bucket/a/%2E%2E/object",
        "s3://bucket/%ZZ",
    ] {
        assert!(
            Location::parse(input, Path::new("/work")).is_err(),
            "{input:?} should be rejected"
        );
    }
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
fn strict_parser_rejects_malformed_or_ambiguous_locations() {
    let working_directory = TempDir::new().unwrap();

    for invalid in [
        "",
        "relative:object",
        "file:///tmp/object#fragment",
        "file:///tmp/../object",
        "file:///tmp/./object",
        "file:///tmp/%2E%2E/object",
        "file:///tmp/%ZZ",
    ] {
        assert!(
            Location::parse(invalid, working_directory.path()).is_err(),
            "{invalid:?} should be rejected"
        );
    }
}

#[test]
#[cfg(feature = "local")]
fn equivalent_locations_share_the_cached_store() {
    let working_directory = TempDir::new().unwrap();
    let relative = location("data.parquet", working_directory.path()).unwrap();
    let file_url = location(relative.url().as_str(), working_directory.path()).unwrap();
    let resolver = StorageResolver::local().unwrap();

    let first = resolver.resolve_input(&relative).unwrap();
    let second = resolver.resolve_input(&file_url).unwrap();

    assert!(Arc::ptr_eq(&first.store, &second.store));
}

#[test]
#[cfg(feature = "local")]
fn resolution_preserves_the_upstream_object_path() {
    let working_directory = TempDir::new().unwrap();
    let location = location("nested/data%20set.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::local()
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
#[cfg(feature = "local")]
async fn absent_object_resolution_is_separate_from_input_validation() {
    let working_directory = TempDir::new().unwrap();
    let location = location("absent.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::local()
        .unwrap()
        .resolve_input(&location)
        .unwrap();

    assert!(validate_input(&resolved).await.is_err());
}

#[tokio::test]
#[cfg(feature = "local")]
async fn absent_output_passes_preflight() {
    let working_directory = TempDir::new().unwrap();
    let location = location("absent.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::local()
        .unwrap()
        .resolve_output(&location)
        .unwrap();

    preflight_output(&resolved, false).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "local")]
async fn existing_output_requires_overwrite() {
    let working_directory = TempDir::new().unwrap();
    let location = location("existing.parquet", working_directory.path()).unwrap();
    let resolved = StorageResolver::local()
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
#[cfg(feature = "local")]
async fn local_store_supports_object_operations() {
    let working_directory = TempDir::new().unwrap();
    let location = location("nested/data.bin", working_directory.path()).unwrap();
    let resolved = StorageResolver::local()
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
