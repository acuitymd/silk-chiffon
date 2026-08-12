#[cfg(feature = "local")]
use std::path::Path;

#[cfg(feature = "local-bare-paths")]
use bytes::Bytes;
#[cfg(feature = "local-bare-paths")]
use futures::TryStreamExt;
#[cfg(feature = "local-bare-paths")]
use object_store::ObjectStoreExt;
use silk_chiffon_storage::{Location, LocationInput, StorageError};
#[cfg(feature = "local-bare-paths")]
use silk_chiffon_storage::{LocationPattern, ensure_output_absent};
#[cfg(feature = "local-bare-paths")]
use std::sync::Arc;
#[cfg(feature = "local-bare-paths")]
use tempfile::TempDir;

fn location(input: &str) -> Result<LocationInput, StorageError> {
    LocationInput::parse(input)
}

#[test]
fn schemeless_input_is_preserved_without_filesystem_interpretation() -> Result<(), StorageError> {
    for input in [
        "nested/data.parquet",
        "/absolute/data.parquet",
        "snapshot?#100%.parquet",
        "résumé.parquet",
    ] {
        assert_eq!(location(input)?, LocationInput::Bare(input.to_owned()));
    }
    Ok(())
}

#[test]
fn url_parser_requires_an_explicit_scheme() {
    assert!(matches!(
        Location::parse_url("data/input.parquet"),
        Err(StorageError::UrlSchemeRequired(input)) if input == "data/input.parquet"
    ));
}

#[test]
#[cfg(feature = "local-bare-paths")]
fn local_mapper_interprets_bare_locations_as_filesystem_paths() -> Result<(), StorageError> {
    let working_directory = TempDir::new().unwrap();
    for name in [
        "data set.parquet",
        "snapshot?#100%.parquet",
        "literal%20name.parquet",
        "résumé.parquet",
    ] {
        let expected = working_directory.path().join(name);
        let input = location(expected.to_str().unwrap())?;
        let handle = silk_chiffon_storage::local::session()
            .unwrap()
            .input_handle(&input)?;
        assert_eq!(handle.local_path()?, expected);
        assert_eq!(
            handle.object_path(),
            &object_store::path::Path::from_absolute_path(&expected).unwrap()
        );
    }

    let relative = location("relative/data.parquet")?;
    let handle = silk_chiffon_storage::local::session()
        .unwrap()
        .input_handle(&relative)?;
    assert_eq!(
        handle.local_path()?,
        std::env::current_dir()
            .unwrap()
            .join("relative/data.parquet")
    );

    Ok(())
}

#[tokio::test]
#[cfg(feature = "local-bare-paths")]
async fn local_mapper_expands_bare_path_patterns() {
    let directory = TempDir::new().unwrap();
    std::fs::write(directory.path().join("one.arrow"), b"one").unwrap();
    std::fs::write(directory.path().join("two.parquet"), b"two").unwrap();

    let source = directory
        .path()
        .join("*.arrow")
        .to_string_lossy()
        .into_owned();
    let pattern = LocationPattern::parse(&source).unwrap();
    let matches = silk_chiffon_storage::local::session()
        .unwrap()
        .expand_input_pattern(&pattern)
        .await
        .unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(
        matches[0].handle().local_path().unwrap(),
        directory.path().join("one.arrow")
    );
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
        let location = Location::parse_url(input)?;
        let handle =
            silk_chiffon_storage::local::session()?.input_handle(&location.clone().into())?;

        assert_eq!(location.url().as_str(), input);
        assert_eq!(
            location.url().to_file_path().unwrap(),
            Path::new(filesystem_path)
        );
        assert_eq!(handle.url(), location.url());
        assert_eq!(handle.local_path()?, Path::new(filesystem_path));
        assert_eq!(handle.store_url().as_str(), "file:///");
        assert_eq!(handle.object_path().as_ref(), object_path);
    }

    Ok(())
}

#[test]
fn noncanonical_url_paths_report_their_source() {
    for input in [
        "file:///tmp/data set.parquet",
        "file:///tmp/résumé.parquet",
        "file:///tmp/../object",
        "file:///tmp/./object",
        "file:///tmp/%2E%2E/object",
        "s3://bucket/data set.parquet",
        "s3://bucket/résumé.parquet",
        "s3://bucket/a/../object",
        "s3://bucket/a/./object",
        "s3://bucket/a/%2E%2E/object",
    ] {
        assert!(matches!(
            LocationInput::parse(input),
            Err(StorageError::NonCanonicalUrlPath(rejected)) if rejected == input
        ));
    }
}

#[test]
fn canonical_storage_urls_parse_before_backend_selection() {
    let input = LocationInput::parse("s3://bucket/object").unwrap();
    let LocationInput::Url(location) = &input else {
        panic!("an explicit URL should not parse as a bare location");
    };

    assert_eq!(location.url().as_str(), "s3://bucket/object");
    #[cfg(feature = "local")]
    assert!(
        matches!(silk_chiffon_storage::local::session().unwrap().input_handle(&input), Err(
        StorageError::UnsupportedScheme(scheme)
    ) if scheme == "s3")
    );
}

#[test]
#[cfg(feature = "local-bare-paths")]
fn object_path_validation_happens_during_handle_creation() {
    let location = LocationInput::parse("bad\0path").unwrap();

    assert!(matches!(
        silk_chiffon_storage::local::session().unwrap().input_handle(&location),
        Err(StorageError::InvalidObjectPath {
            location: _,
            source,
        }) if matches!(*source, object_store::path::Error::BadSegment { .. })
    ));
}

#[test]
fn noncanonical_storage_urls_are_rejected() {
    for input in ["s3:/bucket/object", "S3://bucket/object"] {
        assert!(matches!(
            LocationInput::parse(input),
            Err(StorageError::NonCanonicalStorageUrl { scheme, input: rejected })
                if scheme == "s3" && rejected == input
        ));
    }
}

#[test]
fn storage_urls_reject_authority_and_query_normalization() {
    for input in [
        "https://EXAMPLE.COM:443",
        "https://example.com/object?query=has a space",
    ] {
        assert!(matches!(
            LocationInput::parse(input),
            Err(StorageError::NonCanonicalStorageUrl { input: rejected, .. })
                if rejected == input
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
        let location = Location::parse_url(input).unwrap();

        assert_eq!(location.url().as_str(), input);
        assert_eq!(location.url().path(), path);
        assert_eq!(location.url().query(), Some(query));
    }

    #[cfg(feature = "local")]
    {
        let file = LocationInput::parse("file:///tmp/object?version=1").unwrap();
        let handle = silk_chiffon_storage::local::session()
            .unwrap()
            .input_handle(&file)
            .unwrap();
        assert_eq!(handle.url().query(), Some("version=1"));
        assert_eq!(handle.object_path().as_ref(), "tmp/object");
        assert_eq!(handle.local_path().unwrap(), Path::new("/tmp/object"));
        assert_eq!(handle.store_url().as_str(), "file:///");
    }
}

#[test]
fn storage_urls_reject_fragments_user_information_and_invalid_percent_encoding() {
    for input in [
        "s3://bucket/object#fragment",
        "s3://user:password@bucket/object",
        "https://:@example.com/object",
        "s3://bucket/%ZZ",
    ] {
        assert!(
            LocationInput::parse(input).is_err(),
            "{input:?} should be rejected"
        );
    }
}

#[test]
fn noncanonical_local_file_urls_are_rejected() {
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
                LocationInput::parse(invalid),
                Err(StorageError::NonCanonicalFileUrl(_))
            ),
            "{invalid:?} should be rejected as a noncanonical local file URL"
        );
    }
}

#[test]
fn strict_parser_rejects_malformed_or_ambiguous_locations() {
    for invalid in [
        "",
        "relative:object",
        "file:///tmp/object#fragment",
        "file:///tmp/%ZZ",
    ] {
        assert!(
            LocationInput::parse(invalid).is_err(),
            "{invalid:?} should be rejected"
        );
    }
}

#[test]
#[cfg(feature = "local-bare-paths")]
fn equivalent_locations_share_the_cached_store() {
    let working_directory = TempDir::new().unwrap();
    let path = working_directory.path().join("data.parquet");
    let bare = location(path.to_str().unwrap()).unwrap();
    let storage = silk_chiffon_storage::local::session().unwrap();

    let first = storage.input_handle(&bare).unwrap();
    let file_url = location(first.url().as_str()).unwrap();
    let second = storage.input_handle(&file_url).unwrap();

    assert!(Arc::ptr_eq(&first.object_store(), &second.object_store(),));
}

#[test]
#[cfg(feature = "local-bare-paths")]
fn storage_handle_preserves_the_upstream_object_path() {
    let working_directory = TempDir::new().unwrap();
    let path = working_directory.path().join("nested/data%20set.parquet");
    let location = location(path.to_str().unwrap()).unwrap();
    let handle = silk_chiffon_storage::local::session()
        .unwrap()
        .input_handle(&location)
        .unwrap();

    assert_eq!(
        handle.object_path(),
        &object_store::path::Path::from_absolute_path(
            working_directory.path().join("nested/data%20set.parquet")
        )
        .unwrap()
    );
}

#[tokio::test]
#[cfg(feature = "local-bare-paths")]
async fn absent_object_handle_creation_is_separate_from_input_lookup() {
    let working_directory = TempDir::new().unwrap();
    let path = working_directory.path().join("absent.parquet");
    let location = location(path.to_str().unwrap()).unwrap();
    let storage = silk_chiffon_storage::local::session().unwrap();
    let _handle = storage.input_handle(&location).unwrap();

    assert!(storage.lookup_input(&location).await.is_err());
}

#[tokio::test]
#[cfg(feature = "local-bare-paths")]
async fn absent_output_is_allowed() {
    let working_directory = TempDir::new().unwrap();
    let path = working_directory.path().join("absent.parquet");
    let location = location(path.to_str().unwrap()).unwrap();
    let handle = silk_chiffon_storage::local::session()
        .unwrap()
        .output_handle(&location)
        .unwrap();

    ensure_output_absent(&handle).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "local-bare-paths")]
async fn existing_output_is_rejected() {
    let working_directory = TempDir::new().unwrap();
    let path = working_directory.path().join("existing.parquet");
    let location = location(path.to_str().unwrap()).unwrap();
    let handle = silk_chiffon_storage::local::session()
        .unwrap()
        .output_handle(&location)
        .unwrap();
    handle
        .object_store()
        .put(handle.object_path(), Bytes::from_static(b"existing").into())
        .await
        .unwrap();

    assert!(ensure_output_absent(&handle).await.is_err());
}

#[tokio::test]
#[cfg(feature = "local-bare-paths")]
async fn local_store_supports_object_operations() {
    let working_directory = TempDir::new().unwrap();
    let path = working_directory.path().join("nested/data.bin");
    let location = location(path.to_str().unwrap()).unwrap();
    let handle = silk_chiffon_storage::local::session()
        .unwrap()
        .output_handle(&location)
        .unwrap();
    let object_store = handle.object_store();

    object_store
        .put(handle.object_path(), Bytes::from_static(b"abcdef").into())
        .await
        .unwrap();
    assert_eq!(
        object_store
            .get(handle.object_path())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::from_static(b"abcdef")
    );
    assert_eq!(
        object_store
            .get_range(handle.object_path(), 1..4)
            .await
            .unwrap(),
        Bytes::from_static(b"bcd")
    );

    let listed = object_store
        .list(handle.object_path().parent().as_ref())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(&listed[0].location, handle.object_path());

    object_store.delete(handle.object_path()).await.unwrap();
    assert!(object_store.head(handle.object_path()).await.is_err());
}
