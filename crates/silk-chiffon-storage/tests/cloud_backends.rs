#![cfg(any(feature = "gcs", feature = "s3"))]

use std::sync::Arc;

#[cfg(feature = "gcs")]
use clap::Args;
use clap::Command;
use object_store::{ObjectStore, memory::InMemory};
#[cfg(feature = "gcs")]
use silk_chiffon_storage::gcs;
#[cfg(feature = "s3")]
use silk_chiffon_storage::s3;
use silk_chiffon_storage::{
    LocationInput, StorageAccess, StorageBackend, StorageDirection, StorageError, StorageRegistry,
    StorageRegistryError,
};

#[cfg(feature = "gcs")]
#[test]
fn gcs_backend_has_the_expected_storage_contract() {
    let gcs = gcs::backend().unwrap();
    assert_eq!(gcs.name(), "gcs");
    assert_eq!(gcs.schemes(), ["gs"]);
    assert!(gcs.supports(StorageDirection::Input));
    assert!(gcs.supports(StorageDirection::Output));
    assert!(gcs.uses_shared_retries());
    assert!(!gcs.claims_bare_locations());
}

#[cfg(feature = "s3")]
#[test]
fn s3_backend_has_the_expected_storage_contract_without_an_s3a_alias() {
    let s3 = s3::backend().unwrap();
    assert_eq!(s3.name(), "s3");
    assert_eq!(s3.schemes(), ["s3"]);
    assert!(s3.supports(StorageDirection::Input));
    assert!(s3.supports(StorageDirection::Output));
    assert!(s3.uses_shared_retries());
    assert!(!s3.claims_bare_locations());
}

#[cfg(feature = "gcs")]
#[test]
fn gcs_cli_routes_non_secret_settings_and_rejects_secret_flags() {
    let registry = StorageRegistry::builder()
        .register(gcs::backend().unwrap())
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("gcs-test"));
    let help = command.clone().render_long_help().to_string();
    for option in [
        "--gcs-endpoint",
        "--gcs-anonymous",
        "--gcs-request-timeout",
        "--storage-max-retries",
    ] {
        assert!(help.contains(option), "missing {option} from {help}");
    }
    for secret in [
        "--gcs-credential",
        "--gcs-service-account",
        "--gcs-access-token",
        "--gcs-private-key",
    ] {
        assert!(
            !help.contains(secret),
            "secret-bearing option leaked: {secret}"
        );
    }

    let matches = command
        .try_get_matches_from([
            "gcs-test",
            "--gcs-endpoint",
            "http://127.0.0.1:9",
            "--gcs-anonymous",
            "--gcs-request-timeout",
            "25ms",
            "--storage-max-retries",
            "0",
        ])
        .unwrap();
    let storage = registry.create_session(&matches).unwrap();
    let handle = storage
        .input_handle(&LocationInput::parse("gs://bucket/path/file.arrow").unwrap())
        .unwrap();
    assert_eq!(handle.store_url().as_str(), "gs://bucket/");
    assert_eq!(handle.object_path().as_ref(), "path/file.arrow");
}

#[cfg(feature = "s3")]
#[test]
fn s3_cli_routes_region_endpoint_addressing_and_non_secret_settings() {
    let registry = StorageRegistry::builder()
        .register(s3::backend().unwrap())
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("s3-test"));
    let help = command.clone().render_long_help().to_string();
    for option in [
        "--s3-region",
        "--s3-endpoint",
        "--s3-addressing-style",
        "--s3-anonymous",
        "--s3-request-timeout",
        "--storage-max-retries",
    ] {
        assert!(help.contains(option), "missing {option} from {help}");
    }
    for secret in [
        "--s3-access-key",
        "--s3-secret-key",
        "--s3-session-token",
        "--s3-credential",
    ] {
        assert!(
            !help.contains(secret),
            "secret-bearing option leaked: {secret}"
        );
    }

    let matches = command
        .try_get_matches_from([
            "s3-test",
            "--s3-region",
            "test-region-1",
            "--s3-endpoint",
            "http://127.0.0.1:9",
            "--s3-addressing-style",
            "path",
            "--s3-anonymous",
            "--s3-request-timeout",
            "25ms",
            "--storage-max-retries",
            "0",
        ])
        .unwrap();
    let storage = registry.create_session(&matches).unwrap();
    let handle = storage
        .input_handle(&LocationInput::parse("s3://bucket/path/file.parquet").unwrap())
        .unwrap();
    assert_eq!(handle.store_url().as_str(), "s3://bucket/");
    assert_eq!(handle.object_path().as_ref(), "path/file.parquet");

    assert!(matches!(
        storage.input_handle(&LocationInput::parse("s3a://bucket/path").unwrap()),
        Err(StorageError::UnsupportedScheme(scheme)) if scheme == "s3a"
    ));

    assert!(
        registry
            .augment_args(Command::new("s3-test"))
            .try_get_matches_from(["s3-test", "--s3-region", ""])
            .is_err()
    );
}

#[cfg(any(feature = "gcs", feature = "s3"))]
#[test]
fn cloud_cli_rejects_unsafe_endpoints_and_zero_timeouts() {
    #[cfg(feature = "gcs")]
    let registry = StorageRegistry::builder()
        .register(gcs::backend().unwrap())
        .build()
        .unwrap();
    #[cfg(all(not(feature = "gcs"), feature = "s3"))]
    let registry = StorageRegistry::builder()
        .register(s3::backend().unwrap())
        .build()
        .unwrap();

    let command = registry.augment_args(Command::new("cloud-test"));
    #[cfg(feature = "gcs")]
    let endpoint_option = "--gcs-endpoint";
    #[cfg(all(not(feature = "gcs"), feature = "s3"))]
    let endpoint_option = "--s3-endpoint";
    #[cfg(feature = "gcs")]
    let timeout_option = "--gcs-request-timeout";
    #[cfg(all(not(feature = "gcs"), feature = "s3"))]
    let timeout_option = "--s3-request-timeout";

    for endpoint in [
        "ftp://example.com",
        "https://user:password@example.com",
        "https://example.com?token=visible",
        "https://example.com#fragment",
    ] {
        assert!(
            command
                .clone()
                .try_get_matches_from(["cloud-test", endpoint_option, endpoint])
                .is_err(),
            "accepted unsafe endpoint {endpoint}"
        );
    }
    assert!(
        command
            .try_get_matches_from(["cloud-test", timeout_option, "0s"])
            .is_err()
    );
}

#[cfg(feature = "gcs")]
#[test]
fn gcs_rejects_invalid_bucket_locations_before_storage_requests() {
    assert_invalid_cloud_locations(gcs::backend().unwrap(), "gs");
}

#[cfg(feature = "s3")]
#[test]
fn s3_rejects_invalid_bucket_locations_before_storage_requests() {
    assert_invalid_cloud_locations(s3::backend().unwrap(), "s3");
}

#[cfg(any(feature = "gcs", feature = "s3"))]
fn assert_invalid_cloud_locations(backend: silk_chiffon_storage::StorageBackend, scheme: &str) {
    let registry = StorageRegistry::builder()
        .register(backend)
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("location-test"));
    let matches = command.try_get_matches_from(["location-test"]).unwrap();
    let storage = registry.create_session(&matches).unwrap();

    for input in [
        format!("{scheme}:///path"),
        format!("{scheme}://bucket:9000/path"),
        format!("{scheme}://bucket/path?version=secret"),
    ] {
        assert!(matches!(
            storage.input_handle(&LocationInput::parse(&input).unwrap()),
            Err(StorageError::LocationValidation { .. })
        ));
    }

    let fragmented = format!("{scheme}://bucket/path#fragment");
    assert!(matches!(
        LocationInput::parse(&fragmented),
        Err(StorageError::FragmentNotSupported(input)) if input == fragmented
    ));
}

fn memory_store(
    _url: &url::Url,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(InMemory::new()))
}

fn conflicting_backend(scheme: &'static str) -> StorageBackend {
    StorageBackend::without_args()
        .name("conflict")
        .schemes([scheme])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(memory_store)
        .build()
        .unwrap()
}

#[cfg(feature = "gcs")]
#[test]
fn gcs_participates_in_scheme_collision_validation() {
    let result = StorageRegistry::builder()
        .register(gcs::backend().unwrap())
        .register(conflicting_backend("gs"))
        .build();
    assert!(matches!(
        result,
        Err(StorageRegistryError::DuplicateScheme { scheme: "gs", .. })
    ));
}

#[cfg(feature = "s3")]
#[test]
fn s3_participates_in_scheme_collision_validation() {
    let result = StorageRegistry::builder()
        .register(s3::backend().unwrap())
        .register(conflicting_backend("s3"))
        .build();
    assert!(matches!(
        result,
        Err(StorageRegistryError::DuplicateScheme { scheme: "s3", .. })
    ));
}

#[derive(Args)]
#[cfg(feature = "gcs")]
struct ConflictingGcsArgs {
    #[arg(id = "conflicting-gcs-endpoint", long = "gcs-endpoint")]
    endpoint: Option<String>,
}

#[cfg(feature = "gcs")]
fn conflicting_gcs_cli_backend() -> StorageBackend {
    StorageBackend::with_args::<ConflictingGcsArgs>()
        .name("conflicting-cli")
        .schemes(["other"])
        .access(StorageAccess::ReadWrite)
        .allow_any_location()
        .object_store_creator(|_, _, _| Ok(Arc::new(InMemory::new())))
        .build()
        .unwrap()
}

#[cfg(feature = "gcs")]
#[test]
fn gcs_participates_in_cli_collision_validation() {
    let result = StorageRegistry::builder()
        .register(gcs::backend().unwrap())
        .register(conflicting_gcs_cli_backend())
        .build();
    assert!(matches!(
        result,
        Err(StorageRegistryError::DuplicateCliArgument { argument, .. })
            if argument == "--gcs-endpoint"
    ));
}
