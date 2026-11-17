//! Google Cloud Storage backend for canonical `gs://` locations.

use std::{sync::Arc, time::Duration};

use clap::Args;
use object_store::{
    ClientConfigKey, ObjectStore, StaticCredentialProvider,
    gcp::{GcpCredential, GoogleCloudStorageBuilder},
};
use url::Url;

use crate::{
    RetryConfig, StorageAccess, StorageBackend, StorageBackendBuildError,
    cloud::{endpoint_string, parse_endpoint, parse_positive_duration, validate_bucket_location},
};

/// Non-secret Google Cloud Storage settings for one command invocation.
#[derive(Args, Clone, Debug, Default)]
#[group(id = "gcs-storage-args")]
struct GcsArgs {
    /// Override the Google Cloud Storage API endpoint.
    #[arg(
        id = "gcs-storage-endpoint",
        long = "gcs-endpoint",
        value_name = "URL",
        value_parser = parse_endpoint
    )]
    endpoint: Option<Url>,
    /// Send unsigned requests without discovering credentials.
    #[arg(id = "gcs-storage-anonymous", long = "gcs-anonymous")]
    anonymous: bool,
    /// Limit each Google Cloud Storage HTTP request.
    #[arg(
        id = "gcs-storage-request-timeout",
        long = "gcs-request-timeout",
        value_name = "DURATION",
        value_parser = parse_positive_duration
    )]
    request_timeout: Option<Duration>,
}

/// Builds the feature-selected Google Cloud Storage backend.
///
/// Credentials remain in the upstream Application Default Credentials and
/// environment discovery chain. The command contributes only non-secret
/// endpoint, anonymous-access, and request-timeout settings.
///
/// # Errors
///
/// Returns [`StorageBackendBuildError`] if the built-in definition violates
/// storage-backend invariants.
pub fn backend() -> Result<StorageBackend, StorageBackendBuildError> {
    StorageBackend::with_args::<GcsArgs>()
        .name("gcs")
        .schemes(["gs"])
        .access(StorageAccess::ReadWrite)
        .location_validator(validate_location)
        .object_store_creator(create_object_store)
        .shared_retries()
        .build()
}

fn validate_location(location: &crate::Location, _settings: &GcsArgs) -> anyhow::Result<()> {
    validate_bucket_location(location)
}

fn create_object_store(
    store_url: &Url,
    settings: &GcsArgs,
    retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let bucket = store_url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Google Cloud Storage URL requires a bucket"))?;
    let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);

    if let Some(endpoint) = &settings.endpoint {
        builder = builder.with_base_url(&endpoint_string(endpoint));
    }
    if settings.anonymous {
        builder = builder.with_skip_signature(true).with_credentials(Arc::new(
            StaticCredentialProvider::new(GcpCredential {
                bearer: String::new(),
            }),
        ));
    }
    if let Some(timeout) = settings.request_timeout {
        builder = builder.with_config(
            object_store::gcp::GoogleConfigKey::Client(ClientConfigKey::Timeout),
            humantime::format_duration(timeout).to_string(),
        );
    }
    if let Some(retry) = retry {
        builder = builder.with_retry(retry.clone());
    }

    Ok(Arc::new(builder.build()?))
}
