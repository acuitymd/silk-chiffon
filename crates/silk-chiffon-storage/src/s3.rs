//! Amazon S3 backend for canonical `s3://` locations.

use std::{sync::Arc, time::Duration};

use clap::{Args, ValueEnum};
use object_store::{ClientConfigKey, ObjectStore, aws::AmazonS3Builder};
use url::Url;

use crate::{
    RetryConfig, StorageAccess, StorageBackend, StorageBackendBuildError,
    cloud::{endpoint_string, parse_endpoint, parse_positive_duration, validate_bucket_location},
};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum AddressingStyle {
    Path,
    Virtual,
}

/// Non-secret S3 settings for one command invocation.
#[derive(Args, Clone, Debug, Default)]
#[group(id = "s3-storage-args")]
struct S3Args {
    /// Override the region discovered from the AWS environment.
    #[arg(
        id = "s3-storage-region",
        long = "s3-region",
        value_name = "REGION",
        value_parser = clap::builder::NonEmptyStringValueParser::new()
    )]
    region: Option<String>,
    /// Override the S3 API endpoint for an S3-compatible service.
    #[arg(
        id = "s3-storage-endpoint",
        long = "s3-endpoint",
        value_name = "URL",
        value_parser = parse_endpoint
    )]
    endpoint: Option<Url>,
    /// Select path-style or virtual-hosted-style S3 requests.
    #[arg(
        id = "s3-storage-addressing-style",
        long = "s3-addressing-style",
        value_name = "STYLE",
        value_enum
    )]
    addressing_style: Option<AddressingStyle>,
    /// Send unsigned requests without discovering credentials.
    #[arg(id = "s3-storage-anonymous", long = "s3-anonymous")]
    anonymous: bool,
    /// Limit each S3 HTTP request.
    #[arg(
        id = "s3-storage-request-timeout",
        long = "s3-request-timeout",
        value_name = "DURATION",
        value_parser = parse_positive_duration
    )]
    request_timeout: Option<Duration>,
}

/// Builds the feature-selected S3 backend.
///
/// Credentials remain in the upstream AWS environment, web-identity,
/// container, and instance discovery chain. The command contributes only
/// non-secret routing and request settings.
///
/// # Errors
///
/// Returns [`StorageBackendBuildError`] if the built-in definition violates
/// storage-backend invariants.
pub fn backend() -> Result<StorageBackend, StorageBackendBuildError> {
    StorageBackend::with_args::<S3Args>()
        .name("s3")
        .schemes(["s3"])
        .access(StorageAccess::ReadWrite)
        .location_validator(validate_location)
        .object_store_creator(create_object_store)
        .shared_retries()
        .build()
}

fn validate_location(location: &crate::Location, _settings: &S3Args) -> anyhow::Result<()> {
    validate_bucket_location(location)
}

fn create_object_store(
    store_url: &Url,
    settings: &S3Args,
    retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let bucket = store_url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("S3 URL requires a bucket"))?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

    if let Some(region) = &settings.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &settings.endpoint {
        builder = builder.with_endpoint(endpoint_string(endpoint));
        if endpoint.scheme() == "http" {
            builder = builder.with_allow_http(true);
        }
    }
    if let Some(style) = settings.addressing_style {
        builder =
            builder.with_virtual_hosted_style_request(matches!(style, AddressingStyle::Virtual));
    }
    if settings.anonymous {
        builder = builder.with_skip_signature(true);
    }
    if let Some(timeout) = settings.request_timeout {
        builder = builder.with_config(
            object_store::aws::AmazonS3ConfigKey::Client(ClientConfigKey::Timeout),
            humantime::format_duration(timeout).to_string(),
        );
    }
    if let Some(retry) = retry {
        builder = builder.with_retry(retry.clone());
    }

    Ok(Arc::new(builder.build()?))
}
