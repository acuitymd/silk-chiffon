//! Google Cloud Storage construction and ADC token adaptation.

use std::{collections::BTreeMap, fmt, sync::Arc};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder as CredentialsBuilder};
use object_store::{
    CopyMode, CopyOptions, ObjectStore,
    client::CredentialProvider,
    gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey},
    path::Path,
};

const GCS_READ_WRITE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// Copies an object with the intended conditional mode on object_store 0.13.2.
///
/// GCS 0.13.2 passes the opposite `if_not_exists` value to its client:
/// <https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/src/gcp/mod.rs#L218-L228>.
/// Version 0.14 passes `CopyMode` directly and fixes the inversion:
/// <https://github.com/apache/arrow-rs-object-store/blob/v0.14.0/src/gcp/mod.rs#L221-L230>.
pub(crate) async fn commit_gcs_013(
    store: &dyn ObjectStore,
    from: &Path,
    to: &Path,
    intended_mode: CopyMode,
) -> object_store::Result<()> {
    store
        .copy_opts(
            from,
            to,
            CopyOptions::new().with_mode(gcs_copy_mode_013(intended_mode)),
        )
        .await
}

pub(crate) const fn gcs_copy_mode_013(intended_mode: CopyMode) -> CopyMode {
    match intended_mode {
        CopyMode::Create => CopyMode::Overwrite,
        CopyMode::Overwrite => CopyMode::Create,
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum AuthenticationMode {
    ApplicationDefault,
    Anonymous,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum AdcSource {
    EnvironmentFile(String),
    WellKnownFileOrMetadata,
}

impl fmt::Debug for AdcSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnvironmentFile(_) => f.write_str("EnvironmentFile([redacted])"),
            Self::WellKnownFileOrMetadata => f.write_str("WellKnownFileOrMetadata"),
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct GcsEnvironment {
    pub(crate) authentication: AuthenticationMode,
    pub(crate) adc_source: AdcSource,
    pub(crate) transport: BTreeMap<String, String>,
}

impl fmt::Debug for GcsEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcsEnvironment")
            .field("authentication", &self.authentication)
            .field("adc_source", &self.adc_source)
            .field("transport_keys", &self.transport.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl GcsEnvironment {
    pub(crate) fn from_process() -> Result<Self> {
        Self::from_pairs(std::env::vars())
    }

    pub(crate) fn from_pairs<I, K, V>(values: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values: Vec<(String, String)> = values
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        let adc_source = values
            .iter()
            .find(|(key, value)| key == "GOOGLE_APPLICATION_CREDENTIALS" && !value.is_empty())
            .map_or(AdcSource::WellKnownFileOrMetadata, |(_, value)| {
                AdcSource::EnvironmentFile(value.clone())
            });

        let mut authentication = AuthenticationMode::ApplicationDefault;
        let mut transport = BTreeMap::new();
        for (key, value) in values {
            let Some(key) = key.strip_prefix("GOOGLE_") else {
                continue;
            };
            let Ok(config_key) = format!("google_{}", key.to_ascii_lowercase()).parse() else {
                continue;
            };

            match config_key {
                GoogleConfigKey::SkipSignature => {
                    let skip_signature = value.parse::<bool>().map_err(|_| {
                        anyhow!("GOOGLE_SKIP_SIGNATURE must be either true or false")
                    })?;
                    authentication = if skip_signature {
                        AuthenticationMode::Anonymous
                    } else {
                        AuthenticationMode::ApplicationDefault
                    };
                }
                GoogleConfigKey::BaseUrl | GoogleConfigKey::Client(_) => {
                    transport.insert(config_key.as_ref().to_string(), value);
                }
                _ => {}
            }
        }

        Ok(Self {
            authentication,
            adc_source,
            transport,
        })
    }
}

pub(crate) trait GcsStoreFactory: fmt::Debug + Send + Sync {
    fn build(&self, bucket: &str, environment: &GcsEnvironment) -> Result<Arc<dyn ObjectStore>>;
}

#[derive(Debug, Default)]
pub(crate) struct NativeGcsStoreFactory;

impl GcsStoreFactory for NativeGcsStoreFactory {
    fn build(&self, bucket: &str, environment: &GcsEnvironment) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
        for (key, value) in &environment.transport {
            let config_key: GoogleConfigKey = key
                .parse()
                .map_err(|_| anyhow!("unsupported GCS client setting '{key}'"))?;
            builder = builder.with_config(config_key, value);
        }

        match environment.authentication {
            AuthenticationMode::Anonymous => {
                builder = builder.with_skip_signature(true);
            }
            AuthenticationMode::ApplicationDefault => {
                let credentials = build_adc_credentials(&environment.adc_source)?;
                let provider =
                    GoogleCredentialProvider::new(credentials, environment.adc_source.clone());
                builder = builder.with_credentials(Arc::new(provider));
            }
        }

        let store = builder.build().map_err(|_| {
            anyhow!("could not configure Google Cloud Storage client for bucket '{bucket}'")
        })?;
        Ok(Arc::new(store))
    }
}

fn build_adc_credentials(source: &AdcSource) -> Result<AccessTokenCredentials> {
    CredentialsBuilder::default()
        .with_scopes([GCS_READ_WRITE_SCOPE])
        .build_access_token_credentials()
        .map_err(|error| {
            let problem = if error.is_loading() {
                "the credentials file could not be opened"
            } else if error.is_parsing() {
                "the credentials file could not be parsed"
            } else if error.is_unknown_type() {
                "the credential type is unknown"
            } else if error.is_missing_field() {
                "the credential configuration is missing a required field"
            } else if error.is_not_supported() {
                "the credential type is unsupported"
            } else {
                "credential construction failed"
            };
            anyhow!(
                "could not load Google Application Default Credentials from {}: {problem}",
                source.description()
            )
        })
}

impl AdcSource {
    fn description(&self) -> &'static str {
        match self {
            Self::EnvironmentFile(_) => "GOOGLE_APPLICATION_CREDENTIALS",
            Self::WellKnownFileOrMetadata => "the well-known ADC file or metadata server",
        }
    }
}

#[derive(Clone)]
pub(crate) struct GoogleCredentialProvider {
    credentials: AccessTokenCredentials,
    source: AdcSource,
}

impl GoogleCredentialProvider {
    pub(crate) fn new(credentials: AccessTokenCredentials, source: AdcSource) -> Self {
        Self {
            credentials,
            source,
        }
    }
}

impl fmt::Debug for GoogleCredentialProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GoogleCredentialProvider")
            .field("source", &self.source)
            .field("credentials", &"[redacted]")
            .finish()
    }
}

#[async_trait]
impl CredentialProvider for GoogleCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        let token =
            self.credentials
                .access_token()
                .await
                .map_err(|_| object_store::Error::Generic {
                    store: "GCS",
                    source: Box::new(RedactedCredentialError {
                        source: self.source.description(),
                    }),
                })?;
        Ok(Arc::new(GcpCredential {
            bearer: token.token,
        }))
    }
}

#[derive(Debug)]
struct RedactedCredentialError {
    source: &'static str,
}

impl fmt::Display for RedactedCredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Google Application Default Credentials from {} could not produce an access token",
            self.source
        )
    }
}

impl std::error::Error for RedactedCredentialError {}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::{
        credentials::{
            AccessToken, AccessTokenCredentialsProvider, CacheableResource, CredentialsProvider,
        },
        errors::CredentialsError,
    };
    use http::{Extensions, HeaderMap};

    #[test]
    fn object_store_013_gcs_copy_mode_is_inverted_at_compatibility_boundary() {
        assert_eq!(gcs_copy_mode_013(CopyMode::Create), CopyMode::Overwrite);
        assert_eq!(gcs_copy_mode_013(CopyMode::Overwrite), CopyMode::Create);
    }

    #[derive(Debug)]
    struct StubTokenProvider {
        result: std::result::Result<String, String>,
    }

    impl CredentialsProvider for StubTokenProvider {
        async fn headers(
            &self,
            _extensions: Extensions,
        ) -> std::result::Result<CacheableResource<HeaderMap>, CredentialsError> {
            unreachable!()
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }

    impl AccessTokenCredentialsProvider for StubTokenProvider {
        async fn access_token(&self) -> std::result::Result<AccessToken, CredentialsError> {
            match &self.result {
                Ok(token) => Ok(AccessToken {
                    token: token.clone(),
                }),
                Err(message) => Err(CredentialsError::from_msg(false, message.clone())),
            }
        }
    }

    #[test]
    fn parses_anonymous_endpoint_without_adc() {
        let environment = GcsEnvironment::from_pairs([
            ("GOOGLE_SKIP_SIGNATURE", "true"),
            ("GOOGLE_BASE_URL", "http://127.0.0.1:4443"),
            ("GOOGLE_APPLICATION_CREDENTIALS", "/secret/credentials.json"),
        ])
        .unwrap();

        assert_eq!(environment.authentication, AuthenticationMode::Anonymous);
        assert_eq!(
            environment
                .transport
                .get("google_base_url")
                .map(String::as_str),
            Some("http://127.0.0.1:4443")
        );
    }

    #[test]
    fn preserves_google_client_settings() {
        let environment = GcsEnvironment::from_pairs([
            ("GOOGLE_PROXY_URL", "http://proxy.internal:8080"),
            ("GOOGLE_TIMEOUT", "45s"),
        ])
        .unwrap();

        assert_eq!(
            environment.transport.get("proxy_url").map(String::as_str),
            Some("http://proxy.internal:8080")
        );
        assert_eq!(
            environment.transport.get("timeout").map(String::as_str),
            Some("45s")
        );
        assert_eq!(
            environment.authentication,
            AuthenticationMode::ApplicationDefault
        );
    }

    #[test]
    fn anonymous_store_does_not_open_configured_adc_file() {
        let environment = GcsEnvironment::from_pairs([
            ("GOOGLE_SKIP_SIGNATURE", "true"),
            ("GOOGLE_BASE_URL", "http://127.0.0.1:4443"),
            (
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/path/that/does/not/exist.json",
            ),
        ])
        .unwrap();

        NativeGcsStoreFactory
            .build("public-bucket", &environment)
            .unwrap();
    }

    #[test]
    fn environment_debug_omits_setting_values() {
        let secret = "proxy-password-value";
        let environment = GcsEnvironment::from_pairs([
            ("GOOGLE_PROXY_URL", format!("http://user:{secret}@proxy")),
            (
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/private/credentials.json".to_string(),
            ),
        ])
        .unwrap();

        let output = format!("{environment:?}");
        assert!(!output.contains(secret));
        assert!(!output.contains("/private/credentials.json"));
    }

    #[tokio::test]
    async fn adapts_google_access_token() {
        let credentials = AccessTokenCredentials::from(StubTokenProvider {
            result: Ok("test-token".to_string()),
        });
        let provider =
            GoogleCredentialProvider::new(credentials, AdcSource::WellKnownFileOrMetadata);

        let credential = provider.get_credential().await.unwrap();
        assert_eq!(credential.bearer, "test-token");
    }

    #[tokio::test]
    async fn credential_errors_and_debug_output_redact_secrets() {
        let secret = "secret-token-value";
        let credentials = AccessTokenCredentials::from(StubTokenProvider {
            result: Err(secret.to_string()),
        });
        let provider = GoogleCredentialProvider::new(
            credentials,
            AdcSource::EnvironmentFile("/secret/path.json".to_string()),
        );

        let error = provider.get_credential().await.unwrap_err();
        let output = format!("{provider:?} {error:?} {error}");
        assert!(!output.contains(secret));
        assert!(!output.contains("/secret/path.json"));
        assert!(output.contains("GOOGLE_APPLICATION_CREDENTIALS"));
    }
}
