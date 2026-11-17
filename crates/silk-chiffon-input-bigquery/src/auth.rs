use std::{collections::BTreeSet, error::Error, fmt, sync::Arc};

use google_cloud_auth::credentials::{Builder, CacheableResource, Credentials};
use http::{Extensions, HeaderMap, HeaderValue};

use crate::transport::RequestContext;

pub const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

#[tonic::async_trait]
pub trait CredentialsProvider: fmt::Debug + Send + Sync {
    async fn headers(&self, context: &RequestContext) -> Result<AuthHeaders, CredentialError>;
}

pub type SharedCredentialsProvider = Arc<dyn CredentialsProvider>;

#[derive(Clone)]
pub struct AuthHeaders {
    headers: HeaderMap,
}

impl AuthHeaders {
    pub fn new(mut headers: HeaderMap) -> Self {
        for value in headers.values_mut() {
            value.set_sensitive(true);
        }
        Self { headers }
    }

    pub fn iter(&self) -> http::header::Iter<'_, HeaderValue> {
        self.headers.iter()
    }

    pub fn redacted(&self) -> RedactedHeaderMap {
        RedactedHeaderMap::new(self.headers.clone())
    }
}

impl fmt::Debug for AuthHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.redacted().fmt(formatter)
    }
}

#[derive(Clone)]
pub struct RedactedHeaderMap {
    headers: HeaderMap,
}

impl RedactedHeaderMap {
    pub fn new(headers: HeaderMap) -> Self {
        Self { headers }
    }
}

impl fmt::Debug for RedactedHeaderMap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self
            .headers
            .keys()
            .map(http::HeaderName::as_str)
            .collect::<BTreeSet<_>>();
        let entries = names
            .into_iter()
            .map(|name| (name, "<redacted>"))
            .collect::<Vec<_>>();
        formatter
            .debug_struct("RedactedHeaderMap")
            .field("headers", &entries)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialErrorKind {
    Build,
    Provider,
    InvalidHeader,
    Invariant,
}

pub struct CredentialError {
    kind: CredentialErrorKind,
    retryable: bool,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl CredentialError {
    #[cfg(test)]
    pub fn provider(error: impl Error + Send + Sync + 'static) -> Self {
        Self::provider_with_retryability(error, false)
    }

    #[cfg(test)]
    pub fn transient_provider(error: impl Error + Send + Sync + 'static) -> Self {
        Self::provider_with_retryability(error, true)
    }

    fn provider_with_retryability(
        error: impl Error + Send + Sync + 'static,
        retryable: bool,
    ) -> Self {
        Self {
            kind: CredentialErrorKind::Provider,
            retryable,
            source: Some(Box::new(error)),
        }
    }

    fn google_provider(error: google_cloud_auth::errors::CredentialsError) -> Self {
        let retryable = error.is_transient();
        Self::provider_with_retryability(error, retryable)
    }

    #[cfg(test)]
    pub const fn kind(&self) -> CredentialErrorKind {
        self.kind
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    fn build(error: impl Error + Send + Sync + 'static) -> Self {
        Self {
            kind: CredentialErrorKind::Build,
            retryable: false,
            source: Some(Box::new(error)),
        }
    }

    fn invalid_header() -> Self {
        Self {
            kind: CredentialErrorKind::InvalidHeader,
            retryable: false,
            source: None,
        }
    }

    fn invariant() -> Self {
        Self {
            kind: CredentialErrorKind::Invariant,
            retryable: false,
            source: None,
        }
    }
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            CredentialErrorKind::Build => "failed to build Application Default Credentials",
            CredentialErrorKind::Provider => "credential provider failed",
            CredentialErrorKind::InvalidHeader => "credential provider returned an invalid header",
            CredentialErrorKind::Invariant => "credential provider returned no headers",
        };
        formatter.write_str(message)
    }
}

impl fmt::Debug for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialError")
            .field("kind", &self.kind)
            .field("retryable", &self.retryable)
            .field("source", &self.source.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Error for CredentialError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[derive(Clone)]
pub struct AdcCredentials {
    credentials: Credentials,
    quota_project_override: Option<HeaderValue>,
}

impl AdcCredentials {
    pub fn new(
        quota_project_override: Option<&str>,
        universe_domain_override: Option<&str>,
    ) -> Result<Self, CredentialError> {
        crate::install_crypto_provider();
        let mut builder = Builder::default().with_scopes([CLOUD_PLATFORM_SCOPE]);
        if let Some(project) = quota_project_override {
            builder = builder.with_quota_project_id(project);
        }
        if let Some(domain) = universe_domain_override {
            builder = builder.with_universe_domain(domain);
        }
        let credentials = builder.build().map_err(CredentialError::build)?;
        let quota_project_override = quota_project_override
            .map(HeaderValue::from_str)
            .transpose()
            .map_err(|_| CredentialError::invalid_header())?;
        Ok(Self {
            credentials,
            quota_project_override,
        })
    }

    pub async fn universe_domain(&self) -> Option<String> {
        self.credentials.universe_domain().await
    }
}

impl fmt::Debug for AdcCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdcCredentials")
            .field(
                "quota_project_override",
                &self.quota_project_override.as_ref().map(|_| "<redacted>"),
            )
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl CredentialsProvider for AdcCredentials {
    async fn headers(&self, _context: &RequestContext) -> Result<AuthHeaders, CredentialError> {
        let resource = self
            .credentials
            .headers(Extensions::new())
            .await
            .map_err(CredentialError::google_provider)?;
        let CacheableResource::New { mut data, .. } = resource else {
            return Err(CredentialError::invariant());
        };
        if let Some(quota_project) = &self.quota_project_override {
            data.insert("x-goog-user-project", quota_project.clone());
        }
        Ok(AuthHeaders::new(data))
    }
}

#[cfg(test)]
mod tests {
    use google_cloud_auth::errors::CredentialsError as GoogleCredentialsError;

    use super::{CredentialError, CredentialErrorKind};

    #[test]
    fn google_auth_retryability_is_preserved() {
        let transient =
            CredentialError::google_provider(GoogleCredentialsError::from_msg(true, "transient"));
        let permanent =
            CredentialError::google_provider(GoogleCredentialsError::from_msg(false, "permanent"));

        assert_eq!(transient.kind(), CredentialErrorKind::Provider);
        assert!(transient.retryable());
        assert_eq!(permanent.kind(), CredentialErrorKind::Provider);
        assert!(!permanent.retryable());
    }
}
