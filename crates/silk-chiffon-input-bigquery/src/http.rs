use std::{error::Error, fmt};

use reqwest::{Client, Method, Request, Response};

use crate::{
    auth::{CredentialError, SharedCredentialsProvider},
    transport::{
        EndpointSet, RequestContext, TransportConfig, TransportConfigError, USER_AGENT,
        X_GOOG_API_CLIENT,
    },
};

#[derive(Clone)]
pub struct RestTransport {
    client: Client,
    credentials: SharedCredentialsProvider,
    bigquery_base: url::Url,
    request_timeout: std::time::Duration,
}

impl RestTransport {
    pub fn new(
        credentials: SharedCredentialsProvider,
        endpoints: EndpointSet,
        config: TransportConfig,
    ) -> Result<Self, RestTransportError> {
        let config = config.validate().map_err(RestTransportError::Config)?;
        let client = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(USER_AGENT)
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "x-goog-api-client",
                    reqwest::header::HeaderValue::from_static(X_GOOG_API_CLIENT),
                );
                headers
            })
            .build()
            .map_err(RestTransportError::RequestConstruction)?;
        let bigquery_base = endpoints.into_bigquery();
        Ok(Self {
            client,
            credentials,
            bigquery_base,
            request_timeout: config.request_timeout,
        })
    }

    pub fn request(&self, method: Method, path: &str) -> Result<Request, RestTransportError> {
        if url::Url::parse(path).is_ok()
            || path.starts_with("//")
            || (path.starts_with('/') && self.bigquery_base.path() != "/")
        {
            return Err(RestTransportError::Boundary);
        }
        let url = self
            .bigquery_base
            .join(path)
            .map_err(RestTransportError::Url)?;
        self.validate_url(&url)?;
        self.client
            .request(method, url)
            .build()
            .map_err(RestTransportError::RequestConstruction)
    }

    pub async fn execute(
        &self,
        request: Request,
        context: RequestContext,
    ) -> Result<Response, RestTransportError> {
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        tokio::time::timeout_at(
            deadline,
            self.execute_with_credentials(request, context, deadline),
        )
        .await
        .map_err(|_| RestTransportError::DeadlineExceeded)?
    }

    async fn execute_with_credentials(
        &self,
        mut request: Request,
        context: RequestContext,
        deadline: tokio::time::Instant,
    ) -> Result<Response, RestTransportError> {
        self.validate_url(request.url())?;
        let auth = self
            .credentials
            .headers(&context)
            .await
            .map_err(RestTransportError::Credentials)?;
        apply_auth_headers(request.headers_mut(), &auth);
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(RestTransportError::DeadlineExceeded);
        }
        *request.timeout_mut() = Some(remaining);
        tracing::debug!(
            operation = context.operation(),
            attempt = context.attempt(),
            "sending REST request"
        );
        self.client
            .execute(request)
            .await
            .map_err(RestTransportError::Request)
    }

    pub(crate) fn bigquery_base(&self) -> &url::Url {
        &self.bigquery_base
    }

    fn validate_url(&self, url: &url::Url) -> Result<(), RestTransportError> {
        let same_origin = url.origin() == self.bigquery_base.origin();
        let has_no_userinfo = url.username().is_empty() && url.password().is_none();
        let base_segments = path_segments(&self.bigquery_base);
        let request_segments = path_segments(url);
        if !same_origin || !has_no_userinfo || !request_segments.starts_with(&base_segments) {
            return Err(RestTransportError::Boundary);
        }
        Ok(())
    }
}

fn path_segments(url: &url::Url) -> Vec<&str> {
    let mut segments = url
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>())
        .unwrap_or_default();
    if segments.last() == Some(&"") {
        segments.pop();
    }
    segments
}

fn apply_auth_headers(headers: &mut reqwest::header::HeaderMap, auth: &crate::auth::AuthHeaders) {
    let names = auth
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<std::collections::HashSet<_>>();
    for name in names {
        headers.remove(name);
    }
    for (name, value) in auth.iter() {
        let mut value = value.clone();
        value.set_sensitive(true);
        headers.append(name, value);
    }
}

pub enum RestTransportError {
    Boundary,
    Config(TransportConfigError),
    Credentials(CredentialError),
    DeadlineExceeded,
    RequestConstruction(reqwest::Error),
    Request(reqwest::Error),
    Url(url::ParseError),
}

impl RestTransportError {
    pub fn is_retryable_request_failure(&self) -> bool {
        matches!(self, Self::DeadlineExceeded | Self::Request(_))
            || matches!(self, Self::Credentials(error) if error.retryable())
    }
}

impl fmt::Display for RestTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boundary => {
                formatter.write_str("REST request is outside the configured endpoint")
            }
            Self::Config(_) => formatter.write_str("invalid REST transport configuration"),
            Self::Credentials(_) => formatter.write_str("REST authentication failed"),
            Self::DeadlineExceeded => formatter.write_str("REST request timed out"),
            Self::RequestConstruction(_) => {
                formatter.write_str("failed to construct the REST request")
            }
            Self::Request(error) if error.is_timeout() => {
                formatter.write_str("REST request timed out")
            }
            Self::Request(_) => formatter.write_str("REST transport failed"),
            Self::Url(_) => formatter.write_str("invalid REST request path"),
        }
    }
}

impl fmt::Debug for RestTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestTransportError")
            .field("message", &self.to_string())
            .finish_non_exhaustive()
    }
}

impl Error for RestTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Boundary | Self::DeadlineExceeded => None,
            Self::Config(error) => Some(error),
            Self::Credentials(error) => Some(error),
            Self::RequestConstruction(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::Url(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_request_debug_redacts_provider_values() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer request-debug-sentinel".parse().unwrap(),
        );
        let auth = crate::auth::AuthHeaders::new(headers);
        let mut request = Request::new(Method::GET, "https://example.com/".parse().unwrap());

        apply_auth_headers(request.headers_mut(), &auth);

        assert!(!format!("{request:?}").contains("request-debug-sentinel"));
    }

    #[test]
    fn local_request_construction_is_not_a_retryable_transport_failure() {
        let error = reqwest::Client::new()
            .get("://invalid")
            .build()
            .unwrap_err();
        let error = RestTransportError::RequestConstruction(error);

        assert!(!error.is_retryable_request_failure());
    }

    #[test]
    fn transient_credential_failures_are_retryable_rest_failures() {
        let transient = RestTransportError::Credentials(CredentialError::transient_provider(
            std::io::Error::other("transient credential failure"),
        ));
        let permanent = RestTransportError::Credentials(CredentialError::provider(
            std::io::Error::other("permanent credential failure"),
        ));

        assert!(transient.is_retryable_request_failure());
        assert!(!permanent.is_retryable_request_failure());
    }
}
