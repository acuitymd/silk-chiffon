use std::{error::Error, fmt, num::NonZeroUsize, pin::Pin, sync::Arc, time::Duration};

use futures::Stream;

use tonic::{
    Request, Response, Status,
    codec::Streaming,
    metadata::{Ascii, MetadataKey, MetadataValue},
    transport::{Channel, Endpoint},
};
use url::{Host, Url};

use crate::{
    auth::{AuthHeaders, CredentialError, SharedCredentialsProvider},
    proto::bigquery_storage::{
        CreateReadSessionRequest, ReadRowsRequest, ReadRowsResponse, ReadSession,
        big_query_read_client::BigQueryReadClient,
    },
};

pub const USER_AGENT: &str = concat!("silk-chiffon/", env!("CARGO_PKG_VERSION"));
pub const X_GOOG_API_CLIENT: &str = concat!(
    "gl-rust/1.95.0 gccl/silk-chiffon-",
    env!("CARGO_PKG_VERSION")
);

const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";
pub const MAX_GRPC_CONNECTIONS: usize = 64;
pub const MAX_HTTP2_WINDOW_SIZE: u32 = 2_147_483_647;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    operation: &'static str,
    attempt: u32,
    stream_ordinal: Option<usize>,
    connection_index: Option<usize>,
    accepted_offset: Option<i64>,
}

impl RequestContext {
    pub const fn new(operation: &'static str, attempt: u32) -> Self {
        Self {
            operation,
            attempt,
            stream_ordinal: None,
            connection_index: None,
            accepted_offset: None,
        }
    }

    pub const fn with_read_stream(
        mut self,
        stream_ordinal: usize,
        connection_index: usize,
        accepted_offset: i64,
    ) -> Self {
        self.stream_ordinal = Some(stream_ordinal);
        self.connection_index = Some(connection_index);
        self.accepted_offset = Some(accepted_offset);
        self
    }

    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    #[cfg(test)]
    pub const fn stream_ordinal(&self) -> Option<usize> {
        self.stream_ordinal
    }

    #[cfg(test)]
    pub const fn connection_index(&self) -> Option<usize> {
        self.connection_index
    }

    #[cfg(test)]
    pub const fn accepted_offset(&self) -> Option<i64> {
        self.accepted_offset
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_decoding_message_size: usize,
    pub max_encoding_message_size: usize,
    pub grpc_connections: NonZeroUsize,
    pub http2_initial_stream_window_size: Option<u32>,
    pub http2_initial_connection_window_size: Option<u32>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(60),
            max_decoding_message_size: 256 * 1024 * 1024,
            max_encoding_message_size: 16 * 1024 * 1024,
            grpc_connections: NonZeroUsize::MIN,
            http2_initial_stream_window_size: None,
            http2_initial_connection_window_size: None,
        }
    }
}

impl TransportConfig {
    pub fn validate(self) -> Result<Self, TransportConfigError> {
        if self.connect_timeout.is_zero() {
            return Err(TransportConfigError("connect timeout must be positive"));
        }
        if self.request_timeout.is_zero() {
            return Err(TransportConfigError("request timeout must be positive"));
        }
        if self.max_decoding_message_size == 0 {
            return Err(TransportConfigError(
                "maximum decoded message size must be positive",
            ));
        }
        if self.max_encoding_message_size == 0 {
            return Err(TransportConfigError(
                "maximum encoded message size must be positive",
            ));
        }
        if self.grpc_connections.get() > MAX_GRPC_CONNECTIONS {
            return Err(TransportConfigError(
                "gRPC connection count cannot exceed 64",
            ));
        }
        validate_http2_window(
            self.http2_initial_stream_window_size,
            "HTTP/2 initial stream window must be positive",
            "HTTP/2 initial stream window cannot exceed 2147483647 bytes",
        )?;
        validate_http2_window(
            self.http2_initial_connection_window_size,
            "HTTP/2 initial connection window must be positive",
            "HTTP/2 initial connection window cannot exceed 2147483647 bytes",
        )?;
        Ok(self)
    }

    pub const fn http2_adaptive_window(self) -> Option<bool> {
        if self.http2_initial_stream_window_size.is_some()
            || self.http2_initial_connection_window_size.is_some()
        {
            Some(false)
        } else {
            None
        }
    }
}

fn validate_http2_window(
    value: Option<u32>,
    zero_message: &'static str,
    maximum_message: &'static str,
) -> Result<(), TransportConfigError> {
    match value {
        Some(0) => Err(TransportConfigError(zero_message)),
        Some(value) if value > MAX_HTTP2_WINDOW_SIZE => Err(TransportConfigError(maximum_message)),
        Some(_) | None => Ok(()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportConfigError(&'static str);

impl fmt::Display for TransportConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TransportConfigError {}

#[derive(Clone, Eq, PartialEq)]
pub struct EndpointSet {
    storage: Url,
    bigquery: Url,
    universe_domain: String,
    explicit_override: bool,
}

impl EndpointSet {
    pub fn new(
        universe_domain: Option<&str>,
        endpoint_override: Option<&str>,
    ) -> Result<Self, EndpointError> {
        let universe_domain = universe_domain.unwrap_or(DEFAULT_UNIVERSE_DOMAIN);
        validate_universe_domain(universe_domain)?;
        let storage = endpoint(
            endpoint_override,
            &format!("https://bigquerystorage.{universe_domain}"),
        )?;
        let bigquery = endpoint(
            endpoint_override,
            &format!("https://bigquery.{universe_domain}"),
        )?;
        Ok(Self {
            storage,
            bigquery,
            universe_domain: universe_domain.to_owned(),
            explicit_override: endpoint_override.is_some(),
        })
    }

    pub fn storage(&self) -> &Url {
        &self.storage
    }

    pub fn bigquery(&self) -> &Url {
        &self.bigquery
    }

    pub fn into_bigquery(self) -> Url {
        self.bigquery
    }

    pub fn universe_domain(&self) -> &str {
        &self.universe_domain
    }

    pub const fn has_explicit_override(&self) -> bool {
        self.explicit_override
    }
}

impl fmt::Debug for EndpointSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointSet")
            .field("has_explicit_override", &self.explicit_override)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_defaults_follow_the_universe_domain() {
        let defaults = EndpointSet::new(None, None).unwrap();
        let sovereign = EndpointSet::new(Some("exampleapis.com"), None).unwrap();

        assert_eq!(
            defaults.storage().as_str(),
            "https://bigquerystorage.googleapis.com/"
        );
        assert_eq!(
            defaults.bigquery().as_str(),
            "https://bigquery.googleapis.com/"
        );
        assert_eq!(
            sovereign.storage().as_str(),
            "https://bigquerystorage.exampleapis.com/"
        );
        assert_eq!(
            sovereign.bigquery().as_str(),
            "https://bigquery.exampleapis.com/"
        );
    }

    #[test]
    fn one_override_is_used_for_both_private_transports() {
        for endpoint in ["http://127.0.0.1:8080", "http://[::1]:8080"] {
            let endpoints = EndpointSet::new(None, Some(endpoint)).unwrap();
            assert_eq!(endpoints.storage(), endpoints.bigquery());
        }
    }

    #[test]
    fn endpoint_set_revalidates_private_callers() {
        for invalid in [
            "http://example.com",
            "https://user@example.com",
            "https://example.com?query=value",
        ] {
            assert!(EndpointSet::new(None, Some(invalid)).is_err());
        }
        assert!(EndpointSet::new(Some("bad_domain.example"), None).is_err());
    }

    #[test]
    fn request_context_contains_no_session_replacement_ordinal() {
        let context = RequestContext::new("read_rows", 3).with_read_stream(2, 0, 42);
        let debug = format!("{context:?}");

        assert_eq!(context.attempt(), 3);
        assert_eq!(context.stream_ordinal(), Some(2));
        assert_eq!(context.connection_index(), Some(0));
        assert_eq!(context.accepted_offset(), Some(42));
        assert!(!debug.contains("session_attempt"));
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointError(String);

impl fmt::Display for EndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for EndpointError {}

fn validate_universe_domain(domain: &str) -> Result<(), EndpointError> {
    let parsed = Url::parse(&format!("https://{domain}"))
        .map_err(|_| EndpointError("universe domain must be a DNS name".to_owned()))?;
    let valid_labels = domain.len() <= 253
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        });
    let valid_shape = parsed.host_str() == Some(domain)
        && matches!(parsed.host(), Some(url::Host::Domain(_)))
        && parsed.port().is_none()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.path() == "/";
    if !valid_shape || !valid_labels || !domain.is_ascii() || !domain.contains('.') {
        return Err(EndpointError(
            "universe domain must be an ASCII DNS name without a scheme, port, or path".to_owned(),
        ));
    }
    Ok(())
}

fn endpoint(override_uri: Option<&str>, default_uri: &str) -> Result<Url, EndpointError> {
    let mut endpoint = Url::parse(override_uri.unwrap_or(default_uri))
        .map_err(|_| EndpointError("endpoint must be an absolute URI".to_owned()))?;
    if !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(EndpointError(
            "endpoint cannot contain credentials, a query, or a fragment".to_owned(),
        ));
    }
    match endpoint.scheme() {
        "https" => {}
        "http" if override_uri.is_some() && endpoint_is_loopback(&endpoint) => {}
        "http" => {
            return Err(EndpointError(
                "HTTP endpoint overrides are limited to loopback hosts".to_owned(),
            ));
        }
        _ => {
            return Err(EndpointError(
                "endpoint scheme must be HTTPS, or HTTP for an explicit loopback override"
                    .to_owned(),
            ));
        }
    }
    if !endpoint.path().ends_with('/') {
        endpoint.set_path(&format!("{}/", endpoint.path()));
    }
    Ok(endpoint)
}

pub(crate) fn endpoint_is_loopback(endpoint: &Url) -> bool {
    match endpoint.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

#[derive(Clone)]
pub(crate) struct StorageTransport {
    clients: Arc<[BigQueryReadClient<Channel>]>,
    read_client_index: usize,
    credentials: SharedCredentialsProvider,
    unary_timeout: Duration,
}

pub(crate) type ReadRowsResponseStream =
    Pin<Box<dyn Stream<Item = Result<ReadRowsResponse, Status>> + Send>>;

#[tonic::async_trait]
pub(crate) trait ReadRowsRpc: Send + Sync + fmt::Debug {
    async fn open(
        &self,
        stream_ordinal: usize,
        request: ReadRowsRequest,
        context: RequestContext,
    ) -> Result<ReadRowsResponseStream, StorageTransportError>;
}

#[derive(Clone)]
pub(crate) struct RetriedReadRowsRpc(StorageTransport);

impl RetriedReadRowsRpc {
    pub(crate) const fn new(transport: StorageTransport) -> Self {
        Self(transport)
    }
}

#[tonic::async_trait]
impl ReadRowsRpc for RetriedReadRowsRpc {
    async fn open(
        &self,
        stream_ordinal: usize,
        request: ReadRowsRequest,
        context: RequestContext,
    ) -> Result<ReadRowsResponseStream, StorageTransportError> {
        let response = self
            .0
            .for_stream_ordinal(stream_ordinal)
            .read_rows(request, context)
            .await?;
        Ok(Box::pin(response.into_inner()))
    }
}

impl fmt::Debug for RetriedReadRowsRpc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetriedReadRowsRpc")
            .finish_non_exhaustive()
    }
}

impl StorageTransport {
    pub(crate) async fn connect(
        credentials: SharedCredentialsProvider,
        endpoints: &EndpointSet,
        config: TransportConfig,
    ) -> Result<Self, StorageTransportError> {
        let config = config.validate().map_err(StorageTransportError::Config)?;
        let mut endpoint = Endpoint::new(endpoints.storage().to_string())
            .map_err(StorageTransportError::Connect)?
            .connect_timeout(config.connect_timeout)
            .user_agent(USER_AGENT)
            .map_err(StorageTransportError::Connect)?;
        if let Some(window) = config.http2_initial_stream_window_size {
            endpoint = endpoint.initial_stream_window_size(window);
        }
        if let Some(window) = config.http2_initial_connection_window_size {
            endpoint = endpoint.initial_connection_window_size(window);
        }
        if let Some(enabled) = config.http2_adaptive_window() {
            endpoint = endpoint.http2_adaptive_window(enabled);
        }
        let channels = futures::future::try_join_all(
            (0..config.grpc_connections.get()).map(|_| endpoint.connect()),
        )
        .await
        .map_err(StorageTransportError::Connect)?;
        let clients = channels
            .into_iter()
            .map(|channel| {
                BigQueryReadClient::new(channel)
                    .max_decoding_message_size(config.max_decoding_message_size)
                    .max_encoding_message_size(config.max_encoding_message_size)
            })
            .collect::<Vec<_>>()
            .into();
        Ok(Self {
            clients,
            read_client_index: 0,
            credentials,
            unary_timeout: config.request_timeout,
        })
    }

    pub(crate) fn for_stream_ordinal(&self, ordinal: usize) -> Self {
        let mut transport = self.clone();
        transport.read_client_index = ordinal % self.clients.len();
        transport
    }

    pub(crate) const fn unary_timeout(&self) -> Duration {
        self.unary_timeout
    }

    pub(crate) async fn create_read_session(
        &self,
        request: CreateReadSessionRequest,
        context: RequestContext,
    ) -> Result<Response<ReadSession>, StorageTransportError> {
        tokio::time::timeout(self.unary_timeout, async {
            let table = request
                .read_session
                .as_ref()
                .map_or_else(String::new, |session| session.table.clone());
            let request = self
                .request(
                    request,
                    "read_session.table",
                    &table,
                    Some(self.unary_timeout),
                    &context,
                )
                .await?;
            self.clients[0]
                .clone()
                .create_read_session(request)
                .await
                .map_err(StorageTransportError::Status)
        })
        .await
        .map_err(|_| {
            StorageTransportError::Status(Status::deadline_exceeded(
                "CreateReadSession attempt deadline exceeded",
            ))
        })?
    }

    pub(crate) async fn read_rows(
        &self,
        request: ReadRowsRequest,
        context: RequestContext,
    ) -> Result<Response<Streaming<ReadRowsResponse>>, StorageTransportError> {
        let route = request.read_stream.clone();
        let request = self
            .request(request, "read_stream", &route, None, &context)
            .await?;
        self.clients[self.read_client_index]
            .clone()
            .read_rows(request)
            .await
            .map_err(StorageTransportError::Status)
    }

    async fn request<T>(
        &self,
        message: T,
        route_name: &'static str,
        route_value: &str,
        timeout: Option<Duration>,
        context: &RequestContext,
    ) -> Result<Request<T>, StorageTransportError> {
        let auth = self
            .credentials
            .headers(context)
            .await
            .map_err(StorageTransportError::Credentials)?;
        let mut request = Request::new(message);
        if let Some(timeout) = timeout {
            request.set_timeout(timeout);
        }
        let routing = url::form_urlencoded::Serializer::new(String::new())
            .append_pair(route_name, route_value)
            .finish();
        request.metadata_mut().insert(
            "x-goog-request-params",
            routing
                .parse()
                .map_err(|_| StorageTransportError::InvalidMetadata)?,
        );
        request.metadata_mut().insert(
            "x-goog-api-client",
            MetadataValue::from_static(X_GOOG_API_CLIENT),
        );
        apply_auth_metadata(request.metadata_mut(), &auth)?;
        Ok(request)
    }
}

fn apply_auth_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    auth: &AuthHeaders,
) -> Result<(), StorageTransportError> {
    let mut values = Vec::new();
    let mut names = std::collections::HashSet::new();
    for (name, value) in auth.iter() {
        let key = name
            .as_str()
            .parse::<MetadataKey<Ascii>>()
            .map_err(|_| StorageTransportError::InvalidMetadata)?;
        let mut value = MetadataValue::<Ascii>::try_from(value.as_bytes())
            .map_err(|_| StorageTransportError::InvalidMetadata)?;
        value.set_sensitive(true);
        names.insert(key.clone());
        values.push((key, value));
    }
    for name in names {
        metadata.remove(name);
    }
    for (name, value) in values {
        metadata.append(name, value);
    }
    Ok(())
}

pub(crate) enum StorageTransportError {
    Config(TransportConfigError),
    Connect(tonic::transport::Error),
    Credentials(CredentialError),
    InvalidMetadata,
    Status(Status),
}

impl fmt::Display for StorageTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(_) => formatter.write_str("invalid Storage transport configuration"),
            Self::Connect(_) => formatter.write_str("Storage transport connection failed"),
            Self::Credentials(_) => formatter.write_str("Storage authentication failed"),
            Self::InvalidMetadata => formatter.write_str("invalid Storage request metadata"),
            Self::Status(status) => write!(
                formatter,
                "Storage RPC failed with status {}",
                status.code()
            ),
        }
    }
}

impl fmt::Debug for StorageTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => formatter
                .debug_tuple("Status")
                .field(&status.code())
                .field(&"<redacted>")
                .finish(),
            _ => formatter
                .debug_struct("StorageTransportError")
                .field("message", &self.to_string())
                .finish_non_exhaustive(),
        }
    }
}

impl Error for StorageTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Connect(error) => Some(error),
            Self::Credentials(error) => Some(error),
            Self::Status(status) => Some(status),
            Self::InvalidMetadata => None,
        }
    }
}

#[cfg(test)]
mod grpc_tests {
    use super::*;

    #[test]
    fn authenticated_metadata_debug_redacts_provider_values() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer metadata-debug-sentinel".parse().unwrap(),
        );
        let auth = AuthHeaders::new(headers);
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("authorization", "Bearer stale-metadata".parse().unwrap());

        apply_auth_metadata(&mut metadata, &auth).unwrap();

        assert!(!format!("{metadata:?}").contains("metadata-debug-sentinel"));
        assert_eq!(
            metadata
                .get_all("authorization")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["Bearer metadata-debug-sentinel"]
        );
    }
}

#[cfg(test)]
mod endpoint_integration_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{Router, routing::get};
    use futures::stream;
    use tonic::service::Routes;

    use super::*;
    use crate::{
        auth::{CredentialsProvider, SharedCredentialsProvider},
        http::RestTransport,
        proto::bigquery_storage::{
            SplitReadStreamRequest, SplitReadStreamResponse,
            big_query_read_server::{BigQueryRead, BigQueryReadServer},
        },
        snapshot::{ServerClockProbe, SystemMonotonicClock, TokioSleeper},
    };

    #[derive(Debug, Default)]
    struct SharedEndpointFake {
        create_calls: AtomicUsize,
    }

    #[tonic::async_trait]
    impl BigQueryRead for SharedEndpointFake {
        async fn create_read_session(
            &self,
            _request: Request<CreateReadSessionRequest>,
        ) -> Result<Response<ReadSession>, Status> {
            self.create_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(ReadSession::default()))
        }

        type ReadRowsStream = stream::Empty<Result<ReadRowsResponse, Status>>;

        async fn read_rows(
            &self,
            _request: Request<ReadRowsRequest>,
        ) -> Result<Response<Self::ReadRowsStream>, Status> {
            Ok(Response::new(stream::empty()))
        }

        async fn split_read_stream(
            &self,
            _request: Request<SplitReadStreamRequest>,
        ) -> Result<Response<SplitReadStreamResponse>, Status> {
            Err(Status::unimplemented("not used by the connector"))
        }
    }

    #[derive(Debug)]
    struct CountingCredentials(AtomicUsize);

    #[tonic::async_trait]
    impl CredentialsProvider for CountingCredentials {
        async fn headers(&self, _context: &RequestContext) -> Result<AuthHeaders, CredentialError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(AuthHeaders::new(http::HeaderMap::new()))
        }
    }

    #[tokio::test]
    async fn one_override_serves_real_rest_clock_and_tonic_traffic() {
        let service = Arc::new(SharedEndpointFake::default());
        let rest_calls = Arc::new(AtomicUsize::new(0));
        let rest_calls_for_route = Arc::clone(&rest_calls);
        let grpc = BigQueryReadServer::from_arc(Arc::clone(&service));
        let router = Routes::new(grpc).into_axum_router().route(
            "/bigquery/v2/projects/{project}/datasets",
            get(move || {
                let rest_calls = Arc::clone(&rest_calls_for_route);
                async move {
                    rest_calls.fetch_add(1, Ordering::SeqCst);
                    (
                        [
                            (http::header::DATE, "Sat, 15 Aug 2026 12:00:00 GMT"),
                            (http::header::CONTENT_TYPE, "application/json"),
                        ],
                        "{}",
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().merge(router))
                .await
                .unwrap();
        });

        let endpoint = format!("http://{address}");
        let endpoints = EndpointSet::new(None, Some(&endpoint)).unwrap();
        assert_eq!(endpoints.storage(), endpoints.bigquery());
        let credentials_impl = Arc::new(CountingCredentials(AtomicUsize::new(0)));
        let credentials = Arc::clone(&credentials_impl);
        let credentials: SharedCredentialsProvider = credentials;
        let config = TransportConfig::default();

        let rest = RestTransport::new(Arc::clone(&credentials), endpoints.clone(), config).unwrap();
        let probe = ServerClockProbe::new(
            rest,
            &endpoints,
            Arc::new(SystemMonotonicClock::default()),
            Arc::new(TokioSleeper),
        )
        .unwrap();
        probe.pin_snapshot("session-project").await.unwrap();

        let storage = StorageTransport::connect(credentials, &endpoints, config)
            .await
            .unwrap();
        storage
            .create_read_session(
                CreateReadSessionRequest::default(),
                RequestContext::new("create_read_session", 1),
            )
            .await
            .unwrap();

        assert_eq!(rest_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.create_calls.load(Ordering::SeqCst), 1);
        assert_eq!(credentials_impl.0.load(Ordering::SeqCst), 2);
        server.abort();
    }
}
