//! Command-scoped authentication, transports, clocks, and decode concurrency.

use std::{fmt, sync::Arc};

use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use tokio::sync::Semaphore;

use crate::{
    args::BigQueryInputArgs,
    auth::{AdcCredentials, SharedCredentialsProvider},
    decode::DecodeLimit,
    http::RestTransport,
    retry::{
        JitterSource, Sleeper as RetrySleeper, ThreadJitter, TokioSleeper as RetryTokioSleeper,
    },
    session::{RetriedSessionOpener, SessionOpener},
    snapshot::{
        MonotonicClock, ServerClockProbe, Sleeper as ClockSleeper, SystemMonotonicClock,
        TokioSleeper as ClockTokioSleeper,
    },
    transport::{EndpointSet, ReadRowsRpc, RetriedReadRowsRpc, StorageTransport, TransportConfig},
};

pub(crate) struct CommandResources {
    pub(crate) endpoints: EndpointSet,
    pub(crate) read_rows: Arc<dyn ReadRowsRpc>,
    pub(crate) read_connection_count: usize,
    pub(crate) server_clock: Arc<ServerClockProbe>,
    pub(crate) monotonic_clock: Arc<dyn MonotonicClock>,
    pub(crate) retry_sleeper: Arc<dyn RetrySleeper>,
    pub(crate) retry_jitter: Arc<dyn JitterSource>,
    pub(crate) sessions: Arc<dyn SessionOpener>,
    pub(crate) decode_limit: DecodeLimit,
    pub(crate) decode_permits: Arc<Semaphore>,
}

impl CommandResources {
    pub(crate) async fn initialize(
        args: &BigQueryInputArgs,
        _session: &SessionContext,
    ) -> Result<Arc<Self>> {
        let credentials_impl = Arc::new(
            AdcCredentials::new(
                args.quota_project.as_deref(),
                args.universe_domain.as_deref(),
            )
            .context("failed to initialize BigQuery Application Default Credentials")?,
        );
        let discovered_universe = if args.universe_domain.is_none() {
            credentials_impl.universe_domain().await
        } else {
            None
        };
        let universe = args
            .universe_domain
            .as_deref()
            .or(discovered_universe.as_deref());
        let credentials: SharedCredentialsProvider = credentials_impl;
        Self::initialize_with_credentials(args, credentials, universe).await
    }

    pub(crate) async fn initialize_with_credentials(
        args: &BigQueryInputArgs,
        credentials: SharedCredentialsProvider,
        discovered_universe: Option<&str>,
    ) -> Result<Arc<Self>> {
        let universe = args.universe_domain.as_deref().or(discovered_universe);
        let endpoint_override = args.endpoint.as_ref().map(url::Url::as_str);
        let endpoints = EndpointSet::new(universe, endpoint_override)
            .context("invalid BigQuery endpoint configuration")?;
        let decode_limit = DecodeLimit::new(args.max_response_bytes)
            .context("invalid BigQuery response safety limit")?;
        let transport_config = TransportConfig {
            max_decoding_message_size: args.max_response_bytes,
            ..TransportConfig::default()
        };
        let read_connection_count = transport_config.grpc_connections.get();
        let rest = RestTransport::new(
            Arc::clone(&credentials),
            endpoints.clone(),
            transport_config,
        )
        .context("failed to initialize BigQuery REST transport")?;
        let storage =
            StorageTransport::connect(Arc::clone(&credentials), &endpoints, transport_config)
                .await
                .context("failed to initialize BigQuery Storage transport")?;
        let monotonic_clock: Arc<dyn MonotonicClock> = Arc::new(SystemMonotonicClock::default());
        let clock_sleeper: Arc<dyn ClockSleeper> = Arc::new(ClockTokioSleeper);
        let server_clock = Arc::new(
            ServerClockProbe::new(
                rest,
                &endpoints,
                Arc::clone(&monotonic_clock),
                clock_sleeper,
            )
            .context("failed to initialize the BigQuery server clock probe")?,
        );
        let retry_sleeper: Arc<dyn RetrySleeper> = Arc::new(RetryTokioSleeper);
        let retry_jitter: Arc<dyn JitterSource> = Arc::new(ThreadJitter);
        let sessions: Arc<dyn SessionOpener> = Arc::new(RetriedSessionOpener::new(
            storage.clone(),
            Arc::clone(&monotonic_clock),
            Arc::clone(&retry_sleeper),
            Arc::clone(&retry_jitter),
            decode_limit,
        ));
        let read_rows: Arc<dyn ReadRowsRpc> = Arc::new(RetriedReadRowsRpc::new(storage.clone()));
        let decode_parallelism = tokio::runtime::Handle::current()
            .metrics()
            .num_workers()
            .max(1);

        Ok(Arc::new(Self {
            endpoints,
            read_rows,
            read_connection_count,
            server_clock,
            monotonic_clock,
            retry_sleeper,
            retry_jitter,
            sessions,
            decode_limit,
            decode_permits: Arc::new(Semaphore::new(decode_parallelism)),
        }))
    }
}

impl fmt::Debug for CommandResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandResources")
            .field("endpoints", &self.endpoints)
            .field("decode_limit", &self.decode_limit)
            .field("decode_permits", &self.decode_permits.available_permits())
            .finish_non_exhaustive()
    }
}
