use std::{error::Error, fmt, sync::Arc, time::Duration};

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use futures::future::BoxFuture;
use http::{HeaderMap, header::DATE};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Method, StatusCode};

use crate::{
    http::{RestTransport, RestTransportError},
    transport::{EndpointSet, RequestContext},
};

const HTTP_DATE_UNCERTAINTY: Duration = Duration::from_secs(1);
const SERVER_CLOCK_ATTEMPTS: u32 = 4;
const SERVER_CLOCK_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) struct PinnedSnapshot {
    seconds: i64,
    nanos: i32,
}

impl PinnedSnapshot {
    pub(crate) fn from_rfc3339(value: &str) -> Result<Self, TimestampError> {
        let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| TimestampError)?;
        Self::new(
            parsed.timestamp(),
            i32::try_from(parsed.timestamp_subsec_nanos()).map_err(|_| TimestampError)?,
        )
    }

    pub(crate) fn new(seconds: i64, nanos: i32) -> Result<Self, TimestampError> {
        const MIN_SECONDS: i64 = -62_135_596_800;
        const MAX_SECONDS: i64 = 253_402_300_799;
        if !(MIN_SECONDS..=MAX_SECONDS).contains(&seconds) || !(0..1_000_000_000).contains(&nanos) {
            return Err(TimestampError);
        }
        Ok(Self { seconds, nanos })
    }

    pub(crate) const fn seconds(self) -> i64 {
        self.seconds
    }

    pub(crate) const fn nanos(self) -> i32 {
        self.nanos
    }

    pub(crate) fn to_proto(self) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: self.seconds,
            nanos: self.nanos,
        }
    }

    pub(crate) fn to_rfc3339(self) -> String {
        DateTime::<Utc>::from_timestamp(
            self.seconds,
            u32::try_from(self.nanos).expect("validated timestamp nanoseconds are nonnegative"),
        )
        .expect("validated protobuf timestamp")
        .to_rfc3339_opts(SecondsFormat::AutoSi, true)
    }

    fn checked_sub(self, duration: Duration) -> Result<Self, TimestampError> {
        let total_nanos = i128::from(self.seconds)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(i128::from(self.nanos)))
            .and_then(|value| {
                i128::try_from(duration.as_nanos())
                    .ok()
                    .and_then(|duration| value.checked_sub(duration))
            })
            .ok_or(TimestampError)?;
        let seconds = total_nanos.div_euclid(1_000_000_000);
        let nanos = total_nanos.rem_euclid(1_000_000_000);
        Self::new(
            i64::try_from(seconds).map_err(|_| TimestampError)?,
            i32::try_from(nanos).map_err(|_| TimestampError)?,
        )
    }
}

impl fmt::Debug for PinnedSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PinnedSnapshot")
            .field(&self.to_rfc3339())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimestampError;

impl fmt::Display for TimestampError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("timestamp is outside the protobuf Timestamp range")
    }
}

impl Error for TimestampError {}

pub(crate) trait MonotonicClock: Send + Sync + fmt::Debug {
    fn elapsed(&self) -> Duration;
}

#[derive(Debug)]
pub(crate) struct SystemMonotonicClock(std::time::Instant);

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self(std::time::Instant::now())
    }
}

impl MonotonicClock for SystemMonotonicClock {
    fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

pub(crate) trait Sleeper: Send + Sync + fmt::Debug {
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()>;
}

#[derive(Debug)]
pub(crate) struct TokioSleeper;

impl Sleeper for TokioSleeper {
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

pub(crate) struct ServerClockProbe {
    transport: RestTransport,
    clock: Arc<dyn MonotonicClock>,
    sleeper: Arc<dyn Sleeper>,
    trusted: bool,
}

impl ServerClockProbe {
    pub(crate) fn new(
        transport: RestTransport,
        endpoints: &EndpointSet,
        clock: Arc<dyn MonotonicClock>,
        sleeper: Arc<dyn Sleeper>,
    ) -> Result<Self, ServerClockError> {
        let trusted = transport.bigquery_base() == endpoints.bigquery()
            && Self::endpoint_is_trusted(endpoints);
        if !trusted {
            return Err(ServerClockError::new(
                ServerClockErrorKind::UntrustedEndpoint,
            ));
        }
        Ok(Self {
            transport,
            clock,
            sleeper,
            trusted,
        })
    }

    pub(crate) fn endpoint_is_trusted(endpoints: &EndpointSet) -> bool {
        if endpoints.has_explicit_override() {
            return endpoints.storage() == endpoints.bigquery();
        }
        let universe = endpoints.universe_domain();
        endpoints.storage().as_str() == format!("https://bigquerystorage.{universe}/")
            && endpoints.bigquery().as_str() == format!("https://bigquery.{universe}/")
    }

    pub(crate) async fn pin_snapshot(
        &self,
        session_project: &str,
    ) -> Result<PinnedSnapshot, ServerClockError> {
        if !self.trusted {
            return Err(ServerClockError::new(
                ServerClockErrorKind::UntrustedEndpoint,
            ));
        }
        let project = utf8_percent_encode(session_project, NON_ALPHANUMERIC);
        let path = format!("bigquery/v2/projects/{project}/datasets?maxResults=1");
        let mut attempt = 1_u32;
        loop {
            let request = self
                .transport
                .request(Method::GET, &path)
                .map_err(ServerClockError::transport)?;
            let started = self.clock.elapsed();
            match self
                .transport
                .execute(
                    request,
                    RequestContext::new("bigquery.server-clock", attempt),
                )
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    let received = self.clock.elapsed();
                    match snapshot_from_headers(response.headers(), started, received) {
                        Ok(snapshot) => return Ok(snapshot),
                        Err(error)
                            if error.date_is_unusable()
                                && retryable_http_status(status)
                                && attempt < SERVER_CLOCK_ATTEMPTS =>
                        {
                            self.sleeper
                                .sleep(SERVER_CLOCK_INITIAL_RETRY_DELAY.saturating_mul(attempt))
                                .await;
                            attempt += 1;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error)
                    if error.is_retryable_request_failure() && attempt < SERVER_CLOCK_ATTEMPTS =>
                {
                    self.sleeper
                        .sleep(SERVER_CLOCK_INITIAL_RETRY_DELAY.saturating_mul(attempt))
                        .await;
                    attempt += 1;
                }
                Err(error) => return Err(ServerClockError::transport(error)),
            }
        }
    }
}

impl fmt::Debug for ServerClockProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerClockProbe")
            .field("trusted", &self.trusted)
            .finish_non_exhaustive()
    }
}

fn snapshot_from_headers(
    headers: &HeaderMap,
    started: Duration,
    received: Duration,
) -> Result<PinnedSnapshot, ServerClockError> {
    let uncertainty = received
        .checked_sub(started)
        .ok_or_else(|| ServerClockError::new(ServerClockErrorKind::NonMonotonicClock))?;
    let dates = headers.get_all(DATE).iter().collect::<Vec<_>>();
    let [date] = dates.as_slice() else {
        return Err(ServerClockError::new(if dates.is_empty() {
            ServerClockErrorKind::MissingDate
        } else {
            ServerClockErrorKind::InvalidDate
        }));
    };
    let date = date
        .to_str()
        .map_err(|_| ServerClockError::new(ServerClockErrorKind::InvalidDate))?;
    let parsed = NaiveDateTime::parse_from_str(date, "%a, %d %b %Y %H:%M:%S GMT")
        .map_err(|_| ServerClockError::new(ServerClockErrorKind::InvalidDate))?
        .and_utc();
    let server = PinnedSnapshot::new(
        parsed.timestamp(),
        i32::try_from(parsed.timestamp_subsec_nanos())
            .map_err(|_| ServerClockError::new(ServerClockErrorKind::InvalidDate))?,
    )
    .map_err(|_| ServerClockError::new(ServerClockErrorKind::InvalidDate))?;
    server
        .checked_sub(HTTP_DATE_UNCERTAINTY.saturating_add(uncertainty))
        .map_err(|_| ServerClockError::new(ServerClockErrorKind::InvalidDate))
}

const fn retryable_http_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServerClockErrorKind {
    UntrustedEndpoint,
    Transport,
    MissingDate,
    InvalidDate,
    NonMonotonicClock,
}

pub(crate) struct ServerClockError {
    kind: ServerClockErrorKind,
    transport: Option<RestTransportError>,
}

impl ServerClockError {
    const fn new(kind: ServerClockErrorKind) -> Self {
        Self {
            kind,
            transport: None,
        }
    }

    fn transport(error: RestTransportError) -> Self {
        Self {
            kind: ServerClockErrorKind::Transport,
            transport: Some(error),
        }
    }

    const fn date_is_unusable(&self) -> bool {
        matches!(
            self.kind,
            ServerClockErrorKind::MissingDate | ServerClockErrorKind::InvalidDate
        )
    }
}

impl fmt::Display for ServerClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ServerClockErrorKind::UntrustedEndpoint => {
                "automatic snapshot server time requires the configured endpoint"
            }
            ServerClockErrorKind::Transport => "automatic snapshot server time request failed",
            ServerClockErrorKind::MissingDate => "automatic snapshot response omitted Date",
            ServerClockErrorKind::InvalidDate => {
                "automatic snapshot response contained an invalid Date"
            }
            ServerClockErrorKind::NonMonotonicClock => "monotonic clock moved backwards",
        })
    }
}

impl fmt::Debug for ServerClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerClockError")
            .field("kind", &self.kind)
            .field("transport", &self.transport.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Error for ServerClockError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.transport
            .as_ref()
            .map(|error| error as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http::{HeaderMap, HeaderValue, header::DATE};

    use super::*;
    use crate::transport::EndpointSet;

    #[test]
    fn date_header_is_pinned_before_measured_uncertainty() {
        let mut headers = HeaderMap::new();
        headers.insert(
            DATE,
            HeaderValue::from_static("Tue, 21 Jul 2026 10:06:07 GMT"),
        );

        let snapshot = snapshot_from_headers(
            &headers,
            Duration::from_secs(4),
            Duration::from_millis(4_250),
        )
        .unwrap();

        assert_eq!(snapshot.to_rfc3339(), "2026-07-21T10:06:05.750Z");
    }

    #[test]
    fn date_header_requires_one_strict_rfc_7231_value() {
        for values in [
            vec![],
            vec!["not-a-date"],
            vec!["Tuesday, 21-Jul-26 10:06:07 GMT"],
        ] {
            let mut headers = HeaderMap::new();
            for value in values {
                headers.append(DATE, HeaderValue::from_str(value).unwrap());
            }
            assert!(snapshot_from_headers(&headers, Duration::ZERO, Duration::ZERO).is_err());
        }

        let mut duplicate = HeaderMap::new();
        duplicate.append(
            DATE,
            HeaderValue::from_static("Tue, 21 Jul 2026 10:06:07 GMT"),
        );
        duplicate.append(
            DATE,
            HeaderValue::from_static("Tue, 21 Jul 2026 10:06:08 GMT"),
        );
        assert!(snapshot_from_headers(&duplicate, Duration::ZERO, Duration::ZERO).is_err());
    }

    #[test]
    fn clock_probe_trusts_distinct_defaults_and_one_explicit_override() {
        let defaults = EndpointSet::new(None, None).unwrap();
        let loopback = EndpointSet::new(None, Some("http://127.0.0.1:1234")).unwrap();
        let proxy = EndpointSet::new(None, Some("https://proxy.example.com")).unwrap();

        assert_ne!(defaults.storage(), defaults.bigquery());
        assert!(ServerClockProbe::endpoint_is_trusted(&defaults));
        assert_eq!(loopback.storage(), loopback.bigquery());
        assert!(ServerClockProbe::endpoint_is_trusted(&loopback));
        assert!(ServerClockProbe::endpoint_is_trusted(&proxy));
    }
}
