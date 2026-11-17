use std::{fmt, time::Duration};

use anyhow::Result;
use clap::{Args, ValueEnum};
use thiserror::Error;
use url::{Host, Url};

use crate::resources::CommandResources;

const HELP_HEADING: &str = "BigQuery Storage Read";
const DEFAULT_MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Args, Clone)]
#[group(id = "bqs-input-args")]
pub(crate) struct BigQueryInputArgs {
    /// Override the project that owns Storage Read sessions.
    #[arg(
        id = "bqs-session-project",
        long = "bqs-session-project",
        value_name = "PROJECT",
        value_parser = clap::builder::NonEmptyStringValueParser::new(),
        help_heading = HELP_HEADING
    )]
    pub(crate) session_project: Option<String>,

    /// Override the project charged for API quota.
    #[arg(
        id = "bqs-quota-project",
        long = "bqs-quota-project",
        value_name = "PROJECT",
        value_parser = clap::builder::NonEmptyStringValueParser::new(),
        help_heading = HELP_HEADING
    )]
    pub(crate) quota_project: Option<String>,

    /// Override the BigQuery Storage Read API endpoint.
    #[arg(
        id = "bqs-endpoint",
        long = "bqs-endpoint",
        value_name = "URL",
        value_parser = parse_endpoint,
        conflicts_with = "bqs-universe-domain",
        help_heading = HELP_HEADING
    )]
    pub(crate) endpoint: Option<Url>,

    /// Override the Google Cloud universe domain.
    #[arg(
        id = "bqs-universe-domain",
        long = "bqs-universe-domain",
        value_name = "DOMAIN",
        value_parser = clap::builder::NonEmptyStringValueParser::new(),
        help_heading = HELP_HEADING
    )]
    pub(crate) universe_domain: Option<String>,

    /// AND this GoogleSQL predicate with every pushed DataFusion predicate.
    #[arg(
        id = "bqs-row-restriction",
        long = "bqs-row-restriction",
        value_name = "GOOGLESQL",
        value_parser = clap::builder::NonEmptyStringValueParser::new(),
        help_heading = HELP_HEADING
    )]
    pub(crate) row_restriction: Option<String>,

    /// Override the number of Storage Read streams requested from BigQuery.
    #[arg(
        id = "bqs-max-stream-count",
        long = "bqs-max-stream-count",
        value_name = "COUNT",
        value_parser = parse_stream_count,
        help_heading = HELP_HEADING
    )]
    pub(crate) max_stream_count: Option<u32>,

    /// Reject a serialized Storage Read response larger than this many bytes.
    #[arg(
        id = "bqs-max-response-bytes",
        long = "bqs-max-response-bytes",
        value_name = "BYTES",
        default_value_t = DEFAULT_MAX_RESPONSE_BYTES,
        value_parser = parse_positive_usize,
        help_heading = HELP_HEADING
    )]
    pub(crate) max_response_bytes: usize,

    /// Select native Arrow buffer compression on the wire.
    #[arg(
        id = "bqs-arrow-wire-compression",
        long = "bqs-arrow-wire-compression",
        value_enum,
        default_value_t = ArrowWireCompression::default(),
        help_heading = HELP_HEADING
    )]
    pub(crate) arrow_wire_compression: ArrowWireCompression,

    /// Select whole-response compression on the wire.
    #[arg(
        id = "bqs-response-compression",
        long = "bqs-response-compression",
        value_enum,
        default_value_t = ResponseCompression::default(),
        help_heading = HELP_HEADING
    )]
    pub(crate) response_compression: ResponseCompression,

    /// Select how picosecond timestamps are represented in Arrow.
    #[arg(
        id = "bqs-picos-timestamp-precision",
        long = "bqs-picos-timestamp-precision",
        value_enum,
        default_value_t = PicosTimestampPrecision::default(),
        help_heading = HELP_HEADING
    )]
    pub(crate) picos_timestamp_precision: PicosTimestampPrecision,

    /// Reconnect when an active ReadRows network wait remains idle this long.
    #[arg(
        id = "bqs-read-idle-timeout",
        long = "bqs-read-idle-timeout",
        value_name = "DURATION",
        default_value = "60s",
        value_parser = parse_positive_duration,
        help_heading = HELP_HEADING
    )]
    pub(crate) read_idle_timeout: Duration,

    /// Limit cumulative retry time for one ReadRows stream.
    #[arg(
        id = "bqs-read-retry-window",
        long = "bqs-read-retry-window",
        value_name = "DURATION",
        default_value = "24h",
        value_parser = parse_positive_duration,
        help_heading = HELP_HEADING
    )]
    pub(crate) read_retry_window: Duration,

    /// Set the first ReadRows retry backoff.
    #[arg(
        id = "bqs-read-retry-initial-backoff",
        long = "bqs-read-retry-initial-backoff",
        value_name = "DURATION",
        default_value = "100ms",
        value_parser = parse_positive_duration,
        help_heading = HELP_HEADING
    )]
    pub(crate) read_retry_initial_backoff: Duration,

    /// Cap ReadRows retry backoff.
    #[arg(
        id = "bqs-read-retry-max-backoff",
        long = "bqs-read-retry-max-backoff",
        value_name = "DURATION",
        default_value = "60s",
        value_parser = parse_positive_duration,
        help_heading = HELP_HEADING
    )]
    pub(crate) read_retry_max_backoff: Duration,

    #[arg(skip)]
    resources: tokio::sync::OnceCell<std::sync::Arc<CommandResources>>,
}

impl BigQueryInputArgs {
    pub(crate) fn validate(&self) -> Result<(), BigQueryInputArgsError> {
        if self.arrow_wire_compression != ArrowWireCompression::None
            && self.response_compression != ResponseCompression::None
        {
            return Err(BigQueryInputArgsError::ConflictingCompression);
        }
        if self.read_retry_initial_backoff > self.read_retry_max_backoff {
            return Err(BigQueryInputArgsError::BackoffOrder);
        }
        if self.read_idle_timeout >= self.read_retry_window {
            return Err(BigQueryInputArgsError::RetryWindow);
        }
        Ok(())
    }

    pub(crate) async fn resources(
        &self,
        session: &datafusion::prelude::SessionContext,
    ) -> Result<std::sync::Arc<CommandResources>> {
        self.resources
            .get_or_try_init(|| CommandResources::initialize(self, session))
            .await
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            session_project: None,
            quota_project: None,
            endpoint: None,
            universe_domain: None,
            row_restriction: None,
            max_stream_count: None,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            arrow_wire_compression: ArrowWireCompression::None,
            response_compression: ResponseCompression::None,
            picos_timestamp_precision: PicosTimestampPrecision::Micros,
            read_idle_timeout: Duration::from_secs(60),
            read_retry_window: Duration::from_secs(24 * 60 * 60),
            read_retry_initial_backoff: Duration::from_millis(100),
            read_retry_max_backoff: Duration::from_secs(60),
            resources: tokio::sync::OnceCell::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_test_resources(&self, resources: std::sync::Arc<CommandResources>) {
        self.resources
            .set(resources)
            .expect("test resources are installed only once");
    }
}

impl fmt::Debug for BigQueryInputArgs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BigQueryInputArgs")
            .field("has_session_project", &self.session_project.is_some())
            .field("has_quota_project", &self.quota_project.is_some())
            .field("has_endpoint", &self.endpoint.is_some())
            .field("has_universe_domain", &self.universe_domain.is_some())
            .field("has_row_restriction", &self.row_restriction.is_some())
            .field("max_stream_count", &self.max_stream_count)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("arrow_wire_compression", &self.arrow_wire_compression)
            .field("response_compression", &self.response_compression)
            .field("picos_timestamp_precision", &self.picos_timestamp_precision)
            .field("read_idle_timeout", &self.read_idle_timeout)
            .field("read_retry_window", &self.read_retry_window)
            .field(
                "read_retry_initial_backoff",
                &self.read_retry_initial_backoff,
            )
            .field("read_retry_max_backoff", &self.read_retry_max_backoff)
            .field("resources_initialized", &self.resources.initialized())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum BigQueryInputArgsError {
    #[error("native Arrow and response compression cannot both be requested")]
    ConflictingCompression,
    #[error("initial read retry backoff cannot exceed maximum read retry backoff")]
    BackoffOrder,
    #[error("read retry window must be greater than the active-network idle timeout")]
    RetryWindow,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lowercase")]
pub(crate) enum ArrowWireCompression {
    Lz4,
    Zstd,
    #[default]
    None,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lowercase")]
pub(crate) enum ResponseCompression {
    Lz4,
    #[default]
    None,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lowercase")]
pub(crate) enum PicosTimestampPrecision {
    #[default]
    Micros,
    Nanos,
    Picos,
}

fn parse_stream_count(value: &str) -> Result<u32> {
    let value = value.parse::<u32>()?;
    if value == 0 || value > i32::MAX as u32 {
        anyhow::bail!("value must be between 1 and {}", i32::MAX);
    }
    Ok(value)
}

fn parse_positive_usize(value: &str) -> Result<usize> {
    let value = value.parse::<usize>()?;
    if value == 0 {
        anyhow::bail!("value must be at least 1");
    }
    Ok(value)
}

fn parse_positive_duration(value: &str) -> Result<Duration> {
    let value = humantime::parse_duration(value)?;
    if value.is_zero() {
        anyhow::bail!("value must be greater than zero");
    }
    Ok(value)
}

fn parse_endpoint(value: &str) -> Result<Url, String> {
    let url = Url::parse(value).map_err(|_| "endpoint must be an absolute URL".to_owned())?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("endpoint cannot contain credentials, a query, or a fragment".to_owned());
    }
    match url.scheme() {
        "https" => {}
        "http" if endpoint_is_loopback(&url) => {}
        "http" => {
            return Err("HTTP endpoint overrides are limited to loopback hosts".to_owned());
        }
        _ => return Err("endpoint scheme must be HTTPS, or HTTP for loopback".to_owned()),
    }
    Ok(url)
}

fn endpoint_is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use clap::{Args, Command, FromArgMatches};

    use super::*;

    fn parse(arguments: &[&str]) -> Result<BigQueryInputArgs, clap::Error> {
        let command = BigQueryInputArgs::augment_args(Command::new("test"));
        let matches = command
            .try_get_matches_from(std::iter::once("test").chain(arguments.iter().copied()))?;
        BigQueryInputArgs::from_arg_matches(&matches)
    }

    #[test]
    fn preserves_every_supported_wire_compression_mode() {
        assert_eq!(
            parse(&["--bqs-arrow-wire-compression", "zstd"])
                .unwrap()
                .arrow_wire_compression,
            ArrowWireCompression::Zstd
        );
        assert_eq!(
            parse(&["--bqs-response-compression", "lz4"])
                .unwrap()
                .response_compression,
            ResponseCompression::Lz4
        );
    }

    #[test]
    fn compression_conflict_depends_on_selected_modes_not_argument_presence() {
        parse(&[
            "--bqs-arrow-wire-compression",
            "none",
            "--bqs-response-compression",
            "none",
        ])
        .unwrap()
        .validate()
        .unwrap();
        parse(&[
            "--bqs-arrow-wire-compression",
            "zstd",
            "--bqs-response-compression",
            "none",
        ])
        .unwrap()
        .validate()
        .unwrap();

        let error = parse(&[
            "--bqs-arrow-wire-compression",
            "lz4",
            "--bqs-response-compression",
            "lz4",
        ])
        .unwrap()
        .validate()
        .unwrap_err();
        assert!(error.to_string().contains("cannot both"));
    }

    #[test]
    fn endpoint_parser_rejects_credential_and_boundary_ambiguity() {
        for endpoint in [
            "http://example.com",
            "https://user@example.com",
            "https://user:password@example.com",
            "https://example.com?query=value",
            "https://example.com#fragment",
            "ftp://example.com",
        ] {
            assert!(
                parse(&["--bqs-endpoint", endpoint]).is_err(),
                "{endpoint:?} should be rejected"
            );
        }
        parse(&["--bqs-endpoint", "http://127.0.0.1:1234"]).unwrap();
        parse(&["--bqs-endpoint", "https://example.com"]).unwrap();
    }

    #[test]
    fn debug_output_redacts_policy_and_endpoint_values() {
        let args = parse(&[
            "--bqs-session-project",
            "session-sentinel",
            "--bqs-quota-project",
            "quota-sentinel",
            "--bqs-endpoint",
            "https://endpoint-sentinel.example",
            "--bqs-row-restriction",
            "predicate_sentinel = 'secret'",
        ])
        .unwrap();
        let debug = format!("{args:?}");

        for sentinel in [
            "session-sentinel",
            "quota-sentinel",
            "endpoint-sentinel",
            "predicate_sentinel",
            "secret",
        ] {
            assert!(!debug.contains(sentinel));
        }
    }
}
