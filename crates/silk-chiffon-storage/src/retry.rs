use std::{collections::BTreeMap, time::Duration};

use clap::Args;
use thiserror::Error;

#[derive(Args, Clone, Debug)]
/// Clap arguments contributed once when any provider opts into shared retries.
pub struct RetryArgs {
    /// Maximum retries for one provider request.
    #[arg(long = "storage-max-retries", default_value_t = 10)]
    max_retries: usize,
    /// Total retry window for one provider request.
    #[arg(
        long = "storage-retry-timeout",
        default_value = "3m",
        value_parser = parse_duration
    )]
    retry_timeout: Duration,
    /// First delay before a retry.
    #[arg(
        long = "storage-initial-backoff",
        default_value = "100ms",
        value_parser = parse_duration
    )]
    initial_backoff: Duration,
    /// Maximum delay between retries.
    #[arg(
        long = "storage-max-backoff",
        default_value = "15s",
        value_parser = parse_duration
    )]
    max_backoff: Duration,
    /// Multiplier used by the provider retry policy.
    #[arg(long = "storage-backoff-base", default_value_t = 2.0)]
    backoff_base: f64,
}

#[derive(Clone, Debug, PartialEq)]
/// Validated retry settings passed to each participating provider callback.
pub struct RetryConfiguration {
    max_retries: usize,
    retry_timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    backoff_base: f64,
}

impl RetryConfiguration {
    pub const fn max_retries(&self) -> usize {
        self.max_retries
    }

    pub const fn retry_timeout(&self) -> Duration {
        self.retry_timeout
    }

    pub const fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    pub const fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    pub const fn backoff_base(&self) -> f64 {
        self.backoff_base
    }

    pub(crate) fn append_cache_configuration(&self, configuration: &mut BTreeMap<String, String>) {
        configuration.insert("retry.max-retries".to_owned(), self.max_retries.to_string());
        configuration.insert(
            "retry.timeout-nanos".to_owned(),
            self.retry_timeout.as_nanos().to_string(),
        );
        configuration.insert(
            "retry.initial-backoff-nanos".to_owned(),
            self.initial_backoff.as_nanos().to_string(),
        );
        configuration.insert(
            "retry.max-backoff-nanos".to_owned(),
            self.max_backoff.as_nanos().to_string(),
        );
        configuration.insert(
            "retry.backoff-base-bits".to_owned(),
            self.backoff_base.to_bits().to_string(),
        );
    }
}

impl TryFrom<RetryArgs> for RetryConfiguration {
    type Error = RetryConfigurationError;

    fn try_from(args: RetryArgs) -> Result<Self, Self::Error> {
        let configuration = Self {
            max_retries: args.max_retries,
            retry_timeout: args.retry_timeout,
            initial_backoff: args.initial_backoff,
            max_backoff: args.max_backoff,
            backoff_base: args.backoff_base,
        };

        if configuration.max_retries == 0 {
            return Ok(configuration);
        }
        if configuration.retry_timeout.is_zero() {
            return Err(RetryConfigurationError::ZeroRetryTimeout);
        }
        if configuration.initial_backoff.is_zero() {
            return Err(RetryConfigurationError::ZeroInitialBackoff);
        }
        if configuration.max_backoff.is_zero() {
            return Err(RetryConfigurationError::ZeroMaximumBackoff);
        }
        if !configuration.backoff_base.is_finite() {
            return Err(RetryConfigurationError::NonFiniteBackoffBase(
                configuration.backoff_base,
            ));
        }
        if configuration.backoff_base < 1.0 {
            return Err(RetryConfigurationError::BackoffBaseBelowOne(
                configuration.backoff_base,
            ));
        }
        if configuration.initial_backoff > configuration.max_backoff {
            return Err(RetryConfigurationError::InitialBackoffExceedsMaximum {
                initial: configuration.initial_backoff,
                maximum: configuration.max_backoff,
            });
        }

        Ok(configuration)
    }
}

#[derive(Debug, Error)]
pub enum RetryConfigurationError {
    #[error("storage retry timeout must be greater than zero when retries are enabled")]
    ZeroRetryTimeout,
    #[error("storage retry initial backoff must be greater than zero when retries are enabled")]
    ZeroInitialBackoff,
    #[error("storage retry maximum backoff must be greater than zero when retries are enabled")]
    ZeroMaximumBackoff,
    #[error("storage retry backoff base must be finite: {0}")]
    NonFiniteBackoffBase(f64),
    #[error("storage retry backoff base must be at least 1.0: {0}")]
    BackoffBaseBelowOne(f64),
    #[error("storage retry initial backoff {initial:?} exceeds maximum backoff {maximum:?}")]
    InitialBackoffExceedsMaximum {
        initial: Duration,
        maximum: Duration,
    },
}

fn parse_duration(input: &str) -> Result<Duration, String> {
    humantime::parse_duration(input).map_err(|error| error.to_string())
}
