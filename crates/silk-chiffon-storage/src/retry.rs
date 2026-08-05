//! Shared retry arguments for storage providers.
//!
//! A provider opts into these settings with
//! [`crate::StorageProviderRegistrationBuilder::shared_retries`]. The registry contributes the
//! arguments once, validates them when command matches are bound, and passes the resulting
//! [`RetryConfig`] only to participating provider resolvers. Each participating provider is
//! responsible for applying the configuration to its object-store client.

use std::time::Duration;

use clap::Args;
use object_store::{BackoffConfig, RetryConfig};
use thiserror::Error;

/// Clap arguments contributed once when any provider opts into shared retries.
///
/// Setting `--storage-max-retries=0` disables retries and skips all timing and multiplier
/// validation. When retries are enabled, [`Self::into_retry_config`] enforces this crate's shared
/// retry policy.
#[derive(Args, Clone, Debug)]
pub struct RetryArgs {
    /// Maximum retries for one provider request.
    #[arg(long = "storage-max-retries", default_value_t = 10)]
    max_retries: usize,
    /// Elapsed-time limit checked after each failed attempt, measured from the initial request.
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

impl RetryArgs {
    /// Converts the parsed arguments into an `object_store` retry configuration.
    ///
    /// # Errors
    ///
    /// Returns [`RetryConfigurationError`] when retries are enabled and a duration is zero, the
    /// initial delay exceeds the maximum, or the multiplier is non-finite or no greater than one.
    pub fn into_retry_config(self) -> Result<RetryConfig, RetryConfigurationError> {
        if self.max_retries == 0 {
            return Ok(RetryConfig {
                max_retries: self.max_retries,
                retry_timeout: self.retry_timeout,
                backoff: BackoffConfig {
                    init_backoff: self.initial_backoff,
                    max_backoff: self.max_backoff,
                    base: self.backoff_base,
                },
            });
        }
        if self.retry_timeout.is_zero() {
            return Err(RetryConfigurationError::ZeroRetryTimeout);
        }
        if self.initial_backoff.is_zero() {
            return Err(RetryConfigurationError::ZeroInitialBackoff);
        }
        if self.max_backoff.is_zero() {
            return Err(RetryConfigurationError::ZeroMaximumBackoff);
        }
        if !self.backoff_base.is_finite() {
            return Err(RetryConfigurationError::NonFiniteBackoffBase(
                self.backoff_base,
            ));
        }
        if self.backoff_base <= 1.0 {
            return Err(RetryConfigurationError::BackoffBaseNotGreaterThanOne(
                self.backoff_base,
            ));
        }
        if self.initial_backoff > self.max_backoff {
            return Err(RetryConfigurationError::InitialBackoffExceedsMaximum {
                initial: self.initial_backoff,
                maximum: self.max_backoff,
            });
        }

        Ok(RetryConfig {
            max_retries: self.max_retries,
            retry_timeout: self.retry_timeout,
            backoff: BackoffConfig {
                init_backoff: self.initial_backoff,
                max_backoff: self.max_backoff,
                base: self.backoff_base,
            },
        })
    }
}

/// Invalid combinations of shared storage retry arguments.
#[derive(Debug, Error)]
pub enum RetryConfigurationError {
    /// The total retry window is zero while retries are enabled.
    #[error("storage retry timeout must be greater than zero when retries are enabled")]
    ZeroRetryTimeout,
    /// The first retry delay is zero while retries are enabled.
    #[error("storage retry initial backoff must be greater than zero when retries are enabled")]
    ZeroInitialBackoff,
    /// The maximum delay between retries is zero while retries are enabled.
    #[error("storage retry maximum backoff must be greater than zero when retries are enabled")]
    ZeroMaximumBackoff,
    /// The exponential backoff multiplier is NaN or infinite.
    #[error("storage retry backoff base must be finite: {0}")]
    NonFiniteBackoffBase(f64),
    /// The exponential backoff multiplier is no greater than one.
    #[error("storage retry backoff base must be greater than 1.0: {0}")]
    BackoffBaseNotGreaterThanOne(f64),
    /// The first retry delay exceeds the configured maximum delay.
    #[error("storage retry initial backoff {initial:?} exceeds maximum backoff {maximum:?}")]
    InitialBackoffExceedsMaximum {
        /// The configured first retry delay.
        initial: Duration,
        /// The configured maximum delay between retries.
        maximum: Duration,
    },
}

fn parse_duration(input: &str) -> Result<Duration, String> {
    humantime::parse_duration(input).map_err(|error| error.to_string())
}
