//! Typed retry evidence and deadline planning for BigQuery Storage Read RPCs.

mod backoff;
mod budget;
mod status_detail;

use std::{error::Error, fmt, time::Duration};

use tonic::{Code, Status};

use crate::proto::bigquery_storage::storage_error::StorageErrorCode;

pub(crate) use backoff::BackoffPolicy;
pub(crate) use budget::{RetryBudget, RetryDelay};
#[cfg(test)]
pub(crate) use budget::{RetryBudgetErrorKind, SESSION_EXPIRY_SAFETY_MARGIN};
#[cfg(test)]
pub(crate) use status_detail::StatusDetailErrorKind;
pub(crate) use status_detail::StatusDetails;

#[tonic::async_trait]
pub(crate) trait Sleeper: Send + Sync + fmt::Debug {
    async fn sleep(&self, duration: Duration);
}

pub(crate) trait JitterSource: Send + Sync + fmt::Debug {
    fn sample(&self) -> u64;
}

#[derive(Debug)]
pub(crate) struct TokioSleeper;

#[tonic::async_trait]
impl Sleeper for TokioSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Debug)]
pub(crate) struct ThreadJitter;

impl JitterSource for ThreadJitter {
    fn sample(&self) -> u64 {
        rand::random()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RetryScope {
    CreateReadSession,
    ReadRows,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TransportEvidence {
    ChannelClosed,
    IncompleteMessage,
}

impl TransportEvidence {
    #[cfg(test)]
    pub(crate) const fn provenance(self) -> &'static str {
        match self {
            Self::ChannelClosed => "tonic-0.14.6/hyper-1.x:is_closed",
            Self::IncompleteMessage => "tonic-0.14.6/hyper-1.x:is_incomplete_message",
        }
    }

    pub(crate) fn from_status(status: &Status) -> Option<Self> {
        let mut source = status.source();
        while let Some(error) = source {
            if let Some(error) = error.downcast_ref::<hyper::Error>() {
                if error.is_closed() {
                    return Some(Self::ChannelClosed);
                }
                if error.is_incomplete_message() {
                    return Some(Self::IncompleteMessage);
                }
            }
            source = error.source();
        }
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RetryReason {
    Credentials,
    GrpcDeadlineExceeded,
    GrpcUnavailable,
    GrpcResourceExhausted,
    GrpcStatus(Code),
    Transport(TransportEvidence),
    MalformedStatusDetails,
    IdleTimeout,
    SessionExpired,
    LostSessionState,
    RetryBudgetExhausted,
    Schema,
    Decode,
    OffsetOverflow,
    LocalResource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryDecision {
    Retry {
        scope: RetryScope,
        reason: RetryReason,
        server_delay: Option<Duration>,
    },
    Permanent {
        scope: RetryScope,
        reason: RetryReason,
    },
}

impl RetryDecision {
    pub(crate) const fn retry(
        scope: RetryScope,
        reason: RetryReason,
        server_delay: Option<Duration>,
    ) -> Self {
        Self::Retry {
            scope,
            reason,
            server_delay,
        }
    }

    pub(crate) const fn permanent(scope: RetryScope, reason: RetryReason) -> Self {
        Self::Permanent { scope, reason }
    }

    #[cfg(test)]
    pub(crate) const fn is_retry(self) -> bool {
        matches!(self, Self::Retry { .. })
    }

    #[cfg(test)]
    pub(crate) const fn is_permanent(self) -> bool {
        matches!(self, Self::Permanent { .. })
    }

    pub(crate) const fn reason(self) -> RetryReason {
        match self {
            Self::Retry { reason, .. } | Self::Permanent { reason, .. } => reason,
        }
    }

    #[cfg(test)]
    pub(crate) const fn retry_delay(self) -> Option<Duration> {
        match self {
            Self::Retry { server_delay, .. } => server_delay,
            Self::Permanent { .. } => None,
        }
    }

    pub(crate) fn delay(self, backoff: Duration) -> Option<RetryDelay> {
        match self {
            Self::Retry {
                server_delay: Some(delay),
                ..
            } => Some(RetryDelay::ServerMinimum(delay.max(backoff))),
            Self::Retry {
                server_delay: None, ..
            } => Some(RetryDelay::Backoff(backoff)),
            Self::Permanent { .. } => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CreateReadSessionFailure<'a> {
    Status(&'a Status),
    TransientCredentials,
    Credentials,
    LocalResource,
}

pub(crate) fn classify_create_read_session(failure: CreateReadSessionFailure<'_>) -> RetryDecision {
    match failure {
        CreateReadSessionFailure::TransientCredentials => RetryDecision::retry(
            RetryScope::CreateReadSession,
            RetryReason::Credentials,
            None,
        ),
        CreateReadSessionFailure::Credentials => {
            RetryDecision::permanent(RetryScope::CreateReadSession, RetryReason::Credentials)
        }
        CreateReadSessionFailure::LocalResource => {
            RetryDecision::permanent(RetryScope::CreateReadSession, RetryReason::LocalResource)
        }
        CreateReadSessionFailure::Status(status) => classify_create_status(status),
    }
}

fn classify_create_status(status: &Status) -> RetryDecision {
    let details = match StatusDetails::parse(status) {
        Ok(details) => details,
        Err(_) => {
            return RetryDecision::permanent(
                RetryScope::CreateReadSession,
                RetryReason::MalformedStatusDetails,
            );
        }
    };
    match status.code() {
        Code::DeadlineExceeded => RetryDecision::retry(
            RetryScope::CreateReadSession,
            RetryReason::GrpcDeadlineExceeded,
            details.retry_delay(),
        ),
        Code::Unavailable => RetryDecision::retry(
            RetryScope::CreateReadSession,
            RetryReason::GrpcUnavailable,
            details.retry_delay(),
        ),
        code => {
            RetryDecision::permanent(RetryScope::CreateReadSession, RetryReason::GrpcStatus(code))
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ReadRowsFailure<'a> {
    Status(&'a Status),
    #[cfg(test)]
    Transport(TransportEvidence),
    TransientCredentials,
    Credentials,
    IdleTimeout,
    SessionExpired,
    #[cfg(test)]
    RetriesExhausted,
    #[cfg(test)]
    Schema,
    Decode,
    #[cfg(test)]
    OffsetOverflow,
    LocalResource,
}

pub(crate) fn classify_read_rows(failure: ReadRowsFailure<'_>) -> RetryDecision {
    match failure {
        ReadRowsFailure::Status(status) => classify_read_rows_status(status),
        #[cfg(test)]
        ReadRowsFailure::Transport(evidence) => {
            RetryDecision::retry(RetryScope::ReadRows, RetryReason::Transport(evidence), None)
        }
        ReadRowsFailure::TransientCredentials => {
            RetryDecision::retry(RetryScope::ReadRows, RetryReason::Credentials, None)
        }
        ReadRowsFailure::Credentials => {
            RetryDecision::permanent(RetryScope::ReadRows, RetryReason::Credentials)
        }
        ReadRowsFailure::IdleTimeout => {
            RetryDecision::retry(RetryScope::ReadRows, RetryReason::IdleTimeout, None)
        }
        ReadRowsFailure::SessionExpired => {
            RetryDecision::permanent(RetryScope::ReadRows, RetryReason::SessionExpired)
        }
        #[cfg(test)]
        ReadRowsFailure::RetriesExhausted => {
            RetryDecision::permanent(RetryScope::ReadRows, RetryReason::RetryBudgetExhausted)
        }
        #[cfg(test)]
        ReadRowsFailure::Schema => {
            RetryDecision::permanent(RetryScope::ReadRows, RetryReason::Schema)
        }
        ReadRowsFailure::Decode => {
            RetryDecision::permanent(RetryScope::ReadRows, RetryReason::Decode)
        }
        #[cfg(test)]
        ReadRowsFailure::OffsetOverflow => {
            RetryDecision::permanent(RetryScope::ReadRows, RetryReason::OffsetOverflow)
        }
        ReadRowsFailure::LocalResource => {
            RetryDecision::permanent(RetryScope::ReadRows, RetryReason::LocalResource)
        }
    }
}

fn classify_read_rows_status(status: &Status) -> RetryDecision {
    let details = match StatusDetails::parse(status) {
        Ok(details) => details,
        Err(_) => {
            return RetryDecision::permanent(
                RetryScope::ReadRows,
                RetryReason::MalformedStatusDetails,
            );
        }
    };
    if details.storage_error_code() == Some(StorageErrorCode::StreamNotFound) {
        return RetryDecision::permanent(RetryScope::ReadRows, RetryReason::LostSessionState);
    }
    if details.storage_error_code().is_some() {
        return RetryDecision::permanent(
            RetryScope::ReadRows,
            RetryReason::GrpcStatus(status.code()),
        );
    }
    if let Some(evidence) = TransportEvidence::from_status(status) {
        return RetryDecision::retry(RetryScope::ReadRows, RetryReason::Transport(evidence), None);
    }
    match status.code() {
        Code::DeadlineExceeded => RetryDecision::retry(
            RetryScope::ReadRows,
            RetryReason::GrpcDeadlineExceeded,
            details.retry_delay(),
        ),
        Code::Unavailable => RetryDecision::retry(
            RetryScope::ReadRows,
            RetryReason::GrpcUnavailable,
            details.retry_delay(),
        ),
        Code::ResourceExhausted => match details.retry_delay() {
            Some(delay) => RetryDecision::retry(
                RetryScope::ReadRows,
                RetryReason::GrpcResourceExhausted,
                Some(delay),
            ),
            None => RetryDecision::permanent(
                RetryScope::ReadRows,
                RetryReason::GrpcStatus(Code::ResourceExhausted),
            ),
        },
        code => RetryDecision::permanent(RetryScope::ReadRows, RetryReason::GrpcStatus(code)),
    }
}

#[cfg(test)]
mod tests;
