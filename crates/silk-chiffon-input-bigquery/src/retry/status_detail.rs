use std::{fmt, time::Duration};

use prost::Message;
use tonic::Status;

#[cfg(test)]
use crate::proto::Any;
use crate::proto::{
    bigquery_storage::{StorageError, storage_error::StorageErrorCode},
    rpc::{RetryInfo, Status as RpcStatus},
};

const RETRY_INFO_URL: &str = "type.googleapis.com/google.rpc.RetryInfo";
const STORAGE_ERROR_URL: &str = "type.googleapis.com/google.cloud.bigquery.storage.v1.StorageError";
const PROTO_DURATION_MAX_SECONDS: i64 = 315_576_000_000;

#[derive(Clone)]
pub(crate) struct StatusDetails {
    status_code: tonic::Code,
    retry_delay: Option<Duration>,
    storage_error_code: Option<StorageErrorCode>,
    #[cfg(test)]
    unknown_details: Vec<Any>,
}

impl StatusDetails {
    pub(crate) fn parse(status: &Status) -> Result<Self, StatusDetailError> {
        if status.details().is_empty() {
            return Ok(Self {
                status_code: status.code(),
                retry_delay: None,
                storage_error_code: None,
                #[cfg(test)]
                unknown_details: Vec::new(),
            });
        }
        let envelope = RpcStatus::decode(status.details())
            .map_err(|_| StatusDetailError::new(StatusDetailErrorKind::MalformedEnvelope))?;
        if envelope.code != status.code() as i32 {
            return Err(StatusDetailError::new(StatusDetailErrorKind::CodeMismatch));
        }

        let mut retry_delay = None;
        let mut storage_error_code = None;
        #[cfg(test)]
        let mut unknown_details = Vec::new();
        for detail in envelope.details {
            validate_type_url(&detail.type_url)?;
            match detail.type_url.as_str() {
                RETRY_INFO_URL => {
                    if retry_delay.is_some() {
                        return Err(StatusDetailError::new(
                            StatusDetailErrorKind::DuplicateRetryInfo,
                        ));
                    }
                    let retry = RetryInfo::decode(detail.value.as_slice()).map_err(|_| {
                        StatusDetailError::new(StatusDetailErrorKind::MalformedRetryInfo)
                    })?;
                    let duration = retry.retry_delay.ok_or_else(|| {
                        StatusDetailError::new(StatusDetailErrorKind::MissingRetryDelay)
                    })?;
                    retry_delay = Some(retry_duration(duration.seconds, duration.nanos)?);
                }
                STORAGE_ERROR_URL => {
                    if storage_error_code.is_some() {
                        return Err(StatusDetailError::new(
                            StatusDetailErrorKind::DuplicateStorageError,
                        ));
                    }
                    let storage = StorageError::decode(detail.value.as_slice()).map_err(|_| {
                        StatusDetailError::new(StatusDetailErrorKind::MalformedStorageError)
                    })?;
                    let code = StorageErrorCode::try_from(storage.code).map_err(|_| {
                        StatusDetailError::new(StatusDetailErrorKind::MalformedStorageError)
                    })?;
                    if code == StorageErrorCode::Unspecified {
                        return Err(StatusDetailError::new(
                            StatusDetailErrorKind::MalformedStorageError,
                        ));
                    }
                    storage_error_code = Some(code);
                }
                _ => {
                    #[cfg(test)]
                    unknown_details.push(detail);
                }
            }
        }
        if retry_delay.is_some() && storage_error_code.is_some() {
            return Err(StatusDetailError::new(
                StatusDetailErrorKind::ConflictingDetails,
            ));
        }
        Ok(Self {
            status_code: status.code(),
            retry_delay,
            storage_error_code,
            #[cfg(test)]
            unknown_details,
        })
    }

    pub(crate) const fn retry_delay(&self) -> Option<Duration> {
        self.retry_delay
    }

    pub(crate) const fn storage_error_code(&self) -> Option<StorageErrorCode> {
        self.storage_error_code
    }

    #[cfg(test)]
    pub(crate) fn unknown_details(&self) -> &[Any] {
        &self.unknown_details
    }
}

impl fmt::Debug for StatusDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatusDetails")
            .field("status_code", &self.status_code)
            .field("retry_delay", &self.retry_delay)
            .finish_non_exhaustive()
    }
}

fn validate_type_url(type_url: &str) -> Result<(), StatusDetailError> {
    let Some((prefix, name)) = type_url.rsplit_once('/') else {
        return Err(StatusDetailError::new(
            StatusDetailErrorKind::MalformedTypeUrl,
        ));
    };
    if prefix.is_empty()
        || name.is_empty()
        || !name.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
    {
        return Err(StatusDetailError::new(
            StatusDetailErrorKind::MalformedTypeUrl,
        ));
    }
    Ok(())
}

fn retry_duration(seconds: i64, nanos: i32) -> Result<Duration, StatusDetailError> {
    if !(0..=PROTO_DURATION_MAX_SECONDS).contains(&seconds) || !(0..1_000_000_000).contains(&nanos)
    {
        return Err(StatusDetailError::new(
            StatusDetailErrorKind::InvalidRetryDelay,
        ));
    }
    Ok(Duration::new(
        u64::try_from(seconds)
            .map_err(|_| StatusDetailError::new(StatusDetailErrorKind::InvalidRetryDelay))?,
        u32::try_from(nanos)
            .map_err(|_| StatusDetailError::new(StatusDetailErrorKind::InvalidRetryDelay))?,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatusDetailErrorKind {
    MalformedEnvelope,
    CodeMismatch,
    MalformedTypeUrl,
    MalformedRetryInfo,
    MissingRetryDelay,
    InvalidRetryDelay,
    DuplicateRetryInfo,
    MalformedStorageError,
    DuplicateStorageError,
    ConflictingDetails,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct StatusDetailError {
    kind: StatusDetailErrorKind,
}

impl StatusDetailError {
    const fn new(kind: StatusDetailErrorKind) -> Self {
        Self { kind }
    }

    #[cfg(test)]
    pub(crate) const fn kind(self) -> StatusDetailErrorKind {
        self.kind
    }
}

impl fmt::Display for StatusDetailError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            StatusDetailErrorKind::MalformedEnvelope => "malformed structured gRPC status",
            StatusDetailErrorKind::CodeMismatch => "structured gRPC status code mismatch",
            StatusDetailErrorKind::MalformedTypeUrl => "malformed gRPC status detail type URL",
            StatusDetailErrorKind::MalformedRetryInfo => "malformed RetryInfo detail",
            StatusDetailErrorKind::MissingRetryDelay => "RetryInfo omitted its delay",
            StatusDetailErrorKind::InvalidRetryDelay => "RetryInfo contained an invalid delay",
            StatusDetailErrorKind::DuplicateRetryInfo => "gRPC status repeated RetryInfo",
            StatusDetailErrorKind::MalformedStorageError => "malformed StorageError detail",
            StatusDetailErrorKind::DuplicateStorageError => "gRPC status repeated StorageError",
            StatusDetailErrorKind::ConflictingDetails => {
                "gRPC status contained conflicting retry details"
            }
        })
    }
}

impl fmt::Debug for StatusDetailError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatusDetailError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for StatusDetailError {}
