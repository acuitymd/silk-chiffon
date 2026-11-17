use std::{fmt, time::Duration};

use prost::Message;
use tonic::{Code, Status};

use super::*;
use crate::proto::{
    Any,
    bigquery_storage::{StorageError, storage_error::StorageErrorCode},
    rpc::{RetryInfo, Status as RpcStatus},
};
use prost_types::Duration as ProtoDuration;

const RETRY_INFO_URL: &str = "type.googleapis.com/google.rpc.RetryInfo";
const STORAGE_ERROR_URL: &str = "type.googleapis.com/google.cloud.bigquery.storage.v1.StorageError";

fn detail(type_url: &str, message: &impl Message) -> Any {
    Any {
        type_url: type_url.to_owned(),
        value: message.encode_to_vec(),
    }
}

fn status(code: Code, details: Vec<Any>) -> Status {
    Status::with_details(
        code,
        "provider-message-sentinel",
        RpcStatus {
            code: code as i32,
            message: "provider-detail-message-sentinel".to_owned(),
            details,
        }
        .encode_to_vec()
        .into(),
    )
}

fn retry_info(seconds: i64, nanos: i32) -> Any {
    detail(
        RETRY_INFO_URL,
        &RetryInfo {
            retry_delay: Some(ProtoDuration { seconds, nanos }),
        },
    )
}

fn storage_error(code: StorageErrorCode) -> Any {
    detail(
        STORAGE_ERROR_URL,
        &StorageError {
            code: code as i32,
            entity: "secret-stream-name".to_owned(),
            error_message: "provider-storage-message-sentinel".to_owned(),
        },
    )
}

#[test]
fn create_read_session_retries_only_pinned_typed_failures() {
    for code in [Code::DeadlineExceeded, Code::Unavailable] {
        assert!(
            classify_create_read_session(CreateReadSessionFailure::Status(&Status::new(
                code, "secret"
            )))
            .is_retry()
        );
    }
    assert!(
        classify_create_read_session(CreateReadSessionFailure::TransientCredentials).is_retry()
    );
    for code in [
        Code::Unknown,
        Code::Internal,
        Code::ResourceExhausted,
        Code::Unauthenticated,
        Code::PermissionDenied,
        Code::InvalidArgument,
        Code::NotFound,
        Code::DataLoss,
    ] {
        assert!(
            classify_create_read_session(CreateReadSessionFailure::Status(&Status::new(
                code, "secret"
            )))
            .is_permanent()
        );
    }
    assert!(classify_create_read_session(CreateReadSessionFailure::Credentials).is_permanent());
    assert!(classify_create_read_session(CreateReadSessionFailure::LocalResource).is_permanent());

    let with_delay = status(Code::Unavailable, vec![retry_info(5, 0)]);
    assert_eq!(
        classify_create_read_session(CreateReadSessionFailure::Status(&with_delay)).retry_delay(),
        Some(Duration::from_secs(5))
    );
}

#[test]
fn read_rows_requires_typed_transient_evidence() {
    for failure in [
        ReadRowsFailure::Status(&Status::unavailable("secret")),
        ReadRowsFailure::Transport(TransportEvidence::ChannelClosed),
        ReadRowsFailure::Transport(TransportEvidence::IncompleteMessage),
        ReadRowsFailure::TransientCredentials,
        ReadRowsFailure::IdleTimeout,
    ] {
        assert!(classify_read_rows(failure).is_retry());
    }

    let throttled = status(Code::ResourceExhausted, vec![retry_info(3, 0)]);
    let decision = classify_read_rows(ReadRowsFailure::Status(&throttled));
    assert_eq!(decision.reason(), RetryReason::GrpcResourceExhausted);
    assert_eq!(decision.retry_delay(), Some(Duration::from_secs(3)));
    assert_eq!(
        decision.delay(Duration::from_secs(9)),
        Some(RetryDelay::ServerMinimum(Duration::from_secs(9)))
    );

    for code in [
        Code::Unknown,
        Code::Internal,
        Code::Unauthenticated,
        Code::PermissionDenied,
        Code::InvalidArgument,
        Code::NotFound,
        Code::DataLoss,
    ] {
        assert!(
            classify_read_rows(ReadRowsFailure::Status(&Status::new(
                code,
                "broad-text-signature-sentinel"
            )))
            .is_permanent()
        );
    }
    assert!(
        classify_read_rows(ReadRowsFailure::Status(&Status::resource_exhausted(
            "secret"
        )))
        .is_permanent()
    );
}

#[test]
fn lost_expired_and_exhausted_sessions_are_terminal() {
    let lost = status(
        Code::NotFound,
        vec![storage_error(StorageErrorCode::StreamNotFound)],
    );
    let cases = [
        classify_read_rows(ReadRowsFailure::Status(&lost)),
        classify_read_rows(ReadRowsFailure::SessionExpired),
        classify_read_rows(ReadRowsFailure::RetriesExhausted),
    ];
    assert!(cases.into_iter().all(RetryDecision::is_permanent));
    assert_eq!(cases[0].reason(), RetryReason::LostSessionState);
    assert_eq!(cases[1].reason(), RetryReason::SessionExpired);
    assert_eq!(cases[2].reason(), RetryReason::RetryBudgetExhausted);

    let source = include_str!("mod.rs");
    assert!(!source.contains(concat!("Replace", "Session")));
    assert!(!source.contains(concat!("Session", "Replacement")));
}

#[test]
fn local_schema_decode_offset_and_resource_failures_are_terminal() {
    for failure in [
        ReadRowsFailure::Credentials,
        ReadRowsFailure::Schema,
        ReadRowsFailure::Decode,
        ReadRowsFailure::OffsetOverflow,
        ReadRowsFailure::LocalResource,
    ] {
        assert!(classify_read_rows(failure).is_permanent());
    }
}

#[test]
fn status_details_are_strict_and_redacted() {
    let unknown = Any {
        type_url: "type.googleapis.com/example.private.Opaque".to_owned(),
        value: b"unknown-detail-payload-sentinel".to_vec(),
    };
    let structured = status(
        Code::ResourceExhausted,
        vec![unknown.clone(), retry_info(2, 500_000_000)],
    );
    let parsed = StatusDetails::parse(&structured).unwrap();
    assert_eq!(parsed.retry_delay(), Some(Duration::from_millis(2_500)));
    assert_eq!(parsed.unknown_details(), &[unknown]);
    let rendered = format!("{parsed:?}");
    assert!(!rendered.contains("unknown-detail-payload-sentinel"));
    assert!(!rendered.contains("provider-detail-message-sentinel"));

    let malformed = Status::with_details(Code::Unavailable, "secret", vec![0xff].into());
    assert_eq!(
        StatusDetails::parse(&malformed).unwrap_err().kind(),
        StatusDetailErrorKind::MalformedEnvelope
    );
    let missing = detail(RETRY_INFO_URL, &RetryInfo { retry_delay: None });
    assert_eq!(
        StatusDetails::parse(&status(Code::ResourceExhausted, vec![missing]))
            .unwrap_err()
            .kind(),
        StatusDetailErrorKind::MissingRetryDelay
    );
}

#[test]
fn status_details_reject_duplicate_conflicting_and_invalid_delays() {
    let cases = [
        (
            status(
                Code::ResourceExhausted,
                vec![retry_info(1, 0), retry_info(2, 0)],
            ),
            StatusDetailErrorKind::DuplicateRetryInfo,
        ),
        (
            status(
                Code::ResourceExhausted,
                vec![
                    retry_info(1, 0),
                    storage_error(StorageErrorCode::StreamNotFound),
                ],
            ),
            StatusDetailErrorKind::ConflictingDetails,
        ),
        (
            status(Code::ResourceExhausted, vec![retry_info(-1, 0)]),
            StatusDetailErrorKind::InvalidRetryDelay,
        ),
        (
            status(Code::ResourceExhausted, vec![retry_info(0, 1_000_000_000)]),
            StatusDetailErrorKind::InvalidRetryDelay,
        ),
    ];
    for (status, kind) in cases {
        assert_eq!(StatusDetails::parse(&status).unwrap_err().kind(), kind);
    }
}

#[test]
fn backoff_is_fixed_full_jitter_and_resets_only_after_sustained_acceptance() {
    let policy = BackoffPolicy::create_read_session();
    assert_eq!(policy.initial_delay(), Duration::from_millis(100));
    assert_eq!(policy.multiplier(), (13, 10));
    assert_eq!(policy.maximum_delay(), Duration::from_secs(60));
    assert_eq!(policy.delay_cap(1).unwrap(), Duration::from_millis(100));
    assert_eq!(policy.delay_cap(2).unwrap(), Duration::from_millis(130));
    assert_eq!(policy.delay_cap(3).unwrap(), Duration::from_millis(169));
    assert_eq!(policy.delay_cap(u32::MAX).unwrap(), Duration::from_secs(60));
    assert_eq!(policy.full_jitter(1, 0).unwrap(), Duration::ZERO);
    assert_eq!(
        policy.full_jitter(1, u64::MAX).unwrap(),
        Duration::from_millis(100)
    );

    let read =
        BackoffPolicy::read_rows(Duration::from_millis(100), Duration::from_secs(60)).unwrap();
    let threshold = read.sustained_accepted_progress();
    assert_eq!(
        read.failure_streak_after_progress(7, 1, threshold - Duration::from_nanos(1)),
        7
    );
    assert_eq!(read.failure_streak_after_progress(7, 1, threshold), 0);
    assert_eq!(read.failure_streak_after_progress(7, 0, threshold), 7);
}

#[test]
fn retry_budget_uses_retry_window_and_session_expiry_minus_safety_margin() {
    let budget = RetryBudget::read_rows(
        Duration::ZERO,
        Duration::from_secs(100),
        u32::MAX,
        Duration::from_secs(10),
        Some(Duration::from_secs(75)),
    )
    .unwrap();
    assert_eq!(SESSION_EXPIRY_SAFETY_MARGIN, Duration::from_secs(60));
    assert_eq!(budget.effective_deadline(), Duration::from_secs(15));
    let plan = budget
        .plan_retry(
            Duration::from_secs(1),
            Duration::from_secs(2),
            1,
            RetryDelay::Backoff(Duration::from_secs(20)),
        )
        .unwrap();
    assert_eq!(plan.scope(), RetryScope::ReadRows);
    assert_eq!(plan.attempt(), 2);
    assert_eq!(plan.delay(), Duration::from_secs(3));
    assert_eq!(plan.attempt_timeout(), Duration::from_secs(10));
    assert_eq!(plan.attempt_deadline(), Duration::from_secs(15));
}

#[test]
fn create_budget_is_fixed_ten_minutes_with_sixty_second_attempts() {
    let budget = RetryBudget::create_read_session(Duration::ZERO, Duration::from_secs(60)).unwrap();
    assert_eq!(budget.scope(), RetryScope::CreateReadSession);
    assert_eq!(budget.effective_deadline(), Duration::from_secs(600));
    assert_eq!(budget.attempt_timeout(), Duration::from_secs(60));

    let error = budget
        .plan_retry(
            Duration::from_secs(549),
            Duration::from_secs(550),
            1,
            RetryDelay::ServerMinimum(Duration::from_secs(1)),
        )
        .unwrap_err();
    assert_eq!(error.kind(), RetryBudgetErrorKind::DeadlineExhausted);
}

#[derive(Debug)]
struct FixedJitter(u64);

impl JitterSource for FixedJitter {
    fn sample(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for FixedJitter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[test]
fn jitter_is_injected_as_raw_entropy_for_exact_replay() {
    let jitter = FixedJitter(u64::MAX / 2 + 1);
    assert_eq!(
        BackoffPolicy::create_read_session()
            .full_jitter(1, jitter.sample())
            .unwrap(),
        Duration::from_millis(50)
    );
    assert_eq!(
        TransportEvidence::ChannelClosed.provenance(),
        "tonic-0.14.6/hyper-1.x:is_closed"
    );
}
