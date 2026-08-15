//! CreateReadSession requests, validation, and immutable leases.

use std::{collections::HashSet, error::Error, fmt, sync::Arc, time::Duration};

use sha2::{Digest, Sha256};

use crate::{
    args::{ArrowWireCompression, BigQueryInputArgs, PicosTimestampPrecision, ResponseCompression},
    decode::{DecodeLimit, SessionSchema},
    proto::bigquery_storage::{
        ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadSession,
        arrow_serialization_options, read_session,
    },
    reference::BigQueryReference,
    retry::{
        BackoffPolicy, CreateReadSessionFailure, JitterSource, RetryBudget, RetryDecision, Sleeper,
        classify_create_read_session,
    },
    snapshot::{MonotonicClock, PinnedSnapshot},
    transport::{RequestContext, StorageTransport, StorageTransportError},
};

const GUARANTEED_SESSION_LIFETIME: Duration = Duration::from_secs(6 * 60 * 60);
const DISCOVERY_STREAM_COUNT: i32 = 1;
const MAX_ROW_RESTRICTION_BYTES: usize = 1_048_576;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionPurpose {
    Discovery,
    Execution,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct SourceIdentity([u8; 32]);

impl SourceIdentity {
    fn new(
        reference: &BigQueryReference,
        snapshot: PinnedSnapshot,
        selected_fields: &[String],
        row_restriction: Option<&str>,
    ) -> Self {
        let mut digest = Sha256::new();
        hash_field(&mut digest, b"silk-bqs-source-v1");
        hash_field(&mut digest, reference.table_resource().as_bytes());
        hash_field(&mut digest, &snapshot.seconds().to_be_bytes());
        hash_field(&mut digest, &snapshot.nanos().to_be_bytes());
        for field in selected_fields {
            hash_field(&mut digest, field.as_bytes());
        }
        hash_field(&mut digest, row_restriction.map_or(&[][..], str::as_bytes));
        Self(digest.finalize().into())
    }
}

impl fmt::Debug for SourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SourceIdentity")
            .field(
                &self
                    .0
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>(),
            )
            .finish()
    }
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    let length = u64::try_from(value.len()).expect("source identity field length fits u64");
    digest.update(length.to_be_bytes());
    digest.update(value);
}

#[derive(Clone)]
pub(crate) struct ReadSessionSpec {
    purpose: SessionPurpose,
    table_resource: String,
    owner_project: String,
    expected_location: Option<String>,
    snapshot: PinnedSnapshot,
    selected_fields: Vec<String>,
    row_restriction: Option<String>,
    arrow_wire_compression: ArrowWireCompression,
    response_compression: ResponseCompression,
    picos_timestamp_precision: PicosTimestampPrecision,
    max_stream_count: i32,
    identity: SourceIdentity,
}

impl ReadSessionSpec {
    pub(crate) fn discovery(
        reference: &BigQueryReference,
        snapshot: PinnedSnapshot,
        owner_project: &str,
    ) -> Result<Self, ReadSessionSpecError> {
        Self::new(
            SessionPurpose::Discovery,
            reference,
            snapshot,
            owner_project,
            reference.expected_location().map(str::to_owned),
            Vec::new(),
            None,
            ArrowWireCompression::None,
            ResponseCompression::None,
            PicosTimestampPrecision::Micros,
            DISCOVERY_STREAM_COUNT,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execution(
        reference: &BigQueryReference,
        snapshot: PinnedSnapshot,
        owner_project: &str,
        discovered_location: &str,
        selected_fields: Vec<String>,
        row_restriction: Option<String>,
        max_stream_count: usize,
        args: &BigQueryInputArgs,
    ) -> Result<Self, ReadSessionSpecError> {
        if reference
            .expected_location()
            .is_some_and(|expected| expected != discovered_location)
        {
            return Err(ReadSessionSpecError::LocationMismatch);
        }
        Self::new(
            SessionPurpose::Execution,
            reference,
            snapshot,
            owner_project,
            Some(discovered_location.to_owned()),
            selected_fields,
            row_restriction,
            args.arrow_wire_compression,
            args.response_compression,
            args.picos_timestamp_precision,
            i32::try_from(max_stream_count)
                .map_err(|_| ReadSessionSpecError::InvalidStreamCount)?,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        purpose: SessionPurpose,
        reference: &BigQueryReference,
        snapshot: PinnedSnapshot,
        owner_project: &str,
        expected_location: Option<String>,
        selected_fields: Vec<String>,
        row_restriction: Option<String>,
        arrow_wire_compression: ArrowWireCompression,
        response_compression: ResponseCompression,
        picos_timestamp_precision: PicosTimestampPrecision,
        max_stream_count: i32,
    ) -> Result<Self, ReadSessionSpecError> {
        validate_project(owner_project)?;
        if max_stream_count <= 0 {
            return Err(ReadSessionSpecError::InvalidStreamCount);
        }
        if selected_fields.iter().any(|field| field.trim().is_empty()) {
            return Err(ReadSessionSpecError::InvalidSelectedField);
        }
        if row_restriction
            .as_ref()
            .is_some_and(|restriction| restriction.len() > MAX_ROW_RESTRICTION_BYTES)
        {
            return Err(ReadSessionSpecError::RestrictionTooLarge);
        }
        if arrow_wire_compression != ArrowWireCompression::None
            && response_compression != ResponseCompression::None
        {
            return Err(ReadSessionSpecError::CompressionConflict);
        }
        let row_restriction = row_restriction.filter(|value| !value.is_empty());
        let identity = SourceIdentity::new(
            reference,
            snapshot,
            &selected_fields,
            row_restriction.as_deref(),
        );
        Ok(Self {
            purpose,
            table_resource: reference.table_resource().to_owned(),
            owner_project: owner_project.to_owned(),
            expected_location,
            snapshot,
            selected_fields,
            row_restriction,
            arrow_wire_compression,
            response_compression,
            picos_timestamp_precision,
            max_stream_count,
            identity,
        })
    }

    #[cfg(test)]
    pub(crate) const fn purpose(&self) -> SessionPurpose {
        self.purpose
    }

    #[cfg(test)]
    pub(crate) const fn source_identity(&self) -> &SourceIdentity {
        &self.identity
    }

    pub(crate) fn create_request(&self) -> CreateReadSessionRequest {
        let buffer_compression = match self.arrow_wire_compression {
            ArrowWireCompression::None => {
                arrow_serialization_options::CompressionCodec::CompressionUnspecified
            }
            ArrowWireCompression::Lz4 => arrow_serialization_options::CompressionCodec::Lz4Frame,
            ArrowWireCompression::Zstd => arrow_serialization_options::CompressionCodec::Zstd,
        } as i32;
        let picos_timestamp_precision = match self.picos_timestamp_precision {
            PicosTimestampPrecision::Micros => {
                arrow_serialization_options::PicosTimestampPrecision::TimestampPrecisionMicros
            }
            PicosTimestampPrecision::Nanos => {
                arrow_serialization_options::PicosTimestampPrecision::TimestampPrecisionNanos
            }
            PicosTimestampPrecision::Picos => {
                arrow_serialization_options::PicosTimestampPrecision::TimestampPrecisionPicos
            }
        } as i32;
        let response_compression_codec = match self.response_compression {
            ResponseCompression::None => None,
            ResponseCompression::Lz4 => {
                Some(read_session::table_read_options::ResponseCompressionCodec::Lz4 as i32)
            }
        };
        CreateReadSessionRequest {
            parent: format!("projects/{}", self.owner_project),
            read_session: Some(ReadSession {
                data_format: DataFormat::Arrow as i32,
                table: self.table_resource.clone(),
                table_modifiers: Some(read_session::TableModifiers {
                    snapshot_time: Some(self.snapshot.to_proto()),
                }),
                read_options: Some(read_session::TableReadOptions {
                    selected_fields: self.selected_fields.clone(),
                    row_restriction: self.row_restriction.clone().unwrap_or_default(),
                    sample_percentage: None,
                    response_compression_codec,
                    output_format_serialization_options: Some(
                        read_session::table_read_options::OutputFormatSerializationOptions::ArrowSerializationOptions(
                            ArrowSerializationOptions {
                                buffer_compression,
                                picos_timestamp_precision,
                            },
                        ),
                    ),
                }),
                ..Default::default()
            }),
            max_stream_count: self.max_stream_count,
            preferred_min_stream_count: 0,
        }
    }
}

impl fmt::Debug for ReadSessionSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadSessionSpec")
            .field("purpose", &self.purpose)
            .field("source_identity", &self.identity)
            .field("selected_field_count", &self.selected_fields.len())
            .field("has_row_restriction", &self.row_restriction.is_some())
            .field("arrow_wire_compression", &self.arrow_wire_compression)
            .field("response_compression", &self.response_compression)
            .field("picos_timestamp_precision", &self.picos_timestamp_precision)
            .field("max_stream_count", &self.max_stream_count)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ReadSessionSpecError {
    #[error("invalid read-session owner project")]
    InvalidOwner,
    #[error("invalid Storage Read stream count")]
    InvalidStreamCount,
    #[error("execution session location differs from the discovery session")]
    LocationMismatch,
    #[error("Storage Read selected field is empty")]
    InvalidSelectedField,
    #[error("Storage Read row restriction exceeds 1 MiB")]
    RestrictionTooLarge,
    #[error("native Arrow and response compression cannot both be requested")]
    CompressionConflict,
}

fn validate_project(value: &str) -> Result<(), ReadSessionSpecError> {
    if !value.is_empty()
        && value.len() <= 255
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b':'))
    {
        Ok(())
    } else {
        Err(ReadSessionSpecError::InvalidOwner)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct SessionName {
    value: String,
    location: String,
}

impl SessionName {
    pub(crate) fn location(&self) -> &str {
        &self.location
    }
}

impl fmt::Debug for SessionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionName(<redacted>)")
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct StreamName(String);

impl StreamName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn for_test(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Debug for StreamName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StreamName(<redacted>)")
    }
}

#[derive(Clone)]
pub(crate) struct SessionLease {
    name: SessionName,
    streams: Arc<[StreamName]>,
    schema: SessionSchema,
    conservative_deadline: Duration,
    source_identity: SourceIdentity,
    estimated_row_count: Option<usize>,
    estimated_total_bytes_scanned: u64,
    estimated_total_physical_file_size: u64,
}

impl SessionLease {
    pub(crate) fn streams(&self) -> &[StreamName] {
        &self.streams
    }

    pub(crate) const fn schema(&self) -> &SessionSchema {
        &self.schema
    }

    pub(crate) const fn conservative_deadline(&self) -> Duration {
        self.conservative_deadline
    }

    pub(crate) const fn source_identity(&self) -> &SourceIdentity {
        &self.source_identity
    }

    pub(crate) const fn estimated_row_count(&self) -> Option<usize> {
        self.estimated_row_count
    }

    #[cfg(test)]
    pub(crate) const fn estimated_total_bytes_scanned(&self) -> u64 {
        self.estimated_total_bytes_scanned
    }

    #[cfg(test)]
    pub(crate) const fn estimated_total_physical_file_size(&self) -> u64 {
        self.estimated_total_physical_file_size
    }

    pub(crate) fn location(&self) -> &str {
        self.name.location()
    }
}

impl fmt::Debug for SessionLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionLease")
            .field("stream_count", &self.streams.len())
            .field("schema", &self.schema)
            .field("conservative_deadline", &self.conservative_deadline)
            .field("source_identity", &self.source_identity)
            .field("estimated_row_count", &self.estimated_row_count)
            .field(
                "estimated_total_bytes_scanned",
                &self.estimated_total_bytes_scanned,
            )
            .field(
                "estimated_total_physical_file_size",
                &self.estimated_total_physical_file_size,
            )
            .finish()
    }
}

pub(crate) struct ReadSessionClient {
    transport: StorageTransport,
    clock: Arc<dyn MonotonicClock>,
    decode_limit: DecodeLimit,
}

impl ReadSessionClient {
    pub(crate) fn new(
        transport: StorageTransport,
        clock: Arc<dyn MonotonicClock>,
        decode_limit: DecodeLimit,
    ) -> Self {
        Self {
            transport,
            clock,
            decode_limit,
        }
    }

    async fn create(
        &self,
        spec: &ReadSessionSpec,
        attempt: u32,
    ) -> Result<SessionLease, ReadSessionError> {
        let started = self.clock.elapsed();
        let session = self
            .transport
            .create_read_session(
                spec.create_request(),
                RequestContext::new("storage.create-read-session", attempt),
            )
            .await
            .map_err(ReadSessionError::transport)?
            .into_inner();
        let received = self.clock.elapsed();
        if received < started {
            return Err(ReadSessionError::new(
                ReadSessionErrorKind::NonMonotonicClock,
            ));
        }
        validate_response(spec, session, started, self.decode_limit)
    }

    pub(crate) async fn create_with_retry(
        &self,
        spec: &ReadSessionSpec,
        budget: RetryBudget,
        backoff: BackoffPolicy,
        sleeper: &dyn Sleeper,
        jitter: &dyn JitterSource,
    ) -> Result<SessionLease, ReadSessionError> {
        let mut previous_observation = self.clock.elapsed();
        let mut attempt = 1_u32;
        loop {
            match self.create(spec, attempt).await {
                Ok(lease) => return Ok(lease),
                Err(error) => {
                    let decision = error.retry_decision();
                    if !matches!(decision, RetryDecision::Retry { .. }) {
                        return Err(error);
                    }
                    let now = self.clock.elapsed();
                    let delay = backoff.full_jitter(attempt, jitter.sample()).map_err(|_| {
                        ReadSessionError::new(ReadSessionErrorKind::InvalidRetryPolicy)
                    })?;
                    let retry_delay = decision
                        .delay(delay)
                        .expect("a retry decision always produces a delay");
                    let plan =
                        match budget.plan_retry(previous_observation, now, attempt, retry_delay) {
                            Ok(plan) => plan,
                            Err(_) => return Err(error),
                        };
                    if !plan.delay().is_zero() {
                        sleeper.sleep(plan.delay()).await;
                    }
                    previous_observation = now;
                    attempt = plan.attempt();
                }
            }
        }
    }
}

impl fmt::Debug for ReadSessionClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadSessionClient")
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
pub(crate) trait SessionOpener: Send + Sync + fmt::Debug {
    async fn open(&self, spec: &ReadSessionSpec) -> Result<SessionLease, ReadSessionError>;
}

pub(crate) struct RetriedSessionOpener {
    client: ReadSessionClient,
    clock: Arc<dyn MonotonicClock>,
    sleeper: Arc<dyn Sleeper>,
    jitter: Arc<dyn JitterSource>,
}

impl RetriedSessionOpener {
    pub(crate) fn new(
        transport: StorageTransport,
        clock: Arc<dyn MonotonicClock>,
        sleeper: Arc<dyn Sleeper>,
        jitter: Arc<dyn JitterSource>,
        decode_limit: DecodeLimit,
    ) -> Self {
        Self {
            client: ReadSessionClient::new(transport, Arc::clone(&clock), decode_limit),
            clock,
            sleeper,
            jitter,
        }
    }
}

#[tonic::async_trait]
impl SessionOpener for RetriedSessionOpener {
    async fn open(&self, spec: &ReadSessionSpec) -> Result<SessionLease, ReadSessionError> {
        let budget = RetryBudget::create_read_session(
            self.clock.elapsed(),
            self.client.transport.unary_timeout(),
        )
        .map_err(|_| ReadSessionError::new(ReadSessionErrorKind::InvalidRetryPolicy))?;
        self.client
            .create_with_retry(
                spec,
                budget,
                BackoffPolicy::create_read_session(),
                self.sleeper.as_ref(),
                self.jitter.as_ref(),
            )
            .await
    }
}

impl fmt::Debug for RetriedSessionOpener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetriedSessionOpener")
            .finish_non_exhaustive()
    }
}

fn validate_response(
    spec: &ReadSessionSpec,
    session: ReadSession,
    started: Duration,
    decode_limit: DecodeLimit,
) -> Result<SessionLease, ReadSessionError> {
    if session.data_format != DataFormat::Arrow as i32 || session.table != spec.table_resource {
        return Err(ReadSessionError::new(ReadSessionErrorKind::SourceMismatch));
    }
    let expected_snapshot = Some(spec.snapshot.to_proto());
    let actual_snapshot = session
        .table_modifiers
        .as_ref()
        .and_then(|modifiers| modifiers.snapshot_time);
    if actual_snapshot != expected_snapshot {
        return Err(ReadSessionError::new(ReadSessionErrorKind::SourceMismatch));
    }
    if session.read_options.as_ref().is_some_and(|actual| {
        spec.create_request()
            .read_session
            .and_then(|requested| requested.read_options)
            .as_ref()
            != Some(actual)
    }) {
        return Err(ReadSessionError::new(ReadSessionErrorKind::SourceMismatch));
    }
    let serialized_schema = match session.schema {
        Some(read_session::Schema::ArrowSchema(schema)) if !schema.serialized_schema.is_empty() => {
            schema.serialized_schema
        }
        _ => {
            return Err(ReadSessionError::new(
                ReadSessionErrorKind::MissingArrowSchema,
            ));
        }
    };
    let schema = SessionSchema::from_serialized(&serialized_schema, decode_limit)
        .map_err(|_| ReadSessionError::new(ReadSessionErrorKind::InvalidArrowSchema))?;
    let expiration = session
        .expire_time
        .ok_or_else(|| ReadSessionError::new(ReadSessionErrorKind::InvalidExpiration))?;
    PinnedSnapshot::new(expiration.seconds, expiration.nanos)
        .map_err(|_| ReadSessionError::new(ReadSessionErrorKind::InvalidExpiration))?;
    let name = validate_session_name(
        &session.name,
        &spec.owner_project,
        spec.expected_location.as_deref(),
    )?;
    if session.streams.is_empty()
        || session.streams.len()
            > usize::try_from(spec.max_stream_count).expect("positive i32 stream counts fit usize")
    {
        return Err(ReadSessionError::new(ReadSessionErrorKind::InvalidStreams));
    }
    let prefix = format!("{}/streams/", session.name);
    let mut seen = HashSet::new();
    let mut streams = Vec::with_capacity(session.streams.len());
    for stream in session.streams {
        let Some(id) = stream.name.strip_prefix(&prefix) else {
            return Err(ReadSessionError::new(ReadSessionErrorKind::InvalidStreams));
        };
        if !valid_resource_id(id) || !seen.insert(stream.name.clone()) {
            return Err(ReadSessionError::new(ReadSessionErrorKind::InvalidStreams));
        }
        streams.push(StreamName(stream.name));
    }
    if session.estimated_total_bytes_scanned < 0
        || session.estimated_total_physical_file_size < 0
        || session.estimated_row_count < 0
    {
        return Err(ReadSessionError::new(
            ReadSessionErrorKind::InvalidEstimates,
        ));
    }
    let estimated_row_count = usize::try_from(session.estimated_row_count)
        .ok()
        .filter(|estimate| *estimate > 0);
    let estimated_total_bytes_scanned = u64::try_from(session.estimated_total_bytes_scanned)
        .expect("negative estimates were rejected");
    let estimated_total_physical_file_size =
        u64::try_from(session.estimated_total_physical_file_size)
            .expect("negative estimates were rejected");
    let conservative_deadline = started
        .checked_add(GUARANTEED_SESSION_LIFETIME)
        .ok_or_else(|| ReadSessionError::new(ReadSessionErrorKind::InvalidExpiration))?;
    Ok(SessionLease {
        name,
        streams: streams.into(),
        schema,
        conservative_deadline,
        source_identity: spec.identity.clone(),
        estimated_row_count,
        estimated_total_bytes_scanned,
        estimated_total_physical_file_size,
    })
}

fn validate_session_name(
    value: &str,
    owner_project: &str,
    expected_location: Option<&str>,
) -> Result<SessionName, ReadSessionError> {
    let parts = value.split('/').collect::<Vec<_>>();
    let [
        "projects",
        project,
        "locations",
        location,
        "sessions",
        session,
    ] = parts.as_slice()
    else {
        return Err(ReadSessionError::new(
            ReadSessionErrorKind::InvalidSessionName,
        ));
    };
    if project != &owner_project
        || expected_location.is_some_and(|expected| expected != *location)
        || !valid_resource_id(location)
        || !valid_resource_id(session)
    {
        return Err(ReadSessionError::new(
            ReadSessionErrorKind::InvalidSessionName,
        ));
    }
    Ok(SessionName {
        value: value.to_owned(),
        location: (*location).to_owned(),
    })
}

fn valid_resource_id(value: &str) -> bool {
    !value.is_empty()
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| !byte.is_ascii_whitespace() && !byte.is_ascii_control() && byte != b'/')
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadSessionErrorKind {
    Transport,
    SourceMismatch,
    MissingArrowSchema,
    InvalidArrowSchema,
    InvalidExpiration,
    InvalidSessionName,
    InvalidStreams,
    InvalidEstimates,
    NonMonotonicClock,
    InvalidRetryPolicy,
}

pub(crate) struct ReadSessionError {
    kind: ReadSessionErrorKind,
    transport: Option<StorageTransportError>,
}

impl ReadSessionError {
    const fn new(kind: ReadSessionErrorKind) -> Self {
        Self {
            kind,
            transport: None,
        }
    }

    fn transport(error: StorageTransportError) -> Self {
        Self {
            kind: ReadSessionErrorKind::Transport,
            transport: Some(error),
        }
    }

    #[cfg(test)]
    pub(crate) const fn kind(&self) -> ReadSessionErrorKind {
        self.kind
    }

    fn retry_decision(&self) -> RetryDecision {
        match self.transport.as_ref() {
            Some(StorageTransportError::Status(status)) => {
                classify_create_read_session(CreateReadSessionFailure::Status(status))
            }
            Some(StorageTransportError::Credentials(error)) if error.retryable() => {
                classify_create_read_session(CreateReadSessionFailure::TransientCredentials)
            }
            Some(StorageTransportError::Credentials(_)) => {
                classify_create_read_session(CreateReadSessionFailure::Credentials)
            }
            Some(
                StorageTransportError::Config(_)
                | StorageTransportError::Connect(_)
                | StorageTransportError::InvalidMetadata,
            )
            | None => classify_create_read_session(CreateReadSessionFailure::LocalResource),
        }
    }
}

impl fmt::Display for ReadSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ReadSessionErrorKind::Transport => "CreateReadSession transport failed",
            ReadSessionErrorKind::SourceMismatch => {
                "CreateReadSession returned a different source or read policy"
            }
            ReadSessionErrorKind::MissingArrowSchema => {
                "CreateReadSession returned no Arrow schema"
            }
            ReadSessionErrorKind::InvalidArrowSchema => {
                "CreateReadSession returned an invalid Arrow schema"
            }
            ReadSessionErrorKind::InvalidExpiration => {
                "CreateReadSession returned an invalid expiration"
            }
            ReadSessionErrorKind::InvalidSessionName => {
                "CreateReadSession returned an invalid session name or location"
            }
            ReadSessionErrorKind::InvalidStreams => "CreateReadSession returned invalid streams",
            ReadSessionErrorKind::InvalidEstimates => {
                "CreateReadSession returned a negative estimate"
            }
            ReadSessionErrorKind::NonMonotonicClock => {
                "monotonic clock moved backwards during CreateReadSession"
            }
            ReadSessionErrorKind::InvalidRetryPolicy => "CreateReadSession retry policy is invalid",
        })
    }
}

impl fmt::Debug for ReadSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadSessionError")
            .field("kind", &self.kind)
            .field("transport", &self.transport.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Error for ReadSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.transport
            .as_ref()
            .map(|error| error as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::bigquery_storage::{ArrowSchema, ReadStream};
    use silk_chiffon_test_support::bigquery_arrow::{documented_mapping_fixture, encode_schema};

    fn reference() -> BigQueryReference {
        BigQueryReference::parse(
            "bqs:///projects/table-project/datasets/dataset/tables/table?location=us",
        )
        .unwrap()
    }

    fn snapshot() -> PinnedSnapshot {
        PinnedSnapshot::from_rfc3339("2026-08-15T12:00:00Z").unwrap()
    }

    fn response(spec: &ReadSessionSpec) -> ReadSession {
        let request = spec.create_request().read_session.unwrap();
        let session_name = "projects/session-project/locations/us/sessions/session-1";
        ReadSession {
            name: session_name.to_owned(),
            expire_time: Some(prost_types::Timestamp {
                seconds: 1_800_000_000,
                nanos: 0,
            }),
            data_format: DataFormat::Arrow as i32,
            table: request.table,
            table_modifiers: request.table_modifiers,
            read_options: request.read_options,
            streams: vec![ReadStream {
                name: format!("{session_name}/streams/stream-1"),
            }],
            estimated_total_bytes_scanned: 123,
            estimated_total_physical_file_size: 456,
            estimated_row_count: 7,
            schema: Some(read_session::Schema::ArrowSchema(ArrowSchema {
                serialized_schema: encode_schema(&documented_mapping_fixture().schema),
            })),
            ..Default::default()
        }
    }

    #[test]
    fn discovery_pins_snapshot_and_requests_schema_without_reading_rows() {
        let spec = ReadSessionSpec::discovery(&reference(), snapshot(), "session-project").unwrap();
        let request = spec.create_request();
        let session = request.read_session.unwrap();

        assert_eq!(spec.purpose(), SessionPurpose::Discovery);
        assert_eq!(request.max_stream_count, 1);
        assert_eq!(
            session.table_modifiers.unwrap().snapshot_time,
            Some(snapshot().to_proto())
        );
        assert_eq!(
            session.read_options.unwrap().selected_fields,
            Vec::<String>::new()
        );
    }

    #[test]
    fn execution_request_preserves_projection_filter_and_wire_modes() {
        let mut args = BigQueryInputArgs::for_test();
        args.arrow_wire_compression = ArrowWireCompression::Zstd;
        args.picos_timestamp_precision = PicosTimestampPrecision::Picos;
        let spec = ReadSessionSpec::execution(
            &reference(),
            snapshot(),
            "session-project",
            "us",
            vec!["a".to_owned(), "nested.b".to_owned()],
            Some("(`a` > 1)".to_owned()),
            4,
            &args,
        )
        .unwrap();
        let request = spec.create_request();
        let options = request.read_session.unwrap().read_options.unwrap();

        assert_eq!(request.max_stream_count, 4);
        assert_eq!(options.selected_fields, ["a", "nested.b"]);
        assert_eq!(options.row_restriction, "(`a` > 1)");
        let Some(
            read_session::table_read_options::OutputFormatSerializationOptions::ArrowSerializationOptions(arrow),
        ) = options.output_format_serialization_options else {
            panic!("expected Arrow serialization options")
        };
        assert_eq!(
            arrow.buffer_compression,
            arrow_serialization_options::CompressionCodec::Zstd as i32
        );
        assert_eq!(
            arrow.picos_timestamp_precision,
            arrow_serialization_options::PicosTimestampPrecision::TimestampPrecisionPicos as i32
        );
    }

    #[test]
    fn session_validation_accepts_matching_schema_location_streams_and_estimate() {
        let spec = ReadSessionSpec::discovery(&reference(), snapshot(), "session-project").unwrap();
        let lease = validate_response(
            &spec,
            response(&spec),
            Duration::from_secs(5),
            DecodeLimit::new(256 * 1024 * 1024).unwrap(),
        )
        .unwrap();

        assert_eq!(lease.streams().len(), 1);
        assert_eq!(lease.estimated_row_count(), Some(7));
        assert_eq!(lease.estimated_total_bytes_scanned(), 123);
        assert_eq!(lease.estimated_total_physical_file_size(), 456);
        assert_eq!(lease.location(), "us");
        assert_eq!(lease.conservative_deadline(), Duration::from_secs(21_605));
        assert_eq!(
            lease.schema().as_arrow().fields().len(),
            documented_mapping_fixture().schema.fields().len()
        );
        assert_eq!(lease.source_identity(), spec.source_identity());
        assert!(!format!("{lease:?}").contains("session-project"));
    }

    #[test]
    fn session_validation_rejects_source_location_stream_schema_and_estimate_mismatches() {
        let spec = ReadSessionSpec::discovery(&reference(), snapshot(), "session-project").unwrap();
        let limit = DecodeLimit::new(256 * 1024 * 1024).unwrap();
        let mut cases = Vec::new();

        let mut wrong_table = response(&spec);
        wrong_table.table.push_str("-other");
        cases.push(wrong_table);
        let mut wrong_location = response(&spec);
        wrong_location.name = "projects/session-project/locations/eu/sessions/session-1".to_owned();
        cases.push(wrong_location);
        let mut duplicate_stream = response(&spec);
        duplicate_stream
            .streams
            .push(duplicate_stream.streams[0].clone());
        cases.push(duplicate_stream);
        let mut negative = response(&spec);
        negative.estimated_row_count = -1;
        cases.push(negative);
        let mut avro = response(&spec);
        avro.schema = Some(read_session::Schema::AvroSchema(Default::default()));
        cases.push(avro);

        for session in cases {
            assert!(validate_response(&spec, session, Duration::ZERO, limit).is_err());
        }
    }

    #[test]
    fn execution_requires_the_discovered_location_when_url_omits_location() {
        let reference =
            BigQueryReference::parse("bqs:///projects/table-project/datasets/dataset/tables/table")
                .unwrap();
        let args = BigQueryInputArgs::for_test();
        let spec = ReadSessionSpec::execution(
            &reference,
            snapshot(),
            "session-project",
            "us",
            Vec::new(),
            None,
            1,
            &args,
        )
        .unwrap();
        let mut wrong_location = response(&spec);
        wrong_location.name = "projects/session-project/locations/eu/sessions/session-1".to_owned();

        assert_eq!(
            validate_response(
                &spec,
                wrong_location,
                Duration::ZERO,
                DecodeLimit::new(256 * 1024 * 1024).unwrap(),
            )
            .unwrap_err()
            .kind(),
            ReadSessionErrorKind::InvalidSessionName
        );
    }
}
