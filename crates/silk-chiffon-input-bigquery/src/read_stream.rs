//! Demand-driven ReadRows retries, decode admission, and offset recovery.

use std::{fmt, sync::Arc, time::Duration};

use arrow::{
    array::{RecordBatch, RecordBatchOptions},
    datatypes::SchemaRef,
};
use datafusion::{
    common::DataFusionError,
    execution::{
        SendableRecordBatchStream, TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    physical_plan::{
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, stream};

use crate::{
    args::BigQueryInputArgs,
    decode::{DecodedBatch, RowPayloadCodec, SerializedRows},
    proto::bigquery_storage::{ReadRowsRequest, ReadRowsResponse, read_rows_response},
    resources::CommandResources,
    retry::{
        BackoffPolicy, JitterSource, ReadRowsFailure, RetryBudget, RetryDecision, RetryReason,
        Sleeper, classify_read_rows,
    },
    session::StreamName,
    snapshot::MonotonicClock,
    transport::{ReadRowsResponseStream, ReadRowsRpc, RequestContext, StorageTransportError},
};

const READ_ROWS_OPERATION: &str = "storage.read-rows";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockingDecodePhase {
    Prepare,
    Decode,
}

trait BlockingDecodeGate: fmt::Debug + Send + Sync {
    fn enter(&self, _phase: BlockingDecodePhase) {}
}

#[derive(Debug)]
struct OpenBlockingDecodeGate;

impl BlockingDecodeGate for OpenBlockingDecodeGate {}

#[cfg(test)]
trait StreamFaultGate: fmt::Debug + Send + Sync {
    fn transition(&self, point: crate::fault::Point) -> Result<(), RetryReason>;
}

#[cfg(test)]
#[derive(Debug)]
struct OpenStreamFaultGate;

#[cfg(test)]
impl StreamFaultGate for OpenStreamFaultGate {
    fn transition(&self, _point: crate::fault::Point) -> Result<(), RetryReason> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AcceptedOffset(i64);

impl AcceptedOffset {
    const fn new(value: i64) -> Result<Self, AcceptedOffsetError> {
        if value < 0 {
            Err(AcceptedOffsetError)
        } else {
            Ok(Self(value))
        }
    }

    const fn get(self) -> i64 {
        self.0
    }

    fn checked_advance(self, rows: usize) -> Result<Self, AcceptedOffsetError> {
        let rows = i64::try_from(rows).map_err(|_| AcceptedOffsetError)?;
        self.0
            .checked_add(rows)
            .map(Self)
            .ok_or(AcceptedOffsetError)
    }

    fn commit(&mut self, next: Self) {
        *self = next;
    }
}

impl Default for AcceptedOffset {
    fn default() -> Self {
        Self::new(0).expect("zero is a valid Storage Read offset")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("accepted Storage Read offset overflowed")]
struct AcceptedOffsetError;

#[derive(Clone)]
pub(crate) struct StreamResources {
    rpc: Arc<dyn ReadRowsRpc>,
    clock: Arc<dyn MonotonicClock>,
    sleeper: Arc<dyn Sleeper>,
    jitter: Arc<dyn JitterSource>,
    decode_limit: crate::decode::DecodeLimit,
    decode_permits: Arc<tokio::sync::Semaphore>,
    blocking_decode_gate: Arc<dyn BlockingDecodeGate>,
    connection_count: usize,
    #[cfg(test)]
    fault_gate: Arc<dyn StreamFaultGate>,
}

impl StreamResources {
    pub(crate) fn from_command(resources: &CommandResources) -> Arc<Self> {
        Arc::new(Self {
            rpc: Arc::clone(&resources.read_rows),
            clock: Arc::clone(&resources.monotonic_clock),
            sleeper: Arc::clone(&resources.retry_sleeper),
            jitter: Arc::clone(&resources.retry_jitter),
            decode_limit: resources.decode_limit,
            decode_permits: Arc::clone(&resources.decode_permits),
            blocking_decode_gate: Arc::new(OpenBlockingDecodeGate),
            connection_count: resources.read_connection_count,
            #[cfg(test)]
            fault_gate: Arc::new(OpenStreamFaultGate),
        })
    }
}

pub(crate) struct ReadPartition {
    pub(crate) ordinal: usize,
    pub(crate) stream_name: StreamName,
    pub(crate) session_schema: crate::decode::SessionSchema,
    pub(crate) session_deadline: Duration,
    pub(crate) output_schema: SchemaRef,
    pub(crate) batch_projection: Arc<[usize]>,
    pub(crate) resources: Arc<StreamResources>,
    pub(crate) args: BigQueryInputArgs,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) task_context: Arc<TaskContext>,
}

pub(crate) fn read_rows_stream(
    partition: ReadPartition,
) -> datafusion::common::Result<SendableRecordBatchStream> {
    let metrics = PartitionMetrics::new(&partition.metrics, partition.ordinal);
    let backoff = BackoffPolicy::read_rows(
        partition.args.read_retry_initial_backoff,
        partition.args.read_retry_max_backoff,
    )
    .map_err(|_| {
        stream_error(
            partition.ordinal,
            1,
            AcceptedOffset::default(),
            RetryReason::LocalResource,
        )
    })?;
    let started = partition.resources.clock.elapsed();
    let retry_budget = RetryBudget::read_rows(
        started,
        partition.args.read_retry_window,
        u32::MAX,
        partition.args.read_idle_timeout,
        Some(partition.session_deadline),
    )
    .map_err(|_| {
        stream_error(
            partition.ordinal,
            1,
            AcceptedOffset::default(),
            RetryReason::LocalResource,
        )
    })?;
    let reservation = MemoryConsumer::new(format!(
        "BigQuery Storage Read partition {}",
        partition.ordinal
    ))
    .register(&partition.task_context.runtime_env().memory_pool);
    let schema = Arc::clone(&partition.output_schema);
    let state = ReadRowsStreamState {
        ordinal: partition.ordinal,
        stream_name: partition.stream_name,
        session_schema: partition.session_schema,
        output_schema: partition.output_schema,
        batch_projection: partition.batch_projection,
        resources: partition.resources,
        args: partition.args,
        metrics,
        current: None,
        accepted_offset: AcceptedOffset::default(),
        rpc_attempt: 1,
        failure_streak: 0,
        accepted_progress_started: None,
        accepted_progress_rows: 0,
        previous_observation: started,
        response_ordinal: 0,
        #[cfg(test)]
        terminal: false,
        backoff,
        retry_budget,
        reservation,
    };
    let batches = stream::try_unfold(state, |mut state| async move {
        state.reservation.free();
        match state.next_batch().await {
            Ok(Some(batch)) => Ok(Some((batch, state))),
            Ok(None) => {
                state.finish();
                Ok(None)
            }
            Err(error) => {
                state.finish();
                Err(error)
            }
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

struct ReadRowsStreamState {
    ordinal: usize,
    stream_name: StreamName,
    session_schema: crate::decode::SessionSchema,
    output_schema: SchemaRef,
    batch_projection: Arc<[usize]>,
    resources: Arc<StreamResources>,
    args: BigQueryInputArgs,
    metrics: PartitionMetrics,
    current: Option<ReadRowsResponseStream>,
    accepted_offset: AcceptedOffset,
    rpc_attempt: u32,
    failure_streak: u32,
    accepted_progress_started: Option<Duration>,
    accepted_progress_rows: u64,
    previous_observation: Duration,
    response_ordinal: usize,
    #[cfg(test)]
    terminal: bool,
    backoff: BackoffPolicy,
    retry_budget: RetryBudget,
    reservation: MemoryReservation,
}

impl ReadRowsStreamState {
    async fn next_batch(&mut self) -> datafusion::common::Result<Option<RecordBatch>> {
        loop {
            if self.current.is_none() {
                match self.open().await {
                    Ok(stream) => self.current = Some(stream),
                    Err(failure) => {
                        self.retry_or_fail(failure).await?;
                        continue;
                    }
                }
            }

            let (timeout, session_limited) =
                self.wait_timeout().map_err(|reason| self.error(reason))?;
            let message = tokio::time::timeout(
                timeout,
                self.current.as_mut().expect("the stream was opened").next(),
            )
            .await;
            let response = match message {
                Ok(Some(Ok(response))) => response,
                Ok(Some(Err(status))) => {
                    self.current = None;
                    self.retry_or_fail(AttemptFailure::Status(status)).await?;
                    continue;
                }
                Ok(None) => return Ok(None),
                Err(_) if session_limited => {
                    return Err(self.error(RetryReason::SessionExpired));
                }
                Err(_) => {
                    self.current = None;
                    self.retry_or_fail(AttemptFailure::IdleTimeout).await?;
                    continue;
                }
            };
            #[cfg(test)]
            self.transition(crate::fault::Phase::ReadResponse)
                .map_err(|reason| self.error(reason))?;
            self.response_ordinal = self.response_ordinal.saturating_add(1);
            self.metrics.responses.add(1);
            if let Some(batch) = self.decode(response).await? {
                return Ok(Some(batch));
            }
            self.reservation.free();
        }
    }

    async fn open(&mut self) -> Result<ReadRowsResponseStream, AttemptFailure> {
        #[cfg(test)]
        self.transition(crate::fault::Phase::ReadOpen)
            .map_err(AttemptFailure::Terminal)?;
        let (timeout, session_limited) = self.wait_timeout().map_err(AttemptFailure::Terminal)?;
        self.metrics.rpc_attempts.add(1);
        let request = ReadRowsRequest {
            read_stream: self.stream_name.as_str().to_owned(),
            offset: self.accepted_offset.get(),
        };
        let connection_count = self.resources.connection_count.max(1);
        let context = RequestContext::new(READ_ROWS_OPERATION, self.rpc_attempt).with_read_stream(
            self.ordinal,
            self.ordinal % connection_count,
            self.accepted_offset.get(),
        );
        match tokio::time::timeout(
            timeout,
            self.resources.rpc.open(self.ordinal, request, context),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(error)) => Err(AttemptFailure::Transport(error)),
            Err(_) if session_limited => Err(AttemptFailure::SessionExpired),
            Err(_) => Err(AttemptFailure::IdleTimeout),
        }
    }

    async fn decode(
        &mut self,
        response: ReadRowsResponse,
    ) -> datafusion::common::Result<Option<RecordBatch>> {
        let decode_time = self.metrics.decode_time.clone();
        let _decode_timer = decode_time.timer();
        let serialized_bytes = serialized_payload_len(&response);
        self.metrics.serialized_bytes.add(serialized_bytes);
        #[cfg(test)]
        self.transition(crate::fault::Phase::SerializedAdmission)
            .map_err(|reason| self.error(reason))?;
        self.reservation
            .try_resize(serialized_bytes)
            .map_err(|_| self.error(RetryReason::LocalResource))?;
        #[cfg(test)]
        self.transition(crate::fault::Phase::DecodePermit)
            .map_err(|reason| self.error(reason))?;
        let permit = Arc::clone(&self.resources.decode_permits)
            .acquire_owned()
            .await
            .map_err(|_| self.error(RetryReason::LocalResource))?;
        let schema = self.session_schema.clone();
        let codec = RowPayloadCodec::from(self.args.response_compression);
        let limit = self.resources.decode_limit;
        let blocking_decode_gate = Arc::clone(&self.resources.blocking_decode_gate);
        #[cfg(test)]
        self.transition(crate::fault::Phase::PrepareDecode)
            .map_err(|reason| self.error(reason))?;
        let prepared = tokio::task::spawn_blocking(move || {
            blocking_decode_gate.enter(BlockingDecodePhase::Prepare);
            let rows = SerializedRows::from_response(response, &schema, codec, limit)?;
            rows.map(|rows| -> Result<_, crate::decode::DecodeError> {
                let upper_bound = rows.memory_upper_bound(limit)?;
                Ok((rows, upper_bound))
            })
            .transpose()
        })
        .await
        .map_err(|_| self.error(RetryReason::Decode))?
        .map_err(|_| self.error(classify_read_rows(ReadRowsFailure::Decode).reason()))?;
        let Some((rows, upper_bound)) = prepared else {
            drop(permit);
            return Ok(None);
        };
        #[cfg(test)]
        self.transition(crate::fault::Phase::DecodedAdmission)
            .map_err(|reason| self.error(reason))?;
        self.reservation
            .try_resize(upper_bound)
            .map_err(|_| self.error(RetryReason::LocalResource))?;
        let schema = self.session_schema.clone();
        let blocking_decode_gate = Arc::clone(&self.resources.blocking_decode_gate);
        #[cfg(test)]
        self.transition(crate::fault::Phase::Decode)
            .map_err(|reason| self.error(reason))?;
        let decoded = tokio::task::spawn_blocking(move || {
            blocking_decode_gate.enter(BlockingDecodePhase::Decode);
            rows.decode(&schema, limit)
        })
        .await
        .map_err(|_| self.error(RetryReason::Decode))?
        .map_err(|_| self.error(classify_read_rows(ReadRowsFailure::Decode).reason()))?;
        drop(permit);
        self.accept(&decoded)
    }

    fn accept(
        &mut self,
        decoded: &DecodedBatch,
    ) -> datafusion::common::Result<Option<RecordBatch>> {
        let row_count = decoded.row_count();
        let next_offset = self
            .accepted_offset
            .checked_advance(row_count)
            .map_err(|_| self.error(RetryReason::OffsetOverflow))?;
        let bytes = decoded.bytes();
        self.metrics
            .decoded_arrow_bytes
            .add(bytes.arrow_buffer_memory());
        let projected = decoded
            .record_batch()
            .project(&self.batch_projection)
            .map_err(|_| self.error(RetryReason::Schema))?;
        let output = RecordBatch::try_new_with_options(
            Arc::clone(&self.output_schema),
            projected.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|_| self.error(RetryReason::Schema))?;

        #[cfg(test)]
        self.transition(crate::fault::Phase::AcceptOffset)
            .map_err(|reason| self.error(reason))?;
        self.accepted_offset.commit(next_offset);
        self.metrics.output_rows.add(row_count);
        if row_count > 0 {
            let now = self.resources.clock.elapsed();
            self.accepted_progress_started.get_or_insert(now);
            self.accepted_progress_rows = self
                .accepted_progress_rows
                .saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
        }
        Ok(Some(output))
    }

    async fn retry_or_fail(&mut self, failure: AttemptFailure) -> datafusion::common::Result<()> {
        let now = self.resources.clock.elapsed();
        if now >= self.retry_budget.effective_deadline() {
            return Err(self.error(self.deadline_reason()));
        }
        if let AttemptFailure::Terminal(reason) = failure {
            return Err(self.error(reason));
        }
        let decision = failure.decision();
        if let RetryDecision::Permanent { reason, .. } = decision {
            return Err(self.error(reason));
        }
        if let Some(started) = self.accepted_progress_started {
            let elapsed = now
                .checked_sub(started)
                .ok_or_else(|| self.error(RetryReason::RetryBudgetExhausted))?;
            self.failure_streak = self.backoff.failure_streak_after_progress(
                self.failure_streak,
                self.accepted_progress_rows,
                elapsed,
            );
            if self.failure_streak == 0 {
                self.accepted_progress_started = None;
                self.accepted_progress_rows = 0;
            }
        }
        self.failure_streak = self
            .failure_streak
            .checked_add(1)
            .ok_or_else(|| self.error(RetryReason::RetryBudgetExhausted))?;
        let backoff = self
            .backoff
            .full_jitter(self.failure_streak, self.resources.jitter.sample())
            .map_err(|_| self.error(RetryReason::RetryBudgetExhausted))?;
        let retry_delay = decision
            .delay(backoff)
            .expect("only retry decisions reach delay planning");
        let plan = self
            .retry_budget
            .plan_retry(
                self.previous_observation,
                now,
                self.rpc_attempt,
                retry_delay,
            )
            .map_err(|_| self.error(RetryReason::RetryBudgetExhausted))?;
        self.metrics.retries.add(1);
        #[cfg(test)]
        self.transition(crate::fault::Phase::RetryDelay)
            .map_err(|reason| self.error(reason))?;
        if !plan.delay().is_zero() {
            self.resources.sleeper.sleep(plan.delay()).await;
            self.metrics.retry_delay_time.add_duration(plan.delay());
        }
        self.previous_observation = now;
        self.rpc_attempt = plan.attempt();
        self.current = None;
        Ok(())
    }

    fn wait_timeout(&self) -> Result<(Duration, bool), RetryReason> {
        let now = self.resources.clock.elapsed();
        let remaining = self
            .retry_budget
            .effective_deadline()
            .checked_sub(now)
            .ok_or_else(|| self.deadline_reason())?;
        let session_limited = self.retry_budget.is_session_limited();
        Ok((
            remaining.min(self.args.read_idle_timeout),
            remaining <= self.args.read_idle_timeout && session_limited,
        ))
    }

    fn deadline_reason(&self) -> RetryReason {
        if self.retry_budget.is_session_limited() {
            RetryReason::SessionExpired
        } else {
            RetryReason::RetryBudgetExhausted
        }
    }

    fn error(&self, reason: RetryReason) -> DataFusionError {
        stream_error(self.ordinal, self.rpc_attempt, self.accepted_offset, reason)
    }

    #[cfg(test)]
    fn finish(&mut self) {
        self.terminal = true;
    }

    #[cfg(not(test))]
    fn finish(&mut self) {}

    #[cfg(test)]
    fn transition(&self, phase: crate::fault::Phase) -> Result<(), RetryReason> {
        self.resources.fault_gate.transition(crate::fault::Point {
            phase,
            session: 1,
            stream: self.ordinal,
            attempt: self.rpc_attempt,
            requested_offset: self.accepted_offset.get(),
            response: self.response_ordinal,
            accepted_rows: self.accepted_offset.get(),
        })
    }
}

#[cfg(test)]
impl Drop for ReadRowsStreamState {
    fn drop(&mut self) {
        if !self.terminal {
            let _ = self.transition(crate::fault::Phase::Cancellation);
        }
    }
}

enum AttemptFailure {
    Transport(StorageTransportError),
    Status(tonic::Status),
    IdleTimeout,
    SessionExpired,
    Terminal(RetryReason),
}

impl AttemptFailure {
    fn decision(&self) -> RetryDecision {
        match self {
            Self::Transport(StorageTransportError::Status(status)) | Self::Status(status) => {
                classify_read_rows(ReadRowsFailure::Status(status))
            }
            Self::Transport(StorageTransportError::Credentials(error)) if error.retryable() => {
                classify_read_rows(ReadRowsFailure::TransientCredentials)
            }
            Self::Transport(StorageTransportError::Credentials(_)) => {
                classify_read_rows(ReadRowsFailure::Credentials)
            }
            Self::Transport(StorageTransportError::Connect(_)) => {
                classify_read_rows(ReadRowsFailure::LocalResource)
            }
            Self::Transport(
                StorageTransportError::Config(_) | StorageTransportError::InvalidMetadata,
            ) => classify_read_rows(ReadRowsFailure::LocalResource),
            Self::IdleTimeout => classify_read_rows(ReadRowsFailure::IdleTimeout),
            Self::SessionExpired => classify_read_rows(ReadRowsFailure::SessionExpired),
            Self::Terminal(reason) => {
                RetryDecision::permanent(crate::retry::RetryScope::ReadRows, *reason)
            }
        }
    }
}

#[derive(Clone)]
struct PartitionMetrics {
    output_rows: Count,
    rpc_attempts: Count,
    responses: Count,
    retries: Count,
    serialized_bytes: Count,
    decoded_arrow_bytes: Count,
    decode_time: Time,
    retry_delay_time: Time,
}

impl PartitionMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            rpc_attempts: MetricBuilder::new(metrics).counter("read_rows_rpc_attempts", partition),
            responses: MetricBuilder::new(metrics).counter("read_rows_responses", partition),
            retries: MetricBuilder::new(metrics).counter("read_rows_retries", partition),
            serialized_bytes: MetricBuilder::new(metrics)
                .counter("serialized_bytes_received", partition),
            decoded_arrow_bytes: MetricBuilder::new(metrics)
                .counter("decoded_arrow_bytes", partition),
            decode_time: MetricBuilder::new(metrics).subset_time("decode_time", partition),
            retry_delay_time: MetricBuilder::new(metrics)
                .subset_time("retry_delay_time", partition),
        }
    }
}

fn serialized_payload_len(response: &ReadRowsResponse) -> usize {
    match response.rows.as_ref() {
        Some(read_rows_response::Rows::ArrowRecordBatch(batch)) => {
            batch.serialized_record_batch.len()
        }
        Some(read_rows_response::Rows::AvroRows(rows)) => rows.serialized_binary_rows.len(),
        None => 0,
    }
}

fn stream_error(
    partition: usize,
    attempt: u32,
    accepted_offset: AcceptedOffset,
    reason: RetryReason,
) -> DataFusionError {
    DataFusionError::External(Box::new(StreamReadError {
        operation: READ_ROWS_OPERATION,
        partition,
        attempt,
        accepted_offset,
        reason,
    }))
}

struct StreamReadError {
    operation: &'static str,
    partition: usize,
    attempt: u32,
    accepted_offset: AcceptedOffset,
    reason: RetryReason,
}

impl fmt::Display for StreamReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "BigQuery Storage Read operation failed: operation={} \
             partition={} attempt={} accepted_offset={} reason={}",
            self.operation,
            self.partition,
            self.attempt,
            self.accepted_offset.get(),
            reason_name(self.reason)
        )
    }
}

impl fmt::Debug for StreamReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamReadError")
            .field("operation", &self.operation)
            .field("partition", &self.partition)
            .field("attempt", &self.attempt)
            .field("accepted_offset", &self.accepted_offset)
            .field("reason", &self.reason)
            .finish()
    }
}

impl std::error::Error for StreamReadError {}

const fn reason_name(reason: RetryReason) -> &'static str {
    match reason {
        RetryReason::Credentials => "credentials",
        RetryReason::GrpcDeadlineExceeded => "deadline-exceeded",
        RetryReason::GrpcUnavailable => "unavailable",
        RetryReason::GrpcResourceExhausted => "resource-exhausted",
        RetryReason::GrpcStatus(_) => "grpc-status",
        RetryReason::Transport(_) => "transport",
        RetryReason::MalformedStatusDetails => "malformed-status-details",
        RetryReason::IdleTimeout => "idle-timeout",
        RetryReason::SessionExpired => "session-expired",
        RetryReason::LostSessionState => "lost-session-state",
        RetryReason::RetryBudgetExhausted => "retry-budget-exhausted",
        RetryReason::Schema => "schema",
        RetryReason::Decode => "decode",
        RetryReason::OffsetOverflow => "offset-overflow",
        RetryReason::LocalResource => "local-resource",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    };

    use datafusion::execution::{
        memory_pool::{GreedyMemoryPool, MemoryPool},
        runtime_env::RuntimeEnvBuilder,
    };
    use futures::TryStreamExt;
    use silk_chiffon_test_support::bigquery_arrow::{
        documented_mapping_fixture, encode_batch, encode_schema,
    };

    use crate::{
        decode::{DecodeLimit, SessionSchema},
        fault::{AcceptedOffsetOracle, Phase, Point, Schedule, Selector, Step},
        proto::bigquery_storage::ArrowRecordBatch,
    };

    use super::*;

    enum OpenAction {
        Responses(Vec<Result<ReadRowsResponse, tonic::Status>>),
        Status(tonic::Status),
        Pending(Arc<AtomicBool>),
        PendingMessages(Arc<AtomicBool>),
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct OpenObservation {
        offset: i64,
        attempt: u32,
        stream_ordinal: usize,
    }

    struct FakeReadRowsRpc {
        actions: Mutex<VecDeque<OpenAction>>,
        replay: Mutex<Option<Schedule<OpenAction>>>,
        observations: Mutex<Vec<OpenObservation>>,
    }

    impl FakeReadRowsRpc {
        fn new(actions: impl IntoIterator<Item = OpenAction>) -> Arc<Self> {
            Arc::new(Self {
                actions: Mutex::new(actions.into_iter().collect()),
                replay: Mutex::new(None),
                observations: Mutex::new(Vec::new()),
            })
        }

        fn observations(&self) -> Vec<OpenObservation> {
            self.observations.lock().unwrap().clone()
        }

        fn install_replay(&self, schedule: Schedule<OpenAction>) {
            *self.replay.lock().unwrap() = Some(schedule);
        }

        fn replay_evidence(&self) -> (u64, Vec<Point>, bool) {
            let replay = self.replay.lock().unwrap();
            let replay = replay.as_ref().expect("a replay schedule was installed");
            (replay.seed(), replay.events(), replay.is_exhausted())
        }
    }

    impl fmt::Debug for FakeReadRowsRpc {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("FakeReadRowsRpc")
                .field("remaining_actions", &self.actions.lock().unwrap().len())
                .finish()
        }
    }

    #[tonic::async_trait]
    impl ReadRowsRpc for FakeReadRowsRpc {
        async fn open(
            &self,
            _stream_ordinal: usize,
            request: ReadRowsRequest,
            context: RequestContext,
        ) -> Result<ReadRowsResponseStream, StorageTransportError> {
            self.observations.lock().unwrap().push(OpenObservation {
                offset: request.offset,
                attempt: context.attempt(),
                stream_ordinal: context.stream_ordinal().unwrap(),
            });
            let action = if let Some(replay) = self.replay.lock().unwrap().as_mut() {
                replay
                    .take(Point {
                        phase: Phase::ReadOpen,
                        session: 1,
                        stream: context.stream_ordinal().unwrap(),
                        attempt: context.attempt(),
                        requested_offset: request.offset,
                        response: 0,
                        accepted_rows: request.offset,
                    })
                    .expect("the replay schedule matches this ReadRows request")
            } else {
                self.actions
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("the fake ReadRows script has an action")
            };
            match action {
                OpenAction::Responses(responses) => Ok(Box::pin(stream::iter(responses))),
                OpenAction::Status(status) => Err(StorageTransportError::Status(status)),
                OpenAction::Pending(dropped) => {
                    let _guard = DropFlag(dropped);
                    futures::future::pending().await
                }
                OpenAction::PendingMessages(dropped) => {
                    let pending = stream::unfold(DropFlag(dropped), |_guard| async move {
                        futures::future::pending::<
                            Option<(Result<ReadRowsResponse, tonic::Status>, DropFlag)>,
                        >()
                        .await
                    });
                    Ok(Box::pin(pending))
                }
            }
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug, Default)]
    struct TestClock(AtomicU64);

    impl TestClock {
        fn advance(&self, duration: Duration) {
            self.0.fetch_add(
                u64::try_from(duration.as_nanos()).unwrap(),
                Ordering::SeqCst,
            );
        }
    }

    impl MonotonicClock for TestClock {
        fn elapsed(&self) -> Duration {
            Duration::from_nanos(self.0.load(Ordering::SeqCst))
        }
    }

    #[derive(Debug)]
    struct TestSleeper {
        clock: Arc<TestClock>,
        delays: Mutex<Vec<Duration>>,
    }

    #[tonic::async_trait]
    impl Sleeper for TestSleeper {
        async fn sleep(&self, duration: Duration) {
            self.delays.lock().unwrap().push(duration);
            self.clock.advance(duration);
        }
    }

    #[derive(Debug)]
    struct MaximumJitter;

    impl JitterSource for MaximumJitter {
        fn sample(&self) -> u64 {
            u64::MAX
        }
    }

    #[derive(Debug)]
    struct PendingSleeper {
        entered: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    #[tonic::async_trait]
    impl Sleeper for PendingSleeper {
        async fn sleep(&self, _duration: Duration) {
            self.entered.store(true, Ordering::SeqCst);
            let _guard = DropFlag(Arc::clone(&self.dropped));
            futures::future::pending().await
        }
    }

    #[derive(Debug)]
    struct BlockingDecodeTestGate {
        entered: AtomicBool,
        completed: AtomicBool,
        released: (Mutex<bool>, std::sync::Condvar),
    }

    impl BlockingDecodeTestGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                released: (Mutex::new(false), std::sync::Condvar::new()),
            })
        }

        fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }

    impl BlockingDecodeGate for BlockingDecodeTestGate {
        fn enter(&self, phase: BlockingDecodePhase) {
            if phase != BlockingDecodePhase::Prepare {
                return;
            }
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
            self.completed.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    enum StreamFaultAction {
        Fail(RetryReason),
        Observe,
    }

    struct ScheduledStreamFaultGate {
        replay: Mutex<Schedule<StreamFaultAction>>,
    }

    impl ScheduledStreamFaultGate {
        fn new(seed: u64, phase: Phase, action: StreamFaultAction) -> Arc<Self> {
            Arc::new(Self {
                replay: Mutex::new(Schedule::new(
                    seed,
                    [Step {
                        selector: Selector {
                            phase: Some(phase),
                            session: Some(1),
                            stream: Some(0),
                            ..Selector::default()
                        },
                        action,
                    }],
                )),
            })
        }

        fn evidence(&self) -> (u64, Vec<Point>, bool) {
            let replay = self.replay.lock().unwrap();
            (replay.seed(), replay.events(), replay.is_exhausted())
        }
    }

    impl fmt::Debug for ScheduledStreamFaultGate {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            let replay = self.replay.lock().unwrap();
            formatter
                .debug_struct("ScheduledStreamFaultGate")
                .field("seed", &replay.seed())
                .field("event_count", &replay.events().len())
                .finish()
        }
    }

    impl StreamFaultGate for ScheduledStreamFaultGate {
        fn transition(&self, point: Point) -> Result<(), RetryReason> {
            match self.replay.lock().unwrap().take(point) {
                Some(StreamFaultAction::Fail(reason)) => Err(reason),
                Some(StreamFaultAction::Observe) | None => Ok(()),
            }
        }
    }

    struct Fixture {
        partition: ReadPartition,
        response: ReadRowsResponse,
        rpc: Arc<FakeReadRowsRpc>,
        clock: Arc<TestClock>,
        pool: Arc<GreedyMemoryPool>,
        metrics: ExecutionPlanMetricsSet,
    }

    fn fixture(actions: Vec<OpenAction>) -> Fixture {
        let arrow = documented_mapping_fixture();
        let schema_bytes = encode_schema(&arrow.schema);
        let batch_bytes = encode_batch(&arrow.batch, None);
        let limit = DecodeLimit::new(256 * 1024 * 1024).unwrap();
        let session_schema = SessionSchema::from_serialized(&schema_bytes, limit).unwrap();
        let response = ReadRowsResponse {
            row_count: i64::try_from(arrow.batch.num_rows()).unwrap(),
            rows: Some(read_rows_response::Rows::ArrowRecordBatch(
                ArrowRecordBatch {
                    serialized_record_batch: batch_bytes,
                    #[allow(deprecated)]
                    row_count: i64::try_from(arrow.batch.num_rows()).unwrap(),
                },
            )),
            ..Default::default()
        };
        let rpc = FakeReadRowsRpc::new(actions);
        let clock = Arc::new(TestClock::default());
        let sleeper = Arc::new(TestSleeper {
            clock: Arc::clone(&clock),
            delays: Mutex::new(Vec::new()),
        });
        let shared_rpc = Arc::clone(&rpc);
        let shared_clock = Arc::clone(&clock);
        let resources = Arc::new(StreamResources {
            rpc: shared_rpc,
            clock: shared_clock,
            sleeper,
            jitter: Arc::new(MaximumJitter),
            decode_limit: limit,
            decode_permits: Arc::new(tokio::sync::Semaphore::new(2)),
            blocking_decode_gate: Arc::new(OpenBlockingDecodeGate),
            connection_count: 1,
            fault_gate: Arc::new(OpenStreamFaultGate),
        });
        let pool = Arc::new(GreedyMemoryPool::new(512 * 1024 * 1024));
        let memory_pool = Arc::clone(&pool);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()
            .unwrap();
        let task_context = Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
        let output_schema = arrow.schema;
        let projection = (0..output_schema.fields().len()).collect::<Vec<_>>().into();
        let metrics = ExecutionPlanMetricsSet::new();
        let partition = ReadPartition {
            ordinal: 0,
            stream_name: StreamName::for_test("projects/p/locations/us/sessions/s/streams/0"),
            session_schema,
            session_deadline: Duration::from_secs(6 * 60 * 60),
            output_schema,
            batch_projection: projection,
            resources,
            args: BigQueryInputArgs::for_test(),
            metrics: metrics.clone(),
            task_context,
        };
        Fixture {
            partition,
            response,
            rpc,
            clock,
            pool,
            metrics,
        }
    }

    #[tokio::test]
    async fn demand_opens_only_when_polled_and_holds_memory_until_next_poll() {
        let fixture = fixture(Vec::new());
        fixture
            .rpc
            .actions
            .lock()
            .unwrap()
            .push_back(OpenAction::Responses(vec![Ok(fixture.response.clone())]));
        let mut batches = read_rows_stream(fixture.partition).unwrap();

        assert!(fixture.rpc.observations().is_empty());
        let batch = batches.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(fixture.rpc.observations().len(), 1);
        assert!(fixture.pool.reserved() > 0);

        assert!(batches.next().await.is_none());
        assert_eq!(fixture.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn multiple_failures_resume_the_same_stream_at_the_accepted_offset() {
        let fixture = fixture(Vec::new());
        fixture.rpc.install_replay(Schedule::new(
            0x0b15_5eed,
            [
                Step {
                    selector: Selector {
                        phase: Some(Phase::ReadOpen),
                        session: Some(1),
                        stream: Some(0),
                        attempt: Some(1),
                        requested_offset: Some(0),
                        response: Some(0),
                        accepted_rows: Some(0),
                    },
                    action: OpenAction::Status(tonic::Status::unavailable("first open")),
                },
                Step {
                    selector: Selector {
                        phase: Some(Phase::ReadOpen),
                        session: Some(1),
                        stream: Some(0),
                        attempt: Some(2),
                        requested_offset: Some(0),
                        response: Some(0),
                        accepted_rows: Some(0),
                    },
                    action: OpenAction::Responses(vec![
                        Ok(fixture.response.clone()),
                        Err(tonic::Status::unavailable("after output")),
                    ]),
                },
                Step {
                    selector: Selector {
                        phase: Some(Phase::ReadOpen),
                        session: Some(1),
                        stream: Some(0),
                        attempt: Some(3),
                        requested_offset: Some(2),
                        response: Some(0),
                        accepted_rows: Some(2),
                    },
                    action: OpenAction::Responses(vec![Ok(fixture.response.clone())]),
                },
            ],
        ));
        let batches = read_rows_stream(fixture.partition)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        assert_eq!(
            fixture.rpc.observations(),
            [
                OpenObservation {
                    offset: 0,
                    attempt: 1,
                    stream_ordinal: 0,
                },
                OpenObservation {
                    offset: 0,
                    attempt: 2,
                    stream_ordinal: 0,
                },
                OpenObservation {
                    offset: 2,
                    attempt: 3,
                    stream_ordinal: 0,
                },
            ]
        );
        let (seed, events, exhausted) = fixture.rpc.replay_evidence();
        assert_eq!(seed, 0x0b15_5eed);
        assert_eq!(events.len(), 3);
        assert!(exhausted);
        let mut oracle = AcceptedOffsetOracle::default();
        oracle.observe_request(events[0].requested_offset).unwrap();
        oracle.observe_request(events[1].requested_offset).unwrap();
        oracle.accept(2).unwrap();
        oracle.observe_request(events[2].requested_offset).unwrap();
        let names = fixture
            .metrics
            .clone_inner()
            .iter()
            .map(|metric| metric.value().name().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            names,
            [
                "decode_time",
                "decoded_arrow_bytes",
                "output_rows",
                "read_rows_responses",
                "read_rows_retries",
                "read_rows_rpc_attempts",
                "retry_delay_time",
                "serialized_bytes_received",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        );
    }

    #[tokio::test]
    async fn schedule_injects_every_read_transition_before_offset_acceptance() {
        let phases = [
            Phase::ReadOpen,
            Phase::ReadResponse,
            Phase::SerializedAdmission,
            Phase::DecodePermit,
            Phase::PrepareDecode,
            Phase::DecodedAdmission,
            Phase::Decode,
            Phase::AcceptOffset,
        ];

        for (index, phase) in phases.into_iter().enumerate() {
            let mut fixture = fixture(Vec::new());
            fixture
                .rpc
                .actions
                .lock()
                .unwrap()
                .push_back(OpenAction::Responses(vec![Ok(fixture.response.clone())]));
            let seed = 0xface_0000 + u64::try_from(index).unwrap();
            let gate = ScheduledStreamFaultGate::new(
                seed,
                phase,
                StreamFaultAction::Fail(RetryReason::LocalResource),
            );
            let installed_gate: Arc<dyn StreamFaultGate> =
                Arc::<ScheduledStreamFaultGate>::clone(&gate);
            Arc::make_mut(&mut fixture.partition.resources).fault_gate = installed_gate;
            let mut batches = read_rows_stream(fixture.partition).unwrap();

            let error = batches.next().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("accepted_offset=0"));
            assert!(error.to_string().contains("reason=local-resource"));
            assert!(batches.next().await.is_none());
            assert_eq!(fixture.pool.reserved(), 0);

            let (actual_seed, events, exhausted) = gate.evidence();
            assert_eq!(actual_seed, seed);
            assert!(exhausted, "fault for {phase:?} was not reached");
            assert_eq!(events.last().unwrap().phase, phase);
            let oracle = AcceptedOffsetOracle::default();
            for event in events {
                oracle.observe_request(event.requested_offset).unwrap();
                assert_eq!(event.accepted_rows, 0);
            }
        }
    }

    #[tokio::test]
    async fn schedule_injects_retry_delay_and_repeats_real_cancellation() {
        let mut retry = fixture(vec![OpenAction::Status(tonic::Status::unavailable(
            "retry",
        ))]);
        let retry_gate = ScheduledStreamFaultGate::new(
            0xface_1000,
            Phase::RetryDelay,
            StreamFaultAction::Fail(RetryReason::LocalResource),
        );
        let installed_gate: Arc<dyn StreamFaultGate> =
            Arc::<ScheduledStreamFaultGate>::clone(&retry_gate);
        Arc::make_mut(&mut retry.partition.resources).fault_gate = installed_gate;
        let mut batches = read_rows_stream(retry.partition).unwrap();
        let error = batches.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("accepted_offset=0"));
        assert!(error.to_string().contains("reason=local-resource"));
        assert!(retry_gate.evidence().2);

        for repetition in 0..16_u64 {
            let dropped = Arc::new(AtomicBool::new(false));
            let mut fixture = fixture(vec![OpenAction::Pending(Arc::clone(&dropped))]);
            let gate = ScheduledStreamFaultGate::new(
                0xface_2000 + repetition,
                Phase::Cancellation,
                StreamFaultAction::Observe,
            );
            let installed_gate: Arc<dyn StreamFaultGate> =
                Arc::<ScheduledStreamFaultGate>::clone(&gate);
            Arc::make_mut(&mut fixture.partition.resources).fault_gate = installed_gate;
            let mut batches = read_rows_stream(fixture.partition).unwrap();
            let task = tokio::spawn(async move { batches.next().await });

            while fixture.rpc.observations().is_empty() {
                tokio::task::yield_now().await;
            }
            task.abort();
            let _ = task.await;
            tokio::task::yield_now().await;

            assert!(dropped.load(Ordering::SeqCst));
            assert_eq!(fixture.pool.reserved(), 0);
            let (seed, events, exhausted) = gate.evidence();
            assert_eq!(seed, 0xface_2000 + repetition);
            assert!(exhausted);
            assert_eq!(events.last().unwrap().phase, Phase::Cancellation);
        }
    }

    #[tokio::test]
    async fn invalid_decode_is_terminal_before_offset_acceptance() {
        let fixture = fixture(Vec::new());
        let mut invalid = fixture.response.clone();
        let Some(read_rows_response::Rows::ArrowRecordBatch(batch)) = invalid.rows.as_mut() else {
            unreachable!()
        };
        batch.serialized_record_batch = vec![1, 2, 3];
        fixture
            .rpc
            .actions
            .lock()
            .unwrap()
            .push_back(OpenAction::Responses(vec![Ok(invalid)]));
        let mut batches = read_rows_stream(fixture.partition).unwrap();
        let error = batches.next().await.unwrap().unwrap_err();

        assert_eq!(
            error.to_string(),
            "External error: BigQuery Storage Read operation failed: \
             operation=storage.read-rows partition=0 attempt=1 \
             accepted_offset=0 reason=decode"
        );
        assert_eq!(fixture.rpc.observations()[0].offset, 0);
        assert_eq!(fixture.rpc.observations().len(), 1);
        assert!(batches.next().await.is_none());
        assert!(batches.next().await.is_none());
        let decode_time = fixture
            .metrics
            .clone_inner()
            .into_iter()
            .find(|metric| metric.value().name() == "decode_time")
            .unwrap();
        assert!(decode_time.value().as_usize() > 0);
    }

    #[test]
    fn accepted_offset_rejects_negative_values_and_overflow() {
        assert_eq!(AcceptedOffset::new(-1), Err(AcceptedOffsetError));
        assert_eq!(
            AcceptedOffset::new(i64::MAX).unwrap().checked_advance(1),
            Err(AcceptedOffsetError)
        );
    }

    #[tokio::test]
    async fn dropping_a_pending_partition_cancels_its_owned_rpc_future() {
        let dropped = Arc::new(AtomicBool::new(false));
        let fixture = fixture(vec![OpenAction::Pending(Arc::clone(&dropped))]);
        let mut batches = read_rows_stream(fixture.partition).unwrap();
        let task = tokio::spawn(async move { batches.next().await });

        while fixture.rpc.observations().is_empty() {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;

        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dropping_a_pending_message_wait_releases_the_rpc_stream() {
        let dropped = Arc::new(AtomicBool::new(false));
        let fixture = fixture(vec![OpenAction::PendingMessages(Arc::clone(&dropped))]);
        let mut batches = read_rows_stream(fixture.partition).unwrap();
        let task = tokio::spawn(async move { batches.next().await });

        while fixture.rpc.observations().is_empty() {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;

        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(fixture.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn dropping_a_decode_permit_wait_releases_reserved_memory() {
        let mut fixture = fixture(Vec::new());
        fixture
            .rpc
            .actions
            .lock()
            .unwrap()
            .push_back(OpenAction::Responses(vec![Ok(fixture.response.clone())]));
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        Arc::make_mut(&mut fixture.partition.resources).decode_permits = Arc::clone(&permits);
        let mut batches = read_rows_stream(fixture.partition).unwrap();
        let task = tokio::spawn(async move { batches.next().await });

        while fixture.pool.reserved() == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;
        permits.add_permits(1);

        assert_eq!(permits.available_permits(), 1);
        assert_eq!(fixture.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn dropping_a_retry_delay_wait_cancels_the_private_sleeper() {
        let mut fixture = fixture(vec![OpenAction::Status(tonic::Status::unavailable(
            "retry",
        ))]);
        let entered = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        Arc::make_mut(&mut fixture.partition.resources).sleeper = Arc::new(PendingSleeper {
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
        });
        let mut batches = read_rows_stream(fixture.partition).unwrap();
        let task = tokio::spawn(async move { batches.next().await });

        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;

        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(fixture.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn blocking_decode_finishes_bounded_work_after_stream_drop() {
        let mut fixture = fixture(Vec::new());
        fixture
            .rpc
            .actions
            .lock()
            .unwrap()
            .push_back(OpenAction::Responses(vec![Ok(fixture.response.clone())]));
        let gate = BlockingDecodeTestGate::new();
        let blocking_decode_gate = Arc::clone(&gate);
        Arc::make_mut(&mut fixture.partition.resources).blocking_decode_gate = blocking_decode_gate;
        let mut batches = read_rows_stream(fixture.partition).unwrap();
        let task = tokio::spawn(async move { batches.next().await });

        while !gate.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        assert_eq!(fixture.pool.reserved(), 0);
        gate.release();
        while !gate.completed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        assert_eq!(fixture.rpc.observations().len(), 1);
        assert_eq!(fixture.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn memory_admission_failure_is_terminal_before_offset_acceptance() {
        let mut fixture = fixture(Vec::new());
        fixture
            .rpc
            .actions
            .lock()
            .unwrap()
            .push_back(OpenAction::Responses(vec![Ok(fixture.response.clone())]));
        let pool = Arc::new(GreedyMemoryPool::new(1));
        let memory_pool = Arc::clone(&pool);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()
            .unwrap();
        fixture.partition.task_context =
            Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
        let mut batches = read_rows_stream(fixture.partition).unwrap();

        let error = batches.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("accepted_offset=0"));
        assert!(error.to_string().contains("reason=local-resource"));
        assert!(batches.next().await.is_none());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(fixture.rpc.observations()[0].offset, 0);
    }

    #[tokio::test]
    async fn effective_deadline_reports_the_boundary_that_exhausted() {
        let mut session_limited =
            fixture(vec![OpenAction::Pending(Arc::new(AtomicBool::new(false)))]);
        session_limited.partition.session_deadline = Duration::from_secs(61);
        let mut stream = read_rows_stream(session_limited.partition).unwrap();
        session_limited.clock.advance(Duration::from_secs(1));
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("reason=session-expired"));
        assert!(stream.next().await.is_none());

        let mut retry_limited =
            fixture(vec![OpenAction::Pending(Arc::new(AtomicBool::new(false)))]);
        retry_limited.partition.args.read_retry_window = Duration::from_secs(2);
        retry_limited.partition.args.read_idle_timeout = Duration::from_secs(1);
        let mut stream = read_rows_stream(retry_limited.partition).unwrap();
        retry_limited.clock.advance(Duration::from_secs(2));
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("reason=retry-budget-exhausted"));
        assert!(stream.next().await.is_none());
    }
}
