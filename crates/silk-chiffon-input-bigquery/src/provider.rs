//! Discovery-session table metadata and execution-session scan planning.

use std::{fmt, sync::Arc};

use anyhow::{Context, Result};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::DataFusionError,
    datasource::TableType,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};

use crate::{
    args::BigQueryInputArgs,
    execution::BigQueryReadExec,
    pushdown,
    reference::BigQueryReference,
    resources::CommandResources,
    session::{ReadSessionSpec, SourceIdentity},
    snapshot::PinnedSnapshot,
};

pub(crate) async fn create_provider(
    input: &str,
    session: &SessionContext,
    args: &BigQueryInputArgs,
) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(create_table_provider(input, session, args).await?))
}

async fn create_table_provider(
    input: &str,
    session: &SessionContext,
    args: &BigQueryInputArgs,
) -> Result<BigQueryTableProvider> {
    args.validate()
        .context("invalid BigQuery Storage Read options")?;
    let reference =
        BigQueryReference::parse(input).context("invalid BigQuery Storage Read input")?;
    let resources = args.resources(session).await?;
    let owner_project = args
        .session_project
        .clone()
        .unwrap_or_else(|| reference.table_project().to_owned());
    let snapshot = match reference.snapshot() {
        Some(snapshot) => snapshot,
        None => resources
            .server_clock
            .pin_snapshot(&owner_project)
            .await
            .context("failed to pin the BigQuery snapshot")?,
    };
    let discovery_spec = ReadSessionSpec::discovery(&reference, snapshot, &owner_project)
        .context("invalid BigQuery discovery session")?;
    let discovery = resources
        .sessions
        .open(&discovery_spec)
        .await
        .context("failed to discover the BigQuery table schema")?;

    Ok(BigQueryTableProvider {
        reference,
        owner_project,
        snapshot,
        discovery_identity: discovery.source_identity().clone(),
        discovery_location: discovery.location().to_owned(),
        schema: Arc::clone(discovery.schema().as_arrow()),
        resources,
        args: args.clone(),
    })
}

struct BigQueryTableProvider {
    reference: BigQueryReference,
    owner_project: String,
    snapshot: PinnedSnapshot,
    discovery_identity: SourceIdentity,
    discovery_location: String,
    schema: SchemaRef,
    resources: Arc<CommandResources>,
    args: BigQueryInputArgs,
}

impl BigQueryTableProvider {
    async fn plan_scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> datafusion::common::Result<BigQueryReadExec> {
        let projection = projection_request(&self.schema, projection)?;
        let row_restriction =
            pushdown::row_restriction(&self.schema, filters, self.args.row_restriction.as_deref());
        let requested_streams = self
            .args
            .max_stream_count
            .map(|count| count as usize)
            .unwrap_or_else(|| state.config().target_partitions())
            .max(1);
        let selected_fields = projection.selected_fields;
        let spec = ReadSessionSpec::execution(
            &self.reference,
            self.snapshot,
            &self.owner_project,
            &self.discovery_location,
            selected_fields.clone(),
            row_restriction,
            requested_streams,
            &self.args,
        )
        .map_err(external_error)?;
        let lease = self
            .resources
            .sessions
            .open(&spec)
            .await
            .map_err(external_error)?;
        validate_execution_schema(lease.schema().as_arrow(), &self.schema, &selected_fields)?;
        let batch_projection =
            batch_projection(lease.schema().as_arrow(), &projection.output_schema)?;

        Ok(BigQueryReadExec::new(
            lease,
            projection.output_schema,
            batch_projection,
            Arc::clone(&self.resources),
            &self.args,
        ))
    }
}

#[tonic::async_trait]
impl TableProvider for BigQueryTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self.plan_scan(state, projection, filters).await?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| pushdown::support(&self.schema, filter))
            .collect())
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        None
    }
}

impl fmt::Debug for BigQueryTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BigQueryTableProvider")
            .field("source_identity", &self.discovery_identity)
            .field("field_count", &self.schema.fields().len())
            .finish_non_exhaustive()
    }
}

struct ProjectionRequest {
    selected_fields: Vec<String>,
    output_schema: SchemaRef,
}

fn projection_request(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> datafusion::common::Result<ProjectionRequest> {
    match projection {
        None => Ok(ProjectionRequest {
            selected_fields: Vec::new(),
            output_schema: Arc::clone(schema),
        }),
        Some(indices) => {
            let output_schema = Arc::new(
                schema
                    .project(indices)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?,
            );
            let selected_fields = indices
                .iter()
                .map(|index| {
                    schema
                        .fields()
                        .get(*index)
                        .map(|field| field.name().clone())
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "BigQuery projection index {index} is out of bounds"
                            ))
                        })
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            let mut selected_fields =
                selected_fields
                    .into_iter()
                    .fold(Vec::new(), |mut fields, field| {
                        if !fields.contains(&field) {
                            fields.push(field);
                        }
                        fields
                    });
            if selected_fields.is_empty()
                && let Some(field) = schema.fields().first()
            {
                selected_fields.push(field.name().clone());
            }
            Ok(ProjectionRequest {
                selected_fields,
                output_schema,
            })
        }
    }
}

fn validate_execution_schema(
    returned: &Schema,
    discovered: &Schema,
    selected_fields: &[String],
) -> datafusion::common::Result<()> {
    let expected = if selected_fields.is_empty() {
        discovered.fields().iter().collect::<Vec<_>>()
    } else {
        discovered
            .fields()
            .iter()
            .filter(|field| selected_fields.contains(field.name()))
            .collect::<Vec<_>>()
    };
    if returned.fields().len() != expected.len()
        || returned
            .fields()
            .iter()
            .zip(expected)
            .any(|(returned, expected)| returned.as_ref() != expected.as_ref())
    {
        return Err(DataFusionError::External(Box::new(ExecutionSchemaError)));
    }
    Ok(())
}

fn batch_projection(returned: &Schema, output: &Schema) -> datafusion::common::Result<Vec<usize>> {
    output
        .fields()
        .iter()
        .map(|expected| {
            let index = returned
                .index_of(expected.name())
                .map_err(|_| DataFusionError::External(Box::new(ExecutionSchemaError)))?;
            if returned.field(index) != expected.as_ref() {
                return Err(DataFusionError::External(Box::new(ExecutionSchemaError)));
            }
            Ok(index)
        })
        .collect()
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Debug, thiserror::Error)]
#[error("BigQuery execution schema differs from the discovered schema")]
struct ExecutionSchemaError;

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field},
    };
    use axum::{Router, body::Body, response::Response as AxumResponse, routing::get};
    use datafusion::{logical_expr::col, physical_plan::limit::GlobalLimitExec};
    use futures::stream;
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use silk_chiffon_test_support::bigquery_arrow::{
        documented_mapping_fixture, encode_batch, encode_schema,
    };
    use tonic::{Request, Response, Status, service::Routes};

    use super::*;
    use crate::{
        auth::{AuthHeaders, CredentialError, CredentialsProvider, SharedCredentialsProvider},
        fault::{Phase, Point, Schedule, Selector, Step},
        proto::bigquery_storage::{
            ArrowRecordBatch, ArrowSchema, CreateReadSessionRequest, ReadRowsRequest,
            ReadRowsResponse, ReadSession, ReadStream, SplitReadStreamRequest,
            SplitReadStreamResponse,
            big_query_read_server::{BigQueryRead, BigQueryReadServer},
            read_rows_response, read_session,
        },
        resources::CommandResources,
        transport::RequestContext,
    };

    fn schema(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn assert_arrow_and_parquet_round_trip(
        batches: &[arrow::record_batch::RecordBatch],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!batches.is_empty(), "output proof needs one batch");
        let expected_rows = batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>();
        let output_schema = batches[0].schema();
        let directory = tempfile::tempdir()?;
        let arrow_path = directory.path().join("result.arrow");
        let parquet_path = directory.path().join("result.parquet");

        let mut arrow_writer =
            arrow::ipc::writer::FileWriter::try_new(File::create(&arrow_path)?, &output_schema)?;
        for batch in batches {
            arrow_writer.write(batch)?;
        }
        arrow_writer.finish()?;
        drop(arrow_writer);
        let arrow_rows = arrow::ipc::reader::FileReader::try_new(File::open(&arrow_path)?, None)?
            .try_fold(0_usize, |rows, batch| {
            batch.map(|batch| rows + batch.num_rows())
        })?;
        anyhow::ensure!(arrow_rows == expected_rows);

        let mut parquet_writer = ArrowWriter::try_new(
            File::create(&parquet_path)?,
            Arc::clone(&output_schema),
            None,
        )?;
        for batch in batches {
            parquet_writer.write(batch)?;
        }
        parquet_writer.close()?;
        let parquet_rows = ParquetRecordBatchReaderBuilder::try_new(File::open(&parquet_path)?)?
            .build()?
            .try_fold(0_usize, |rows, batch| {
                batch.map(|batch| rows + batch.num_rows())
            })?;
        anyhow::ensure!(parquet_rows == expected_rows);
        Ok(())
    }

    #[test]
    fn projection_pushdown_preserves_requested_output_order() {
        let full = schema(&["first", "second", "third"]);
        let request = projection_request(&full, Some(&vec![2, 0])).unwrap();
        let returned = schema(&["first", "third"]);

        assert_eq!(request.selected_fields, ["third", "first"]);
        assert_eq!(request.output_schema, schema(&["third", "first"]));
        assert_eq!(
            batch_projection(&returned, &request.output_schema).unwrap(),
            [1, 0]
        );
    }

    #[test]
    fn empty_projection_reads_one_field_but_outputs_zero_columns() {
        let full = schema(&["only"]);
        let request = projection_request(&full, Some(&Vec::new())).unwrap();

        assert_eq!(request.selected_fields, ["only"]);
        assert!(request.output_schema.fields().is_empty());
        assert!(
            batch_projection(&full, &request.output_schema)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn execution_schema_drift_is_terminal() {
        let output = schema(&["value"]);
        let returned = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));

        assert!(batch_projection(&returned, &output).is_err());
        assert!(validate_execution_schema(&returned, &output, &["value".to_owned()],).is_err());
    }

    #[test]
    fn repeated_projected_columns_are_read_once_and_repeated_locally() {
        let full = schema(&["first", "second"]);
        let request = projection_request(&full, Some(&vec![1, 1, 0])).unwrap();
        let returned = schema(&["first", "second"]);

        assert_eq!(request.selected_fields, ["second", "first"]);
        assert_eq!(
            batch_projection(&returned, &request.output_schema).unwrap(),
            [1, 1, 0]
        );
    }

    #[derive(Debug)]
    enum LifecycleFaultAction {
        Fail,
    }

    type LifecycleReplay = Arc<std::sync::Mutex<Schedule<LifecycleFaultAction>>>;

    struct CountingCredentials {
        calls: AtomicUsize,
        replay: LifecycleReplay,
    }

    impl fmt::Debug for CountingCredentials {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("CountingCredentials")
                .field("calls", &self.calls.load(Ordering::SeqCst))
                .finish()
        }
    }

    #[tonic::async_trait]
    impl CredentialsProvider for CountingCredentials {
        async fn headers(&self, context: &RequestContext) -> Result<AuthHeaders, CredentialError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self
                .replay
                .lock()
                .unwrap()
                .take(Point {
                    phase: Phase::Credentials,
                    session: 0,
                    stream: context.stream_ordinal().unwrap_or(0),
                    attempt: context.attempt(),
                    requested_offset: context.accepted_offset().unwrap_or(0),
                    response: 0,
                    accepted_rows: context.accepted_offset().unwrap_or(0),
                })
                .is_some()
            {
                return Err(CredentialError::transient_provider(std::io::Error::other(
                    "scripted transient credential failure",
                )));
            }
            let mut headers = http::HeaderMap::new();
            headers.insert(
                http::header::AUTHORIZATION,
                "Bearer fake-provider-token".parse().unwrap(),
            );
            Ok(AuthHeaders::new(headers))
        }
    }

    struct LifecycleFake {
        discovery_schema: Vec<u8>,
        execution_schema: Vec<u8>,
        execution_batch: Vec<u8>,
        creates: std::sync::Mutex<Vec<CreateReadSessionRequest>>,
        reads: std::sync::Mutex<Vec<ReadRowsRequest>>,
        create_attempts: AtomicUsize,
        discovery_attempts: AtomicUsize,
        execution_attempts: AtomicUsize,
        replay: LifecycleReplay,
    }

    impl fmt::Debug for LifecycleFake {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("LifecycleFake")
                .field("create_count", &self.creates.lock().unwrap().len())
                .field("read_count", &self.reads.lock().unwrap().len())
                .finish()
        }
    }

    #[tonic::async_trait]
    impl BigQueryRead for LifecycleFake {
        async fn create_read_session(
            &self,
            request: Request<CreateReadSessionRequest>,
        ) -> Result<Response<ReadSession>, Status> {
            assert!(request.metadata().contains_key("authorization"));
            let request = request.into_inner();
            self.create_attempts.fetch_add(1, Ordering::SeqCst);
            let requested = request.read_session.as_ref().unwrap();
            let discovery = requested
                .read_options
                .as_ref()
                .is_none_or(|options| options.selected_fields.is_empty());
            let (phase, session, attempt, session_id) = if discovery {
                (
                    Phase::DiscoverySession,
                    0,
                    self.discovery_attempts.fetch_add(1, Ordering::SeqCst) + 1,
                    "discovery",
                )
            } else {
                (
                    Phase::ExecutionSession,
                    1,
                    self.execution_attempts.fetch_add(1, Ordering::SeqCst) + 1,
                    "execution",
                )
            };
            if self
                .replay
                .lock()
                .unwrap()
                .take(Point {
                    phase,
                    session,
                    stream: 0,
                    attempt: u32::try_from(attempt).unwrap(),
                    requested_offset: 0,
                    response: 0,
                    accepted_rows: 0,
                })
                .is_some()
            {
                return Err(Status::unavailable("scripted CreateReadSession failure"));
            }
            self.creates.lock().unwrap().push(request.clone());
            let requested = request.read_session.unwrap();
            let name = format!("projects/p/locations/us/sessions/{session_id}");
            let schema = if discovery {
                self.discovery_schema.clone()
            } else {
                self.execution_schema.clone()
            };
            Ok(Response::new(ReadSession {
                name: name.clone(),
                expire_time: Some(prost_types::Timestamp {
                    seconds: 2_000_000_000,
                    nanos: 0,
                }),
                streams: vec![ReadStream {
                    name: format!("{name}/streams/0"),
                }],
                schema: Some(read_session::Schema::ArrowSchema(ArrowSchema {
                    serialized_schema: schema,
                })),
                estimated_row_count: 1,
                estimated_total_bytes_scanned: 128,
                estimated_total_physical_file_size: 256,
                ..requested
            }))
        }

        type ReadRowsStream = stream::Iter<std::vec::IntoIter<Result<ReadRowsResponse, Status>>>;

        async fn read_rows(
            &self,
            request: Request<ReadRowsRequest>,
        ) -> Result<Response<Self::ReadRowsStream>, Status> {
            assert!(request.metadata().contains_key("authorization"));
            let request = request.into_inner();
            self.reads.lock().unwrap().push(request);
            let response = ReadRowsResponse {
                row_count: 1,
                rows: Some(read_rows_response::Rows::ArrowRecordBatch(
                    ArrowRecordBatch {
                        serialized_record_batch: self.execution_batch.clone(),
                        #[allow(deprecated)]
                        row_count: 1,
                    },
                )),
                ..Default::default()
            };
            Ok(Response::new(stream::iter(vec![Ok(response)])))
        }

        async fn split_read_stream(
            &self,
            _request: Request<SplitReadStreamRequest>,
        ) -> Result<Response<SplitReadStreamResponse>, Status> {
            Err(Status::unimplemented("the connector never splits streams"))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scheduled_discovery_and_execution_faults_recover_on_one_pinned_source() {
        let fixture = documented_mapping_fixture();
        let string_index = fixture.schema.index_of("string").unwrap();
        let execution_schema = Arc::new(Schema::new(vec![
            fixture.schema.field(string_index).clone(),
        ]));
        let execution_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&execution_schema),
            vec![fixture.batch.column(string_index).slice(0, 1)],
        )
        .unwrap();
        let replay = Arc::new(std::sync::Mutex::new(Schedule::new(
            0xd15c_0bed,
            [
                Step {
                    selector: Selector {
                        phase: Some(Phase::Credentials),
                        attempt: Some(1),
                        ..Selector::default()
                    },
                    action: LifecycleFaultAction::Fail,
                },
                Step {
                    selector: Selector {
                        phase: Some(Phase::ServerClock),
                        attempt: Some(1),
                        ..Selector::default()
                    },
                    action: LifecycleFaultAction::Fail,
                },
                Step {
                    selector: Selector {
                        phase: Some(Phase::DiscoverySession),
                        session: Some(0),
                        attempt: Some(1),
                        ..Selector::default()
                    },
                    action: LifecycleFaultAction::Fail,
                },
                Step {
                    selector: Selector {
                        phase: Some(Phase::ExecutionSession),
                        session: Some(1),
                        attempt: Some(1),
                        ..Selector::default()
                    },
                    action: LifecycleFaultAction::Fail,
                },
            ],
        )));
        let service = Arc::new(LifecycleFake {
            discovery_schema: encode_schema(&fixture.schema),
            execution_schema: encode_schema(&execution_schema),
            execution_batch: encode_batch(&execution_batch, None),
            creates: std::sync::Mutex::new(Vec::new()),
            reads: std::sync::Mutex::new(Vec::new()),
            create_attempts: AtomicUsize::new(0),
            discovery_attempts: AtomicUsize::new(0),
            execution_attempts: AtomicUsize::new(0),
            replay: Arc::clone(&replay),
        });
        let rest_calls = Arc::new(AtomicUsize::new(0));
        let rest_calls_for_route = Arc::clone(&rest_calls);
        let replay_for_route = Arc::clone(&replay);
        let grpc = BigQueryReadServer::from_arc(Arc::clone(&service));
        let router = Routes::new(grpc).into_axum_router().route(
            "/bigquery/v2/projects/{project}/datasets",
            get(move || {
                let rest_calls = Arc::clone(&rest_calls_for_route);
                let replay = Arc::clone(&replay_for_route);
                async move {
                    let attempt = rest_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let failed = replay
                        .lock()
                        .unwrap()
                        .take(Point {
                            phase: Phase::ServerClock,
                            session: 0,
                            stream: 0,
                            attempt: u32::try_from(attempt).unwrap(),
                            requested_offset: 0,
                            response: 0,
                            accepted_rows: 0,
                        })
                        .is_some();
                    let mut response = AxumResponse::builder()
                        .header(http::header::CONTENT_TYPE, "application/json");
                    if failed {
                        response = response
                            .status(http::StatusCode::SERVICE_UNAVAILABLE)
                            .header(http::header::DATE, "invalid");
                    } else {
                        response =
                            response.header(http::header::DATE, "Sat, 15 Aug 2026 12:00:00 GMT");
                    }
                    response.body(Body::from("{}")).unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().merge(router))
                .await
                .unwrap();
        });

        let session = SessionContext::new();
        let mut args = BigQueryInputArgs::for_test();
        args.endpoint = Some(format!("http://{address}").parse().unwrap());
        args.max_stream_count = Some(1);
        args.row_restriction = Some("`string` IS NOT NULL".to_owned());
        let credentials = Arc::new(CountingCredentials {
            calls: AtomicUsize::new(0),
            replay: Arc::clone(&replay),
        });
        let shared_credentials = Arc::clone(&credentials);
        let shared_credentials: SharedCredentialsProvider = shared_credentials;
        let resources =
            CommandResources::initialize_with_credentials(&args, shared_credentials, None)
                .await
                .unwrap();
        assert_eq!(resources.decode_permits.available_permits(), 2);
        args.set_test_resources(resources);

        let provider = create_provider("bqs:///projects/p/datasets/d/tables/t", &session, &args)
            .await
            .unwrap();
        assert_eq!(service.creates.lock().unwrap().len(), 1);
        assert!(service.reads.lock().unwrap().is_empty());
        session.register_table("data", provider).unwrap();
        let dataframe = session
            .sql("SELECT string FROM data WHERE int64 > 0")
            .await
            .unwrap();
        let plan = dataframe.create_physical_plan().await.unwrap();
        let batches = datafusion::physical_plan::collect(Arc::clone(&plan), session.task_ctx())
            .await
            .unwrap();
        let repeated = datafusion::physical_plan::collect(plan, session.task_ctx())
            .await
            .unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        assert_eq!(
            repeated.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "alpha");
        assert_arrow_and_parquet_round_trip(&batches).unwrap();
        assert_eq!(rest_calls.load(Ordering::SeqCst), 2);
        assert_eq!(service.creates.lock().unwrap().len(), 2);
        assert_eq!(service.create_attempts.load(Ordering::SeqCst), 4);
        assert_eq!(service.reads.lock().unwrap().len(), 2);
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 9);

        let replay = replay.lock().unwrap();
        assert_eq!(replay.seed(), 0xd15c_0bed);
        assert!(replay.is_exhausted());
        for phase in [
            Phase::Credentials,
            Phase::ServerClock,
            Phase::DiscoverySession,
            Phase::ExecutionSession,
        ] {
            assert!(replay.events().iter().any(|point| point.phase == phase));
        }

        let creates = service.creates.lock().unwrap();
        let discovery = creates[0].read_session.as_ref().unwrap();
        let execution = creates[1].read_session.as_ref().unwrap();
        assert_eq!(discovery.table_modifiers, execution.table_modifiers);
        assert_eq!(creates[1].max_stream_count, 1);
        let options = execution.read_options.as_ref().unwrap();
        assert_eq!(options.selected_fields, ["string"]);
        assert_eq!(
            options.row_restriction,
            "(`int64` > 0) AND (`string` IS NOT NULL)"
        );
        assert!(creates[1].parent.ends_with("/p"));
        assert!(
            service.reads.lock().unwrap()[0]
                .read_stream
                .contains("/locations/us/sessions/execution/")
        );
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires explicit live BigQuery acknowledgement and ADC"]
    async fn live_small_table_writes_arrow_and_parquet() {
        fn required(name: &str) -> String {
            std::env::var(name).unwrap_or_else(|_| panic!("live BigQuery test requires {name}"))
        }

        assert_eq!(
            required("SILK_CHIFFON_BQS_LIVE_ACKNOWLEDGE_COST"),
            "1",
            "live BigQuery cost acknowledgement must equal 1"
        );
        let session_project = required("SILK_CHIFFON_BQS_LIVE_SESSION_PROJECT");
        let table_project = required("SILK_CHIFFON_BQS_LIVE_TABLE_PROJECT");
        let dataset = required("SILK_CHIFFON_BQS_LIVE_DATASET");
        let table = required("SILK_CHIFFON_BQS_LIVE_TABLE");
        let location = required("SILK_CHIFFON_BQS_LIVE_EXPECTED_LOCATION");
        let max_estimated_bytes = required("SILK_CHIFFON_BQS_LIVE_MAX_ESTIMATED_BYTES")
            .parse::<u64>()
            .expect("live BigQuery byte guard must be a positive integer");
        assert!(
            max_estimated_bytes > 0,
            "live BigQuery byte guard must be positive"
        );
        let quota_project = std::env::var("SILK_CHIFFON_BQS_LIVE_QUOTA_PROJECT").ok();

        tokio::time::timeout(Duration::from_secs(120), async move {
            let session = SessionContext::new_with_config(
                datafusion::prelude::SessionConfig::new()
                    .with_target_partitions(1),
            );
            let mut args = BigQueryInputArgs::for_test();
            args.session_project = Some(session_project);
            args.quota_project = quota_project;
            args.max_stream_count = Some(1);
            let reference = format!(
                "bqs:///projects/{table_project}/datasets/{dataset}/tables/{table}?location={location}"
            );
            let provider =
                create_table_provider(&reference, &session, &args).await?;
            let schema = provider.schema();
            let (field_index, field) = schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| {
                    matches!(
                        field.data_type(),
                        DataType::Boolean
                            | DataType::Int64
                            | DataType::Float64
                            | DataType::Utf8
                            | DataType::Binary
                            | DataType::Date32
                            | DataType::Time64(
                                arrow::datatypes::TimeUnit::Microsecond
                            )
                            | DataType::Timestamp(
                                arrow::datatypes::TimeUnit::Microsecond
                                    | arrow::datatypes::TimeUnit::Nanosecond,
                                _,
                            )
                            | DataType::Decimal128(_, _)
                            | DataType::Decimal256(_, _)
                    )
                })
                .expect("live fixture needs one exactly filterable field");
            let filter = col(field.name()).is_not_null();
            assert_eq!(
                provider.supports_filters_pushdown(&[&filter])?,
                [TableProviderFilterPushDown::Exact]
            );
            let projection = vec![field_index];
            let state = session.state();
            let execution = provider
                .plan_scan(&state, Some(&projection), &[filter])
                .await?;
            let estimate = execution.estimated_total_bytes_scanned();
            anyhow::ensure!(
                estimate > 0,
                "live read refused because BigQuery returned no positive byte estimate"
            );
            anyhow::ensure!(
                estimate <= max_estimated_bytes,
                "live read refused because BigQuery byte estimate exceeds the configured guard"
            );
            let scan: Arc<dyn ExecutionPlan> = Arc::new(execution);
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(GlobalLimitExec::new(scan, 0, Some(100)));
            let batches = datafusion::physical_plan::collect(
                plan,
                session.task_ctx(),
            )
            .await?;
            let expected_rows = batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>();
            anyhow::ensure!(
                expected_rows > 0,
                "live fixture projection/filter returned no rows"
            );
            assert_arrow_and_parquet_round_trip(&batches)?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .expect("live BigQuery test exceeded its 120-second wall-clock bound")
        .unwrap();
    }
}
