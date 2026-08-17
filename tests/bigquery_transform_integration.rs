#![cfg(feature = "bigquery-integration-tests")]

use std::{
    collections::HashMap,
    fmt,
    fs::File,
    io::Read,
    pin::Pin,
    process::{Command, Output, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::reader::FileReader as ArrowFileReader,
    record_batch::RecordBatch,
};
use axum::{
    Json, Router,
    routing::{get, post},
};
use futures::{Stream, stream};
use silk_chiffon_input_bigquery::integration_test_support::{
    ArrowRecordBatch, ArrowSchema, CreateReadSessionRequest, ReadRowsRequest, ReadRowsResponse,
    ReadSession, ReadStream, SplitReadStreamRequest, SplitReadStreamResponse,
    big_query_read_server::{BigQueryRead, BigQueryReadServer},
    read_rows_response, read_session,
};
use silk_chiffon_test_support::{
    TestFile,
    bigquery_arrow::{encode_batch, encode_schema},
};
use tempfile::TempDir;
use tonic::{Request, Response, Status, service::Routes};

#[derive(Clone)]
struct StreamFixture {
    batch: Vec<u8>,
    rows: usize,
    pending_after_batch: bool,
}

struct LifecycleFake {
    full_batch: RecordBatch,
    creates: Mutex<Vec<CreateReadSessionRequest>>,
    table_create_counts: Mutex<HashMap<String, usize>>,
    streams: Mutex<HashMap<String, StreamFixture>>,
    reads: Mutex<Vec<ReadRowsRequest>>,
    pending_stream_dropped: Arc<AtomicBool>,
}

impl LifecycleFake {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            full_batch: bigquery_batch(&[1, 2], &["alpha", "beta"]),
            creates: Mutex::new(Vec::new()),
            table_create_counts: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            reads: Mutex::new(Vec::new()),
            pending_stream_dropped: Arc::new(AtomicBool::new(false)),
        })
    }

    fn execution_batch(&self, request: &ReadSession) -> RecordBatch {
        let selected = request
            .read_options
            .as_ref()
            .map(|options| options.selected_fields.as_slice())
            .unwrap_or_default();
        if selected.is_empty() {
            return self.full_batch.clone();
        }
        let schema = self.full_batch.schema();
        let indices = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| selected.contains(field.name()).then_some(index))
            .collect::<Vec<_>>();
        self.full_batch.project(&indices).unwrap()
    }
}

fn bigquery_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

impl fmt::Debug for LifecycleFake {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleFake")
            .field("create_count", &self.creates.lock().unwrap().len())
            .field("read_count", &self.reads.lock().unwrap().len())
            .finish_non_exhaustive()
    }
}

struct ResponsesThenPending {
    response: ReadRowsResponse,
    remaining: usize,
    dropped: Arc<AtomicBool>,
}

impl Stream for ResponsesThenPending {
    type Item = Result<ReadRowsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            Poll::Pending
        } else {
            self.remaining -= 1;
            Poll::Ready(Some(Ok(self.response.clone())))
        }
    }
}

impl Drop for ResponsesThenPending {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ReadRowsResponse, Status>> + Send>>;

#[tonic::async_trait]
impl BigQueryRead for LifecycleFake {
    async fn create_read_session(
        &self,
        request: Request<CreateReadSessionRequest>,
    ) -> Result<Response<ReadSession>, Status> {
        assert!(request.metadata().contains_key("authorization"));
        let request = request.into_inner();
        let requested = request.read_session.clone().unwrap();
        let table = requested.table.clone();
        let table_id = table.rsplit('/').next().unwrap().to_owned();
        let table_ordinal = {
            let mut counts = self.table_create_counts.lock().unwrap();
            let ordinal = *counts.entry(table.clone()).or_default();
            counts.insert(table, ordinal + 1);
            ordinal
        };
        let discovery = table_ordinal.is_multiple_of(2);
        let batch = if discovery {
            self.full_batch.clone()
        } else {
            self.execution_batch(&requested)
        };
        let session_name = format!("projects/p/locations/us/sessions/{table_id}-{table_ordinal}");
        let empty = table_id == "empty";
        let streams = if empty {
            Vec::new()
        } else {
            let stream_name = format!("{session_name}/streams/0");
            self.streams.lock().unwrap().insert(
                stream_name.clone(),
                StreamFixture {
                    batch: encode_batch(&batch, None),
                    rows: batch.num_rows(),
                    pending_after_batch: table_id == "pending",
                },
            );
            vec![ReadStream { name: stream_name }]
        };
        self.creates.lock().unwrap().push(request);

        Ok(Response::new(ReadSession {
            name: session_name,
            expire_time: Some(prost_types::Timestamp {
                seconds: 2_000_000_000,
                nanos: 0,
            }),
            streams,
            schema: Some(read_session::Schema::ArrowSchema(ArrowSchema {
                serialized_schema: encode_schema(batch.schema().as_ref()),
            })),
            estimated_row_count: if empty {
                0
            } else {
                i64::try_from(batch.num_rows()).unwrap()
            },
            estimated_total_bytes_scanned: if empty { 0 } else { 128 },
            estimated_total_physical_file_size: if empty { 0 } else { 256 },
            ..requested
        }))
    }

    type ReadRowsStream = ResponseStream;

    async fn read_rows(
        &self,
        request: Request<ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        assert!(request.metadata().contains_key("authorization"));
        let request = request.into_inner();
        assert_eq!(request.offset, 0);
        let fixture = self
            .streams
            .lock()
            .unwrap()
            .get(&request.read_stream)
            .cloned()
            .unwrap();
        self.reads.lock().unwrap().push(request);
        let response = ReadRowsResponse {
            row_count: i64::try_from(fixture.rows).unwrap(),
            rows: Some(read_rows_response::Rows::ArrowRecordBatch(
                ArrowRecordBatch {
                    serialized_record_batch: fixture.batch,
                    #[allow(deprecated)]
                    row_count: i64::try_from(fixture.rows).unwrap(),
                },
            )),
            ..Default::default()
        };
        let stream: ResponseStream = if fixture.pending_after_batch {
            Box::pin(ResponsesThenPending {
                response,
                remaining: 8,
                dropped: Arc::clone(&self.pending_stream_dropped),
            })
        } else {
            Box::pin(stream::iter(vec![Ok(response)]))
        };
        Ok(Response::new(stream))
    }

    async fn split_read_stream(
        &self,
        _request: Request<SplitReadStreamRequest>,
    ) -> Result<Response<SplitReadStreamResponse>, Status> {
        Err(Status::unimplemented(
            "Silk never splits Storage Read streams",
        ))
    }
}

struct RootFixture {
    service: Arc<LifecycleFake>,
    endpoint: String,
    credentials_path: std::path::PathBuf,
    _temp_dir: TempDir,
    server: tokio::task::JoinHandle<()>,
}

impl RootFixture {
    async fn start() -> Self {
        let service = LifecycleFake::new();
        let grpc = BigQueryReadServer::from_arc(Arc::clone(&service));
        let router = Routes::new(grpc)
            .into_axum_router()
            .route(
                "/token",
                post(|| async {
                    Json(serde_json::json!({
                        "access_token": "root-integration-token",
                        "expires_in": 3600,
                        "token_type": "Bearer"
                    }))
                }),
            )
            .route(
                "/bigquery/v2/projects/{project}/datasets",
                get(|| async {
                    (
                        [
                            (http::header::DATE, "Sat, 15 Aug 2026 12:00:00 GMT"),
                            (http::header::CONTENT_TYPE, "application/json"),
                        ],
                        "{}",
                    )
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}");
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().merge(router))
                .await
                .unwrap();
        });

        let temp_dir = tempfile::tempdir().unwrap();
        let credentials_path = temp_dir.path().join("adc.json");
        std::fs::write(
            &credentials_path,
            serde_json::to_vec(&serde_json::json!({
                "client_id": "root-test-client",
                "client_secret": "root-test-secret",
                "refresh_token": "root-test-refresh",
                "type": "authorized_user",
                "token_uri": format!("{endpoint}/token")
            }))
            .unwrap(),
        )
        .unwrap();

        Self {
            service,
            endpoint,
            credentials_path,
            _temp_dir: temp_dir,
            server,
        }
    }

    async fn run(&self, arguments: &[&str]) -> Output {
        let binary = assert_cmd::cargo::cargo_bin!("silk-chiffon");
        let credentials_path = self.credentials_path.clone();
        let arguments = arguments
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            let mut child = Command::new(binary)
                .args(arguments)
                .env("GOOGLE_APPLICATION_CREDENTIALS", credentials_path)
                .env("NO_PROXY", "*")
                .env("no_proxy", "*")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    let mut stdout = Vec::new();
                    let mut stderr = Vec::new();
                    child
                        .stdout
                        .take()
                        .unwrap()
                        .read_to_end(&mut stdout)
                        .unwrap();
                    child
                        .stderr
                        .take()
                        .unwrap()
                        .read_to_end(&mut stderr)
                        .unwrap();
                    return Output {
                        status,
                        stdout,
                        stderr,
                    };
                }
                if Instant::now() >= deadline {
                    child.kill().unwrap();
                    let _ = child.wait();
                    panic!("silk-chiffon subprocess exceeded the offline test deadline");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        })
        .await
        .unwrap()
    }

    fn common_arguments(&self, table: &str) -> Vec<String> {
        vec![
            "transform".to_owned(),
            "--from".to_owned(),
            format!("bqs:///projects/p/datasets/d/tables/{table}"),
            "--bqs-endpoint".to_owned(),
            self.endpoint.clone(),
            "--bqs-max-stream-count".to_owned(),
            "1".to_owned(),
            "--thread-budget".to_owned(),
            "2".to_owned(),
        ]
    }
}

impl Drop for RootFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn argument_refs(arguments: &[String]) -> Vec<&str> {
    arguments.iter().map(String::as_str).collect()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_bqs_exercises_every_output_format_and_partitioned_output() {
    let fixture = RootFixture::start().await;
    let output_dir = tempfile::tempdir().unwrap();

    let arrow_output = output_dir.path().join("query.arrow");
    let mut arguments = fixture.common_arguments("table");
    arguments.extend([
        "--to".to_owned(),
        arrow_output.to_string_lossy().into_owned(),
        "--output-format".to_owned(),
        "arrow".to_owned(),
        "--query".to_owned(),
        "SELECT name, id FROM data WHERE id > 0".to_owned(),
        "--bqs-row-restriction".to_owned(),
        "name IS NOT NULL".to_owned(),
    ]);
    let output = fixture.run(&argument_refs(&arguments)).await;
    assert_success(&output);
    let arrow_batches = TestFile::read_arrow(&arrow_output);
    assert_eq!(
        arrow_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    assert_eq!(arrow_batches[0].schema().field(0).name(), "name");
    assert_eq!(arrow_batches[0].schema().field(1).name(), "id");

    let parquet_output = output_dir.path().join("full.parquet");
    let mut arguments = fixture.common_arguments("table");
    arguments.extend([
        "--to".to_owned(),
        parquet_output.to_string_lossy().into_owned(),
        "--output-format".to_owned(),
        "parquet".to_owned(),
    ]);
    let output = fixture.run(&argument_refs(&arguments)).await;
    assert_success(&output);
    assert_eq!(
        TestFile::read_parquet(&parquet_output)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );

    let file_input = output_dir.path().join("local.arrow");
    TestFile::write_arrow_batch(&file_input, &bigquery_batch(&[3], &["gamma"]));
    let mixed_output = output_dir.path().join("mixed.parquet");
    let mut arguments = fixture.common_arguments("table");
    arguments.extend([
        "--from".to_owned(),
        file_input.to_string_lossy().into_owned(),
        "--to".to_owned(),
        mixed_output.to_string_lossy().into_owned(),
        "--output-format".to_owned(),
        "parquet".to_owned(),
    ]);
    let output = fixture.run(&argument_refs(&arguments)).await;
    assert_success(&output);
    assert_eq!(
        TestFile::read_parquet(&mixed_output)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );

    let stream_output = output_dir.path().join("full.arrows");
    let mut arguments = fixture.common_arguments("table");
    arguments.extend([
        "--to".to_owned(),
        stream_output.to_string_lossy().into_owned(),
        "--output-format".to_owned(),
        "arrow".to_owned(),
        "--arrow-format".to_owned(),
        "stream".to_owned(),
    ]);
    let output = fixture.run(&argument_refs(&arguments)).await;
    assert_success(&output);
    assert_eq!(
        TestFile::read_arrow_stream(&stream_output)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );

    let vortex_output = output_dir.path().join("full.vortex");
    let mut arguments = fixture.common_arguments("table");
    arguments.extend([
        "--to".to_owned(),
        vortex_output.to_string_lossy().into_owned(),
        "--output-format".to_owned(),
        "vortex".to_owned(),
    ]);
    let output = fixture.run(&argument_refs(&arguments)).await;
    assert_success(&output);
    let vortex_verification = output_dir.path().join("vortex-verification.arrow");
    let output = fixture
        .run(&[
            "transform",
            "--from",
            vortex_output.to_str().unwrap(),
            "--to",
            vortex_verification.to_str().unwrap(),
        ])
        .await;
    assert_success(&output);
    assert_eq!(
        TestFile::read_arrow(&vortex_verification)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );

    let partition_root = output_dir.path().join("partitioned");
    let partition_template = partition_root.join("{{name}}.parquet");
    let mut arguments = fixture.common_arguments("table");
    arguments.extend([
        "--to-many".to_owned(),
        partition_template.to_string_lossy().into_owned(),
        "--by".to_owned(),
        "name".to_owned(),
        "--output-format".to_owned(),
        "parquet".to_owned(),
    ]);
    let output = fixture.run(&argument_refs(&arguments)).await;
    assert_success(&output);
    let partition_rows = ["alpha.parquet", "beta.parquet"]
        .iter()
        .flat_map(|name| TestFile::read_parquet(&partition_root.join(name)))
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(partition_rows, 2);

    let creates = fixture.service.creates.lock().unwrap();
    assert_eq!(creates.len(), 12);
    for pair in creates.chunks_exact(2) {
        let discovery = pair[0].read_session.as_ref().unwrap();
        let execution = pair[1].read_session.as_ref().unwrap();
        assert_eq!(discovery.table_modifiers, execution.table_modifiers);
        assert!(execution.table_modifiers.is_some());
        assert_eq!(pair[1].max_stream_count, 1);
    }
    let first_execution = creates[1]
        .read_session
        .as_ref()
        .unwrap()
        .read_options
        .as_ref()
        .unwrap();
    assert_eq!(first_execution.selected_fields, ["id", "name"]);
    assert_eq!(
        first_execution.row_restriction,
        "(`id` > 0) AND (name IS NOT NULL)"
    );
    assert_eq!(fixture.service.reads.lock().unwrap().len(), 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_bqs_result_writes_a_schema_only_arrow_file_without_reading_rows() {
    let fixture = RootFixture::start().await;
    let output_dir = tempfile::tempdir().unwrap();
    let arrow_output = output_dir.path().join("empty.arrow");
    let mut arguments = fixture.common_arguments("empty");
    arguments.extend([
        "--to".to_owned(),
        arrow_output.to_string_lossy().into_owned(),
        "--output-format".to_owned(),
        "arrow".to_owned(),
    ]);

    let output = fixture.run(&argument_refs(&arguments)).await;
    assert_success(&output);

    let reader = ArrowFileReader::try_new(File::open(&arrow_output).unwrap(), None).unwrap();
    assert_eq!(reader.schema().fields().len(), 2);
    assert_eq!(reader.count(), 0);
    let creates = fixture.service.creates.lock().unwrap();
    assert_eq!(creates.len(), 2);
    assert!(creates.iter().all(|request| {
        request
            .read_session
            .as_ref()
            .is_some_and(|session| session.table.ends_with("/empty"))
    }));
    assert!(fixture.service.reads.lock().unwrap().is_empty());
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_failure_cancels_the_pending_bqs_partition() {
    let fixture = RootFixture::start().await;
    let mut arguments = fixture.common_arguments("pending");
    arguments.extend([
        "--to".to_owned(),
        "/dev/full".to_owned(),
        "--output-format".to_owned(),
        "arrow".to_owned(),
        "--overwrite".to_owned(),
        "--arrow-writing-queue-size".to_owned(),
        "1".to_owned(),
        "--object-store-upload-part-size".to_owned(),
        "1B".to_owned(),
    ]);
    let output = fixture.run(&argument_refs(&arguments)).await;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("file output failed")
            && stderr.contains("/dev/full")
            && stderr.contains("writer task died"),
        "{stderr}"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !fixture
        .service
        .pending_stream_dropped
        .load(Ordering::SeqCst)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the server-side ReadRows stream was not cancelled"
        );
        tokio::task::yield_now().await;
    }
}
