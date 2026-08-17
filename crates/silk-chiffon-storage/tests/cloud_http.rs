#![cfg(any(feature = "gcs", feature = "s3"))]

use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::Command;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header::AUTHORIZATION};
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use object_store::ObjectStoreExt;
use silk_chiffon_storage::{LocationInput, StorageRegistry, StorageSession};
use tokio::{
    net::TcpListener,
    sync::{Notify, oneshot},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Debug)]
struct RequestRecord {
    method: Method,
    uri: String,
    headers: HeaderMap,
}

struct ResponsePlan {
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    body: Bytes,
    delay: Duration,
}

impl ResponsePlan {
    fn new(status: StatusCode) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Bytes::new(),
            delay: Duration::ZERO,
        }
    }

    fn header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.push((
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        ));
        self
    }

    fn body(mut self, body: &'static str) -> Self {
        self.body = Bytes::from_static(body.as_bytes());
        self
    }

    fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

struct MockServer {
    endpoint: String,
    plans: Arc<Mutex<VecDeque<ResponsePlan>>>,
    requests: Arc<Mutex<Vec<RequestRecord>>>,
    active_requests: Arc<AtomicUsize>,
    request_finished: Arc<Notify>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

struct ActiveRequest {
    active_requests: Arc<AtomicUsize>,
    request_finished: Arc<Notify>,
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
        self.request_finished.notify_waiters();
    }
}

impl MockServer {
    async fn new() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let plans = Arc::new(Mutex::new(VecDeque::<ResponsePlan>::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let active_requests = Arc::new(AtomicUsize::new(0));
        let request_finished = Arc::new(Notify::new());
        let (shutdown, mut shutdown_receiver) = oneshot::channel();
        let server_plans = Arc::clone(&plans);
        let server_requests = Arc::clone(&requests);
        let server_active_requests = Arc::clone(&active_requests);
        let server_request_finished = Arc::clone(&request_finished);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = &mut shutdown_receiver => break,
                };
                let Ok((stream, _)) = accepted else {
                    break;
                };
                let plans = Arc::clone(&server_plans);
                let requests = Arc::clone(&server_requests);
                let active_requests = Arc::clone(&server_active_requests);
                let request_finished = Arc::clone(&server_request_finished);
                connections.spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let plans = Arc::clone(&plans);
                        let requests = Arc::clone(&requests);
                        let active_requests = Arc::clone(&active_requests);
                        let request_finished = Arc::clone(&request_finished);
                        async move {
                            active_requests.fetch_add(1, Ordering::SeqCst);
                            let _active_request = ActiveRequest {
                                active_requests,
                                request_finished,
                            };
                            requests.lock().unwrap().push(RequestRecord {
                                method: request.method().clone(),
                                uri: request.uri().to_string(),
                                headers: request.headers().clone(),
                            });
                            let plan = plans
                                .lock()
                                .unwrap()
                                .pop_front()
                                .unwrap_or_else(|| ResponsePlan::new(StatusCode::OK));
                            if !plan.delay.is_zero() {
                                tokio::time::sleep(plan.delay).await;
                            }
                            let mut response = Response::builder().status(plan.status);
                            for (name, value) in plan.headers {
                                response = response.header(name, value);
                            }
                            Ok::<_, Infallible>(response.body(Full::new(plan.body)).unwrap())
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });

        Self {
            endpoint,
            plans,
            requests,
            active_requests,
            request_finished,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    fn push(&self, plan: ResponsePlan) {
        self.plans.lock().unwrap().push_back(plan);
    }

    fn requests(&self) -> Vec<RequestRecord> {
        self.requests.lock().unwrap().clone()
    }

    async fn wait_for_requests(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.requests.lock().unwrap().len() >= expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("mock endpoint did not receive the expected request count");
    }

    async fn wait_for_no_active_requests(&self) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.request_finished.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.active_requests.load(Ordering::SeqCst) == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("the server-side request remained active after client cancellation");
    }

    async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.unwrap();
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn successful_head() -> ResponsePlan {
    ResponsePlan::new(StatusCode::OK)
        .header("content-length", "4")
        .header("etag", "\"etag-one\"")
        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
        .header("x-goog-generation", "1")
}

fn successful_put() -> ResponsePlan {
    ResponsePlan::new(StatusCode::OK)
        .header("etag", "\"etag-put\"")
        .header("x-goog-generation", "2")
}

fn multipart_start() -> ResponsePlan {
    ResponsePlan::new(StatusCode::OK).body(
        "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>object</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>",
    )
}

fn multipart_complete() -> ResponsePlan {
    ResponsePlan::new(StatusCode::OK)
        .header("x-goog-generation", "3")
        .body(
            "<CompleteMultipartUploadResult><Location>ignored</Location><Bucket>bucket</Bucket><Key>object</Key><ETag>\"etag-complete\"</ETag></CompleteMultipartUploadResult>",
        )
}

fn retry_args() -> [&'static str; 10] {
    [
        "--storage-max-retries",
        "2",
        "--storage-retry-timeout",
        "2s",
        "--storage-initial-backoff",
        "1ms",
        "--storage-max-backoff",
        "1ms",
        "--storage-backoff-base",
        "2",
    ]
}

#[cfg(feature = "gcs")]
fn gcs_session(server: &MockServer, request_timeout: &str) -> StorageSession {
    let registry = StorageRegistry::builder()
        .register(silk_chiffon_storage::gcs::backend().unwrap())
        .build()
        .unwrap();
    let mut arguments = vec![
        "gcs-http-test",
        "--gcs-endpoint",
        &server.endpoint,
        "--gcs-anonymous",
        "--gcs-request-timeout",
        request_timeout,
    ];
    arguments.extend(retry_args());
    let command = registry.augment_args(Command::new("gcs-http-test"));
    let matches = command.try_get_matches_from(arguments).unwrap();
    registry.create_session(&matches).unwrap()
}

#[cfg(feature = "s3")]
fn s3_session(server: &MockServer, request_timeout: &str) -> StorageSession {
    let registry = StorageRegistry::builder()
        .register(silk_chiffon_storage::s3::backend().unwrap())
        .build()
        .unwrap();
    let mut arguments = vec![
        "s3-http-test",
        "--s3-endpoint",
        &server.endpoint,
        "--s3-region",
        "test-region-1",
        "--s3-addressing-style",
        "path",
        "--s3-anonymous",
        "--s3-request-timeout",
        request_timeout,
    ];
    arguments.extend(retry_args());
    let command = registry.augment_args(Command::new("s3-http-test"));
    let matches = command.try_get_matches_from(arguments).unwrap();
    registry.create_session(&matches).unwrap()
}

async fn assert_read_retry(storage: StorageSession, url: &str, server: &MockServer) {
    server.push(ResponsePlan::new(StatusCode::SERVICE_UNAVAILABLE));
    server.push(successful_head());
    let input = storage
        .lookup_input(&LocationInput::parse(url).unwrap())
        .await
        .unwrap();
    assert_eq!(input.metadata().size, 4);
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.method == Method::HEAD)
    );
    assert_anonymous_bucket_object_requests(&requests, "/bucket/read");
}

async fn assert_put_retry(
    storage: StorageSession,
    url: &str,
    server: &MockServer,
    allow_empty_bearer: bool,
) {
    server.push(ResponsePlan::new(StatusCode::SERVICE_UNAVAILABLE));
    server.push(successful_put());
    let handle = storage
        .input_handle(&LocationInput::parse(url).unwrap())
        .unwrap();
    handle
        .object_store()
        .put(handle.object_path(), Bytes::from_static(b"data").into())
        .await
        .unwrap();
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.method == Method::PUT));
    assert_bucket_object_requests(&requests, "/bucket/put", allow_empty_bearer);
}

async fn assert_retry_exhaustion(storage: StorageSession, url: &str, server: &MockServer) {
    for _ in 0..3 {
        server.push(ResponsePlan::new(StatusCode::SERVICE_UNAVAILABLE));
    }
    let error = storage
        .lookup_input(&LocationInput::parse(url).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("after 2 retries"));
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_anonymous_bucket_object_requests(&requests, "/bucket/read");
}

async fn assert_request_timeout(storage: StorageSession, url: &str, server: &MockServer) {
    for _ in 0..3 {
        server.push(ResponsePlan::new(StatusCode::OK).delay(Duration::from_millis(200)));
    }
    let started = Instant::now();
    let error = storage
        .lookup_input(&LocationInput::parse(url).unwrap())
        .await
        .unwrap_err();
    assert!(!error.to_string().is_empty());
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_anonymous_bucket_object_requests(&requests, "/bucket/read");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the per-request timeout was not applied: {error}"
    );
}

async fn assert_cancellation(storage: StorageSession, url: &str, server: &MockServer) {
    server.push(ResponsePlan::new(StatusCode::OK).delay(Duration::from_secs(30)));
    let input = LocationInput::parse(url).unwrap();
    let task = tokio::spawn(async move { storage.lookup_input(&input).await });
    server.wait_for_requests(1).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    server.wait_for_no_active_requests().await;
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_anonymous_bucket_object_requests(&requests, "/bucket/read");
}

async fn assert_multipart_part_retry_and_abort(
    storage: StorageSession,
    url: &str,
    server: &MockServer,
    allow_empty_bearer: bool,
) {
    let handle = storage
        .input_handle(&LocationInput::parse(url).unwrap())
        .unwrap();
    server.push(multipart_start());
    server.push(ResponsePlan::new(StatusCode::SERVICE_UNAVAILABLE));
    server.push(successful_put());
    server.push(multipart_complete());
    let mut upload = handle
        .object_store()
        .put_multipart(handle.object_path())
        .await
        .unwrap();
    upload
        .put_part(Bytes::from_static(b"part").into())
        .await
        .unwrap();
    upload.complete().await.unwrap();

    let requests = server.requests();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[1].method, Method::PUT);
    assert_eq!(requests[2].method, Method::PUT);
    assert_eq!(requests[3].method, Method::POST);
    assert!(requests[1].uri.contains("partNumber=1"));
    assert_bucket_object_requests(&requests, "/bucket/object", allow_empty_bearer);

    server.push(multipart_start());
    server.push(ResponsePlan::new(StatusCode::NO_CONTENT));
    let mut abandoned = handle
        .object_store()
        .put_multipart(handle.object_path())
        .await
        .unwrap();
    abandoned.abort().await.unwrap();
    let requests = server.requests();
    assert_eq!(requests.last().unwrap().method, Method::DELETE);
    assert!(requests.last().unwrap().uri.contains("uploadId=upload-1"));
    assert_bucket_object_requests(&requests, "/bucket/object", allow_empty_bearer);
}

fn assert_anonymous_bucket_object_requests(requests: &[RequestRecord], expected_path: &str) {
    assert_bucket_object_requests(requests, expected_path, false);
}

fn assert_bucket_object_requests(
    requests: &[RequestRecord],
    expected_path: &str,
    allow_empty_bearer: bool,
) {
    for request in requests {
        assert_eq!(
            request.uri.split('?').next().unwrap(),
            expected_path,
            "request used the wrong bucket/object path"
        );
        match request.headers.get(AUTHORIZATION) {
            None => {}
            Some(value) if allow_empty_bearer && value.as_bytes() == b"Bearer" => {}
            Some(value) => {
                panic!(
                    "anonymous request unexpectedly contained Authorization: {} {} {value:?}",
                    request.method, request.uri
                )
            }
        }
    }
}

#[cfg(feature = "gcs")]
#[tokio::test]
async fn gcs_retries_reads_puts_and_multipart_parts_offline() {
    let server = MockServer::new().await;
    let storage = gcs_session(&server, "2s");
    assert_read_retry(storage.clone(), "gs://bucket/read", &server).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    let storage = gcs_session(&server, "2s");
    // object_store 0.13.2 unconditionally adds an empty bearer header to GCS
    // mutation requests even when signature generation is disabled.
    assert_put_retry(storage.clone(), "gs://bucket/put", &server, true).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    let storage = gcs_session(&server, "2s");
    assert_multipart_part_retry_and_abort(storage, "gs://bucket/object", &server, true).await;
    server.shutdown().await;
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn s3_retries_reads_puts_and_multipart_parts_offline() {
    let server = MockServer::new().await;
    let storage = s3_session(&server, "2s");
    assert_read_retry(storage.clone(), "s3://bucket/read", &server).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    let storage = s3_session(&server, "2s");
    assert_put_retry(storage.clone(), "s3://bucket/put", &server, false).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    let storage = s3_session(&server, "2s");
    assert_multipart_part_retry_and_abort(storage, "s3://bucket/object", &server, false).await;
    server.shutdown().await;
}

#[cfg(feature = "gcs")]
#[tokio::test]
async fn gcs_reports_retry_exhaustion_timeout_and_supports_cancellation() {
    let server = MockServer::new().await;
    assert_retry_exhaustion(gcs_session(&server, "2s"), "gs://bucket/read", &server).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    assert_request_timeout(gcs_session(&server, "20ms"), "gs://bucket/read", &server).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    assert_cancellation(gcs_session(&server, "2s"), "gs://bucket/read", &server).await;
    server.shutdown().await;
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn s3_reports_retry_exhaustion_timeout_and_supports_cancellation() {
    let server = MockServer::new().await;
    assert_retry_exhaustion(s3_session(&server, "2s"), "s3://bucket/read", &server).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    assert_request_timeout(s3_session(&server, "20ms"), "s3://bucket/read", &server).await;
    server.shutdown().await;

    let server = MockServer::new().await;
    assert_cancellation(s3_session(&server, "2s"), "s3://bucket/read", &server).await;
    server.shutdown().await;
}
