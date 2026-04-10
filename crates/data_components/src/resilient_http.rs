/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use reqwest::{
    ClientBuilder, RequestBuilder, Response, StatusCode,
    header::{ACCEPT_ENCODING, HeaderMap, RETRY_AFTER},
};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;
use util::retry_strategy::{Backoff, BackoffMethod, RetryBackoffBuilder};


const DEFAULT_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HTTP_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_HTTP_TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const DEFAULT_HTTP_POOL_MAX_IDLE_PER_HOST: usize = 16;
const DEFAULT_HTTP_RETRIES: usize = 10;
const MAX_HTTP_BACKOFF: Duration = Duration::from_secs(300);
const RETRY_AFTER_MS_HEADER: &str = "retry-after-ms";
const X_RETRY_AFTER_MS_HEADER: &str = "x-retry-after-ms";
pub const SUPPORTED_ACCEPT_ENCODINGS: &str = "zstd, br, gzip, deflate";

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum RetryReason {
    RateLimit,
    ServerError,
    Network,
}

pub async fn send_request_with_retry<F>(
    service_name: &str,
    operation_name: &str,
    build_request: F,
) -> Result<Response, reqwest::Error>
where
    F: Fn() -> RequestBuilder,
{
    send_request_with_retry_and_concurrency_limit(
        service_name,
        operation_name,
        build_request,
        None,
        None,
        None,
    )
    .await
}

pub async fn send_request_with_retry_and_concurrency_limit<F>(
    service_name: &str,
    operation_name: &str,
    build_request: F,
    concurrency_limit: Option<&Semaphore>,
    max_retries: Option<usize>,
    backoff_method: Option<BackoffMethod>,
) -> Result<Response, reqwest::Error>
where
    F: Fn() -> RequestBuilder,
{
    send_request_with_retry_and_concurrency_limit_with_counter(
        service_name,
        operation_name,
        build_request,
        concurrency_limit,
        max_retries,
        backoff_method,
        None,
    )
    .await
}

pub async fn send_request_with_retry_and_concurrency_limit_with_counter<F>(
    service_name: &str,
    operation_name: &str,
    build_request: F,
    concurrency_limit: Option<&Semaphore>,
    max_retries: Option<usize>,
    backoff_method: Option<BackoffMethod>,
    retry_counter: Option<&AtomicU64>,
) -> Result<Response, reqwest::Error>
where
    F: Fn() -> RequestBuilder,
{
    send_request_with_retry_concurrency_and_inflight::<F>(
        service_name,
        operation_name,
        build_request,
        concurrency_limit,
        max_retries,
        backoff_method,
        retry_counter,
        None,
    )
    .await
}

/// Like [`send_request_with_retry_and_concurrency_limit_with_counter`] but
/// also accepts an optional `inflight_counter` that is incremented only while
/// a concurrency permit is held (i.e. an HTTP request is actually in-flight).
pub async fn send_request_with_retry_concurrency_and_inflight<F>(
    service_name: &str,
    operation_name: &str,
    build_request: F,
    concurrency_limit: Option<&Semaphore>,
    max_retries: Option<usize>,
    backoff_method: Option<BackoffMethod>,
    retry_counter: Option<&AtomicU64>,
    inflight_counter: Option<&AtomicU64>,
) -> Result<Response, reqwest::Error>
where
    F: Fn() -> RequestBuilder,
{
    let max_retries = max_retries.unwrap_or(DEFAULT_HTTP_RETRIES);
    let transient_method = backoff_method.unwrap_or(BackoffMethod::Fibonacci);
    let mut transient_backoff = RetryBackoffBuilder::new()
        .method(transient_method)
        .max_duration(Some(MAX_HTTP_BACKOFF))
        .build();
    let mut rate_limit_backoff = RetryBackoffBuilder::new()
        .method(BackoffMethod::Exponential)
        .base_interval(Duration::from_secs(1))
        .max_duration(Some(MAX_HTTP_BACKOFF))
        .build();

    let mut retries = 0usize;

    let inc_inflight = || {
        if let Some(c) = inflight_counter {
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    };
    let dec_inflight = || {
        if let Some(c) = inflight_counter {
            c.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    };

    loop {
        let permit =
            acquire_concurrency_permit(concurrency_limit, service_name, operation_name).await;
        inc_inflight();
        match add_supported_accept_encoding_header(build_request())
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                if let Some(reason) = retry_reason_from_status(status) {
                    if retries >= max_retries {
                        tracing::warn!(
                            service = service_name,
                            operation = operation_name,
                            status = %status,
                            retries,
                            max_retries,
                            "HTTP retries exhausted after retryable response"
                        );
                        dec_inflight();
                        return Ok(response);
                    }

                    retries += 1;
                    if let Some(counter) = retry_counter {
                        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    let retry_after = retry_after_duration(response.headers());
                    let backoff =
                        next_backoff(reason, &mut transient_backoff, &mut rate_limit_backoff);
                    let delay = bounded_retry_delay(retry_after, backoff, MAX_HTTP_BACKOFF);

                    tracing::warn!(
                        service = service_name,
                        operation = operation_name,
                        attempt = retries,
                        max_retries,
                        status = %status,
                        retry_after = ?retry_after,
                        delay = ?delay,
                        "Retrying HTTP request after retryable response"
                    );

                    drain_response_body(response, service_name, operation_name, retries, status)
                        .await;
                    dec_inflight();
                    drop(permit);
                    tokio::time::sleep(delay).await;
                    continue;
                }

                dec_inflight();
                return Ok(response);
            }
            Err(error) => {
                let Some(reason) = retry_reason_from_error(&error) else {
                    dec_inflight();
                    return Err(error);
                };

                if retries >= max_retries {
                    tracing::warn!(
                        service = service_name,
                        operation = operation_name,
                        retries,
                        max_retries,
                        error = %error,
                        "HTTP retries exhausted after transient request failure"
                    );
                    dec_inflight();
                    return Err(error);
                }

                retries += 1;
                if let Some(counter) = retry_counter {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let delay = next_backoff(reason, &mut transient_backoff, &mut rate_limit_backoff);

                tracing::warn!(
                    service = service_name,
                    operation = operation_name,
                    attempt = retries,
                    max_retries,
                    delay = ?delay,
                    error = %error,
                    "Retrying HTTP request after transient request failure"
                );

                dec_inflight();
                drop(permit);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

pub fn enable_supported_compression(builder: ClientBuilder) -> ClientBuilder {
    builder.gzip(true).brotli(true).zstd(true).deflate(true)
}

pub fn configure_client_builder(builder: ClientBuilder) -> ClientBuilder {
    enable_supported_compression(builder)
        .connect_timeout(DEFAULT_HTTP_CONNECT_TIMEOUT)
        .timeout(DEFAULT_HTTP_REQUEST_TIMEOUT)
        .pool_idle_timeout(DEFAULT_HTTP_POOL_IDLE_TIMEOUT)
        .pool_max_idle_per_host(DEFAULT_HTTP_POOL_MAX_IDLE_PER_HOST)
        .tcp_keepalive(DEFAULT_HTTP_TCP_KEEPALIVE)
}

async fn acquire_concurrency_permit<'a>(
    semaphore: Option<&'a Semaphore>,
    service_name: &str,
    operation_name: &str,
) -> Option<tokio::sync::SemaphorePermit<'a>> {
    let sem = semaphore?;
    if let Ok(permit) = sem.acquire().await {
        Some(permit)
    } else {
        tracing::warn!(
            service = service_name,
            operation = operation_name,
            "Request concurrency limiter closed; proceeding without limit"
        );
        None
    }
}

async fn drain_response_body(
    mut response: Response,
    service_name: &str,
    operation_name: &str,
    attempt: usize,
    status: StatusCode,
) {
    loop {
        match response.chunk().await {
            Ok(Some(_)) => {}
            Ok(None) => return,
            Err(error) => {
                tracing::debug!(
                    service = service_name,
                    operation = operation_name,
                    attempt,
                    status = %status,
                    error = %error,
                    "Failed to drain retry response body before retry"
                );
                return;
            }
        }
    }
}

fn add_supported_accept_encoding_header(request: RequestBuilder) -> RequestBuilder {
    request.header(ACCEPT_ENCODING, SUPPORTED_ACCEPT_ENCODINGS)
}

fn next_backoff(
    reason: RetryReason,
    transient_backoff: &mut impl Backoff,
    rate_limit_backoff: &mut impl Backoff,
) -> Duration {
    match reason {
        RetryReason::RateLimit => rate_limit_backoff
            .next_backoff()
            .unwrap_or(MAX_HTTP_BACKOFF),
        RetryReason::ServerError | RetryReason::Network => {
            transient_backoff.next_backoff().unwrap_or(MAX_HTTP_BACKOFF)
        }
    }
}

fn retry_reason_from_error(error: &reqwest::Error) -> Option<RetryReason> {
    if error.is_connect() || error.is_timeout() {
        return Some(RetryReason::Network);
    }

    error.status().and_then(retry_reason_from_status)
}

fn retry_reason_from_status(status: StatusCode) -> Option<RetryReason> {
    match status.as_u16() {
        408 | 429 => Some(RetryReason::RateLimit),
        500..=599 => Some(RetryReason::ServerError),
        _ => None,
    }
}

fn retry_after_duration(headers: &HeaderMap) -> Option<Duration> {
    retry_after_millis_duration(headers).or_else(|| {
        headers
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| retry_after_duration_from_value(value, SystemTime::now()))
    })
}

fn bounded_retry_delay(
    retry_after: Option<Duration>,
    backoff: Duration,
    max_delay: Duration,
) -> Duration {
    retry_after
        .map_or(backoff, |retry_after| retry_after.max(backoff))
        .min(max_delay)
}

fn retry_after_millis_duration(headers: &HeaderMap) -> Option<Duration> {
    [RETRY_AFTER_MS_HEADER, X_RETRY_AFTER_MS_HEADER]
        .into_iter()
        .find_map(|header_name| {
            headers
                .get(header_name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(Duration::from_millis)
        })
}

fn retry_after_duration_from_value(value: &str, now: SystemTime) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    trimmed
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(trimmed)
                .ok()
                .map(|retry_after| retry_after.duration_since(now).unwrap_or(Duration::ZERO))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use brotli::CompressorReader;
    use flate2::{
        Compression,
        write::{GzEncoder, ZlibEncoder},
    };
    use reqwest::Client;
    use serde_json::json;
    use std::{collections::VecDeque, io::Read, io::Write, sync::Arc};
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct MockHttpResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl MockHttpResponse {
        fn json(status_line: &'static str, body: &serde_json::Value) -> Self {
            Self {
                status_line,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::to_vec(&body).expect("mock JSON should serialize"),
            }
        }
    }

    async fn start_mock_server(
        responses: Vec<MockHttpResponse>,
    ) -> (
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let address = listener
            .local_addr()
            .expect("should have a listener address");
        let requests = Arc::new(AtomicUsize::new(0));
        let queued_responses = Arc::new(Mutex::new(VecDeque::from(responses)));
        let captured_requests = Arc::new(Mutex::new(Vec::new()));

        let requests_for_server = Arc::clone(&requests);
        let captured_requests_for_server = Arc::clone(&captured_requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };

                let requests = Arc::clone(&requests_for_server);
                let queued_responses = Arc::clone(&queued_responses);
                let captured_requests = Arc::clone(&captured_requests_for_server);
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;

                    let captured_request = read_http_request(&mut stream).await;
                    requests.fetch_add(1, Ordering::SeqCst);
                    captured_requests
                        .lock()
                        .await
                        .push(String::from_utf8_lossy(&captured_request).into_owned());

                    let response = queued_responses
                        .lock()
                        .await
                        .pop_front()
                        .unwrap_or_else(|| MockHttpResponse::json("200 OK", &json!({"ok": true})));

                    let mut http_response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\n",
                        response.status_line,
                        response.body.len()
                    );
                    for (header_name, header_value) in response.headers {
                        let _ = std::fmt::Write::write_fmt(
                            &mut http_response,
                            format_args!("{header_name}: {header_value}\r\n"),
                        );
                    }
                    http_response.push_str("\r\n");
                    let _ = stream.write_all(http_response.as_bytes()).await;
                    let _ = stream.write_all(&response.body).await;
                });
            }
        });

        (format!("http://{address}"), requests, captured_requests)
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;

        let mut captured_request = Vec::with_capacity(4096);
        let mut buf = [0u8; 1024];
        let mut expected_total_len = None;

        loop {
            let bytes_read = match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(bytes_read) => bytes_read,
            };

            captured_request.extend_from_slice(&buf[..bytes_read]);

            if expected_total_len.is_none() {
                expected_total_len = expected_http_request_len(&captured_request);
            }

            if let Some(expected_total_len) = expected_total_len
                && captured_request.len() >= expected_total_len
            {
                break;
            }
        }

        captured_request
    }

    fn expected_http_request_len(request: &[u8]) -> Option<usize> {
        let headers_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)?;

        let content_length = String::from_utf8_lossy(&request[..headers_end])
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.trim().eq_ignore_ascii_case("Content-Length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);

        Some(headers_end.saturating_add(content_length))
    }

    fn encode_body(encoding: &str, body: &[u8]) -> Vec<u8> {
        match encoding {
            "gzip" => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder
                    .write_all(body)
                    .expect("gzip encoder should accept the response body");
                encoder.finish().expect("gzip encoder should finish")
            }
            "deflate" => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder
                    .write_all(body)
                    .expect("deflate encoder should accept the response body");
                encoder.finish().expect("deflate encoder should finish")
            }
            "br" => {
                let mut compressed = Vec::new();
                let mut reader = CompressorReader::new(body, 4096, 5, 22);
                reader
                    .read_to_end(&mut compressed)
                    .expect("brotli encoder should finish");
                compressed
            }
            "zstd" => zstd::stream::encode_all(body, 0).expect("zstd encoder should finish"),
            other => panic!("unsupported encoding in test: {other}"),
        }
    }

    #[test]
    fn test_retry_after_duration_from_seconds() {
        let duration = retry_after_duration_from_value("42", SystemTime::UNIX_EPOCH)
            .expect("seconds-based Retry-After should parse");

        assert_eq!(duration, Duration::from_secs(42));
    }

    #[test]
    fn test_retry_after_duration_from_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(50);
        let duration = retry_after_duration_from_value("Thu, 01 Jan 1970 00:01:40 GMT", now)
            .expect("HTTP-date Retry-After should parse");

        assert_eq!(duration, Duration::from_secs(50));
    }

    #[test]
    fn test_retry_after_duration_from_millisecond_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            RETRY_AFTER_MS_HEADER,
            reqwest::header::HeaderValue::from_static("1250"),
        );

        assert_eq!(
            retry_after_duration(&headers),
            Some(Duration::from_millis(1250))
        );
    }

    #[test]
    fn test_bounded_retry_delay_clamps_large_retry_after() {
        let delay = bounded_retry_delay(
            Some(Duration::from_secs(3600)),
            Duration::from_secs(5),
            MAX_HTTP_BACKOFF,
        );

        assert_eq!(delay, MAX_HTTP_BACKOFF);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_send_request_with_retry_retries_rate_limited_response() {
        let (base_url, requests, _) = start_mock_server(vec![
            MockHttpResponse {
                status_line: "429 Too Many Requests",
                headers: vec![("Retry-After".to_string(), "0".to_string())],
                body: serde_json::to_vec(&json!({"error_code": "RATE_LIMITED"}))
                    .expect("rate-limit JSON should serialize"),
            },
            MockHttpResponse::json("200 OK", &json!({"ok": true})),
        ])
        .await;

        let client = enable_supported_compression(Client::builder())
            .build()
            .expect("compression-enabled client should build");
        let response =
            send_request_with_retry("Databricks SQL Warehouse", "test rate limit", || {
                client.get(format!("{base_url}/api/2.0/sql/statements/"))
            })
            .await
            .expect("request should eventually succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "expected a retry after the initial rate-limited response"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_send_request_with_retry_retries_server_error() {
        let (base_url, requests, _) = start_mock_server(vec![
            MockHttpResponse::json("503 Service Unavailable", &json!({"error": "busy"})),
            MockHttpResponse::json("200 OK", &json!({"ok": true})),
        ])
        .await;

        let client = enable_supported_compression(Client::builder())
            .build()
            .expect("compression-enabled client should build");
        let response =
            send_request_with_retry("Databricks SQL Warehouse", "test server error", || {
                client.get(format!("{base_url}/api/2.0/sql/statements/"))
            })
            .await
            .expect("request should eventually succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "expected a retry after the initial 503 response"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_send_request_with_retry_requests_supported_encodings_and_decodes_responses() {
        for encoding in ["zstd", "br", "gzip", "deflate"] {
            let plain_body = json!({"encoding": encoding}).to_string();
            let compressed_body = encode_body(encoding, plain_body.as_bytes());
            let (base_url, requests, captured_requests) =
                start_mock_server(vec![MockHttpResponse {
                    status_line: "200 OK",
                    headers: vec![
                        ("Content-Type".to_string(), "application/json".to_string()),
                        ("Content-Encoding".to_string(), encoding.to_string()),
                    ],
                    body: compressed_body,
                }])
                .await;

            let client = enable_supported_compression(Client::builder())
                .build()
                .expect("compression-enabled client should build");
            let response = send_request_with_retry(
                "Databricks SQL Warehouse",
                "decode compressed response",
                || client.get(format!("{base_url}/api/2.0/sql/statements/")),
            )
            .await
            .expect("request should succeed");
            let body = response.text().await.expect("response body should decode");
            let request = captured_requests.lock().await.remove(0);

            assert_eq!(
                body, plain_body,
                "{encoding} response should be decompressed"
            );
            assert_eq!(
                requests.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "{encoding} response should not need a retry"
            );
            let request_lower = request.to_ascii_lowercase();
            assert!(
                request_lower.contains("accept-encoding: zstd, br, gzip, deflate"),
                "request should advertise all supported encodings: {request}"
            );
        }
    }
}
