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
    header::{ACCEPT_ENCODING, HeaderMap},
};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;
use util::retry_strategy::{Backoff, BackoffMethod, RetryBackoffBuilder};
pub(crate) const DEFAULT_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HTTP_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_HTTP_TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const DEFAULT_HTTP_POOL_MAX_IDLE_PER_HOST: usize = 16;
const DEFAULT_HTTP_RETRIES: usize = 3;
const MAX_HTTP_BACKOFF: Duration = Duration::from_secs(300);
pub const SUPPORTED_ACCEPT_ENCODINGS: &str = "zstd, br, gzip, deflate";

/// Groups the optional retry, concurrency, and observability knobs accepted by
/// the `send_request_with_retry*` family of helpers.
#[derive(Default)]
pub struct RetryConfig<'a> {
    pub concurrency_limit: Option<&'a Semaphore>,
    pub max_retries: Option<usize>,
    pub backoff_method: Option<BackoffMethod>,
    pub retry_counter: Option<&'a AtomicU64>,
    pub inflight_counter: Option<&'a AtomicU64>,
}

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
        &RetryConfig::default(),
    )
    .await
}

pub async fn send_request_with_retry_and_concurrency_limit<F>(
    service_name: &str,
    operation_name: &str,
    build_request: F,
    config: &RetryConfig<'_>,
) -> Result<Response, reqwest::Error>
where
    F: Fn() -> RequestBuilder,
{
    let max_retries = config.max_retries.unwrap_or(DEFAULT_HTTP_RETRIES);
    let transient_method = config.backoff_method.unwrap_or(BackoffMethod::Fibonacci);
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
        if let Some(c) = config.inflight_counter {
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    };
    let dec_inflight = || {
        if let Some(c) = config.inflight_counter {
            c.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    };

    loop {
        let permit =
            acquire_concurrency_permit(config.concurrency_limit, service_name, operation_name)
                .await;

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
                    if let Some(counter) = config.retry_counter {
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
                if let Some(counter) = config.retry_counter {
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
    configure_client_builder_with_timeouts(
        builder,
        DEFAULT_HTTP_CONNECT_TIMEOUT,
        DEFAULT_HTTP_REQUEST_TIMEOUT,
    )
}

/// Like [`configure_client_builder`] but lets the caller override the connect
/// and per-request timeouts. Pool and keepalive settings remain at their
/// defaults.
pub(crate) fn configure_client_builder_with_timeouts(
    builder: ClientBuilder,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> ClientBuilder {
    enable_supported_compression(builder)
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
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
    crate::rate_limit::retry_after_duration(headers, SystemTime::now())
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

/// Marker appended to a sanitized body when it was truncated. Defined here so
/// the content budget in [`sanitize_error_body`] can reserve room for it.
pub const TRUNCATED_BODY_MARKER: &str = "…<truncated>";

/// Stream chunks from an HTTP error response, stopping as soon as we have
/// enough bytes to fill `max_bytes` after sanitization. Prevents a
/// misbehaving or malicious endpoint from forcing us to buffer an unbounded
/// body just so we can surface its first few hundred bytes in an error
/// message. The returned string is already passed through
/// [`sanitize_error_body`] and is capped at `max_bytes` bytes *including*
/// the truncation marker.
pub async fn read_bounded_error_body(mut response: Response, max_bytes: usize) -> String {
    // Cap the raw read at a small multiple of the sanitized cap to allow for
    // UTF-8 completion and whitespace expansion, while still bounding memory.
    let read_cap_bytes = max_bytes.saturating_mul(2);
    let mut raw: Vec<u8> = Vec::new();
    while raw.len() < read_cap_bytes {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = read_cap_bytes - raw.len();
                if chunk.len() <= remaining {
                    raw.extend_from_slice(&chunk);
                } else {
                    raw.extend_from_slice(&chunk[..remaining]);
                    break;
                }
            }
            // End of body (Ok(None)) or a network error mid-stream (Err(_))
            // both stop the read with whatever we have so far — an error
            // diagnostic is best-effort by design.
            Ok(None) | Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&raw);
    sanitize_error_body(&text, max_bytes)
}

/// Trim/flatten an arbitrary error response body for safe inclusion in logs
/// and error messages. Guarantees the returned string is at most `max_bytes`
/// bytes *including* the [`TRUNCATED_BODY_MARKER`]; replaces whitespace with
/// spaces so the result stays single-line. When `max_bytes` is smaller than
/// the marker (a degenerate cap), we fill up to `max_bytes` bytes of
/// sanitized content with no marker, matching the documented upper bound.
#[must_use]
pub fn sanitize_error_body(body: &str, max_bytes: usize) -> String {
    // If the cap can accommodate the marker, reserve room for it so a truncated
    // result still fits inside `max_bytes`. If it can't, we skip the marker and
    // let the content itself fill `max_bytes` — otherwise small caps would yield
    // an empty string.
    let (content_budget, emit_marker) = if max_bytes >= TRUNCATED_BODY_MARKER.len() {
        (max_bytes - TRUNCATED_BODY_MARKER.len(), true)
    } else {
        (max_bytes, false)
    };
    let mut out = String::with_capacity(body.len().min(max_bytes));
    let mut truncated = false;
    for ch in body.chars() {
        // Replace any whitespace character (including newlines/tabs/CR) with a
        // regular space so the error string stays a single line in logs. Runs
        // of whitespace are preserved as runs of spaces rather than collapsed.
        let mapped = if ch.is_whitespace() { ' ' } else { ch };
        if out.len() + mapped.len_utf8() > content_budget {
            truncated = true;
            break;
        }
        out.push(mapped);
    }
    if truncated && emit_marker {
        out.push_str(TRUNCATED_BODY_MARKER);
    }
    debug_assert!(out.len() <= max_bytes);
    out
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
        let duration =
            crate::rate_limit::retry_after_duration_from_value("42", SystemTime::UNIX_EPOCH)
                .expect("seconds-based Retry-After should parse");

        assert_eq!(duration, Duration::from_secs(42));
    }

    #[test]
    fn test_retry_after_duration_from_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(50);
        let duration = crate::rate_limit::retry_after_duration_from_value(
            "Thu, 01 Jan 1970 00:01:40 GMT",
            now,
        )
        .expect("HTTP-date Retry-After should parse");

        assert_eq!(duration, Duration::from_secs(50));
    }

    #[test]
    fn test_retry_after_duration_from_millisecond_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after-ms",
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

    #[test]
    fn sanitize_error_body_replaces_whitespace_and_truncates() {
        const MAX: usize = 512;
        let out = sanitize_error_body("line1\nline2\tfield", MAX);
        assert_eq!(out, "line1 line2 field");

        let long = "a".repeat(MAX + 64);
        let out = sanitize_error_body(&long, MAX);
        assert!(out.ends_with(TRUNCATED_BODY_MARKER), "got: {out}");
        // The total returned string (content + truncation marker) must fit
        // inside the cap, not just the content portion.
        assert!(
            out.len() <= MAX,
            "sanitized body exceeded total cap: {} bytes",
            out.len(),
        );
    }

    #[test]
    fn sanitize_error_body_small_cap_fills_content_without_marker() {
        // When the cap is too small to hold the truncation marker, the
        // function must still return up to `max_bytes` bytes of sanitized
        // content — an empty string would violate the documented upper bound.
        let small_cap = TRUNCATED_BODY_MARKER.len() - 1;
        let long = "a".repeat(small_cap * 4);
        let out = sanitize_error_body(&long, small_cap);
        assert!(!out.is_empty(), "expected some content even at tiny caps");
        assert!(
            !out.contains(TRUNCATED_BODY_MARKER),
            "marker should not appear when the cap can't hold it: {out:?}"
        );
        assert!(
            out.len() <= small_cap,
            "sanitized body exceeded tiny cap: {} bytes (cap={small_cap})",
            out.len(),
        );

        // Exactly zero cap still returns an empty string.
        let out = sanitize_error_body("anything", 0);
        assert_eq!(out, "");
    }
}
