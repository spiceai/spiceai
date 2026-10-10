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

//! The `Server-Timing` field a `/v1/sql` caller reads its server time from.
//!
//! A buffered response carries it in the head. A streamed JSON response is not
//! complete when its head is sent, so it carries the field as a trailer, when
//! the request allows one. The exact wire bytes are what a driver reads, so the
//! streamed cases go over a raw connection rather than through a client that
//! would hide the trailer section.

use std::sync::Arc;
use std::time::Instant;

use app::AppBuilder;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use reqwest::Client;
use runtime::Runtime;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

use super::start_runtime_http_server;
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

async fn start() -> Result<(Arc<Runtime>, String), String> {
    register_test_connectors().await;
    configure_test_datafusion();

    let rt = Arc::new(
        Runtime::builder()
            .with_app(AppBuilder::new("server_timing_test").build())
            .build()
            .await,
    );
    Arc::clone(&rt).load_components().await;
    runtime_ready_check(&rt).await;

    let base_url = start_runtime_http_server(Arc::clone(&rt)).await?;
    Ok((rt, base_url))
}

/// The milliseconds in a `total;dur=<ms>` value, which must have exactly that
/// shape: one `total` metric, a duration with three decimals.
#[track_caller]
fn parse_total(value: &str) -> f64 {
    let duration = value
        .strip_prefix("total;dur=")
        .unwrap_or_else(|| panic!("expected `total;dur=<ms>`, got `{value}`"));
    let (_, decimals) = duration
        .split_once('.')
        .unwrap_or_else(|| panic!("the duration must carry decimals, got `{value}`"));
    assert_eq!(
        decimals.len(),
        3,
        "the duration must carry exactly three decimals, got `{value}`"
    );
    duration
        .parse::<f64>()
        .unwrap_or_else(|e| panic!("the duration must be a number, got `{value}`: {e}"))
}

/// Posts `sql` with `accept`, returning the status, the `Server-Timing` header
/// if any, the `Trailer` header if any, and how long the client waited.
async fn post(
    base_url: &str,
    sql: &str,
    accept: Option<&str>,
    content_type: Option<&str>,
) -> (reqwest::StatusCode, Option<String>, Option<String>, f64) {
    let mut request = Client::new()
        .post(format!("{base_url}/v1/sql"))
        .body(sql.to_string());
    if let Some(accept) = accept {
        request = request.header("Accept", accept);
    }
    if let Some(content_type) = content_type {
        request = request.header("Content-Type", content_type);
    }

    let started = Instant::now();
    let response = request
        .send()
        .await
        .expect("the request reaches the runtime");
    let status = response.status();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .map(|value| value.to_str().expect("an ASCII header").to_string())
    };
    let server_timing = header("server-timing");
    let trailer = header("trailer");
    response.bytes().await.expect("the body is read in full");
    let client_ms = started.elapsed().as_secs_f64() * 1000.0;

    (status, server_timing, trailer, client_ms)
}

/// A buffered `/v1/sql` request: label, SQL, `Accept`, `Content-Type`, and the
/// status it must answer with.
type BufferedCase = (
    &'static str,
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
    u16,
);

/// Every response whose body is complete before its head is sent carries the
/// server time in the head — each buffered format, and the errors, which are
/// the responses a caller most wants timed. The value is a server measurement,
/// so it can never exceed what the client itself waited.
#[tokio::test]
async fn a_buffered_sql_response_carries_its_server_time_in_the_head() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (rt, base_url) = start().await?;

            let cases: [BufferedCase; 5] = [
                ("csv", "SELECT 1 AS n", Some("text/csv"), None, 200),
                ("plain", "SELECT 1 AS n", Some("text/plain"), None, 200),
                (
                    "vnd json",
                    "SELECT 1 AS n",
                    Some("application/vnd.spiceai.sql.v1+json"),
                    None,
                    200,
                ),
                (
                    "query error",
                    "SELECT * FROM no_such_table",
                    Some("text/csv"),
                    None,
                    400,
                ),
                (
                    "rejected body",
                    "{not json",
                    None,
                    Some("application/json"),
                    400,
                ),
            ];

            for (case, sql, accept, content_type, expected_status) in cases {
                let (status, server_timing, trailer, client_ms) =
                    post(&base_url, sql, accept, content_type).await;
                assert_eq!(status.as_u16(), expected_status, "{case}: status");
                let server_timing = server_timing
                    .unwrap_or_else(|| panic!("{case}: the head must carry `Server-Timing`"));
                let server_ms = parse_total(&server_timing);
                assert!(
                    server_ms > 0.0 && server_ms <= client_ms,
                    "{case}: the server time {server_ms} ms must be positive and within the \
                     {client_ms} ms the client waited"
                );
                assert_eq!(
                    trailer, None,
                    "{case}: a buffered response announces no trailer"
                );
            }

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// The streamed JSON body has not finished when its head is sent, so the head
/// must not carry a number that would read as the total. Without `TE: trailers`
/// an HTTP/1.1 response cannot carry a trailer either, so it announces none.
#[tokio::test]
async fn a_streamed_sql_response_does_not_put_a_partial_time_in_the_head() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (rt, base_url) = start().await?;

            let (status, server_timing, trailer, _) =
                post(&base_url, "SELECT 1 AS n", None, None).await;
            assert!(status.is_success(), "SELECT 1 should succeed, got {status}");
            assert_eq!(
                server_timing, None,
                "the streamed head must not carry a total"
            );
            assert_eq!(
                trailer, None,
                "a request without `TE: trailers` must not be promised a trailer"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// The raw HTTP/1.1 response to `request`, read until the server closes.
async fn raw_http1(base_url: &str, request: &str) -> String {
    let address = base_url
        .strip_prefix("http://")
        .expect("the test server is plain HTTP");
    let mut stream = TcpStream::connect(address)
        .await
        .expect("the test server accepts a connection");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("the request is written");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("the response is read until the server closes");
    String::from_utf8(response).expect("the response is UTF-8")
}

/// Splits a raw chunked HTTP/1.1 response into its head, its decoded body and
/// its trailer section.
fn split_chunked(response: &str) -> (String, String, String) {
    let (head, mut rest) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("no end of head in `{response}`"));
    let mut body = String::new();
    loop {
        let (size, after) = rest
            .split_once("\r\n")
            .unwrap_or_else(|| panic!("no chunk size line in `{rest}`"));
        let size = usize::from_str_radix(size.trim(), 16)
            .unwrap_or_else(|e| panic!("not a chunk size: `{size}`: {e}"));
        if size == 0 {
            let trailers = after
                .strip_suffix("\r\n")
                .unwrap_or_else(|| panic!("the message must end with an empty line: `{after}`"));
            return (head.to_lowercase(), body, trailers.to_lowercase());
        }
        body.push_str(&after[..size]);
        rest = after[size..]
            .strip_prefix("\r\n")
            .unwrap_or_else(|| panic!("a chunk must end with CRLF: `{after}`"));
    }
}

fn header_line<'a>(section: &'a str, name: &str) -> Option<&'a str> {
    section
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name}: ")))
}

/// With `TE: trailers` the streamed body ends with the total as a trailer the
/// head declared, and its rows are unchanged by it.
#[tokio::test]
async fn a_streamed_sql_response_sends_its_server_time_as_a_trailer() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (rt, base_url) = start().await?;
            let sql = "SELECT 1 AS n";
            let host = base_url.trim_start_matches("http://");

            let started = Instant::now();
            let response = raw_http1(
                &base_url,
                &format!(
                    "POST /v1/sql HTTP/1.1\r\nHost: {host}\r\nContent-Type: text/plain\r\nTE: trailers\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{sql}",
                    sql.len()
                ),
            )
            .await;
            let client_ms = started.elapsed().as_secs_f64() * 1000.0;

            let (head, body, trailers) = split_chunked(&response);
            assert!(head.starts_with("http/1.1 200"), "status line: {head}");
            assert_eq!(header_line(&head, "server-timing"), None, "no total in the head");
            assert_eq!(
                header_line(&head, "trailer"),
                Some("server-timing"),
                "the head declares the trailer"
            );
            assert_eq!(body, r#"[{"n":1}]"#, "the rows are unchanged");
            let server_ms = parse_total(
                header_line(&trailers, "server-timing")
                    .unwrap_or_else(|| panic!("no `server-timing` trailer in `{trailers}`")),
            );
            assert!(
                server_ms > 0.0 && server_ms <= client_ms,
                "the server time {server_ms} ms must be positive and within the {client_ms} ms \
                 the client waited"
            );

            // The same request without `TE: trailers` ends with an empty trailer
            // section rather than an announced field that never arrives.
            let response = raw_http1(
                &base_url,
                &format!(
                    "POST /v1/sql HTTP/1.1\r\nHost: {host}\r\nContent-Type: text/plain\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{sql}",
                    sql.len()
                ),
            )
            .await;
            let (head, body, trailers) = split_chunked(&response);
            assert_eq!(header_line(&head, "trailer"), None, "nothing declared");
            assert_eq!(body, r#"[{"n":1}]"#, "the rows are unchanged");
            assert_eq!(trailers, "", "no trailer without `TE: trailers`");

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// HTTP/2 always permits trailers, so a streamed body sends its total as one
/// without the client asking.
#[tokio::test]
async fn an_http2_streamed_sql_response_sends_its_server_time_as_a_trailer() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (rt, base_url) = start().await?;
            let address = base_url.trim_start_matches("http://").to_string();

            let stream = TcpStream::connect(&address)
                .await
                .map_err(|e| format!("connect: {e}"))?;
            let (mut sender, connection) =
                hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                    .await
                    .map_err(|e| format!("HTTP/2 handshake: {e}"))?;
            tokio::spawn(connection);

            let request = http::Request::post(format!("http://{address}/v1/sql"))
                .header("content-type", "text/plain")
                .body(Full::new(Bytes::from_static(b"SELECT 1 AS n")))
                .map_err(|e| format!("request: {e}"))?;
            let started = Instant::now();
            let response = sender
                .send_request(request)
                .await
                .map_err(|e| format!("send: {e}"))?;
            assert_eq!(response.status(), http::StatusCode::OK);
            assert!(
                response.headers().get("server-timing").is_none(),
                "no total in the head"
            );
            let collected = response
                .into_body()
                .collect()
                .await
                .map_err(|e| format!("body: {e}"))?;
            let client_ms = started.elapsed().as_secs_f64() * 1000.0;
            let trailers = collected
                .trailers()
                .cloned()
                .ok_or("an HTTP/2 streamed response must end with trailers")?;
            assert_eq!(
                collected.to_bytes(),
                Bytes::from_static(br#"[{"n":1}]"#),
                "the rows are unchanged"
            );
            let server_ms = parse_total(
                trailers
                    .get("server-timing")
                    .ok_or("no `server-timing` trailer")?
                    .to_str()
                    .map_err(|e| format!("trailer value: {e}"))?,
            );
            assert!(
                server_ms > 0.0 && server_ms <= client_ms,
                "the server time {server_ms} ms must be positive and within the {client_ms} ms \
                 the client waited"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}
