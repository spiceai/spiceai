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

//! Requests through [`SpawnedReqwestConnector`] to a local HTTP server that stalls a response
//! body past the client timeout.

#![expect(clippy::expect_used, reason = "integration-test helpers")]

use std::{
    future::poll_fn,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use http_body::Body;
use object_store::{
    BackoffConfig, ClientOptions, ObjectStoreExt, RetryConfig,
    aws::AmazonS3Builder,
    client::{HttpConnector, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponseBody},
    path::Path,
};
use object_store_spawn::SpawnedReqwestConnector;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Handle,
};

/// How long a stalled response keeps its connection open; far longer than any client timeout here.
const STALL: Duration = Duration::from_mins(1);

/// Reads one request head and returns its `Range` header as an exclusive byte range.
async fn read_request(socket: &mut TcpStream) -> Option<Range<usize>> {
    let mut head = Vec::new();
    let mut buf = [0u8; 4096];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = socket.read(&mut buf).await.expect("read request head");
        assert!(n > 0, "connection closed before the request head ended");
        head.extend_from_slice(&buf[..n]);
    }
    let head = String::from_utf8(head).expect("request head is UTF-8");
    head.lines().find_map(|line| {
        let range = line
            .to_ascii_lowercase()
            .strip_prefix("range: bytes=")?
            .to_string();
        let (start, end) = range.split_once('-').expect("bounded range");
        let start: usize = start.parse().expect("range start");
        let end: usize = end.parse().expect("range end");
        Some(start..end + 1)
    })
}

async fn next_frame(
    body: &mut HttpResponseBody,
) -> Option<Result<http_body::Frame<bytes::Bytes>, object_store::client::HttpError>> {
    poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

/// After the request timeout, the body yields one timeout error and then ends.
#[tokio::test]
async fn timed_out_body_ends_after_one_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        read_request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n\r\n0123456789abcdef")
            .await
            .expect("write response");
        tokio::time::sleep(STALL).await;
    });

    let options = ClientOptions::new()
        .with_allow_http(true)
        .with_timeout(Duration::from_millis(300));
    let client = SpawnedReqwestConnector::new(Handle::current())
        .connect(&options)
        .expect("connect");
    let mut request = HttpRequest::new(HttpRequestBody::empty());
    *request.uri_mut() = format!("http://{addr}/object").parse().expect("URI");
    let mut body = client
        .execute(request)
        .await
        .expect("response head")
        .into_body();

    let mut data = Vec::new();
    let error = loop {
        match next_frame(&mut body)
            .await
            .expect("body ended before the timeout")
        {
            Ok(frame) => data.extend_from_slice(&frame.into_data().expect("data frame")),
            Err(e) => break e,
        }
    };
    assert_eq!(data, b"0123456789abcdef");
    assert_eq!(error.kind(), HttpErrorKind::Timeout, "{error}");

    let after_error = tokio::time::timeout(Duration::from_secs(5), next_frame(&mut body))
        .await
        .expect("body neither ended nor yielded a frame within 5s");
    assert!(
        after_error.is_none(),
        "body yielded another frame after its timeout error: {after_error:?}"
    );

    server.abort();
}

const OBJECT_LEN: usize = 4 * 1024 * 1024;
/// Body bytes the first response sends before it stalls.
const STALL_AFTER: usize = 1024 * 1024;

fn object_bytes() -> Vec<u8> {
    (0..OBJECT_LEN)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13).to_le_bytes()[0])
        .collect()
}

/// Serves ranged GETs of `object`. The first response stalls after `STALL_AFTER` body bytes.
async fn serve_object(
    listener: TcpListener,
    object: Arc<Vec<u8>>,
    requested: Arc<Mutex<Vec<Range<usize>>>>,
) {
    loop {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let object = Arc::clone(&object);
        let requested = Arc::clone(&requested);
        tokio::spawn(async move {
            let range = read_request(&mut socket).await.expect("ranged GET");
            let is_first = {
                let mut requested = requested.lock().expect("requested ranges lock");
                requested.push(range.clone());
                requested.len() == 1
            };
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nETag: \"v1\"\r\nLast-Modified: Wed, 07 Oct 2026 00:00:00 GMT\r\n\r\n",
                range.len(),
                range.start,
                range.end - 1,
                object.len()
            );
            socket.write_all(head.as_bytes()).await.expect("write head");
            if is_first {
                socket
                    .write_all(&object[range.start..range.start + STALL_AFTER])
                    .await
                    .expect("write partial body");
                tokio::time::sleep(STALL).await;
            } else {
                socket.write_all(&object[range]).await.expect("write body");
            }
        });
    }
}

/// An S3 range read whose response stalls past the client timeout resumes where the stalled
/// body stopped and returns the exact object bytes.
#[tokio::test]
async fn s3_range_read_resumes_after_body_timeout() {
    let object = Arc::new(object_bytes());
    let requested = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local address");
    let server = tokio::spawn(serve_object(
        listener,
        Arc::clone(&object),
        Arc::clone(&requested),
    ));

    let store = AmazonS3Builder::new()
        .with_bucket_name("bucket")
        .with_region("us-east-1")
        .with_endpoint(format!("http://{addr}"))
        .with_skip_signature(true)
        .with_virtual_hosted_style_request(false)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_timeout(Duration::from_secs(1)),
        )
        .with_retry(RetryConfig {
            backoff: BackoffConfig {
                init_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                base: 2.0,
            },
            max_retries: 3,
            retry_timeout: Duration::from_secs(30),
        })
        .with_http_connector(SpawnedReqwestConnector::new(Handle::current()))
        .build()
        .expect("S3 store");

    let range = 1000..3_000_000;
    let bytes = store
        .get_range(
            &Path::from("lineitem.parquet"),
            u64::try_from(range.start).expect("u64")..u64::try_from(range.end).expect("u64"),
        )
        .await
        .expect("range read");

    assert_eq!(bytes.len(), range.len());
    assert!(
        bytes.as_ref() == &object[range.clone()],
        "range read returned different bytes than the object holds"
    );
    let requested = requested.lock().expect("requested ranges lock").clone();
    assert_eq!(
        requested,
        vec![range.clone(), range.start + STALL_AFTER..range.end],
        "expected one stalled request and one retry that resumes after the bytes already read"
    );

    server.abort();
}
