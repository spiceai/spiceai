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

//! A response body read through [`SpawnedReqwestConnector`] is read no further ahead of its
//! reader than the read-ahead budget, however fast the server sends it, and a reader that
//! pauses past the client timeout still reads the whole object.

#![expect(clippy::expect_used, reason = "integration-test helpers")]

use std::{
    future::poll_fn,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::StreamExt;
use http_body::Body;
use object_store::{
    BackoffConfig, ClientOptions, GetResultPayload, ObjectStoreExt, RetryConfig,
    aws::AmazonS3Builder,
    client::{HttpConnector, HttpRequest, HttpRequestBody, HttpResponseBody},
    path::Path,
};
use object_store_spawn::{RESPONSE_READ_AHEAD_BYTES, SpawnedReqwestConnector};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    runtime::Handle,
};

const MIB: usize = 1024 * 1024;

/// Far more than the read-ahead budget and the socket buffers on both ends together, so a
/// connector that drains the body regardless of its reader takes all of it.
const BODY_LEN: usize = 256 * MIB;

/// The most the server may get onto the wire while the reader is paused: the read-ahead
/// budget, the client's receive and the server's send socket buffers (a few MiB each when
/// autotuned on loopback), and the frame in flight. A quarter of the body.
const STALLED_BOUND: usize = 64 * MIB;

const CHUNK: usize = 64 * 1024;

fn byte_at(offset: usize) -> u8 {
    (offset.wrapping_mul(2_654_435_761) >> 13).to_le_bytes()[0]
}

/// Serves one `BODY_LEN` response as fast as the connection takes it, counting the bytes the
/// socket has accepted.
async fn serve(listener: TcpListener, written: Arc<AtomicUsize>) {
    let (mut socket, _) = listener.accept().await.expect("accept");
    let mut head = Vec::new();
    let mut buf = [0u8; 4096];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = socket.read(&mut buf).await.expect("read request head");
        assert!(n > 0, "connection closed before the request head ended");
        head.extend_from_slice(&buf[..n]);
    }
    socket
        .write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {BODY_LEN}\r\n\r\n").as_bytes())
        .await
        .expect("write head");
    let mut chunk = vec![0u8; CHUNK];
    let mut offset = 0;
    while offset < BODY_LEN {
        for (i, byte) in chunk.iter_mut().enumerate() {
            *byte = byte_at(offset + i);
        }
        if socket.write_all(&chunk).await.is_err() {
            return;
        }
        offset += CHUNK;
        written.store(offset, Ordering::Relaxed);
    }
}

async fn next_frame(
    body: &mut HttpResponseBody,
) -> Option<Result<http_body::Frame<bytes::Bytes>, object_store::client::HttpError>> {
    poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

/// Waits until `written` has not moved for half a second, and returns where it stopped.
async fn settled(written: &AtomicUsize) -> usize {
    let mut last = written.load(Ordering::Relaxed);
    let mut still_for = Duration::ZERO;
    let step = Duration::from_millis(50);
    for _ in 0..600 {
        tokio::time::sleep(step).await;
        let now = written.load(Ordering::Relaxed);
        if now == last {
            still_for += step;
            if still_for >= Duration::from_millis(500) {
                return now;
            }
        } else {
            last = now;
            still_for = Duration::ZERO;
        }
    }
    panic!("the server never stopped writing; last at {last} bytes")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_reader_stops_the_body_and_resumed_reader_gets_all_of_it() {
    const { assert!(STALLED_BOUND > RESPONSE_READ_AHEAD_BYTES) };

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local address");
    let written = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(serve(listener, Arc::clone(&written)));

    let options = ClientOptions::new()
        .with_allow_http(true)
        .with_timeout(Duration::from_mins(5));
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

    let first = next_frame(&mut body)
        .await
        .expect("a first frame")
        .expect("first frame is not an error")
        .into_data()
        .expect("data frame");
    let mut received = first.len();
    // The offset of the first byte that differs from what the server sent.
    let mut mismatch = first
        .iter()
        .enumerate()
        .find(|&(i, &b)| b != byte_at(i))
        .map(|(i, _)| i);

    // Reader paused: the server gets only as far as the read-ahead and the sockets allow.
    let stalled_at = settled(&written).await;
    eprintln!("reader paused after {received} bytes; server stopped at {stalled_at} bytes");
    assert!(
        stalled_at < STALLED_BOUND,
        "with the reader paused after {received} bytes, the server wrote {stalled_at} of \
         {BODY_LEN} bytes; the connector read more than {STALLED_BOUND} bytes ahead"
    );

    // Reader resumed: the rest arrives, in order and intact.
    while let Some(frame) = next_frame(&mut body).await {
        let data = frame
            .expect("body frame is not an error")
            .into_data()
            .expect("data frame");
        if mismatch.is_none() {
            mismatch = data
                .iter()
                .enumerate()
                .find(|&(i, &b)| b != byte_at(received + i))
                .map(|(i, _)| received + i);
        }
        received += data.len();
    }
    assert_eq!(received, BODY_LEN);
    assert_eq!(mismatch, None, "the body differs from what the server sent");

    server.await.expect("server task");
}

/// The object the S3 server below holds.
const OBJECT_LEN: usize = 32 * MIB;

/// Serves ranged and whole GETs of a `OBJECT_LEN` object as fast as the connection takes
/// them, recording the first byte each request asked for.
async fn serve_object(listener: TcpListener, starts: Arc<Mutex<Vec<usize>>>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let starts = Arc::clone(&starts);
        tokio::spawn(async move {
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => head.extend_from_slice(&buf[..n]),
                }
            }
            let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
            let range = head.lines().find_map(|line| {
                let (start, end) = line.strip_prefix("range: bytes=")?.split_once('-')?;
                let start: usize = start.trim().parse().ok()?;
                let end: usize = end.trim().parse().unwrap_or(OBJECT_LEN - 1);
                Some(start..end.min(OBJECT_LEN - 1) + 1)
            });
            let (status, range) = match range {
                Some(range) => ("206 Partial Content", range),
                None => ("200 OK", 0..OBJECT_LEN),
            };
            starts.lock().expect("starts lock").push(range.start);
            let response_head = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{OBJECT_LEN}\r\nETag: \"v1\"\r\nLast-Modified: Wed, 07 Oct 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n",
                range.len(),
                range.start,
                range.end - 1,
            );
            if socket.write_all(response_head.as_bytes()).await.is_err() {
                return;
            }
            let mut chunk = vec![0u8; CHUNK];
            let mut offset = range.start;
            while offset < range.end {
                let n = CHUNK.min(range.end - offset);
                for (i, byte) in chunk[..n].iter_mut().enumerate() {
                    *byte = byte_at(offset + i);
                }
                if socket.write_all(&chunk[..n]).await.is_err() {
                    return;
                }
                offset += n;
            }
        });
    }
}

/// A reader that stops reading for longer than the client timeout no longer has the rest of
/// the body waiting in memory; the body times out on the wire instead, and `object_store`
/// resumes it from the first byte the reader has not seen, so the reader still gets the
/// whole object, byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_paused_past_the_client_timeout_reads_the_whole_object() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local address");
    let starts = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(serve_object(listener, Arc::clone(&starts)));

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

    let result = store
        .get(&Path::from("events.csv"))
        .await
        .expect("object head");
    let GetResultPayload::Stream(mut stream) = result.payload else {
        panic!("an HTTP store returns a stream");
    };
    let first = stream
        .next()
        .await
        .expect("a first chunk")
        .expect("first chunk is not an error");
    let mut received = first.len();
    let mut mismatch = first
        .iter()
        .enumerate()
        .find(|&(i, &b)| b != byte_at(i))
        .map(|(i, _)| i);

    // Past the 1 s client timeout, with the rest of the object still on the wire.
    tokio::time::sleep(Duration::from_secs(3)).await;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("the body resumes rather than failing");
        if mismatch.is_none() {
            mismatch = chunk
                .iter()
                .enumerate()
                .find(|&(i, &b)| b != byte_at(received + i))
                .map(|(i, _)| received + i);
        }
        received += chunk.len();
    }
    assert_eq!(received, OBJECT_LEN);
    assert_eq!(mismatch, None, "the object differs from what the server holds");

    let starts = starts.lock().expect("starts lock").clone();
    assert_eq!(starts.first(), Some(&0), "{starts:?}");
    assert!(
        starts.len() >= 2 && starts.windows(2).all(|w| w[0] < w[1]),
        "the paused body is resumed from where the reader stopped: {starts:?}"
    );

    server.abort();
}

