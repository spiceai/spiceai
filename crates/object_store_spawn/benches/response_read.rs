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

//! Latency of reading response bodies through [`SpawnedReqwestConnector`], the path every
//! object-store read (S3, Azure, GCS, HTTP) takes, from a local server that sends as fast as
//! the socket allows.
//!
//! Three readers, each the shape of a real one:
//!
//! - `whole`: one 64 MiB body read to its end as fast as frames arrive, as `get().bytes()` and a
//!   Parquet range read collect a body.
//! - `ranges`: 16 concurrent 4 MiB range reads collected together, as `get_ranges` reads the
//!   column chunks of a Parquet row group.
//! - `streamed`: one 64 MiB body read frame by frame with a pass over each frame's bytes, as a
//!   CSV or JSON scan decodes a stream.
//!
//! Each request is timed on its own; the bench prints P50, P99 and max per reader, and the
//! throughput at P50. Run it as `cargo bench -p object_store_spawn --bench response_read`, and
//! compare builds on one machine with the arms interleaved.

#![expect(
    clippy::expect_used,
    clippy::cast_precision_loss,
    reason = "bench harness"
)]

use std::{
    future::poll_fn,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use http_body::Body;
use object_store::{
    ClientOptions,
    client::{HttpClient, HttpConnector, HttpRequest, HttpRequestBody},
};
use object_store_spawn::SpawnedReqwestConnector;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    runtime::Handle,
};

const MIB: usize = 1024 * 1024;
const WHOLE_LEN: usize = 64 * MIB;
const RANGE_LEN: usize = 4 * MIB;
const RANGES: usize = 16;
const WARMUP: usize = 5;
const ITERATIONS: usize = 200;

/// Serves `/<len>` with a body of `len` bytes to every connection, keeping connections alive.
async fn serve(listener: TcpListener, body: Arc<Vec<u8>>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let body = Arc::clone(&body);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            let mut head = Vec::new();
            loop {
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let end = head
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .expect("request head end")
                    + 4;
                let request = String::from_utf8_lossy(&head[..end]).into_owned();
                head.drain(..end);
                let len: usize = request
                    .split_whitespace()
                    .nth(1)
                    .and_then(|path| path.trim_start_matches('/').parse().ok())
                    .unwrap_or(0)
                    .min(body.len());
                let response_head = format!("HTTP/1.1 200 OK\r\nContent-Length: {len}\r\n\r\n");
                if socket.write_all(response_head.as_bytes()).await.is_err()
                    || socket.write_all(&body[..len]).await.is_err()
                {
                    return;
                }
            }
        });
    }
}

async fn get(client: &HttpClient, addr: &str, len: usize) -> object_store::client::HttpResponse {
    let mut request = HttpRequest::new(HttpRequestBody::empty());
    *request.uri_mut() = format!("http://{addr}/{len}").parse().expect("URI");
    client.execute(request).await.expect("response")
}

/// Reads `response` to its end, passing over each frame's bytes when `touch` is set, and
/// returns how many bytes it read.
async fn read(response: object_store::client::HttpResponse, touch: bool) -> usize {
    let mut body = response.into_body();
    let mut read = 0;
    let mut checksum = 0u64;
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let data = frame.expect("frame").into_data().expect("data frame");
        if touch {
            checksum = data.iter().fold(checksum, |sum, &b| {
                sum.wrapping_mul(31).wrapping_add(u64::from(b))
            });
        }
        read += data.len();
    }
    std::hint::black_box(checksum);
    read
}

fn report(name: &str, bytes_per_request: usize, mut samples: Vec<Duration>) {
    samples.sort();
    let at = |q: f64| samples[((samples.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    let p50 = at(0.50);
    let p99 = at(0.99);
    let max = samples[samples.len() - 1];
    let mib_per_s = bytes_per_request as f64 / MIB as f64 / p50.as_secs_f64();
    println!(
        "{name:<10} n={} p50={:.2}ms p99={:.2}ms max={:.2}ms throughput_at_p50={mib_per_s:.0}MiB/s",
        samples.len(),
        p50.as_secs_f64() * 1e3,
        p99.as_secs_f64() * 1e3,
        max.as_secs_f64() * 1e3,
    );
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("address").to_string();
        let body: Arc<Vec<u8>> = Arc::new((0..WHOLE_LEN).map(|i| (i % 251) as u8).collect());
        tokio::spawn(serve(listener, body));

        let client = SpawnedReqwestConnector::new(Handle::current())
            .connect(&ClientOptions::new().with_allow_http(true))
            .expect("client");

        let mut whole = Vec::with_capacity(ITERATIONS);
        let mut ranges = Vec::with_capacity(ITERATIONS);
        let mut streamed = Vec::with_capacity(ITERATIONS);
        for i in 0..WARMUP + ITERATIONS {
            let started = Instant::now();
            assert_eq!(
                read(get(&client, &addr, WHOLE_LEN).await, false).await,
                WHOLE_LEN
            );
            let whole_took = started.elapsed();

            let started = Instant::now();
            let reads = (0..RANGES)
                .map(|_| async { read(get(&client, &addr, RANGE_LEN).await, false).await });
            let total: usize = futures::future::join_all(reads).await.into_iter().sum();
            assert_eq!(total, RANGES * RANGE_LEN);
            let ranges_took = started.elapsed();

            let started = Instant::now();
            assert_eq!(
                read(get(&client, &addr, WHOLE_LEN).await, true).await,
                WHOLE_LEN
            );
            let streamed_took = started.elapsed();

            if i >= WARMUP {
                whole.push(whole_took);
                ranges.push(ranges_took);
                streamed.push(streamed_took);
            }
        }
        report("whole", WHOLE_LEN, whole);
        report("ranges", RANGES * RANGE_LEN, ranges);
        report("streamed", WHOLE_LEN, streamed);
    });
}
