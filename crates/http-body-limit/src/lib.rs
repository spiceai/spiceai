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

//! Size-capped reads of HTTP response bodies.
//!
//! `reqwest`'s `text()`, `json()` and `bytes()` read a whole body into memory with no
//! cap, and with transparent decompression on, the cap that matters is on the
//! *decompressed* size: a few hundred kilobytes of gzip, brotli or zstd can expand to
//! gigabytes. These helpers read the body chunk by chunk as the decoder produces it and
//! stop with [`Error::TooLarge`] as soon as the limit would be passed, so a response
//! never costs more than the limit, however it is encoded.

use bytes::{Bytes, BytesMut};
use reqwest::{Response, ResponseBuilderExt};
use snafu::prelude::*;

/// The most a response body is allowed to hold once decoded, for a body that is read
/// into memory whole.
///
/// It matches the 128 MiB the runtime accepts as a request body on its own HTTP API,
/// and is far above what one API or metadata response carries in practice.
pub const MAX_RESPONSE_BODY_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Snafu)]
pub enum Error {
    /// The decoded body would pass `limit` bytes. Asking again gets the same answer.
    #[snafu(display(
        "the response body is larger than the {} that is read from one HTTP response",
        format_limit(*limit)
    ))]
    TooLarge { limit: usize },

    /// Reading the body failed: a connection reset, a timeout, a truncated body, or a
    /// compressed stream that does not decode.
    #[snafu(display("{source}"))]
    Read { source: reqwest::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Read the decoded body of `response`, failing with [`Error::TooLarge`] as soon as it
/// would hold more than `limit` bytes.
///
/// A declared `Content-Length` over the limit is refused before any of the body is read.
///
/// # Errors
///
/// [`Error::TooLarge`] when the body is larger than `limit`, and [`Error::Read`] when
/// reading it fails.
pub async fn read_bytes(mut response: Response, limit: usize) -> Result<Bytes> {
    let declared = response.content_length();
    ensure!(
        declared.is_none_or(|length| length <= u64::try_from(limit).unwrap_or(u64::MAX)),
        TooLargeSnafu { limit }
    );

    // Most bodies arrive as one chunk, which is returned as it is rather than copied.
    let Some(first) = response.chunk().await.context(ReadSnafu)? else {
        return Ok(Bytes::new());
    };
    ensure!(first.len() <= limit, TooLargeSnafu { limit });
    let Some(second) = response.chunk().await.context(ReadSnafu)? else {
        return Ok(first);
    };

    let capacity = declared
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(0)
        .max(first.len().saturating_add(second.len()))
        .min(limit);
    let mut body = BytesMut::with_capacity(capacity);
    body.extend_from_slice(&first);
    let mut next = Some(second);
    while let Some(chunk) = next {
        ensure!(
            chunk.len() <= limit - body.len(),
            TooLargeSnafu { limit }
        );
        body.extend_from_slice(&chunk);
        next = response.chunk().await.context(ReadSnafu)?;
    }
    Ok(body.freeze())
}

/// Read the body of `response` into memory under `limit` (see [`read_bytes`]) and hand
/// back a response that replays it, with the same status, version, headers and URL.
///
/// The returned response's `text()` and `json()` behave exactly as on the original —
/// the same charset handling and the same errors — but can no longer allocate more than
/// `limit`, so a call site keeps its decoding and only gains the cap.
///
/// # Errors
///
/// As [`read_bytes`].
pub async fn buffer(response: Response, limit: usize) -> Result<Response> {
    let status = response.status();
    let version = response.version();
    let headers = response.headers().clone();
    let url = response.url().clone();

    let body = read_bytes(response, limit).await?;

    // The builder carries only the URL, which cannot fail to set; the default is never
    // reached and would only lose the URL from later error messages.
    let (mut parts, ()) = http::Response::builder()
        .url(url)
        .body(())
        .unwrap_or_default()
        .into_parts();
    parts.status = status;
    parts.version = version;
    parts.headers = headers;
    Ok(Response::from(http::Response::from_parts(parts, body)))
}

fn format_limit(limit: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if limit >= MIB && limit.is_multiple_of(MIB) {
        format!("{} MiB", limit / MIB)
    } else {
        format!("{limit} bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const MIB: usize = 1024 * 1024;

    /// `len` bytes of `b'A'` encoded as `encoding`, built a block at a time so the
    /// plain text is never held whole.
    fn compressed(encoding: &str, len: usize) -> Vec<u8> {
        let block = vec![b'A'; MIB];
        let mut remaining = len;
        let mut write_all = |w: &mut dyn Write| {
            while remaining > 0 {
                let n = remaining.min(block.len());
                w.write_all(&block[..n]).expect("compress block");
                remaining -= n;
            }
        };
        match encoding {
            "gzip" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
                write_all(&mut encoder);
                encoder.finish().expect("finish gzip")
            }
            "zstd" => {
                let mut encoder = zstd::Encoder::new(Vec::new(), 19).expect("zstd encoder");
                write_all(&mut encoder);
                encoder.finish().expect("finish zstd")
            }
            other => panic!("unsupported encoding {other}"),
        }
    }

    /// Serve `body` with the given headers to every connection, over HTTP/1.1.
    async fn serve(headers: &'static str, body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server address");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let head = format!(
                        "HTTP/1.1 200 OK\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    // The client hangs up once it has seen enough; that is the point.
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        format!("http://{addr}/")
    }

    async fn get(url: &str) -> Response {
        reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("request the test server")
    }

    #[tokio::test]
    async fn gzip_bomb_stops_at_the_limit() {
        let body = compressed("gzip", 64 * MIB);
        assert!(body.len() < MIB, "the bomb is small on the wire");
        let url = serve("Content-Encoding: gzip\r\n", body).await;

        let error = read_bytes(get(&url).await, MIB)
            .await
            .expect_err("a body that decodes past the limit is refused");
        assert!(
            matches!(error, Error::TooLarge { limit } if limit == MIB),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "the response body is larger than the 1 MiB that is read from one HTTP response"
        );
    }

    #[tokio::test]
    async fn zstd_bomb_stops_at_the_limit() {
        let url = serve("Content-Encoding: zstd\r\n", compressed("zstd", 64 * MIB)).await;

        let error = buffer(get(&url).await, MIB)
            .await
            .expect_err("a body that decodes past the limit is refused");
        assert!(
            matches!(error, Error::TooLarge { limit } if limit == MIB),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn compressed_body_under_the_limit_is_decoded() {
        let url = serve("Content-Encoding: gzip\r\n", compressed("gzip", 3 * MIB)).await;

        let body = read_bytes(get(&url).await, 4 * MIB)
            .await
            .expect("a body under the limit is read");
        assert_eq!(body.len(), 3 * MIB);
        assert!(body.iter().all(|&b| b == b'A'));
    }

    #[tokio::test]
    async fn limit_is_inclusive() {
        let url = serve("Content-Encoding: gzip\r\n", compressed("gzip", 2 * MIB)).await;
        let body = read_bytes(get(&url).await, 2 * MIB)
            .await
            .expect("a body of exactly the limit is read");
        assert_eq!(body.len(), 2 * MIB);

        let error = read_bytes(get(&url).await, 2 * MIB - 1)
            .await
            .expect_err("one byte over the limit is refused");
        assert!(
            matches!(error, Error::TooLarge { limit } if limit == 2 * MIB - 1),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn declared_length_over_the_limit_is_refused_before_reading() {
        let url = serve("", vec![b'A'; 4096]).await;
        let response = get(&url).await;
        assert_eq!(response.content_length(), Some(4096));

        let error = read_bytes(response, 4095)
            .await
            .expect_err("a declared length over the limit is refused");
        assert!(
            matches!(error, Error::TooLarge { limit: 4095 }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn empty_body_reads_as_empty() {
        let url = serve("", Vec::new()).await;
        let body = read_bytes(get(&url).await, 0)
            .await
            .expect("an empty body fits any limit");
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn buffered_response_keeps_status_headers_url_and_charset() {
        // "café" in ISO-8859-1: `text()` must still decode by the declared charset.
        let url = serve(
            "Content-Type: text/plain; charset=iso-8859-1\r\nX-Probe: kept\r\n",
            b"caf\xe9".to_vec(),
        )
        .await;
        let response = buffer(get(&url).await, MIB)
            .await
            .expect("a small body is buffered");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.url().as_str(), url);
        assert_eq!(
            response
                .headers()
                .get("x-probe")
                .and_then(|v| v.to_str().ok()),
            Some("kept")
        );
        assert_eq!(response.text().await.expect("decode text"), "café");
    }

    #[tokio::test]
    async fn buffered_response_decodes_json() {
        let url = serve(
            "Content-Type: application/json\r\n",
            br#"{"rows":[1,2,3]}"#.to_vec(),
        )
        .await;
        let value: serde_json::Value = buffer(get(&url).await, MIB)
            .await
            .expect("a small body is buffered")
            .json()
            .await
            .expect("decode json");
        assert_eq!(value.to_string(), r#"{"rows":[1,2,3]}"#);
    }
}
