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

//! Reads of HTTP response bodies that refuse decompression amplification.
//!
//! A compressed body costs the sender far less than it costs the reader: a few hundred
//! kilobytes of gzip decode to hundreds of megabytes, and brotli or zstd to gigabytes.
//! An uncompressed body costs both sides the same, like any other large data source,
//! and is read as it always was. So the check here applies only to a body sent with a
//! `Content-Encoding`: once it has decoded past [`RATIO_CHECK_FLOOR_BYTES`], it is
//! refused as soon as it has expanded more than [`MAX_COMPRESSION_RATIO`] times the
//! bytes received for it. The check runs as each block is decoded, so a refused body
//! never holds much more than the floor.
//!
//! Counting the bytes received needs the body before it is decoded, which `reqwest`'s
//! transparent decompression does not expose. A client whose bodies are checked is
//! therefore built with [`decode_manually`], and *every* body read from it must go
//! through [`buffer`], [`read_bytes`] or [`read_prefix`]: they decode with the same
//! decoders `reqwest` uses, while counting both sides.
//!
//! On a client that still decompresses transparently, `reqwest` has already removed
//! the `Content-Encoding` header from a body it decoded, so these functions read that
//! body as an uncompressed one: correct, but without the ratio check.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder, ZstdDecoder};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, HeaderMap, HeaderValue};
use reqwest::{ClientBuilder, Response, ResponseBuilderExt};
use snafu::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

/// How much a compressed body may decode to before its compression ratio is checked.
///
/// Below it, any ratio is accepted: a small response that compresses extremely well
/// (a page of identical rows) is ordinary, and costs little however small it was on
/// the wire. It is also roughly the most a refused body holds before it is refused.
pub const RATIO_CHECK_FLOOR_BYTES: usize = 64 * 1024 * 1024;

/// The most a compressed body may expand, decoded bytes per byte received, once it has
/// decoded past [`RATIO_CHECK_FLOOR_BYTES`].
///
/// Realistic JSON responses compress far less: TPC-H-shaped rows as a JSON array or
/// NDJSON 6–13:1, Elasticsearch search hits 8–16:1 and column-oriented JSON 7–21:1,
/// across gzip 1–9, deflate, brotli 5–11 and zstd 3–19. A compression bomb sits at the
/// codec's limit: 1028:1 for gzip and deflate, and 32,000:1 to 645,000:1 for zstd and
/// brotli.
pub const MAX_COMPRESSION_RATIO: usize = 100;

/// The most a metadata or token response is allowed to hold once decoded, compressed
/// or not, on the paths that also cap size because they never carry bulk data.
///
/// It matches the 128 MiB the runtime accepts as a request body on its own HTTP API.
pub const MAX_METADATA_BODY_BYTES: usize = 128 * 1024 * 1024;

/// The `Accept-Encoding` a client built with [`decode_manually`] sends, the same one
/// `reqwest` sends when it decompresses transparently.
pub const ACCEPT_ENCODING_VALUE: &str = "zstd,gzip,deflate,br";

/// The size of each block decoded at a time, and so how far past a limit a body can
/// get before the limit is enforced.
pub const DECODE_BLOCK_BYTES: usize = 64 * 1024;

/// What a read enforces beyond decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    /// The compression ratio check on a compressed body. An uncompressed body is read
    /// exactly as `reqwest` reads it, at any size.
    CompressionRatio,
    /// The compression ratio check, and a cap on the decoded size of any body, for
    /// responses that are never bulk data.
    CompressionRatioAndSize(usize),
}

impl Limit {
    /// The limit for a metadata or token response: the ratio check, and
    /// [`MAX_METADATA_BODY_BYTES`].
    pub const METADATA: Self = Self::CompressionRatioAndSize(MAX_METADATA_BODY_BYTES);

    fn max_bytes(self) -> Option<usize> {
        match self {
            Self::CompressionRatio => None,
            Self::CompressionRatioAndSize(max) => Some(max),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    /// The compressed body expanded past [`MAX_COMPRESSION_RATIO`] once past
    /// [`RATIO_CHECK_FLOOR_BYTES`]. Asking again gets the same answer.
    #[snafu(display(
        "the {encoding}-compressed response body expanded more than {MAX_COMPRESSION_RATIO}:1 \
         when decoded ({decoded} bytes from {received} received), which is refused once a \
         body passes {} decoded",
        format_bytes(RATIO_CHECK_FLOOR_BYTES)
    ))]
    CompressionRatio {
        encoding: Encoding,
        decoded: usize,
        received: usize,
    },

    /// The decoded body would pass the size cap. Asking again gets the same answer.
    #[snafu(display(
        "the response body is larger than the {} that is read from one HTTP response",
        format_bytes(*limit)
    ))]
    TooLarge { limit: usize },

    /// Reading the body failed: a connection reset, a timeout or a truncated body.
    #[snafu(display("{source}"))]
    Read { source: reqwest::Error },

    /// The body does not decode as the encoding it was sent with.
    #[snafu(display("Failed to decode the {encoding}-compressed response body: {source}"))]
    Decode {
        encoding: Encoding,
        source: io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `Content-Encoding` this crate decodes: exactly the ones `reqwest` decodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Gzip,
    Deflate,
    Brotli,
    Zstd,
}

impl Encoding {
    /// The encoding a response was sent with, matched as `reqwest` matches it: one
    /// exact token. Anything else is read as it is, as `reqwest` reads it.
    #[must_use]
    pub fn of(headers: &HeaderMap) -> Option<Self> {
        match headers.get(CONTENT_ENCODING)?.as_bytes() {
            b"gzip" => Some(Self::Gzip),
            b"deflate" => Some(Self::Deflate),
            b"br" => Some(Self::Brotli),
            b"zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
            Self::Brotli => "br",
            Self::Zstd => "zstd",
        }
    }
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Turn off `reqwest`'s transparent decompression, so bodies read through this crate
/// can be checked, while still asking for compressed responses as `reqwest` does.
///
/// Apply it after anything that turns decompression on. Every body read from the
/// built client must go through [`buffer`], [`read_bytes`] or [`read_prefix`]; a body
/// read any other way arrives still compressed.
pub fn decode_manually(builder: ClientBuilder) -> ClientBuilder {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT_ENCODING,
        HeaderValue::from_static(ACCEPT_ENCODING_VALUE),
    );
    builder
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .default_headers(headers)
}

/// Read the decoded body of `response` under `limit`.
///
/// # Errors
///
/// [`Error::CompressionRatio`] or [`Error::TooLarge`] when the body breaks `limit`,
/// [`Error::Read`] when reading it fails, and [`Error::Decode`] when it does not decode.
pub async fn read_bytes(response: Response, limit: Limit) -> Result<Bytes> {
    match (Encoding::of(response.headers()), limit.max_bytes()) {
        (Some(encoding), max_bytes) => decode(response, encoding, max_bytes).await,
        (None, None) => response.bytes().await.context(ReadSnafu),
        (None, Some(max_bytes)) => read_capped(response, max_bytes).await,
    }
}

/// Read the body of `response` under `limit` (see [`read_bytes`]) and hand back a
/// response that replays it decoded, with the same status, version, headers and URL.
///
/// The returned response's `text()` and `json()` behave as on a client that
/// decompresses transparently — the same charset handling and the same errors. Like
/// that client, a decoded response loses its `Content-Encoding` and `Content-Length`.
/// An uncompressed response under [`Limit::CompressionRatio`] is returned untouched.
///
/// # Errors
///
/// As [`read_bytes`].
pub async fn buffer(response: Response, limit: Limit) -> Result<Response> {
    let encoding = Encoding::of(response.headers());
    if encoding.is_none() && limit == Limit::CompressionRatio {
        return Ok(response);
    }

    let status = response.status();
    let version = response.version();
    let mut headers = response.headers().clone();
    let url = response.url().clone();
    if encoding.is_some() {
        headers.remove(CONTENT_ENCODING);
        headers.remove(CONTENT_LENGTH);
    }

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

/// Read at most `max_bytes` of the decoded body of `response`, for an error message.
///
/// Best effort by design: a read or decode failure ends the read with what was read so
/// far. It never reads more than `max_bytes`, so it needs no ratio check.
pub async fn read_prefix(response: Response, max_bytes: usize) -> Vec<u8> {
    let mut out = Vec::new();
    match Encoding::of(response.headers()) {
        None => {
            let mut response = response;
            while out.len() < max_bytes {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
                        let take = chunk.len().min(max_bytes - out.len());
                        out.extend_from_slice(&chunk[..take]);
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        }
        Some(encoding) => {
            let (mut decoder, _) = decoder(response, encoding);
            let mut block = vec![0u8; DECODE_BLOCK_BYTES.min(max_bytes.max(1))];
            while out.len() < max_bytes {
                match decoder.read(&mut block).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let take = n.min(max_bytes - out.len());
                        out.extend_from_slice(&block[..take]);
                    }
                }
            }
        }
    }
    out
}

async fn read_capped(mut response: Response, max_bytes: usize) -> Result<Bytes> {
    // Most bodies arrive as one chunk, which is returned as it is rather than copied.
    let Some(first) = response.chunk().await.context(ReadSnafu)? else {
        return Ok(Bytes::new());
    };
    ensure!(first.len() <= max_bytes, TooLargeSnafu { limit: max_bytes });
    let Some(second) = response.chunk().await.context(ReadSnafu)? else {
        return Ok(first);
    };

    let mut body = BytesMut::with_capacity(first.len().saturating_add(second.len()));
    body.extend_from_slice(&first);
    let mut next = Some(second);
    while let Some(chunk) = next {
        ensure!(
            chunk.len() <= max_bytes - body.len(),
            TooLargeSnafu { limit: max_bytes }
        );
        body.extend_from_slice(&chunk);
        next = response.chunk().await.context(ReadSnafu)?;
    }
    Ok(body.freeze())
}

type Decoder = Pin<Box<dyn AsyncRead + Send>>;

/// A decoder over the body of `response`, and the count of body bytes it has been
/// handed so far.
fn decoder(response: Response, encoding: Encoding) -> (Decoder, Arc<AtomicUsize>) {
    let received = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&received);
    let chunks = futures::stream::try_unfold(response, |mut response| async move {
        let chunk: Option<Bytes> = response.chunk().await?;
        Ok::<_, reqwest::Error>(chunk.map(|chunk| (chunk, response)))
    })
    .map_ok(move |chunk: Bytes| {
        counter.fetch_add(chunk.len(), Ordering::Relaxed);
        chunk
    })
    .map_err(io::Error::other)
    .boxed();
    let reader = StreamReader::new(chunks);
    let decoder: Decoder = match encoding {
        Encoding::Gzip => Box::pin(GzipDecoder::new(reader)),
        Encoding::Deflate => Box::pin(ZlibDecoder::new(reader)),
        Encoding::Brotli => Box::pin(BrotliDecoder::new(reader)),
        Encoding::Zstd => Box::pin(ZstdDecoder::new(reader)),
    };
    (decoder, received)
}

async fn decode(response: Response, encoding: Encoding, max_bytes: Option<usize>) -> Result<Bytes> {
    let (mut reader, received_bytes) = decoder(response, encoding);

    // Decoded into blocks of a fixed size and joined once at the end, so a body that
    // is refused holds no more than the blocks it decoded, and one that is not is
    // copied once into an allocation of exactly its size.
    let mut blocks: Vec<Bytes> = Vec::new();
    let mut block = BytesMut::with_capacity(DECODE_BLOCK_BYTES);
    let mut decoded = 0usize;
    loop {
        if block.len() == block.capacity() {
            blocks.push(block.split().freeze());
            block.reserve(DECODE_BLOCK_BYTES);
        }
        let n = reader
            .read_buf(&mut block)
            .await
            .map_err(|error| read_failure(error, encoding))?;
        if n == 0 {
            break;
        }
        decoded += n;
        if let Some(max_bytes) = max_bytes {
            ensure!(decoded <= max_bytes, TooLargeSnafu { limit: max_bytes });
        }
        let received = received_bytes.load(Ordering::Relaxed);
        ensure!(
            decoded <= RATIO_CHECK_FLOOR_BYTES
                || decoded <= received.saturating_mul(MAX_COMPRESSION_RATIO),
            CompressionRatioSnafu {
                encoding,
                decoded,
                received,
            }
        );
    }

    if blocks.is_empty() {
        return Ok(block.freeze());
    }
    let mut body = BytesMut::with_capacity(decoded);
    for part in blocks.drain(..) {
        body.extend_from_slice(&part);
    }
    body.extend_from_slice(&block);
    Ok(body.freeze())
}

/// A failed read from a decoder: either reading the body failed, which is passed
/// through, or the body does not decode.
fn read_failure(error: io::Error, encoding: Encoding) -> Error {
    match error.downcast::<reqwest::Error>() {
        Ok(source) => Error::Read { source },
        Err(source) => Error::Decode { encoding, source },
    }
}

fn format_bytes(bytes: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else {
        format!("{bytes} bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    const MIB: usize = 1024 * 1024;

    /// `len` bytes produced by `fill`, encoded as `encoding`, built a block at a time
    /// so the plain text is never held whole. The fastest levels keep the test quick;
    /// a run of one byte still compresses past 200:1 at every one of them.
    fn compressed(encoding: Encoding, len: usize, fill: impl Fn(usize) -> u8) -> Vec<u8> {
        let mut remaining = len;
        let mut offset = 0;
        let mut write_all = |w: &mut dyn Write| {
            while remaining > 0 {
                let n = remaining.min(MIB);
                let block: Vec<u8> = (offset..offset + n).map(&fill).collect();
                w.write_all(&block).expect("compress block");
                remaining -= n;
                offset += n;
            }
        };
        match encoding {
            Encoding::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                write_all(&mut encoder);
                encoder.finish().expect("finish gzip")
            }
            Encoding::Deflate => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
                write_all(&mut encoder);
                encoder.finish().expect("finish deflate")
            }
            Encoding::Zstd => {
                let mut encoder = zstd::Encoder::new(Vec::new(), 1).expect("zstd encoder");
                write_all(&mut encoder);
                encoder.finish().expect("finish zstd")
            }
            Encoding::Brotli => {
                let mut out = Vec::new();
                {
                    let mut encoder = brotli::CompressorWriter::new(&mut out, 4096, 1, 22);
                    write_all(&mut encoder);
                }
                out
            }
        }
    }

    fn same_byte(_: usize) -> u8 {
        b'A'
    }

    /// Byte `i` of zero-padded ascending decimal numbers, one per line, like an id
    /// column: text that compresses a few times over, as real data does.
    fn counter_text(i: usize) -> u8 {
        let (line, position) = (i / 9, i % 9);
        if position == 8 {
            return b'\n';
        }
        let place = 10usize.pow(u32::try_from(7 - position).expect("a digit place"));
        b"0123456789"[line / place % 10]
    }

    /// Serve `body` with the given headers to every connection, over HTTP/1.1.
    async fn serve(headers: &'static str, body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server address");
        let body = Arc::new(body);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let body = Arc::clone(&body);
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

    fn manual_client() -> reqwest::Client {
        decode_manually(reqwest::Client::builder())
            .build()
            .expect("build client")
    }

    async fn get(url: &str) -> Response {
        manual_client()
            .get(url)
            .send()
            .await
            .expect("request the test server")
    }

    async fn assert_bomb_refused_near_the_floor(encoding: Encoding, header: &'static str) {
        let body = compressed(encoding, RATIO_CHECK_FLOOR_BYTES + 64 * MIB, same_byte);
        assert!(body.len() < MIB, "the bomb is small on the wire");
        let url = serve(header, body).await;

        let error = read_bytes(get(&url).await, Limit::CompressionRatio)
            .await
            .expect_err("a body that expands past the ratio is refused");
        let Error::CompressionRatio {
            encoding: refused,
            decoded,
            received,
        } = error
        else {
            panic!("expected a compression ratio error, got {error:?}");
        };
        assert_eq!(refused, encoding);
        assert!(
            decoded > RATIO_CHECK_FLOOR_BYTES
                && decoded <= RATIO_CHECK_FLOOR_BYTES + DECODE_BLOCK_BYTES,
            "refused just past the floor, at {decoded} bytes"
        );
        assert!(decoded > received * MAX_COMPRESSION_RATIO);
    }

    #[tokio::test]
    async fn gzip_bomb_is_refused_just_past_the_floor() {
        assert_bomb_refused_near_the_floor(Encoding::Gzip, "Content-Encoding: gzip\r\n").await;
    }

    #[tokio::test]
    async fn deflate_bomb_is_refused_just_past_the_floor() {
        assert_bomb_refused_near_the_floor(Encoding::Deflate, "Content-Encoding: deflate\r\n")
            .await;
    }

    #[tokio::test]
    async fn brotli_bomb_is_refused_just_past_the_floor() {
        assert_bomb_refused_near_the_floor(Encoding::Brotli, "Content-Encoding: br\r\n").await;
    }

    #[tokio::test]
    async fn zstd_bomb_is_refused_just_past_the_floor() {
        assert_bomb_refused_near_the_floor(Encoding::Zstd, "Content-Encoding: zstd\r\n").await;
    }

    #[tokio::test]
    async fn compressed_body_at_a_realistic_ratio_is_decoded_past_the_floor() {
        let len = RATIO_CHECK_FLOOR_BYTES + 16 * MIB;
        let body = compressed(Encoding::Gzip, len, counter_text);
        let ratio = len / body.len();
        assert!(
            (2..MAX_COMPRESSION_RATIO).contains(&ratio),
            "the test body compresses {ratio}:1"
        );
        let url = serve("Content-Encoding: gzip\r\n", body).await;

        let decoded = read_bytes(get(&url).await, Limit::CompressionRatio)
            .await
            .expect("a realistic ratio is read in full");
        assert_eq!(decoded.len(), len);
        assert!(
            decoded
                .iter()
                .enumerate()
                .all(|(i, &b)| b == counter_text(i))
        );
    }

    #[tokio::test]
    async fn any_ratio_is_accepted_under_the_floor() {
        let url = serve(
            "Content-Encoding: gzip\r\n",
            compressed(Encoding::Gzip, 8 * MIB, same_byte),
        )
        .await;
        let decoded = read_bytes(get(&url).await, Limit::CompressionRatio)
            .await
            .expect("a small body is read at any ratio");
        assert_eq!(decoded.len(), 8 * MIB);
        assert!(decoded.iter().all(|&b| b == b'A'));
    }

    #[tokio::test]
    async fn uncompressed_body_is_not_capped_and_its_length_is_not_refused() {
        let len = 3 * MIB;
        let url = serve("", vec![b'A'; len]).await;
        let response = get(&url).await;
        assert_eq!(response.content_length(), Some(len as u64));
        let body = read_bytes(response, Limit::CompressionRatio)
            .await
            .expect("an uncompressed body is read at any size");
        assert_eq!(body.len(), len);
    }

    #[tokio::test]
    async fn size_cap_applies_to_compressed_and_uncompressed_bodies() {
        let url = serve("", vec![b'A'; 2 * MIB]).await;
        let body = read_bytes(get(&url).await, Limit::CompressionRatioAndSize(2 * MIB))
            .await
            .expect("a body of exactly the cap is read");
        assert_eq!(body.len(), 2 * MIB);
        let error = read_bytes(get(&url).await, Limit::CompressionRatioAndSize(2 * MIB - 1))
            .await
            .expect_err("one byte over the cap is refused");
        assert!(
            matches!(error, Error::TooLarge { limit } if limit == 2 * MIB - 1),
            "{error:?}"
        );

        let url = serve(
            "Content-Encoding: gzip\r\n",
            compressed(Encoding::Gzip, 2 * MIB, same_byte),
        )
        .await;
        let body = read_bytes(get(&url).await, Limit::CompressionRatioAndSize(2 * MIB))
            .await
            .expect("a compressed body of exactly the cap is read");
        assert_eq!(body.len(), 2 * MIB);
        let error = read_bytes(get(&url).await, Limit::CompressionRatioAndSize(2 * MIB - 1))
            .await
            .expect_err("one decoded byte over the cap is refused");
        assert!(
            matches!(error, Error::TooLarge { limit } if limit == 2 * MIB - 1),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "the response body is larger than the 2097151 bytes that is read from one HTTP response"
        );
    }

    #[tokio::test]
    async fn corrupt_compressed_body_is_a_decode_error() {
        let url = serve("Content-Encoding: gzip\r\n", b"not gzip at all".to_vec()).await;
        let error = read_bytes(get(&url).await, Limit::CompressionRatio)
            .await
            .expect_err("a body that does not decode is refused");
        assert!(
            matches!(
                error,
                Error::Decode {
                    encoding: Encoding::Gzip,
                    ..
                }
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn unknown_encoding_is_read_as_it_is() {
        let url = serve("Content-Encoding: x-custom\r\n", b"raw bytes".to_vec()).await;
        let body = read_bytes(get(&url).await, Limit::CompressionRatio)
            .await
            .expect("an encoding reqwest does not decode is passed through");
        assert_eq!(&body[..], b"raw bytes");
    }

    #[tokio::test]
    async fn empty_bodies_read_as_empty() {
        let url = serve("", Vec::new()).await;
        let body = read_bytes(get(&url).await, Limit::CompressionRatioAndSize(0))
            .await
            .expect("an empty body fits any cap");
        assert!(body.is_empty());

        let url = serve(
            "Content-Encoding: gzip\r\n",
            compressed(Encoding::Gzip, 0, same_byte),
        )
        .await;
        let body = read_bytes(get(&url).await, Limit::CompressionRatio)
            .await
            .expect("an empty compressed body decodes");
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn buffered_response_is_decoded_and_keeps_status_headers_url_and_charset() {
        // "café" in ISO-8859-1: `text()` must still decode by the declared charset.
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(b"caf\xe9").expect("compress");
        let url = serve(
            "Content-Type: text/plain; charset=iso-8859-1\r\nX-Probe: kept\r\nContent-Encoding: gzip\r\n",
            gz.finish().expect("finish gzip"),
        )
        .await;
        let response = buffer(get(&url).await, Limit::CompressionRatio)
            .await
            .expect("a small body is buffered");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.url().as_str(), url);
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        assert_eq!(header("x-probe").as_deref(), Some("kept"));
        assert_eq!(header("content-encoding"), None);
        assert_eq!(header("content-length"), None);
        assert_eq!(response.text().await.expect("decode text"), "café");
    }

    #[tokio::test]
    async fn buffered_response_decodes_json() {
        let url = serve(
            "Content-Type: application/json\r\n",
            br#"{"rows":[1,2,3]}"#.to_vec(),
        )
        .await;
        let value: serde_json::Value = buffer(get(&url).await, Limit::CompressionRatioAndSize(MIB))
            .await
            .expect("a small body is buffered")
            .json()
            .await
            .expect("decode json");
        assert_eq!(value.to_string(), r#"{"rows":[1,2,3]}"#);
    }

    #[tokio::test]
    async fn manual_client_asks_for_compressed_responses() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server address");
        let request = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.expect("read request");
                request.extend_from_slice(&buf[..n]);
            }
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
            String::from_utf8_lossy(&request).to_lowercase()
        });
        let _ = get(&format!("http://{addr}/")).await;
        let request = request.await.expect("test server");
        assert!(
            request.contains("accept-encoding: zstd,gzip,deflate,br\r\n"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn read_prefix_decodes_and_stops_at_the_limit() {
        let url = serve(
            "Content-Encoding: gzip\r\n",
            compressed(Encoding::Gzip, RATIO_CHECK_FLOOR_BYTES * 4, same_byte),
        )
        .await;
        let prefix = read_prefix(get(&url).await, 1024).await;
        assert_eq!(prefix, vec![b'A'; 1024]);

        let url = serve("", b"short error".to_vec()).await;
        assert_eq!(read_prefix(get(&url).await, 1024).await, b"short error");
    }
}
