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

//! Bounded reads of remote data: HTTP response bodies, and data read whole through
//! [`std::io::Read`], such as a decompressed file or archive entry.
//!
//! - **Every read is bounded by what it holds.** A [`Limit`] says how much a read may hold once decoded: a
//!   share of the query memory pool for a body that carries data ([`Limit::Reserve`]),
//!   or a fixed cap for a token, schema or error response ([`Limit::Size`]). A body the
//!   pool cannot hold is refused instead of exhausting the process, so a response is
//!   held to `runtime.query.memory_limit` like any other buffer a query holds.
//! - **A compressed HTTP body is also bounded by how far it expands.** It costs the
//!   sender far less than it costs the reader: a few hundred kilobytes of gzip decode to hundreds of megabytes,
//!   and brotli or zstd to gigabytes. Once a compressed input has decoded past
//!   [`RATIO_CHECK_FLOOR_BYTES`], it is refused as soon as it has expanded more than
//!   [`MAX_COMPRESSION_RATIO`] times the bytes received for it. The
//!   check runs as each block is decoded, so a refused input holds at most one block more
//!   than the larger of the floor and that ratio times the bytes received. The ratio is
//!   over the whole input, so bytes sent early that compress poorly delay the refusal of
//!   a run that compresses well: a sender gets past the floor only by sending one byte
//!   for every [`MAX_COMPRESSION_RATIO`] it makes spiced hold.
//!
//! Counting the bytes received for an HTTP body needs the body before it is decoded,
//! which `reqwest`'s transparent decompression does not expose. A client whose bodies are
//! checked is therefore built with [`decode_manually`], and *every* body read from it
//! must go through [`buffer`], [`read_bytes`] or [`read_prefix`]: they decode with the
//! same decoders `reqwest` uses, while counting both sides.
//!
//! On a client that still decompresses transparently, `reqwest` has already removed the
//! `Content-Encoding` header from a body it decoded, so these functions read that body
//! as an uncompressed one: still held to its [`Limit`], but without the ratio check.

mod expansion;

pub use expansion::{Limit, read_to_end};

use std::io;
use std::pin::Pin;

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder, ZstdDecoder};
use bytes::{Bytes, BytesMut};
use datafusion_common::DataFusionError;
use futures::{StreamExt, TryStreamExt};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, HeaderMap, HeaderValue};
use reqwest::{ClientBuilder, Response, ResponseBuilderExt};
use snafu::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

use expansion::{Expansion, Held};

/// How much a compressed input may decode to before its compression ratio is checked.
///
/// Below it, any ratio is accepted: a small response that compresses extremely well
/// (a page of identical rows) is ordinary, and costs little however small it was on
/// the wire. An input refused for its ratio holds no more than one block past the larger
/// of this and [`MAX_COMPRESSION_RATIO`] times the bytes received for it.
pub const RATIO_CHECK_FLOOR_BYTES: usize = 64 * 1024 * 1024;

/// The most a compressed input may expand, decoded bytes per byte received, once it has
/// decoded past [`RATIO_CHECK_FLOOR_BYTES`].
///
/// Realistic JSON responses compress far less: TPC-H-shaped rows as a JSON array or
/// NDJSON 6–13:1, Elasticsearch search hits 8–16:1 and column-oriented JSON 7–21:1,
/// across gzip 1–9, deflate, brotli 5–11 and zstd 3–19. A compression bomb sits at the
/// codec's limit: 1028:1 for gzip and deflate, and 32,000:1 to 645,000:1 for zstd and
/// brotli.
pub const MAX_COMPRESSION_RATIO: usize = 100;

/// The most a metadata or token response is allowed to hold once decoded, compressed
/// or not, on the paths that cap size because they never carry bulk data.
///
/// It matches the 128 MiB the runtime accepts as a request body on its own HTTP API.
pub const MAX_METADATA_BODY_BYTES: usize = 128 * 1024 * 1024;

/// The `Accept-Encoding` a client built with [`decode_manually`] sends, the same one
/// `reqwest` sends when it decompresses transparently.
const ACCEPT_ENCODING_VALUE: &str = "zstd,gzip,deflate,br";

/// The size of each block decoded at a time, and so how far past a limit a read can
/// get before the limit is enforced.
pub const DECODE_BLOCK_BYTES: usize = 64 * 1024;

#[derive(Debug, Snafu)]
pub enum Error {
    /// The compressed input expanded past [`MAX_COMPRESSION_RATIO`] once past
    /// [`RATIO_CHECK_FLOOR_BYTES`]. Asking again gets the same answer.
    #[snafu(display(
        "the {codec}-compressed response body expanded more than {MAX_COMPRESSION_RATIO}:1 \
         when decoded ({decoded} bytes from {received} received), which is refused once a \
         body passes {} decoded",
        format_bytes(RATIO_CHECK_FLOOR_BYTES)
    ))]
    CompressionRatio {
        codec: &'static str,
        decoded: usize,
        received: usize,
    },

    /// The decoded body would pass a [`Limit::Size`] cap. Asking again gets the same
    /// answer.
    #[snafu(display(
        "the response body is larger than the {} that is read from one HTTP response",
        format_bytes(*limit)
    ))]
    TooLarge { limit: usize },

    /// The query memory pool could not hold the decoded data ([`Limit::Reserve`]).
    #[snafu(display(
        "holding the decoded data would pass the query memory limit \
         (`runtime.query.memory_limit`): {source}"
    ))]
    MemoryLimit { source: DataFusionError },

    /// Reading the body failed: a connection reset, a timeout or a truncated body.
    #[snafu(display("{source}"))]
    Read { source: reqwest::Error },

    /// The body does not decode as the encoding it was sent with.
    #[snafu(display("Failed to decode the {codec}-compressed response body: {source}"))]
    Decode {
        codec: &'static str,
        source: io::Error,
    },

    /// Reading data through [`read_to_end`] failed, including a decoder in it failing to
    /// decode.
    #[snafu(display("{source}"))]
    Io { source: io::Error },
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

    /// The `Content-Encoding` token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
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
/// An uncompressed body that declares its length is charged that length before any of it
/// is read, so a body larger than `limit` allows is refused before it is received, and is
/// read into one allocation of exactly its size. Any other body is read a block at a
/// time, each block charged before it is allocated, and joined once at the end into an
/// allocation of exactly its size: a refused body holds no more than the blocks it read,
/// and the join briefly holds the body twice, which is charged too. A compressed body is
/// also held to the compression ratio check.
///
/// # Errors
///
/// [`Error::CompressionRatio`], [`Error::TooLarge`] or [`Error::MemoryLimit`] when the
/// body breaks `limit` or the ratio, [`Error::Read`] when reading it fails, and
/// [`Error::Decode`] when it does not decode.
pub async fn read_bytes(response: Response, limit: Limit<'_>) -> Result<Bytes> {
    let encoding = Encoding::of(response.headers());
    let declared = match encoding {
        None => response
            .content_length()
            .and_then(|len| usize::try_from(len).ok()),
        Some(_) => None,
    };
    let (mut reader, expansion) = reader(response, encoding);
    let mut held = Held::new(limit);

    if let Some(declared) = declared {
        held.check_size(declared)?;
        held.grow(declared)?;
        let mut body = BytesMut::with_capacity(declared);
        // The HTTP client ends the body at its declared length, and fails a read of one
        // that ends early.
        while body.len() < declared {
            let n = reader
                .read_buf(&mut body)
                .await
                .map_err(|error| read_failure(error, encoding))?;
            if n == 0 {
                break;
            }
        }
        held.shrink(declared - body.len());
        held.keep();
        return Ok(body.freeze());
    }

    let mut blocks: Vec<Bytes> = Vec::new();
    held.grow(DECODE_BLOCK_BYTES)?;
    let mut block = BytesMut::with_capacity(DECODE_BLOCK_BYTES);
    let mut decoded = 0usize;
    loop {
        if block.len() == block.capacity() {
            blocks.push(block.split().freeze());
            held.grow(DECODE_BLOCK_BYTES)?;
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
        held.check_size(decoded)?;
        if let Some(expansion) = &expansion {
            expansion.decoded(n)?;
        }
    }

    // Charged at block capacity while reading, and held at exactly the body's size once
    // it is joined: the join is charged before it allocates, and the blocks are released
    // after it.
    let charged_for_blocks = (blocks.len() + 1) * DECODE_BLOCK_BYTES;
    let body = if blocks.is_empty() {
        held.shrink(charged_for_blocks - block.len());
        let body = Bytes::copy_from_slice(&block);
        drop(block);
        body
    } else {
        held.grow(decoded)?;
        let mut body = BytesMut::with_capacity(decoded);
        for part in blocks.drain(..) {
            body.extend_from_slice(&part);
        }
        body.extend_from_slice(&block);
        drop(block);
        held.shrink(charged_for_blocks);
        body.freeze()
    };
    held.keep();
    Ok(body)
}

/// Read the body of `response` under `limit` (see [`read_bytes`]) and hand back a
/// response that replays it decoded, with the same status, version, headers and URL.
///
/// The returned response's `text()` and `json()` behave as on a client that
/// decompresses transparently — the same charset handling and the same errors. Like
/// that client, a decoded response loses its `Content-Encoding` and `Content-Length`.
///
/// # Errors
///
/// As [`read_bytes`].
pub async fn buffer(response: Response, limit: Limit<'_>) -> Result<Response> {
    let encoding = Encoding::of(response.headers());
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
    let encoding = Encoding::of(response.headers());
    let (reader, _) = reader(response, encoding);
    let mut out = Vec::new();
    // `read_to_end` keeps what it read before a failure, which is all this wants.
    let _ = reader
        .take(u64::try_from(max_bytes).unwrap_or(u64::MAX))
        .read_to_end(&mut out)
        .await;
    out
}

type Reader = Pin<Box<dyn AsyncRead + Send>>;

/// A reader of the body of `response`, decoded as `encoding` if it has one, and the
/// ratio check that counts the body bytes it has been handed when it does.
fn reader(response: Response, encoding: Option<Encoding>) -> (Reader, Option<Expansion>) {
    let expansion = encoding.map(|encoding| Expansion::new(encoding.as_str()));
    let counter = expansion.clone();
    let chunks = futures::stream::try_unfold(response, |mut response| async move {
        let chunk: Option<Bytes> = response.chunk().await?;
        Ok::<_, reqwest::Error>(chunk.map(|chunk| (chunk, response)))
    })
    .map_ok(move |chunk: Bytes| {
        if let Some(counter) = &counter {
            counter.received(chunk.len());
        }
        chunk
    })
    .map_err(io::Error::other)
    .boxed();
    let body = StreamReader::new(chunks);
    let reader: Reader = match encoding {
        None => Box::pin(body),
        Some(Encoding::Gzip) => Box::pin(GzipDecoder::new(body)),
        Some(Encoding::Deflate) => Box::pin(ZlibDecoder::new(body)),
        Some(Encoding::Brotli) => Box::pin(BrotliDecoder::new(body)),
        Some(Encoding::Zstd) => Box::pin(ZstdDecoder::new(body)),
    };
    (reader, expansion)
}

/// A failed read from the body: either reading the body failed, which is passed
/// through, or a compressed body does not decode.
fn read_failure(error: io::Error, encoding: Option<Encoding>) -> Error {
    match (error.downcast::<reqwest::Error>(), encoding) {
        (Ok(source), _) => Error::Read { source },
        (Err(source), Some(encoding)) => Error::Decode {
            codec: encoding.as_str(),
            source,
        },
        (Err(source), None) => Error::Io { source },
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
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
    };
    use std::io::Write;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    const MIB: usize = 1024 * 1024;

    /// A reservation on a pool of `bytes`, or on an unbounded pool.
    fn reservation(bytes: Option<usize>) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = match bytes {
            Some(bytes) => Arc::new(GreedyMemoryPool::new(bytes)),
            None => Arc::new(UnboundedMemoryPool::default()),
        };
        MemoryConsumer::new("test body").register(&pool)
    }

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

        let held = reservation(None);
        let error = read_bytes(get(&url).await, Limit::Reserve(&held))
            .await
            .expect_err("a body that expands past the ratio is refused");
        let Error::CompressionRatio {
            codec: refused,
            decoded,
            received,
        } = error
        else {
            panic!("expected a compression ratio error, got {error:?}");
        };
        assert_eq!(refused, encoding.as_str());
        assert_eq!(held.size(), 0, "a refused body returns what it charged");
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

        let held = reservation(None);
        let decoded = read_bytes(get(&url).await, Limit::Reserve(&held))
            .await
            .expect("a realistic ratio is read in full");
        assert_eq!(decoded.len(), len);
        assert_eq!(held.size(), len, "the body is charged at exactly its size");
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
        let held = reservation(None);
        let decoded = read_bytes(get(&url).await, Limit::Reserve(&held))
            .await
            .expect("a small body is read at any ratio");
        assert_eq!(decoded.len(), 8 * MIB);
        assert!(decoded.iter().all(|&b| b == b'A'));
    }

    /// How many bytes at the start of [`padded_bomb`] are incompressible.
    const PADDING: usize = MIB;

    /// Byte `i` of a body that compresses poorly for its first [`PADDING`] bytes and
    /// extremely well after them.
    fn padded_bomb(i: usize) -> u8 {
        if i >= PADDING {
            return b'A';
        }
        // SplitMix64 of the offset: incompressible, and the same on every run.
        let mut z = (i as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)).to_le_bytes()[0]
    }

    #[tokio::test]
    async fn bytes_received_early_delay_the_refusal_only_by_the_ratio() {
        let len = 5 * RATIO_CHECK_FLOOR_BYTES;
        let body = compressed(Encoding::Gzip, len, padded_bomb);
        assert!(
            body.len() * MAX_COMPRESSION_RATIO < len,
            "the body expands past the ratio overall: {} bytes on the wire",
            body.len()
        );
        let url = serve("Content-Encoding: gzip\r\n", body).await;

        let held = reservation(None);
        let error = read_bytes(get(&url).await, Limit::Reserve(&held))
            .await
            .expect_err("a body that expands past the ratio is refused");
        let Error::CompressionRatio {
            decoded, received, ..
        } = error
        else {
            panic!("expected a compression ratio error, got {error:?}");
        };
        // Incompressible padding is received byte for byte, so it carries the body past the
        // floor to at least the ratio times the padding, but never past the ratio times what
        // was received by more than the block that crossed it. Where between those the
        // refusal lands depends on the zlib the encoder links and on how much each socket
        // read delivers, so the test bounds only these.
        const { assert!(PADDING * MAX_COMPRESSION_RATIO > RATIO_CHECK_FLOOR_BYTES) };
        assert!(
            decoded > PADDING * MAX_COMPRESSION_RATIO,
            "refused at {decoded} bytes from {received} received"
        );
        assert!(decoded > received * MAX_COMPRESSION_RATIO);
        assert!(
            decoded <= received * MAX_COMPRESSION_RATIO + DECODE_BLOCK_BYTES,
            "refused at {decoded} bytes from {received} received"
        );
    }

    #[tokio::test]
    async fn size_cap_applies_to_compressed_and_uncompressed_bodies() {
        let url = serve("", vec![b'A'; 2 * MIB]).await;
        let body = read_bytes(get(&url).await, Limit::Size(2 * MIB))
            .await
            .expect("a body of exactly the cap is read");
        assert_eq!(body.len(), 2 * MIB);
        let error = read_bytes(get(&url).await, Limit::Size(2 * MIB - 1))
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
        let body = read_bytes(get(&url).await, Limit::Size(2 * MIB))
            .await
            .expect("a compressed body of exactly the cap is read");
        assert_eq!(body.len(), 2 * MIB);
        let error = read_bytes(get(&url).await, Limit::Size(2 * MIB - 1))
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
        let error = read_bytes(get(&url).await, Limit::METADATA)
            .await
            .expect_err("a body that does not decode is refused");
        assert!(
            matches!(error, Error::Decode { codec: "gzip", .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn unknown_encoding_is_read_as_it_is() {
        let url = serve("Content-Encoding: x-custom\r\n", b"raw bytes".to_vec()).await;
        let body = read_bytes(get(&url).await, Limit::METADATA)
            .await
            .expect("an encoding reqwest does not decode is passed through");
        assert_eq!(&body[..], b"raw bytes");
    }

    #[tokio::test]
    async fn empty_bodies_read_as_empty() {
        let url = serve("", Vec::new()).await;
        let body = read_bytes(get(&url).await, Limit::Size(0))
            .await
            .expect("an empty body fits any cap");
        assert!(body.is_empty());

        let url = serve(
            "Content-Encoding: gzip\r\n",
            compressed(Encoding::Gzip, 0, same_byte),
        )
        .await;
        let body = read_bytes(get(&url).await, Limit::METADATA)
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
        let response = buffer(get(&url).await, Limit::METADATA)
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

    #[tokio::test]
    async fn reserved_body_is_charged_and_refused_past_the_pool() {
        let url = serve("", vec![b'A'; 4 * MIB]).await;

        let held = reservation(Some(8 * MIB));
        let body = read_bytes(get(&url).await, Limit::Reserve(&held))
            .await
            .expect("a body the pool can hold is read");
        assert_eq!(body.len(), 4 * MIB);
        assert_eq!(held.size(), 4 * MIB, "the caller holds the body's charge");
        drop(body);
        held.free();

        // Refused on its declared length, before any of it is read.
        let held = reservation(Some(2 * MIB));
        let error = read_bytes(get(&url).await, Limit::Reserve(&held))
            .await
            .expect_err("a body larger than the pool is refused");
        assert!(matches!(error, Error::MemoryLimit { .. }), "{error:?}");
        assert!(
            error
                .to_string()
                .starts_with("holding the decoded data would pass the query memory limit (`runtime.query.memory_limit`): Resources exhausted"),
            "{error}"
        );
        assert_eq!(held.size(), 0, "a refused body returns what it charged");
    }

    #[tokio::test]
    async fn compressed_body_is_charged_by_its_decoded_size() {
        let url = serve(
            "Content-Encoding: gzip\r\n",
            compressed(Encoding::Gzip, 8 * MIB, same_byte),
        )
        .await;
        let held = reservation(Some(4 * MIB));
        let error = read_bytes(get(&url).await, Limit::Reserve(&held))
            .await
            .expect_err("a body that decodes past the pool is refused");
        assert!(matches!(error, Error::MemoryLimit { .. }), "{error:?}");
        assert_eq!(held.size(), 0);
    }

    #[test]
    fn sync_read_is_held_to_the_pool_and_the_size_cap() {
        let data = vec![b'x'; 3 * MIB];
        let held = reservation(Some(2 * MIB));
        let error = read_to_end(data.as_slice(), Limit::Reserve(&held))
            .expect_err("more than the pool holds is refused");
        assert!(matches!(error, Error::MemoryLimit { .. }), "{error:?}");
        assert_eq!(held.size(), 0);

        let error = read_to_end(data.as_slice(), Limit::Size(3 * MIB - 1))
            .expect_err("one byte over the cap is refused");
        assert!(
            matches!(error, Error::TooLarge { limit } if limit == 3 * MIB - 1),
            "{error:?}"
        );
        let read = read_to_end(data.as_slice(), Limit::Size(3 * MIB)).expect("the cap itself fits");
        assert_eq!(read, data);

        let empty = reservation(Some(DECODE_BLOCK_BYTES));
        assert!(
            read_to_end(&b""[..], Limit::Reserve(&empty))
                .expect("empty input")
                .is_empty()
        );
        assert_eq!(empty.size(), 0);
    }
}
