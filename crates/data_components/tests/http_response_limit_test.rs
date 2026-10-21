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

//! A dynamic HTTP dataset refuses a compressed response that expands like a
//! compression bomb, as soon as it does, and reads every other response — compressed
//! at a realistic ratio or not compressed at all — at any size.
#![expect(
    clippy::expect_used,
    reason = "test helpers fail the test with a message when setup fails"
)]

use std::io::Write;
use std::sync::Arc;

use arrow::array::Array;
use data_components::http::provider::HttpTableProvider;
use datafusion::prelude::*;
use http_body_limit::{DECODE_BLOCK_BYTES, MAX_COMPRESSION_RATIO, RATIO_CHECK_FLOOR_BYTES};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

const MIB: usize = 1024 * 1024;

/// Past the 128 MiB every response was once capped at, so a response this size
/// is read only because no such cap applies.
const LARGE_RESPONSE_BYTES: usize = 136 * MIB;

/// A JSON object `{"a":"…"}` of `len` bytes in total, whose string is `fill(i)` for
/// each of its bytes, built a block at a time so the whole is never held twice.
fn json_object(len: usize, fill: fn(usize) -> u8, mut sink: impl FnMut(&[u8])) {
    let (head, tail) = (br#"{"a":""#, br#""}"#);
    sink(head);
    let mut offset = 0;
    let mut remaining = len - head.len() - tail.len();
    while remaining > 0 {
        let n = remaining.min(MIB);
        let block: Vec<u8> = (offset..offset + n).map(fill).collect();
        sink(&block);
        offset += n;
        remaining -= n;
    }
    sink(tail);
}

fn same_byte(_: usize) -> u8 {
    b'A'
}

/// Zero-padded ascending eight-digit numbers separated by spaces, like an id column:
/// text that compresses a few times over, as real data does.
fn ids(i: usize) -> u8 {
    let (number, position) = (i / 9, i % 9);
    if position == 8 {
        return b' ';
    }
    let place = 10usize.pow(u32::try_from(7 - position).expect("a digit place"));
    b"0123456789"[number / place % 10]
}

fn gzip_json(len: usize, fill: fn(usize) -> u8, level: flate2::Compression) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), level);
    json_object(len, fill, |block| {
        encoder.write_all(block).expect("compress");
    });
    encoder.finish().expect("finish gzip")
}

fn plain_json(len: usize, fill: fn(usize) -> u8) -> Vec<u8> {
    let mut body = Vec::with_capacity(len);
    json_object(len, fill, |block| body.extend_from_slice(block));
    body
}

async fn origin(encoding: Option<&str>, body: Vec<u8>) -> MockServer {
    let server = MockServer::start().await;
    let mut response = ResponseTemplate::new(200).insert_header("content-type", "application/json");
    if let Some(encoding) = encoding {
        response = response.insert_header("content-encoding", encoding);
    }
    Mock::given(method("GET"))
        .respond_with(response.set_body_bytes(body))
        .mount(&server)
        .await;
    server
}

async fn query_content(server: &MockServer) -> datafusion::error::Result<Vec<String>> {
    let provider = HttpTableProvider::new(
        url::Url::parse(&server.uri()).expect("valid URL"),
        // Decoding is left to the provider, as on the connector's own client.
        http_body_limit::decode_manually(reqwest::Client::builder())
            .build()
            .expect("build client"),
        "json".to_string(),
        false,
    );
    let ctx = SessionContext::new();
    ctx.register_table("origin", Arc::new(provider))
        .expect("register the table");
    let batches = ctx
        .sql("SELECT content FROM origin")
        .await?
        .collect()
        .await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            let content = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("content is a string column");
            (0..content.len())
                .map(|i| content.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect())
}

async fn requests(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .expect("request recording is on")
        .len()
}

/// The decoded and received byte counts a compression ratio refusal reports.
fn refused_at(message: &str) -> (usize, usize) {
    let counts = message
        .split_once("when decoded (")
        .and_then(|(_, rest)| rest.split_once(" received)"))
        .map_or_else(
            || panic!("no byte counts in {message}"),
            |(counts, _)| counts,
        );
    let (decoded, received) = counts
        .split_once(" bytes from ")
        .unwrap_or_else(|| panic!("malformed byte counts in {message}"));
    (
        decoded.parse().expect("decoded byte count"),
        received.parse().expect("received byte count"),
    )
}

#[tokio::test]
async fn compression_bomb_is_refused_just_past_the_floor_without_retrying() {
    let len = RATIO_CHECK_FLOOR_BYTES + 64 * MIB;
    let body = gzip_json(len, same_byte, flate2::Compression::default());
    assert!(
        body.len() * MAX_COMPRESSION_RATIO < len,
        "the response expands past the ratio: {} bytes on the wire",
        body.len()
    );
    let server = origin(Some("gzip"), body).await;

    let error = match query_content(&server).await {
        Err(error) => error,
        Ok(rows) => panic!(
            "a compression bomb fails the query, but it returned rows of {:?} bytes",
            rows.iter().map(String::len).collect::<Vec<_>>()
        ),
    };
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "Failed to read the response from {} for this dataset: the gzip-compressed \
             response body expanded more than 100:1 when decoded (",
            server.uri()
        )) && message.contains(
            "received), which is refused once a body passes 64 MiB decoded. Ask the endpoint \
             for an uncompressed response by adding `Accept-Encoding: identity` to \
             `http_headers`, or request less data per call. \
             See: https://spiceai.org/docs/components/data-connectors/https"
        ),
        "{message}"
    );

    // Refused as soon as it passed the floor: never more than one decode block past it.
    let (decoded, received) = refused_at(&message);
    assert!(
        decoded > RATIO_CHECK_FLOOR_BYTES
            && decoded <= RATIO_CHECK_FLOOR_BYTES + DECODE_BLOCK_BYTES,
        "refused at {decoded} decoded bytes"
    );
    assert!(decoded > received * MAX_COMPRESSION_RATIO);

    // The refusal is permanent: the origin is asked once, not once per retry.
    assert_eq!(requests(&server).await, 1);
}

#[tokio::test]
async fn large_compressed_response_at_a_realistic_ratio_is_read_in_full() {
    // The fastest level keeps the test quick, and compresses the least.
    let body = gzip_json(LARGE_RESPONSE_BYTES, ids, flate2::Compression::fast());
    let ratio = LARGE_RESPONSE_BYTES / body.len();
    assert!(
        (2..MAX_COMPRESSION_RATIO).contains(&ratio),
        "the response compresses {ratio}:1"
    );
    let server = origin(Some("gzip"), body).await;

    let content = query_content(&server)
        .await
        .expect("a large compressed response at a realistic ratio is read");
    assert_eq!(content.len(), 1);
    let row = &content[0];
    assert_eq!(row.len(), LARGE_RESPONSE_BYTES);
    assert_eq!(row.as_bytes(), plain_json(LARGE_RESPONSE_BYTES, ids));
    assert_eq!(requests(&server).await, 1);
}

#[tokio::test]
async fn large_uncompressed_response_is_read_in_full() {
    let server = origin(None, plain_json(LARGE_RESPONSE_BYTES, ids)).await;

    let content = query_content(&server)
        .await
        .expect("a large uncompressed response is read");
    assert_eq!(content.len(), 1);
    let row = &content[0];
    assert_eq!(row.len(), LARGE_RESPONSE_BYTES);
    assert_eq!(row.as_bytes(), plain_json(LARGE_RESPONSE_BYTES, ids));
}

#[tokio::test]
async fn brotli_response_is_decoded() {
    let json = br#"{"id":1,"name":"brotli"}"#;
    let mut body = Vec::new();
    {
        let mut encoder = brotli::CompressorWriter::new(&mut body, 4096, 11, 22);
        encoder.write_all(json).expect("compress");
    }
    let server = origin(Some("br"), body).await;

    let content = query_content(&server)
        .await
        .expect("a brotli response is read");
    assert_eq!(content, vec![String::from_utf8_lossy(json).into_owned()]);
}
