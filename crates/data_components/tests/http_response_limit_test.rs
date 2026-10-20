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

//! A dynamic HTTP dataset reads the body its origin sends under a cap on the
//! *decoded* size, so a small compressed response cannot expand into an
//! unbounded allocation.

use std::io::Write;
use std::sync::Arc;

use data_components::http::provider::HttpTableProvider;
use datafusion::prelude::*;
use http_body_limit::MAX_RESPONSE_BODY_BYTES;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

const MIB: usize = 1024 * 1024;

/// A JSON object `{"a":"AAAA…"}` of `len` bytes in total, gzip-compressed a block at a
/// time so the plain text is never held whole.
fn gzip_json(len: usize) -> Vec<u8> {
    let (head, tail) = (br#"{"a":""#, br#""}"#);
    let block = vec![b'A'; MIB];
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    encoder.write_all(head).expect("compress");
    let mut remaining = len - head.len() - tail.len();
    while remaining > 0 {
        let n = remaining.min(block.len());
        encoder.write_all(&block[..n]).expect("compress");
        remaining -= n;
    }
    encoder.write_all(tail).expect("compress");
    encoder.finish().expect("finish gzip")
}

async fn origin(encoding: &str, body: Vec<u8>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .insert_header("content-encoding", encoding)
                .set_body_bytes(body),
        )
        .mount(&server)
        .await;
    server
}

async fn query_content(server: &MockServer) -> datafusion::error::Result<Vec<String>> {
    let provider = HttpTableProvider::new(
        url::Url::parse(&server.uri()).expect("valid URL"),
        // Transparent decompression is on, as on the connector's own client.
        reqwest::Client::new(),
        "json".to_string(),
        false,
    );
    let ctx = SessionContext::new();
    ctx.register_table("origin", Arc::new(provider))
        .expect("register the table");
    let batches = ctx.sql("SELECT content FROM origin").await?.collect().await?;
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

#[tokio::test]
async fn compressed_response_past_the_limit_fails_the_query_without_retrying() {
    let body = gzip_json(MAX_RESPONSE_BODY_BYTES + MIB);
    assert!(body.len() < MIB, "the response is small on the wire");
    let server = origin("gzip", body).await;

    let error = query_content(&server)
        .await
        .expect_err("a response that decodes past the limit fails the query");
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "Failed to read the response from {} for this dataset: the response body is \
             larger than the 128 MiB that is read from one HTTP response.",
            server.uri()
        )),
        "{message}"
    );

    // Too large is permanent: the origin is asked once, not once per retry.
    let requests = server
        .received_requests()
        .await
        .expect("request recording is on");
    assert_eq!(requests.len(), 1, "requests made: {}", requests.len());
}

#[tokio::test]
async fn compressed_response_under_the_limit_is_decoded() {
    let body = gzip_json(3 * MIB);
    let server = origin("gzip", body).await;

    let content = query_content(&server)
        .await
        .expect("a compressed response under the limit is read");
    assert_eq!(content.len(), 1);
    let row = &content[0];
    assert_eq!(row.len(), 3 * MIB);
    assert!(row.starts_with(r#"{"a":"AAAA"#) && row.ends_with(r#"AAAA"}"#));
}

#[tokio::test]
async fn brotli_response_is_decoded() {
    let json = br#"{"id":1,"name":"brotli"}"#;
    let mut body = Vec::new();
    {
        let mut encoder = brotli::CompressorWriter::new(&mut body, 4096, 11, 22);
        encoder.write_all(json).expect("compress");
    }
    let server = origin("br", body).await;

    let content = query_content(&server)
        .await
        .expect("a brotli response is read");
    assert_eq!(content, vec![String::from_utf8_lossy(json).into_owned()]);
}
