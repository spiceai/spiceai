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

//! Object-store list responses are XML from the server the caller named.
//!
//! `object_store` 0.13 deserializes those bodies with `quick-xml` 0.39, whose
//! serde map walks every attribute of a start tag and compares each new name
//! with every name already seen. A single start tag with many distinct
//! attributes is therefore quadratic CPU after the body has already been
//! read, so the HTTP client's request timeout does not bound it.
//!
//! These tests drive the public list entry points Spice uses for S3, GCS,
//! Azure, and HTTP (URL tables and `http`/`https` datasets). Each response is
//! a normal empty-enough listing plus one flooded start tag, and the list
//! must still return `marker.txt` within a bound a linear scan meets.

#![expect(
    clippy::expect_used,
    reason = "setup failures in this test should panic with the reason the store or socket could not be built"
)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::{ClientOptions, ObjectStore, RetryConfig};

/// Distinct attribute names on the one start tag. At `64_000`, quick-xml 0.39's
/// linear duplicate scan takes multiple seconds in a release build of the
/// parser; the hashed scan in quick-xml 0.41 finishes in well under 100ms.
const ATTRIBUTE_COUNT: usize = 64_000;

const MARKER: &str = "marker.txt";

/// Above the fixed parser and below the vulnerable one on this class of host.
const LIST_LIMIT: Duration = Duration::from_secs(1);

const S3_LIST_BODY_INNER: &str = "\
<Contents>\
<Key>marker.txt</Key>\
<LastModified>2020-01-01T00:00:00Z</LastModified>\
<Size>4</Size>\
</Contents>";

const AZURE_LIST_BODY_INNER: &str = "\
<Blobs>\
<Blob>\
<Name>marker.txt</Name>\
<Properties>\
<Last-Modified>Wed, 01 Jan 2020 00:00:00 GMT</Last-Modified>\
<Content-Length>4</Content-Length>\
<Content-Type>text/plain</Content-Type>\
</Properties>\
</Blob>\
</Blobs>";

const HTTP_LIST_BODY_INNER: &str = "\
<response>\
<href>/marker.txt</href>\
<propstat>\
<prop>\
<getlastmodified>Wed, 01 Jan 2020 00:00:00 GMT</getlastmodified>\
<getcontentlength>4</getcontentlength>\
<resourcetype></resourcetype>\
<getetag>\"abc\"</getetag>\
</prop>\
<status>HTTP/1.1 200 OK</status>\
</propstat>\
</response>";

fn no_retries() -> RetryConfig {
    RetryConfig {
        max_retries: 0,
        ..RetryConfig::default()
    }
}

fn http_client_options() -> ClientOptions {
    ClientOptions::new().with_allow_http(true)
}

/// `<open` + ` aN="1"` * [`ATTRIBUTE_COUNT`] + `>` + `inner` + `close`.
fn flooded_element(open: &str, inner: &str, close: &str) -> Vec<u8> {
    let mut body =
        Vec::with_capacity(open.len() + inner.len() + close.len() + ATTRIBUTE_COUNT * 16);
    body.extend_from_slice(open.as_bytes());
    for index in 0..ATTRIBUTE_COUNT {
        body.extend_from_slice(b" a");
        body.extend_from_slice(index.to_string().as_bytes());
        body.extend_from_slice(b"=\"1\"");
    }
    body.push(b'>');
    body.extend_from_slice(inner.as_bytes());
    body.extend_from_slice(close.as_bytes());
    body
}

struct XmlServer {
    port: u16,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl XmlServer {
    fn spawn(body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the list-response server");
        listener
            .set_nonblocking(true)
            .expect("set the list-response server nonblocking");
        let port = listener
            .local_addr()
            .expect("read the list-response server port")
            .port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let join = thread::spawn(move || serve(&listener, &body, &stop_flag));
        Self {
            port,
            stop,
            join: Some(join),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for XmlServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn serve(listener: &TcpListener, body: &[u8], stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => write_response(stream, body),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
}

fn write_response(mut stream: TcpStream, body: &[u8]) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut buf = [0_u8; 1024];
    let mut seen = Vec::new();
    loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                seen.extend_from_slice(&buf[..count]);
                if seen.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

async fn assert_marker_within_limit(store: &dyn ObjectStore) {
    let started = Instant::now();
    let listed = store.list(None).try_collect::<Vec<_>>().await;
    let elapsed = started.elapsed();
    let objects = listed.unwrap_or_else(|error| {
        panic!("list failed after {elapsed:?} before yielding objects: {error}")
    });
    let locations: Vec<String> = objects
        .iter()
        .map(|meta| meta.location.to_string())
        .collect();
    assert!(
        locations.iter().any(|location| location == MARKER),
        "list did not yield {MARKER}, so the attribute scan was not the parse of a real listing: {locations:?}"
    );
    assert!(
        elapsed < LIST_LIMIT,
        "listing a start tag with {ATTRIBUTE_COUNT} distinct attributes took {elapsed:?}; the duplicate-attribute scan must finish within {LIST_LIMIT:?} after the body is already in memory"
    );
}

#[tokio::test]
async fn s3_list_of_many_attributes_stays_bounded() {
    let server = XmlServer::spawn(flooded_element(
        "<ListBucketResult",
        S3_LIST_BODY_INNER,
        "</ListBucketResult>",
    ));
    let store = AmazonS3Builder::new()
        .with_bucket_name("bucket")
        .with_region("us-east-1")
        .with_endpoint(server.endpoint())
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .with_skip_signature(true)
        .with_retry(no_retries())
        .build()
        .expect("build the S3 store pointed at the local list server");
    assert_marker_within_limit(&store).await;
}

#[tokio::test]
async fn gcs_list_of_many_attributes_stays_bounded() {
    let server = XmlServer::spawn(flooded_element(
        "<ListBucketResult",
        S3_LIST_BODY_INNER,
        "</ListBucketResult>",
    ));
    let store = GoogleCloudStorageBuilder::new()
        .with_bucket_name("bucket")
        .with_base_url(&server.endpoint())
        .with_skip_signature(true)
        .with_client_options(http_client_options())
        .with_retry(no_retries())
        .build()
        .expect("build the GCS store pointed at the local list server");
    assert_marker_within_limit(&store).await;
}

#[tokio::test]
async fn azure_list_of_many_attributes_stays_bounded() {
    let server = XmlServer::spawn(flooded_element(
        "<EnumerationResults",
        AZURE_LIST_BODY_INNER,
        "</EnumerationResults>",
    ));
    let store = MicrosoftAzureBuilder::new()
        .with_account("account")
        .with_container_name("container")
        .with_endpoint(server.endpoint())
        .with_allow_http(true)
        .with_skip_signature(true)
        .with_retry(no_retries())
        .build()
        .expect("build the Azure store pointed at the local list server");
    assert_marker_within_limit(&store).await;
}

#[tokio::test]
async fn http_propfind_of_many_attributes_stays_bounded() {
    let server = XmlServer::spawn(flooded_element(
        "<multistatus",
        HTTP_LIST_BODY_INNER,
        "</multistatus>",
    ));
    let store = HttpBuilder::new()
        .with_url(format!("{}/", server.endpoint()))
        .with_client_options(http_client_options())
        .with_retry(no_retries())
        .build()
        .expect("build the HTTP store pointed at the local PROPFIND server");
    assert_marker_within_limit(&store).await;
}
