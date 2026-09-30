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

//! Exercises dependency logging with Iceberg's configured S3 read path and a
//! real TCP peer that closes the first request before sending response headers.

#[path = "../src/tracing/dependency_log.rs"]
mod dependency_log;

use iceberg::io::{
    FileIOBuilder, S3_ALLOW_ANONYMOUS, S3_DISABLE_CONFIG_LOAD, S3_ENDPOINT, S3_PATH_STYLE_ACCESS,
    S3_REGION,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::Instrument as _;
use tracing_subscriber::{EnvFilter, fmt::MakeWriter, prelude::*};

#[derive(Clone, Default)]
struct Writer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("writer lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Writer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn read_after_connection_closes() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("S3 request arrives")
                .expect("accept S3 request");
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let byte = tokio::time::timeout(Duration::from_secs(5), stream.read_u8())
                    .await
                    .expect("request headers arrive")
                    .expect("read request byte");
                request.push(byte);
                assert!(request.len() < 16_384, "request headers are bounded");
            }
            assert!(
                request.starts_with(b"GET /probe/team_app/task_history/metadata/test-m0.avro ")
            );
            if attempt == 1 {
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ntest",
                    )
                    .await
                    .expect("write complete response");
            }
            stream.shutdown().await.expect("close connection");
        }
    });

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }))
    .with_props([
        (S3_ENDPOINT, endpoint),
        (S3_REGION, "us-east-1".to_string()),
        (S3_PATH_STYLE_ACCESS, "true".to_string()),
        (S3_ALLOW_ANONYMOUS, "true".to_string()),
        (S3_DISABLE_CONFIG_LOAD, "true".to_string()),
    ])
    .build();
    let file = file_io
        .new_input("s3://probe/team_app/task_history/metadata/test-m0.avro")
        .expect("Iceberg input file");
    let bytes = tokio::time::timeout(
        Duration::from_secs(10),
        file.read().instrument(tracing::info_span!(
            target: "runtime",
            "sql_query",
            dataset = "history"
        )),
    )
    .await
    .expect("read completes within its retry budget")
    .expect("read recovers on retry");
    assert_eq!(bytes.as_ref(), b"test");
    server.await.expect("two HTTP requests completed");
}

#[tokio::test]
async fn iceberg_retry_uses_friendly_warning_and_preserves_debug_diagnostics() {
    let writer = Writer::default();
    let (filter, reload) =
        tracing_subscriber::reload::Layer::new(EnvFilter::new("runtime=info,warn"));
    let subscriber = tracing_subscriber::registry().with(filter).with(
        tracing_subscriber::fmt::layer()
            .without_time()
            .with_ansi(false)
            .with_writer(writer.clone()),
    );
    tracing::subscriber::set_global_default(subscriber).expect("global subscriber");
    dependency_log::init().expect("Spice dependency logger");

    for debug in [false, true] {
        if debug {
            reload
                .reload(EnvFilter::new(
                    "runtime=info,warn,opendal::layers::retry=debug",
                ))
                .expect("enable dependency diagnostics");
        }
        writer.0.lock().expect("writer lock").clear();
        read_after_connection_closes().await;
        let output = String::from_utf8(writer.0.lock().expect("writer lock").clone())
            .expect("UTF-8 log output");
        assert_eq!(output.matches("WARN").count(), 1, "{output}");
        assert!(
            output.contains("Reading file 'http://127.0.0.1:"),
            "{output}"
        );
        assert!(output.contains("/probe/team_app/task_history/metadata/test-m0.avro' (S3)"));
        assert!(output.contains("Spice will retry in 1s (attempt 1)"));
        assert!(output.contains("dataset=\"history\""), "{output}");
        assert_eq!(output.contains("SendRequest"), debug, "{output}");
        assert_eq!(
            output.lines().count(),
            if debug { 2 } else { 1 },
            "{output}"
        );
    }
}
