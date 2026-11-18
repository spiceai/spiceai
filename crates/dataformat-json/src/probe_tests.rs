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

//! Schema and key probes over files read from an object store, for both the
//! local-file payload and the streamed payload a remote store returns.

use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::execution::context::SessionContext;
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreExt, local::LocalFileSystem, memory::InMemory, path::Path,
};

use crate::{Format, JsonKeyProbe, JsonSchemaProbe, SpiceJsonFormat};

#[derive(Clone, Copy, Debug)]
enum Payload {
    File,
    Stream,
}

/// A store holding `body` as its only object, and that object's metadata.
/// The tempdir backs the local-file store and must outlive the probe.
async fn store_with(
    body: &[u8],
    payload: Payload,
) -> (Arc<dyn ObjectStore>, ObjectMeta, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("create fixture directory");
    let store: Arc<dyn ObjectStore> = match payload {
        Payload::File => Arc::new(
            LocalFileSystem::new_with_prefix(directory.path()).expect("local store over tempdir"),
        ),
        Payload::Stream => Arc::new(InMemory::new()),
    };
    let location = Path::from("records.json");
    store
        .put(&location, body.to_vec().into())
        .await
        .expect("write fixture");
    let meta = store.head(&location).await.expect("fixture metadata");
    (store, meta, directory)
}

fn keys(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(ToString::to_string).collect()
}

async fn probe_keys(
    format: &SpiceJsonFormat,
    body: &[u8],
    payload: Payload,
    until_found: Option<&BTreeSet<String>>,
) -> datafusion::error::Result<JsonKeyProbe> {
    let (store, meta, _directory) = store_with(body, payload).await;
    let state = SessionContext::new().state();
    format
        .clone()
        .with_schema_infer_max_rec(usize::MAX)
        .probe_keys(&state, &store, &[meta], &BTreeSet::new(), until_found)
        .await
}

async fn probe_schema(
    format: &SpiceJsonFormat,
    body: &[u8],
    payload: Payload,
) -> datafusion::error::Result<JsonSchemaProbe> {
    let (store, meta, _directory) = store_with(body, payload).await;
    let state = SessionContext::new().state();
    format.probe_schema(&state, &store, &[meta]).await
}

fn gzip(body: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip fixture");
    encoder.finish().expect("finish gzip fixture")
}

const JSONL: &[u8] = b"{\"a\":1}\n{\"a\":2,\"b\":2}\n\n{\"c\":3}\n{\"c\":4}\n";

#[tokio::test]
async fn jsonl_key_probe_stops_at_the_record_that_completes_the_wanted_keys() {
    let format = SpiceJsonFormat::default().with_format(Format::Jsonl);
    for payload in [Payload::File, Payload::Stream] {
        let probe = probe_keys(&format, JSONL, payload, Some(&keys(&["a", "b"])))
            .await
            .expect("probe JSONL");
        assert_eq!(probe.record_count, 2, "{payload:?}");
        assert_eq!(probe.keys, keys(&["a", "b"]), "{payload:?}");

        let probe = probe_keys(&format, JSONL, payload, None)
            .await
            .expect("probe JSONL");
        assert_eq!(
            probe.record_count, 4,
            "{payload:?}: the blank line is no record"
        );
        assert_eq!(probe.keys, keys(&["a", "b", "c"]), "{payload:?}");
    }
}

#[tokio::test]
async fn jsonl_key_probe_reads_every_record_when_a_wanted_key_is_absent() {
    let format = SpiceJsonFormat::default().with_format(Format::Jsonl);
    for payload in [Payload::File, Payload::Stream] {
        let probe = probe_keys(&format, JSONL, payload, Some(&keys(&["a", "renamed"])))
            .await
            .expect("probe JSONL");
        assert_eq!(probe.record_count, 4, "{payload:?}");
        assert_eq!(probe.keys, keys(&["a", "b", "c"]), "{payload:?}");
    }
}

/// The decompressed body is consumed as the probe parses it: records after the
/// one that completes the wanted keys are never parsed, so a malformed tail does
/// not fail a probe that stopped before it.
#[tokio::test]
async fn gzip_jsonl_key_probe_does_not_parse_past_the_wanted_keys() {
    let body = gzip(b"{\"a\":1,\"b\":1}\nnot json\n");
    let format = SpiceJsonFormat::default()
        .with_format(Format::Jsonl)
        .with_file_compression_type(FileCompressionType::GZIP);
    for payload in [Payload::File, Payload::Stream] {
        let probe = probe_keys(&format, &body, payload, Some(&keys(&["a", "b"])))
            .await
            .expect("the probe stops before the malformed line");
        assert_eq!(probe.record_count, 1, "{payload:?}");
        assert_eq!(probe.keys, keys(&["a", "b"]), "{payload:?}");

        probe_keys(&format, &body, payload, None)
            .await
            .expect_err("a probe of every record parses the malformed line");
    }
}

#[tokio::test]
async fn jsonl_key_probe_skips_a_leading_byte_order_mark() {
    let body = b"\xEF\xBB\xBF{\"a\":1}\n{\"b\":2}\n";
    let format = SpiceJsonFormat::default().with_format(Format::Jsonl);
    for payload in [Payload::File, Payload::Stream] {
        let probe = probe_keys(&format, body, payload, None)
            .await
            .expect("probe JSONL with a BOM");
        assert_eq!(probe.record_count, 2, "{payload:?}");
        assert_eq!(probe.keys, keys(&["a", "b"]), "{payload:?}");
    }
}

#[tokio::test]
async fn array_key_probe_streams_records_and_stops_at_the_wanted_keys() {
    let body = br#" [{"a":1},{"b":2},{"c":3}]"#;
    for format in [Format::Array, Format::Auto, Format::Json] {
        let json = SpiceJsonFormat::default().with_format(format);
        for payload in [Payload::File, Payload::Stream] {
            let probe = probe_keys(&json, body, payload, Some(&keys(&["a", "b"])))
                .await
                .expect("probe array");
            assert_eq!(probe.record_count, 2, "{format:?}/{payload:?}");
            assert_eq!(probe.keys, keys(&["a", "b"]), "{format:?}/{payload:?}");

            let probe = probe_keys(&json, body, payload, None)
                .await
                .expect("probe array");
            assert_eq!(probe.record_count, 3, "{format:?}/{payload:?}");
            assert_eq!(probe.keys, keys(&["a", "b", "c"]), "{format:?}/{payload:?}");
        }
    }
}

/// A single object, a NDJSON body under `json`/`auto`, and an empty body are
/// parsed whole, as before.
#[tokio::test]
async fn document_key_probes_count_records_as_before() {
    let cases: [(Format, &[u8], usize, &[&str]); 5] = [
        (
            Format::Object,
            b"{\n  \"a\": 1,\n  \"b\": 2\n}\n",
            1,
            &["a", "b"],
        ),
        (Format::Auto, b"{\n  \"a\": 1\n}", 1, &["a"]),
        (Format::Json, b"{\"a\":1}\n{\"b\":2}\n", 2, &["a", "b"]),
        (Format::Jsonl, b"", 0, &[]),
        (Format::Auto, b"  \n", 0, &[]),
    ];
    for (format, body, records, expected) in cases {
        let json = SpiceJsonFormat::default().with_format(format);
        for payload in [Payload::File, Payload::Stream] {
            let probe = probe_keys(&json, body, payload, None)
                .await
                .expect("probe document");
            assert_eq!(probe.record_count, records, "{format:?}/{payload:?}");
            assert_eq!(probe.keys, keys(expected), "{format:?}/{payload:?}");
        }
    }
}

#[tokio::test]
async fn schema_probe_reads_only_the_inference_sample() {
    let format = SpiceJsonFormat::default()
        .with_format(Format::Jsonl)
        .with_schema_infer_max_rec(2);
    for payload in [Payload::File, Payload::Stream] {
        let probe = probe_schema(&format, JSONL, payload)
            .await
            .expect("infer JSONL schema");
        assert_eq!(probe.record_count, 2, "{payload:?}");
        let fields: Vec<_> = probe
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(fields, ["a", "b"], "{payload:?}");
    }
}

#[tokio::test]
async fn schema_probe_flattens_streamed_records() {
    let format = SpiceJsonFormat::default()
        .with_format(Format::Jsonl)
        .with_flatten_json(".".to_string());
    for payload in [Payload::File, Payload::Stream] {
        let probe = probe_schema(&format, b"{\"user\":{\"name\":\"a\"}}\n", payload)
            .await
            .expect("infer flattened JSONL schema");
        assert_eq!(probe.record_count, 1, "{payload:?}");
        assert!(
            probe.schema.field_with_name("user.name").is_ok(),
            "{payload:?}: {:?}",
            probe.schema
        );
    }
}
