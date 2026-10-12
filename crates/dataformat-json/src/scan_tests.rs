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

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

use crate::{Format, SpiceJsonFormat};

#[derive(Clone, Copy, Debug)]
enum Payload {
    File,
    Stream,
}

async fn scan(
    body: &[u8],
    format: Format,
    payload: Payload,
) -> datafusion::error::Result<Vec<i64>> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let directory = tempfile::tempdir().expect("create JSON fixture directory");
    let location = match payload {
        Payload::File => {
            let path = directory.path().join("records.json");
            std::fs::write(&path, body).expect("write JSON fixture");
            ListingTableUrl::parse(path.to_str().expect("UTF-8 fixture path"))?
        }
        Payload::Stream => {
            let store = Arc::new(InMemory::new());
            store
                .put(&Path::from("records.json"), body.to_vec().into())
                .await?;
            let url = ObjectStoreUrl::parse("memory://json-audit")?;
            ctx.runtime_env().register_object_store(url.as_ref(), store);
            ListingTableUrl::parse("memory://json-audit/records.json")?
        }
    };
    // A registered schema can outlive the file from which it was inferred.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let options = ListingOptions::new(Arc::new(SpiceJsonFormat::default().with_format(format)))
        .with_file_extension(".json");
    let config = ListingTableConfig::new(location)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("records", Arc::new(ListingTable::try_new(config)?))?;
    let batches = ctx
        .sql("SELECT id FROM records ORDER BY id")
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        assert_eq!(ids.null_count(), 0, "fixture IDs are not null");
        rows.extend(ids.values().iter().copied());
    }
    Ok(rows)
}

#[tokio::test]
async fn array_scan_preserves_complete_input() {
    for body in [
        &b"[]"[..],
        &br#"[{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}]"#[..],
    ] {
        let oracle: Vec<serde_json::Value> = serde_json::from_slice(body).expect("valid array");
        let expected: Vec<i64> = oracle
            .iter()
            .map(|row| row["id"].as_i64().expect("integer ID"))
            .collect();
        for format in [Format::Array, Format::Auto, Format::Json] {
            for payload in [Payload::File, Payload::Stream] {
                let rows = scan(body, format, payload)
                    .await
                    .expect("scan complete array");
                assert_eq!(rows, expected, "{format:?}/{payload:?}");
            }
        }
    }
}

#[tokio::test]
async fn array_scan_rejects_eof_after_separator() {
    let mut accepted = Vec::new();
    for body in [
        &b"["[..],
        &b"[ \n\t"[..],
        &br#"[{"id":1},"#[..],
        &b"[{\"id\":1}, \n\t"[..],
        &br#"[{"id":1},{"id":2},{"id":3},"#[..],
    ] {
        let oracle = serde_json::from_slice::<Vec<serde_json::Value>>(body)
            .expect_err("the independent parser must reject the truncated fixture");
        assert_eq!(oracle.classify(), serde_json::error::Category::Eof);
        for format in [Format::Array, Format::Auto, Format::Json] {
            for payload in [Payload::File, Payload::Stream] {
                match scan(body, format, payload).await {
                    Ok(rows) => accepted.push(format!(
                        "{format:?}/{payload:?}: {:?} returned {rows:?}",
                        String::from_utf8_lossy(body)
                    )),
                    Err(error) => assert!(
                        error.to_string().contains("closing ']'"),
                        "expected the end-of-array error, got {error} for {format:?}/{payload:?}"
                    ),
                }
            }
        }
    }
    assert_eq!(
        accepted,
        Vec::<String>::new(),
        "truncated arrays must fail, not return a successful prefix"
    );
}
