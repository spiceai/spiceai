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

//! Integration tests for push-based Debezium CDC ingest (`from: cdc:…`).
//!
//! Verifies the dual-path design end-to-end: HTTP POST of Debezium JSON change
//! events → decode → changes stream → accelerated table apply → query.

#![cfg(feature = "debezium")]
#![allow(clippy::expect_used)]

mod search;

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use app::AppBuilder;
use futures::TryStreamExt;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use spicepod::{
    acceleration::{Acceleration, OnConflictBehavior, RefreshMode},
    component::dataset::Dataset,
    semantic::Column,
};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

fn cdc_orders_dataset() -> Dataset {
    let mut dataset = Dataset::new("cdc:orders", "orders");
    dataset.columns = vec![
        Column {
            name: "id".to_string(),
            r#type: Some("int64".to_string()),
            nullable: Some(true),
            ..Column::new("id")
        },
        Column {
            name: "name".to_string(),
            r#type: Some("utf8".to_string()),
            nullable: Some(true),
            ..Column::new("name")
        },
    ];
    let mut on_conflict = HashMap::new();
    on_conflict.insert("id".to_string(), OnConflictBehavior::Upsert);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some("id".to_string()),
        on_conflict,
        ..Acceleration::default()
    });
    dataset
}

async fn start_http(rt: Arc<Runtime>) -> String {
    let http_listener =
        std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0)).expect("bind http");
    let http_port = http_listener.local_addr().expect("addr").port();
    drop(http_listener);
    let flight_listener =
        std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0)).expect("bind flight");
    let flight_port = flight_listener.local_addr().expect("addr").port();
    drop(flight_listener);

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));
    let base = format!("http://{LOCALHOST}:{http_port}");
    let health = format!("{base}/health");
    let server_rt = Arc::clone(&rt);
    tokio::spawn(async move {
        let _ = server_rt
            .start_servers(api_config, None, EndpointAuth::no_auth())
            .await;
    });

    let client = reqwest::Client::new();
    let ready = wait_until_true(Duration::from_secs(10), || {
        let client = client.clone();
        let health = health.clone();
        async move {
            client
                .get(&health)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
        }
    })
    .await;
    assert!(ready, "HTTP server did not become ready");
    base
}

#[tokio::test]
async fn cdc_ingest_json_create_update_delete() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("cdc_ingest_test")
                .with_dataset(cdc_orders_dataset())
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let load_rt = Arc::clone(&rt);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    anyhow::bail!("timed out loading components");
                }
                () = load_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let base = start_http(Arc::clone(&rt)).await;
            let client = reqwest::Client::new();
            let url = format!("{base}/v1/datasets/orders/cdc");

            // Wait until the CDC ingest stream has registered.
            let registered = wait_until_true(Duration::from_secs(15), || async {
                runtime::dataconnector::cdc_ingest::lookup("orders").is_some()
            })
            .await;
            assert!(registered, "CDC ingest handle never registered");

            // CREATE
            let create = r#"{"before":null,"after":{"id":1,"name":"alice"},"op":"c","ts_ms":1,"source":{}}"#;
            let resp = client
                .post(&url)
                .header("content-type", "application/json")
                .body(create)
                .send()
                .await
                .expect("post create");
            assert_eq!(resp.status(), reqwest::StatusCode::OK, "create: {}", resp.text().await.unwrap_or_default());

            // UPDATE
            let update = r#"{"before":{"id":1,"name":"alice"},"after":{"id":1,"name":"bob"},"op":"u","ts_ms":2,"source":{}}"#;
            let resp = client
                .post(&url)
                .header("content-type", "application/vnd.debezium+json")
                .body(update)
                .send()
                .await
                .expect("post update");
            assert_eq!(resp.status(), reqwest::StatusCode::OK, "update: {}", resp.text().await.unwrap_or_default());

            // Query via SQL
            let mut seen_bob = false;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while tokio::time::Instant::now() < deadline {
                let batches = rt
                    .datafusion()
                    .query_builder("SELECT id, name FROM orders ORDER BY id")
                    .build()
                    .run()
                    .await
                    .expect("query")
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("collect");
                if !batches.is_empty() && batches[0].num_rows() > 0 {
                    let names = batches[0]
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("name col");
                    if names.value(0) == "bob" {
                        seen_bob = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(seen_bob, "expected updated row name=bob in accelerator");

            // DELETE
            let delete = r#"{"before":{"id":1,"name":"bob"},"after":null,"op":"d","ts_ms":3,"source":{}}"#;
            let resp = client
                .post(&url)
                .header("content-type", "application/json")
                .body(delete)
                .send()
                .await
                .expect("post delete");
            assert_eq!(resp.status(), reqwest::StatusCode::OK, "delete: {}", resp.text().await.unwrap_or_default());

            // AVRO create: raw Avro datum with the schema supplied via the
            // X-Avro-Schema header. Bytes are the Avro binary encoding of
            // {before: null, after: {id: 2, name: "eve"}, op: "c", ts_ms: 5}
            // against the header schema (field order: before, after, op, ts_ms):
            //   before: union branch 0 (null)          -> 0x00
            //   after:  union branch 1 (Value)         -> 0x02
            //     id:   long 2 (zigzag 4)              -> 0x04
            //     name: string "eve" (len 3, zigzag 6) -> 0x06 'e' 'v' 'e'
            //   op:     string "c" (len 1)             -> 0x02 'c'
            //   ts_ms:  long 5 (zigzag 10)             -> 0x0A
            let avro_schema = r#"{"type":"record","name":"Envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"op","type":"string"},{"name":"ts_ms","type":"long","default":0}]}"#;
            let avro_create: &[u8] = &[0x00, 0x02, 0x04, 0x06, b'e', b'v', b'e', 0x02, b'c', 0x0A];
            let resp = client
                .post(&url)
                .header("content-type", "application/vnd.debezium+avro")
                .header("x-avro-schema", avro_schema)
                .body(avro_create.to_vec())
                .send()
                .await
                .expect("post avro create");
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "avro create: {}",
                resp.text().await.unwrap_or_default()
            );

            // The JSON delete removed id=1, so the table should converge to
            // exactly the Avro-created row (2, "eve") — verifying both the
            // delete apply and the Avro ingest end-to-end.
            let avro_applied = wait_until_true(Duration::from_secs(10), || {
                let rt = Arc::clone(&rt);
                async move {
                    let batches = rt
                        .datafusion()
                        .query_builder("SELECT id, name FROM orders ORDER BY id")
                        .build()
                        .run()
                        .await
                        .expect("query")
                        .data
                        .try_collect::<Vec<_>>()
                        .await
                        .expect("collect");
                    let mut rows = Vec::new();
                    for batch in &batches {
                        let ids = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .expect("id col");
                        let names = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .expect("name col");
                        for i in 0..batch.num_rows() {
                            rows.push((ids.value(i), names.value(i).to_string()));
                        }
                    }
                    rows == [(2, "eve".to_string())]
                }
            })
            .await;
            assert!(
                avro_applied,
                "expected exactly one row (2, 'eve') after JSON delete + Avro create"
            );

            // Bad content-type
            let resp = client
                .post(&url)
                .header("content-type", "text/plain")
                .body("not json")
                .send()
                .await
                .expect("post bad");
            assert!(
                resp.status().is_client_error(),
                "expected 4xx for bad content-type"
            );

            Ok(())
        })
        .await
}
