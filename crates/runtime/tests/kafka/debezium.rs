/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use rdkafka::producer::{FutureProducer, FutureRecord};
use runtime::Runtime;
use serde_json::json;
use spicepod::acceleration::RefreshMode;
use spicepod::semantic::Column;
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};

use super::bootstrap::{
    KAFKA_SASL_MECHANISM, KAFKA_SASL_PASSWORD, KAFKA_SASL_USERNAME, start_kafka_docker_container,
};
use super::{run_and_snapshot_query, wait_for_query_rows};
use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

const DEBEZIUM_PORT: u16 = 19095;

fn make_debezium_dataset(topic: &str, name: &str, port: u16) -> Dataset {
    let params = HashMap::from([
        (
            "kafka_bootstrap_servers".to_string(),
            format!("localhost:{port}"),
        ),
        (
            "kafka_security_protocol".to_string(),
            "SASL_PLAINTEXT".to_string(),
        ),
        (
            "kafka_sasl_mechanism".to_string(),
            KAFKA_SASL_MECHANISM.to_string(),
        ),
        (
            "kafka_sasl_username".to_string(),
            KAFKA_SASL_USERNAME.to_string(),
        ),
        (
            "kafka_sasl_password".to_string(),
            KAFKA_SASL_PASSWORD.to_string(),
        ),
    ]);

    let mut dataset = Dataset::new(format!("debezium:{topic}"), name.to_string());
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset.columns = vec![
        Column::new("id").with_type("int4"),
        Column::new("name").with_type("text"),
        Column::new("version").with_type("bigint"),
    ];
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some("id".to_string()),
        ..Acceleration::default()
    });

    dataset
}

/// Sends Debezium INSERT change events to a Kafka topic.
///
/// Each row is sent as a key/value pair where the key contains the primary key
/// envelope and the value contains the full change event envelope.
async fn send_debezium_inserts(
    producer: &FutureProducer,
    topic: &str,
    rows: &[(i32, &str, i64)],
) -> anyhow::Result<()> {
    const SEND_TIMEOUT: Duration = Duration::from_secs(10);

    // Minimal field descriptors reused in every message schema.
    let field_defs = json!([
        {"type": "int32",  "optional": false, "field": "id"},
        {"type": "string", "optional": true,  "field": "name"},
        {"type": "int64",  "optional": true,  "field": "version"}
    ]);

    let source_field_defs = json!([
        {"type": "string", "optional": false, "field": "version"},
        {"type": "string", "optional": false, "field": "connector"},
        {"type": "string", "optional": false, "field": "name"},
        {"type": "int64",  "optional": false, "field": "ts_ms"},
        {"type": "string", "optional": false, "field": "snapshot"},
        {"type": "string", "optional": false, "field": "db"},
        {"type": "string", "optional": false, "field": "table"}
    ]);

    for (id, name, version) in rows {
        let key = serde_json::to_string(&json!({
            "schema": {
                "type": "struct",
                "fields": [{"type": "int32", "optional": false, "field": "id"}],
                "optional": false,
                "name": "test.Key"
            },
            "payload": {"id": id}
        }))?;

        let value = serde_json::to_string(&json!({
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "struct",
                        "fields": field_defs,
                        "optional": true,
                        "name": "test.Envelope.after",
                        "field": "after"
                    },
                    {
                        "type": "struct",
                        "fields": field_defs,
                        "optional": true,
                        "name": "test.Envelope.before",
                        "field": "before"
                    },
                    {
                        "type": "struct",
                        "fields": source_field_defs,
                        "optional": false,
                        "name": "io.debezium.connector.postgresql.Source",
                        "field": "source"
                    },
                    {"type": "string", "optional": false, "field": "op"},
                    {"type": "int64",  "optional": true,  "field": "ts_ms"},
                    {
                        "type": "struct",
                        "fields": [],
                        "optional": true,
                        "name": "event.block",
                        "field": "transaction"
                    }
                ],
                "optional": false,
                "name": "test.Envelope"
            },
            "payload": {
                "before": null,
                "after": {"id": id, "name": name, "version": version},
                "source": {
                    "version": "1.9.0.Final",
                    "connector": "postgresql",
                    "name": "test",
                    "ts_ms": 1_234_567_890_i64,
                    "snapshot": "false",
                    "db": "testdb",
                    "table": "orders"
                },
                "op": "c",
                "ts_ms": 1_234_567_890_i64,
                "transaction": null
            }
        }))?;

        producer
            .send(
                FutureRecord::<String, String>::to(topic)
                    .key(&key)
                    .payload(&value),
                SEND_TIMEOUT,
            )
            .await
            .map_err(|(e, _)| anyhow::Error::msg(format!("Kafka send failed: {e}")))?;
    }

    Ok(())
}

/// Verifies that a Debezium-accelerated dataset with a declared schema initializes
/// successfully when the Kafka topic is empty, then becomes queryable once CDC
/// messages are produced.
#[tokio::test(flavor = "multi_thread")]
async fn debezium_declared_schema_empty_topic() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,debezium=debug,info",
    ));

    let topic = "debezium_test_orders";
    let dataset_name = "test_orders";

    test_request_context()
        .scope(async {
            // Start Redpanda with the topic created but empty.
            let (running_container, producer) =
                start_kafka_docker_container(DEBEZIUM_PORT, &[topic]).await?;

            let ds = make_debezium_dataset(topic, dataset_name, DEBEZIUM_PORT);

            let app = AppBuilder::new("debezium_declared_schema_test")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Expect successful load even though the topic has no messages.
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Schema should reflect the declared columns.
            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {dataset_name}"),
                "debezium_declared_schema_empty_topic_schema",
            )
            .await?;

            // Produce Debezium INSERT events.
            send_debezium_inserts(
                &producer,
                topic,
                &[(1, "Alice", 100), (2, "Bob", 200), (3, "Carol", 300)],
            )
            .await?;

            // Wait for the changes to be applied to the accelerated dataset.
            wait_for_query_rows(&rt, &format!("SELECT * FROM {dataset_name}"), 3).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {dataset_name} ORDER BY id"),
                "debezium_declared_schema_empty_topic_data",
            )
            .await?;

            rt.shutdown().await;
            drop(rt);

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}
