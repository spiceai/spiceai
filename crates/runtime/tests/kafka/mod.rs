/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Int64Array, UInt64Array};
use data_components::kafka::{KafkaConfig, KafkaConsumer, SslIdentification};
use futures::TryStreamExt;
use runtime::Runtime;
use serde_json::json;

pub mod bootstrap;
mod full_text;

use bootstrap::{
    create_kafka_topic_with_partitions, make_kafka_dataset, send_message_to_kafka_partition,
    send_messages_to_kafka, send_tombstone_to_kafka, start_kafka_docker_container,
};

use crate::configure_test_datafusion;
use crate::utils::runtime_ready_check;
use crate::{init_tracing, utils::test_request_context};

const KAFKA_PORT: u16 = 19093;
const KAFKA_MESSAGE_PROCESSING_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test]
async fn kafka_sasl_connect_test() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (running_container, producer) = start_kafka_docker_container(
                KAFKA_PORT,
                &["orders", "schema_infer_test", "flattent_json_test"],
            )
            .await?;

            tracing::debug!("Container started");

            // Load test data for orders representing the simple case where all fields are present in the first topic message
            let orders_simple: Vec<serde_json::Value> =
                serde_json::from_str(include_str!("./test_data/orders_simple.json"))?;
            send_messages_to_kafka(&producer, "orders", &orders_simple).await?;

            // Load test data for orders representing a more complex schema inference case where
            // the first messages do not have all fields present and some contain nulls
            let orders_schema_infer: Vec<serde_json::Value> =
                serde_json::from_str(include_str!("./test_data/orders_schema_infer.json"))?;
            send_messages_to_kafka(&producer, "schema_infer_test", &orders_schema_infer).await?;

            // Load test data that contains complex json to test 'flatten_json' param
            let orders_nested: Vec<serde_json::Value> =
                serde_json::from_str(include_str!("./test_data/orders_nested.json"))?;
            send_messages_to_kafka(&producer, "flattent_json_test", &orders_nested).await?;

            let ds = make_kafka_dataset("orders", "kafka_orders", KAFKA_PORT, None);
            let options = [("schema_infer_max_records".to_string(), "3".to_string())].into();
            let ds_schema_infer = make_kafka_dataset(
                "schema_infer_test",
                "kafka_schema_infer_test",
                KAFKA_PORT,
                Some(options),
            );

            let options = [("flatten_json".to_string(), "true".to_string())].into();
            let ds_flatten_json = make_kafka_dataset(
                "flattent_json_test",
                "kafka_flattent_json_test",
                KAFKA_PORT,
                Some(options),
            );

            let app = AppBuilder::new("kafka_sasl_connect_test")
                .with_dataset(ds)
                .with_dataset(ds_schema_infer)
                .with_dataset(ds_flatten_json)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            for (table, expected_rows) in [
                ("kafka_orders", orders_simple.len()),
                ("kafka_schema_infer_test", orders_schema_infer.len()),
                ("kafka_flattent_json_test", orders_nested.len()),
            ] {
                wait_for_query_rows(&rt, &format!("select * from {table}"), expected_rows).await?;
            }

            for table in [
                "kafka_orders",
                "kafka_schema_infer_test",
                "kafka_flattent_json_test",
            ] {
                let schema_snapshot = format!("{table}_schema");
                let data_snapshot = format!("{table}_data");

                run_and_snapshot_query(&rt, &format!("describe {table}"), &schema_snapshot).await?;
                run_and_snapshot_query(
                    &rt,
                    &format!("select * from {table} order by order_id"),
                    &data_snapshot,
                )
                .await?;
            }

            rt.shutdown().await;
            drop(rt);

            // Clean up container after test
            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

/// Verifies that `fetch_latest_message` correctly returns the latest non-tombstone
/// message when the tail of the partition contains tombstones.
#[tokio::test]
async fn kafka_fetch_latest_message_with_tombstone_test() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (running_container, producer) =
                start_kafka_docker_container(KAFKA_PORT, &["fetch_latest_tombstone_test"]).await?;

            // Send two normal messages followed by a tombstone on the tail.
            let messages: Vec<serde_json::Value> = vec![
                json!({"id": 1, "schema": "v1"}),
                json!({"id": 2, "schema": "v2"}),
            ];
            send_messages_to_kafka(&producer, "fetch_latest_tombstone_test", &messages).await?;
            send_tombstone_to_kafka(&producer, "fetch_latest_tombstone_test", 0, "key3").await?;

            let kafka_config = KafkaConfig {
                brokers: format!("localhost:{KAFKA_PORT}"),
                security_protocol: "SASL_PLAINTEXT".to_string(),
                sasl_mechanism: bootstrap::KAFKA_SASL_MECHANISM.to_string(),
                sasl_username: Some(bootstrap::KAFKA_SASL_USERNAME.to_string()),
                sasl_password: Some(bootstrap::KAFKA_SASL_PASSWORD.to_string()),
                ssl_ca_location: None,
                enable_ssl_certificate_verification: true,
                ssl_endpoint_identification_algorithm: SslIdentification::None,
                consumer_group_id: None,
                metrics_store: None,
            };

            let result = KafkaConsumer::fetch_latest_message::<String, serde_json::Value>(
                "fetch_latest_tombstone_test",
                &kafka_config,
                Duration::from_secs(10),
            )
            .await?;

            assert!(
                result.is_some(),
                "fetch_latest_message should return a message"
            );
            let (key, value) = result.expect("result");
            assert!(key.is_none(), "normal messages have no key");
            assert_eq!(
                value,
                json!({"id": 2, "schema": "v2"}),
                "latest non-tombstone should be the second message"
            );

            // Clean up container after test
            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

/// Verifies that `fetch_latest_message` inspects every partition and returns
/// the latest non-tombstone message by timestamp across all partitions.
#[tokio::test]
async fn kafka_fetch_latest_message_multi_partition_test() -> anyhow::Result<()> {
    const TEST_PORT: u16 = 19095;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (running_container, producer) =
                start_kafka_docker_container(TEST_PORT, &[]).await?;

            // Create a 2-partition topic explicitly.
            create_kafka_topic_with_partitions(
                &running_container,
                TEST_PORT,
                "fetch_latest_multi_partition_test",
                2,
            )
            .await?;

            // Send message to partition 0 with timestamp 1000.
            send_message_to_kafka_partition(
                &producer,
                "fetch_latest_multi_partition_test",
                0,
                1000,
                &json!({"id": 1, "schema": "v1"}),
            )
            .await?;

            // Send message to partition 1 with timestamp 2000.
            send_message_to_kafka_partition(
                &producer,
                "fetch_latest_multi_partition_test",
                1,
                2000,
                &json!({"id": 2, "schema": "v2"}),
            )
            .await?;

            // Send tombstone to partition 1 with timestamp 3000.
            send_tombstone_to_kafka(
                &producer,
                "fetch_latest_multi_partition_test",
                1,
                "tombstone-key",
            )
            .await?;

            let kafka_config = KafkaConfig {
                brokers: format!("localhost:{TEST_PORT}"),
                security_protocol: "SASL_PLAINTEXT".to_string(),
                sasl_mechanism: bootstrap::KAFKA_SASL_MECHANISM.to_string(),
                sasl_username: Some(bootstrap::KAFKA_SASL_USERNAME.to_string()),
                sasl_password: Some(bootstrap::KAFKA_SASL_PASSWORD.to_string()),
                ssl_ca_location: None,
                enable_ssl_certificate_verification: true,
                ssl_endpoint_identification_algorithm: SslIdentification::None,
                consumer_group_id: None,
                metrics_store: None,
            };

            let result = KafkaConsumer::fetch_latest_message::<String, serde_json::Value>(
                "fetch_latest_multi_partition_test",
                &kafka_config,
                Duration::from_secs(10),
            )
            .await?;

            assert!(
                result.is_some(),
                "fetch_latest_message should return a message across partitions"
            );
            let (key, value) = result.expect("result");
            assert!(key.is_none(), "normal messages have no key");
            assert_eq!(
                value,
                json!({"id": 2, "schema": "v2"}),
                "latest non-tombstone across all partitions should be the message with timestamp 2000"
            );

            // Clean up container after test
            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

pub(super) async fn wait_for_query_rows(
    rt: &Runtime,
    query: &str,
    expected_rows: usize,
) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let mut last_error = None;

    while start_time.elapsed() <= KAFKA_MESSAGE_PROCESSING_TIMEOUT {
        match query_row_count(rt, query).await {
            Ok(actual_rows) if actual_rows >= expected_rows => return Ok(()),
            Ok(actual_rows) => {
                last_error = Some(format!(
                    "query returned {actual_rows} rows; expected at least {expected_rows}"
                ));
            }
            Err(error) => last_error = Some(error.to_string()),
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(anyhow::anyhow!(
        "Timed out waiting for Kafka query to return at least {expected_rows} rows within {}s. Last error: {}",
        KAFKA_MESSAGE_PROCESSING_TIMEOUT.as_secs(),
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

async fn query_row_count(rt: &Runtime, query: &str) -> Result<usize, anyhow::Error> {
    let count_query =
        format!("SELECT COUNT(*) AS row_count FROM ({query}) AS kafka_readiness_query");
    let query_result = rt
        .datafusion()
        .query_builder(&count_query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let data = query_result.data.try_collect::<Vec<_>>().await?;
    let batch = data
        .first()
        .ok_or_else(|| anyhow::anyhow!("Kafka row count query returned no batches"))?;
    let column = batch.column(0);

    if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        if array.is_empty() {
            return Err(anyhow::anyhow!("Kafka row count query returned no rows"));
        }

        return usize::try_from(array.value(0))
            .map_err(|_| anyhow::anyhow!("Kafka row count overflowed usize"));
    }

    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        if array.is_empty() {
            return Err(anyhow::anyhow!("Kafka row count query returned no rows"));
        }

        return usize::try_from(array.value(0))
            .map_err(|_| anyhow::anyhow!("Kafka row count was negative or overflowed usize"));
    }

    Err(anyhow::anyhow!(
        "Kafka row count query returned unexpected column type {:?}",
        column.data_type()
    ))
}

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    test_name: &str,
) -> Result<(), anyhow::Error> {
    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let data = query_result.data.try_collect::<Vec<_>>().await?;

    let formatted = arrow::util::pretty::pretty_format_batches(&data)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(test_name, formatted);
    Ok(())
}
