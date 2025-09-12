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
use futures::TryStreamExt;
use runtime::Runtime;

pub mod bootstrap;

use bootstrap::{make_kafka_dataset, send_messages_to_kafka, start_kafka_docker_container};
use tokio::time::sleep;

use crate::configure_test_datafusion;
use crate::utils::runtime_ready_check;
use crate::{init_tracing, utils::test_request_context};

const KAFKA_PORT: u16 = 19093;

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
            let orders_schema_infer: Vec<serde_json::Value> =
                serde_json::from_str(include_str!("./test_data/orders_nested.json"))?;
            send_messages_to_kafka(&producer, "flattent_json_test", &orders_schema_infer).await?;

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

            // Ensure all messages are processed
            sleep(Duration::from_secs(2)).await;

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
