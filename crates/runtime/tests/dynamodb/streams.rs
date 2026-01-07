/*
Copyright 2025 The Spice.ai OSS Authors

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
use crate::docker::{ContainerRunnerBuilder, RunningContainer};
use crate::utils::{runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};
use app::AppBuilder;
use async_graphql::futures_util::TryStreamExt;
use aws_config::{BehaviorVersion, Region, SdkConfig, retry::RetryConfig};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, StreamSpecification, StreamViewType,
    },
};
use bollard::secret::HealthConfig;
use runtime::Runtime;
use spicepod::acceleration::RefreshMode;
use spicepod::component::caching::ResultsCache;
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;

const DYNAMODB_DOCKER_CONTAINER: &str = "runtime-integration-test-dynamodb";
const PORT1: u16 = 8001;
const PORT2: u16 = 8002;

#[instrument]
pub async fn start_dynamodb_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{DYNAMODB_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image("amazon/dynamodb-local:latest".to_string())
        .add_port_binding(8000, port)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "curl -s http://localhost:8000 | grep -q 'MissingAuthenticationToken' || exit 1"
                    .to_string(),
            ]),
            interval: Some(2_000_000_000), // 2 seconds
            timeout: Some(10_000_000_000), // 10 seconds
            retries: Some(15),
            start_period: Some(10_000_000_000), // 10 seconds
            start_interval: None,
        })
        .build()?
        .run(None)
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

pub fn make_dynamodb_dataset(
    table_name: &str,
    port: u16,
    access_key: &str,
    secret_key: &str,
    accelerated: bool,
) -> Dataset {
    let mut dataset = Dataset::new(format!("dynamodb:{table_name}"), table_name.to_string());
    let params = HashMap::from([
        (
            "dynamodb_aws_access_key_id".to_string(),
            access_key.to_string(),
        ),
        (
            "dynamodb_aws_secret_access_key".to_string(),
            secret_key.to_string(),
        ),
        ("dynamodb_aws_region".to_string(), "us-east-1".to_string()),
        ("dynamodb_aws_auth".to_string(), "key".to_string()),
        (
            "endpoint_url".to_string(),
            format!("http://localhost:{port}"),
        ),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            refresh_mode: Some(RefreshMode::Changes),
            ..Acceleration::default()
        });
    }
    dataset
}

async fn create_table(client: &Client, table_name: &str) {
    client
        .create_table()
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("Attribute definition created"),
        )
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .expect("Key schema element created"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .expect("Stream specification created"),
        )
        .send()
        .await
        .expect("Table created");
}

fn get_client(port: u16, access_key: &str, secret_key: &str) -> Client {
    let config = SdkConfig::builder()
        .endpoint_url(format!("http://localhost:{port}"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::from_keys(
            access_key, secret_key, None,
        )))
        .retry_config(RetryConfig::standard().with_max_attempts(5))
        .behavior_version(BehaviorVersion::latest())
        .region(Some(Region::from_static("us-east-1")))
        .build();
    Client::new(&config)
}

async fn insert_rows(client: &Client, table_name: &str, range: Range<usize>) {
    for i in range {
        client
            .put_item()
            .table_name(table_name)
            .item("id", AttributeValue::S(format!("id-{i}")))
            .item("name", AttributeValue::S(format!("Item {i}")))
            .item("version", AttributeValue::N(i.to_string()))
            .send()
            .await
            .expect("Failed to insert item");
    }
}

async fn insert_item(client: &Client, table_name: &str, id: &str, name: &str, version: i32) {
    client
        .put_item()
        .table_name(table_name)
        .item("id", AttributeValue::S(id.to_string()))
        .item("name", AttributeValue::S(name.to_string()))
        .item("version", AttributeValue::N(version.to_string()))
        .send()
        .await
        .expect("Failed to insert item");
}

async fn delete_item(client: &Client, table_name: &str, id: &str) {
    client
        .delete_item()
        .table_name(table_name)
        .key("id", AttributeValue::S(id.to_string()))
        .send()
        .await
        .expect("Failed to delete item");
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "test_table";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT2).await?;

            let client = get_client(PORT2, access_key, secret_key);

            create_table(&client, table_name).await;
            insert_rows(&client, "test_table", 0..5).await;
            sleep(Duration::from_secs(2)).await;

            let app = AppBuilder::new("dynamodb_integration_test")
                .with_dataset(make_dynamodb_dataset(
                    table_name, PORT2, access_key, secret_key, true,
                ))
                .with_results_cache(ResultsCache {
                    enabled: false,
                    ..Default::default()
                })
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
            sleep(Duration::from_secs(2)).await;
            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "test1",
            )
            .await?;

            insert_rows(&client, "test_table", 5..7).await;
            sleep(Duration::from_secs(2)).await;
            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "test2",
            )
            .await?;

            insert_rows(&client, "test_table", 7..10).await;
            sleep(Duration::from_secs(2)).await;
            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "test3",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams_delete() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "batch_delete_test";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT1).await?;
            let client = get_client(PORT1, access_key, secret_key);

            create_table(&client, table_name).await;
            for i in 0..5 {
                insert_item(
                    &client,
                    table_name,
                    &format!("id-{i}"),
                    &format!("Item {i}"),
                    i,
                )
                .await;
            }
            for i in 5..8 {
                insert_item(
                    &client,
                    table_name,
                    &format!("id-{i}"),
                    &format!("Item {i}"),
                    i,
                )
                .await;
            }

            delete_item(&client, table_name, "id-5").await;
            delete_item(&client, table_name, "id-6").await;
            delete_item(&client, table_name, "id-7").await;

            sleep(Duration::from_secs(1)).await;

            let app = AppBuilder::new("dynamodb_batch_delete_test")
                .with_dataset(make_dynamodb_dataset(
                    table_name, PORT1, access_key, secret_key, true,
                ))
                .with_results_cache(ResultsCache {
                    enabled: false,
                    ..Default::default()
                })
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
            sleep(Duration::from_secs(3)).await;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "batch_delete_final_state",
            )
            .await?;

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
