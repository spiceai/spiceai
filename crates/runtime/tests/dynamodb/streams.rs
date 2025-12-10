use crate::docker::{ContainerRunnerBuilder, RunningContainer};
use crate::utils::{runtime_ready_check, test_request_context};
use crate::{ValidateFn, configure_test_datafusion, init_tracing, run_query_and_check_results};
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
use spicepod::acceleration::{Mode, OnConflictBehavior, RefreshMode};
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
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{RetryError, retry};

const DYNAMODB_DOCKER_CONTAINER: &str = "runtime-integration-test-dynamodb";
const PORT: u16 = 8001;

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
        (
            "endpoint_url".to_string(),
            format!("http://localhost:{port}"),
        ),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            // engine: Some("duckdb".to_string()),
            // mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            // refresh_mode: Some(RefreshMode::Full),
            // on_conflict: HashMap::from([("id".to_string(), OnConflictBehavior::Upsert)]),
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
                .unwrap(),
        )
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
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
        let id = format!("id-{}", i);

        client
            .put_item()
            .table_name(table_name)
            .item("id", AttributeValue::S(id))
            .item("name", AttributeValue::S(format!("Item {}", i)))
            .item("version", AttributeValue::N(i.to_string()))
            .send()
            .await
            .expect("Failed to insert item");
    }
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
            let running_container = start_dynamodb_docker_container(PORT).await?;

            let client = get_client(PORT, &access_key, &secret_key);

            create_table(&client, table_name).await;
            insert_rows(&client, "test_table", 0..5).await;

            let app = AppBuilder::new("dynamodb_integration_test")
                .with_dataset(make_dynamodb_dataset(
                    table_name,
                    PORT,
                    &access_key,
                    &secret_key,
                    true,
                ))
                .with_results_cache(ResultsCache {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            sleep(Duration::from_secs(2)).await;

            let query_result = rt
                .datafusion()
                .query_builder(&format!("SELECT * FROM {table_name} ORDER BY id"))
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let data = query_result.data.try_collect::<Vec<_>>().await?;
            println!("Result: {data:#?}");

            insert_rows(&client, "test_table", 5..7).await;
            sleep(Duration::from_secs(2)).await;
            let query_result = rt
                .datafusion()
                .query_builder(&format!("SELECT * FROM {table_name} ORDER BY id"))
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let data = query_result.data.try_collect::<Vec<_>>().await?;
            println!("Result2: {data:#?}");

            insert_rows(&client, "test_table", 7..10).await;
            sleep(Duration::from_secs(2)).await;
            let query_result = rt
                .datafusion()
                .query_builder(&format!("SELECT * FROM {table_name} ORDER BY id"))
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let data = query_result.data.try_collect::<Vec<_>>().await?;
            println!("Result3: {data:#?}");

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}
