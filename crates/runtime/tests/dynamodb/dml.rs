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

#![allow(clippy::expect_used)]

use super::streams::{make_dynamodb_dataset, start_dynamodb_docker_container};
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
        ScalarAttributeType,
    },
};
use runtime::Runtime;
use spicepod::component::access::AccessMode;
use spicepod::component::caching::SQLResultsCacheConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const PORT_DML_INSERT: u16 = 8010;
const PORT_DML_DELETE: u16 = 8011;
const PORT_DML_INSERT_DELETE: u16 = 8012;
const PORT_DML_TYPES: u16 = 8013;
const PORT_DML_COMPOSITE_DELETE: u16 = 8014;

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
        .send()
        .await
        .expect("Table created");
}

async fn create_composite_key_table(client: &Client, table_name: &str) {
    client
        .create_table()
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("pk attribute definition"),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("sk attribute definition"),
        )
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .expect("pk key schema"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .expect("sk key schema"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .expect("Composite key table created");
}

async fn run_query(rt: &Runtime, query: &str) -> Result<String, anyhow::Error> {
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
    Ok(formatted.to_string())
}

async fn wait_for_query_contents(
    rt: &Runtime,
    query: &str,
    required: &[&str],
    forbidden: &[&str],
) -> Result<String, anyhow::Error> {
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(30);
    let mut last_result = String::new();
    let mut last_error = None;

    while start_time.elapsed() <= timeout {
        match run_query(rt, query).await {
            Ok(result) => {
                let has_required = required.iter().all(|value| result.contains(value));
                let has_no_forbidden = forbidden.iter().all(|value| !result.contains(value));

                if has_required && has_no_forbidden {
                    return Ok(result);
                }

                last_result = result;
            }
            Err(error) => last_error = Some(error.to_string()),
        }

        sleep(Duration::from_millis(100)).await;
    }

    let last_result = compact_message(&last_result, 500);
    let last_error = last_error
        .as_deref()
        .map_or_else(|| "none".to_string(), |error| compact_message(error, 500));

    Err(anyhow::anyhow!(
        "Timed out waiting for query contents. Required: {required:?}; forbidden: {forbidden:?}; last result: {last_result}; last error: {last_error}"
    ))
}

fn compact_message(message: &str, max_chars: usize) -> String {
    let mut compact = String::with_capacity(message.len().min(max_chars));
    let mut chars = message.chars();

    for _ in 0..max_chars {
        let Some(ch) = chars.next() else {
            return compact;
        };

        match ch {
            '\n' | '\r' => compact.push(' '),
            _ => compact.push(ch),
        }
    }

    if chars.next().is_some() {
        compact.push_str("...");
    }

    compact
}

async fn setup_runtime(table_name: &str, port: u16) -> Result<Runtime, anyhow::Error> {
    let mut dataset = make_dynamodb_dataset(table_name, port, "foo", "bar", false);
    dataset.access = AccessMode::ReadWrite;

    let app = AppBuilder::new("dynamodb_dml_test")
        .with_dataset(dataset)
        .with_sql_cache(SQLResultsCacheConfig {
            enabled: false,
            ..Default::default()
        })
        .build();

    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;

    let cloned_rt = Arc::new(rt.clone());
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;
    Ok(rt)
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_dml_insert() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "dml_insert_test";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT_DML_INSERT).await?;
            let client = get_client(PORT_DML_INSERT, "foo", "bar");

            create_table(&client, table_name).await;

            // Seed one row so schema inference works
            client
                .put_item()
                .table_name(table_name)
                .item("id", AttributeValue::S("seed".to_string()))
                .item("name", AttributeValue::S("Seed Item".to_string()))
                .send()
                .await?;

            let rt = setup_runtime(table_name, PORT_DML_INSERT).await?;

            // Verify seed data
            let result = run_query(&rt, &format!("SELECT * FROM {table_name} ORDER BY id")).await?;
            assert!(result.contains("seed"), "Should contain seed row");

            // Insert via SQL
            run_query(
                &rt,
                &format!(
                    "INSERT INTO {table_name} (id, name) VALUES ('1', 'Item 1'), ('2', 'Item 2')"
                ),
            )
            .await?;

            // Verify the inserted rows are visible via DynamoDB scan
            let result = wait_for_query_contents(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                &["Item 1", "Item 2", "seed"],
                &[],
            )
            .await?;
            assert!(result.contains("Item 1"), "Should contain inserted Item 1");
            assert!(result.contains("Item 2"), "Should contain inserted Item 2");
            assert!(result.contains("seed"), "Should still contain seed row");

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_dml_delete() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "dml_delete_test";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT_DML_DELETE).await?;
            let client = get_client(PORT_DML_DELETE, "foo", "bar");

            create_table(&client, table_name).await;

            // Seed rows
            for i in 1..=4 {
                client
                    .put_item()
                    .table_name(table_name)
                    .item("id", AttributeValue::S(format!("{i}")))
                    .item("name", AttributeValue::S(format!("Item {i}")))
                    .send()
                    .await?;
            }

            let rt = setup_runtime(table_name, PORT_DML_DELETE).await?;

            // Verify all 4 rows exist
            let result = run_query(&rt, &format!("SELECT * FROM {table_name} ORDER BY id")).await?;
            assert!(result.contains("Item 1"));
            assert!(result.contains("Item 4"));

            // Delete specific rows via SQL
            run_query(
                &rt,
                &format!("DELETE FROM {table_name} WHERE id IN ('1', '2')"),
            )
            .await?;

            // Verify deletion
            let result = wait_for_query_contents(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                &["Item 3", "Item 4"],
                &["Item 1", "Item 2"],
            )
            .await?;
            assert!(!result.contains("Item 1"), "Item 1 should be deleted");
            assert!(!result.contains("Item 2"), "Item 2 should be deleted");
            assert!(result.contains("Item 3"), "Item 3 should remain");
            assert!(result.contains("Item 4"), "Item 4 should remain");

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_dml_insert_then_delete() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "dml_insert_delete_test";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_DML_INSERT_DELETE).await?;
            let client = get_client(PORT_DML_INSERT_DELETE, "foo", "bar");

            create_table(&client, table_name).await;

            // Seed one row
            client
                .put_item()
                .table_name(table_name)
                .item("id", AttributeValue::S("seed".to_string()))
                .item("name", AttributeValue::S("Seed".to_string()))
                .send()
                .await?;

            let rt = setup_runtime(table_name, PORT_DML_INSERT_DELETE).await?;

            // Insert rows via SQL
            run_query(
                &rt,
                &format!(
                    "INSERT INTO {table_name} (id, name) VALUES ('a', 'Alpha'), ('b', 'Beta'), ('c', 'Charlie')"
                ),
            )
            .await?;

            let result = wait_for_query_contents(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                &["Alpha", "Beta", "Charlie", "Seed"],
                &[],
            )
            .await?;
            assert!(result.contains("Alpha"));
            assert!(result.contains("Beta"));
            assert!(result.contains("Charlie"));
            assert!(result.contains("Seed"));

            // Delete two of the inserted rows
            run_query(
                &rt,
                &format!("DELETE FROM {table_name} WHERE id = 'a' OR id = 'b'"),
            )
            .await?;

            let result = wait_for_query_contents(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                &["Charlie", "Seed"],
                &["Alpha", "Beta"],
            )
            .await?;
            assert!(!result.contains("Alpha"), "Alpha should be deleted");
            assert!(!result.contains("Beta"), "Beta should be deleted");
            assert!(result.contains("Charlie"), "Charlie should remain");
            assert!(result.contains("Seed"), "Seed should remain");

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_dml_insert_multiple_types() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "dml_types_test";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT_DML_TYPES).await?;
            let client = get_client(PORT_DML_TYPES, "foo", "bar");

            create_table(&client, table_name).await;

            // Seed a row with multiple types so schema inference picks them up
            client
                .put_item()
                .table_name(table_name)
                .item("id", AttributeValue::S("seed".to_string()))
                .item("int_val", AttributeValue::N("100".to_string()))
                .item("float_val", AttributeValue::N("1.5".to_string()))
                .item("bool_val", AttributeValue::Bool(false))
                .item("name", AttributeValue::S("Seed Row".to_string()))
                .send()
                .await?;

            let rt = setup_runtime(table_name, PORT_DML_TYPES).await?;

            // Verify seed data
            let result = run_query(&rt, &format!("SELECT * FROM {table_name} ORDER BY id")).await?;
            assert!(result.contains("seed"), "Should contain seed row");

            // Insert row with multiple types via SQL
            run_query(
                &rt,
                &format!(
                    "INSERT INTO {table_name} (id, int_val, float_val, bool_val, name) \
                     VALUES ('new', 42, 3.14, true, 'Test Row')"
                ),
            )
            .await?;

            // Verify roundtrip
            let result = wait_for_query_contents(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                &["new", "42", "3.14", "true", "Test Row", "seed"],
                &[],
            )
            .await?;

            assert!(result.contains("new"), "Should contain new row");
            assert!(result.contains("42"), "Should contain int_val 42");
            assert!(result.contains("3.14"), "Should contain float_val 3.14");
            assert!(result.contains("true"), "Should contain bool_val true");
            assert!(result.contains("Test Row"), "Should contain name");
            assert!(result.contains("seed"), "Should still contain seed row");

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_dml_delete_composite_key_tuple_in() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "dml_composite_delete_test";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_DML_COMPOSITE_DELETE).await?;
            let client = get_client(PORT_DML_COMPOSITE_DELETE, "foo", "bar");

            create_composite_key_table(&client, table_name).await;

            // Seed 4 rows with composite keys
            for (pk, sk, val) in [
                ("a", "1", "alpha-one"),
                ("a", "2", "alpha-two"),
                ("b", "1", "beta-one"),
                ("b", "2", "beta-two"),
            ] {
                client
                    .put_item()
                    .table_name(table_name)
                    .item("pk", AttributeValue::S(pk.to_string()))
                    .item("sk", AttributeValue::S(sk.to_string()))
                    .item("val", AttributeValue::S(val.to_string()))
                    .send()
                    .await?;
            }

            let rt = setup_runtime(table_name, PORT_DML_COMPOSITE_DELETE).await?;

            // Verify all 4 rows exist
            let result =
                run_query(&rt, &format!("SELECT * FROM {table_name} ORDER BY pk, sk")).await?;
            assert!(result.contains("alpha-one"));
            assert!(result.contains("alpha-two"));
            assert!(result.contains("beta-one"));
            assert!(result.contains("beta-two"));

            // Delete 1 row using single-tuple IN list (DataFusion optimizes this to struct = struct)
            run_query(
                &rt,
                &format!("DELETE FROM {table_name} WHERE (pk, sk) IN (('a', '1'))"),
            )
            .await?;

            let result = wait_for_query_contents(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY pk, sk"),
                &["alpha-two", "beta-one", "beta-two"],
                &["alpha-one"],
            )
            .await?;
            assert!(
                !result.contains("alpha-one"),
                "alpha-one should be deleted by single-tuple IN"
            );
            assert!(result.contains("alpha-two"), "alpha-two should remain");
            assert!(result.contains("beta-one"), "beta-one should remain");
            assert!(result.contains("beta-two"), "beta-two should remain");

            // Delete 2 rows using multi-tuple IN list on composite key
            run_query(
                &rt,
                &format!("DELETE FROM {table_name} WHERE (pk, sk) IN (('a', '2'), ('b', '1'))"),
            )
            .await?;

            let result = wait_for_query_contents(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY pk, sk"),
                &["beta-two"],
                &["alpha-two", "beta-one"],
            )
            .await?;
            assert!(
                !result.contains("alpha-two"),
                "alpha-two should be deleted by multi-tuple IN"
            );
            assert!(
                !result.contains("beta-one"),
                "beta-one should be deleted by multi-tuple IN"
            );
            assert!(result.contains("beta-two"), "beta-two should remain");

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}
