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

use std::collections::HashMap;
use std::sync::Arc;

use app::AppBuilder;
use futures::StreamExt;

use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::BehaviorVersion;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use rand::Rng;
use std::env;

pub async fn get_dynamodb_client() -> Result<aws_sdk_dynamodb::Client, anyhow::Error> {
    let Ok(dynamodb_access_key_id) = env::var("AWS_DYNAMODB_KEY") else {
        panic!("AWS_DYNAMODB_KEY not set")
    };

    let Ok(dynamodb_secret_access_key) = env::var("AWS_DYNAMODB_SECRET") else {
        panic!("AWS_DYNAMODB_SECRET not set")
    };

    let credentials = Credentials::new(
        dynamodb_access_key_id,
        dynamodb_secret_access_key,
        None,
        None,
        "dynamodb",
    );

    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("ap-northeast-2"))
        .credentials_provider(credentials)
        .build();

    let client = aws_sdk_dynamodb::Client::from_conf(config);

    Ok(client)
}

async fn init_test_table(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
) -> Result<(), anyhow::Error> {
    tracing::info!("Initializing test table: {}", table_name);

    let _ = client.delete_table().table_name(table_name).send().await;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    client
        .create_table()
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("version")
                .key_type(KeyType::Range)
                .build()?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::N)
                .build()?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("version")
                .attribute_type(ScalarAttributeType::N)
                .build()?,
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await?;

    // Wait for table to be active
    tracing::info!("Waiting for table to become active...");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // Insert test items with comprehensive type coverage

    // Item 1: All types with values
    let mut item1 = HashMap::new();
    item1.insert("id".to_string(), AttributeValue::N("1".to_string()));
    item1.insert("version".to_string(), AttributeValue::N("2".to_string()));
    item1.insert("col_bool".to_string(), AttributeValue::Bool(true));
    item1.insert(
        "col_string".to_string(),
        AttributeValue::S("string 🚀😊".to_string()),
    );
    item1.insert(
        "col_number_int".to_string(),
        AttributeValue::N("42".to_string()),
    );
    item1.insert(
        "col_number_float".to_string(),
        AttributeValue::N("3.14159".to_string()),
    );
    item1.insert(
        "col_number_scientific".to_string(),
        AttributeValue::N("1.23e10".to_string()),
    );
    item1.insert(
        "col_binary".to_string(),
        AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(b"blob")),
    );
    item1.insert(
        "col_string_set".to_string(),
        AttributeValue::Ss(vec!["apple".to_string(), "banana".to_string()]),
    );
    item1.insert(
        "col_number_set_int".to_string(),
        AttributeValue::Ns(vec!["1".to_string(), "2".to_string(), "3".to_string()]),
    );
    item1.insert(
        "col_number_set_float".to_string(),
        AttributeValue::Ns(vec![
            "1.1".to_string(),
            "2.2".to_string(),
            "3.3".to_string(),
        ]),
    );
    item1.insert(
        "col_binary_set".to_string(),
        AttributeValue::Bs(vec![
            aws_sdk_dynamodb::primitives::Blob::new(b"data1"),
            aws_sdk_dynamodb::primitives::Blob::new(b"data2"),
        ]),
    );

    // Heterogeneous list
    item1.insert(
        "col_list".to_string(),
        AttributeValue::L(vec![
            AttributeValue::N("1".to_string()),
            AttributeValue::S("foo".to_string()),
            AttributeValue::Bool(true),
        ]),
    );

    // Map (nested object)
    let mut map = HashMap::new();
    map.insert("name".to_string(), AttributeValue::S("John".to_string()));
    map.insert("age".to_string(), AttributeValue::N("30".to_string()));
    map.insert("is_active".to_string(), AttributeValue::Bool(true));
    map.insert(
        "balance".to_string(),
        AttributeValue::N("1234.56".to_string()),
    );
    item1.insert("col_map".to_string(), AttributeValue::M(map));

    // Temporal types (stored as strings)
    item1.insert(
        "col_timestamp".to_string(),
        AttributeValue::S("2019-01-01T00:00:00Z".to_string()),
    );
    item1.insert(
        "col_date".to_string(),
        AttributeValue::S("2019-01-01".to_string()),
    );
    item1.insert(
        "col_time".to_string(),
        AttributeValue::S("12:34:56".to_string()),
    );

    client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item1))
        .send()
        .await?;

    // Item 2: All nulls
    let mut item2 = HashMap::new();
    item2.insert("id".to_string(), AttributeValue::N("2".to_string()));
    item2.insert("version".to_string(), AttributeValue::N("2".to_string()));
    item2.insert("col_bool".to_string(), AttributeValue::Null(true));
    item2.insert("col_string".to_string(), AttributeValue::Null(true));
    item2.insert("col_number_int".to_string(), AttributeValue::Null(true));
    item2.insert("col_number_float".to_string(), AttributeValue::Null(true));
    item2.insert(
        "col_number_scientific".to_string(),
        AttributeValue::Null(true),
    );
    item2.insert("col_binary".to_string(), AttributeValue::Null(true));
    item2.insert("col_string_set".to_string(), AttributeValue::Null(true));
    item2.insert("col_number_set_int".to_string(), AttributeValue::Null(true));
    item2.insert(
        "col_number_set_float".to_string(),
        AttributeValue::Null(true),
    );
    item2.insert("col_binary_set".to_string(), AttributeValue::Null(true));
    item2.insert("col_list".to_string(), AttributeValue::Null(true));
    item2.insert("col_map".to_string(), AttributeValue::Null(true));
    item2.insert("col_timestamp".to_string(), AttributeValue::Null(true));
    item2.insert("col_date".to_string(), AttributeValue::Null(true));
    item2.insert("col_time".to_string(), AttributeValue::Null(true));

    client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item2))
        .send()
        .await?;

    tracing::info!("Test data inserted successfully");
    Ok(())
}

async fn cleanup_test_table(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
) -> Result<(), anyhow::Error> {
    tracing::info!("Cleaning up test table: {}", table_name);
    client.delete_table().table_name(table_name).send().await?;
    Ok(())
}

#[tokio::test]
async fn dynamodb_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let table_name = "spice_integration_test";
    let client = get_dynamodb_client().await?;

    let table_name = format!(
        "spice_integration_test_{}",
        rand::rng().random_range(1000..=9999)
    );
    init_test_table(&client, &table_name).await?;

    let test_result = test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{}", table_name),
                    "test_dynamodb",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Test 1: Retrieve schema information
            // Note: Column order may vary for DynamoDB, so we order by column_name
            run_and_snapshot_query(
                &rt,
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = 'public' \
                   AND table_name = 'test_dynamodb' \
                 ORDER BY column_name;",
                "dynamodb_federated_schema",
            )
            .await?;

            // Test 2: Full table scan with ordering
            run_and_snapshot_query(
                &rt,
                "SELECT * FROM test_dynamodb ORDER BY id;",
                "dynamodb_federated_full_scan",
            )
            .await?;

            // Test 3: Filtered query
            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int, col_bool \
                 FROM test_dynamodb \
                 WHERE id = 1;",
                "dynamodb_federated_filtered",
            )
            .await?;

            // Test 8: Query with filter
            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int, col_bool \
                 FROM test_dynamodb \
                 WHERE id = 1 and version > '0';",
                "dynamodb_federated_query_filtered",
            )
            .await?;

            // Test 4: Aggregation
            run_and_snapshot_query(
                &rt,
                "SELECT COUNT(*) as total_count FROM test_dynamodb;",
                "dynamodb_federated_count",
            )
            .await?;

            // Test 5: Test null handling
            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int \
                 FROM test_dynamodb \
                 WHERE col_string IS NULL;",
                "dynamodb_federated_nulls",
            )
            .await?;

            // Test 5: Test non-null handling
            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int \
                 FROM test_dynamodb \
                 WHERE col_string IS NOT NULL;",
                "dynamodb_federated_not_nulls",
            )
            .await?;

            // Test 6: Test temporal types
            run_and_snapshot_query(
                &rt,
                "SELECT id, col_timestamp, col_date, col_time \
                 FROM test_dynamodb \
                 WHERE id = 1;",
                "dynamodb_federated_temporal",
            )
            .await?;

            // Test 7: Test collections (sets and lists)
            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string_set, col_number_set_int, col_list \
                 FROM test_dynamodb \
                 WHERE id = 1;",
                "dynamodb_federated_collections",
            )
            .await?;

            Ok(())
        })
        .await;

    cleanup_test_table(&client, &table_name).await?;

    test_result
}

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    test_name: &str,
) -> Result<(), anyhow::Error> {
    let mut query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let mut batches = vec![];
    while let Some(batch) = query_result.data.next().await {
        batches.push(batch?);
    }

    let formatted = arrow::util::pretty::pretty_format_batches(&batches)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(test_name, formatted);
    Ok(())
}

fn get_test_dataset(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(Params::from_string_map(
        vec![
            (
                "dynamodb_aws_region".to_string(),
                "ap-northeast-2".to_string(),
            ),
            (
                "dynamodb_aws_access_key_id".to_string(),
                "${ env:AWS_DYNAMODB_KEY }".to_string(),
            ),
            (
                "dynamodb_aws_secret_access_key".to_string(),
                "${ env:AWS_DYNAMODB_SECRET }".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}
