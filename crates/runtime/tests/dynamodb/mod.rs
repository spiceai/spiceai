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
use aws_sdk_credential_bridge::default_aws_config;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use std::env;

const TABLE_NAME: &str = "spice_integration_test_v2";

#[tokio::test]
async fn dynamodb_schema() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    // init_test_table(TABLE_NAME).await?;

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = 'public' \
                   AND table_name = 'test_dynamodb' \
                 ORDER BY column_name;",
                "schema",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_scan_no_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(&rt, "SELECT * FROM test_dynamodb ORDER BY id;", "full_scan")
                .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_query_no_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int, col_bool \
                 FROM test_dynamodb \
                 WHERE id = 1;",
                "query_no_filter",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_query_with_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int, col_bool \
                 FROM test_dynamodb \
                 WHERE id = 1 and version > '0';",
                "query_with_filter",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_aggregation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT COUNT(*) as total_count, MAX(col_timestamp) as max_timestamp, MAX(col_timestamp_tz) as max_timestamp_tz FROM test_dynamodb;",
                "aggregation",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_nulls() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int \
                 FROM test_dynamodb \
                 WHERE col_string IS NULL;",
                "nulls",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_not_nulls() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT id, col_string, col_number_int \
                 FROM test_dynamodb \
                 WHERE col_string IS NOT NULL;",
                "not_nulls",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_temporal() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT id, col_timestamp, col_timestamp_tz, col_date, col_time \
                 FROM test_dynamodb \
                 WHERE id = 1;",
                "temporal",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_timestamp_filter_pushdown() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "explain SELECT * \
                 FROM test_dynamodb \
                 WHERE col_timestamp_tz > '2024-12-01 12:34:56.123456789Z' and col_timestamp <= '2024-12-01 12:34:56.123456789Z';",
                "timestamp_filter_pushdown",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_collections() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT id, array_sort(col_string_set), array_sort(col_number_set_int), array_sort(col_list) \
                 FROM test_dynamodb \
                 WHERE id = 1;",
                "collections",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_timestamp_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                "SELECT id, col_timestamp, col_date \
                 FROM test_dynamodb \
                 WHERE col_timestamp > '2010-01-01';",
                "timestamp_filter",
            )
            .await?;

            Ok(())
        })
        .await
}
#[tokio::test]
async fn dynamodb_nested_projection_no_nested_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                r#"SELECT id, "col_map_fully_unnested.age", "col_map_fully_unnested.balance", "col_map_fully_unnested.is_active", "col_map_fully_unnested.name", "col_map_partially_unnested.foo", "col_map_partially_unnested.nested_lvl_1"
                 FROM test_dynamodb
                 "#,
                "nested_projection_no_nested_filter",
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn dynamodb_nested_projection_with_nested_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    &format!("dynamodb:{TABLE_NAME}"),
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

            run_and_snapshot_query(
                &rt,
                r#"SELECT id, "col_map_fully_unnested.age", "col_map_fully_unnested.balance", "col_map_fully_unnested.is_active", "col_map_fully_unnested.name", "col_map_partially_unnested.foo", "col_map_partially_unnested.nested_lvl_1"
                 FROM test_dynamodb
                 WHERE "col_map_fully_unnested.age" = 30
                 "#,
                "nested_projection_with_nested_filter",
            )
                .await?;

            Ok(())
        })
        .await
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
            ("unnest_depth".to_string(), "1".to_string()),
            (
                "time_format".to_string(),
                "2006-01-02T15:04:05.000Z07:00".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

#[allow(clippy::missing_panics_doc)]
#[allow(clippy::missing_errors_doc)]
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

    let config = default_aws_config()
        .region(Region::new("ap-northeast-2"))
        .credentials_provider(credentials)
        .load()
        .await;

    let client = aws_sdk_dynamodb::Client::new(&config);

    Ok(client)
}

#[allow(clippy::too_many_lines)]
#[allow(dead_code)]
async fn init_test_table(table_name: &str) -> Result<(), anyhow::Error> {
    let client = get_dynamodb_client().await?;

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
    let mut fully_unnested_map = HashMap::new();
    fully_unnested_map.insert("name".to_string(), AttributeValue::S("John".to_string()));
    fully_unnested_map.insert("age".to_string(), AttributeValue::N("30".to_string()));
    fully_unnested_map.insert("is_active".to_string(), AttributeValue::Bool(true));
    fully_unnested_map.insert(
        "balance".to_string(),
        AttributeValue::N("1234.56".to_string()),
    );

    // Map (nested object)
    let mut partially_unnested_map = HashMap::new();
    let mut nested_lvl_1 = HashMap::new();
    nested_lvl_1.insert("foo".to_string(), AttributeValue::S("baz".to_string()));
    partially_unnested_map.insert("nested_lvl_1".to_string(), AttributeValue::M(nested_lvl_1));
    partially_unnested_map.insert("foo".to_string(), AttributeValue::S("bar".to_string()));
    item1.insert(
        "col_map_fully_unnested".to_string(),
        AttributeValue::M(fully_unnested_map),
    );
    item1.insert(
        "col_map_partially_unnested".to_string(),
        AttributeValue::M(partially_unnested_map),
    );

    // Temporal types (stored as strings)
    item1.insert(
        "col_timestamp".to_string(),
        AttributeValue::S("2019-01-01T00:00:00.123Z".to_string()),
    );
    item1.insert(
        "col_timestamp_tz".to_string(),
        AttributeValue::S("2019-01-01T00:00:00.456+05:00".to_string()),
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
    item2.insert(
        "col_map_fully_unnested".to_string(),
        AttributeValue::M(HashMap::new()),
    );
    item2.insert(
        "col_map_partially_unnested".to_string(),
        AttributeValue::M(HashMap::new()),
    );

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
