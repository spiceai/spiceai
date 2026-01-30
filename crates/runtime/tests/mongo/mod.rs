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
use std::time::SystemTime;

use common::{get_mongodb_client, make_mongodb_dataset, start_mongodb_docker_container};
use mongodb::{Collection, bson::doc};

use chrono::{DateTime, Utc};
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::init_tracing;
use crate::utils::{register_test_connectors, test_request_context};

pub mod common;

use super::*;
use app::AppBuilder;
use runtime::Runtime;
use tracing::instrument;

const MONGODB_PORT1: u16 = 27019;

#[instrument]
async fn init_mongodb_db(port: u16) -> Result<(), anyhow::Error> {
    tracing::debug!("INIT DB: test");
    let client = get_mongodb_client(port).await?;
    let database = client.database("testdb");

    tracing::debug!("DROP COLLECTION test");
    let _ = database
        .collection::<mongodb::bson::Document>("test")
        .drop()
        .await;

    let collection: Collection<mongodb::bson::Document> = database.collection("test");

    let ts = DateTime::parse_from_rfc3339("2019-01-01T00:00:00Z")?.with_timezone(&Utc);

    // Insert test documents
    let test_docs = vec![
        doc! {
            "_id": 1,
            "col_bit": true,
            "col_tiny": 1i32,
            "col_short": 1i32,
            "col_long": 1i64,
            "col_longlong": 1i64,
            "col_float": 1.1f64,
            "col_double": 1.1f64,
            "col_timestamp": mongodb::bson::DateTime::from(SystemTime::from(ts)),
            "col_date": mongodb::bson::DateTime::from(SystemTime::from(ts)),
            "col_time": "12:34:56",
            "col_blob": mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: b"blob".to_vec(),
            },
            "col_string": "string 🚀😊",
            "col_decimal": 1.11f64,
            "col_unsigned_int": 10u32,
            "col_char": "USA",
            "col_set": ["apple", "banana"],
            "col_json": doc! {
                "name": "John",
                "age": 30,
                "is_active": true,
                "balance": 1234.56
            }
        },
        doc! {
            "_id": 2,
            "col_bit": null,
            "col_tiny": null,
            "col_short": null,
            "col_long": null,
            "col_longlong": null,
            "col_float": null,
            "col_double": null,
            "col_timestamp": null,
            "col_date": null,
            "col_time": null,
            "col_blob": null,
            "col_string": null,
            "col_decimal": null,
            "col_unsigned_int": null,
            "col_char": null,
            "col_set": null,
            "col_json": null
        },
    ];

    collection.insert_many(test_docs).await?;
    Ok(())
}

#[tokio::test]
async fn mongodb_integration_test() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(MONGODB_PORT1)
                .await
                .map_err(|e| {
                    tracing::error!("start_mongodb_docker_container: {e}");
                    e.to_string()
                })?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mongodb_db(MONGODB_PORT1).await.map_err(|e| {
                    tracing::error!("Failed transiently  to initialize MongoDB database: {e}");
                    RetryError::transient(e)
                })
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MongoDB database: {e}");
                e.to_string()
            })?;
            let app = AppBuilder::new("mongodb_integration_test")
                .with_dataset(make_mongodb_dataset("test", "test", MONGODB_PORT1, false))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            let queries: QueryTests = vec![(
                "SELECT * FROM test",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 18, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 2, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("MongoDB Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!("mongodb_integration_test", results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("mongodb_integration_test_{snapshot_suffix}"),
                    query,
                    false, // can't snapshot this plan
                    validate_result,
                )
                .await?;
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}
