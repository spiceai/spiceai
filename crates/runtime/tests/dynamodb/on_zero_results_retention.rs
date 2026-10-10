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

//! `on_zero_results: use_source` against a source that rejects filters it did
//! not accept for pushdown.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::display::array_value_to_string;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, ZeroResultsAction};
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params as DatasetParams;

use super::pushdown_roundtrip::create_table;
use super::streams::{get_client, start_dynamodb_docker_container};
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

const TABLE: &str = "on_zero_results_retention";

async fn put(client: &Client, pk: &str, sk: &str, name: &str) -> Result<(), anyhow::Error> {
    client
        .put_item()
        .table_name(TABLE)
        .item("pk", AttributeValue::S(pk.to_string()))
        .item("sk", AttributeValue::S(sk.to_string()))
        .item("name", AttributeValue::S(name.to_string()))
        .send()
        .await?;
    Ok(())
}

/// An Arrow acceleration of the table that applies `retention_sql` as rows are
/// written. `DynamoDB` cannot evaluate `upper()`, so its keep predicate is one the
/// source does not accept for pushdown.
fn dataset(port: u16) -> Dataset {
    let mut dataset = Dataset::new(format!("dynamodb:{TABLE}"), "retained".to_string());
    dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
        ("dynamodb_aws_access_key_id".to_string(), "fake".to_string()),
        (
            "dynamodb_aws_secret_access_key".to_string(),
            "fake".to_string(),
        ),
        ("dynamodb_aws_region".to_string(), "us-east-1".to_string()),
        ("dynamodb_aws_auth".to_string(), "key".to_string()),
        (
            "endpoint_url".to_string(),
            format!("http://localhost:{port}"),
        ),
    ])));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        on_zero_results: ZeroResultsAction::UseSource,
        retention_sql: Some("DELETE FROM retained WHERE upper(name) = 'DROP'".to_string()),
        retention_check_enabled: false,
        retention_check_interval: None,
        ..Acceleration::default()
    });
    dataset
}

/// The `(pk, sk, name)` rows of `batches`, in key order.
fn rows(batches: &[RecordBatch]) -> Result<Vec<(String, String, String)>, anyhow::Error> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .with_context(|| format!("the result has a `{name}` column"))
        };
        let (pk, sk, name) = (column("pk")?, column("sk")?, column("name")?);
        for row in 0..batch.num_rows() {
            rows.push((
                array_value_to_string(pk, row)?,
                array_value_to_string(sk, row)?,
                array_value_to_string(name, row)?,
            ));
        }
    }
    rows.sort();
    Ok(rows)
}

fn row(pk: &str, sk: &str, name: &str) -> (String, String, String) {
    (pk.to_string(), sk.to_string(), name.to_string())
}

/// A retention predicate `DynamoDB` cannot translate still applies on fallback:
/// the source is asked only for the filters it accepted, and the rest run
/// locally on what it returns.
#[tokio::test]
async fn dynamodb_fallback_applies_a_retention_predicate_the_source_cannot_evaluate()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let container = start_dynamodb_docker_container().await?;
            let port = container.host_port(8000)?;
            let client = get_client(port, "fake", "fake");
            create_table(&client, TABLE).await?;
            put(&client, "a", "1", "keep").await?;
            put(&client, "a", "2", "drop").await?;

            let app = AppBuilder::new("dynamodb_on_zero_results_retention")
                .with_dataset(dataset(port))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // Retention dropped `drop` as the load was written, so the
            // acceleration answers this from the kept row alone.
            let loaded = run_query(&rt, "SELECT pk, sk, name FROM retained WHERE pk = 'a'").await?;
            assert_eq!(rows(&loaded)?, vec![row("a", "1", "keep")]);

            // Written after the load, so only the source has them.
            put(&client, "b", "1", "late").await?;
            put(&client, "b", "2", "drop").await?;

            let fallback =
                run_query(&rt, "SELECT pk, sk, name FROM retained WHERE pk = 'b'").await?;
            assert_eq!(
                rows(&fallback)?,
                vec![row("b", "1", "late")],
                "fallback must return the new row retention keeps, and not the one it would delete"
            );

            let evicted = run_query(
                &rt,
                "SELECT pk, sk, name FROM retained WHERE pk = 'a' AND sk = '2'",
            )
            .await?;
            assert_eq!(
                rows(&evicted)?,
                Vec::<(String, String, String)>::new(),
                "a row retention dropped at load must not come back from the source"
            );

            rt.shutdown().await;
            container
                .remove()
                .await
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            Ok(())
        })
        .await
}
