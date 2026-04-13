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

use std::sync::Arc;

use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("databricks:{path}"), name.to_string());
    dataset.params = Some(get_params());
    dataset
}

#[expect(clippy::expect_used)]
fn get_params() -> Params {
    let _ = std::env::var("TEST_DATABRICKS_HOST").expect("TEST_DATABRICKS_HOST is not set");
    let _ = std::env::var("TEST_DATABRICKS_TOKEN").expect("TEST_DATABRICKS_TOKEN is not set");
    let _ = std::env::var("TEST_DATABRICKS_SQL_WAREHOUSE_ID")
        .expect("TEST_DATABRICKS_SQL_WAREHOUSE_ID is not set");

    Params::from_string_map(
        vec![
            (
                "databricks_endpoint".to_string(),
                "${ env:TEST_DATABRICKS_HOST }".to_string(),
            ),
            (
                "databricks_token".to_string(),
                "${ env:TEST_DATABRICKS_TOKEN }".to_string(),
            ),
            (
                "databricks_sql_warehouse_id".to_string(),
                "${ env:TEST_DATABRICKS_SQL_WAREHOUSE_ID }".to_string(),
            ),
            ("client_timeout".to_string(), "120s".to_string()),
            ("mode".to_string(), "sql_warehouse".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}

/// Test querying a MANAGED table through the SQL Warehouse connector.
/// MANAGED tables are the default UC table type backed by Delta storage.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_sql_warehouse_managed_table_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_sql_warehouse_managed_test")
                .with_dataset(make_dataset("spiceai_sandbox.tpch.lineitem", "lineitem"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder("SELECT * FROM lineitem LIMIT 5")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let total_rows: usize = result
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(total_rows, 5, "Expected 5 rows from MANAGED table");
            for batch in &result {
                assert_eq!(
                    batch.num_columns(),
                    16,
                    "Expected 16 columns in TPCH lineitem"
                );
            }

            Ok(())
        })
        .await
}

/// Test schema inference for a dataset loaded via SQL Warehouse.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_sql_warehouse_schema_inference_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_sql_warehouse_schema_test")
                .with_dataset(make_dataset("spiceai_sandbox.tpch.nation", "nation"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // TPCH nation table has 4 columns
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_name = 'nation' ORDER BY ordinal_position",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let total_columns: usize = result
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(
                total_columns, 4,
                "Expected 4 columns in TPCH nation schema, got {total_columns}"
            );

            Ok(())
        })
        .await
}

/// Test that a dataset registered via SQL Warehouse appears in `information_schema.tables`.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_sql_warehouse_dataset_registration_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_sql_warehouse_registration_test")
                .with_dataset(make_dataset("spiceai_sandbox.tpch.nation", "nation"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_name = 'nation'",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let expected = [
                "+------------+",
                "| table_name |",
                "+------------+",
                "| nation     |",
                "+------------+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test querying an EXTERNAL table through the SQL Warehouse connector.
/// EXTERNAL tables are UC tables backed by external storage locations.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_sql_warehouse_external_table_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_sql_warehouse_external_test")
                .with_dataset(make_dataset(
                    "spiceai_sandbox.integration.external_table",
                    "external_table",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder("SELECT * FROM external_table LIMIT 5")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let total_rows: usize = result
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum();
            assert!(
                total_rows <= 5,
                "Expected at most 5 rows from EXTERNAL table"
            );

            Ok(())
        })
        .await
}

/// Test querying a FOREIGN table (e.g., Lakehouse Federation) through the
/// SQL Warehouse connector. FOREIGN tables skip strict UC permission
/// prechecks because Databricks validates access at query time.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_sql_warehouse_foreign_table_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_sql_warehouse_foreign_test")
                .with_dataset(make_dataset(
                    "spiceai_sandbox.integration.foreign_table",
                    "foreign_table",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder("SELECT * FROM foreign_table LIMIT 5")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let total_rows: usize = result
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum();
            assert!(
                total_rows <= 5,
                "Expected at most 5 rows from FOREIGN table"
            );

            Ok(())
        })
        .await
}

/// Test querying a `MATERIALIZED_VIEW` through the SQL Warehouse connector.
/// Materialized views are pre-computed result sets maintained by Databricks.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_sql_warehouse_materialized_view_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_sql_warehouse_mv_test")
                .with_dataset(make_dataset(
                    "spiceai_sandbox.integration.materialized_view_table",
                    "mv_table",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder("SELECT * FROM mv_table LIMIT 5")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let total_rows: usize = result
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum();
            assert!(
                total_rows <= 5,
                "Expected at most 5 rows from MATERIALIZED_VIEW"
            );

            Ok(())
        })
        .await
}
