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
use futures::TryStreamExt;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

use runtime::Runtime;
use spicepod::{component::catalog::Catalog, param::Params};

#[expect(clippy::expect_used)]
fn get_params() -> Params {
    let warehouse =
        std::env::var("SNOWFLAKE_WAREHOUSE").unwrap_or_else(|_| "COMPUTE_WH".to_string());
    let role = std::env::var("SNOWFLAKE_ROLE").unwrap_or_else(|_| "accountadmin".to_string());
    let _ = std::env::var("SNOWFLAKE_ACCOUNT").expect("SNOWFLAKE_ACCOUNT is not set");
    let _ = std::env::var("SNOWFLAKE_USERNAME").expect("SNOWFLAKE_USERNAME is not set");
    let _ = std::env::var("SNOWFLAKE_PASSWORD").expect("SNOWFLAKE_PASSWORD is not set");

    Params::from_string_map(
        vec![
            ("snowflake_warehouse".to_string(), warehouse),
            ("snowflake_role".to_string(), role),
            (
                "snowflake_account".to_string(),
                "${ env:SNOWFLAKE_ACCOUNT }".to_string(),
            ),
            (
                "snowflake_username".to_string(),
                "${ env:SNOWFLAKE_USERNAME }".to_string(),
            ),
            (
                "snowflake_password".to_string(),
                "${ env:SNOWFLAKE_PASSWORD }".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    )
}

/// Test that the Snowflake catalog connector discovers schemas and tables
/// from a Snowflake database and registers them in `information_schema`.
#[tokio::test]
async fn snowflake_catalog_discovery_test() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let mut catalog = Catalog::new(
                "snowflake:SNOWFLAKE_SAMPLE_DATA".to_string(),
                "sf".to_string(),
            );
            catalog.include = vec!["TPCH_SF1.*".to_string()];
            catalog.params = Some(get_params());

            let app = AppBuilder::new("snowflake_catalog_discovery_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for catalog to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify TPCH_SF1 tables appear under the sf catalog
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT COUNT(*) as cnt FROM information_schema.tables \
                     WHERE table_catalog = 'sf'",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let table_count: i64 = result
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .map(|a| a.value(0))
                })
                .unwrap_or(0);
            assert!(
                table_count > 0,
                "Expected TPCH_SF1 tables to be registered under sf catalog, found {table_count}"
            );

            Ok(())
        })
        .await
}

/// Test that the Snowflake catalog include filter correctly limits which
/// schemas are registered. Only included schemas should appear.
#[tokio::test]
async fn snowflake_catalog_include_filter_test() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Only include TPCH_SF1 — TPCH_SF10 and others should not appear
            let mut catalog = Catalog::new(
                "snowflake:SNOWFLAKE_SAMPLE_DATA".to_string(),
                "sf".to_string(),
            );
            catalog.include = vec!["TPCH_SF1.*".to_string()];
            catalog.params = Some(get_params());

            let app = AppBuilder::new("snowflake_catalog_include_filter_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for catalog to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // TPCH_SF1 tables should be present
            let sf1_result = rt
                .datafusion()
                .query_builder(
                    "SELECT COUNT(*) as cnt FROM information_schema.tables \
                     WHERE table_catalog = 'sf' AND table_schema = 'TPCH_SF1'",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let sf1_count: i64 = sf1_result
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .map(|a| a.value(0))
                })
                .unwrap_or(0);
            assert!(
                sf1_count > 0,
                "Expected TPCH_SF1 tables to be registered, found {sf1_count}"
            );

            // TPCH_SF10 should NOT be present (excluded by include filter)
            let excluded_result = rt
                .datafusion()
                .query_builder(
                    "SELECT COUNT(*) as cnt FROM information_schema.tables \
                     WHERE table_catalog = 'sf' AND table_schema = 'TPCH_SF10'",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let excluded_count: i64 = excluded_result
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .map(|a| a.value(0))
                })
                .unwrap_or(0);
            assert_eq!(
                excluded_count, 0,
                "Expected TPCH_SF10 tables to be excluded by include filter, found {excluded_count}"
            );

            Ok(())
        })
        .await
}

/// Test that schema inference works correctly for a table discovered via
/// the Snowflake catalog connector.
#[tokio::test]
async fn snowflake_catalog_schema_inference_test() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let mut catalog = Catalog::new(
                "snowflake:SNOWFLAKE_SAMPLE_DATA".to_string(),
                "sf".to_string(),
            );
            catalog.include = vec!["TPCH_SF1.*".to_string()];
            catalog.params = Some(get_params());

            let app = AppBuilder::new("snowflake_catalog_schema_inference_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for catalog to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify LINEITEM schema has all 16 columns
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_catalog = 'sf' AND table_schema = 'TPCH_SF1' \
                     AND table_name = 'LINEITEM' \
                     ORDER BY ordinal_position",
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
                total_columns, 16,
                "Expected 16 columns in TPCH LINEITEM via catalog, got {total_columns}"
            );

            Ok(())
        })
        .await
}
