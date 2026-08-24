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
use std::time::{Duration, Instant};

use app::AppBuilder;

use crate::{
    ValidateFn, configure_test_datafusion, init_tracing, run_query_and_check_results,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};
use futures::TryStreamExt;

use runtime::Runtime;
use spicepod::{component::catalog::Catalog, param::Params};

/// Cold Spark Connect clusters can take several minutes to become Ready in CI.
const LOAD_TIMEOUT: Duration = Duration::from_mins(10);
const CATALOG_RETRY_DELAY: Duration = Duration::from_secs(10);

fn make_catalog(name: &str) -> Catalog {
    let mut catalog = Catalog::new("databricks:spiceai_sandbox".to_string(), name.to_string());
    catalog.include = vec!["tpch.*".to_string()];
    catalog.params = Some(get_params());
    catalog
}

#[expect(clippy::expect_used)]
fn get_params() -> Params {
    // Verify that the environment variables are set
    let _ = std::env::var("TEST_DATABRICKS_HOST").expect("TEST_DATABRICKS_HOST is not set");
    let _ = std::env::var("TEST_DATABRICKS_TOKEN").expect("TEST_DATABRICKS_TOKEN is not set");
    let _ =
        std::env::var("TEST_DATABRICKS_CLUSTER_ID").expect("TEST_DATABRICKS_CLUSTER_ID is not set");

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
                "databricks_cluster_id".to_string(),
                "${ env:TEST_DATABRICKS_CLUSTER_ID }".to_string(),
            ),
            ("mode".to_string(), "spark_connect".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}

async fn catalog_table_count(rt: &Runtime, catalog: &str) -> Result<i64, anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder(&format!(
            "SELECT COUNT(*) as cnt FROM information_schema.tables \
             WHERE table_catalog = '{catalog}'"
        ))
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .data
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(result
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .map(|a| a.value(0))
        })
        .unwrap_or(0))
}

/// Loads components and retries while the Spark cluster is still Pending.
///
/// Unity Catalog registration soft-fails individual Spark Connect table providers when the
/// cluster is Pending, so `load_components` can finish quickly with an empty catalog. Keep
/// rebuilding until tables appear or [`LOAD_TIMEOUT`] elapses (same overall wait budget as
/// `databricks_spark_m2m`).
async fn load_runtime_waiting_for_catalog(app_name: &str) -> Result<Runtime, anyhow::Error> {
    let start = Instant::now();

    loop {
        let remaining = LOAD_TIMEOUT.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            return Err(anyhow::anyhow!(
                "Timed out waiting for Spark Connect cluster/catalog to become ready"
            ));
        }

        let app = AppBuilder::new(app_name)
            .with_catalog(make_catalog("db_uc"))
            .build();

        configure_test_datafusion();
        let rt = Runtime::builder().with_app(app).build().await;
        let cloned_rt = Arc::new(rt.clone());

        tokio::select! {
            // We may need to wait for the cluster to startup and become ready, so wait for up to 10 minutes
            () = tokio::time::sleep(remaining) => {
                return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
            }
            () = cloned_rt.load_components() => {}
        }

        let table_count = catalog_table_count(&rt, "db_uc").await?;
        if table_count > 0 {
            return Ok(rt);
        }

        tracing::warn!(
            "Catalog 'db_uc' registered with 0 tables (cluster may still be Pending); retrying in {}s",
            CATALOG_RETRY_DELAY.as_secs()
        );

        let retry_wait = CATALOG_RETRY_DELAY.min(LOAD_TIMEOUT.saturating_sub(start.elapsed()));
        if retry_wait.is_zero() {
            return Err(anyhow::anyhow!(
                "Timed out waiting for Spark Connect cluster/catalog to become ready"
            ));
        }
        tokio::time::sleep(retry_wait).await;
    }
}

#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_spark_integration_test() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let mut rt = load_runtime_waiting_for_catalog("databricks_spark_connector").await?;

            let queries: QueryTests = vec![(
                "SELECT * FROM db_uc.tpch.nation ORDER BY n_nationkey LIMIT 10",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 4, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("Databricks (mode: spark_connect) Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("databricks_spark_connect_select"), results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("databricks_spark_connect_test_{snapshot_suffix}"),
                    query,
                    true,
                    validate_result,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            }

            Ok(())
        })
        .await
}

#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_spark_schema_inference_test() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let rt = load_runtime_waiting_for_catalog("databricks_spark_schema_test").await?;

            runtime_ready_check(&rt).await;

            // Verify tpch.nation columns appear in information_schema
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_catalog = 'db_uc' AND table_schema = 'tpch' \
                     AND table_name = 'nation' \
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
                total_columns, 4,
                "Expected 4 columns in tpch.nation schema, got {total_columns}"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_spark_dataset_registration_test() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let rt = load_runtime_waiting_for_catalog("databricks_spark_registration_test").await?;

            runtime_ready_check(&rt).await;

            let table_count = catalog_table_count(&rt, "db_uc").await?;
            assert!(
                table_count > 0,
                "Expected at least one table registered under db_uc catalog, found {table_count}"
            );

            Ok(())
        })
        .await
}
