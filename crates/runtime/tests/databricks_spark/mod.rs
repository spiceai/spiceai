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

use app::AppBuilder;

use crate::{
    ValidateFn, configure_test_datafusion, init_tracing, run_query_and_check_results,
    utils::{register_test_connectors, test_request_context},
};

use runtime::Runtime;
use spicepod::{component::catalog::Catalog, param::Params};

fn make_catalog(path: &str, name: &str) -> Catalog {
    let mut catalog = Catalog::new(format!("databricks:{path}"), name.to_string());
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
            let app = AppBuilder::new("databricks_spark_connector")
                .with_catalog(make_catalog(
                    "catalog-dash-test",
                    "db_uc",
                ))
                .build();

            configure_test_datafusion();
            let mut rt =
                Runtime::builder()
                    .with_app(app)
                    .build()
                    .await;

            let cloned_rt = Arc::new(rt.clone());
            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let queries: QueryTests = vec![(
                "SELECT * FROM db_uc.default.databricks_demo ORDER BY network, isp LIMIT 10",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 7, "num_cols: {}", batch.num_columns());
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
