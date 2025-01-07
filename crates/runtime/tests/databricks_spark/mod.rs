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
    get_test_datafusion, init_tracing, run_query_and_check_results, utils::test_request_context,
    ValidateFn,
};
use runtime::{status, Runtime};
use spicepod::component::{catalog::Catalog, params::Params};

fn make_catalog(path: &str, name: &str) -> Catalog {
    let mut catalog = Catalog::new(format!("databricks:{path}"), name.to_string());
    catalog.params = Some(get_params());
    catalog
}

#[allow(clippy::expect_used)]
fn get_params() -> Params {
    // Verify that the environment variables are set
    let _ = std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST is not set");
    let _ = std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN is not set");
    let _ = std::env::var("DATABRICKS_CLUSTER_ID").expect("DATABRICKS_CLUSTER_ID is not set");

    Params::from_string_map(
        vec![
            (
                "databricks_endpoint".to_string(),
                "${ env:DATABRICKS_HOST }".to_string(),
            ),
            (
                "databricks_token".to_string(),
                "${ env:DATABRICKS_TOKEN }".to_string(),
            ),
            (
                "databricks_cluster_id".to_string(),
                "${ env:DATABRICKS_CLUSTER_ID }".to_string(),
            ),
            ("mode".to_string(), "spark_connect".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}

#[tokio::test]
async fn databricks_spark_integration_test() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_spark_connector")
                .with_catalog(make_catalog(
                    "catalog-dash-test",
                    "db_uc",
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let mut rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
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
