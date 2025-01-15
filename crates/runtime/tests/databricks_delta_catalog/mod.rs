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

use crate::{
    get_test_datafusion, init_tracing, run_query_and_check_results,
    utils::{runtime_ready_check, test_request_context},
    ValidateFn,
};
use app::AppBuilder;
use runtime::{status, Runtime};
use spicepod::component::{catalog::Catalog, params::Params};
use std::sync::Arc;

#[tokio::test]
async fn databricks_delta_lake_integration_test_catalog() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(None);
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let mut db_catalog =
                Catalog::new("databricks:spiceai_sandbox".to_string(), "db_uc".to_string());
            db_catalog.include = vec!["tpch.*".to_string(), "tpcds.*".to_string()];
            db_catalog.params = Some(get_params());

            let app = AppBuilder::new("databricks_delta_lake_catalog_test")
                .with_catalog(db_catalog)
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let queries: QueryTests = vec![(
                "SELECT * FROM db_uc.tpch.lineitem LIMIT 10",
                "select_tpch",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("Databricks Catalog (mode: delta_lake) Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("test_select_tpch"), results);
                    });
                })),
            ),
            (
                "SELECT * FROM db_uc.tpcds.catalog_sales LIMIT 10",
                "select_tpcds",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 34, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("Databricks Catalog (mode: delta_lake) Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("test_select_tpcds"), results);
                    });
                })),
            ),
            ];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("test_select_{snapshot_suffix}"),
                    query,
                    false,
                    validate_result,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            }

            Ok(())
        })
        .await
}

fn get_params() -> Params {
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
                "databricks_aws_secret_access_key".to_string(),
                "${ env:AWS_DATABRICKS_DELTA_SECRET_ACCESS_KEY }".to_string(),
            ),
            (
                "databricks_aws_access_key_id".to_string(),
                "${ env:AWS_DATABRICKS_DELTA_ACCESS_KEY_ID }".to_string(),
            ),
            ("client_timeout".to_string(), "120s".to_string()),
            ("mode".to_string(), "delta_lake".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}
