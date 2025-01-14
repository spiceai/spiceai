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
use spicepod::component::{dataset::Dataset, params::Params};

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("snowflake:{path}"), name.to_string());
    dataset.params = Some(get_params());
    dataset
}

#[allow(clippy::expect_used)]
fn get_params() -> Params {
    // Verify that the environment variables are set
    let warehouse =
        std::env::var("SNOWFLAKE_WAREHOUSE").unwrap_or_else(|_| "COMPUTE_WH".to_string());
    let role = std::env::var("SNOWFLAKE_ROLE").unwrap_or_else(|_| "accountadmin".to_string());
    let _ = std::env::var("SNOWFLAKE_ACCOUNT").expect("SNOWFLAKE_ACCOUNT is not set"); // i.e: PPNILLP.RRB93167
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

#[tokio::test]
async fn snowflake_integration_test() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("snowflake_connector")
                .with_dataset(make_dataset(
                    "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM",
                    "lineitem",
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
                r#"SELECT * FROM lineitem ORDER BY "L_SHIPDATE", "L_LINENUMBER", "L_SUPPKEY" LIMIT 10"#,
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("Snowflake Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("snowflake_select"), results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("snowflake_test_{snapshot_suffix}"),
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
