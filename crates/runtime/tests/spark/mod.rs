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

fn make_spark_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("spark:{path}"), name.to_string());
    dataset.params = Some(Params::from_string_map(
        vec![(
            "spark_remote".to_string(),
            format!(
                "sc://{}:443/;use_ssl=true;user_id=spice.ai;session_id={};token={};x-databricks-cluster-id={}",
                std::env::var("DATABRICKS_HOST").unwrap_or_default(),
                uuid::Uuid::new_v4(),
                std::env::var("DATABRICKS_TOKEN").unwrap_or_default(),
                std::env::var("DATABRICKS_CLUSTER_ID").unwrap_or_default(),
            ),
        )]
            .into_iter()
            .collect(),
    ));

    dataset
}

#[tokio::test]
async fn spark_integration_test() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("spark_connector")
                .with_dataset(make_spark_dataset(
                    "spiceai_sandbox.tpch.lineitem",
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
                "select l_comment, l_partkey from lineitem order by l_linenumber desc limit 10",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("Spark Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("spark_select"), results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("spark_test_{snapshot_suffix}"),
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
