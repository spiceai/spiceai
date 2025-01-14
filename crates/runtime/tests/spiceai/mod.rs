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
use runtime::{status, Runtime};
use spicepod::component::{dataset::Dataset, params::Params};

use crate::{
    get_test_datafusion, init_tracing, run_query_and_check_results, utils::test_request_context,
    ValidateFn,
};

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("spice.ai/{path}"), name.to_string());
    dataset.params = Some(Params::from_string_map(
        vec![
            (
                "spiceai_api_key".to_string(),
                "${ env:SPICEAI_API_KEY }".to_string(),
            ),
            (
                "spiceai_endpoint".to_string(),
                "https://flight.spiceai.io".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

#[tokio::test]
async fn spiceai_federation() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("spiceai_federation")
                .with_dataset(make_spiceai_dataset(
                    "spiceai/quickstart/datasets/taxi_trips",
                    "taxi_trips",
                ))
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
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = rt.load_components() => {}
            }

            let queries: QueryTests = vec![(
                r#"
                    SELECT * FROM taxi_trips
                    WHERE taxi_trips."Airport_fee" > 0
                    ORDER BY tpep_pickup_datetime DESC, tpep_dropoff_datetime DESC, passenger_count DESC LIMIT 10"#,
                "select",
                Some(Box::new(|result_batches| {
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("Spice.ai Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("select"), results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("spiceai_federation_test_{snapshot_suffix}"),
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
