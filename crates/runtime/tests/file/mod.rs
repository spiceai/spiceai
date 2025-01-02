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
use spicepod::component::dataset::Dataset;

use crate::{
    get_test_datafusion, init_tracing, run_query_and_check_results, utils::test_request_context,
    ValidateFn,
};

pub fn get_dataset() -> Result<Dataset, anyhow::Error> {
    // if tests are running with `cargo test --package runtime`, this path is relative to the `runtime` crate
    // if tests are running as a built binary, this path is relative to the binary.
    // in binary mode, we expect to be running in the root of the project
    let file_path = if std::fs::exists("./tests/file/datatypes.parquet")? {
        "./tests/file/datatypes.parquet"
    } else if std::fs::exists("./crates/runtime/tests/file/datatypes.parquet")? {
        "./crates/runtime/tests/file/datatypes.parquet"
    } else {
        return Err(anyhow::anyhow!("Could not find datatypes.parquet file"));
    };

    Ok(Dataset::new(format!("file:{file_path}"), "datatypes"))
}

#[tokio::test]
async fn file_connector_datatypes() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("file_connector")
                .with_dataset(get_dataset()?)
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
                "SELECT * FROM datatypes",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 10, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 1, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("File Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("file_integration_test_select"), results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("file_integration_test_{snapshot_suffix}"),
                    query,
                    false, // snapshot plan changes depending on the runner's filesystem
                    // the file_groups outputs the absolute path to the parquet file
                    validate_result,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            }

            Ok(())
        })
        .await
}
