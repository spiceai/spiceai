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

use std::{collections::HashMap, sync::Arc};

use app::AppBuilder;

use runtime::Runtime;
use spicepod::{
    component::dataset::Dataset,
    param::{ParamValue, Params},
};

use crate::{
    ValidateFn, configure_test_datafusion, init_tracing, run_query_and_check_results,
    run_query_and_check_results_with_plan_checks, utils::test_request_context,
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

pub fn get_raw_file_dataset() -> Result<Dataset, anyhow::Error> {
    // if tests are running with `cargo test --package runtime`, this path is relative to the `runtime` crate
    // if tests are running as a built binary, this path is relative to the binary.
    // in binary mode, we expect to be running in the root of the project
    let file_path = if std::fs::exists("./tests/file/test_docs")? {
        "./tests/file/test_docs"
    } else if std::fs::exists("./crates/runtime/tests/file/test_docs")? {
        "./crates/runtime/tests/file/test_docs"
    } else {
        return Err(anyhow::anyhow!("Could not find test_docs directory"));
    };

    let mut dataset = Dataset::new(format!("file:{file_path}"), "docs");

    dataset.params = Some(Params {
        data: HashMap::from([(
            "file_format".to_string(),
            ParamValue::String("md".to_string()),
        )]),
    });

    Ok(dataset)
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

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
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

#[tokio::test]
async fn file_connector_projection_pushdown() -> Result<(), anyhow::Error> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("file_connector")
                .with_dataset(get_raw_file_dataset()?)
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder()
                .with_app(app)
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let queries: QueryTests = vec![(
                "SELECT content FROM docs",
                "projection_pushdown",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 1, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 1, "num_rows: {}", batch.num_rows());
                    }

                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("File Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!("file_integration_test_projection_pushdown", results);
                    });
                })),
            )];


            for (query, _, validate_result) in queries {
                let plan_check =
                        ("TableScan", Box::new(|plan: &str| {
                            plan.contains("docs") && plan.contains("projection=[content]")
                        }) as Box<dyn Fn(&str) -> bool + 'static>);
                run_query_and_check_results_with_plan_checks(&mut rt, query, vec![plan_check], validate_result).await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }

            Ok(())
        })
        .await
}
