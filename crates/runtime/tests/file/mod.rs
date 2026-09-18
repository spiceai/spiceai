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

use futures::StreamExt;
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

/// Builds a `file:` dataset pointed at a directory of NDJSON files with the
/// `_last_modified` and `_location` listing-table metadata columns enabled,
/// mirroring the config in <https://github.com/spiceai/spiceai/issues/14113>.
fn get_metadata_ndjson_dataset(dir: &std::path::Path) -> Dataset {
    let mut dataset = Dataset::new(format!("file:{}/", dir.display()), "t");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "json".to_string()),
            ("json_format".to_string(), "jsonl".to_string()),
            ("file_extension".to_string(), ".jsonl".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.metadata.insert(
        "_last_modified".to_string(),
        serde_json::Value::String("enabled".to_string()),
    );
    dataset.metadata.insert(
        "_location".to_string(),
        serde_json::Value::String("enabled".to_string()),
    );
    dataset
}

/// Runs `query`, drains the stream and returns `(schema, total_rows)`.
///
/// A stream that panics or errors (the failure mode in the issue) propagates out
/// of here and fails the test.
async fn run_and_count(
    rt: &Runtime,
    query: &str,
) -> Result<(arrow::datatypes::SchemaRef, usize), anyhow::Error> {
    let mut result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("query '{query}' failed to plan/execute: {e}"))?;

    let schema = result.data.schema();
    let mut rows = 0;
    while let Some(batch) = result.data.next().await {
        rows += batch
            .map_err(|e| anyhow::anyhow!("query '{query}' produced an error batch: {e}"))?
            .num_rows();
    }
    Ok((schema, rows))
}

/// Regression test for <https://github.com/spiceai/spiceai/issues/14113>:
/// projecting an enabled metadata column (`_last_modified` / `_location`) from a
/// flat, non-partitioned local NDJSON dataset used to panic the query executor
/// with `index out of bounds` in the file-scan projection layer. The metadata
/// columns must be as queryable as the data columns.
#[tokio::test]
async fn file_connector_metadata_columns_projection() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            // A single flat NDJSON file, exactly as in the issue reproduction.
            std::fs::write(
                dir.path().join("f.jsonl"),
                "{\"id\":0,\"value\":10}\n{\"id\":1,\"value\":20}\n{\"id\":2,\"value\":30}\n",
            )?;

            let app = AppBuilder::new("file_connector_metadata")
                .with_dataset(get_metadata_ndjson_dataset(dir.path()))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            // The metadata columns must register in the schema, ordered *after*
            // the data columns (the layout invariant whose violation caused the
            // out-of-bounds panic at scan time).
            let (schema, star_rows) = run_and_count(&rt, "SELECT * FROM t").await?;
            assert_eq!(star_rows, 3, "SELECT * should return all 3 rows");
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            let pos = |name: &str| {
                names
                    .iter()
                    .position(|n| *n == name)
                    .unwrap_or_else(|| panic!("column '{name}' missing from schema {names:?}"))
            };
            // The out-of-bounds panic came from the file-scan classifying a
            // metadata column as a partition column, which only holds when the
            // metadata columns sit *after* the data columns in the table schema.
            assert!(
                pos("_last_modified") > pos("id") && pos("_last_modified") > pos("value"),
                "_last_modified must be appended after the data columns: {names:?}"
            );
            assert!(
                pos("_location") > pos("id") && pos("_location") > pos("value"),
                "_location must be appended after the data columns: {names:?}"
            );

            // Every query from the issue must return rows instead of panicking.
            // These all *materialize* a metadata column, which is the crashing path.
            let (_, rows) = run_and_count(&rt, "SELECT _last_modified FROM t LIMIT 3").await?;
            assert_eq!(rows, 3, "SELECT _last_modified should return 3 rows");

            let (_, rows) = run_and_count(&rt, "SELECT max(_last_modified) FROM t").await?;
            assert_eq!(rows, 1, "SELECT max(_last_modified) should return 1 row");

            let (_, rows) = run_and_count(&rt, "SELECT _location FROM t LIMIT 3").await?;
            assert_eq!(rows, 3, "SELECT _location should return 3 rows");

            let (_, rows) =
                run_and_count(&rt, "SELECT _last_modified, _location, id, value FROM t").await?;
            assert_eq!(
                rows, 3,
                "combined data + metadata projection should return 3 rows"
            );

            // Data-only queries were never affected; assert they still hold.
            let (_, rows) = run_and_count(&rt, "SELECT id, value FROM t").await?;
            assert_eq!(rows, 3, "data-only projection should return 3 rows");

            let (_, rows) = run_and_count(&rt, "SELECT count(*) FROM t").await?;
            assert_eq!(rows, 1, "count(*) should return 1 row");

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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
