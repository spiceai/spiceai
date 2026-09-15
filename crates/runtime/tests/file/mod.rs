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

/// Lay out a hive-partitioned Parquet dataset on disk with three partitions
/// (`p=1`, `p=2`, `p=3`) and multiple rows per partition, returning the temp dir
/// (which must be kept alive for the lifetime of the query) and a `file:`
/// collection dataset over it. Parquet is used because the partition-only-scan
/// rewrite requires exact per-file row counts, which Parquet carries in its
/// metadata (formats without exact statistics are intentionally not rewritten).
fn hive_partitioned_dataset() -> Result<(tempfile::TempDir, Dataset), anyhow::Error> {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;

    let dir = tempfile::TempDir::new()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    for p in 1_i64..=3 {
        let partition_dir = dir.path().join(format!("p={p}"));
        std::fs::create_dir_all(&partition_dir)?;
        // Several rows per partition so a content scan would produce far more
        // rows than the single distinct partition value the listing carries.
        let ids: Vec<i64> = (0..5).collect();
        let values: Vec<i64> = (0..5).map(|id| id * p).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;

        let file = std::fs::File::create(partition_dir.join("f.parquet"))?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
        writer.write(&batch)?;
        writer.close()?;
    }

    // Trailing slash marks the path as a collection so hive partitioning applies.
    let mut dataset = Dataset::new(format!("file:{}/", dir.path().display()), "hivepart");
    dataset.params = Some(Params {
        data: HashMap::from([
            (
                "file_format".to_string(),
                ParamValue::String("parquet".to_string()),
            ),
            (
                "file_extension".to_string(),
                ParamValue::String(".parquet".to_string()),
            ),
            (
                "hive_partitioning_enabled".to_string(),
                ParamValue::Bool(true),
            ),
        ]),
    });

    Ok((dir, dataset))
}

/// Regression test for <https://github.com/spiceai/spiceai/issues/14112>: a
/// `GROUP BY`/`DISTINCT` over only the hive partition column must be answered
/// from the directory listing (an in-memory `DataSourceExec`) instead of opening
/// and parsing every data file (a `file_groups=` `DataSourceExec`).
#[tokio::test]
async fn file_connector_partition_only_scan_uses_listing() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (_dir, dataset) = hive_partitioned_dataset()?;
            let app = AppBuilder::new("file_connector")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // (a)/(b): partition-only `GROUP BY` is answered from the listing —
            // the scan is an in-memory source, not a `file_groups=` file scan —
            // and still returns exactly the three distinct partition values.
            let partition_only_scan_is_from_listing = (
                "DataSourceExec",
                Box::new(|plan: &str| plan.contains("partitions=") && !plan.contains("file_groups"))
                    as Box<dyn Fn(&str) -> bool + 'static>,
            );
            run_query_and_check_results_with_plan_checks(
                &mut rt,
                "SELECT p FROM hivepart GROUP BY p ORDER BY p DESC LIMIT 1",
                vec![partition_only_scan_is_from_listing],
                Some(|result_batches: Vec<arrow::array::RecordBatch>| {
                    let rows: usize = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum();
                    assert_eq!(rows, 1, "latest-partition query should return one row");
                }),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

            let distinct_scan_is_from_listing = (
                "DataSourceExec",
                Box::new(|plan: &str| plan.contains("partitions=") && !plan.contains("file_groups"))
                    as Box<dyn Fn(&str) -> bool + 'static>,
            );
            run_query_and_check_results_with_plan_checks(
                &mut rt,
                "SELECT p FROM hivepart GROUP BY p",
                vec![distinct_scan_is_from_listing],
                Some(|result_batches: Vec<arrow::array::RecordBatch>| {
                    let rows: usize = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum();
                    assert_eq!(rows, 3, "one row per distinct partition value");
                }),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

            // Negative contrast: an aggregate that depends on row counts
            // (`count(*)`) still scans file contents — the rewrite must not fire.
            let count_star_reads_files = (
                "DataSourceExec",
                Box::new(|plan: &str| plan.contains("file_groups"))
                    as Box<dyn Fn(&str) -> bool + 'static>,
            );
            run_query_and_check_results_with_plan_checks(
                &mut rt,
                "SELECT p, count(*) FROM hivepart GROUP BY p",
                vec![count_star_reads_files],
                Some(|result_batches: Vec<arrow::array::RecordBatch>| {
                    let rows: usize = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum();
                    assert_eq!(rows, 3, "one row per partition, with counts");
                }),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

            Ok(())
        })
        .await
}

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
