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
use arrow::array::ArrayRef;
use arrow::datatypes::DecimalType;
use arrow::{
    array::{Decimal128Array, RecordBatch},
    datatypes::{DataType, Decimal128Type},
};

use futures::TryStreamExt;
use runtime::Runtime;
use scopeguard::defer;
use spicepod::acceleration::{Acceleration, Mode};
use spicepod::component::dataset::Dataset;

use crate::{
    PlanCheckFn, ValidateFn, configure_test_datafusion, init_tracing, run_query_and_check_results,
    run_query_and_check_results_with_plan_checks,
    utils::{runtime_ready_check, test_request_context},
};

fn make_sqlite_decimal_dataset(mode: Mode) -> Dataset {
    let mut ds = Dataset::new("https://public-data.spiceai.org/decimal.parquet", "decimal");
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("sqlite".to_string()),
        mode,
        ..Default::default()
    });
    ds
}

enum CheckFunction {
    ValidateFullPlan(String),
    ValidateSubPlan(Vec<(&'static str, PlanCheckFn)>),
}

type QueryTests<'a> = Vec<(&'a str, CheckFunction, Option<Box<ValidateFn>>)>;

#[derive(Debug, Copy, Clone)]
enum DecimalQuery {
    Federated,
    NonFederated,
}

fn decimal_queries(snapshot_name: &str, query_type: DecimalQuery) -> QueryTests<'static> {
    let expected_plan: CheckFunction = match query_type {
        DecimalQuery::Federated => CheckFunction::ValidateSubPlan(vec![(
            "VirtualExecutionPlan",
            Box::new(|plan| {
                plan.contains("sql=SELECT sum(`decimal`.`small_decimal`), sum(`decimal`.`medium_decimal`), sum(`decimal`.`large_decimal`), sum(`decimal`.`precise_decimal`) FROM `decimal`")
            }),
        )]),
        DecimalQuery::NonFederated => {
            CheckFunction::ValidateFullPlan(format!("{snapshot_name}_non_federated"))
        }
    };
    vec![(
        "SELECT SUM(small_decimal), SUM(medium_decimal), SUM(large_decimal), SUM(precise_decimal) FROM decimal",
        expected_plan,
        Some(Box::new(|results: Vec<RecordBatch>| {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].num_columns(), 4);
            assert_eq!(results[0].num_rows(), 1);
            assert_eq!(
                downcast_decimal_array(results[0].column(0))
                    .value(0)
                    .to_string(),
                "22381"
            );
            let schema = results[0].schema();

            // small_decimal
            let DataType::Decimal128(precision, scale) = schema.field(0).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(0));
            assert_eq!(
                Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale),
                "223.81"
            );

            // medium_decimal
            let DataType::Decimal128(precision, scale) = schema.field(1).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(1));
            assert_eq!(
                Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale),
                "186109.5051"
            );

            // large_decimal
            let DataType::Decimal128(precision, scale) = schema.field(2).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(2));
            assert_eq!(
                Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale),
                "10866582.506250"
            );

            // precise_decimal
            let DataType::Decimal128(precision, scale) = schema.field(3).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(3));
            assert_eq!(
                Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale),
                "-1.7443152324"
            );
        })),
    )]
}

fn downcast_decimal_array(array: &ArrayRef) -> &Decimal128Array {
    match array.as_any().downcast_ref::<Decimal128Array>() {
        Some(array) => array,
        None => panic!("Expected decimal array"),
    }
}

#[tokio::test]
async fn test_sqlite_decimal_memory() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("test_sqlite_decimal_memory")
                .with_dataset(make_sqlite_decimal_dataset(Mode::Memory))
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

            runtime_ready_check(&rt).await;

            for (query, check_function, validate_result) in
                decimal_queries("test_sqlite_decimal_memory", DecimalQuery::NonFederated)
            {
                match check_function {
                    CheckFunction::ValidateFullPlan(snapshot_name) => {
                        run_query_and_check_results(
                            &mut rt,
                            &snapshot_name,
                            query,
                            true,
                            validate_result,
                        )
                        .await
                    }
                    CheckFunction::ValidateSubPlan(plan_checks) => {
                        run_query_and_check_results_with_plan_checks(
                            &mut rt,
                            query,
                            plan_checks,
                            validate_result,
                        )
                        .await
                    }
                }
                .expect("query to succeed");
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_sqlite_decimal_file() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("test_sqlite_decimal_file")
                .with_dataset(make_sqlite_decimal_dataset(Mode::File))
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

            runtime_ready_check(&rt).await;

            for (query, check_function, validate_result) in
                decimal_queries("test_sqlite_decimal_file", DecimalQuery::Federated)
            {
                match check_function {
                    CheckFunction::ValidateFullPlan(snapshot_name) => {
                        run_query_and_check_results(
                            &mut rt,
                            &snapshot_name,
                            query,
                            true,
                            validate_result,
                        )
                        .await
                    }
                    CheckFunction::ValidateSubPlan(plan_checks) => {
                        run_query_and_check_results_with_plan_checks(
                            &mut rt,
                            query,
                            plan_checks,
                            validate_result,
                        )
                        .await
                    }
                }
                .expect("query to succeed");
            }

            // Clean up files
            let dir_path = "./.spice";
            if std::path::Path::new(dir_path).exists() {
                std::fs::remove_dir_all(dir_path).expect("Failed to remove directory");
            }

            Ok(())
        })
        .await
}

/// `trim(col)` resolves to `DataFusion`'s `btrim` UDF and federates under that
/// canonical name, which `SQLite` has no function for — the query fails with
/// `no such function: btrim` (the `SQLite` arm of issue #13794). `SQLite` cannot
/// be given a rewrite, because `datafusion-table-providers` constructs its
/// unparser dialect internally, so `btrim` is deny-listed and the projection
/// evaluates locally instead.
///
/// This exercises the wired path the deny-list unit tests cannot see: the
/// backend-specific deny-list has to actually reach
/// `SqliteTableProviderFactory`. Take away
/// `SqliteAccelerator`'s `with_function_support` call and the unit tests still
/// pass while this one fails.
#[tokio::test]
async fn sqlite_btrim_evaluates_locally() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            // Quoted so the CSV reader keeps the padding that makes `trim`
            // observable, and one row padded with `x` rather than spaces so the
            // two-argument character-set form has something to strip.
            let csv_path = "./test_sqlite_btrim.csv";
            std::fs::write(
                csv_path,
                concat!(
                    "id,name\n",
                    "1,\"  alpha  \"\n",
                    "2,\"xxbetaxx\"\n",
                    "3,\"  gamma\"\n",
                    // Unicode space separators, which `btrim` does *not* strip.
                    // Denying the call keeps evaluation local so these cannot
                    // diverge — this pins that, and would catch a future
                    // rewrite that federated them to a wider `trim`.
                    "4,\"\u{a0}nbsp\u{a0}\"\n",
                    "5,\"\u{2003}emsp\u{2003}\"\n",
                    "6,\"\u{3000}ideo\u{3000}\"\n",
                ),
            )?;
            defer! {
                let _ = std::fs::remove_file(csv_path);
            }

            let mut lite = Dataset::new(format!("file:{csv_path}"), "lite");
            lite.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("sqlite".to_string()),
                mode: Mode::Memory,
                ..Default::default()
            });
            let mut local = Dataset::new(format!("file:{csv_path}"), "local");
            local.acceleration = Some(Acceleration {
                enabled: true,
                ..Default::default()
            });

            let app = AppBuilder::new("sqlite_btrim")
                .with_dataset(lite)
                .with_dataset(local)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let run = |query: String| {
                let rt = rt.clone();
                async move {
                    let batches: Vec<RecordBatch> = rt
                        .datafusion()
                        .query_builder(&query)
                        .build()
                        .run()
                        .await
                        .map_err(|e| anyhow::anyhow!("`{query}` failed: {e}"))?
                        .data
                        .try_collect()
                        .await
                        .map_err(|e| anyhow::anyhow!("`{query}` collection failed: {e}"))?;
                    Ok::<_, anyhow::Error>(batches)
                }
            };

            // The query the deny-list makes possible at all: on an un-denied
            // `btrim` every one of these fails outright rather than returning a
            // row, so agreeing with local evaluation is the whole assertion.
            for projection in [
                "trim(name)",
                "trim(name, 'x')",
                "btrim(name)",
                "trim(name, cast(null as varchar))",
                "length(trim(name))",
            ] {
                let diff = run(format!(
                    "WITH pushed AS (SELECT {projection} AS v FROM lite),
                          plain AS (SELECT {projection} AS v FROM local),
                          missing_locally AS (SELECT v FROM pushed EXCEPT ALL SELECT v FROM plain),
                          missing_pushed AS (SELECT v FROM plain EXCEPT ALL SELECT v FROM pushed)
                     SELECT v FROM missing_locally UNION ALL SELECT v FROM missing_pushed"
                ))
                .await?;
                assert_eq!(
                    diff.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    0,
                    "`{projection}` disagrees between the SQLite accelerator and local evaluation"
                );
            }

            // A filter too: the deny-list sees filter expressions on their own
            // path, and a pushed-down `WHERE btrim(...)` fails the same way.
            let filtered =
                run("SELECT id FROM lite WHERE trim(name) = 'alpha'".to_string()).await?;
            assert_eq!(
                filtered.iter().map(RecordBatch::num_rows).sum::<usize>(),
                1,
                "a filter on trim(name) should match exactly the one padded 'alpha' row"
            );

            // And the deny-list's mechanism, not just its outcome: `btrim` must
            // be left out of the SQL sent to SQLite and evaluated above the
            // scan. Asserting the emitted SQL is what distinguishes "denied"
            // from "SQLite grew a btrim".
            let plan = run("EXPLAIN SELECT trim(name) AS v FROM lite".to_string()).await?;
            let plan = arrow::util::pretty::pretty_format_batches(&plan)?.to_string();
            let base_sql = plan
                .split_once("VirtualExecutionPlan name=sqlite")
                .and_then(|(_, rest)| rest.split_once("base_sql="))
                .map(|(_, sql)| sql)
                .ok_or_else(|| anyhow::anyhow!("no SQLite scan in the plan:\n{plan}"))?;
            assert!(
                !base_sql.contains("btrim("),
                "btrim must not reach SQLite; the pushed-down SQL was:\n{base_sql}"
            );
            assert!(
                plan.contains("btrim("),
                "btrim should still be evaluated locally above the scan; plan was:\n{plan}"
            );

            Ok(())
        })
        .await
}
