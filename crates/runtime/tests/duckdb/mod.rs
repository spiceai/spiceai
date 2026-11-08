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

use crate::configure_test_datafusion;
use crate::{
    RecordBatch, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};
use app::AppBuilder;
use arrow::array::{Array, Float64Array};
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use scopeguard::defer;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use tempfile::NamedTempFile;

fn make_duckdb_dataset(ds_name: &str, fn_name: &str, path_str: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("duckdb:read_{fn_name}({path_str})"),
        fn_name.to_string(),
    );
    dataset.name = ds_name.to_string();
    dataset
}

fn make_test_query(table_name: &str) -> String {
    format!("SELECT DISTINCT(\"VendorID\") FROM {table_name} ORDER BY \"VendorID\" DESC")
}

#[tokio::test]
async fn duckdb_from_functions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_json_contents = include_str!("../test_data/taxi_sample.json");
            // Write the sample file to a temporary directory
            let temp_dir = std::env::temp_dir().join("spiced_test_data");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
            let sample_csv_path = temp_dir.join("taxi_sample.csv");
            std::fs::write(&sample_csv_path, sample_csv_contents)
                .expect("failed to write sample file");
            let sample_json_path = temp_dir.join("taxi_sample.json");
            std::fs::write(&sample_json_path, sample_json_contents)
                .expect("failed to write sample file");
            defer! {
                std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
            }
            let app = AppBuilder::new("duckdb_function_test")
        .with_dataset(make_duckdb_dataset(
            "csv_remote",
            "csv",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.csv', HEADER=true",
        ))
        .with_dataset(make_duckdb_dataset(
            "csv_local",
            "csv",
            &format!("'{}'", sample_csv_path.display()),
        ))
        .with_dataset(make_duckdb_dataset(
            "parquet_remote",
            "parquet",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet'",
        ))
        .with_dataset(make_duckdb_dataset(
            "json_remote",
            "json",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.json'",
        ))
        .with_dataset(make_duckdb_dataset(
            "json_local",
            "json",
            &format!("'{}'", sample_json_path.display()),
        ))
        .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            let queries = vec![
                ("csv_remote", make_test_query("csv_remote")),
                ("csv_local", make_test_query("csv_local")),
                ("parquet_remote", make_test_query("parquet_remote")),
                //("parquet_local", make_test_query("parquet_local")),
                ("json_remote", make_test_query("json_remote")),
                ("json_local", make_test_query("json_local")),
            ];

            let expected_results = [
                "+----------+",
                "| VendorID |",
                "+----------+",
                "| 2        |",
                "| 1        |",
                "+----------+",
            ];

            for (ds_name, query) in queries {
                let query_result = rt
                    .datafusion()
                    .query_builder(&query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{query}` to plan: {e}"))?;
                let data = query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("{ds_name}: query `{query}` to results: {e}"))?;

                assert_batches_eq!(expected_results, &data);
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn duckdb_order_by_special_cases() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            // Write the sample file to a temporary directory
            let temp_dir = std::env::temp_dir().join("spiced_test_data_order_by");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
            let sample_csv_path = temp_dir.join("taxi_sample.csv");
            std::fs::write(&sample_csv_path, sample_csv_contents)
                .expect("failed to write sample file");
            defer! {
                std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
            }

            let app = AppBuilder::new("duckdb_order_by_test")
                .with_dataset(make_duckdb_dataset(
                    "csv_test",
                    "csv",
                    &format!("'{}'", sample_csv_path.display()),
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test ORDER BY NULL
            let order_by_null_query = "SELECT \"VendorID\" FROM csv_test ORDER BY NULL LIMIT 5";
            let query_result = rt
                .datafusion()
                .query_builder(order_by_null_query)
                .build()
                .run()
                .await
                .map_err(|e| format!("ORDER BY NULL query failed: {e}"))?;

            let _data = query_result
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| format!("ORDER BY NULL query execution failed: {e}"))?;

            // Test ORDER BY rand()
            let order_by_rand_query = "SELECT \"VendorID\" FROM csv_test ORDER BY rand() LIMIT 5";
            let query_result = rt
                .datafusion()
                .query_builder(order_by_rand_query)
                .build()
                .run()
                .await
                .map_err(|e| format!("ORDER BY rand() query failed: {e}"))?;

            let _data = query_result
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| format!("ORDER BY rand() query execution failed: {e}"))?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn duckdb_regexp() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/regions.csv");
            let temp_file = NamedTempFile::new().expect("Should create temp file");
            std::fs::write(temp_file.path(), sample_csv_contents)
                .expect("failed to write sample file");

            let mut other_dataset = make_duckdb_dataset(
                "csv_test_cayenne",
                "csv",
                &format!("'{}'", temp_file.path().display()),
            );
            other_dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                ..Default::default()
            });

            let app = AppBuilder::new("duckdb_regexp_test")
                .with_dataset(make_duckdb_dataset(
                    "csv_test",
                    "csv",
                    &format!("'{}'", temp_file.path().display()),
                ))
                .with_dataset(other_dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let regex_metachar_semantics = r"
                WITH duckdb_regex AS (
                    SELECT region FROM csv_test WHERE regexp_like(region, 'A.*A')
                ), cayenne_regex AS (
                    SELECT region FROM csv_test_cayenne WHERE regexp_like(region, 'A.*A')
                ), missing_in_duckdb AS (
                    SELECT region FROM cayenne_regex
                    EXCEPT
                    SELECT region FROM duckdb_regex
                ), missing_in_cayenne AS (
                    SELECT region FROM duckdb_regex
                    EXCEPT
                    SELECT region FROM cayenne_regex
                )
                SELECT region FROM missing_in_duckdb
                UNION ALL
                SELECT region FROM missing_in_cayenne
            ";

            let regex_semantic_diff: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(regex_metachar_semantics)
                .build()
                .run()
                .await
                .expect("regex metachar semantic comparison query is successful")
                .data
                .try_collect()
                .await
                .expect("collects regex metachar semantic comparison results");

            assert_eq!(
                regex_semantic_diff.iter().map(RecordBatch::num_rows).sum::<usize>(),
                0,
                "regexp_like regex metacharacter semantics diverged between DuckDB and Cayenne"
            );

            let cases = vec![
                (
                    "test_regexp_like_is_case_sensitive",
                    "SELECT * FROM csv_test WHERE regexp_like(region, 'america')",
                ),
                (
                    "test_regexp_like_with_case_insensitive_flag",
                    "SELECT * FROM csv_test WHERE regexp_like(region, 'america', 'i')",
                ),
                (
                    "test_regexp_match",
                    "SELECT regexp_match(region, 'AMERICA') FROM csv_test",
                ),
                (
                    "test_regexp_count",
                    "SELECT regexp_count(region, 'AMERICA') FROM csv_test",
                ),
                (
                    "test_regexp_replace",
                    "SELECT regexp_replace(region, 'AMERICA', 'AUSTRALIA') FROM csv_test",
                ),
                (
                    "test_regexp_replace_case_insensitive",
                    "SELECT regexp_replace(region, 'america', 'australia', 'i') FROM csv_test",
                ),
                (
                    "test_regexp_results_match",
                    "WITH duckdb_regexp_like AS (
                        SELECT * FROM csv_test WHERE regexp_like(region, 'america', 'i')
                    ), cayenne_regexp_like AS (
                        SELECT * FROM csv_test_cayenne WHERE regexp_like(region, 'america', 'i')
                    )

                    SELECT * FROM duckdb_regexp_like d JOIN cayenne_regexp_like a ON d.region = a.region",
                ),
            ];

            for (name, query) in cases {
                let result: Vec<RecordBatch> = rt
                    .datafusion()
                    .query_builder(query)
                    .build()
                    .run()
                    .await
                    .expect("query is successful")
                    .data
                    .try_collect()
                    .await
                    .expect("collects results");

                let pretty = arrow::util::pretty::pretty_format_batches(&result)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))
                    .expect("Should format batches");
                insta::assert_snapshot!(format!("{name}_results"), pretty);

                let explain_plan = rt
                    .datafusion()
                    .query_builder(&format!("EXPLAIN {query}"))
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("explain plan for `{query}` failed: {e}"))?
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("explain plan for `{query}` execution failed: {e}"))?;
                insta::assert_snapshot!(format!("{name}_explain"), stable_plan(&explain_plan));
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn duckdb_json_functions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/json_data.csv");
            let temp_file = NamedTempFile::new().expect("Should create temp file");
            std::fs::write(temp_file.path(), sample_csv_contents)
                .expect("failed to write sample file");

            let app = AppBuilder::new("duckdb_json_test")
                .with_dataset(make_duckdb_dataset(
                    "json_test",
                    "csv",
                    &format!("'{}'", temp_file.path().display()),
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let cases = vec![
                (
                    "test_json_get_str",
                    "SELECT json_get_str(data, 'name') AS name FROM json_test ORDER BY id",
                ),
                (
                    "test_json_get_int",
                    "SELECT json_get_int(data, 'age') AS age FROM json_test ORDER BY id",
                ),
                (
                    "test_json_get_float",
                    "SELECT json_get_float(data, 'score') AS score FROM json_test ORDER BY id",
                ),
                (
                    "test_json_get_bool",
                    "SELECT json_get_bool(data, 'active') AS active FROM json_test ORDER BY id",
                ),
                (
                    "test_json_contains",
                    "SELECT json_contains(data, 'name') AS has_name FROM json_test ORDER BY id",
                ),
                (
                    "test_json_as_text",
                    "SELECT json_as_text(data, 'name') AS name_text FROM json_test ORDER BY id",
                ),
                (
                    "test_json_length",
                    "SELECT json_length(data, 'tags') AS tag_count FROM json_test ORDER BY id",
                ),
                (
                    "test_json_get_str_in_filter",
                    "SELECT id FROM json_test WHERE json_get_str(data, 'name') = 'alice'",
                ),
            ];

            for (name, query) in cases {
                let result: Vec<RecordBatch> = rt
                    .datafusion()
                    .query_builder(query)
                    .build()
                    .run()
                    .await
                    .expect("query is successful")
                    .data
                    .try_collect()
                    .await
                    .expect("collects results");

                let pretty = arrow::util::pretty::pretty_format_batches(&result)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))
                    .expect("Should format batches");
                insta::assert_snapshot!(format!("{name}_results"), pretty);

                let explain_plan = rt
                    .datafusion()
                    .query_builder(&format!("EXPLAIN {query}"))
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("explain plan for `{query}` failed: {e}"))?
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("explain plan for `{query}` execution failed: {e}"))?;
                insta::assert_snapshot!(format!("{name}_explain"), stable_plan(&explain_plan));
            }

            Ok(())
        })
        .await
}

/// Regression test for <https://github.com/spiceai/spiceai/issues/10703>.
///
/// The `DuckDB` *connector* (federation to `DuckDB`, no Spice acceleration) must
/// not push Spice-only UDFs such as `json_get_str` into the SQL it sends to
/// `DuckDB` — those functions don't exist in `DuckDB`, so the query fails with an
/// "unknown function" error. Before the fix, `connector-duckdb` built its
/// `DuckDBTableFactory` without the Spice function deny-list, so `DuckDB`'s
/// `can_execute_plan` allowed the whole plan to federate and `json_get_str` was
/// unparsed into the `DuckDB` SQL. With the deny-list installed, the projection
/// is evaluated locally by `DataFusion` and only the bare scan is pushed down.
#[tokio::test]
async fn duckdb_connector_does_not_push_down_spice_functions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/json_data.csv");
            let temp_file = NamedTempFile::new().expect("Should create temp file");
            std::fs::write(temp_file.path(), sample_csv_contents)
                .expect("failed to write sample file");

            // No `.acceleration` — this exercises the connector federation path.
            let app = AppBuilder::new("duckdb_connector_json_test")
                .with_dataset(make_duckdb_dataset(
                    "json_test",
                    "csv",
                    &format!("'{}'", temp_file.path().display()),
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let query = "SELECT json_get_str(data, 'name') AS name FROM json_test ORDER BY id";

            // 1. The query must execute end-to-end. Before the fix this errored
            //    because `json_get_str` was pushed to DuckDB, which rejects it.
            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(query)
                .build()
                .run()
                .await
                .map_err(|e| format!("connector query `{query}` failed: {e}"))?
                .data
                .try_collect()
                .await
                .map_err(|e| format!("connector query `{query}` collect failed: {e}"))?;
            assert_batches_eq!(
                [
                    "+---------+",
                    "| name    |",
                    "+---------+",
                    "| alice   |",
                    "| bob     |",
                    "| charlie |",
                    "+---------+",
                ],
                &result
            );

            // 2. The pushed-down DuckDB SQL must not contain `json_get_str`. Assert
            //    on the `DuckSqlExec sql=` text directly so the test pins the actual
            //    failure mode (federated SQL) rather than incidental formatting.
            let explain_plan = rt
                .datafusion()
                .query_builder(&format!("EXPLAIN {query}"))
                .build()
                .run()
                .await
                .map_err(|e| format!("explain plan for `{query}` failed: {e}"))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| format!("explain plan for `{query}` execution failed: {e}"))?;
            let pretty = arrow::util::pretty::pretty_format_batches(&explain_plan)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            let plan = pretty.to_string();

            // The DuckDB connector reads through ADBC: with federation working the
            // scan appears as `VirtualExecutionPlan name=adbc`, carrying the SQL
            // DuckDB receives.
            assert!(
                plan.contains("VirtualExecutionPlan name=adbc"),
                "expected the connector plan to push a scan down to DuckDB; plan was:\n{plan}"
            );
            // json_get_str must not appear in any SQL sent to DuckDB.
            for line in plan
                .lines()
                .filter(|l| l.contains("DuckSqlExec sql=") || l.contains("base_sql="))
            {
                assert!(
                    !line.contains("json_get_str"),
                    "json_get_str was pushed into DuckDB SQL (deny-list not applied):\n{line}"
                );
            }
            assert!(
                plan.contains("json_get_str"),
                "json_get_str must still appear in the plan, evaluated locally:\n{plan}"
            );

            Ok(())
        })
        .await
}

/// A federated `inner_product` must answer what the kernel answers for a row
/// whose dot product is not a finite number. (Finite results are a separate,
/// unclosed question — #13893 — so this asserts only the non-finite contract.)
///
/// `array_inner_product` hands back the `inf` or `nan` it computed, where
/// Spice's kernel calls an undefined dot product NULL — and `DuckDB` sorts
/// `nan` above every real number under `ORDER BY … DESC`, so a vector the
/// kernel drops as undefined became the top row of every federated query
/// (issue #13787). The dialect now screens the pushed-down call with
/// `isfinite`.
///
/// The `EXPLAIN` assertion is the half a unit test cannot reach: it pins that
/// the screen is in the SQL `DuckDB` actually receives, rather than an
/// expression the runtime happened to evaluate locally.
#[tokio::test]
async fn duckdb_federated_inner_product_nulls_a_non_finite_result() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // A TempDir rather than a NamedTempFile: `duckdb::Connection::open`
            // wants to create the database itself, and the directory guard is
            // what removes it (and DuckDB's `.wal` sidecar) when the test ends.
            let db_dir = tempfile::tempdir().expect("should create temp dir");
            let db_path = db_dir.path().join("vecs.db");

            {
                let conn = duckdb::Connection::open(&db_path)
                    .map_err(|e| format!("failed to open duckdb file: {e}"))?;
                conn.execute_batch(
                    "CREATE TABLE vecs (id INTEGER, emb FLOAT[3], q FLOAT[3]);
                     INSERT INTO vecs VALUES
                       (1, [1.0, 2.0, 3.0]::FLOAT[3],        [4.0, 5.0, 6.0]::FLOAT[3]),
                       (2, [1e20, 0.0, 0.0]::FLOAT[3],       [1e20, 0.0, 0.0]::FLOAT[3]),
                       (3, ['nan'::FLOAT, 1.0, 1.0]::FLOAT[3], [1.0, 1.0, 1.0]::FLOAT[3]),
                       (4, ['inf'::FLOAT, 1.0, 1.0]::FLOAT[3], [1.0, 1.0, 1.0]::FLOAT[3]),
                       (5, ['-inf'::FLOAT, 1.0, 1.0]::FLOAT[3], [1.0, 1.0, 1.0]::FLOAT[3]),
                       (6, [0.0, 0.0, 0.0]::FLOAT[3],        [1.0, 2.0, 3.0]::FLOAT[3]);",
                )
                .map_err(|e| format!("failed to seed duckdb file: {e}"))?;
            }

            let mut dataset = Dataset::new("duckdb:vecs", "vecs");
            // `duckdb_open`, not `duckdb_file`: the latter is the accelerator's
            // parameter, and the connector ignores it with a warning and then
            // fails to load the dataset for want of a database to open.
            dataset.params = Some(spicepod::param::Params::from_string_map(
                vec![("duckdb_open".to_string(), db_path.display().to_string())]
                    .into_iter()
                    .collect(),
            ));

            let app = AppBuilder::new("duckdb_inner_product_non_finite")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let query = "SELECT id, inner_product(emb, q) AS ip FROM vecs ORDER BY id";
            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(query)
                .build()
                .run()
                .await
                .map_err(|e| format!("query `{query}` failed: {e}"))?
                .data
                .try_collect()
                .await
                .map_err(|e| format!("query `{query}` collect failed: {e}"))?;

            // Rows 2-5 overflow or carry a non-finite element; the kernel calls
            // each of those undefined, so the federated answer must be NULL too.
            // Rows 1 and 6 are ordinary and must survive the screen.
            //
            // Both infinities are rows because they fail a screen in opposite
            // directions: `-inf` sorts below every real number under `ORDER BY
            // … DESC`, so it never surfaced as the top row that made #13787
            // visible, and an upper-bound screen would reject `+inf` while
            // letting `-inf` through.
            //
            // Asserted per value rather than against a rendered table: a NULL
            // and a `nan` are both blank in the pretty-printed form, which is
            // exactly the difference this test exists to catch.
            let expected = vec![Some(32.0_f64), None, None, None, None, Some(0.0_f64)];
            let scores: Vec<Option<f64>> = result
                .iter()
                .flat_map(|batch| {
                    let scores = batch
                        .column_by_name("ip")
                        .expect("result carries an `ip` column")
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("`ip` is the kernel's Float64 return type")
                        .clone();
                    (0..scores.len())
                        .map(|row| (!scores.is_null(row)).then(|| scores.value(row)))
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(
                scores, expected,
                "a federated inner_product must match the kernel wherever the result \
                 is not a finite number: NULL for the overflow (id 2), the NaN \
                 element (id 3) and the positive and negative infinite elements \
                 (ids 4 and 5). Finite results are not claimed to agree exactly \
                 — see #13893"
            );

            let explain: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(&format!("EXPLAIN {query}"))
                .build()
                .run()
                .await
                .map_err(|e| format!("explain for `{query}` failed: {e}"))?
                .data
                .try_collect()
                .await
                .map_err(|e| format!("explain for `{query}` collect failed: {e}"))?;
            let plan = arrow::util::pretty::pretty_format_batches(&explain)
                .map_err(|e| format!("failed to format explain: {e}"))?
                .to_string();

            let federated_sql: Vec<&str> = plan
                .lines()
                .filter(|line| line.contains("base_sql=") || line.contains("DuckSqlExec sql="))
                .collect();
            assert!(
                !federated_sql.is_empty(),
                "expected the scan to be pushed down to DuckDB; plan was:\n{plan}"
            );
            for line in federated_sql {
                assert!(
                    line.contains("array_inner_product"),
                    "inner_product must still push down to DuckDB:\n{line}"
                );
                assert!(
                    line.contains("isfinite"),
                    "the pushed-down inner_product must be screened by isfinite, so a \
                     non-finite result comes back NULL rather than topping the ranking:\n{line}"
                );
            }

            Ok(())
        })
        .await
}

/// The rows of an `EXPLAIN` as `plan_type: line`, one plan line each, with the
/// tokens that change from run to run replaced: the view a `duckdb:` table function
/// is read through is named after the file path it reads, and the compute context
/// is a fresh identifier per database.
fn stable_plan(batches: &[RecordBatch]) -> String {
    let view = regex::Regex::new(r"spice_duckdb_[0-9a-f]+__view").expect("a valid pattern");
    let context = regex::Regex::new(r"compute_context=\S+").expect("a valid pattern");
    let mut lines = Vec::new();
    for batch in batches {
        let column = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("EXPLAIN returns string columns")
        };
        let (plan_types, plans) = (column(0), column(1));
        for row in 0..batch.num_rows() {
            for line in plans.value(row).lines() {
                lines.push(format!("{}: {}", plan_types.value(row), line.trim_end()));
            }
        }
    }
    let plan = lines.join("\n");
    let plan = view.replace_all(&plan, "<duckdb_view>");
    context
        .replace_all(&plan, "compute_context=<id>")
        .into_owned()
}

/// `EXPLAIN` a statement and return the formatted plan.
async fn formatted_explain(rt: &Runtime, sql: &str) -> Result<String, String> {
    let batches: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder(&format!("EXPLAIN {sql}"))
        .build()
        .run()
        .await
        .map_err(|e| format!("explain for `{sql}` failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| format!("explain for `{sql}` collect failed: {e}"))?;

    Ok(arrow::util::pretty::pretty_format_batches(&batches)
        .map_err(|e| format!("failed to format the plan for `{sql}`: {e}"))?
        .to_string())
}

/// Run a statement and return its single `Int64` column, sorted.
async fn sorted_int64_column(rt: &Runtime, sql: &str) -> Result<Vec<i64>, String> {
    let batches: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query `{sql}` failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| format!("query `{sql}` collect failed: {e}"))?;

    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| format!("`{sql}` did not return an Int64 column"))?;
        values.extend(column.iter().flatten());
    }
    values.sort_unstable();
    Ok(values)
}

/// The `id`s a query returns, in order, with `parameters` bound positionally.
async fn binary_filter_ids(
    rt: &Runtime,
    sql: &str,
    parameters: Option<datafusion::common::ParamValues>,
) -> Result<Vec<i32>, String> {
    let batches: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder(sql)
        .parameters(parameters)
        .build()
        .run()
        .await
        .map_err(|e| format!("query `{sql}` failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| format!("query `{sql}` collect failed: {e}"))?;
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name("id")
            .ok_or_else(|| format!("query `{sql}` returned no `id` column"))?;
        let column = arrow::compute::cast(column, &arrow::datatypes::DataType::Int32)
            .map_err(|e| format!("query `{sql}` returned an `id` that is not an integer: {e}"))?;
        let column = column
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .ok_or_else(|| format!("query `{sql}` returned a non-Int32 `id`"))?;
        ids.extend(column.iter().flatten());
    }
    Ok(ids)
}

/// A filter on a binary literal answers the same through `DuckDB` as it does
/// locally, on every row: a one-byte value, the empty blob, NULL, a multi-byte
/// value with a non-UTF-8 byte, the three bytes `xff`, and the one byte `x`.
///
/// The unparser spells a binary literal `X'ff'`, and `DuckDB` reads that as the
/// *text* `'xff'`: compared against a BLOB column it matched the row holding
/// the bytes `xff` instead of the byte `0xFF`, with no error. The literal
/// therefore must not reach `DuckDB` in that spelling, whether it was written
/// as a literal, folded from a cast, or bound as a parameter.
#[tokio::test]
async fn duckdb_binary_literal_filters_agree_with_local_evaluation() -> Result<(), String> {
    use datafusion::common::{ParamValues, ScalarValue};

    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let db_dir = tempfile::tempdir().expect("should create temp dir");
            let db_path = db_dir.path().join("blobs.db");
            {
                let conn = duckdb::Connection::open(&db_path)
                    .map_err(|e| format!("failed to open duckdb file: {e}"))?;
                conn.execute_batch(
                    "CREATE TABLE blobs (id INTEGER, b BLOB);
                     INSERT INTO blobs VALUES
                       (1, '\\xFF'::BLOB),
                       (2, ''::BLOB),
                       (3, NULL),
                       (4, '\\x00\\x41\\xC3\\xA9'::BLOB),
                       (5, 'xff'::BLOB),
                       (6, 'x'::BLOB);",
                )
                .map_err(|e| format!("failed to seed duckdb file: {e}"))?;
            }

            let dataset = |name: &str, engine: Option<&str>| {
                let mut dataset = Dataset::new("duckdb:blobs", name);
                dataset.params = Some(spicepod::param::Params::from_string_map(
                    vec![("duckdb_open".to_string(), db_path.display().to_string())]
                        .into_iter()
                        .collect(),
                ));
                dataset.acceleration = engine.map(|engine| Acceleration {
                    enabled: true,
                    engine: Some(engine.to_string()),
                    mode: Mode::Memory,
                    refresh_mode: Some(RefreshMode::Full),
                    ..Acceleration::default()
                });
                dataset
            };

            let app = AppBuilder::new("duckdb_binary_literal_filters")
                .with_dataset(dataset("federated", None))
                .with_dataset(dataset("local", Some("arrow")))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let byte_ff = || {
                Some(ParamValues::List(vec![
                    ScalarValue::Binary(Some(vec![0xff])).into(),
                ]))
            };
            let cases: Vec<(&str, Option<ParamValues>, Vec<i32>)> = vec![
                ("b = X'ff'", None, vec![1]),
                ("b = X''", None, vec![2]),
                ("b = X'0041c3a9'", None, vec![4]),
                ("b <> X'ff'", None, vec![2, 4, 5, 6]),
                ("b IN (X'ff', X'')", None, vec![1, 2]),
                ("b >= X'78'", None, vec![1, 5, 6]),
                ("b = CAST('xff' AS BYTEA)", None, vec![5]),
                ("b = $1", byte_ff(), vec![1]),
                ("b IS NULL", None, vec![3]),
            ];

            for (predicate, parameters, expected) in cases {
                let local = binary_filter_ids(
                    &rt,
                    &format!("SELECT id FROM local WHERE {predicate} ORDER BY id"),
                    parameters.clone(),
                )
                .await?;
                assert_eq!(
                    local, expected,
                    "local evaluation of `{predicate}` is the reference"
                );

                for table in ["federated"] {
                    let sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id");
                    let ids = binary_filter_ids(&rt, &sql, parameters.clone()).await?;
                    let plan = if parameters.is_none() {
                        formatted_explain(&rt, &sql).await?
                    } else {
                        String::from("(parameterized: no EXPLAIN)")
                    };
                    eprintln!("binary filter `{sql}` -> {ids:?}\n{plan}");
                    assert_eq!(
                        ids, expected,
                        "`{predicate}` on `{table}` must return the rows local evaluation \
                         does; plan was:\n{plan}"
                    );
                    assert!(
                        !plan.contains("X'"),
                        "a binary literal must not reach DuckDB as X'..', which it reads as \
                         text; plan was:\n{plan}"
                    );
                }
            }

            // In a projection the literal is a value rather than a predicate:
            // read as text, it would come back as the bytes of `x00ff`.
            let projection = "SELECT id, b = X'ff' AS is_ff, X'00ff' AS literal \
                              FROM {table} ORDER BY id";
            let mut rendered = Vec::new();
            for table in ["local", "federated"] {
                let sql = projection.replace("{table}", table);
                let batches: Vec<RecordBatch> = rt
                    .datafusion()
                    .query_builder(&sql)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{sql}` failed: {e}"))?
                    .data
                    .try_collect()
                    .await
                    .map_err(|e| format!("query `{sql}` collect failed: {e}"))?;
                rendered.push(
                    arrow::util::pretty::pretty_format_batches(&batches)
                        .map_err(|e| format!("failed to format `{sql}`: {e}"))?
                        .to_string(),
                );
            }
            assert_eq!(
                rendered[0],
                [
                    "+----+-------+---------+",
                    "| id | is_ff | literal |",
                    "+----+-------+---------+",
                    "| 1  | true  | 00ff    |",
                    "| 2  | false | 00ff    |",
                    "| 3  |       | 00ff    |",
                    "| 4  | false | 00ff    |",
                    "| 5  | false | 00ff    |",
                    "| 6  | false | 00ff    |",
                    "+----+-------+---------+",
                ]
                .join("\n"),
                "local evaluation of the projection is the reference"
            );
            assert_eq!(
                rendered[1], rendered[0],
                "federated projection must agree with local"
            );

            Ok(())
        })
        .await
}

/// A union input's own `ORDER BY` and `LIMIT` choose which of *that input's*
/// rows reach the union, and must keep doing so when the union is federated.
/// The unparser keeps them inside that input's own derived table; written on
/// the union's query after the last operand, they would bound the whole
/// `UNION ALL`, so `u UNION ALL` the top row of `v` would return that one row.
/// Regression test for #14737 (spiceai/datafusion#261).
///
/// Differential: each query also runs straight against the `DuckDB` file, and
/// the federated answer must be the same multiset of rows. Each query must be
/// federated whole — the union inside `base_sql` — or the comparison would only
/// show that `DataFusion` evaluates a union locally.
#[tokio::test]
async fn duckdb_federated_union_keeps_each_inputs_own_bound() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let db_dir = tempfile::tempdir().expect("should create temp dir");
            let db_path = db_dir.path().join("union_bounds.db");
            let oracle = duckdb::Connection::open(&db_path)
                .map_err(|e| format!("failed to open duckdb file: {e}"))?;
            oracle
                .execute_batch(
                    "CREATE TABLE u (a BIGINT);
                     INSERT INTO u VALUES (1), (2), (3), (4), (5);
                     CREATE TABLE v (a BIGINT);
                     INSERT INTO v VALUES (10), (20), (30);",
                )
                .map_err(|e| format!("failed to seed duckdb file: {e}"))?;

            let mut app = AppBuilder::new("duckdb_federated_union_bounds");
            for table in ["u", "v"] {
                let mut dataset = Dataset::new(format!("duckdb:{table}"), table);
                dataset.params = Some(spicepod::param::Params::from_string_map(
                    vec![("duckdb_open".to_string(), db_path.display().to_string())]
                        .into_iter()
                        .collect(),
                ));
                app = app.with_dataset(dataset);
            }

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app.build()).build().await;
            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let cases = [
                (
                    "SELECT a FROM u UNION ALL (SELECT a FROM v ORDER BY a DESC LIMIT 1)",
                    vec![1, 2, 3, 4, 5, 30],
                ),
                (
                    "(SELECT a FROM u ORDER BY a LIMIT 2) \
                     UNION ALL (SELECT a FROM v ORDER BY a DESC LIMIT 1)",
                    vec![1, 2, 30],
                ),
                (
                    "SELECT a FROM u UNION ALL (SELECT a FROM v ORDER BY a LIMIT 1 OFFSET 1)",
                    vec![1, 2, 3, 4, 5, 20],
                ),
            ];
            for (sql, expected) in cases {
                let mut from_duckdb: Vec<i64> = oracle
                    .prepare(sql)
                    .and_then(|mut statement| {
                        statement
                            .query_map([], |row| row.get::<_, i64>(0))?
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .map_err(|e| format!("`{sql}` failed in DuckDB: {e}"))?;
                from_duckdb.sort_unstable();
                assert_eq!(
                    from_duckdb, expected,
                    "the oracle itself disagrees with the expected rows for `{sql}`"
                );

                let plan = formatted_explain(&rt, sql).await?;
                assert!(
                    plan.lines()
                        .any(|line| line.contains("base_sql=") && line.contains("UNION ALL")),
                    "`{sql}` must be federated to DuckDB as one union; plan was:\n{plan}"
                );

                assert_eq!(
                    sorted_int64_column(&rt, sql).await?,
                    from_duckdb,
                    "federated `{sql}` must return the rows DuckDB returns for the same \
                     SQL; plan was:\n{plan}"
                );
            }

            Ok(())
        })
        .await
}
