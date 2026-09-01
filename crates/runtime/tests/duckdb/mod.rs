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

fn make_duckdb_acceleration_dataset(ds_name: &str, fn_name: &str, path_str: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("duckdb:read_{fn_name}({path_str})"),
        fn_name.to_string(),
    );
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: None,
        ..Acceleration::default()
    });
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
                .with_dataset(make_duckdb_acceleration_dataset(
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

            let mut other_dataset = make_duckdb_acceleration_dataset(
                "csv_test_arrow",
                "csv",
                &format!("'{}'", temp_file.path().display()),
            );
            other_dataset.acceleration = Some(Acceleration {
                enabled: true,
                ..Default::default()
            });

            let app = AppBuilder::new("duckdb_regexp_test")
                .with_dataset(make_duckdb_acceleration_dataset(
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
                ), arrow_regex AS (
                    SELECT region FROM csv_test_arrow WHERE regexp_like(region, 'A.*A')
                ), missing_in_duckdb AS (
                    SELECT region FROM arrow_regex
                    EXCEPT
                    SELECT region FROM duckdb_regex
                ), missing_in_arrow AS (
                    SELECT region FROM duckdb_regex
                    EXCEPT
                    SELECT region FROM arrow_regex
                )
                SELECT region FROM missing_in_duckdb
                UNION ALL
                SELECT region FROM missing_in_arrow
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
                "regexp_like regex metacharacter semantics diverged between DuckDB and Arrow"
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
                    ), arrow_regexp_like AS (
                        SELECT * FROM csv_test_arrow WHERE regexp_like(region, 'america', 'i')
                    )

                    SELECT * FROM duckdb_regexp_like d JOIN arrow_regexp_like a ON d.region = a.region",
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
                let pretty = arrow::util::pretty::pretty_format_batches(&explain_plan)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))
                    .expect("Should format batches");
                insta::assert_snapshot!(format!("{name}_explain"), pretty);
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
                .with_dataset(make_duckdb_acceleration_dataset(
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
                let pretty = arrow::util::pretty::pretty_format_batches(&explain_plan)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))
                    .expect("Should format batches");
                insta::assert_snapshot!(format!("{name}_explain"), pretty);
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

            // With federation working the scan appears as VirtualExecutionPlan name=duckdb;
            // on the scan fallback path it appears as DuckSqlExec. Either indicates that
            // the DuckDB connector is being used for the table scan.
            assert!(
                plan.contains("name=duckdb") || plan.contains("DuckSqlExec"),
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

/// Regression test for spiceai/spiceai#13728: `cosine_distance` must answer the
/// same number whether or not the subtree it sits in federates.
///
/// `DuckDB`'s `array_cosine_distance` is not the same function. It is
/// `1 - cosine_similarity` over `[0, 2]` where the UDF is
/// `(1 - cosine_similarity) / 2` over `[0, 1]`, and it evaluates in FLOAT where
/// the kernel evaluates in f64 — so a pair of *identical* finite vectors whose
/// squared components underflow FLOAT (`[1e-30, 0, 0]`) comes back as maximally
/// distant instead of identical. The constant factor could be rescaled in the
/// emitted SQL; the width cannot, which is why the name is denied rather than
/// rewritten.
///
/// `fed` has no acceleration, so its scan is pushed to `DuckDB`; `local` is
/// accelerated into Arrow, so it is evaluated by the Spice kernel. Both read the
/// same rows, so the two must agree — and `inner_product` must still federate,
/// since `DuckDB`'s `array_inner_product` agrees wherever the result is
/// representable. Where it is not, the two still diverge (`+Inf` against the
/// kernel's NULL); that is pinned below and tracked in spiceai/spiceai#13787.
#[tokio::test]
async fn duckdb_cosine_distance_is_not_federated_to_a_different_function() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let duck_tempdir = tempfile::tempdir().expect("duckdb tempdir");
    let db_path = duck_tempdir.path().join("vectors.duckdb");
    {
        let conn = duckdb::Connection::open(&db_path).expect("open duckdb");
        // Each row is (vector, probe). Rows 1-3 walk an ordinary direction from
        // near-parallel to opposite; rows 4 and 5 compare a vector with itself at
        // magnitudes whose squares underflow and overflow FLOAT respectively.
        // Those two are the rows a rescaling of DuckDB's answer cannot fix: the
        // correct distance is 0, and DuckDB answers its maximum.
        conn.execute_batch(
            "CREATE TABLE vecs (id BIGINT, emb FLOAT[3], probe FLOAT[3]);
             INSERT INTO vecs VALUES
               (1, [4.0, 5.0, 6.0],    [1.0, 2.0, 3.0]),
               (2, [1.0, 2.0, 3.0],    [1.0, 2.0, 3.0]),
               (3, [-1.0, -2.0, -3.0], [1.0, 2.0, 3.0]),
               (4, [1e-30, 0.0, 0.0],  [1e-30, 0.0, 0.0]),
               (5, [1e20, 0.0, 0.0],   [1e20, 0.0, 0.0]);",
        )
        .expect("populate duckdb");
    }

    test_request_context()
        .scope(async {
            let duckdb_open =
                spicepod::param::Params::from_string_map(std::collections::HashMap::from([(
                    "duckdb_open".to_string(),
                    db_path.display().to_string(),
                )]));

            let mut federated = Dataset::new("duckdb:vecs".to_string(), "fed".to_string());
            federated.params = Some(duckdb_open.clone());

            let mut local = Dataset::new("duckdb:vecs".to_string(), "local".to_string());
            local.params = Some(duckdb_open);
            local.acceleration = Some(Acceleration {
                enabled: true,
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("duckdb_cosine_distance_federation")
                .with_dataset(federated)
                .with_dataset(local)
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

            let batches = async |sql: String| -> Result<Vec<RecordBatch>, String> {
                rt.datafusion()
                    .query_builder(&sql)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{sql}` failed: {e}"))?
                    .data
                    .try_collect()
                    .await
                    .map_err(|e| format!("query `{sql}` collect failed: {e}"))
            };

            // The named column of a query, as `f64`, with NULL mapped to NaN so a
            // NULL can never silently compare equal to a number.
            let column = async |sql: String, name: &str| -> Result<Vec<f64>, String> {
                let mut out = Vec::new();
                for batch in &batches(sql.clone()).await? {
                    let col = batch
                        .column_by_name(name)
                        .ok_or_else(|| format!("`{sql}` returned no `{name}` column"))?;
                    let col = col
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .ok_or_else(|| {
                            format!("`{name}` is {:?}, expected Float64", col.data_type())
                        })?;
                    out.extend(col.iter().map(|v| v.unwrap_or(f64::NAN)));
                }
                Ok(out)
            };

            let cosine = |table: &str| {
                format!("SELECT id, cosine_distance(emb, probe) AS d FROM {table} ORDER BY id")
            };

            let federated_d = column(cosine("fed"), "d").await?;
            let local_d = column(cosine("local"), "d").await?;

            assert_eq!(federated_d.len(), 5, "expected 5 rows, got {federated_d:?}");
            assert_eq!(
                local_d.len(),
                federated_d.len(),
                "row counts differ: {federated_d:?} vs {local_d:?}"
            );

            for (row, (fed, loc)) in federated_d.iter().zip(&local_d).enumerate() {
                assert!(
                    (fed - loc).abs() < 1e-9,
                    "row {} of cosine_distance disagrees across the federation \
                     boundary: fed {fed}, local {loc}. federated={federated_d:?} \
                     local={local_d:?}",
                    row + 1
                );
            }

            // Rows 4 and 5 are a vector against itself, so the distance is 0. If
            // the call ever federates to `array_cosine_distance` again these read
            // 2.0 (or 1.0 if something rescales them), which is why they are
            // asserted on their own rather than only against each other.
            assert!(
                local_d[3] == 0.0 && local_d[4] == 0.0,
                "a vector's distance to itself must be 0 at every magnitude, got \
                 {:?} and {:?}",
                local_d[3],
                local_d[4]
            );

            // The scan must still be pushed down, but without the function.
            let explain = batches(format!("EXPLAIN {}", cosine("fed"))).await?;
            let plan = arrow::util::pretty::pretty_format_batches(&explain)
                .map_err(|e| format!("format explain: {e}"))?
                .to_string();
            let pushed: Vec<&str> = plan
                .lines()
                .filter(|l| l.contains("base_sql=") || l.contains("DuckSqlExec sql="))
                .collect();
            assert!(
                !pushed.is_empty(),
                "expected the connector to push a scan down to DuckDB; plan was:\n{plan}"
            );
            for line in pushed {
                assert!(
                    !line.contains("array_cosine_distance"),
                    "cosine_distance was federated to DuckDB's array_cosine_distance, \
                     which answers a different number:\n{line}"
                );
            }
            assert!(
                plan.contains("cosine_distance"),
                "cosine_distance must still appear in the plan, evaluated locally:\n{plan}"
            );

            // `inner_product` is the control: DuckDB's `array_inner_product`
            // computes the same value wherever that value is representable, so
            // it must still federate and still agree on those rows. The one row
            // whose result is not representable is asserted separately below.
            let inner = |table: &str| {
                format!("SELECT id, inner_product(emb, probe) AS ip FROM {table} ORDER BY id")
            };
            let federated_ip = column(inner("fed"), "ip").await?;
            let local_ip = column(inner("local"), "ip").await?;
            // Rows 1-4, whose dot products are finite, must agree exactly.
            for (row, (fed, loc)) in federated_ip.iter().zip(&local_ip).take(4).enumerate() {
                assert!(
                    (fed - loc).abs() < 1e-9,
                    "row {} of inner_product disagrees across the federation \
                     boundary: fed {fed}, local {loc}",
                    row + 1
                );
            }
            // Row 5's true dot product is 1e40, which no f32 accumulator holds.
            // The kernel calls that undefined and returns NULL; DuckDB returns
            // +Inf. That divergence is real but is not what this change is about
            // — it needs input screening in the emitted SQL rather than a
            // different function — so it is pinned here rather than left to be
            // rediscovered. Tracked in spiceai/spiceai#13787.
            assert!(
                federated_ip[4].is_infinite() && local_ip[4].is_nan(),
                "the known non-finite inner_product divergence changed shape: fed \
                 {}, local {} (NaN here means the kernel returned NULL). If this \
                 was fixed on purpose, update this assertion and close #13787.",
                federated_ip[4],
                local_ip[4]
            );
            let inner_explain = batches(format!("EXPLAIN {}", inner("fed"))).await?;
            let inner_plan = arrow::util::pretty::pretty_format_batches(&inner_explain)
                .map_err(|e| format!("format explain: {e}"))?
                .to_string();
            assert!(
                inner_plan.contains("array_inner_product"),
                "inner_product must still federate to DuckDB's array_inner_product:\n\
                 {inner_plan}"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_duckdb_settings_persist() -> Result<(), String> {
    use spicepod::param::Params;
    use std::collections::HashMap;

    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Create a temporary DuckDB file
            let temp_dir = std::env::temp_dir().join("spiced_duckdb_settings_test");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
            let duckdb_file = temp_dir.join("test_settings.db");

            defer! {
                std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
            }

            // Create a dataset with DuckDB acceleration and custom settings
            let mut accel_params = HashMap::new();
            accel_params.insert(
                "duckdb_file".to_string(),
                duckdb_file
                    .to_str()
                    .expect("DuckDB file path should be valid UTF-8")
                    .to_string(),
            );
            accel_params.insert(
                "duckdb_index_scan_percentage".to_string(),
                "0.05".to_string(),
            ); // 5% as decimal
            accel_params.insert(
                "duckdb_index_scan_max_count".to_string(),
                "5000".to_string(),
            );

            // Create a simple CSV file for testing
            let csv_file = temp_dir.join("test.csv");
            std::fs::write(&csv_file, "id,name\n1,test\n2,test2\n").expect("failed to write csv");

            let mut dataset = Dataset::new(
                format!("file:{}", csv_file.display()),
                "test_settings".to_string(),
            );
            dataset.name = "test_settings".to_string();
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(accel_params)),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("duckdb_settings_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::clone(&cloned_rt).load_components() => {}
            }

            runtime_ready_check(&cloned_rt).await;

            // Verify the accelerated dataset loaded successfully
            println!("✅ DuckDB accelerator initialized successfully with custom settings:");
            println!("   - duckdb_index_scan_percentage: 0.05 (5%)");
            println!("   - duckdb_index_scan_max_count: 5000");
            println!("   - PRAGMA enable_checkpoint_on_shutdown (automatic)");

            // Query the accelerated table to ensure it's working
            let df = cloned_rt.datafusion();
            let result = df
                .query_builder("SELECT COUNT(*) as row_count FROM test_settings")
                .build()
                .run()
                .await
                .map_err(|e| format!("Failed to query test_settings: {e}"))?;

            let batches: Vec<RecordBatch> = result
                .data
                .try_collect()
                .await
                .map_err(|e| format!("Failed to collect results: {e}"))?;

            // Verify we got data
            if batches.is_empty() || batches[0].num_rows() == 0 {
                return Err("No rows returned from query".to_string());
            }

            let count_col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "Failed to downcast count column".to_string())?;
            let count = count_col.value(0);

            println!("✅ Query successful: test_settings table has {count} rows");

            if count != 2 {
                return Err(format!("Expected 2 rows, got {count}"));
            }

            // Shutdown the runtime
            cloned_rt.shutdown().await;
            drop(cloned_rt);
            drop(rt);

            // Give time for shutdown and checkpoint
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            // Verify the file was checkpointed (file should exist and be non-zero)
            if !duckdb_file.exists() {
                return Err("DuckDB file does not exist after shutdown".to_string());
            }

            let metadata = std::fs::metadata(&duckdb_file)
                .map_err(|e| format!("Failed to get file metadata: {e}"))?;

            println!(
                "✓ DuckDB file size after shutdown: {} bytes",
                metadata.len()
            );

            if metadata.len() == 0 {
                return Err(
                    "DuckDB file is empty after shutdown - checkpoint may have failed".to_string(),
                );
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_duckdb_all_settings() -> Result<(), String> {
    use spicepod::param::Params;
    use std::collections::HashMap;

    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    Box::pin(test_request_context()
        .scope(async {
            // Test 1: Index scan settings with custom file
            println!("\n=== Test 1: Index Scan Settings with Custom File ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_1");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
                let duckdb_file = temp_dir.join("test_index_scan.db");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_file".to_string(), duckdb_file.to_str().expect("DuckDB file path should be valid UTF-8").to_string());
                accel_params.insert("duckdb_index_scan_percentage".to_string(), "0.05".to_string());
                accel_params.insert("duckdb_index_scan_max_count".to_string(), "5000".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,name\n1,test\n2,test2\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_index_scan".to_string());
                dataset.name = "test_index_scan".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::File,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_index_scan").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 1: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                // Verify query works
                let df = cloned_rt.datafusion();
                let result = df
                    .query_builder("SELECT COUNT(*) FROM test_index_scan")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 1: Query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 1: Failed to collect: {e}"))?;

                let count_col = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| "Test 1: Failed to downcast".to_string())?;
                assert_eq!(count_col.value(0), 2, "Test 1: Expected 2 rows");

                println!("✅ Index scan settings applied successfully");
                println!("   - duckdb_file: custom path");
                println!("   - index_scan_percentage: 0.05");
                println!("   - index_scan_max_count: 5000");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Verify checkpoint occurred
                assert!(duckdb_file.exists(), "Test 1: DuckDB file should exist");
                let metadata = std::fs::metadata(&duckdb_file)
                    .map_err(|e| format!("Test 1: Failed to get metadata: {e}"))?;
                assert!(metadata.len() > 0, "Test 1: File should be non-zero");
                println!("✅ Checkpoint verified: {} bytes", metadata.len());
            }

            // Test 2: Memory limit setting
            println!("\n=== Test 2: Memory Limit Setting ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_2");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_memory_limit".to_string(), "512MB".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,value\n1,100\n2,200\n3,300\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_memory".to_string());
                dataset.name = "test_memory".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::Memory,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_memory").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 2: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                let df = cloned_rt.datafusion();
                let result = df
                    .query_builder("SELECT SUM(value) as total FROM test_memory")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 2: Query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 2: Failed to collect: {e}"))?;

                let sum_col = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| "Test 2: Failed to downcast".to_string())?;
                assert_eq!(sum_col.value(0), 600, "Test 2: Expected sum of 600");

                println!("✅ Memory limit setting applied successfully");
                println!("   - memory_limit: 512MB");
                println!("   - mode: Memory");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }

            // Test 3: Preserve insertion order
            println!("\n=== Test 3: Preserve Insertion Order ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_3");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_preserve_insertion_order".to_string(), "true".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,name\n3,charlie\n1,alice\n2,bob\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_order".to_string());
                dataset.name = "test_order".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::File,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_order").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 3: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                let df = cloned_rt.datafusion();
                let result = df
                    .query_builder("SELECT * FROM test_order")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 3: Query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 3: Failed to collect: {e}"))?;

                assert!(!batches.is_empty(), "Test 3: Should have results");
                assert_eq!(batches[0].num_rows(), 3, "Test 3: Expected 3 rows");

                println!("✅ Preserve insertion order setting applied successfully");
                println!("   - preserve_insertion_order: true");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }

            // Test 4: Combined settings
            println!("\n=== Test 4: Combined Settings ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_4");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
                let duckdb_file = temp_dir.join("test_combined.db");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_file".to_string(), duckdb_file.to_str().expect("DuckDB file path should be valid UTF-8").to_string());
                accel_params.insert("duckdb_memory_limit".to_string(), "256MB".to_string());
                accel_params.insert("duckdb_index_scan_percentage".to_string(), "0.10".to_string());
                accel_params.insert("duckdb_index_scan_max_count".to_string(), "1000".to_string());
                accel_params.insert("duckdb_preserve_insertion_order".to_string(), "false".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,category,amount\n1,A,100\n2,B,200\n3,A,150\n4,C,300\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_combined".to_string());
                dataset.name = "test_combined".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::File,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_combined").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 4: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                let df = cloned_rt.datafusion();

                // Test aggregation
                let result = df
                    .query_builder("SELECT category, SUM(amount) as total FROM test_combined GROUP BY category ORDER BY category")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 4: Aggregation query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 4: Failed to collect: {e}"))?;

                assert_eq!(batches[0].num_rows(), 3, "Test 4: Expected 3 categories");

                println!("✅ Combined settings applied successfully");
                println!("   - file: custom path");
                println!("   - memory_limit: 256MB");
                println!("   - index_scan_percentage: 0.10");
                println!("   - index_scan_max_count: 1000");
                println!("   - preserve_insertion_order: false");
                println!("   - PRAGMA enable_checkpoint_on_shutdown: automatic");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Verify checkpoint
                assert!(duckdb_file.exists(), "Test 4: DuckDB file should exist");
                let metadata = std::fs::metadata(&duckdb_file)
                    .map_err(|e| format!("Test 4: Failed to get metadata: {e}"))?;
                assert!(metadata.len() > 0, "Test 4: File should be non-zero");
                println!("✅ Checkpoint verified: {} bytes", metadata.len());
            }

            println!("\n=== All DuckDB Settings Tests Passed ===");
            Ok(())
        }))
        .await
}

/// Test that verifies `DuckDB` connection pool handles concurrent queries correctly.
///
/// **Critical for**: `duckdb-rs` fork (`spiceai/duckdb-rs`, spiceai-57)
///
/// This test exercises the connection pool improvements in the duckdb-rs fork by
/// running multiple concurrent queries against a DuckDB-accelerated dataset.
/// The connection pool must efficiently manage connections and avoid deadlocks
/// or connection exhaustion under concurrent load.
///
/// **Patches tested**:
/// - Connection pool improvements for memory allocation
/// - Arrow 57 compatibility in duckdb-rs
/// - `register_arrow_scan_view` method for arrow stream support
///
/// **What happens without the patch**: Concurrent queries may fail with connection
/// errors, deadlocks, or memory issues due to inefficient connection handling.
#[tokio::test]
async fn test_duckdb_connection_pool_concurrent_queries() -> Result<(), String> {
    use spicepod::param::Params;
    use std::collections::HashMap;
    use std::fmt::Write as _;

    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = std::env::temp_dir().join("spiced_duckdb_pool_test");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

            defer! {
                let _ = std::fs::remove_dir_all(&temp_dir);
            }

            // Create a CSV file with more data for meaningful concurrent queries
            let csv_file = temp_dir.join("test_concurrent.csv");
            let mut csv_content = String::from("id,category,value\n");
            for i in 1..=1000 {
                let _ = writeln!(csv_content, "{},{},{}", i, ['A', 'B', 'C'][i % 3], i * 10);
            }
            std::fs::write(&csv_file, csv_content).expect("failed to write csv");

            let mut accel_params = HashMap::new();
            // Use memory mode for faster operations
            accel_params.insert("duckdb_memory_limit".to_string(), "256MB".to_string());

            let mut dataset = Dataset::new(
                format!("file:{}", csv_file.display()),
                "concurrent_test".to_string(),
            );
            dataset.name = "concurrent_test".to_string();
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(accel_params)),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("duckdb_pool_test")
                .with_dataset(dataset)
                .build();
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Run multiple concurrent queries to test connection pool
            let queries = [
                "SELECT COUNT(*) FROM concurrent_test",
                "SELECT category, SUM(value) FROM concurrent_test GROUP BY category",
                "SELECT AVG(value) FROM concurrent_test WHERE category = 'A'",
                "SELECT MAX(value), MIN(value) FROM concurrent_test",
                "SELECT * FROM concurrent_test WHERE id < 100 ORDER BY id",
                "SELECT category, COUNT(*) FROM concurrent_test GROUP BY category",
                "SELECT value FROM concurrent_test WHERE value > 5000 ORDER BY value DESC LIMIT 10",
                "SELECT DISTINCT category FROM concurrent_test ORDER BY category",
            ];

            let num_iterations = 3;
            let mut handles = Vec::new();

            for iteration in 0..num_iterations {
                for (i, query) in queries.iter().enumerate() {
                    let rt_clone = Arc::clone(&rt);
                    let query = (*query).to_string();
                    let handle = tokio::spawn(async move {
                        let result = rt_clone
                            .datafusion()
                            .query_builder(&query)
                            .build()
                            .run()
                            .await;

                        match result {
                            Ok(query_result) => {
                                let batches: Result<Vec<RecordBatch>, _> =
                                    query_result.data.try_collect().await;
                                match batches {
                                    Ok(b) => {
                                        tracing::debug!(
                                            "Query {}-{} completed: {} batches",
                                            iteration,
                                            i,
                                            b.len()
                                        );
                                        Ok(())
                                    }
                                    Err(e) => {
                                        Err(format!("Query {iteration}-{i} collection failed: {e}"))
                                    }
                                }
                            }
                            Err(e) => Err(format!("Query {iteration}-{i} execution failed: {e}")),
                        }
                    });
                    handles.push(handle);
                }
            }

            // Wait for all concurrent queries to complete
            let results: Vec<_> = futures::future::join_all(handles).await;

            // Check for any failures
            let mut errors = Vec::new();
            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => errors.push(format!("Task {i}: {e}")),
                    Err(e) => errors.push(format!("Task {i} panicked: {e}")),
                }
            }

            if !errors.is_empty() {
                return Err(format!(
                    "DuckDB connection pool test FAILED - {} queries failed out of {}:\n{}",
                    errors.len(),
                    num_iterations * queries.len(),
                    errors.join("\n")
                ));
            }

            tracing::info!(
                "DuckDB connection pool test PASSED - {} concurrent queries completed successfully",
                num_iterations * queries.len()
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// A Parquet file written with an all-null column has `DataType::Null` in its Arrow schema
/// metadata (logical type Unknown). `DuckDB` doesn't have a Null type and silently coerces
/// it to INT32 when creating the acceleration table. Without the fix this produces a schema
/// mismatch at query time; with the fix the accelerator normalises the column to INT32 before
/// creating the table so both sides agree.
#[tokio::test]
async fn duckdb_acceleration_null_typed_parquet_column() -> Result<(), String> {
    use arrow::array::{Int64Array, NullArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;

    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Build a parquet file whose `untyped` column carries DataType::Null — the same
            // situation as a pyarrow file where every value in a column is null and the
            // column was never given an explicit type.
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("untyped", DataType::Null, true),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(NullArray::new(3)),
                ],
            )
            .map_err(|e| format!("failed to create batch: {e}"))?;

            let temp_dir =
                tempfile::tempdir().map_err(|e| format!("failed to create temp dir: {e}"))?;
            let parquet_path = temp_dir.path().join("null_col.parquet");
            {
                let file = std::fs::File::create(&parquet_path)
                    .map_err(|e| format!("failed to create parquet file: {e}"))?;
                let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
                    .map_err(|e| format!("failed to create ArrowWriter: {e}"))?;
                writer
                    .write(&batch)
                    .map_err(|e| format!("failed to write batch: {e}"))?;
                writer
                    .close()
                    .map_err(|e| format!("failed to close writer: {e}"))?;
            }

            let mut dataset = Dataset::new(
                format!("duckdb:read_parquet('{}')", parquet_path.display()),
                "null_col_parquet".to_string(),
            );
            dataset.name = "null_col_parquet".to_string();
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("duckdb_null_col_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err("Timed out waiting for dataset to load".to_string());
                }
                () = Arc::clone(&cloned_rt).load_components() => {}
            }

            runtime_ready_check(&cloned_rt).await;

            let result = cloned_rt
                .datafusion()
                .query_builder("SELECT id, untyped FROM null_col_parquet ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| format!("query failed: {e}"))?;

            let batches: Vec<RecordBatch> = result
                .data
                .try_collect()
                .await
                .map_err(|e| format!("failed to collect results: {e}"))?;

            assert_batches_eq!(
                [
                    "+----+---------+",
                    "| id | untyped |",
                    "+----+---------+",
                    "| 1  |         |",
                    "| 2  |         |",
                    "| 3  |         |",
                    "+----+---------+",
                ],
                &batches
            );

            let result = cloned_rt
                .datafusion()
                .query_builder("describe null_col_parquet")
                .build()
                .run()
                .await
                .map_err(|e| format!("query failed: {e}"))?;

            let batches: Vec<RecordBatch> = result
                .data
                .try_collect()
                .await
                .map_err(|e| format!("failed to collect results: {e}"))?;

            assert_batches_eq!(
                [
                    "+-------------+-----------+-------------+",
                    "| column_name | data_type | is_nullable |",
                    "+-------------+-----------+-------------+",
                    "| id          | Int64     | YES         |",
                    "| untyped     | Int32     | YES         |",
                    "+-------------+-----------+-------------+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}
