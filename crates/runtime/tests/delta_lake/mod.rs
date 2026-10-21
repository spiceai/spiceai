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

use crate::RecordBatch;
use crate::configure_test_datafusion;
use crate::init_tracing;
use crate::utils::{register_test_connectors, test_request_context};
use app::AppBuilder;
use arrow::util::pretty::pretty_format_batches;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::acceleration::Acceleration;
use spicepod::component::dataset::Dataset;
use std::{fs::File, io::Write, sync::Arc};

pub fn make_delta_lake_dataset(path: &str, name: &str, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("delta_lake:{path}"), name.to_string());
    if accelerated {
        dataset.acceleration = Some(Acceleration::default());
    }
    dataset
}

struct FileCleanup {
    path: String,
}

impl Drop for FileCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[expect(clippy::expect_used)]
async fn setup_test_data(zip_url: &str, dir_name: &str) -> Result<String, String> {
    let tmp_dir = std::env::temp_dir();
    let path = format!("{}/{}", tmp_dir.display(), dir_name);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::create_dir(&path);

    // Download and extract the test data
    let resp = reqwest::get(zip_url).await.expect("request failed");
    let mut out = File::create(format!("{path}/{dir_name}.zip")).expect("failed to create file");
    let _ = out.write_all(&resp.bytes().await.expect("failed to read bytes"));
    let _ = std::process::Command::new("unzip")
        .stdin(std::process::Stdio::null())
        .arg(format!("{path}/{dir_name}.zip"))
        .arg("-d")
        .arg(&path)
        .spawn()
        .expect("unzip failed")
        .wait()
        .expect("unzip failed");

    Ok(path)
}

async fn run_delta_lake_test(
    app_name: &str,
    dataset_path: &str,
    dataset_name: &str,
    query: &str,
    expected_results: &[&str],
) -> Result<Runtime, String> {
    let app = AppBuilder::new(app_name)
        .with_dataset(make_delta_lake_dataset(dataset_path, dataset_name, false))
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

    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?;

    let data = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    assert_batches_eq!(expected_results, &data);
    Ok(rt)
}

#[tokio::test]
async fn query_delta_lake_with_partitions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context().scope(async {
        let path = setup_test_data(
            "https://public-data.spiceai.org/delta-lake-nation-with-partitionkey.zip",
            "nation",
        )
        .await?;
        let _hook = FileCleanup { path: path.clone() };

        let query = "SELECT * from test order by n_nationkey";
        let expected_results = [
        "+-------------+----------------+-------------+--------------------------------------------------------------------------------------------------------------------+",
        "| n_nationkey | n_name         | n_regionkey | n_comment                                                                                                          |",
        "+-------------+----------------+-------------+--------------------------------------------------------------------------------------------------------------------+",
        "| 0           | ALGERIA        | 0           | furiously regular requests. platelets affix furious                                                                |",
        "| 1           | ARGENTINA      | 1           | instructions wake quickly. final deposits haggle. final, silent theodolites                                        |",
        "| 2           | BRAZIL         | 1           | asymptotes use fluffily quickly bold instructions. slyly bold dependencies sleep carefully pending accounts        |",
        "| 3           | CANADA         | 1           | ss deposits wake across the pending foxes. packages after the carefully bold requests integrate caref              |",
        "| 4           | EGYPT          | 4           | usly ironic, pending foxes. even, special instructions nag. sly, final foxes detect slyly fluffily                 |",
        "| 5           | ETHIOPIA       | 0           | regular requests sleep carefull                                                                                    |",
        "| 6           | FRANCE         | 3           | oggedly. regular packages solve across                                                                             |",
        "| 7           | GERMANY        | 3           | ong the regular requests: blithely silent pinto beans hagg                                                         |",
        "| 8           | INDIA          | 2           | uriously unusual deposits about the slyly final pinto beans could                                                  |",
        "| 9           | INDONESIA      | 2           | d deposits sleep quickly according to the dogged, regular dolphins. special excuses haggle furiously special reque |",
        "| 10          | IRAN           | 4           | furiously idle platelets nag. express asymptotes s                                                                 |",
        "| 11          | IRAQ           | 4           | pendencies; slyly express foxes integrate carefully across the reg                                                 |",
        "| 12          | JAPAN          | 2           |  quickly final packages. furiously i                                                                               |",
        "| 13          | JORDAN         | 4           | the slyly regular ideas. silent Tiresias affix slyly fu                                                            |",
        "| 14          | KENYA          | 0           | lyly special foxes. slyly regular deposits sleep carefully. carefully permanent accounts slee                      |",
        "| 15          | MOROCCO        | 0           | ct blithely: blithely express accounts nag carefully. silent packages haggle carefully abo                         |",
        "| 16          | MOZAMBIQUE     | 0           |  beans after the carefully regular accounts r                                                                      |",
        "| 17          | PERU           | 1           | ly final foxes. blithely ironic accounts haggle. regular foxes about the regular deposits are furiously ir         |",
        "| 18          | CHINA          | 2           | ckly special packages cajole slyly. unusual, unusual theodolites mold furiously. slyly sile                        |",
        "| 19          | ROMANIA        | 3           | sly blithe requests. thinly bold deposits above the blithely regular accounts nag special, final requests. care    |",
        "| 20          | SAUDI ARABIA   | 4           | se slyly across the blithely regular deposits. deposits use carefully regular                                      |",
        "| 21          | VIETNAM        | 2           | lly across the quickly even pinto beans. caref                                                                     |",
        "| 22          | RUSSIA         | 3           | uctions. furiously unusual instructions sleep furiously ironic packages. slyly                                     |",
        "| 23          | UNITED KINGDOM | 3           | carefully pending courts sleep above the ironic, regular theo                                                      |",
        "| 24          | UNITED STATES  | 1           | ly ironic requests along the slyly bold ideas hang after the blithely special notornis; blithely even accounts     |",
        "+-------------+----------------+-------------+--------------------------------------------------------------------------------------------------------------------+",
        ];

        let _ = run_delta_lake_test(
            "delta_lake_partition_test",
            &format!("{path}/nation"),
            "test",
            query,
            &expected_results,
        )
        .await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn query_delta_lake_with_null_partitions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let path = setup_test_data(
                "https://public-data.spiceai.org/delta-lake-null-partition.zip",
                "delta_partition",
            )
            .await?;
            let _hook = FileCleanup { path: path.clone() };

            let test_cases = [
                (
                    "SELECT tenant_id, day FROM test ORDER BY tenant_id, day",
                    vec![
                        "+-----------+------------+",
                        "| tenant_id | day        |",
                        "+-----------+------------+",
                        "| tenant1   | 2024-01-01 |",
                        "| tenant1   | 2024-01-01 |",
                        "|           | 2024-01-02 |",
                        "+-----------+------------+",
                    ],
                ),
                (
                    "SELECT * FROM test ORDER BY tenant_id, day, document_id",
                    vec![
                        "+-----------+------------+-------------+--------------------------+-------------+-------------------+-----------------------------+",
                        "| tenant_id | day        | document_id | raw_email                | compression | compression_level | ingested_at                 |",
                        "+-----------+------------+-------------+--------------------------+-------------+-------------------+-----------------------------+",
                        "| tenant1   | 2024-01-01 | doc1        | 7465737420656d61696c     | gzip        | 6                 | 2025-01-11T03:14:44.193684Z |",
                        "| tenant1   | 2024-01-01 | doc3        | 7465737420656d61696c2033 | gzip        | 6                 | 2025-01-11T03:14:44.193688Z |",
                        "|           | 2024-01-02 | doc2        | 7465737420656d61696c2032 | gzip        | 6                 | 2025-01-11T03:14:44.193687Z |",
                        "+-----------+------------+-------------+--------------------------+-------------+-------------------+-----------------------------+",
                    ],
                ),
            ];

            for (query, expected_results) in test_cases {
                run_delta_lake_test(
                    "delta_lake_null_partition_test",
                    &format!("{path}/delta_partition"),
                    "test",
                    query,
                    &expected_results,
                )
                .await?;
            }
            Ok(())
        })
        .await
}

#[tokio::test]
async fn query_delta_lake_with_percent_encoded_path() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let path = setup_test_data(
                "https://public-data.spiceai.org/delta_table_partition.zip",
                "delta_table_partition",
            )
            .await?;
            let _hook = FileCleanup { path: path.clone() };

            let query = "SELECT * FROM test ORDER BY date_col, name, value";
            let expected_results = [
                "+--------------+---------+-------+",
                "| date_col     | name    | value |",
                "+--------------+---------+-------+",
                "| 2024-02-04   | Alice   | 100   |",
                "| 2025-01-01   | Charlie | 300   |",
                "| 2030-06-15   | David   | 400   |",
                "| +10999-12-31 | Bob     | 200   |",
                "+--------------+---------+-------+",
            ];

            let _ = run_delta_lake_test(
                "delta_lake_percent_encoded_path_test",
                &format!("{path}/delta_table_partition"),
                "test",
                query,
                &expected_results,
            )
            .await?;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn query_delta_lake_with_partition_pruning() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let path = setup_test_data(
                "https://public-data.spiceai.org/delta_table_partition.zip",
                "delta_table_partition",
            )
            .await?;
            let _hook = FileCleanup { path: path.clone() };

            let query =
                "SELECT * FROM test WHERE date_col > '2025-01-01' ORDER BY date_col, name, value";
            let expected_results = [
                "+--------------+-------+-------+",
                "| date_col     | name  | value |",
                "+--------------+-------+-------+",
                "| 2030-06-15   | David | 400   |",
                "| +10999-12-31 | Bob   | 200   |",
                "+--------------+-------+-------+",
            ];

            let rt = run_delta_lake_test(
                "query_delta_lake_with_partition_pruning",
                &format!("{path}/delta_table_partition"),
                "test",
                query,
                &expected_results,
            )
            .await?;

            let explain_plan = rt
                .datafusion()
                .query_builder(&format!("EXPLAIN {query}"))
                .build()
                .run()
                .await
                .map_err(|e| format!("query `{query}` to plan: {e}"))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| format!("query `{query}` to results: {e}"))?;

            let pretty_explain_plan = pretty_format_batches(&explain_plan)
                .expect("failed to format explain plan")
                .to_string();

            // The explain plan should contain only the partitions greater than 2025-01-01
            assert!(pretty_explain_plan.contains("date_col=%2B10999-12-31"));
            assert!(pretty_explain_plan.contains("date_col=2030-06-15"));
            assert!(!pretty_explain_plan.contains("date_col=2025-01-01"));
            assert!(!pretty_explain_plan.contains("date_col=2024-02-04"));

            Ok(())
        })
        .await
}

/// Tests min/max file-level pruning on a non-partitioned Delta Lake table with timestamp columns.
///
/// Dataset: 500 rows across 10 Parquet files, each file covers one day (2025-01-01 .. 2025-01-10)
/// with non-overlapping `event_ts` ranges, enabling file-level pruning via Parquet column statistics.
#[tokio::test]
async fn query_delta_lake_with_timestamp_pruning() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let path = setup_test_data(
                "https://spiceai-public-datasets.s3.us-east-1.amazonaws.com/delta_firewall_events.zip",
                "delta_firewall_events",
            )
            .await?;
            let _hook = FileCleanup { path: path.clone() };

            let app = AppBuilder::new("delta_lake_timestamp_pruning")
                .with_dataset(make_delta_lake_dataset(
                    &format!("{path}/firewall_events"),
                    "events",
                    false,
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

            let pruning_queries = [
                (
                    "ts_lte_jan03",
                    "SELECT * FROM events WHERE event_ts <= '2025-01-03'",
                ),
                (
                    "ts_lt_jan03",
                    "SELECT * FROM events WHERE event_ts < '2025-01-03'",
                ),
                (
                    "ts_gte_jan10",
                    "SELECT * FROM events WHERE event_ts >= '2025-01-10'",
                ),
                (
                    "ts_between_jan03_jan05",
                    "SELECT * FROM events WHERE event_ts BETWEEN '2025-01-03' AND '2025-01-05'",
                ),
            ];

            // We use `FORMAT TREE` to verify file-level pruning as it reports the number of files to scan in concise aggregated manner: `files: 2`.
            for (snapshot_name, query) in pruning_queries {
                let explain_query = format!("EXPLAIN FORMAT TREE {query}");
                let explain_result = rt
                    .datafusion()
                    .query_builder(&explain_query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("EXPLAIN query failed: {e}"))?
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("EXPLAIN query results: {e}"))?;

                let explain_plan = pretty_format_batches(&explain_result)
                    .expect("failed to format explain plan")
                    .to_string();

                insta::with_settings!({
                    description => format!("Query: {query}"),
                    omit_expression => true
                }, {
                    insta::assert_snapshot!(snapshot_name, explain_plan);
                });
            }

            Ok(())
        })
        .await
}

/// Tests that Delta Lake tables with `columnMapping.mode = name` return data with correct
/// logical column names, including nested struct fields which are stored under physical UUIDs.
/// Runs multiple queries to verify projections, filters, aggregations, and nested type access.
#[tokio::test]
async fn query_delta_lake_column_mapping_mode_name() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let path = setup_test_data(
                "https://spiceai-public-datasets.s3.us-east-1.amazonaws.com/delta_column_mapping_name.zip",
                "delta_column_mapping_name",
            )
            .await?;
            let _hook = FileCleanup { path: path.clone() };

            let app = AppBuilder::new("delta_lake_column_mapping_name")
                .with_dataset(make_delta_lake_dataset(
                    &format!("{path}/table-with-columnmapping-mode-name"),
                    "col_mapping_name",
                    false,
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

            let test_cases: Vec<(&str, Vec<&str>)> = vec![
                // Full scan with all column types + nested struct + array
                (
                    "SELECT ByteType, ShortType, IntegerType, LongType, BooleanType, StringType, nested_struct, array_of_prims FROM col_mapping_name ORDER BY IntegerType",
                    vec![
                        "+----------+-----------+-------------+----------+-------------+------------+-----------------------+----------------+",
                        "| ByteType | ShortType | IntegerType | LongType | BooleanType | StringType | nested_struct         | array_of_prims |",
                        "+----------+-----------+-------------+----------+-------------+------------+-----------------------+----------------+",
                        "| 0        | 0         | 0           | 0        | true        | 0          | {aa: 0, ac: {aca: 0}} | [0, 1]         |",
                        "| 1        | 1         | 1           | 1        | false       | 1          | {aa: 1, ac: {aca: 1}} | [1, 2]         |",
                        "| 2        | 2         | 2           | 2        | true        | 2          | {aa: 2, ac: {aca: 2}} | [2, 3]         |",
                        "| 3        | 3         | 3           | 3        | false       | 3          | {aa: 3, ac: {aca: 3}} | [3, 4]         |",
                        "| 4        | 4         | 4           | 4        | true        | 4          | {aa: 4, ac: {aca: 4}} | [4, 5]         |",
                        "|          |           |             |          |             |            |                       |                |",
                        "+----------+-----------+-------------+----------+-------------+------------+-----------------------+----------------+",
                    ],
                ),
                // Projection: subset of columns
                (
                    "SELECT IntegerType, StringType FROM col_mapping_name ORDER BY IntegerType",
                    vec![
                        "+-------------+------------+",
                        "| IntegerType | StringType |",
                        "+-------------+------------+",
                        "| 0           | 0          |",
                        "| 1           | 1          |",
                        "| 2           | 2          |",
                        "| 3           | 3          |",
                        "| 4           | 4          |",
                        "|             |            |",
                        "+-------------+------------+",
                    ],
                ),
                // Filter pushdown with logical column names
                (
                    "SELECT IntegerType, StringType FROM col_mapping_name WHERE IntegerType > 2 ORDER BY IntegerType",
                    vec![
                        "+-------------+------------+",
                        "| IntegerType | StringType |",
                        "+-------------+------------+",
                        "| 3           | 3          |",
                        "| 4           | 4          |",
                        "+-------------+------------+",
                    ],
                ),
                // Aggregation
                (
                    "SELECT count(*) as cnt, sum(IntegerType) as total FROM col_mapping_name",
                    vec![
                        "+-----+-------+",
                        "| cnt | total |",
                        "+-----+-------+",
                        "| 6   | 10    |",
                        "+-----+-------+",
                    ],
                ),
                // Nested struct field access
                (
                    "SELECT nested_struct['aa'] as aa, nested_struct['ac']['aca'] as aca FROM col_mapping_name WHERE IntegerType <= 2 ORDER BY IntegerType",
                    vec![
                        "+----+-----+",
                        "| aa | aca |",
                        "+----+-----+",
                        "| 0  | 0   |",
                        "| 1  | 1   |",
                        "| 2  | 2   |",
                        "+----+-----+",
                    ],
                ),
            ];

            for (query, expected_results) in &test_cases {
                let query_result = rt
                    .datafusion()
                    .query_builder(query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{query}` to plan: {e}"))?;

                let data = query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("query `{query}` to results: {e}"))?;

                assert_batches_eq!(expected_results, &data);
            }

            Ok(())
        })
        .await
}

/// Tests that Delta Lake tables with `columnMapping.mode = id` return data with correct
/// logical column names.
#[tokio::test]
async fn query_delta_lake_column_mapping_mode_id() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let path = setup_test_data(
                "https://spiceai-public-datasets.s3.us-east-1.amazonaws.com/delta_column_mapping_id.zip",
                "delta_column_mapping_id",
            )
            .await?;
            let _hook = FileCleanup { path: path.clone() };

            let app = AppBuilder::new("delta_lake_column_mapping_id")
                .with_dataset(make_delta_lake_dataset(
                    &format!("{path}/table-with-columnmapping-mode-id"),
                    "col_mapping_id",
                    false,
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

            let test_cases: Vec<(&str, Vec<&str>)> = vec![
                // Full scan with all column types + nested struct + array
                (
                    "SELECT ByteType, ShortType, IntegerType, LongType, BooleanType, StringType, nested_struct, array_of_prims FROM col_mapping_id ORDER BY IntegerType",
                    vec![
                        "+----------+-----------+-------------+----------+-------------+------------+-----------------------+----------------+",
                        "| ByteType | ShortType | IntegerType | LongType | BooleanType | StringType | nested_struct         | array_of_prims |",
                        "+----------+-----------+-------------+----------+-------------+------------+-----------------------+----------------+",
                        "| 0        | 0         | 0           | 0        | true        | 0          | {aa: 0, ac: {aca: 0}} | [0, 1]         |",
                        "| 1        | 1         | 1           | 1        | false       | 1          | {aa: 1, ac: {aca: 1}} | [1, 2]         |",
                        "| 2        | 2         | 2           | 2        | true        | 2          | {aa: 2, ac: {aca: 2}} | [2, 3]         |",
                        "| 3        | 3         | 3           | 3        | false       | 3          | {aa: 3, ac: {aca: 3}} | [3, 4]         |",
                        "| 4        | 4         | 4           | 4        | true        | 4          | {aa: 4, ac: {aca: 4}} | [4, 5]         |",
                        "|          |           |             |          |             |            |                       |                |",
                        "+----------+-----------+-------------+----------+-------------+------------+-----------------------+----------------+",
                    ],
                ),
                // Filter pushdown with logical column names
                (
                    "SELECT IntegerType, StringType FROM col_mapping_id WHERE IntegerType > 2 ORDER BY IntegerType",
                    vec![
                        "+-------------+------------+",
                        "| IntegerType | StringType |",
                        "+-------------+------------+",
                        "| 3           | 3          |",
                        "| 4           | 4          |",
                        "+-------------+------------+",
                    ],
                ),
                // Nested struct field access
                (
                    "SELECT nested_struct['aa'] as aa, nested_struct['ac']['aca'] as aca FROM col_mapping_id WHERE IntegerType <= 2 ORDER BY IntegerType",
                    vec![
                        "+----+-----+",
                        "| aa | aca |",
                        "+----+-----+",
                        "| 0  | 0   |",
                        "| 1  | 1   |",
                        "| 2  | 2   |",
                        "+----+-----+",
                    ],
                ),
            ];

            for (query, expected_results) in &test_cases {
                let query_result = rt
                    .datafusion()
                    .query_builder(query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{query}` to plan: {e}"))?;

                let data = query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("query `{query}` to results: {e}"))?;

                assert_batches_eq!(expected_results, &data);
            }

            Ok(())
        })
        .await
}

/// Tests that Delta Lake tables with column mapping and partitions correctly translate
/// physical partition keys to logical column names. Also exercises deletion vectors.
#[tokio::test]
async fn query_delta_lake_column_mapping_with_partitions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let path = setup_test_data(
                "https://spiceai-public-datasets.s3.us-east-1.amazonaws.com/delta_column_mapping_partitioned.zip",
                "delta_column_mapping_partitioned",
            )
            .await?;
            let _hook = FileCleanup { path: path.clone() };

            let app = AppBuilder::new("delta_lake_column_mapping_partitions")
                .with_dataset(make_delta_lake_dataset(
                    &format!("{path}/dv-with-columnmapping/delta"),
                    "cm_partitioned",
                    false,
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

            let test_cases: Vec<(&str, Vec<&str>)> = vec![
                // Full scan with limit — verifies partition column values are correctly populated
                (
                    "SELECT part, col1, col2 FROM cm_partitioned ORDER BY part, col1 LIMIT 10",
                    vec![
                        "+------+------+------+",
                        "| part | col1 | col2 |",
                        "+------+------+------+",
                        "| 0    | 30   | foo0 |",
                        "| 0    | 40   | foo0 |",
                        "| 1    | 1    | foo1 |",
                        "| 1    | 11   | foo1 |",
                        "| 1    | 21   | foo1 |",
                        "| 1    | 31   | foo1 |",
                        "| 1    | 41   | foo1 |",
                        "| 2    | 32   | foo2 |",
                        "| 2    | 42   | foo2 |",
                        "| 3    | 3    | foo3 |",
                        "+------+------+------+",
                    ],
                ),
                // Filter on partition column (logical name)
                (
                    "SELECT part, col1 FROM cm_partitioned WHERE part = 3 ORDER BY col1",
                    vec![
                        "+------+------+",
                        "| part | col1 |",
                        "+------+------+",
                        "| 3    | 3    |",
                        "| 3    | 13   |",
                        "| 3    | 23   |",
                        "| 3    | 33   |",
                        "| 3    | 43   |",
                        "+------+------+",
                    ],
                ),
                // Aggregation — row count confirms deletion vectors are applied
                (
                    "SELECT count(*) as cnt FROM cm_partitioned",
                    vec![
                        "+-----+",
                        "| cnt |",
                        "+-----+",
                        "| 35  |",
                        "+-----+",
                    ],
                ),
            ];

            for (query, expected_results) in &test_cases {
                let query_result = rt
                    .datafusion()
                    .query_builder(query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{query}` to plan: {e}"))?;

                let data = query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("query `{query}` to results: {e}"))?;

                assert_batches_eq!(expected_results, &data);
            }

            // Verify partition pruning: EXPLAIN for `part = 3` should only scan
            // the physical partition directory for partition 3 and not others.
            let partition_query =
                "SELECT part, col1 FROM cm_partitioned WHERE part = 3 ORDER BY col1";
            let explain_plan = rt
                .datafusion()
                .query_builder(&format!("EXPLAIN {partition_query}"))
                .build()
                .run()
                .await
                .map_err(|e| format!("query EXPLAIN to plan: {e}"))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| format!("query EXPLAIN to results: {e}"))?;

            let pretty_explain_plan = pretty_format_batches(&explain_plan)
                .expect("failed to format explain plan")
                .to_string();

            // Only the physical partition dir for part=3 should appear
            for i in 0..10 {
                let pattern = format!("col-60c949ca-b8bc-4330-b931-b73fb4c60037={i}");
                if i == 3 {
                    assert!(
                        pretty_explain_plan.contains(&pattern),
                        "EXPLAIN should show partition {i} directory"
                    );
                } else {
                    assert!(
                        !pretty_explain_plan.contains(&pattern),
                        "EXPLAIN should not show partition {i} directory"
                    );
                }
            }

            Ok(())
        })
        .await
}
