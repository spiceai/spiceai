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

use app::AppBuilder;
use runtime::Runtime;
use test_framework::queries::{get_tpcds_test_queries, get_tpch_test_queries, QueryOverrides};

use crate::results::BenchmarkResultsBuilder;
use spicepod::component::{dataset::Dataset, params::Params};

use duckdb::Connection;
use std::time::Duration;
use tokio::{task::JoinHandle, time::sleep};

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    bench_name: &str,
) -> Result<(), String> {
    let test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(None),
        "tpcds" => get_tpcds_test_queries(Some(QueryOverrides::DuckDB)),
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        let verify_query_results =
            query_name.starts_with("tpch_q") || query_name.starts_with("tpcds_q");
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "duckdb",
            query_name,
            query,
            verify_query_results,
        )
        .await
        {
            errors.push(format!("Query {query_name} failed with error: {e}"));
        };
    }

    if !errors.is_empty() {
        tracing::error!("There are failed queries:\n{}", errors.join("\n"));
    }

    Ok(())
}

pub fn build_app(app_builder: AppBuilder, bench_name: &str) -> Result<AppBuilder, String> {
    match bench_name {
        "tpch" => Ok(app_builder
            .with_dataset(make_dataset("customer", "customer", bench_name))
            .with_dataset(make_dataset("lineitem", "lineitem", bench_name))
            .with_dataset(make_dataset("part", "part", bench_name))
            .with_dataset(make_dataset("partsupp", "partsupp", bench_name))
            .with_dataset(make_dataset("orders", "orders", bench_name))
            .with_dataset(make_dataset("nation", "nation", bench_name))
            .with_dataset(make_dataset("region", "region", bench_name))
            .with_dataset(make_dataset("supplier", "supplier", bench_name))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset("call_center", "call_center", bench_name))
            .with_dataset(make_dataset("catalog_page", "catalog_page", bench_name))
            .with_dataset(make_dataset("catalog_sales", "catalog_sales", bench_name))
            .with_dataset(make_dataset(
                "catalog_returns",
                "catalog_returns",
                bench_name,
            ))
            .with_dataset(make_dataset("income_band", "income_band", bench_name))
            .with_dataset(make_dataset("inventory", "inventory", bench_name))
            .with_dataset(make_dataset("store_sales", "store_sales", bench_name))
            .with_dataset(make_dataset("store_returns", "store_returns", bench_name))
            .with_dataset(make_dataset("web_sales", "web_sales", bench_name))
            .with_dataset(make_dataset("web_returns", "web_returns", bench_name))
            .with_dataset(make_dataset("customer", "customer", bench_name))
            .with_dataset(make_dataset(
                "customer_address",
                "customer_address",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "customer_demographics",
                "customer_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset("date_dim", "date_dim", bench_name))
            .with_dataset(make_dataset(
                "household_demographics",
                "household_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset("item", "item", bench_name))
            .with_dataset(make_dataset("promotion", "promotion", bench_name))
            .with_dataset(make_dataset("reason", "reason", bench_name))
            .with_dataset(make_dataset("ship_mode", "ship_mode", bench_name))
            .with_dataset(make_dataset("store", "store", bench_name))
            .with_dataset(make_dataset("time_dim", "time_dim", bench_name))
            .with_dataset(make_dataset("warehouse", "warehouse", bench_name))
            .with_dataset(make_dataset("web_page", "web_page", bench_name))
            .with_dataset(make_dataset("web_site", "web_site", bench_name))),
        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
}

fn make_dataset(path: &str, name: &str, bench_name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("duckdb:{path}"), name.to_string());
    dataset.params = Some(get_params(bench_name));
    dataset
}

fn get_params(bench_name: &str) -> Params {
    let db_file = format!("./{bench_name}.db");
    Params::from_string_map(
        vec![("duckdb_open".to_string(), db_file)]
            .into_iter()
            .collect(),
    )
}

/// Spawn a new thread to load data into the database over a period of time
/// This is useful for benchmarks that require data changes over time, like append-mode acceleration
#[allow(clippy::too_many_lines)]
pub(crate) fn delayed_source_load_to_parquet(
    bench_name: &str,
    load_count: usize,
    load_interval: Duration,
    scale_factor: f64,
) -> Result<JoinHandle<Result<(), String>>, String> {
    if std::fs::exists(format!("./{bench_name}.db")).map_err(|e| e.to_string())? {
        std::fs::remove_file(format!("./{bench_name}.db")).map_err(|e| e.to_string())?;
    }

    let tables = match bench_name {
        "tpch" => [
            ("customer", "c_created_at"),
            ("lineitem", "l_created_at"),
            ("nation", "n_created_at"),
            ("orders", "o_created_at"),
            ("part", "p_created_at"),
            ("partsupp", "ps_created_at"),
            ("region", "r_created_at"),
            ("supplier", "s_created_at"),
        ]
        .to_vec(),
        "tpcds" => [
            ("call_center", "cc_created_at"),
            ("catalog_page", "cp_created_at"),
            ("catalog_sales", "cs_created_at"),
            ("catalog_returns", "cr_created_at"),
            ("income_band", "ib_created_at"),
            ("inventory", "i_created_at"),
            ("store_sales", "ss_created_at"),
            ("store_returns", "sr_created_at"),
            ("web_sales", "ws_created_at"),
            ("web_returns", "wr_created_at"),
            ("customer", "c_created_at"),
            ("customer_address", "ca_created_at"),
            ("customer_demographics", "cd_created_at"),
            ("date_dim", "d_created_at"),
            ("household_demographics", "hd_created_at"),
            ("item", "i_created_at"),
            ("promotion", "p_created_at"),
            ("reason", "r_created_at"),
            ("ship_mode", "sm_created_at"),
            ("store", "s_created_at"),
            ("time_dim", "t_created_at"),
            ("warehouse", "w_created_at"),
            ("web_page", "wp_created_at"),
            ("web_site", "ws_created_at"),
        ]
        .to_vec(),
        "clickbench" => vec![("hits_delayed", "created_at")],
        _ => {
            return Err(
                "Only tpch, tpcds and clickbench benchmark suites are supported".to_string(),
            )
        }
    };

    for (table, _) in &tables {
        if std::fs::exists(format!("./{table}.parquet")).map_err(|e| e.to_string())? {
            std::fs::remove_file(format!("./{table}.parquet")).map_err(|e| e.to_string())?;
        }
    }

    let bench_name = bench_name.to_string();

    Ok(tokio::spawn(async move {
        let dest_db_file = format!("./{bench_name}.db");

        // setup tasks
        match bench_name.as_str() {
            "tpcds" => {
                let mut setup_sql = format!(
                    "
                INSTALL tpcds;
                LOAD tpcds;
                BEGIN;
                CALL dsdgen(sf={scale_factor}, suffix='_gen');
            "
                );

                for (table, column) in &tables {
                    setup_sql += &format!(
                        "
                    CREATE TABLE {table} AS SELECT * FROM {table}_gen WHERE 1=0;
                    ALTER TABLE {table} ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                "
                    );
                }

                setup_sql += "COMMIT;";

                println!("Running TPCDS data setup");
                let dest_conn = Connection::open(&dest_db_file).map_err(|e| e.to_string())?;
                dest_conn
                    .execute_batch(&setup_sql)
                    .map_err(|e| e.to_string())?;
            }
            "clickbench" => {
                // import the parquet file into the database so we can use it for OFFSET delayed loading
                // limit to 40 million rows because the file connector goes OOM with the full file
                let setup_sql = "
                    BEGIN;
                    CREATE TABLE hits AS SELECT * FROM read_parquet('hits.parquet') LIMIT 40000000;
                    CREATE TABLE hits_delayed AS SELECT * FROM hits WHERE 1=0;
                    ALTER TABLE hits_delayed ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    COMMIT;
                    ";

                println!("Running Clickbench data setup");
                let dest_conn = Connection::open(&dest_db_file).map_err(|e| e.to_string())?;
                dest_conn
                    .execute_batch(setup_sql)
                    .map_err(|e| e.to_string())?;
            }
            _ => {}
        };

        // data generation
        for i in 0..load_count {
            // hold the connection in the loop so it can get dropped while sleeping, so the DuckDB cache can be flushed
            let dest_conn = Connection::open(&dest_db_file).map_err(|e| e.to_string())?;
            println!("Loading data for {bench_name} benchmark suite, iteration {i}");
            match bench_name.as_str() {
                "tpch" => {
                    let mut sql = format!(
                        "
                    INSTALL tpch;
                    LOAD tpch;
                    BEGIN;
                    CALL dbgen(sf={scale_factor}, children={load_count}, step={i}, suffix={suffix});
                    ",
                        suffix = if i == 0 { "''" } else { "'_new'" },
                    );

                    for (table, column) in &tables {
                        if i == 0 {
                            sql += &format!("
                            ALTER TABLE {table} ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                            COPY {table} TO '{table}.parquet' (FORMAT 'parquet');
                            ");
                        } else {
                            sql += &format!("
                            ALTER TABLE {table}_new ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                            INSERT INTO {table} SELECT * FROM {table}_new;
                            DROP TABLE {table}_new;
                            COPY {table} TO '{table}.parquet' (FORMAT 'parquet');
                            ");
                        }
                    }

                    sql += "COMMIT;";

                    dest_conn.execute_batch(&sql).map_err(|e| e.to_string())?;
                }
                "tpcds" => {
                    let mut sql = "BEGIN;".to_string();

                    for (table, column) in &tables {
                        // DuckDB's TPCDS generation doesn't support partitioning and generating in steps
                        // Instead, generate the whole dataset and load it with incrementally increasing OFFSET and LIMIT
                        sql += &format!("
                            INSERT INTO {table} SELECT *, CURRENT_TIMESTAMP AS {column} FROM {table}_gen LIMIT (SELECT COUNT(*) / {load_count} FROM {table}_gen) OFFSET (SELECT COUNT(*) / {load_count} * {i} FROM {table}_gen);
                            COPY {table} TO '{table}.parquet' (FORMAT 'parquet');
                        ");
                    }

                    sql += "COMMIT;";

                    dest_conn.execute_batch(&sql).map_err(|e| e.to_string())?;
                }
                "clickbench" => {
                    let sql = format!("
                    BEGIN;
                    INSERT INTO hits_delayed SELECT *, CURRENT_TIMESTAMP AS created_at FROM hits LIMIT (SELECT COUNT(*) / {load_count} FROM hits) OFFSET (SELECT COUNT(*) / {load_count} * {i} FROM hits);
                    COPY hits_delayed TO 'hits_delayed.parquet' (FORMAT 'parquet');
                    COMMIT;
                    ");

                    dest_conn.execute_batch(&sql).map_err(|e| e.to_string())?;
                }
                _ => {
                    return Err(
                        "Only tpch, tpcds, clickbench benchmark suites are supported".to_string(),
                    );
                }
            }

            sleep(load_interval).await;
        }

        // teardown
        if bench_name == "tpcds" {
            // cleanup _gen data
            let mut cleanup_sql = "BEGIN;".to_string();
            for (table, _) in &tables {
                cleanup_sql += &format!("DROP TABLE {table}_gen;");
            }

            cleanup_sql += "COMMIT;";

            let dest_conn = Connection::open(&dest_db_file).map_err(|e| e.to_string())?;
            dest_conn
                .execute_batch(&cleanup_sql)
                .map_err(|e| e.to_string())?;
        }

        Ok::<(), String>(())
    }))
}
