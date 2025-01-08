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

use arrow::array::AsArray;
use runtime::Runtime;
use std::time::Duration;
use test_framework::queries::QueryOverrides;
use tokio::time::sleep;

use app::AppBuilder;

use spicepod::component::dataset::{
    acceleration::{Acceleration, RefreshMode},
    Dataset,
};

use crate::run_query;

use super::{
    get_clickbench_test_queries, get_tpcds_test_queries, get_tpch_test_queries,
    BenchmarkResultsBuilder,
};

#[allow(clippy::too_many_lines)]
pub(crate) async fn run_file_append(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    bench_name: &str,
    accelerator: Option<Acceleration>,
) -> Result<(), String> {
    let mut test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(None),
        "tpcds" => match accelerator.clone() {
            Some(Acceleration { engine, .. }) => get_tpcds_test_queries(engine.as_deref()),
            None => get_tpcds_test_queries(None),
        },
        "clickbench" => match accelerator.clone() {
            Some(Acceleration { engine, .. }) => {
                get_clickbench_test_queries(engine.as_deref().and_then(QueryOverrides::from_engine))
            }
            None => get_clickbench_test_queries(None),
        },
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let mut errors = Vec::new();
    let is_append = accelerator
        .clone()
        .and_then(|a| a.refresh_mode)
        .is_some_and(|m| m == RefreshMode::Append);

    if is_append {
        // remove q72 if the engine is arrow in append mode because it uses too much memory for the runner
        if bench_name == "tpcds"
            && accelerator
                .clone()
                .and_then(|a| a.engine)
                .is_none_or(|e| e == "arrow")
        {
            test_queries.retain(|(query_name, _)| *query_name != "tpcds_q72");
        }

        let start_time = std::time::Instant::now();

        loop {
            sleep(Duration::from_secs(60 * 4)).await; // refresh interval is 3 minutes - check every 4 minutes

            let (target_table, expected_count) = match bench_name {
                // DuckDB dbgen at scale 10 generates 59.9, not 60, million rows for lineitem when using partitioned generation
                "tpch" => ("lineitem", 59_900_000),
                "tpcds" => ("inventory", 130_000_000),
                "clickbench" => ("hits", 39_000_000),
                _ => return Err(format!("Invalid benchmark to run {bench_name}")),
            };

            // check if the data has finished loading
            let res = run_query(
                rt,
                "duckdb",
                "table_count",
                &format!("SELECT COUNT(*) as table_count FROM {target_table}"),
            )
            .await;

            if let Err(e) = res {
                return Err(format!(
                    "Append mode data load failed. Failed to count rows in lineitem table: {e}"
                ));
            }

            let count = res
                .map_err(|e| e.to_string())?
                .first()
                .ok_or("No rows returned from count query")?
                .column_by_name("table_count")
                .ok_or("No column named table_count")?
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0);
            if count < expected_count {
                if start_time.elapsed() > Duration::from_secs(60 * 60) {
                    // if more than 1 hour has passed, the test has failed
                    tracing::error!("Append mode data load failed. Expected over {expected_count} rows in lineitem table, got {count}");
                    return Err(format!("Append mode data load failed. Expected over {expected_count} rows in lineitem table, got {count}"));
                }

                tracing::info!("Append mode data load in progress. Expected over {expected_count} rows in lineitem table, got {count}");
            } else {
                tracing::info!(
                    "Append mode data load complete. Loaded {count} rows in lineitem table"
                );
                break;
            }
        }
    }

    let bench_name = match (accelerator, is_append) {
        (Some(accelerator), true) => {
            format!(
                "file_{}_{}_append",
                accelerator.engine.unwrap_or("arrow".to_string()),
                accelerator.mode
            )
        }
        _ => "file".to_string(),
    };

    for (query_name, query) in test_queries {
        if let Err(e) = crate::run_query_and_record_result(
            rt,
            benchmark_results,
            &bench_name,
            query_name,
            query,
            false,
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

#[allow(clippy::too_many_lines)]
pub fn build_app(
    app_builder: AppBuilder,
    bench_name: &str,
    acceleration: Option<Acceleration>,
) -> Result<AppBuilder, String> {
    let is_append = acceleration
        .and_then(|a| a.refresh_mode)
        .is_some_and(|m| m == RefreshMode::Append);

    if is_append {
        tracing::info!(
            "Running DuckDB connector in append mode - data will be loaded incrementally"
        );
    }

    match bench_name {
        "tpch" => Ok(app_builder
            .with_dataset(make_dataset(
                "customer.parquet",
                "customer",
                bench_name,
                is_append.then_some("c_created_at"),
            ))
            .with_dataset(make_dataset(
                "lineitem.parquet",
                "lineitem",
                bench_name,
                is_append.then_some("l_created_at"),
            ))
            .with_dataset(make_dataset(
                "orders.parquet",
                "orders",
                bench_name,
                is_append.then_some("o_created_at"),
            ))
            .with_dataset(make_dataset(
                "part.parquet",
                "part",
                bench_name,
                is_append.then_some("p_created_at"),
            ))
            .with_dataset(make_dataset(
                "partsupp.parquet",
                "partsupp",
                bench_name,
                is_append.then_some("ps_created_at"),
            ))
            .with_dataset(make_dataset(
                "region.parquet",
                "region",
                bench_name,
                is_append.then_some("r_created_at"),
            ))
            .with_dataset(make_dataset(
                "nation.parquet",
                "nation",
                bench_name,
                is_append.then_some("n_created_at"),
            ))
            .with_dataset(make_dataset(
                "supplier.parquet",
                "supplier",
                bench_name,
                is_append.then_some("s_created_at"),
            ))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset(
                "call_center.parquet",
                "call_center",
                bench_name,
                is_append.then_some("cc_created_at"),
            ))
            .with_dataset(make_dataset(
                "catalog_page.parquet",
                "catalog_page",
                bench_name,
                is_append.then_some("cp_created_at"),
            ))
            .with_dataset(make_dataset(
                "catalog_returns.parquet",
                "catalog_returns",
                bench_name,
                is_append.then_some("cr_created_at"),
            ))
            .with_dataset(make_dataset(
                "catalog_sales.parquet",
                "catalog_sales",
                bench_name,
                is_append.then_some("cs_created_at"),
            ))
            .with_dataset(make_dataset(
                "customer.parquet",
                "customer",
                bench_name,
                is_append.then_some("c_created_at"),
            ))
            .with_dataset(make_dataset(
                "customer_address.parquet",
                "customer_address",
                bench_name,
                is_append.then_some("ca_created_at"),
            ))
            .with_dataset(make_dataset(
                "customer_demographics.parquet",
                "customer_demographics",
                bench_name,
                is_append.then_some("cd_created_at"),
            ))
            .with_dataset(make_dataset(
                "date_dim.parquet",
                "date_dim",
                bench_name,
                is_append.then_some("d_created_at"),
            ))
            .with_dataset(make_dataset(
                "household_demographics.parquet",
                "household_demographics",
                bench_name,
                is_append.then_some("hd_created_at"),
            ))
            .with_dataset(make_dataset(
                "income_band.parquet",
                "income_band",
                bench_name,
                is_append.then_some("ib_created_at"),
            ))
            .with_dataset(make_dataset(
                "inventory.parquet",
                "inventory",
                bench_name,
                is_append.then_some("i_created_at"),
            ))
            .with_dataset(make_dataset(
                "item.parquet",
                "item",
                bench_name,
                is_append.then_some("i_created_at"),
            ))
            .with_dataset(make_dataset(
                "promotion.parquet",
                "promotion",
                bench_name,
                is_append.then_some("p_created_at"),
            ))
            .with_dataset(make_dataset(
                "reason.parquet",
                "reason",
                bench_name,
                is_append.then_some("r_created_at"),
            ))
            .with_dataset(make_dataset(
                "ship_mode.parquet",
                "ship_mode",
                bench_name,
                is_append.then_some("sm_created_at"),
            ))
            .with_dataset(make_dataset(
                "store.parquet",
                "store",
                bench_name,
                is_append.then_some("s_created_at"),
            ))
            .with_dataset(make_dataset(
                "store_returns.parquet",
                "store_returns",
                bench_name,
                is_append.then_some("sr_created_at"),
            ))
            .with_dataset(make_dataset(
                "store_sales.parquet",
                "store_sales",
                bench_name,
                is_append.then_some("ss_created_at"),
            ))
            .with_dataset(make_dataset(
                "time_dim.parquet",
                "time_dim",
                bench_name,
                is_append.then_some("t_created_at"),
            ))
            .with_dataset(make_dataset(
                "warehouse.parquet",
                "warehouse",
                bench_name,
                is_append.then_some("w_created_at"),
            ))
            .with_dataset(make_dataset(
                "web_page.parquet",
                "web_page",
                bench_name,
                is_append.then_some("wp_created_at"),
            ))
            .with_dataset(make_dataset(
                "web_returns.parquet",
                "web_returns",
                bench_name,
                is_append.then_some("wr_created_at"),
            ))
            .with_dataset(make_dataset(
                "web_sales.parquet",
                "web_sales",
                bench_name,
                is_append.then_some("ws_created_at"),
            ))
            .with_dataset(make_dataset(
                "web_site.parquet",
                "web_site",
                bench_name,
                is_append.then_some("ws_created_at"),
            ))),
        "clickbench" => Ok(app_builder.with_dataset(make_dataset(
            if is_append {
                "hits_delayed.parquet"
            } else {
                "hits.parquet"
            },
            "hits",
            bench_name,
            is_append.then_some("created_at"),
        ))),
        _ => Err(
            "Only tpch and tpcds benchmark suites are supported for the file connector".to_string(),
        ),
    }
}

fn make_dataset(path: &str, name: &str, _bench_name: &str, time_column: Option<&str>) -> Dataset {
    let mut dataset = Dataset::new(format!("file:./{path}"), name.to_string());

    if let Some(time_column) = time_column {
        dataset.time_column = Some(time_column.to_string());
    };

    dataset
}
