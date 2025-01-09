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

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    bench_name: &str,
) -> Result<(), String> {
    let test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(None),
        "tpcds" => get_tpcds_test_queries(Some(QueryOverrides::MySQL)),
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "mysql",
            query_name,
            query,
            true,
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
    let mut dataset = Dataset::new(format!("mysql:{path}"), name.to_string());
    dataset.params = Some(get_params(bench_name));
    dataset
}

fn get_params(bench_name: &str) -> Params {
    let host = std::env::var("MYSQL_BENCHMARK_MYSQL_HOST").unwrap_or_default();
    let user = std::env::var("MYSQL_BENCHMARK_MYSQL_USER").unwrap_or_default();
    let pass = std::env::var("MYSQL_BENCHMARK_MYSQL_PASS").unwrap_or_default();
    let db = match bench_name {
        "tpch" => std::env::var("MYSQL_TPCH_BENCHMARK_MYSQL_DB").unwrap_or_default(),
        "tpcds" => std::env::var("MYSQL_TPCDS_BENCHMARK_MYSQL_DB").unwrap_or_default(),
        _ => panic!("Only tpcds or tpch benchmark suites are supported"),
    };

    Params::from_string_map(
        vec![
            ("mysql_host".to_string(), host),
            ("mysql_user".to_string(), user),
            ("mysql_db".to_string(), db),
            ("mysql_pass".to_string(), pass),
            ("mysql_tcp_port".to_string(), "3306".to_string()),
            ("mysql_sslmode".to_string(), "preferred".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}
