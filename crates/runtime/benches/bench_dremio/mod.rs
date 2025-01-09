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

use crate::results::BenchmarkResultsBuilder;
use spicepod::component::{dataset::Dataset, params::Params};
use test_framework::queries::{
    get_clickbench_test_queries, get_tpcds_test_queries, get_tpch_test_queries, QueryOverrides,
};

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    bench_name: &str,
) -> Result<(), String> {
    let test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(None),
        "tpcds" => get_tpcds_test_queries(None),
        "clickbench" => get_clickbench_test_queries(Some(QueryOverrides::Dremio)),
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        let verify_query_results =
            query_name.starts_with("tpch_q") || query_name.starts_with("tpcds_q");
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "dremio",
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
            .with_dataset(make_dataset("tpch.customer", "customer"))
            .with_dataset(make_dataset("tpch.lineitem", "lineitem"))
            .with_dataset(make_dataset("tpch.part", "part"))
            .with_dataset(make_dataset("tpch.partsupp", "partsupp"))
            .with_dataset(make_dataset("tpch.orders", "orders"))
            .with_dataset(make_dataset("tpch.nation", "nation"))
            .with_dataset(make_dataset("tpch.region", "region"))
            .with_dataset(make_dataset("tpch.supplier", "supplier"))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset("tpcds.call_center", "call_center"))
            .with_dataset(make_dataset("tpcds.catalog_page", "catalog_page"))
            .with_dataset(make_dataset("tpcds.catalog_sales", "catalog_sales"))
            .with_dataset(make_dataset("tpcds.catalog_returns", "catalog_returns"))
            .with_dataset(make_dataset("tpcds.income_band", "income_band"))
            .with_dataset(make_dataset("tpcds.inventory", "inventory"))
            .with_dataset(make_dataset("tpcds.store_sales", "store_sales"))
            .with_dataset(make_dataset("tpcds.store_returns", "store_returns"))
            .with_dataset(make_dataset("tpcds.web_sales", "web_sales"))
            .with_dataset(make_dataset("tpcds.web_returns", "web_returns"))
            .with_dataset(make_dataset("tpcds.customer", "customer"))
            .with_dataset(make_dataset("tpcds.customer_address", "customer_address"))
            .with_dataset(make_dataset(
                "tpcds.customer_demographics",
                "customer_demographics",
            ))
            .with_dataset(make_dataset("tpcds.date_dim", "date_dim"))
            .with_dataset(make_dataset(
                "tpcds.household_demographics",
                "household_demographics",
            ))
            .with_dataset(make_dataset("tpcds.item", "item"))
            .with_dataset(make_dataset("tpcds.promotion", "promotion"))
            .with_dataset(make_dataset("tpcds.reason", "reason"))
            .with_dataset(make_dataset("tpcds.ship_mode", "ship_mode"))
            .with_dataset(make_dataset("tpcds.store", "store"))
            .with_dataset(make_dataset("tpcds.time_dim", "time_dim"))
            .with_dataset(make_dataset("tpcds.warehouse", "warehouse"))
            .with_dataset(make_dataset("tpcds.web_page", "web_page"))
            .with_dataset(make_dataset("tpcds.web_site", "web_site"))),
        "clickbench" => Ok(app_builder.with_dataset(make_dataset("clickbench.hits", "hits"))),
        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("dremio:{path}"), name.to_string());
    dataset.params = Some(get_params());
    dataset
}

fn get_params() -> Params {
    Params::from_string_map(
        vec![
            (
                "dremio_endpoint".to_string(),
                "grpc://20.163.171.8:32010".to_string(),
            ),
            (
                "dremio_username".to_string(),
                "${ env:DREMIO_USERNAME }".to_string(),
            ),
            (
                "dremio_password".to_string(),
                "${ env:DREMIO_PASSWORD }".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    )
}
