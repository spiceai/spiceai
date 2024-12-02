/*
Copyright 2024 The Spice.ai OSS Authors

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

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    bench_name: &str,
) -> Result<(), String> {
    let test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(),
        "tpcds" => get_tpcds_test_queries(),
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        let verify_query_results = query_name.starts_with("tpch_q");
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "databricks_delta",
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
            .with_dataset(make_dataset("spiceai_sandbox.tpch.customer", "customer"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.lineitem", "lineitem"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.part", "part"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.partsupp", "partsupp"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.orders", "orders"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.nation", "nation"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.region", "region"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.supplier", "supplier"))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.call_center",
                "call_center",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.catalog_page",
                "catalog_page",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.catalog_returns",
                "catalog_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.catalog_sales",
                "catalog_sales",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.customer", "customer"))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.customer_address",
                "customer_address",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.customer_demographics",
                "customer_demographics",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.date_dim", "date_dim"))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.household_demographics",
                "household_demographics",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.income_band",
                "income_band",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.inventory", "inventory"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.item", "item"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.promotion", "promotion"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.reason", "reason"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.ship_mode", "ship_mode"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.store", "store"))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.store_returns",
                "store_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.store_sales",
                "store_sales",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.time_dim", "time_dim"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.warehouse", "warehouse"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.web_page", "web_page"))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds.web_returns",
                "web_returns",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.web_sales", "web_sales"))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds.web_site", "web_site"))),
        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("databricks:{path}"), name.to_string());
    dataset.params = Some(get_params());
    dataset
}

fn get_params() -> Params {
    Params::from_string_map(
        vec![
            (
                "databricks_endpoint".to_string(),
                "${ env:DATABRICKS_HOST }".to_string(),
            ),
            (
                "databricks_token".to_string(),
                "${ env:DATABRICKS_TOKEN }".to_string(),
            ),
            (
                "databricks_aws_secret_access_key".to_string(),
                "${ env:AWS_DATABRICKS_DELTA_SECRET_ACCESS_KEY }".to_string(),
            ),
            (
                "databricks_aws_access_key_id".to_string(),
                "${ env:AWS_DATABRICKS_DELTA_ACCESS_KEY_ID }".to_string(),
            ),
            ("client_timeout".to_string(), "120s".to_string()),
            ("mode".to_string(), "delta_lake".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}

fn get_tpch_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tpch_q1", include_str!("../queries/tpch/q1.sql")),
        ("tpch_q2", include_str!("../queries/tpch/q2.sql")),
        ("tpch_q3", include_str!("../queries/tpch/q3.sql")),
        ("tpch_q4", include_str!("../queries/tpch/q4.sql")),
        ("tpch_q5", include_str!("../queries/tpch/q5.sql")),
        ("tpch_q6", include_str!("../queries/tpch/q6.sql")),
        ("tpch_q7", include_str!("../queries/tpch/q7.sql")),
        ("tpch_q8", include_str!("../queries/tpch/q8.sql")),
        ("tpch_q9", include_str!("../queries/tpch/q9.sql")),
        ("tpch_q10", include_str!("../queries/tpch/q10.sql")),
        ("tpch_q11", include_str!("../queries/tpch/q11.sql")),
        ("tpch_q12", include_str!("../queries/tpch/q12.sql")),
        ("tpch_q13", include_str!("../queries/tpch/q13.sql")),
        ("tpch_q14", include_str!("../queries/tpch/q14.sql")),
        // tpch_q15 has a view creation which we don't support by design
        ("tpch_q16", include_str!("../queries/tpch/q16.sql")),
        ("tpch_q17", include_str!("../queries/tpch/q17.sql")),
        ("tpch_q18", include_str!("../queries/tpch/q18.sql")),
        ("tpch_q19", include_str!("../queries/tpch/q19.sql")),
        ("tpch_q20", include_str!("../queries/tpch/q20.sql")),
        ("tpch_q21", include_str!("../queries/tpch/q21.sql")),
        ("tpch_q22", include_str!("../queries/tpch/q22.sql")),
        (
            "tpch_simple_q1",
            include_str!("../queries/tpch/simple_q1.sql"),
        ),
        (
            "tpch_simple_q2",
            include_str!("../queries/tpch/simple_q2.sql"),
        ),
        (
            "tpch_simple_q3",
            include_str!("../queries/tpch/simple_q3.sql"),
        ),
        (
            "tpch_simple_q4",
            include_str!("../queries/tpch/simple_q4.sql"),
        ),
        (
            "tpch_simple_q5",
            include_str!("../queries/tpch/simple_q5.sql"),
        ),
    ]
}

macro_rules! generate_tpcds_queries {
    ( $( $i:literal ),* ) => {
        vec![
            $(
                (
                    concat!("tpcds_q", stringify!($i)),
                    include_str!(concat!("../queries/tpcds/q", stringify!($i), ".sql"))
                )
            ),*
        ]
    }
}

fn get_tpcds_test_queries() -> Vec<(&'static str, &'static str)> {
    generate_tpcds_queries!(
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
        49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
        72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94,
        95, 96, 97, 98, 99
    )
}
