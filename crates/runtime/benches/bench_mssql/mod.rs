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
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "mssql",
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
    // wait for 30 seconds for MS SQL server to restore
    std::thread::sleep(std::time::Duration::from_secs(30));

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
        _ => Err("Only tpch benchmark suites are supported".to_string()),
    }
}

fn make_dataset(path: &str, name: &str, bench_name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("mssql:{path}"), name.to_string());
    dataset.params = Some(get_params(bench_name));
    dataset
}

fn get_params(bench_name: &str) -> Params {
    let host = std::env::var("MSSQL_BENCHMARK_MSSQL_HOST").unwrap_or_default();
    let user = std::env::var("MSSQL_BENCHMARK_MSSQL_USER").unwrap_or_default();
    let pass = std::env::var("MSSQL_BENCHMARK_MSSQL_PASS").unwrap_or_default();
    let db = match bench_name {
        "tpch" => std::env::var("MSSQL_TPCH_BENCHMARK_MSSQL_DB").unwrap_or_default(),
        _ => panic!("Only tpch benchmark suites are supported"),
    };

    Params::from_string_map(
        vec![
            ("mssql_host".to_string(), host),
            ("mssql_username".to_string(), user),
            ("mssql_database".to_string(), db),
            ("mssql_password".to_string(), pass),
            ("mssql_encrypt".to_string(), "false".to_string()),
            (
                "mssql_trust_server_certificate".to_string(),
                "true".to_string(),
            ),
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
        ("tpch_q12", include_str!("./tpch_q12.sql")),
        ("tpch_q13", include_str!("../queries/tpch/q13.sql")),
        ("tpch_q14", include_str!("../queries/tpch/q14.sql")),
        // tpch_q15 has a view creation which we don't support by design
        ("tpch_q16", include_str!("../queries/tpch/q16.sql")),
        ("tpch_q17", include_str!("../queries/tpch/q17.sql")),
        ("tpch_q18", include_str!("../queries/tpch/q18.sql")),
        ("tpch_q19", include_str!("./tpch_q19.sql")),
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
