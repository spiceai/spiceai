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
) -> Result<(), String> {
    let test_queries = get_test_queries();
    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        if let Err(e) =
            super::run_query_and_record_result(rt, benchmark_results, "mysql", query_name, query)
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

pub fn build_app(app_builder: AppBuilder) -> AppBuilder {
    app_builder
        .with_dataset(make_dataset("customer", "customer"))
        .with_dataset(make_dataset("lineitem", "lineitem"))
        .with_dataset(make_dataset("part", "part"))
        .with_dataset(make_dataset("partsupp", "partsupp"))
        .with_dataset(make_dataset("orders", "orders"))
        .with_dataset(make_dataset("nation", "nation"))
        .with_dataset(make_dataset("region", "region"))
        .with_dataset(make_dataset("supplier", "supplier"))
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("mysql:{path}"), name.to_string());
    dataset.params = Some(get_params());
    dataset
}

fn get_params() -> Params {
    let host = std::env::var("MYSQL_BENCHMARK_MYSQL_HOST").unwrap_or_default();
    let user = std::env::var("MYSQL_BENCHMARK_MYSQL_USER").unwrap_or_default();
    let pass = std::env::var("MYSQL_BENCHMARK_MYSQL_PASS").unwrap_or_default();
    let db = std::env::var("MYSQL_BENCHMARK_MYSQL_DB").unwrap_or_default();
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

fn get_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tpch_q1", include_str!("../queries/tpch_q1.sql")),
        ("tpch_q2", include_str!("../queries/tpch_q2.sql")),
        ("tpch_q3", include_str!("../queries/tpch_q3.sql")),
        ("tpch_q4", include_str!("../queries/tpch_q4.sql")),
        ("tpch_q5", include_str!("../queries/tpch_q5.sql")),
        ("tpch_q6", include_str!("../queries/tpch_q6.sql")),
        ("tpch_q7", include_str!("../queries/tpch_q7.sql")),
        ("tpch_q8", include_str!("../queries/tpch_q8.sql")),
        ("tpch_q9", include_str!("../queries/tpch_q9.sql")),
        ("tpch_q10", include_str!("../queries/tpch_q10.sql")),
        ("tpch_q11", include_str!("../queries/tpch_q11.sql")),
        ("tpch_q12", include_str!("../queries/tpch_q12.sql")),
        ("tpch_q13", include_str!("../queries/tpch_q13.sql")),
        ("tpch_q14", include_str!("../queries/tpch_q14.sql")),
        // tpch_q15 has a view creation which we don't support by design
        ("tpch_q16", include_str!("../queries/tpch_q16.sql")),
        ("tpch_q17", include_str!("../queries/tpch_q17.sql")),
        ("tpch_q18", include_str!("../queries/tpch_q18.sql")),
        ("tpch_q19", include_str!("../queries/tpch_q19.sql")),
        ("tpch_q20", include_str!("../queries/tpch_q20.sql")),
        ("tpch_q21", include_str!("../queries/tpch_q21.sql")),
        ("tpch_q22", include_str!("../queries/tpch_q22.sql")),
        (
            "tpch_simple_q1",
            include_str!("../queries/tpch_simple_q1.sql"),
        ),
        (
            "tpch_simple_q2",
            include_str!("../queries/tpch_simple_q2.sql"),
        ),
        (
            "tpch_simple_q3",
            include_str!("../queries/tpch_simple_q3.sql"),
        ),
        (
            "tpch_simple_q4",
            include_str!("../queries/tpch_simple_q4.sql"),
        ),
        (
            "tpch_simple_q5",
            include_str!("../queries/tpch_simple_q5.sql"),
        ),
    ]
}
