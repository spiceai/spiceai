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
use spicepod::component::dataset::Dataset;

use crate::results::BenchmarkResultsBuilder;

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
) -> Result<(), String> {
    let test_queries = get_tpch_test_queries();

    let mut errors = Vec::new();
    for (query_name, query) in test_queries {
        let verify_query_results = query_name.starts_with("tpch_q");
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "spark",
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

pub fn build_app(app_builder: AppBuilder) -> AppBuilder {
    app_builder
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.customer",
            "customer",
        ))
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.lineitem",
            "lineitem",
        ))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.part", "part"))
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.partsupp",
            "partsupp",
        ))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.orders", "orders"))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.nation", "nation"))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.region", "region"))
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.supplier",
            "supplier",
        ))
}

fn make_spark_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spark:{path}"), name.to_string())
}

fn get_tpch_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tpch_q1", include_str!("../queries/tpch/q1.sql")),
        // Analysis error: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY] Unsupported subquery expression: Correlated scalar subqueries can only be used in filters, aggregations, projections, and UPDATE/MERGE/DELETE commands
        // ("tpch_q2", include_str!("../queries/tpch/q2.sql")),
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
        // Analysis error: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY] Unsupported subquery expression: Correlated scalar subqueries can only be used in filters, aggregations, projections, and UPDATE/MERGE/DELETE commands
        // ("tpch_q17", include_str!("../queries/tpch/q17.sql")),
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
