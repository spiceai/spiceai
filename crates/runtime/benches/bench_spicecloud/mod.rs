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
use crate::results::BenchmarkResultsBuilder;
use app::AppBuilder;
use runtime::Runtime;
use spicepod::component::dataset::Dataset;

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
) -> Result<(), String> {
    let test_queries = get_tpch_test_queries();
    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        // results of some tpch_simple_ queries are non deterministic, temporarily disable verification
        let verify_query_results = !query_name.contains("simple_");

        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "spice.ai",
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
        .with_dataset(make_spiceai_dataset("tpch.customer", "customer"))
        .with_dataset(make_spiceai_dataset("tpch.lineitem", "lineitem"))
        .with_dataset(make_spiceai_dataset("tpch.part", "part"))
        .with_dataset(make_spiceai_dataset("tpch.partsupp", "partsupp"))
        .with_dataset(make_spiceai_dataset("tpch.orders", "orders"))
        .with_dataset(make_spiceai_dataset("tpch.nation", "nation"))
        .with_dataset(make_spiceai_dataset("tpch.region", "region"))
        .with_dataset(make_spiceai_dataset("tpch.supplier", "supplier"))
}

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spice.ai:{path}"), name.to_string())
}

fn get_tpch_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tpch_q1", include_str!("../queries/tpch/q1.sql")),
        // Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.nation' not found
        // ("tpch_q2", include_str!("../queries/tpch/q2.sql")),
        ("tpch_q3", include_str!("../queries/tpch/q3.sql")),
        // Error: "query `tpch_q4` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.lineitem' not found\\nstartLine 1\\nstartColumn 846\\nendLine 1\\nendColumn 862\\nSQL Query SELECT \\\"tpch\\\".\\\"orders\\\".\\\"o_orderpriority\\\", COUNT(1) AS \\\"order_count\\\" FROM \\\"tpch\\\".\\\"orders\\\" WHERE (((\\\"tpch\\\".\\\"orders\\\".\\\"o_orderdate\\\" >= CAST('1993-07-01' AS TIMESTAMP)) AND (\\\"tpch\\\".\\\"orders\\\".\\\"o_orderdate\\\" < CAST((CAST('1993-07-01' AS DATE) + INTERVAL '3' MONTH) AS TIMESTAMP))) AND EXISTS (SELECT \\\"tpch\\\".\\\"lineitem\\\".\\\"l_orderkey\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_partkey\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_suppkey\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_linenumber\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_quantity\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_extendedprice\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_discount\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_tax\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_returnflag\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_linestatus\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipdate\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_commitdate\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_receiptdate\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipinstruct\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipmode\\\", \\\"tpch\\\".\\\"lineitem\\\".\\\"l_comment\\\" FROM \\\"tpch\\\".\\\"lineitem\\\" WHERE ((\\\"tpch\\\".\\\"lineitem\\\".\\\"l_orderkey\\\" = \\\"tpch\\\".\\\"orders\\\".\\\"o_orderkey\\\") AND (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_commitdate\\\" < \\\"tpch\\\".\\\"lineitem\\\".\\\"l_receiptdate\\\")))) GROUP BY \\\"tpch\\\".\\\"orders\\\".\\\"o_orderpriority\\\" ORDER BY \\\"tpch\\\".\\\"orders\\\".\\\"o_orderpriority\\\" ASC NULLS LAST\", details: [], metadata: MetadataMap { headers: {\"date\": \"Thu, 11 Jul 2024 10:46:10 GMT\", \"content-type\": \"application/grpc\", \"strict-transport-security\": \"max-age=15724800; includeSubDomains\", \"alt-svc\": \"h3=\\\":443\\\"; ma=86400\", \"cf-ray\": \"8a18354d2b5f5ac0-MEL\", \"cf-cache-status\": \"DYNAMIC\", \"server\": \"cloudflare\"} }"
        // potential data source bug
        // ("tpch_q4", include_str!("tpch_q4.sql")),
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

        // Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.supplier' not found
        // ("tpch_q16", include_str!("tpch_q16.sql")),
        ("tpch_q17", include_str!("../queries/tpch/q17.sql")),
        // Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.lineitem' not found
        // ("tpch_q18", include_str!("../queries/tpch/q18.sql")),
        ("tpch_q19", include_str!("../queries/tpch/q19.sql")),
        // Error: "query `tpch_q20` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.partsupp' not found\\nstartLine 1\\nstartColumn 228\\nendLine 1\\nendColumn 244\\nSQL Query SELECT \\\"tpch\\\".\\\"supplier\\\".\\\"s_name\\\", \\\"tpch\\\".\\\"supplier\\\".\\\"s_address\\\" FROM \\\"tpch\\\".\\\"supplier\\\" JOIN \\\"tpch\\\".\\\"nation\\\" ON true WHERE ((\\\"tpch\\\".\\\"supplier\\\".\\\"s_suppkey\\\" IN (SELECT \\\"tpch\\\".\\\"partsupp\\\".\\\"ps_suppkey\\\" FROM \\\"tpch\\\".\\\"partsupp\\\" WHERE (\\\"tpch\\\".\\\"partsupp\\\".\\\"ps_partkey\\\" IN (SELECT \\\"tpch\\\".\\\"part\\\".\\\"p_partkey\\\" FROM \\\"tpch\\\".\\\"part\\\" WHERE \\\"tpch\\\".\\\"part\\\".\\\"p_name\\\" LIKE 'forest%') AND (CAST(\\\"tpch\\\".\\\"partsupp\\\".\\\"ps_availqty\\\" AS DOUBLE) > (SELECT (0.5 * CAST(SUM(\\\"tpch\\\".\\\"lineitem\\\".\\\"l_quantity\\\") AS DOUBLE)) FROM \\\"tpch\\\".\\\"lineitem\\\" WHERE ((((\\\"tpch\\\".\\\"lineitem\\\".\\\"l_partkey\\\" = \\\"tpch\\\".\\\"partsupp\\\".\\\"ps_partkey\\\") AND (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_suppkey\\\" = \\\"tpch\\\".\\\"partsupp\\\".\\\"ps_suppkey\\\")) AND (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipdate\\\" >= CAST(CAST('1994-01-01' AS DATE) AS TIMESTAMP))) AND (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipdate\\\" < CAST((CAST('1994-01-01' AS DATE) + INTERVAL '12' MONTH) AS TIMESTAMP))))))) AND (\\\"tpch\\\".\\\"supplier\\\".\\\"s_nationkey\\\" = \\\"tpch\\\".\\\"nation\\\".\\\"n_nationkey\\\")) AND (\\\"tpch\\\".\\\"nation\\\".\\\"n_name\\\" = 'CANADA')) ORDER BY \\\"tpch\\\".\\\"supplier\\\".\\\"s_name\\\" ASC NULLS LAST\", details: [], metadata: MetadataMap { headers: {\"date\": \"Thu, 11 Jul 2024 10:51:30 GMT\", \"content-type\": \"application/grpc\", \"strict-transport-security\": \"max-age=15724800; includeSubDomains\", \"alt-svc\": \"h3=\\\":443\\\"; ma=86400\", \"cf-ray\": \"8a183d1d0a305ab0-MEL\", \"cf-cache-status\": \"DYNAMIC\", \"server\": \"cloudflare\"} }"
        // ("tpch_q20", include_str!("tpch_q20.sql")),
        ("tpch_q21", include_str!("../queries/tpch/q21.sql")),
        // Error: "query `tpch_q22` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.orders' not found
        // ("tpch_q22", include_str!("tpch_q22.sql")),
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
        (
            "tpch_simple_q6",
            include_str!("../queries/tpch/simple_q6.sql"),
        ),
        (
            "tpch_simple_q7",
            include_str!("../queries/tpch/simple_q7.sql"),
        ),
    ]
}
