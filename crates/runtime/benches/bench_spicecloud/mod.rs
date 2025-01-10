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
use crate::results::BenchmarkResultsBuilder;
use app::AppBuilder;
use runtime::Runtime;
use spicepod::component::{dataset::Dataset, params::Params};
use test_framework::queries::get_tpch_test_queries;

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    bench_name: &str,
) -> Result<(), String> {
    let test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(None),
        "tpcds" => get_tpcds_test_queries(),
        "clickbench" => get_clickbench_test_queries(),
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

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

pub fn build_app(app_builder: AppBuilder, bench_name: &str) -> Result<AppBuilder, String> {
    match bench_name {
        "tpch" => Ok(app_builder
            .with_dataset(make_spiceai_dataset("customer", bench_name))
            .with_dataset(make_spiceai_dataset("lineitem", bench_name))
            .with_dataset(make_spiceai_dataset("part", bench_name))
            .with_dataset(make_spiceai_dataset("partsupp", bench_name))
            .with_dataset(make_spiceai_dataset("orders", bench_name))
            .with_dataset(make_spiceai_dataset("nation", bench_name))
            .with_dataset(make_spiceai_dataset("region", bench_name))
            .with_dataset(make_spiceai_dataset("supplier", bench_name))),

        "tpcds" => Ok(app_builder
            .with_dataset(make_spiceai_dataset("call_center", bench_name))
            .with_dataset(make_spiceai_dataset("catalog_page", bench_name))
            .with_dataset(make_spiceai_dataset("catalog_sales", bench_name))
            .with_dataset(make_spiceai_dataset("catalog_returns", bench_name))
            .with_dataset(make_spiceai_dataset("income_band", bench_name))
            .with_dataset(make_spiceai_dataset("inventory", bench_name))
            .with_dataset(make_spiceai_dataset("store_sales", bench_name))
            .with_dataset(make_spiceai_dataset("store_returns", bench_name))
            .with_dataset(make_spiceai_dataset("web_sales", bench_name))
            .with_dataset(make_spiceai_dataset("web_returns", bench_name))
            .with_dataset(make_spiceai_dataset("customer", bench_name))
            .with_dataset(make_spiceai_dataset("customer_address", bench_name))
            .with_dataset(make_spiceai_dataset("customer_demographics", bench_name))
            .with_dataset(make_spiceai_dataset("date_dim", bench_name))
            .with_dataset(make_spiceai_dataset("household_demographics", bench_name))
            .with_dataset(make_spiceai_dataset("item", bench_name))
            .with_dataset(make_spiceai_dataset("promotion", bench_name))
            .with_dataset(make_spiceai_dataset("reason", bench_name))
            .with_dataset(make_spiceai_dataset("ship_mode", bench_name))
            .with_dataset(make_spiceai_dataset("store", bench_name))
            .with_dataset(make_spiceai_dataset("time_dim", bench_name))
            .with_dataset(make_spiceai_dataset("warehouse", bench_name))
            .with_dataset(make_spiceai_dataset("web_page", bench_name))
            .with_dataset(make_spiceai_dataset("web_site", bench_name))),

        "clickbench" => Ok(app_builder.with_dataset(make_spiceai_dataset("hits", bench_name))),

        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
}

fn make_spiceai_dataset(name: &str, bench_name: &str) -> Dataset {
    let from = match bench_name {
        "tpch" => format!("spice.ai:spiceai/tpch/datasets/{bench_name}.{name}"),
        "tpcds" => format!("spice.ai:spiceai/benchmarks-tpcds/datasets/{bench_name}.{name}"),
        "clickbench" => {
            format!("spice.ai:spiceai/benchmarks-clickbench/datasets/{bench_name}.{name}")
        }
        _ => panic!("Only tpcds or tpch benchmark suites are supported"),
    };

    let mut dataset = Dataset::new(from, name.to_string());
    dataset.params = Some(get_params(bench_name));
    dataset
}

fn get_params(bench_name: &str) -> Params {
    let api_key = match bench_name {
        "tpch" => std::env::var("SPICEAI_TPCH_API_KEY").unwrap_or_default(),
        "tpcds" => std::env::var("SPICEAI_TPCDS_API_KEY").unwrap_or_default(),
        "clickbench" => std::env::var("SPICEAI_CLICKBENCH_API_KEY").unwrap_or_default(),
        _ => panic!("Only tpcds or tpch benchmark suites are supported"),
    };

    Params::from_string_map(
        vec![("spiceai_api_key".to_string(), api_key)]
            .into_iter()
            .collect(),
    )
}

#[allow(clippy::too_many_lines)]
fn get_tpcds_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        // see workarounds for more information on skipped queries: https://github.com/spiceai/spiceai/blob/trunk/crates/runtime/benches/queries/README.md
        ("tpcds_q1", include_str!("../queries/tpcds/q1.sql")),
        ("tpcds_q2", include_str!("../queries/tpcds/q2.sql")),
        ("tpcds_q3", include_str!("../queries/tpcds/q3.sql")),
        ("tpcds_q4", include_str!("../queries/tpcds/q4.sql")),
        // Query tpcds_q5 failed with error: Query Error: query `spice.ai` `tpcds_q5` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q5
        // ("tpcds_q5", include_str!("../queries/tpcds/q5.sql")),
        // Query tpcds_q6 failed with error: Query Error: query `spice.ai` `tpcds_q6` to results: Execution error: DoGet recv error: rpc error: code = InvalidArgument desc = Correlated scalar subquery can only be used in Projection, Filter, Aggregate plan nodes; Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q6
        // ("tpcds_q6", include_str!("../queries/tpcds/q6.sql")),
        ("tpcds_q7", include_str!("../queries/tpcds/q7.sql")),
        // ("tpcds_q8", include_str!("../queries/tpcds/q8.sql")), // EXCEPT and INTERSECT aren't supported
        ("tpcds_q9", include_str!("../queries/tpcds/q9.sql")),
        // Query tpcds_q10 failed with error: Query Error: query `spice.ai` `tpcds_q10` to results: Execution error: DoGet recv error: rpc error: code = InvalidArgument desc = No field named c.c_customer_sk. Valid fields are __correlated_sq_1.ss_customer_sk.; Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q10
        // ("tpcds_q10", include_str!("../queries/tpcds/q10.sql")),
        ("tpcds_q11", include_str!("../queries/tpcds/q11.sql")),
        // Query tpcds_q12 failed with error: Query Error: query `spice.ai` `tpcds_q12` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q12
        // ("tpcds_q12", include_str!("../queries/tpcds/q12.sql")),
        ("tpcds_q13", include_str!("../queries/tpcds/q13.sql")),
        // ("tpcds_q14", include_str!("../queries/tpcds/q14.sql")), // EXCEPT and INTERSECT aren't supported
        ("tpcds_q15", include_str!("../queries/tpcds/q15.sql")),
        // Query tpcds_q16 failed with error: Query Error: query `spice.ai` `tpcds_q16` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q16
        // ("tpcds_q16", include_str!("../queries/tpcds/q16.sql")),
        ("tpcds_q17", include_str!("../queries/tpcds/q17.sql")),
        ("tpcds_q18", include_str!("../queries/tpcds/q18.sql")),
        ("tpcds_q19", include_str!("../queries/tpcds/q19.sql")),
        // Query tpcds_q20 failed with error: Query Error: query `spice.ai` `tpcds_q20` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q20
        // ("tpcds_q20", include_str!("../queries/tpcds/q20.sql")),
        // Query tpcds_q21 failed with error: Query Error: query `spice.ai` `tpcds_q21` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q21
        // ("tpcds_q21", include_str!("../queries/tpcds/q21.sql")),
        // Query tpcds_q22 failed with error: Query Error: query `spice.ai` `tpcds_q22` to results: Execution error: Query execution failed.
        //   Tonic error: status: ResourceExhausted, message: "DoGet recv error: rpc error: code = ResourceExhausted desc = grpc: received message larger than max (144718330 vs. 104857600)", details: [], metadata: MetadataMap { headers: {} }
        //   Verify the configuration and try again.; Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q22
        // ("tpcds_q22", include_str!("../queries/tpcds/q22.sql")),
        // ("tpcds_q23", include_str!("../queries/tpcds/q23.sql")), // this query contains multiple queries, which aren't supported
        // ("tpcds_q24", include_str!("../queries/tpcds/q24.sql")), // this query contains multiple queries, which aren't supported
        ("tpcds_q25", include_str!("../queries/tpcds/q25.sql")),
        ("tpcds_q26", include_str!("../queries/tpcds/q26.sql")),
        ("tpcds_q27", include_str!("../queries/tpcds/q27.sql")),
        ("tpcds_q28", include_str!("../queries/tpcds/q28.sql")),
        ("tpcds_q29", include_str!("../queries/tpcds/q29.sql")),
        ("tpcds_q30", include_str!("../queries/tpcds/q30.sql")),
        ("tpcds_q31", include_str!("../queries/tpcds/q31.sql")),
        // Query tpcds_q32 failed with error: Query Error: query `spice.ai` `tpcds_q32` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q32
        // ("tpcds_q32", include_str!("../queries/tpcds/q32.sql")),
        ("tpcds_q33", include_str!("../queries/tpcds/q33.sql")),
        ("tpcds_q34", include_str!("../queries/tpcds/q34.sql")),
        // Query tpcds_q35 failed with error: Query Error: query `spice.ai` `tpcds_q35` to results: Execution error: DoGet recv error: rpc error: code = InvalidArgument desc = No field named c.c_customer_sk. Valid fields are __correlated_sq_1.ss_customer_sk.; Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q35
        // ("tpcds_q35", include_str!("../queries/tpcds/q35.sql")),
        ("tpcds_q36", include_str!("../queries/tpcds/q36.sql")),
        // Query tpcds_q37 failed with error: Query Error: query `spice.ai` `tpcds_q37` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q37
        // ("tpcds_q37", include_str!("../queries/tpcds/q37.sql")),
        // ("tpcds_q38", include_str!("../queries/tpcds/q38.sql")), // EXCEPT and INTERSECT aren't supported
        // ("tpcds_q39", include_str!("../queries/tpcds/q39.sql")), // this query contains multiple queries, which aren't supported
        // Query tpcds_q40 failed with error: Query Error: query `spice.ai` `tpcds_q40` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q40
        // ("tpcds_q40", include_str!("../queries/tpcds/q40.sql")),
        ("tpcds_q41", include_str!("../queries/tpcds/q41.sql")),
        ("tpcds_q42", include_str!("../queries/tpcds/q42.sql")),
        ("tpcds_q43", include_str!("../queries/tpcds/q43.sql")),
        ("tpcds_q44", include_str!("../queries/tpcds/q44.sql")),
        ("tpcds_q45", include_str!("../queries/tpcds/q45.sql")),
        ("tpcds_q46", include_str!("../queries/tpcds/q46.sql")),
        ("tpcds_q47", include_str!("../queries/tpcds/q47.sql")),
        ("tpcds_q48", include_str!("../queries/tpcds/q48.sql")),
        ("tpcds_q49", include_str!("../queries/tpcds/q49.sql")),
        ("tpcds_q50", include_str!("../queries/tpcds/q50.sql")),
        ("tpcds_q51", include_str!("../queries/tpcds/q51.sql")),
        ("tpcds_q52", include_str!("../queries/tpcds/q52.sql")),
        ("tpcds_q53", include_str!("../queries/tpcds/q53.sql")),
        ("tpcds_q54", include_str!("../queries/tpcds/q54.sql")),
        ("tpcds_q55", include_str!("../queries/tpcds/q55.sql")),
        ("tpcds_q56", include_str!("../queries/tpcds/q56.sql")),
        ("tpcds_q57", include_str!("../queries/tpcds/q57.sql")),
        ("tpcds_q58", include_str!("../queries/tpcds/q58.sql")),
        ("tpcds_q59", include_str!("../queries/tpcds/q59.sql")),
        ("tpcds_q60", include_str!("../queries/tpcds/q60.sql")),
        ("tpcds_q61", include_str!("../queries/tpcds/q61.sql")),
        ("tpcds_q62", include_str!("../queries/tpcds/q62.sql")),
        ("tpcds_q63", include_str!("../queries/tpcds/q63.sql")),
        ("tpcds_q64", include_str!("../queries/tpcds/q64.sql")),
        ("tpcds_q65", include_str!("../queries/tpcds/q65.sql")),
        ("tpcds_q66", include_str!("../queries/tpcds/q66.sql")),
        ("tpcds_q67", include_str!("../queries/tpcds/q67.sql")),
        ("tpcds_q68", include_str!("../queries/tpcds/q68.sql")),
        // Query tpcds_q69 failed with error: Query Error: query `spice.ai` `tpcds_q69` to results: Execution error: DoGet recv error: rpc error: code = InvalidArgument desc = No field named c.c_customer_sk. Valid fields are __correlated_sq_1.ss_customer_sk.; Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q69
        // ("tpcds_q69", include_str!("../queries/tpcds/q69.sql")),
        ("tpcds_q70", include_str!("../queries/tpcds/q70.sql")),
        ("tpcds_q71", include_str!("../queries/tpcds/q71.sql")),
        // Query tpcds_q72 failed with error: Query Error: query `spice.ai` `tpcds_q72` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q72
        // ("tpcds_q72", include_str!("../queries/tpcds/q72.sql")),
        ("tpcds_q73", include_str!("../queries/tpcds/q73.sql")),
        ("tpcds_q74", include_str!("../queries/tpcds/q74.sql")),
        ("tpcds_q75", include_str!("../queries/tpcds/q75.sql")),
        ("tpcds_q76", include_str!("../queries/tpcds/q76.sql")),
        // Query tpcds_q77 failed with error: Query Error: query `spice.ai` `tpcds_q77` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q77
        // ("tpcds_q77", include_str!("../queries/tpcds/q77.sql")),
        ("tpcds_q78", include_str!("../queries/tpcds/q78.sql")),
        ("tpcds_q79", include_str!("../queries/tpcds/q79.sql")),
        // Query tpcds_q80 failed with error: Query Error: query `spice.ai` `tpcds_q80` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q80
        // ("tpcds_q80", include_str!("../queries/tpcds/q80.sql")),
        ("tpcds_q81", include_str!("../queries/tpcds/q81.sql")),
        // Query tpcds_q82 failed with error: Query Error: query `spice.ai` `tpcds_q82` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q82
        // ("tpcds_q82", include_str!("../queries/tpcds/q82.sql")),
        ("tpcds_q83", include_str!("../queries/tpcds/q83.sql")),
        ("tpcds_q84", include_str!("../queries/tpcds/q84.sql")),
        ("tpcds_q85", include_str!("../queries/tpcds/q85.sql")),
        ("tpcds_q86", include_str!("../queries/tpcds/q86.sql")),
        // ("tpcds_q87", include_str!("../queries/tpcds/q87.sql")), // EXCEPT and INTERSECT aren't supported
        ("tpcds_q88", include_str!("../queries/tpcds/q88.sql")),
        ("tpcds_q89", include_str!("../queries/tpcds/q89.sql")),
        ("tpcds_q90", include_str!("../queries/tpcds/q90.sql")),
        // Query tpcds_q91 failed with error: Query Error: query `spice.ai` `tpcds_q91` to plan: Failed to execute query: Schema error: No field named "tpcds.call_center". Valid fields are call_center, call_center_name, manager, returns_loss, "sum(tpcds.catalog_returns.cr_net_loss)".; Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q91
        // ("tpcds_q91", include_str!("../queries/tpcds/q91.sql")),
        // Query tpcds_q92 failed with error: Query Error: query `spice.ai` `tpcds_q92` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q92
        // ("tpcds_q92", include_str!("../queries/tpcds/q92.sql")),
        ("tpcds_q93", include_str!("../queries/tpcds/q93.sql")),
        // Query tpcds_q94 failed with error: Query Error: query `spice.ai` `tpcds_q94` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q94
        // ("tpcds_q94", include_str!("../queries/tpcds/q94.sql")),
        // Query tpcds_q95 failed with error: Query Error: query `spice.ai` `tpcds_q95` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q95
        // ("tpcds_q95", include_str!("../queries/tpcds/q95.sql")),
        ("tpcds_q96", include_str!("../queries/tpcds/q96.sql")),
        ("tpcds_q97", include_str!("../queries/tpcds/q97.sql")),
        // Query tpcds_q98 failed with error: Query Error: query `spice.ai` `tpcds_q98` to results: Execution error: This feature is not implemented: Unsupported Interval Expression with last_field Some(Second); Snapshot Test Error: Snapshort assertion failed for spice.ai, tpcds_q98
        // ("tpcds_q98", include_str!("../queries/tpcds/q98.sql")),
        ("tpcds_q99", include_str!("../queries/tpcds/q99.sql")),
    ]
}

#[allow(clippy::too_many_lines)]
fn get_clickbench_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "clickbench_q1",
            include_str!("../queries/clickbench/q1.sql"),
        ),
        // Empty results
        (
            "clickbench_q2",
            include_str!("../queries/clickbench/q2.sql"),
        ),
        (
            "clickbench_q3",
            include_str!("../queries/clickbench/q3.sql"),
        ),
        (
            "clickbench_q4",
            include_str!("../queries/clickbench/q4.sql"),
        ),
        (
            "clickbench_q5",
            include_str!("../queries/clickbench/q5.sql"),
        ),
        (
            "clickbench_q6",
            include_str!("../queries/clickbench/q6.sql"),
        ),
        (
            "clickbench_q7",
            include_str!("../queries/clickbench/q7.sql"),
        ),
        // Empty results
        (
            "clickbench_q8",
            include_str!("../queries/clickbench/q8.sql"),
        ),
        (
            "clickbench_q9",
            include_str!("../queries/clickbench/q9.sql"),
        ),
        (
            "clickbench_q10",
            include_str!("../queries/clickbench/q10.sql"),
        ),
        // Empty results
        (
            "clickbench_q11",
            include_str!("../queries/clickbench/q11.sql"),
        ),
        // Empty results
        (
            "clickbench_q12",
            include_str!("../queries/clickbench/q12.sql"),
        ),
        // Empty results
        (
            "clickbench_q13",
            include_str!("../queries/clickbench/q13.sql"),
        ),
        // Empty results
        (
            "clickbench_q14",
            include_str!("../queries/clickbench/q14.sql"),
        ),
        // Empty results
        (
            "clickbench_q15",
            include_str!("../queries/clickbench/q15.sql"),
        ),
        (
            "clickbench_q16",
            include_str!("../queries/clickbench/q16.sql"),
        ),
        (
            "clickbench_q17",
            include_str!("../queries/clickbench/q17.sql"),
        ),
        (
            "clickbench_q18",
            include_str!("../queries/clickbench/q18.sql"),
        ),
        (
            "clickbench_q19",
            include_str!("../queries/clickbench/q19.sql"),
        ),
        // Empty results
        (
            "clickbench_q20",
            include_str!("../queries/clickbench/q20.sql"),
        ),
        // Empty results
        (
            "clickbench_q21",
            include_str!("../queries/clickbench/q21.sql"),
        ),
        // Query clickbench_q22 failed with error: Query Error: query `spice.ai` `clickbench_q22` to results: Execution error: Query execution failed.
        // Tonic error: status: InvalidArgument, message: "DoGet recv error: rpc error: code = InvalidArgument desc = Failed to execute query.\nBinder Error: Referenced table \"clickbench\" not found!\nCandidate tables: \"clickbench.hits\"\nLINE 1: ...) AS \"c\" FROM \"clickbench.hits\" WHERE (\"clickbench\".\"hits\".\"URL\" LIKE '%google...\n
        // (
        //     "clickbench_q22",
        //     include_str!("../queries/clickbench/q22.sql"),
        // ),
        // Query clickbench_q23 failed with error: Query Error: query `spice.ai` `clickbench_q23` to results: Execution error: Query execution failed.
        // Tonic error: status: InvalidArgument, message: "DoGet recv error: rpc error: code = InvalidArgument desc = Failed to execute query.\nBinder Error: Referenced table \"clickbench\" not found!\nCandidate tables: \"clickbench.hits\"\nLINE 1: ...serID\") FROM \"clickbench.hits\" WHERE ((\"clickbench\".\"hits\".\"Title\" LIKE '%Goog...\n
        // (
        //     "clickbench_q23",
        //     include_str!("../queries/clickbench/q23.sql"),
        // ),
        // Empty results
        (
            "clickbench_q24",
            include_str!("../queries/clickbench/q24.sql"),
        ),
        // Empty results
        (
            "clickbench_q25",
            include_str!("../queries/clickbench/q25.sql"),
        ),
        // Empty results
        (
            "clickbench_q26",
            include_str!("../queries/clickbench/q26.sql"),
        ),
        // Empty results
        (
            "clickbench_q27",
            include_str!("../queries/clickbench/q27.sql"),
        ),
        // Empty results
        (
            "clickbench_q28",
            include_str!("../queries/clickbench/q28.sql"),
        ),
        // Query clickbench_q29 failed with error: Query Error: query `spice.ai` `clickbench_q29` to results: Execution error: Query execution failed.
        // Tonic error: status: InvalidArgument, message: "DoGet recv error: rpc error: code = InvalidArgument desc = Failed to execute query.\nBinder Error: Referenced table \"clickbench\" not found!\nCandidate tables: \"clickbench.hits\"\nLINE 1: ...eferer\") FROM \"clickbench.hits\" WHERE (\"clickbench\".\"hits\".\"Referer\" <> '') GR...\n
        // (
        //     "clickbench_q29",
        //     include_str!("../queries/clickbench/q29.sql"),
        // ),
        (
            "clickbench_q30",
            include_str!("../queries/clickbench/q30.sql"),
        ),
        // Query clickbench_q31 failed with error: Query Error: query `spice.ai` `clickbench_q31` to results: Execution error: Query execution failed.
        // Tonic error: status: InvalidArgument, message: "DoGet recv error: rpc error: code = InvalidArgument desc = Failed to execute query.\nBinder Error: Referenced table \"clickbench\" not found!\nCandidate tables: \"clickbench.hits\"\nLINE 1: ...nWidth\") FROM \"clickbench.hits\" WHERE (\"clickbench\".\"hits\".\"SearchPhrase\" <> '...\n
        // (
        //     "clickbench_q31",
        //     include_str!("../queries/clickbench/q31.sql"),
        // ),
        // Empty results
        (
            "clickbench_q32",
            include_str!("../queries/clickbench/q32.sql"),
        ),
        (
            "clickbench_q33",
            include_str!("../queries/clickbench/q33.sql"),
        ),
        (
            "clickbench_q34",
            include_str!("../queries/clickbench/q34.sql"),
        ),
        (
            "clickbench_q35",
            include_str!("../queries/clickbench/q35.sql"),
        ),
        // Query clickbench_q36 failed with error: Query Error: query `spice.ai` `clickbench_q36` to plan: Failed to execute query: Schema error: No field named "hits.ClientIP - Int64(1)". Valid fields are clickbench.hits."ClientIP", "clickbench.hits.ClientIP - Int64(1)", "clickbench.hits.ClientIP - Int64(2)", "clickbench.hits.ClientIP - Int64(3)", "count(*)".
        // (
        //     "clickbench_q36",
        //     include_str!("../queries/clickbench/q36.sql"),
        // ),
        // Empty results
        (
            "clickbench_q37",
            include_str!("../queries/clickbench/q37.sql"),
        ),
        // Empty results
        (
            "clickbench_q38",
            include_str!("../queries/clickbench/q38.sql"),
        ),
        // Empty results
        (
            "clickbench_q39",
            include_str!("../queries/clickbench/q39.sql"),
        ),
        // Query clickbench_q40 failed with error: Query Error: query `spice.ai` `clickbench_q40` to results: Execution error: Query execution failed.
        // Tonic error: status: InvalidArgument, message: "DoGet recv error: rpc error: code = InvalidArgument desc = Failed to execute query.\nBinder Error: Referenced table \"clickbench\" not found!\nCandidate tables: \"clickbench.hits\"\nLINE 1: ...iews\" FROM \"clickbench.hits\" WHERE ((((\"clickbench\".\"hits\".\"CounterID\" = 62) A...\n
        // (
        //     "clickbench_q40",
        //     include_str!("../queries/clickbench/q40.sql"),
        // ),
        // Query clickbench_q41 failed with error: Query Error: query `spice.ai` `clickbench_q41` to results: Execution error: Query execution failed.
        // Tonic error: status: InvalidArgument, message: "DoGet recv error: rpc error: code = InvalidArgument desc = Failed to execute query.\nBinder Error: Referenced table \"clickbench\" not found!\nCandidate tables: \"clickbench.hits\"\nLINE 1: ...ws\" FROM \"clickbench.hits\" WHERE ((((((\"clickbench\".\"hits\".\"CounterID\" = 62) A...\n
        // (
        //     "clickbench_q41",
        //     include_str!("../queries/clickbench/q41.sql"),
        //         // ),
        // Query clickbench_q42 failed with error: Query Error: query `spice.ai` `clickbench_q42` to results: Execution error: Query execution failed.
        // Tonic error: status: InvalidArgument, message: "DoGet recv error: rpc error: code = InvalidArgument desc = Failed to execute query.\nBinder Error: Referenced table \"clickbench\" not found!\nCandidate tables: \"clickbench.hits\"\nLINE 1: ...ws\" FROM \"clickbench.hits\" WHERE ((((((\"clickbench\".\"hits\".\"CounterID\" = 62) A...\n
        // (
        //     "clickbench_q42",
        //     include_str!("../queries/clickbench/q42.sql"),
        // ),
        // Empty results
        (
            "clickbench_q43",
            include_str!("../queries/clickbench/q43.sql"),
        ),
    ]
}
