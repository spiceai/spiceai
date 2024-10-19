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
use spicepod::component::dataset::acceleration::Mode;

pub(crate) mod abfs;
pub(crate) mod s3;

pub(crate) fn build_app(
    connector: &str,
    app_builder: AppBuilder,
    bench_name: &str,
) -> Result<AppBuilder, String> {
    match connector {
        "s3" => s3::build_app(app_builder, bench_name),
        "abfs" => Ok(abfs::build_app(app_builder, bench_name)),
        _ => Err(format!("Unsupported connector {connector}")),
    }
}

pub(crate) async fn run(
    connector: &str,
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    engine: Option<String>,
    mode: Option<Mode>,
    bench_name: &str,
) -> Result<(), String> {
    let test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(),
        "tpcds" => {
            // TPCDS Query 1, 30, 64, 81 are commented out for Postgres accelerator, see details in `get_postgres_tpcds_test_queries` function
            #[cfg(feature = "postgres")]
            {
                if engine.clone().unwrap_or_default().as_str() == "postgres" {
                    super::bench_postgres::get_tpcds_test_queries()
                } else {
                    get_tpcds_test_queries()
                }
            }

            #[cfg(not(feature = "postgres"))]
            {
                get_tpcds_test_queries()
            }
        }
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let bench_name = match mode {
        Some(mode) => {
            format!("{}_{}_{}", connector, engine.unwrap_or_default(), mode).to_lowercase()
        }
        None => connector.to_string(),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        let verify_query_results = if query_name.starts_with("tpch_q") {
            matches!(
                bench_name.as_str(),
                "s3" | "s3_arrow_memory"
                    | "s3_sqlite_memory"
                    | "s3_sqlite_file"
                    | "s3_duckdb_memory"
                    | "abfs"
                    | "s3_duckdb_file"
            )
        } else if query_name.starts_with("tpcds_q") {
            matches!(
                bench_name.as_str(),
                "s3" | "s3_postgres_memory" | "s3_arrow_memory" | "s3_duckdb_file"
            )
        } else {
            false
        };

        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            bench_name.as_str(),
            query_name,
            query,
            verify_query_results,
        )
        .await
        {
            errors.push(format!("Query {query_name} failed with error: {e}"));
        }
    }

    if !errors.is_empty() {
        tracing::error!("There are failed queries:\n{}", errors.join("\n"));
    }

    Ok(())
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

#[allow(clippy::too_many_lines)]
fn get_tpcds_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tpcds_q1", include_str!("../queries/tpcds/q1.sql")),
        ("tpcds_q2", include_str!("../queries/tpcds/q2.sql")),
        ("tpcds_q3", include_str!("../queries/tpcds/q3.sql")),
        ("tpcds_q4", include_str!("../queries/tpcds/q4.sql")),
        ("tpcds_q5", include_str!("../queries/tpcds/q5.sql")),
        ("tpcds_q6", include_str!("../queries/tpcds/q6.sql")),
        ("tpcds_q7", include_str!("../queries/tpcds/q7.sql")),
        ("tpcds_q8", include_str!("../queries/tpcds/q8.sql")),
        ("tpcds_q9", include_str!("../queries/tpcds/q9.sql")),
        ("tpcds_q10", include_str!("../queries/tpcds/q10.sql")),
        ("tpcds_q11", include_str!("../queries/tpcds/q11.sql")),
        ("tpcds_q12", include_str!("../queries/tpcds/q12.sql")),
        ("tpcds_q13", include_str!("../queries/tpcds/q13.sql")),
        ("tpcds_q14", include_str!("../queries/tpcds/q14.sql")),
        ("tpcds_q15", include_str!("../queries/tpcds/q15.sql")),
        ("tpcds_q16", include_str!("../queries/tpcds/q16.sql")),
        ("tpcds_q17", include_str!("../queries/tpcds/q17.sql")),
        ("tpcds_q18", include_str!("../queries/tpcds/q18.sql")),
        ("tpcds_q19", include_str!("../queries/tpcds/q19.sql")),
        ("tpcds_q20", include_str!("../queries/tpcds/q20.sql")),
        ("tpcds_q21", include_str!("../queries/tpcds/q21.sql")),
        ("tpcds_q22", include_str!("../queries/tpcds/q22.sql")),
        ("tpcds_q23", include_str!("../queries/tpcds/q23.sql")),
        ("tpcds_q24", include_str!("../queries/tpcds/q24.sql")),
        ("tpcds_q25", include_str!("../queries/tpcds/q25.sql")),
        ("tpcds_q26", include_str!("../queries/tpcds/q26.sql")),
        ("tpcds_q27", include_str!("../queries/tpcds/q27.sql")),
        ("tpcds_q28", include_str!("../queries/tpcds/q28.sql")),
        ("tpcds_q29", include_str!("../queries/tpcds/q29.sql")),
        ("tpcds_q30", include_str!("../queries/tpcds/q30.sql")),
        ("tpcds_q31", include_str!("../queries/tpcds/q31.sql")),
        ("tpcds_q32", include_str!("../queries/tpcds/q32.sql")),
        ("tpcds_q33", include_str!("../queries/tpcds/q33.sql")),
        ("tpcds_q34", include_str!("../queries/tpcds/q34.sql")),
        ("tpcds_q35", include_str!("../queries/tpcds/q35.sql")),
        ("tpcds_q36", include_str!("../queries/tpcds/q36.sql")),
        ("tpcds_q37", include_str!("../queries/tpcds/q37.sql")),
        ("tpcds_q38", include_str!("../queries/tpcds/q38.sql")),
        ("tpcds_q39", include_str!("../queries/tpcds/q39.sql")),
        ("tpcds_q40", include_str!("../queries/tpcds/q40.sql")),
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
        ("tpcds_q69", include_str!("../queries/tpcds/q69.sql")),
        ("tpcds_q70", include_str!("../queries/tpcds/q70.sql")),
        ("tpcds_q71", include_str!("../queries/tpcds/q71.sql")),
        ("tpcds_q72", include_str!("../queries/tpcds/q72.sql")),
        ("tpcds_q73", include_str!("../queries/tpcds/q73.sql")),
        ("tpcds_q74", include_str!("../queries/tpcds/q74.sql")),
        ("tpcds_q75", include_str!("../queries/tpcds/q75.sql")),
        ("tpcds_q76", include_str!("../queries/tpcds/q76.sql")),
        ("tpcds_q77", include_str!("../queries/tpcds/q77.sql")),
        ("tpcds_q78", include_str!("../queries/tpcds/q78.sql")),
        ("tpcds_q79", include_str!("../queries/tpcds/q79.sql")),
        ("tpcds_q80", include_str!("../queries/tpcds/q80.sql")),
        ("tpcds_q81", include_str!("../queries/tpcds/q81.sql")),
        ("tpcds_q82", include_str!("../queries/tpcds/q82.sql")),
        ("tpcds_q83", include_str!("../queries/tpcds/q83.sql")),
        ("tpcds_q84", include_str!("../queries/tpcds/q84.sql")),
        ("tpcds_q85", include_str!("../queries/tpcds/q85.sql")),
        ("tpcds_q86", include_str!("../queries/tpcds/q86.sql")),
        ("tpcds_q87", include_str!("../queries/tpcds/q87.sql")),
        ("tpcds_q88", include_str!("../queries/tpcds/q88.sql")),
        ("tpcds_q89", include_str!("../queries/tpcds/q89.sql")),
        ("tpcds_q90", include_str!("../queries/tpcds/q90.sql")),
        ("tpcds_q91", include_str!("../queries/tpcds/q91.sql")),
        ("tpcds_q92", include_str!("../queries/tpcds/q92.sql")),
        ("tpcds_q93", include_str!("../queries/tpcds/q93.sql")),
        ("tpcds_q94", include_str!("../queries/tpcds/q94.sql")),
        ("tpcds_q95", include_str!("../queries/tpcds/q95.sql")),
        ("tpcds_q96", include_str!("../queries/tpcds/q96.sql")),
        ("tpcds_q97", include_str!("../queries/tpcds/q97.sql")),
        ("tpcds_q98", include_str!("../queries/tpcds/q98.sql")),
        ("tpcds_q99", include_str!("../queries/tpcds/q99.sql")),
    ]
}
