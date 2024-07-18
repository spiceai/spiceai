use runtime::Runtime;

use crate::results::BenchmarkResultsBuilder;

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
) -> Result<(), String> {
    let test_queries = get_test_queries();

    for (query_name, query) in test_queries {
        super::run_query_and_record_result(rt, benchmark_results, query_name, query).await?;
    }

    Ok(())
}

fn get_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tpch_q1", include_str!("../tpch_queries/tpch_q1.sql")),
        ("tpch_q2", include_str!("../tpch_queries/tpch_q2.sql")),
        ("tpch_q3", include_str!("../tpch_queries/tpch_q3.sql")),
        ("tpch_q4", include_str!("../tpch_queries/tpch_q4.sql")),
        ("tpch_q5", include_str!("../tpch_queries/tpch_q5.sql")),
        ("tpch_q6", include_str!("../tpch_queries/tpch_q6.sql")),
        ("tpch_q7", include_str!("../tpch_queries/tpch_q7.sql")),
        ("tpch_q8", include_str!("../tpch_queries/tpch_q8.sql")),
        ("tpch_q9", include_str!("../tpch_queries/tpch_q9.sql")),
        ("tpch_q10", include_str!("../tpch_queries/tpch_q10.sql")),
        ("tpch_q11", include_str!("../tpch_queries/tpch_q11.sql")),
        ("tpch_q12", include_str!("../tpch_queries/tpch_q12.sql")),
        ("tpch_q13", include_str!("../tpch_queries/tpch_q13.sql")),
        ("tpch_q14", include_str!("../tpch_queries/tpch_q14.sql")),
        // tpch_q15 has a view creation which we don't support by design
        ("tpch_q16", include_str!("../tpch_queries/tpch_q16.sql")),
        ("tpch_q17", include_str!("../tpch_queries/tpch_q17.sql")),
        ("tpch_q18", include_str!("../tpch_queries/tpch_q18.sql")),
        ("tpch_q19", include_str!("../tpch_queries/tpch_q19.sql")),
        // ("tpch_q20", include_str!("../tpch_queries/tpch_q20.sql")),
        // ("tpch_q21", include_str!("../tpch_queries/tpch_q21.sql")),
        ("tpch_q22", include_str!("../tpch_queries/tpch_q22.sql")),
        (
            "tpch_simple_q1",
            include_str!("../tpch_queries/tpch_simple_q1.sql"),
        ),
        (
            "tpch_simple_q2",
            include_str!("../tpch_queries/tpch_simple_q2.sql"),
        ),
        // Error: "query `tpch_simple_q3` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.lineitem' not found
        // ("tpch_simple_q3", include_str!("tpch_simple_q3.sql")),
        (
            "tpch_simple_q4",
            include_str!("../tpch_queries/tpch_simple_q4.sql"),
        ),
        (
            "tpch_simple_q5",
            include_str!("../tpch_queries/tpch_simple_q5.sql"),
        ),
    ]
}
