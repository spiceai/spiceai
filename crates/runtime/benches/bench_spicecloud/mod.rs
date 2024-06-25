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
        // Error: "query `tpch_q1` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q1", include_str!("tpch_q1.sql")),
        ("tpch_q2", include_str!("tpch_q2.sql")),
        // Error: "query `tpch_q3` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q3", include_str!("tpch_q3.sql")),
        // Error: "query `tpch_q4` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Encountered \\\")\\\" at line 1, column 276.\\nWas expecting one of:\\n    \\\"YEAR\\\" ...\\n    \\\"MONTH\\\" ...\\n    \\\"DAY\\\" ...\\n    \\\"HOUR\\\" ...\\n    \\\"MINUTE\\\" ...\\n    \\\"SECOND\\\" ...\\n    \\nstartLine 1\\nstartColumn 276\\nendLine 1\\nendColumn 276\\nSQL Query SELECT tpch.orders.o_orderpriority, COUNT(1) AS order_count FROM tpch.orders WHERE (((tpch.orders.o_orderdate >= CAST('1993-07-01' AS DATETIME)) AND (tpch.orders.o_orderdate < CAST((CAST('1993-07-01' AS DATE) + INTERVAL '0 YEARS 3 MONS 0 DAYS 0 HOURS 0 MINS 0.000000000 SECS') AS DATETIME)))
        // ("tpch_q4", include_str!("tpch_q4.sql")),
        // Error: "query `tpch_q5` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q5", include_str!("tpch_q5.sql")),
        // Error: "query `tpch_q6` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q6", include_str!("tpch_q6.sql")),
        // Error: "query `tpch_q7` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q7", include_str!("tpch_q7.sql")),
        // Error: "query `tpch_q8` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q8", include_str!("tpch_q8.sql")),
        // Error: "query `tpch_q9` to results: federation_optimizer_rule\ncaused by\nfederate_sql\ncaused by\nSchema error: No field named profit.\"tpch.nation\". Valid fields are profit.nation, profit.o_year, profit.amount."
        // ("tpch_q9", include_str!("tpch_q9.sql")),
        // Error: "query `tpch_q10` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q10", include_str!("tpch_q10.sql")),
        ("tpch_q11", include_str!("tpch_q11.sql")),
        // Error: "query `tpch_q12` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'\\nstartLine 1\\nstartColumn 751\\nendLine 1\\nendColumn 758\\n
        //        SQL Query SELECT \\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipmode\\\", SUM(CASE WHEN ((\\\"tpch\\\".\\\"orders\\\".\\\"o_orderpriority\\\" = '1-URGENT') OR (\\\"tpch\\\".\\\"orders\\\".\\\"o_orderpriority\\\" = '2-HIGH')) THEN 1 ELSE 0 END)
        //        AS \\\"high_line_count\\\", SUM(CASE WHEN ((\\\"tpch\\\".\\\"orders\\\".\\\"o_orderpriority\\\" <> '1-URGENT') AND (\\\"tpch\\\".\\\"orders\\\".\\\"o_orderpriority\\\" <> '2-HIGH')) THEN 1 ELSE 0 END) AS \\\"low_line_count\\\" FROM
        //        \\\"tpch\\\".\\\"lineitem\\\" JOIN \\\"tpch\\\".\\\"orders\\\" ON (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_orderkey\\\" = \\\"tpch\\\".\\\"orders\\\".\\\"o_orderkey\\\") WHERE ((((\\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipmode\\\" IN ('MAIL', 'SHIP')
        //        AND (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_commitdate\\\" < \\\"tpch\\\".\\\"lineitem\\\".\\\"l_receiptdate\\\")) AND (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipdate\\\" < \\\"tpch\\\".\\\"lineitem\\\".\\\"l_commitdate\\\")) AND
        //        (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_receiptdate\\\" >= CAST(CAST('1994-01-01' AS DATE) AS DATETIME))) AND (\\\"tpch\\\".\\\"lineitem\\\".\\\"l_receiptdate\\\" < CAST(CAST('1995-01-01' AS DATE) AS DATETIME))) GROUP BY \\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipmode\\\"
        //        ORDER BY \\\"tpch\\\".\\\"lineitem\\\".\\\"l_shipmode\\\" ASC NULLS LAST\", ... }"
        // ("tpch_q12", include_str!("tpch_q12.sql")),
        // Error: "query `tpch_q13` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.customer' not found ...
        // ("tpch_q13", include_str!("tpch_q13.sql")),
        // Error: "query `tpch_q14` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q14", include_str!("tpch_q14.sql")),

        // tpch_q15 has a view creation which we don't support by design

        // Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.supplier' not found
        // ("tpch_q16", include_str!("tpch_q16.sql")),
        ("tpch_q17", include_str!("tpch_q17.sql")),
        ("tpch_q18", include_str!("tpch_q18.sql")),
        ("tpch_q19", include_str!("tpch_q19.sql")),
        // Error: "query `tpch_q20` to results: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Unknown identifier 'DATETIME'
        // ("tpch_q20", include_str!("tpch_q20.sql")),
        ("tpch_q21", include_str!("tpch_q21.sql")),
        // Error: "query `tpch_q22` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.orders' not found
        // ("tpch_q22", include_str!("tpch_q22.sql")),
        ("tpch_simple_q1", include_str!("tpch_simple_q1.sql")),
        ("tpch_simple_q2", include_str!("tpch_simple_q2.sql")),
        // Error: "query `tpch_simple_q3` to results: External error: Execution error: Unable to query Flight: Unable to query: status: InvalidArgument, message: \"Table 'tpch.lineitem' not found
        // ("tpch_simple_q3", include_str!("tpch_simple_q3.sql")),
        ("tpch_simple_q4", include_str!("tpch_simple_q4.sql")),
        ("tpch_simple_q5", include_str!("tpch_simple_q5.sql")),
    ]
}
