use runtime::Runtime;

use crate::results::BenchmarkResultsBuilder;
use crate::setup::BenchAppBuilder;
use app::{App, AppBuilder};
use spicepod::component::{dataset::Dataset, params::Params};

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
) -> Result<(), String> {
    let test_queries = get_test_queries();

    for (query_name, query) in test_queries {
        super::run_query_and_record_result(rt, benchmark_results, "postgres", query_name, query)
            .await?;
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

pub struct PostgresBenchAppBuilder {}

impl PostgresBenchAppBuilder {
    fn get_postgres_params() -> Params {
        let pg_host = std::env::var("PG_BENCHMARK_PG_HOST").unwrap_or_default();
        let pg_user = std::env::var("PG_BENCHMARK_PG_USER").unwrap_or_default();
        let pg_pass = std::env::var("PG_BENCHMARK_PG_PASS").unwrap_or_default();
        let pg_db = std::env::var("PG_BENCHMARK_PG_DB").unwrap_or_default();
        let pg_sslmode = std::env::var("PG_BENCHMARK_PG_SSLMODE").unwrap_or_default();
        // Get postgres params from github secret?
        Params::from_string_map(
            vec![
                ("pg_host".to_string(), pg_host),
                ("pg_user".to_string(), pg_user),
                ("pg_db".to_string(), pg_db),
                ("pg_pass".to_string(), pg_pass),
                ("pg_sslmode".to_string(), pg_sslmode),
            ]
            .into_iter()
            .collect(),
        )
    }
}

impl BenchAppBuilder for PostgresBenchAppBuilder {
    fn build_app(&self, upload_results_dataset: &Option<String>) -> App {
        let mut app_builder = AppBuilder::new("runtime_benchmark_test")
            .with_dataset(self.make_dataset("customer", "customer"))
            .with_dataset(self.make_dataset("lineitem", "lineitem"))
            .with_dataset(self.make_dataset("part", "part"))
            .with_dataset(self.make_dataset("partsupp", "partsupp"))
            .with_dataset(self.make_dataset("orders", "orders"))
            .with_dataset(self.make_dataset("nation", "nation"))
            .with_dataset(self.make_dataset("region", "region"))
            .with_dataset(self.make_dataset("supplier", "supplier"));

        if let Some(upload_results_dataset) = upload_results_dataset {
            app_builder = app_builder
                .with_dataset(self.make_rw_dataset(upload_results_dataset, "oss_benchmarks"));
        }

        app_builder.build()
    }

    fn make_dataset(&self, path: &str, name: &str) -> Dataset {
        let mut dataset = Dataset::new(format!("postgres:{path}"), name.to_string());
        dataset.params = Some(Self::get_postgres_params());
        dataset
    }
}
