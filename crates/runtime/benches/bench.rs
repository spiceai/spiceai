//! This is a benchmark test suite for the Spice runtime.
//!
//! It performs the following actions:
//! 1. Starts the runtime with all of the datasets to test loaded.
//! 2. Runs a series of queries against the runtime.
//! 3. Reports the results to the spice.ai dataset <https://spice.ai/spicehq/spice-tests/datasets/oss_benchmarks>

// spice.ai/spicehq/spice-tests/datasets/spicehq."spice-tests".oss_benchmarks
// schema
// run_id, started_at, finished_at, query_name, status, min_duration, max_duration, iterations, commit_sha

use clap::Parser;
use results::BenchmarkResultsBuilder;
use runtime::Runtime;

use crate::results::Status;

mod results;
mod setup;

mod bench_spiceai;

#[derive(Parser)]
pub struct Args {
    #[arg(long)]
    pub upload_results_dataset: Option<String>,

    // Needed to prevent Clap from erroring when this is set automatically by `cargo bench`.
    #[arg(long)]
    pub bench: bool,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = Args::parse();

    let (mut benchmark_results, mut rt) = setup::setup_benchmark(&args).await;

    bench_spiceai::run(&mut rt, &mut benchmark_results).await?;

    if let Some(upload_results_dataset) = args.upload_results_dataset {
        tracing::info!("Writing benchmark results to dataset {upload_results_dataset}...");
        setup::write_benchmark_results(benchmark_results, &rt).await?;
    }

    Ok(())
}

fn get_current_unix_ms() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
        .unwrap_or(0)
}

async fn run_query_and_record_result(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    query_name: &str,
    query: &str,
) -> Result<(), String> {
    tracing::info!("Running query `{query_name}`...");
    let start_time = get_current_unix_ms();

    let mut min_iter_duration_ms = i64::MAX;
    let mut max_iter_duration_ms = i64::MIN;

    for _ in 0..benchmark_results.iterations() {
        let start_iter_time = get_current_unix_ms();
        let _ = rt
            .df
            .ctx
            .sql(query)
            .await
            .map_err(|e| format!("query `{query_name}` to plan: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("query `{query_name}` to results: {e}"))?;
        let end_iter_time = get_current_unix_ms();

        let iter_duration_ms = end_iter_time - start_iter_time;
        if iter_duration_ms < min_iter_duration_ms {
            min_iter_duration_ms = iter_duration_ms;
        }
        if iter_duration_ms > max_iter_duration_ms {
            max_iter_duration_ms = iter_duration_ms;
        }
    }

    let end_time = get_current_unix_ms();

    benchmark_results.record_result(
        start_time,
        end_time,
        query_name,
        Status::Passed,
        min_iter_duration_ms,
        max_iter_duration_ms,
    );

    Ok(())
}
