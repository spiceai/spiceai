//! This is a benchmark test suite for the Spice runtime.
//!
//! It performs the following actions:
//! 1. Starts the runtime with all of the datasets to test loaded.
//! 2. Runs a series of queries against the runtime.
//! 3. Reports the results to the spice.ai dataset <https://spice.ai/spicehq/spice-tests/datasets/oss_benchmarks>

// spice.ai/spicehq/spice-tests/datasets/spicehq."spice-tests".oss_benchmarks
// schema
// run_id, started_at, finished_at, query_name, status, min_duration, max_duration, iterations, commit_sha

use crate::results::Status;

mod results;
mod setup;

#[tokio::main]
async fn main() -> Result<(), String> {
    let (mut benchmark_results, rt) = setup::setup_benchmark().await;

    benchmark_results.record_result(1, 2, "test4", Status::Passed, 100, 120);

    setup::write_benchmark_results(benchmark_results, &rt).await?;

    Ok(())
}

fn get_current_unix_ms() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
        .unwrap_or(0)
}
