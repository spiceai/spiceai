#[cfg(feature = "mysql")]
use crate::bench_mysql::MySqlBenchAppBuilder;
#[cfg(feature = "postgres")]
use crate::bench_postgres::PostgresBenchAppBuilder;
use crate::bench_spicecloud::SpiceAIBenchAppBuilder;
use crate::results::BenchmarkResultsBuilder;
use app::App;
use runtime::{dataupdate::DataUpdate, Runtime};
use spicepod::component::dataset::{replication::Replication, Dataset, Mode};
use std::process::Command;
use tracing_subscriber::EnvFilter;

/// The number of times to run each query in the benchmark.
const ITERATIONS: i32 = 5;

#[derive(Clone, Copy)]
pub enum DataConnector {
    Postgres,
    SpiceAI,
    MySql,
}

impl std::fmt::Display for DataConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataConnector::Postgres => write!(f, "postgres"),
            DataConnector::SpiceAI => write!(f, "spiceai"),
            DataConnector::MySql => write!(f, "mysql"),
        }
    }
}

#[allow(unreachable_patterns)]
pub(crate) async fn setup_benchmark(
    upload_results_dataset: &Option<String>,
    dataconnector: DataConnector,
) -> Result<(BenchmarkResultsBuilder, Runtime), String> {
    init_tracing();

    let app = match dataconnector {
        DataConnector::SpiceAI => {
            SpiceAIBenchAppBuilder::build_app(&SpiceAIBenchAppBuilder {}, upload_results_dataset)
        }
        #[cfg(feature = "postgres")]
        DataConnector::Postgres => {
            PostgresBenchAppBuilder::build_app(&PostgresBenchAppBuilder {}, upload_results_dataset)
        }
        #[cfg(feature = "mysql")]
        DataConnector::MySql => {
            MySqlBenchAppBuilder::build_app(&MySqlBenchAppBuilder {}, upload_results_dataset)
        }
        _ => return Err("Not reachable".to_string()),
    };

    let rt = Runtime::builder().with_app(app).build().await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
            panic!("Timed out waiting for datasets to load in setup_benchmark()");
        }
        () = rt.load_components() => {}
    }

    let benchmark_results =
        BenchmarkResultsBuilder::new(get_commit_sha(), get_branch_name(), ITERATIONS);

    Ok((benchmark_results, rt))
}

pub(crate) async fn write_benchmark_results(
    benchmark_results: DataUpdate,
    rt: &Runtime,
) -> Result<(), String> {
    rt.datafusion()
        .write_data("oss_benchmarks".into(), benchmark_results)
        .await
        .map_err(|e| e.to_string())
}

fn init_tracing() {
    let filter = match std::env::var("SPICED_LOG").ok() {
        Some(level) => EnvFilter::new(level),
        _ => EnvFilter::new(
            "runtime=TRACE,datafusion-federation=TRACE,datafusion-federation-sql=TRACE,bench=TRACE",
        ),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

// This should also append "-dirty" if there are uncommitted changes
fn get_commit_sha() -> String {
    let short_sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .map_or_else(
            |_| "unknown".to_string(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_string(),
        );
    format!(
        "{}{}",
        short_sha,
        if is_repo_dirty() { "-dirty" } else { "" }
    )
}

#[allow(clippy::map_unwrap_or)]
fn is_repo_dirty() -> bool {
    let output = Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .output()
        .map(|output| {
            std::str::from_utf8(&output.stdout)
                .map(ToString::to_string)
                .unwrap_or_else(|_| String::new())
        })
        .unwrap_or_else(|_| String::new());

    !output.trim().is_empty()
}

fn get_branch_name() -> String {
    Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .map_or_else(
            |_| "unknown".to_string(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_string(),
        )
}

pub trait BenchAppBuilder {
    fn build_app(&self, upload_results_dataset: &Option<String>) -> App;
    fn make_dataset(&self, path: &str, name: &str) -> Dataset;

    fn make_rw_dataset(&self, path: &str, name: &str) -> Dataset {
        let mut ds = Dataset::new(format!("spiceai:{path}"), name.to_string());
        ds.mode = Mode::ReadWrite;
        ds.replication = Some(Replication { enabled: true });
        ds
    }
}
