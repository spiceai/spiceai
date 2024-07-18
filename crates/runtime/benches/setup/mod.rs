use crate::results::BenchmarkResultsBuilder;
use app::{App, AppBuilder};
use runtime::{dataconnector, dataupdate::DataUpdate, Runtime};
use spicepod::component::{
    dataset::{replication::Replication, Dataset, Mode},
    params::Params,
    secrets::SpiceSecretStore,
};
use std::process::Command;
use tracing_subscriber::EnvFilter;

/// The number of times to run each query in the benchmark.
const ITERATIONS: i32 = 5;

pub enum DataConnector {
    Postgres,
    SpiceAI,
}

pub(crate) async fn setup_benchmark(
    upload_results_dataset: &Option<String>,
    dataconnector: DataConnector,
) -> (BenchmarkResultsBuilder, Runtime) {
    init_tracing();

    let app =
        match dataconnector {
            DataConnector::Postgres => PostgresBenchAppBuilder::new(upload_results_dataset)
                .build_app(upload_results_dataset),
            DataConnector::SpiceAI => SpiceAIBenchAppBuilder::new(upload_results_dataset)
                .build_app(upload_results_dataset),
            _ => {
                unimplemented!()
            }
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

    (benchmark_results, rt)
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

trait BenchAppBuilder {
    fn build_app(&self, upload_results_dataset: Option<&str>) -> App {
        let mut app_builder = AppBuilder::new("runtime_benchmark_test")
            .with_secret_store(SpiceSecretStore::File)
            .with_dataset(self.make_dataset("tpch.customer", "customer"))
            .with_dataset(self.make_dataset("tpch.lineitem", "lineitem"))
            .with_dataset(self.make_dataset("tpch.part", "part"))
            .with_dataset(self.make_dataset("tpch.partsupp", "partsupp"))
            .with_dataset(self.make_dataset("tpch.orders", "orders"))
            .with_dataset(self.make_dataset("tpch.nation", "nation"))
            .with_dataset(self.make_dataset("tpch.region", "region"))
            .with_dataset(self.make_dataset("tpch.supplier", "supplier"));

        if let Some(upload_results_dataset) = upload_results_dataset {
            app_builder = app_builder
                .with_dataset(self.make_rw_dataset(upload_results_dataset, "oss_benchmarks"));
        }
    }
    fn make_dataset(&self, path: &str, name: &str) -> Dataset;
    fn make_rw_dataset(&self, path: &str, name: &str) -> Dataset;
}

struct SpiceAIBenchAppBuilder {}

impl BenchAppBuilder for SpiceAIBenchAppBuilder {
    fn make_dataset(&self, path: &str, name: &str) -> Dataset {
        Dataset::new(format!("spiceai:{path}"), name.to_string())
    }

    fn make_rw_dataset(&self, path: &str, name: &str) -> Dataset {
        let mut ds = Dataset::new(format!("spiceai:{path}"), name.to_string());
        ds.mode = Mode::ReadWrite;
        ds.replication = Some(Replication { enabled: true });
        ds
    }
}

struct PostgresBenchAppBuilder {}

impl PostgresBenchAppBuilder {
    fn get_postgres_params() -> Option<Params> {
        // Get postgres params from github secret?
        todo!()
    }
}

impl BenchAppBuilder for PostgresBenchAppBuilder {
    fn make_dataset(&self, path: &str, name: &str) -> Dataset {
        let mut dataset = Dataset::new(format!("postgres:{path}"), name.to_string());
        ddataset.params = Self::get_postgres_params();
        dataset
    }

    fn make_rw_dataset(&self, path: &str, name: &str) -> Dataset {
        let mut ds = Dataset::new(format!("postgres:{path}"), name.to_string());
        ds.mode = Mode::ReadWrite;
        dataset.params = Self::get_postgres_params();
        ds.replication = Some(Replication { enabled: true });
        ds
    }
}
