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

use crate::{bench_s3, results::BenchmarkResultsBuilder};
use app::{App, AppBuilder};
use runtime::{dataupdate::DataUpdate, Runtime};
use spicepod::component::dataset::{replication::Replication, Dataset, Mode};
use std::process::Command;
use tracing_subscriber::EnvFilter;

#[cfg(feature = "mysql")]
use crate::bench_mysql;
#[cfg(feature = "odbc")]
use crate::bench_odbc_databricks;
#[cfg(feature = "postgres")]
use crate::bench_postgres;

/// The number of times to run each query in the benchmark.
const ITERATIONS: i32 = 5;

pub(crate) async fn setup_benchmark(
    upload_results_dataset: &Option<String>,
    connector: &str,
) -> (BenchmarkResultsBuilder, Runtime) {
    init_tracing();

    let app = build_app(upload_results_dataset, connector);

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

fn build_app(upload_results_dataset: &Option<String>, connector: &str) -> App {
    let mut app_builder = AppBuilder::new("runtime_benchmark_test");

    app_builder = match connector {
        "spice.ai" => app_builder
            .with_dataset(make_spiceai_dataset("tpch.customer", "customer"))
            .with_dataset(make_spiceai_dataset("tpch.lineitem", "lineitem"))
            .with_dataset(make_spiceai_dataset("tpch.part", "part"))
            .with_dataset(make_spiceai_dataset("tpch.partsupp", "partsupp"))
            .with_dataset(make_spiceai_dataset("tpch.orders", "orders"))
            .with_dataset(make_spiceai_dataset("tpch.nation", "nation"))
            .with_dataset(make_spiceai_dataset("tpch.region", "region"))
            .with_dataset(make_spiceai_dataset("tpch.supplier", "supplier")),
        "spark" => app_builder
            .with_dataset(make_spark_dataset("samples.tpch.customer", "customer"))
            .with_dataset(make_spark_dataset("samples.tpch.lineitem", "lineitem"))
            .with_dataset(make_spark_dataset("samples.tpch.part", "part"))
            .with_dataset(make_spark_dataset("samples.tpch.partsupp", "partsupp"))
            .with_dataset(make_spark_dataset("samples.tpch.orders", "orders"))
            .with_dataset(make_spark_dataset("samples.tpch.nation", "nation"))
            .with_dataset(make_spark_dataset("samples.tpch.region", "region"))
            .with_dataset(make_spark_dataset("samples.tpch.supplier", "supplier")),
        "s3" => bench_s3::build_app(app_builder),
        #[cfg(feature = "postgres")]
        "postgres" => bench_postgres::build_app(app_builder),
        #[cfg(feature = "mysql")]
        "mysql" => bench_mysql::build_app(app_builder),
        #[cfg(feature = "odbc")]
        "odbc" => bench_odbc_databricks::build_app(app_builder),
        _ => app_builder,
    };

    if let Some(upload_results_dataset) = upload_results_dataset {
        app_builder = app_builder.with_dataset(make_spiceai_rw_dataset(
            upload_results_dataset,
            "oss_benchmarks",
        ));
    }

    app_builder.build()
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

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spiceai:{path}"), name.to_string())
}

fn make_spark_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spark:{path}"), name.to_string())
}

fn make_spiceai_rw_dataset(path: &str, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("spiceai:{path}"), name.to_string());
    ds.mode = Mode::ReadWrite;
    ds.replication = Some(Replication { enabled: true });
    ds
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
