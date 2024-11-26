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
use app::{App, AppBuilder};
use datafusion::prelude::SessionContext;
use futures::Future;
use runtime::{
    datafusion::DataFusion,
    dataupdate::DataUpdate,
    status::{self, RuntimeStatus},
    Runtime,
};
use spicepod::component::dataset::{
    acceleration::{Acceleration, IndexType},
    replication::Replication,
    Dataset, Mode,
};
use std::{collections::HashMap, process::Command, sync::Arc, time::Duration};
use tracing_subscriber::EnvFilter;

/// The number of times to run each query in the benchmark.
const ITERATIONS: i32 = 5;

/// Gets a test `DataFusion` to make test results reproducible across all machines.
///
/// 1) Sets the number of `target_partitions` to 4, by default its the number of CPU cores available.
fn get_test_datafusion(status: Arc<RuntimeStatus>) -> Arc<DataFusion> {
    let mut df = DataFusion::builder(status).build();

    // Set the target partitions to 3 to make RepartitionExec show consistent partitioning across machines with different CPU counts.
    let mut new_state = df.ctx.state();
    new_state
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 4;
    let new_ctx = SessionContext::new_with_state(new_state);

    // Replace the old context with the modified one
    df.ctx = new_ctx.into();
    Arc::new(df)
}

pub(crate) async fn setup_benchmark(
    upload_results_dataset: &Option<String>,
    connector: &str,
    acceleration: Option<Acceleration>,
    bench_name: &str,
) -> Result<(BenchmarkResultsBuilder, Runtime), String> {
    init_tracing();

    let app = build_app(upload_results_dataset, connector, acceleration, bench_name)?;

    let status = status::RuntimeStatus::new();
    let rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(get_test_datafusion(Arc::clone(&status)))
        .with_runtime_status(status)
        .build()
        .await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(20 * 60)) => { // Databricks can take awhile to start up
            panic!("Timed out waiting for datasets to load in setup_benchmark()");
        }
        () = rt.load_components() => {}
    }

    let wait_time = match bench_name {
        "clickbench" => std::time::Duration::from_secs(3 * 60 * 60),
        _ => std::time::Duration::from_secs(10 * 60),
    };

    runtime_ready_check(&rt, wait_time).await;

    let benchmark_results =
        BenchmarkResultsBuilder::new(get_commit_sha(), get_branch_name(), ITERATIONS);

    Ok((benchmark_results, rt))
}

async fn runtime_ready_check(rt: &Runtime, wait_time: Duration) {
    assert!(wait_until_true(wait_time, || async { rt.status().is_ready() }).await);
}

async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    false
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

fn build_app(
    upload_results_dataset: &Option<String>,
    connector: &str,
    acceleration: Option<Acceleration>,
    bench_name: &str,
) -> Result<App, String> {
    let mut app_builder = AppBuilder::new("runtime_benchmark_test");

    app_builder = match connector {
        "spice.ai" => Ok(crate::bench_spicecloud::build_app(app_builder)),
        // Run both S3, ABFS and any other object store benchmarks
        "s3" | "abfs" => {
            // SQLite acceleration does not support default TPC-DS source scale so we use a smaller scale
            if bench_name == "tpcds"
                && acceleration
                    .as_ref()
                    .is_some_and(|a| a.engine == Some("sqlite".to_string()))
            {
                crate::bench_object_store::build_app(connector, app_builder, "tpcds_sf0_01")
            } else {
                crate::bench_object_store::build_app(connector, app_builder, bench_name)
            }
        }
        #[cfg(feature = "spark")]
        "spark" => Ok(crate::bench_spark::build_app(app_builder)),
        #[cfg(feature = "postgres")]
        "postgres" => crate::bench_postgres::build_app(app_builder, bench_name),
        #[cfg(feature = "mysql")]
        "mysql" => crate::bench_mysql::build_app(app_builder, bench_name),
        #[cfg(feature = "duckdb")]
        "duckdb" => crate::bench_duckdb::build_app(app_builder, bench_name),
        #[cfg(feature = "odbc")]
        "odbc-databricks" => Ok(crate::bench_odbc_databricks::build_app(app_builder)),
        #[cfg(feature = "odbc")]
        "odbc-athena" => Ok(crate::bench_odbc_athena::build_app(app_builder)),
        #[cfg(feature = "delta_lake")]
        "delta_lake" => Ok(crate::bench_delta::build_app(app_builder)),
        _ => Err(format!("Unknown connector: {connector}")),
    }?;

    if let Some(upload_results_dataset) = upload_results_dataset {
        app_builder = app_builder.with_dataset(make_spiceai_rw_dataset(
            upload_results_dataset,
            "oss_benchmarks",
        ));
    }

    let mut app = app_builder.build();

    if let Some(accel) = acceleration {
        app.datasets.iter_mut().for_each(|ds| {
            let mut accel = accel.clone();
            let indexes = get_accelerator_indexes(accel.engine.clone(), &ds.name, bench_name);
            if let Some(indexes) = indexes {
                accel.indexes = indexes;
            }
            if ds.name != "oss_benchmarks" {
                ds.acceleration = Some(accel);
            }
        });
    }

    Ok(app)
}

#[allow(clippy::too_many_lines)]
fn get_accelerator_indexes(
    engine: Option<String>,
    dataset: &str,
    bench_name: &str,
) -> Option<HashMap<String, IndexType>> {
    if let Some(engine) = engine {
        match engine.as_str() {
            "sqlite" => match bench_name {
                "tpch" => match dataset {
                    "orders" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("o_orderdate".to_string(), IndexType::Enabled);
                        indexes.insert("o_orderkey".to_string(), IndexType::Enabled);
                        indexes.insert("o_custkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "lineitem" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("l_orderkey".to_string(), IndexType::Enabled);
                        indexes.insert("l_suppkey".to_string(), IndexType::Enabled);
                        indexes.insert("l_discount".to_string(), IndexType::Enabled);
                        indexes.insert("l_shipdate".to_string(), IndexType::Enabled);
                        indexes.insert("l_partkey".to_string(), IndexType::Enabled);
                        indexes.insert("l_quantity".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "partsupp" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ps_suppkey".to_string(), IndexType::Enabled);
                        indexes.insert("ps_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "part" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("p_partkey".to_string(), IndexType::Enabled);
                        indexes.insert("p_brand".to_string(), IndexType::Enabled);
                        indexes.insert("p_container".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "nation" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("n_nationkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "supplier" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("s_suppkey".to_string(), IndexType::Enabled);
                        indexes.insert("s_nationkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("c_phone".to_string(), IndexType::Enabled);
                        indexes.insert("c_acctbal".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                "tpcds" => match dataset {
                    "catalog_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("cs_ship_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("cs_sold_date_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer_address" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ca_county".to_string(), IndexType::Enabled);
                        indexes.insert(
                            "(ca_address_sk, ca_country, ca_state)".to_string(),
                            IndexType::Enabled,
                        );
                        Some(indexes)
                    }
                    "customer_demographics" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert(
                            "(cd_demo_sk, cd_marital_status, cd_education_status)".to_string(),
                            IndexType::Enabled,
                        );
                        Some(indexes)
                    }
                    "date_dim" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("(d_year, d_date_sk)".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "household_demographics" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes
                            .insert("(hd_demo_sk, hd_dep_count)".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "store_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ss_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_sold_date_sk".to_string(), IndexType::Enabled);
                        indexes.insert(
                            "(ss_store_sk, ss_sold_date_sk, ss_sales_price, ss_net_profit)"
                                .to_string(),
                            IndexType::Enabled,
                        );
                        Some(indexes)
                    }
                    "web_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ws_bill_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ws_sold_date_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                _ => None,
            },
            "postgres" => match bench_name {
                "tpch" => match dataset {
                    "partsupp" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ps_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "part" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("p_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "lineitem" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("l_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                "tpcds" => match dataset {
                    "store_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ss_item_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_store_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_ticket_number".to_string(), IndexType::Enabled);
                        indexes.insert("ss_hdemo_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "store" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("s_store_name".to_string(), IndexType::Enabled);
                        indexes.insert("s_zip".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "item" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("i_item_sk".to_string(), IndexType::Enabled);
                        indexes.insert("i_manufact_id".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("c_current_addr_sk".to_string(), IndexType::Enabled);
                        indexes.insert("c_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("c_current_hdemo_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "catalog_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("cs_sold_date_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "date_dim" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("d_year".to_string(), IndexType::Enabled);
                        indexes.insert("d_date".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer_demographics" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("cd_demo_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    }
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

fn make_spiceai_rw_dataset(path: &str, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai:{path}"), name.to_string());
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
