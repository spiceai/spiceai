/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Integration test for distributed acceleration with `bucket()` partitioning.
//!
//! Verifies that in a cluster with scheduler + executors:
//! 1. Partition values are resolved statically for deterministic `bucket(N, col)` expressions
//! 2. Executors correctly load accelerated data for their assigned partitions
//! 3. Queries through the scheduler return correct results from accelerated executors
//! 4. The `bucket()` UDF is available in the refresh context for partition filtering

use app::AppBuilder;
use arrow::array::RecordBatch;
use ballista_scheduler::state::executor_manager::ExecutorManager;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::ClusterConfig;
use runtime::datafusion::query::QueryBuilder;
use runtime::{auth::EndpointAuth, config::Config};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use spicepod::component::dataset::Dataset;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    partitioning::PartitionedBy,
};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use test_framework::pki::init_pki;
use tokio::time::{Instant, sleep};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

/// CSV test data
const TEST_DATA_CSV: &str = r"id,name,age,city,score
1,John Doe,28,New York,85
2,Jane Smith,34,Los Angeles,92
3,Mike Johnson,45,Chicago,78
4,Emily Brown,31,Houston,89
5,David Lee,39,Phoenix,76
6,Sarah Wilson,26,Philadelphia,94
7,Tom Anderson,52,San Antonio,81
8,Lisa Taylor,29,San Diego,88
9,Chris Martin,37,Dallas,79
10,Anna Garcia,41,San Jose,90
";

async fn wait_for_executor_count(
    executor_manager: &ExecutorManager,
    expected: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start = Instant::now();
    loop {
        let count = executor_manager
            .get_executor_state()
            .await
            .map_err(|err| anyhow::Error::msg(err.to_string()))?
            .len();
        if count == expected {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(anyhow::Error::msg(format!(
                "Timed out waiting for {expected} executors; found {count}"
            )));
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn run_distributed_query_with_retries(
    runtime: &Arc<Runtime>,
    sql: &str,
    job_name: &str,
    max_attempts: usize,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    for attempt in 1..=max_attempts {
        let query = QueryBuilder::new(sql, runtime.datafusion());
        let attempt_job_name = format!("{job_name}_{attempt}");
        let query_handle = query
            .build()
            .submit_distributed(&attempt_job_name)
            .await
            .map_err(|err| {
                anyhow::Error::msg(format!(
                    "Failed to submit distributed query {attempt_job_name}: {err}"
                ))
            })?;

        let stream_result = query_handle.into_stream().await;
        match stream_result {
            Ok(stream) => match stream.try_collect::<Vec<RecordBatch>>().await {
                Ok(results) => return Ok(results),
                Err(err) => {
                    let message = err.to_string();
                    let is_retryable =
                        message.contains("reported as completed but status is not successful");
                    if attempt < max_attempts && is_retryable {
                        tracing::warn!(
                            attempt,
                            max_attempts,
                            %message,
                            "Distributed query failed with retryable error; retrying"
                        );
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    return Err(anyhow::Error::msg(format!(
                        "Distributed query failed (attempt {attempt}/{max_attempts}): {message}"
                    )));
                }
            },
            Err(err) => {
                let message = err.to_string();
                let is_retryable =
                    message.contains("reported as completed but status is not successful");
                if attempt < max_attempts && is_retryable {
                    tracing::warn!(
                        attempt,
                        max_attempts,
                        %message,
                        "Distributed query stream creation failed with retryable error; retrying"
                    );
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(anyhow::Error::msg(format!(
                    "Failed to get distributed query stream (attempt {attempt}/{max_attempts}): {message}"
                )));
            }
        }
    }

    Err(anyhow::Error::msg(
        "Distributed query failed after all retry attempts",
    ))
}

/// Create a dataset configured with `DuckDB` in-memory acceleration and `bucket()` partitioning.
///
/// Uses `DuckDB` in-memory mode so each runtime (scheduler + executors) gets its own
/// isolated `DuckDB` instance — no file-path contention in single-process tests.
fn make_accelerated_dataset(
    source_path: &str,
    name: &str,
    num_buckets: i64,
    partition_column: &str,
) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: format!("bucket({num_buckets}, {partition_column})"),
        }],
        ..Acceleration::default()
    });

    dataset
}

/// Test that distributed acceleration with `bucket()` partitioning works end to end.
///
/// Sets up a cluster with 1 scheduler + 2 executors, each accelerating data
/// with `partition_by: bucket(3, id)`. Verifies:
/// - Static partition discovery (no source scan needed for deterministic bucket)
/// - `bucket()` UDF is available in the refresh context
/// - Executors load their assigned partitions into `DuckDB` in-memory acceleration
/// - Queries through the scheduler return correct, complete results
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
#[cfg(feature = "duckdb")]
#[ignore = "WIP: distributed acceleration with bucket partitioning; pending https://github.com/spiceai/spiceai/pull/9502"]
async fn test_distributed_acceleration_with_bucket_partitioning() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let tempdir = tempfile::tempdir().expect("should create temp dir");
            CryptoProvider::install_default(aws_lc_rs::default_provider())
                .expect("should install aws-lc-rs");

            let pki = init_pki(tempdir.path()).expect("should create PKI");
            let scheduler_cert = pki
                .create_client_cert("scheduler")
                .expect("should create scheduler cert");
            let executor1_cert = pki
                .create_client_cert("executor1")
                .expect("should create executor1 cert");
            let executor2_cert = pki
                .create_client_cert("executor2")
                .expect("should create executor2 cert");

            // Write test CSV data
            let csv_path = tempdir.path().join("test_data.csv");
            std::fs::write(&csv_path, TEST_DATA_CSV).expect("write test data file");
            let csv_source = format!("file:{}", csv_path.display());

            configure_test_datafusion();

            // --- Scheduler ---
            // Scheduler owns the dataset definition. Executors receive it via
            // `get_app_definition` gRPC and load their own DuckDB in-memory acceleration.
            let scheduler_dataset = make_accelerated_dataset(&csv_source, "test_data", 3, "id");

            let scheduler_app = AppBuilder::new("test_distributed_accel")
                .with_dataset(scheduler_dataset)
                .build();

            let scheduler_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8390,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50351,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Scheduler),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50352,
                    )),
                    node_advertise_address: Some("127.0.0.1".to_string()),
                    node_mtls_ca_certificate_file: Some(
                        pki.ca_cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_certificate_file: Some(
                        scheduler_cert.cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_key_file: Some(scheduler_cert.key_path.to_string_lossy().to_string()),
                    ..Default::default()
                },
            };

            let scheduler_rt = Arc::new(
                Runtime::builder()
                    .with_runtime_config(scheduler_config.clone())
                    .with_resolved_cluster_config(
                        ResolvedClusterConfig::try_new(scheduler_config.cluster.clone())
                            .expect("should resolve cluster config"),
                    )
                    .with_app(scheduler_app)
                    .build()
                    .await,
            );

            let cloned_scheduler_rt = Arc::clone(&scheduler_rt);
            let scheduler_server_thread = tokio::spawn(async move {
                Box::pin(cloned_scheduler_rt.start_servers(
                    scheduler_config,
                    None,
                    EndpointAuth::no_auth(),
                ))
                .await
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for scheduler to start"));
                }
                () = Arc::clone(&scheduler_rt).load_components() => {}
            }

            // --- Executor 1 ---
            // Executor apps are empty — they receive the dataset definition from
            // the scheduler via `get_app_definition` during cluster handshake.
            let executor1_app = AppBuilder::new("test_distributed_accel_executor1").build();

            let executor1_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8391,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50353,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Executor),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50354,
                    )),
                    scheduler_address: Some("127.0.0.1:50352".to_string()),
                    node_advertise_address: Some("127.0.0.1".to_string()),
                    node_mtls_ca_certificate_file: Some(
                        pki.ca_cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_certificate_file: Some(
                        executor1_cert.cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_key_file: Some(executor1_cert.key_path.to_string_lossy().to_string()),
                    ..Default::default()
                },
            };

            let executor1_rt = Arc::new(
                Runtime::builder()
                    .with_runtime_config(executor1_config.clone())
                    .with_resolved_cluster_config(
                        ResolvedClusterConfig::try_new(executor1_config.cluster.clone())
                            .expect("should resolve cluster config"),
                    )
                    .with_app(executor1_app)
                    .build()
                    .await,
            );

            let cloned_executor1_rt = Arc::clone(&executor1_rt);
            let executor1_server_thread = tokio::spawn(async move {
                Box::pin(cloned_executor1_rt.start_servers(
                    executor1_config,
                    None,
                    EndpointAuth::no_auth(),
                ))
                .await
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for executor1 to start"));
                }
                () = Arc::clone(&executor1_rt).load_components() => {}
            }

            // --- Executor 2 ---
            let executor2_app = AppBuilder::new("test_distributed_accel_executor2").build();

            let executor2_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8392,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50355,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Executor),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50356,
                    )),
                    scheduler_address: Some("127.0.0.1:50352".to_string()),
                    node_advertise_address: Some("127.0.0.1".to_string()),
                    node_mtls_ca_certificate_file: Some(
                        pki.ca_cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_certificate_file: Some(
                        executor2_cert.cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_key_file: Some(executor2_cert.key_path.to_string_lossy().to_string()),
                    ..Default::default()
                },
            };

            let executor2_rt = Arc::new(
                Runtime::builder()
                    .with_runtime_config(executor2_config.clone())
                    .with_resolved_cluster_config(
                        ResolvedClusterConfig::try_new(executor2_config.cluster.clone())
                            .expect("should resolve cluster config"),
                    )
                    .with_app(executor2_app)
                    .build()
                    .await,
            );

            let cloned_executor2_rt = Arc::clone(&executor2_rt);
            let executor2_server_thread = tokio::spawn(async move {
                Box::pin(cloned_executor2_rt.start_servers(
                    executor2_config,
                    None,
                    EndpointAuth::no_auth(),
                ))
                .await
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for executor2 to start"));
                }
                () = Arc::clone(&executor2_rt).load_components() => {}
            }

            runtime_ready_check(&scheduler_rt).await;
            runtime_ready_check(&executor1_rt).await;
            runtime_ready_check(&executor2_rt).await;

            // Wait for both executors to register with scheduler
            let scheduler_server = scheduler_rt
                .datafusion()
                .scheduler_server
                .read()
                .expect("scheduler server lock")
                .clone()
                .expect("scheduler server should be available");
            let executor_manager = scheduler_server.state.executor_manager.clone();

            wait_for_executor_count(&executor_manager, 2, Duration::from_secs(15)).await?;

            // Wait for executor acceleration to complete. Executors load datasets
            // asynchronously after connecting to the scheduler (inside `start_servers`).
            sleep(Duration::from_secs(5)).await;

            // Test 1: SELECT all rows — confirms all partitions are loaded across executors
            let results = run_distributed_query_with_retries(
                &scheduler_rt,
                "SELECT id, name, age, city, score FROM test_data ORDER BY id",
                "distributed_accel_select_all",
                6,
            )
            .await?;

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            insta::assert_snapshot!("distributed_accel_select_all", pretty);

            // Test 2: Aggregation query — tests distributed GROUP BY with accelerated data
            let results = run_distributed_query_with_retries(
                &scheduler_rt,
                "SELECT COUNT(*) as total_rows, AVG(score) as avg_score, MIN(age) as min_age, MAX(age) as max_age FROM test_data",
                "distributed_accel_aggregation",
                6,
            )
            .await?;

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            insta::assert_snapshot!("distributed_accel_aggregation", pretty);

            // Cleanup
            executor2_rt.shutdown().await;
            drop(executor2_rt);
            executor2_server_thread.abort();

            executor1_rt.shutdown().await;
            drop(executor1_rt);
            executor1_server_thread.abort();

            let _ = wait_for_executor_count(&executor_manager, 0, Duration::from_secs(10)).await;

            scheduler_rt.shutdown().await;
            drop(scheduler_rt);
            scheduler_server_thread.abort();

            tokio::time::sleep(Duration::from_secs(2)).await;

            Ok(())
        })
        .await
}
