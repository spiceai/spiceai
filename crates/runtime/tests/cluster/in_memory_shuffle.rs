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

//! Integration test for in-memory shuffle with multiple executors.
//!
//! This test verifies that shuffle partitions stored in memory on one executor
//! can be correctly fetched by another executor during distributed query execution.
//!
//! Related issue: <https://github.com/spiceai/spice/issues/9290>
//!
//! The fix ensures that when a shuffle partition with a `memory://` path is requested,
//! the executor first checks if the partition exists in its local `InMemoryShuffleManager`.
//! If not found locally, it falls back to fetching the partition from the remote executor
//! via Arrow Flight.

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
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use test_framework::pki::init_pki;
use tokio::time::{Instant, sleep};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

/// CSV data with more rows to ensure shuffle occurs across multiple partitions.
/// The data has multiple cities to enable meaningful GROUP BY operations.
const TEST_DATA_CSV: &str = r"id,name,age,city,score
1,Alice,30,New York,85
2,Bob,25,Los Angeles,90
3,Charlie,35,Chicago,88
4,Diana,28,New York,92
5,Eve,32,Los Angeles,78
6,Frank,29,Chicago,95
7,Grace,31,New York,82
8,Henry,27,Los Angeles,87
9,Ivy,33,Chicago,91
10,Jack,26,New York,84
11,Kate,34,Los Angeles,89
12,Leo,30,Chicago,86
13,Mia,29,New York,93
14,Noah,31,Los Angeles,81
15,Olivia,28,Chicago,88
16,Paul,32,New York,90
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
                            "Distributed query failed with retryable status; retrying"
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
                        "Distributed query stream creation failed with retryable status; retrying"
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
        "Distributed query failed after retry attempts",
    ))
}

/// Test that in-memory shuffle works correctly with multiple executors.
///
/// This test creates a cluster with:
/// - 1 scheduler
/// - 2 executors (both configured with `shuffle_location: memory`)
///
/// Then runs a GROUP BY query that requires shuffle between executors.
/// Before the fix for issue #9290, this would fail with:
/// `Shuffle partition not found in memory: memory://job-id/stage/partition`
///
/// The fix ensures executors correctly fetch shuffle partitions from remote
/// executors when they don't exist in local memory.
#[tokio::test(flavor = "multi_thread")]
async fn test_in_memory_shuffle_multiple_executors() -> Result<(), anyhow::Error> {
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

            // Write test data file
            std::fs::write(tempdir.path().join("test_shuffle_data.csv"), TEST_DATA_CSV)
                .expect("write test data file");

            // Scheduler app with dataset
            let scheduler_app = AppBuilder::new("test_in_memory_shuffle")
                .with_dataset(Dataset::new(
                    format!(
                        "file:{}",
                        tempdir
                            .path()
                            .join("test_shuffle_data.csv")
                            .to_str()
                            .expect("should have str")
                    )
                    .as_str(),
                    "test_data",
                ))
                .build();

            // Executor apps with in-memory shuffle configured
            let mut runtime_params = HashMap::new();
            runtime_params.insert("shuffle_location".to_string(), "memory".to_string());

            let executor1_app = AppBuilder::new("test_in_memory_shuffle_executor1")
                .with_runtime_params(runtime_params.clone())
                .build();
            let executor2_app = AppBuilder::new("test_in_memory_shuffle_executor2")
                .with_runtime_params(runtime_params)
                .build();

            configure_test_datafusion();

            // Scheduler config
            let scheduler_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8290,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50251,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Scheduler),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50252,
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

            // Start scheduler
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for scheduler to start"));
                }
                () = Arc::clone(&scheduler_rt).load_components() => {}
            }

            // Executor 1 config
            let executor1_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8291,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50253,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Executor),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50254,
                    )),
                    scheduler_address: Some("127.0.0.1:50252".to_string()),
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for executor1 to start"));
                }
                () = Arc::clone(&executor1_rt).load_components() => {}
            }

            // Executor 2 config
            let executor2_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8292,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50255,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Executor),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50256,
                    )),
                    scheduler_address: Some("127.0.0.1:50252".to_string()),
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for executor2 to start"));
                }
                () = Arc::clone(&executor2_rt).load_components() => {}
            }

            runtime_ready_check(&scheduler_rt).await;
            runtime_ready_check(&executor1_rt).await;
            runtime_ready_check(&executor2_rt).await;

            let scheduler_server = scheduler_rt
                .datafusion()
                .scheduler_server
                .read()
                .expect("scheduler server lock")
                .clone()
                .expect("scheduler server should be available");
            let executor_manager = scheduler_server.state.executor_manager.clone();

            // Wait for both executors to connect
            wait_for_executor_count(&executor_manager, 2, Duration::from_secs(15)).await?;

            // Give the scheduler a moment to observe executor capacity before planning.
            // Without this, the first query can race with cluster-capacity propagation and
            // fail with a transient non-successful completed job status.
            sleep(Duration::from_secs(2)).await;

            // Run a distributed GROUP BY query that requires shuffle.
            // This query aggregates by city, which will cause hash repartitioning
            // and require shuffle data exchange between executors.
            let results = run_distributed_query_with_retries(
                &scheduler_rt,
                "SELECT city, COUNT(*) as count, AVG(score) as avg_score \
                 FROM test_data \
                 GROUP BY city \
                 ORDER BY city",
                "test_in_memory_shuffle_group_by",
                6,
            )
            .await?;

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            insta::assert_snapshot!("in_memory_shuffle_group_by_results", pretty);

            // Run an ORDER BY query that also requires shuffle (sort merge).
            let results = run_distributed_query_with_retries(
                &scheduler_rt,
                "SELECT name, score FROM test_data ORDER BY score DESC LIMIT 5",
                "test_in_memory_shuffle_order_by",
                6,
            )
            .await?;

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            insta::assert_snapshot!("in_memory_shuffle_order_by_results", pretty);

            // Cleanup
            executor2_rt.shutdown().await;
            drop(executor2_rt);
            executor2_server_thread.abort();

            executor1_rt.shutdown().await;
            drop(executor1_rt);
            executor1_server_thread.abort();

            // Wait for executors to disconnect (best effort, don't fail test on cleanup race)
            let _ = wait_for_executor_count(&executor_manager, 0, Duration::from_secs(10)).await;

            scheduler_rt.shutdown().await;
            drop(scheduler_rt);
            scheduler_server_thread.abort();

            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            Ok(())
        })
        .await
}
