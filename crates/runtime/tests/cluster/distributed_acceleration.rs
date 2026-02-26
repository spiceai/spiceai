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
use spicepod::component::runtime::{Runtime as SpicepodRuntime, Scheduler as SchedulerConfig};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    partitioning::PartitionedBy,
};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use test_framework::pki::init_pki;
use tokio::time::{Instant, sleep};
use tracing_subscriber::EnvFilter;

use crate::{
    configure_test_datafusion,
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
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

/// Test that distributed acceleration with `bucket()` partitioning works end to end.
///
/// Sets up a cluster with 1 scheduler + 1 executor accelerating data
/// with `partition_by: bucket(3, id)` using the Cayenne engine. Verifies:
/// - `bucket()` UDF can be used in the dataset definition for partitioning
/// - Queries return correct, complete results
///
/// Only a single executor is used because all nodes in this in-process test share
/// the same filesystem; multiple executors would race writing to the same Cayenne
/// data/metadata directories during acceleration.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_acceleration_with_bucket_partitioning() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

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

            // Write test CSV data
            let csv_path = tempdir.path().join("test_data.csv");
            std::fs::write(&csv_path, TEST_DATA_CSV).expect("write test data file");
            let csv_source = format!("file:{}", csv_path.display());

            configure_test_datafusion();

            // --- Scheduler ---
            // Scheduler owns the dataset definition. The executor receives it via
            // `get_app_definition` gRPC and loads Cayenne file-mode acceleration.
            let scheduler_data_dir = tempdir.path().join("scheduler_data");
            std::fs::create_dir_all(&scheduler_data_dir).expect("create scheduler data dir");
            let scheduler_dataset =
                make_accelerated_dataset(&csv_source, "test_data", 3, "id", &scheduler_data_dir);

            // The scheduler requires a `runtime.scheduler` config with a
            // `state_location` so that the `PartitionManager` is created.
            // Without it, partition allocation to executors never happens and
            // the distributed query resolves to `EmptyExec`.
            let scheduler_app = AppBuilder::new("test_distributed_accel")
                .with_dataset(scheduler_dataset)
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_scheduler_config()),
                    ..SpicepodRuntime::default()
                })
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

            // Ensure the scheduler's HTTP and cluster gRPC servers are listening
            // before starting the executor — otherwise the executor's
            // `start_servers` → `initialize_cluster_executor` fails with a
            // transport error connecting to the scheduler.
            runtime_ready_check(&scheduler_rt).await;

            // Wait for port reachability so the executor can connect.
            wait_for_port("127.0.0.1:50352", Duration::from_secs(30)).await;

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
            let mut executor1_server_thread = tokio::spawn(async move {
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
                result = &mut executor1_server_thread => {
                    match result {
                        Ok(Ok(())) => return Err(anyhow::Error::msg("Executor1 server thread finished unexpectedly")),
                        Ok(Err(e)) => return Err(anyhow::Error::msg(format!("Executor1 server failed to start: {e}"))),
                        Err(e) => return Err(anyhow::Error::msg(format!("Executor1 server thread panicked: {e}"))),
                    }
                }
                () = Arc::clone(&executor1_rt).load_components() => {}
            }

            runtime_ready_check(&executor1_rt).await;

            // Wait for the executor to register with the scheduler
            let scheduler_server = scheduler_rt
                .datafusion()
                .scheduler_server
                .read()
                .expect("scheduler server lock")
                .clone()
                .expect("scheduler server should be available");
            let executor_manager = scheduler_server.state.executor_manager.clone();

            wait_for_executor_count(&executor_manager, 1, Duration::from_secs(15)).await?;

            // Wait for executor acceleration to complete. The executor loads datasets asynchronously after connecting to the scheduler
            sleep(Duration::from_secs(5)).await;

            // Test 1: SELECT all rows
            let query = QueryBuilder::new(
                "SELECT id, name, age, city, score FROM test_data ORDER BY id",
                scheduler_rt.datafusion(),
            );
            let result = query.build().run().await.map_err(|e| {
                anyhow::Error::msg(format!("Query 'select all' failed: {e}"))
            })?;
            let results: Vec<RecordBatch> = result.data.try_collect().await.map_err(|e| {
                anyhow::Error::msg(format!("Query 'select all' stream failed: {e}"))
            })?;

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            insta::assert_snapshot!("distributed_accel_select_all", pretty);

            // Test 2: Aggregation query — tests distributed GROUP BY with accelerated data
            let query = QueryBuilder::new(
                "SELECT COUNT(*) as total_rows, AVG(score) as avg_score, MIN(age) as min_age, MAX(age) as max_age FROM test_data",
                scheduler_rt.datafusion(),
            );
            let result = query.build().run().await.map_err(|e| {
                anyhow::Error::msg(format!("Query 'aggregation' failed: {e}"))
            })?;
            let results: Vec<RecordBatch> = result.data.try_collect().await.map_err(|e| {
                anyhow::Error::msg(format!("Query 'aggregation' stream failed: {e}"))
            })?;

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            insta::assert_snapshot!("distributed_accel_aggregation", pretty);

            // Cleanup
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

async fn wait_for_port(addr: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

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

/// Return a `SchedulerConfig` pointing at an S3 bucket for partition state.
///
/// `PartitionManager` uses OCC (optimistic concurrency control) which needs
/// conditional-put support (`PutMode::Update`); the local filesystem `ObjectStore`
/// does not support this, so S3 is required.
fn make_scheduler_config() -> SchedulerConfig {
    SchedulerConfig {
        state_location: "s3://spiceai-integration-tests/cluster-state/test_distributed_acceleration_with_bucket_partitioning/".to_string(),
        params: Some(spicepod::param::Params::from_string_map(
            std::collections::HashMap::from([
                ("s3_region".to_string(), "us-east-1".to_string()),
                ("s3_key".to_string(), "${env:AWS_S3_VECTORS_KEY}".to_string()),
                (
                    "s3_secret".to_string(),
                    "${env:AWS_S3_VECTORS_SECRET}".to_string(),
                ),
                ("s3_auth".to_string(), "key".to_string()),
            ]),
        )),
        partition_management: None,
    }
}

/// Create a dataset configured with Cayenne file-mode acceleration and `bucket()` partitioning.
///
/// Uses explicit `cayenne_file_path` and `cayenne_metadata_dir` so that each runtime in
/// a single-process test gets its own data/metadata directories without contention.
fn make_accelerated_dataset(
    source_path: &str,
    name: &str,
    num_buckets: i64,
    partition_column: &str,
    data_dir: &std::path::Path,
) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: format!("bucket({num_buckets}, {partition_column})"),
        }],
        params: Some(spicepod::param::Params::from_string_map(
            std::collections::HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    data_dir.join("data").to_string_lossy().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    data_dir.join("metadata").to_string_lossy().to_string(),
                ),
            ]),
        )),
        ..Acceleration::default()
    });

    dataset
}