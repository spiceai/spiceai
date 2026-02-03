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

use app::AppBuilder;

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::ClusterConfig;
use runtime::datafusion::query::QueryBuilder;
use runtime::{auth::EndpointAuth, config::Config};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use spicepod::component::dataset::Dataset;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use test_framework::pki::init_pki;
use tokio::time::{Instant, sleep_until};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

const NAMES_CSV: &str = include_str!("../acceleration/data/names.csv");

#[expect(clippy::expect_used)]
async fn snapshot_names_from_runtime(
    name: &str,
    rt: &Arc<Runtime>,
    dataset_name: Option<&str>,
    temp_path: String,
) {
    let explain_result: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder(
            format!(
                "EXPLAIN SELECT id, name, age, city, score FROM {} ORDER BY id",
                dataset_name.unwrap_or("names")
            )
            .as_str(),
        )
        .build()
        .run()
        .await
        .expect("query is successful")
        .data
        .try_collect()
        .await
        .expect("collects results");

    let pretty = arrow::util::pretty::pretty_format_batches(&explain_result)
        .map_err(|e| anyhow::Error::msg(e.to_string()))
        .expect("Should format batches")
        .to_string();
    insta::assert_snapshot!(
        format!("explain_{name}"),
        pretty.replace(
            temp_path
                .as_str()
                .split_once('/')
                .expect("should have leading /")
                .1,
            "<TEMP_PATH>"
        )
    );

    let result: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder(
            format!(
                "SELECT id, name, age, city, score FROM {} ORDER BY id",
                dataset_name.unwrap_or("names")
            )
            .as_str(),
        )
        .build()
        .run()
        .await
        .expect("query is successful")
        .data
        .try_collect()
        .await
        .expect("collects results");

    let pretty = arrow::util::pretty::pretty_format_batches(&result)
        .map_err(|e| anyhow::Error::msg(e.to_string()))
        .expect("Should format batches");
    insta::assert_snapshot!(name, pretty);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_cluster_mode() -> Result<(), anyhow::Error> {
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
            let executor_cert = pki
                .create_client_cert("executor")
                .expect("should create executor cert");

            std::fs::write(
                tempdir.path().join("./test_simple_cluster_mode.csv"),
                NAMES_CSV,
            )
            .expect("write file");

            let scheduler_app = AppBuilder::new("test_simple_cluster_mode")
                .with_dataset(Dataset::new(
                    format!(
                        "file:{}",
                        tempdir
                            .path()
                            .join("test_simple_cluster_mode.csv")
                            .to_str()
                            .expect("should have str")
                    )
                    .as_str(),
                    "names",
                ))
                .build();

            let executor_app = AppBuilder::new("test_simple_cluster_mode_executor").build();

            configure_test_datafusion();

            let scheduler_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8190,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50151,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Scheduler),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50152,
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&scheduler_rt).load_components() => {}
            }

            let executor_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8191,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    50153,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Executor),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        50154,
                    )),
                    scheduler_address: Some("127.0.0.1:50152".to_string()),
                    node_advertise_address: Some("127.0.0.1".to_string()),
                    node_mtls_ca_certificate_file: Some(
                        pki.ca_cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_certificate_file: Some(
                        executor_cert.cert_path.to_string_lossy().to_string(),
                    ),
                    node_mtls_key_file: Some(executor_cert.key_path.to_string_lossy().to_string()),
                    ..Default::default()
                },
            };

            let executor_rt = Arc::new(
                Runtime::builder()
                    .with_runtime_config(executor_config.clone())
                    .with_resolved_cluster_config(
                        ResolvedClusterConfig::try_new(executor_config.cluster.clone())
                            .expect("should resolve cluster config"),
                    )
                    .with_app(executor_app)
                    .build()
                    .await,
            );

            let cloned_executor_rt = Arc::clone(&executor_rt);
            let executor_server_thread = tokio::spawn(async move {
                Box::pin(cloned_executor_rt.start_servers(
                    executor_config,
                    None,
                    EndpointAuth::no_auth(),
                ))
                .await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&executor_rt).load_components() => {}
            }

            runtime_ready_check(&scheduler_rt).await;

            sleep_until(Instant::now() + Duration::from_secs(1)).await; // let executor connect

            // Datafusion query will run directly on the scheduler node without any clustering
            snapshot_names_from_runtime(
                "simple_cluster_mode_datafusion_execution_on_scheduler",
                &scheduler_rt,
                Some("names"),
                tempdir.path().to_string_lossy().to_string(),
            )
            .await;

            // Now run a distributed query
            let query = QueryBuilder::new(
                "EXPLAIN SELECT id, name, age, city, score FROM names ORDER BY id",
                scheduler_rt.datafusion(),
            );

            let query_handle = query
                .build()
                .submit_distributed("testing_explain")
                .await
                .expect("should submit distributed query");

            let query_result = query_handle.into_stream().await.expect("should get stream");
            let results = query_result
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("should collect results");

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches")
                .to_string();
            insta::assert_snapshot!(
                "explain_simple_cluster_mode_distributed_query",
                pretty.replace(
                    tempdir
                        .path()
                        .to_string_lossy()
                        .split_once('/')
                        .expect("should have leading /")
                        .1,
                    "<TEMP_PATH>"
                )
            );

            let query = QueryBuilder::new(
                "SELECT id, name, age, city, score FROM names ORDER BY id",
                scheduler_rt.datafusion(),
            );

            let query_handle = query
                .build()
                .submit_distributed("testing")
                .await
                .expect("should submit distributed query");

            let query_result = query_handle.into_stream().await.expect("should get stream");
            let results = query_result
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("should collect results");

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))
                .expect("Should format batches");
            insta::assert_snapshot!("simple_cluster_mode_distributed_query", pretty);

            executor_rt.shutdown().await;
            drop(executor_rt);
            executor_server_thread.abort();

            scheduler_rt.shutdown().await;
            drop(scheduler_rt);
            scheduler_server_thread.abort();

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            Ok(())
        })
        .await
}
