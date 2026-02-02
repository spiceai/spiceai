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

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, path::Path};
use runtime::Runtime;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::ClusterConfig;
use runtime::jobs::{JobExecutor, JobStore};
use runtime::{auth::EndpointAuth, config::Config};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use spicepod::component::dataset::Dataset;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use test_framework::object_store::MemoryObjectStore;
use test_framework::pki::init_pki;
use tokio::time::{Instant, sleep_until};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

const NAMES_CSV: &str = include_str!("../acceleration/data/names.csv");

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_job_store() -> Result<(), anyhow::Error> {
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

            sleep_until(Instant::now() + Duration::from_secs(5)).await; // let executor connect

            let memory_object_store = Arc::new(MemoryObjectStore::new()) as Arc<dyn ObjectStore>;

            let job_store = Arc::new(JobStore::new(
                Arc::clone(&memory_object_store),
                "testing/",
                "scheduler1",
            ));

            let job_executor = JobExecutor::new(
                Arc::clone(&job_store),
                Arc::clone(&scheduler_rt).datafusion(),
            );

            let result = job_executor
                .submit(
                    "SELECT id, name, age, city, score FROM names ORDER BY id".to_string(),
                    None,
                )
                .await
                .expect("should submit job");

            for _ in 0..30 {
                let job_status = job_executor
                    .get_status(&result.job_id)
                    .await
                    .expect("should get job status");
                println!("job state: {job_status:?}");
                if job_status.is_terminal() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }

            let job_status = job_executor
                .get_status(&result.job_id)
                .await
                .expect("should get job status");
            assert!(
                job_status.succeeded(),
                "job did not succeed: {job_status:?}"
            );

            let job_results = job_executor
                .get_chunk(&result.job_id, 0)
                .await
                .expect("should get job results");
            let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&job_results)
                .expect("should format results")
                .to_string();

            insta::assert_snapshot!("test_simple_job_store", pretty);

            // validate the result is stored in the memory object store
            let expected_chunk_path = format!(
                "testing/jobs/{job_id}/chunk_0.arrow",
                job_id = result.job_id
            );

            let locations = memory_object_store
                .list(None)
                .map_ok(|m| m.location)
                .boxed()
                .try_collect::<Vec<_>>()
                .await
                .expect("should list locations");

            assert!(
                locations.contains(&Path::from(expected_chunk_path.clone())),
                "stored locations did not contain expected chunk path. found: {locations:?}",
            );

            let stored_data = memory_object_store
                .get(&Path::from(expected_chunk_path.clone()))
                .await
                .expect("should get stored chunk");

            let bytes = stored_data
                .bytes()
                .await
                .expect("should get bytes from stored chunk");
            let cursor = std::io::Cursor::new(bytes.as_ref());
            let reader = StreamReader::try_new(cursor, None).expect("should create stream reader");

            // Collect all batches, propagating any errors that occur during deserialization
            let batches: Vec<RecordBatch> = reader
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("should collect record batches");

            let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                .expect("should format results")
                .to_string();

            insta::assert_snapshot!("test_simple_job_store_stored_chunk", pretty);

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
