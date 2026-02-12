use app::AppBuilder;
use ballista_executor::executor::Executor;
use ballista_scheduler::state::executor_manager::ExecutorManager;
use runtime::Runtime;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::ClusterConfig;
use runtime::datafusion::query::QueryBuilder;
use runtime::{auth::EndpointAuth, config::Config};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use spicepod::component::dataset::Dataset;
use std::fmt::Write;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use test_framework::pki::init_pki;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

fn build_large_csv(rows: usize) -> String {
    let mut csv = String::from("id,name,age,city,score\n");
    for i in 1..=rows {
        let city = match i % 5 {
            0 => "New York",
            1 => "Los Angeles",
            2 => "Chicago",
            3 => "Houston",
            _ => "Phoenix",
        };
        writeln!(
            &mut csv,
            "{i},name-{i},{},{},{}",
            20 + (i % 50),
            city,
            50 + (i % 50)
        )
        .expect("writing csv row to String should not fail");
    }
    csv
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

async fn wait_for_executor_instance(
    runtime: &Arc<Runtime>,
    timeout: Duration,
) -> Result<Arc<Executor>, anyhow::Error> {
    let start = Instant::now();
    loop {
        if let Some(executor) = runtime
            .datafusion()
            .executor
            .read()
            .expect("executor lock")
            .clone()
        {
            return Ok(executor);
        }

        if start.elapsed() > timeout {
            return Err(anyhow::Error::msg(
                "Timed out waiting for executor instance to be bound",
            ));
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_active_tasks_at_least(
    executor: &Executor,
    minimum: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start = Instant::now();
    loop {
        let count = executor.active_task_count();
        if count >= minimum {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(anyhow::Error::msg(format!(
                "Timed out waiting for active task count >= {minimum}; found {count}"
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_active_tasks_eq(
    executor: &Executor,
    expected: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start = Instant::now();
    loop {
        let count = executor.active_task_count();
        if count == expected {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(anyhow::Error::msg(format!(
                "Timed out waiting for active task count == {expected}; found {count}"
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_tasks_via_control_stream() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let tempdir = tempfile::tempdir().expect("should create temp dir");
            let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());

            let pki = init_pki(tempdir.path()).expect("should create PKI");
            let scheduler_cert = pki
                .create_client_cert("scheduler")
                .expect("should create scheduler cert");
            let executor_cert = pki
                .create_client_cert("executor")
                .expect("should create executor cert");

            let csv_data = build_large_csv(200);
            std::fs::write(
                tempdir.path().join("./cancel_tasks_via_control_stream.csv"),
                csv_data,
            )
            .expect("write file");

            let scheduler_app = AppBuilder::new("cancel_tasks_via_control_stream")
                .with_dataset(Dataset::new(
                    format!(
                        "file:{}",
                        tempdir
                            .path()
                            .join("cancel_tasks_via_control_stream.csv")
                            .to_str()
                            .expect("should have str")
                    )
                    .as_str(),
                    "names",
                ))
                .build();

            let executor_app = AppBuilder::new("cancel_tasks_via_control_stream_executor").build();

            configure_test_datafusion();

            let scheduler_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8390,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    52151,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Scheduler),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        52152,
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
                    return Err(anyhow::Error::msg("Timed out waiting for scheduler components"));
                }
                () = Arc::clone(&scheduler_rt).load_components() => {}
            }

            let executor_config = Config {
                http_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    8391,
                )),
                flight_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    52153,
                )),
                cluster: ClusterConfig {
                    role: Some(runtime::config::ClusterRole::Executor),
                    node_bind_address: std::net::SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        52154,
                    )),
                    scheduler_address: Some("127.0.0.1:52152".to_string()),
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
                    return Err(anyhow::Error::msg("Timed out waiting for executor components"));
                }
                () = Arc::clone(&executor_rt).load_components() => {}
            }

            runtime_ready_check(&scheduler_rt).await;
            runtime_ready_check(&executor_rt).await;

            let scheduler_server = scheduler_rt
                .datafusion()
                .scheduler_server
                .read()
                .expect("scheduler server lock")
                .clone()
                .expect("scheduler server should be available");
            let executor_manager = scheduler_server.state.executor_manager.clone();

            let executor =
                wait_for_executor_instance(&executor_rt, Duration::from_secs(10)).await?;

            sleep_until(Instant::now() + Duration::from_secs(1)).await; // let executor connect
            wait_for_executor_count(&executor_manager, 1, Duration::from_secs(10)).await?;

            let query = QueryBuilder::new(
                "SELECT COUNT(*) FROM names a, names b, names c, names d, names e, names f",
                scheduler_rt.datafusion(),
            );

            let query_handle = query
                .build()
                .submit_distributed("testing_cancel_via_control_stream")
                .await
                .expect("should submit distributed query");

            wait_for_active_tasks_at_least(executor.as_ref(), 1, Duration::from_secs(20)).await?;

            query_handle
                .cancel()
                .await
                .expect("cancellation request should succeed");

            wait_for_active_tasks_eq(executor.as_ref(), 0, Duration::from_secs(20)).await?;

            let completion = timeout(Duration::from_secs(20), query_handle.wait_for_completion())
                .await
                .map_err(|_| anyhow::Error::msg("Timed out waiting for query cancellation"))?;
            assert!(
                completion.is_err(),
                "Cancelled query should not complete successfully"
            );

            executor_rt.shutdown().await;
            drop(executor_rt);
            executor_server_thread.abort();

            wait_for_executor_count(&executor_manager, 0, Duration::from_secs(5)).await?;

            scheduler_rt.shutdown().await;
            drop(scheduler_rt);
            scheduler_server_thread.abort();

            Ok(())
        })
        .await
}
