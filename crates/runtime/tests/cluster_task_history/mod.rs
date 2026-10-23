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

//! `runtime.task_history` in a cluster with two schedulers that share cluster state.
//!
//! A scheduler's `runtime.task_history` combines its own rows with the rows its peer
//! schedulers return from their `runtime.local_task_history`, so each peer must read its
//! local table on the scheduler itself, whether or not it has executors. It also includes
//! the rows of every executor in the cluster, whichever scheduler the executor started
//! against.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use app::AppBuilder;
use arrow::array::{
    ArrayBuilder, ArrayRef, AsArray as _, Float64Array, MapBuilder, RecordBatch, StringArray,
    TimestampNanosecondArray, make_builder,
};
use arrow::datatypes::DataType;
use datafusion::common::TableReference;
use futures::TryStreamExt;
use runtime::auth::EndpointAuth;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::{ClusterConfig, ClusterRole, Config};
use runtime::dataupdate::{DataUpdate, UpdateType};
use runtime::{Result as RuntimeResult, Runtime};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use spicepod::component::runtime::{Runtime as SpicepodRuntime, RuntimeState};
use test_framework::pki::{PkiConfig, init_pki};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{free_ports, runtime_ready_check, test_request_context},
};

/// A scheduler or executor running in this process.
struct Node {
    rt: Arc<Runtime>,
    handle: JoinHandle<RuntimeResult<()>>,
    /// `host:port` of the node's cluster endpoint, which is also its task history `node_id`.
    cluster_addr: String,
}

async fn start_node(
    name: &str,
    pki: &PkiConfig,
    role: ClusterRole,
    scheduler_address: Option<&str>,
    runtime: SpicepodRuntime,
) -> Result<Node, anyhow::Error> {
    let cert = pki.create_client_cert(name).map_err(anyhow::Error::msg)?;
    let [cluster_port, http_port, flight_port] = free_ports();
    let cluster_addr = format!("127.0.0.1:{cluster_port}");
    let config = Config {
        http_bind_address: format!("127.0.0.1:{http_port}").parse()?,
        flight_bind_address: format!("127.0.0.1:{flight_port}").parse()?,
        cluster: ClusterConfig {
            role: Some(role),
            node_bind_address: cluster_addr.parse()?,
            node_advertise_address: Some("127.0.0.1".to_string()),
            scheduler_address: scheduler_address.map(str::to_string),
            node_mtls_ca_certificate_file: Some(pki.ca_cert_path.to_string_lossy().to_string()),
            node_mtls_certificate_file: Some(cert.cert_path.to_string_lossy().to_string()),
            node_mtls_key_file: Some(cert.key_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let rt = Arc::new(
        Runtime::builder()
            .with_runtime_config(config.clone())
            .with_resolved_cluster_config(
                ResolvedClusterConfig::try_new(config.cluster.clone())
                    .map_err(|e| anyhow::Error::msg(format!("cluster config: {e}")))?
                    .with_instance_id(name),
            )
            .with_app(AppBuilder::new(name).with_runtime(runtime).build())
            .build()
            .await,
    );
    let cloned = Arc::clone(&rt);
    let handle = tokio::spawn(async move {
        Box::pin(cloned.start_servers(config, None, EndpointAuth::no_auth())).await
    });
    Ok(Node {
        rt,
        handle,
        cluster_addr,
    })
}

async fn start_scheduler(
    name: &str,
    pki: &PkiConfig,
    state_location: &str,
) -> Result<Node, anyhow::Error> {
    let runtime = SpicepodRuntime {
        state: Some(RuntimeState {
            location: state_location.to_string(),
            params: None,
        }),
        ..SpicepodRuntime::default()
    };
    let scheduler = start_node(name, pki, ClusterRole::Scheduler, None, runtime).await?;
    Arc::clone(&scheduler.rt).load_components().await;
    runtime_ready_check(&scheduler.rt).await;
    Ok(scheduler)
}

/// Starts an executor attached to `scheduler` and waits until it is ready. Executors bind
/// the scheduler's app themselves, so `load_components` is not called here.
async fn start_executor(
    name: &str,
    pki: &PkiConfig,
    scheduler: &Node,
) -> Result<Node, anyhow::Error> {
    let start = Instant::now();
    while tokio::net::TcpStream::connect(&scheduler.cluster_addr)
        .await
        .is_err()
    {
        if start.elapsed() > Duration::from_secs(30) {
            anyhow::bail!(
                "scheduler cluster port {} is not open",
                scheduler.cluster_addr
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
    let executor = start_node(
        name,
        pki,
        ClusterRole::Executor,
        Some(&scheduler.cluster_addr),
        SpicepodRuntime::default(),
    )
    .await?;
    let start = Instant::now();
    while !executor.rt.status().is_ready() {
        if executor.handle.is_finished() || start.elapsed() > Duration::from_mins(2) {
            anyhow::bail!("executor {name} did not become ready");
        }
        sleep(Duration::from_millis(100)).await;
    }
    Ok(executor)
}

/// Writes one row with `trace_id` to `node`'s `runtime.<table>`: `local_task_history` on a
/// scheduler, `task_history` on an executor.
async fn seed_task_history(node: &Node, table: &str, trace_id: &str) -> Result<(), anyhow::Error> {
    let table = TableReference::partial("runtime", table);
    let df = node.rt.datafusion();
    let schema = df
        .get_table(&table)
        .await
        .ok_or_else(|| anyhow::Error::msg(format!("{table} is not registered")))?
        .schema();
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef, anyhow::Error> {
            Ok(match (field.name().as_str(), field.data_type()) {
                ("trace_id", _) => Arc::new(StringArray::from(vec![trace_id])),
                ("node_id", _) => Arc::new(StringArray::from(vec![node.cluster_addr.as_str()])),
                (_, DataType::Utf8) => Arc::new(StringArray::from(vec!["seed"])),
                (_, DataType::Timestamp(_, tz)) => Arc::new(
                    TimestampNanosecondArray::from(vec![now]).with_timezone_opt(tz.clone()),
                ),
                (_, DataType::Float64) => Arc::new(Float64Array::from(vec![0.0])),
                (_, map_type @ DataType::Map(..)) => {
                    let mut labels = make_builder(map_type, 1);
                    labels
                        .as_any_mut()
                        .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                        .ok_or_else(|| anyhow::Error::msg("labels is not a map"))?
                        .append(true)?;
                    labels.finish()
                }
                (name, data_type) => {
                    anyhow::bail!("unexpected task history column {name}: {data_type}")
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    df.write_data(
        &table,
        DataUpdate {
            schema,
            data: vec![batch],
            update_type: UpdateType::Append,
        },
    )
    .await
    .map_err(|e| anyhow::Error::msg(format!("seed task history: {e}")))
}

/// The seeded trace ids that `scheduler` returns from `runtime.task_history`, sorted.
async fn seeded_trace_ids(scheduler: &Node) -> Result<Vec<String>, String> {
    let query = async {
        scheduler
            .rt
            .datafusion()
            .query_builder("SELECT trace_id FROM runtime.task_history")
            .build()
            .run()
            .await
            .map_err(|e| e.to_string())?
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| e.to_string())
    };
    let batches = tokio::time::timeout(Duration::from_secs(30), query)
        .await
        .map_err(|_| "query timed out".to_string())??;
    let mut ids: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .flatten()
                .filter(|id| id.starts_with("seed-"))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    Ok(ids)
}

/// Scheduler 1 has an executor and scheduler 2 has none. Each scheduler's
/// `runtime.task_history` must include the row seeded on the other.
async fn run_task_history_across_schedulers() -> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    configure_test_datafusion();

    let pki_dir = tempfile::tempdir()?;
    let state_dir = tempfile::tempdir()?;
    let pki = init_pki(pki_dir.path()).map_err(anyhow::Error::msg)?;
    let state_location = format!("file://{}", state_dir.path().display());

    let s1 = start_scheduler("scheduler1", &pki, &state_location).await?;
    let s2 = start_scheduler("scheduler2", &pki, &state_location).await?;
    let executor = start_executor("executor", &pki, &s1).await?;

    // Scheduler 1 sends executor-bound scans once it holds the executor's FlightSQL client.
    let registry = s1
        .rt
        .datafusion()
        .executor_registry()
        .cloned()
        .ok_or_else(|| anyhow::Error::msg("scheduler1 has no executor registry"))?;
    let start = Instant::now();
    while registry.flight_sql_clients_count().await == 0 {
        if start.elapsed() > Duration::from_mins(2) {
            anyhow::bail!("scheduler1 has no FlightSQL client for the executor");
        }
        sleep(Duration::from_millis(100)).await;
    }

    seed_task_history(&s1, "local_task_history", "seed-scheduler1").await?;
    seed_task_history(&s2, "local_task_history", "seed-scheduler2").await?;

    // Schedulers find each other through heartbeats in the shared state.
    let expected = ["seed-scheduler1", "seed-scheduler2"];
    let start = Instant::now();
    loop {
        let from_s1 = seeded_trace_ids(&s1).await;
        let from_s2 = seeded_trace_ids(&s2).await;
        if [&from_s1, &from_s2]
            .iter()
            .all(|result| matches!(result, Ok(ids) if *ids == expected))
        {
            break;
        }
        if start.elapsed() > Duration::from_mins(2) {
            anyhow::bail!(
                "each scheduler should return both seeded rows; scheduler1: {from_s1:?}, scheduler2: {from_s2:?}"
            );
        }
        sleep(Duration::from_millis(500)).await;
    }

    executor.handle.abort();
    for scheduler in [s1, s2] {
        scheduler.rt.shutdown().await;
        scheduler.handle.abort();
    }
    Ok(())
}

/// The executor starts against scheduler 1. Scheduler 2's `runtime.task_history` must
/// include the row seeded on the executor.
async fn run_executor_task_history_on_peer_scheduler() -> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    configure_test_datafusion();

    let pki_dir = tempfile::tempdir()?;
    let state_dir = tempfile::tempdir()?;
    let pki = init_pki(pki_dir.path()).map_err(anyhow::Error::msg)?;
    let state_location = format!("file://{}", state_dir.path().display());

    let s1 = start_scheduler("scheduler1", &pki, &state_location).await?;
    let s2 = start_scheduler("scheduler2", &pki, &state_location).await?;
    let executor = start_executor("executor", &pki, &s1).await?;
    seed_task_history(&executor, "task_history", "seed-executor").await?;

    // The executor finds scheduler 2 through scheduler 1's membership.
    let expected = ["seed-executor"];
    let start = Instant::now();
    loop {
        let from_s2 = seeded_trace_ids(&s2).await;
        if matches!(&from_s2, Ok(ids) if *ids == expected) {
            break;
        }
        if start.elapsed() > Duration::from_mins(2) {
            anyhow::bail!(
                "scheduler2 should return the row seeded on the executor of scheduler1; got {from_s2:?}"
            );
        }
        sleep(Duration::from_millis(500)).await;
    }

    executor.handle.abort();
    for scheduler in [s1, s2] {
        scheduler.rt.shutdown().await;
        scheduler.handle.abort();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn task_history_on_each_scheduler_includes_its_peer() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async { run_task_history_across_schedulers().await })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn task_history_on_a_peer_scheduler_includes_the_executor() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async { run_executor_task_history_on_peer_scheduler().await })
        .await
}
