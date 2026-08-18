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

//! Scheduler failover integration tests.
//!
//! Validates that when the scheduler driving a distributed async query becomes
//! unavailable, another scheduler sharing the same object-store job state
//! recovers the orphaned job and drives it **to completion** with correct
//! results.
//!
//! ## Reliable hold (no sleep-based timing)
//!
//! The in-flight query is held deterministically by *withholding an executor*
//! from the submitting scheduler. The single executor attaches to the recovery
//! scheduler (`s2`), so a job submitted to `s1` is registered as `Running` but
//! cannot make progress — `s1` has no executor to dispatch tasks to. Recovery
//! then lets `s2` (which owns the executor) drive the job to completion. This
//! avoids any "is the query slow enough to still be running when we kill it"
//! race: the job is provably stalled until recovery.
//!
//! ## SIGKILL vs SIGTERM
//!
//! In-process a scheduler can only be stopped *gracefully* (`Runtime::shutdown`):
//! every stop path the runtime exposes deregisters the scheduler (removes its
//! cluster entry and deletes its heartbeat), which is the SIGTERM behavior.
//! `execute_job` does not mark the job terminal on cancellation, so the job is
//! left `Running` in the shared store for another scheduler to recover.
//!
//! A true SIGKILL — the heartbeat going stale *without* deregistration, so
//! recovery happens via heartbeat-TTL expiry rather than immediate
//! deregistration — requires an abrupt process kill, which would need a
//! subprocess harness and is not covered here. Both tests below therefore
//! drive recovery via graceful shutdown and assert the same recovery +
//! completion contract; [`recovers_job_after_scheduler_loss_sigkill`] documents
//! the TTL-expiry path it does not cover in-process.

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::AsArray as _;
use arrow::datatypes::Int64Type;
use ballista_scheduler::state::executor_manager::ExecutorManager;
use runtime::auth::EndpointAuth;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::{ClusterConfig, ClusterRole, Config};
use runtime::http_types::SubmitQueryRequest;
use runtime::jobs::{JobExecutor, JobState, JobStatus};
use runtime::{Result as RuntimeResult, Runtime};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime as SpicepodRuntime, Scheduler as SchedulerConfig};
use test_framework::pki::{PkiConfig, init_pki};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

/// Number of rows in the `names` dataset; the query under test is
/// `SELECT COUNT(*) FROM names`, so this is also the expected result.
const NAMES_ROWS: usize = 64;

fn alloc_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn build_names_csv() -> String {
    use std::fmt::Write as _;
    let mut csv = String::from("id,name\n");
    for i in 1..=NAMES_ROWS {
        writeln!(csv, "{i},name-{i}").expect("write csv row to String");
    }
    csv
}

/// A scheduler node wired to a shared object-store job-state `state_location`.
struct SchedulerNode {
    rt: Arc<Runtime>,
    handle: JoinHandle<RuntimeResult<()>>,
    /// `host:port` other nodes use to reach this scheduler's cluster endpoint.
    cluster_addr: String,
}

async fn build_scheduler(
    name: &str,
    pki: &PkiConfig,
    state_location: &str,
    csv_path: &str,
) -> Result<SchedulerNode, anyhow::Error> {
    let cert = pki.create_client_cert(name).map_err(anyhow::Error::msg)?;
    let (http, flight, cluster) = (alloc_port(), alloc_port(), alloc_port());
    let cluster_addr = format!("127.0.0.1:{cluster}");

    let scheduler_cfg = SchedulerConfig {
        state_location: state_location.to_string(),
        params: None,
        partition_assignment_interval: "1s".to_string(),
        max_partition_assignments_per_interval:
            spicepod::component::runtime::default_max_partition_assignments_per_interval(),
        max_partitions_per_executor: 10,
        partition_discovery_timeout:
            spicepod::component::runtime::default_partition_discovery_timeout(),
    };

    let app = AppBuilder::new(format!("failover_{name}"))
        .with_dataset(Dataset::new(format!("file:{csv_path}"), "names"))
        .with_runtime(SpicepodRuntime {
            scheduler: Some(scheduler_cfg),
            ..SpicepodRuntime::default()
        })
        .build();

    let config = Config {
        http_bind_address: format!("127.0.0.1:{http}").parse().expect("http addr"),
        flight_bind_address: format!("127.0.0.1:{flight}").parse().expect("flight addr"),
        cluster: ClusterConfig {
            role: Some(ClusterRole::Scheduler),
            node_bind_address: cluster_addr.parse().expect("cluster addr"),
            node_advertise_address: Some("127.0.0.1".to_string()),
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
                    .map_err(|e| anyhow::Error::msg(format!("cluster config: {e}")))?,
            )
            .with_app(app)
            .build()
            .await,
    );

    let cloned = Arc::clone(&rt);
    let handle = tokio::spawn(async move {
        Box::pin(cloned.start_servers(config, None, EndpointAuth::no_auth())).await
    });
    Arc::clone(&rt).load_components().await;
    runtime_ready_check(&rt).await;

    Ok(SchedulerNode {
        rt,
        handle,
        cluster_addr,
    })
}

async fn build_executor(
    name: &str,
    pki: &PkiConfig,
    scheduler_cluster_addr: &str,
    csv_path: &str,
) -> Result<(Arc<Runtime>, JoinHandle<RuntimeResult<()>>), anyhow::Error> {
    let cert = pki.create_client_cert(name).map_err(anyhow::Error::msg)?;
    let (http, flight, cluster) = (alloc_port(), alloc_port(), alloc_port());

    let app = AppBuilder::new(format!("failover_{name}"))
        .with_dataset(Dataset::new(format!("file:{csv_path}"), "names"))
        .build();

    let config = Config {
        http_bind_address: format!("127.0.0.1:{http}").parse().expect("http addr"),
        flight_bind_address: format!("127.0.0.1:{flight}").parse().expect("flight addr"),
        cluster: ClusterConfig {
            role: Some(ClusterRole::Executor),
            node_bind_address: format!("127.0.0.1:{cluster}")
                .parse()
                .expect("cluster addr"),
            scheduler_address: Some(scheduler_cluster_addr.to_string()),
            node_advertise_address: Some("127.0.0.1".to_string()),
            node_mtls_ca_certificate_file: Some(pki.ca_cert_path.to_string_lossy().to_string()),
            node_mtls_certificate_file: Some(cert.cert_path.to_string_lossy().to_string()),
            node_mtls_key_file: Some(cert.key_path.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    // Wait until the scheduler cluster port accepts connections before starting
    // the executor, so `initialize_cluster_executor`'s connect cannot hang on a
    // not-yet-bound listener (scheduler `runtime_ready_check` only covers
    // dataset readiness).
    let start = Instant::now();
    loop {
        if tokio::net::TcpStream::connect(scheduler_cluster_addr)
            .await
            .is_ok()
        {
            break;
        }
        if start.elapsed() > Duration::from_secs(30) {
            return Err(anyhow::Error::msg(format!(
                "timed out waiting for scheduler cluster port {scheduler_cluster_addr}"
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }

    let rt = Arc::new(
        Runtime::builder()
            .with_runtime_config(config.clone())
            .with_resolved_cluster_config(
                ResolvedClusterConfig::try_new(config.cluster.clone())
                    .map_err(|e| anyhow::Error::msg(format!("cluster config: {e}")))?,
            )
            .with_app(app)
            .build()
            .await,
    );

    let cloned = Arc::clone(&rt);
    let mut handle = tokio::spawn(async move {
        Box::pin(cloned.start_servers(config, None, EndpointAuth::no_auth())).await
    });

    // Do not call `load_components` on executors — Fix B gates Ready/slots on
    // `executor_bind_app` + object-store bind; a concurrent load races that path.
    tokio::select! {
        () = sleep(Duration::from_mins(2)) => {
            handle.abort();
            let _ = handle.await;
            return Err(anyhow::Error::msg(
                "timed out waiting for executor to become ready (object stores bound / task slots open)",
            ));
        }
        result = &mut handle => {
            return Err(anyhow::Error::msg(match result {
                Ok(Ok(())) => "executor server thread finished unexpectedly".to_string(),
                Ok(Err(e)) => format!("executor server failed to start: {e}"),
                Err(e) => format!("executor server thread panicked: {e}"),
            }));
        }
        () = async {
            while !rt.status().is_ready() {
                sleep(Duration::from_millis(100)).await;
            }
        } => {}
    }

    Ok((rt, handle))
}

/// Poll until the scheduler has bound its async-jobs [`JobExecutor`].
async fn wait_for_job_executor(
    rt: &Arc<Runtime>,
    timeout: Duration,
) -> Result<Arc<JobExecutor>, anyhow::Error> {
    let start = Instant::now();
    loop {
        if let Some(je) = rt.job_executor() {
            return Ok(je);
        }
        if start.elapsed() > timeout {
            return Err(anyhow::Error::msg("timed out waiting for job executor"));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn executor_manager(rt: &Arc<Runtime>) -> ExecutorManager {
    rt.datafusion()
        .scheduler_server
        .read()
        .expect("scheduler server lock")
        .clone()
        .expect("scheduler server should be available")
        .state
        .executor_manager
        .clone()
}

async fn wait_for_executor_count(
    manager: &ExecutorManager,
    expected: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start = Instant::now();
    loop {
        let count = manager
            .get_executors_state()
            .await
            .map_err(|e| anyhow::Error::msg(e.to_string()))?
            .len();
        if count == expected {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(anyhow::Error::msg(format!(
                "timed out waiting for {expected} executors on scheduler; found {count}"
            )));
        }
        sleep(Duration::from_millis(150)).await;
    }
}

/// Poll a job's status via the given executor until `pred` is satisfied.
async fn wait_for_job<F>(
    je: &JobExecutor,
    job_id: &str,
    timeout: Duration,
    label: &str,
    pred: F,
) -> Result<JobState, anyhow::Error>
where
    F: Fn(&JobState) -> bool,
{
    let start = Instant::now();
    loop {
        let state = je
            .get_status(job_id, runtime::jobs::PUBLIC_JOB_OWNER)
            .await
            .map_err(|e| anyhow::Error::msg(format!("get_status: {e}")))?;
        if pred(&state) {
            return Ok(state);
        }
        if start.elapsed() > timeout {
            return Err(anyhow::Error::msg(format!(
                "timed out waiting for job {job_id} to reach `{label}` within {timeout:?}; last status = {:?}",
                state.status
            )));
        }
        sleep(Duration::from_millis(150)).await;
    }
}

/// Drives the full failover scenario and asserts the recovered job completes on
/// the second scheduler with the correct row count.
///
/// `recovery_timeout` differs by variant because graceful deregistration (the
/// only in-process stop) makes recovery fast; the larger SIGKILL budget leaves
/// room for the (documented, not-exercised-here) TTL-expiry path.
async fn run_failover(recovery_timeout: Duration) -> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    configure_test_datafusion();

    let tempdir = tempfile::tempdir().expect("tempdir");
    let state_tempdir = tempfile::tempdir().expect("state tempdir");
    let pki = init_pki(tempdir.path()).map_err(anyhow::Error::msg)?;

    let csv_path = tempdir.path().join("names.csv");
    std::fs::write(&csv_path, build_names_csv()).expect("write csv");
    let csv_path = csv_path.to_str().expect("csv path str").to_string();

    // Both schedulers share one object-store job state (local filesystem, so the
    // test is hermetic — no S3/AWS creds needed).
    let state_location = format!("file://{}", state_tempdir.path().display());

    let s1 = build_scheduler("scheduler1", &pki, &state_location, &csv_path).await?;
    let s2 = build_scheduler("scheduler2", &pki, &state_location, &csv_path).await?;

    // The single executor attaches to s2 only, so a job submitted to s1 stalls
    // (s1 has no executor to dispatch to) — this is the deterministic hold.
    let (_executor_rt, executor_handle) =
        build_executor("executor", &pki, &s2.cluster_addr, &csv_path).await?;
    wait_for_executor_count(&executor_manager(&s2.rt), 1, Duration::from_mins(1)).await?;

    let s1_je = wait_for_job_executor(&s1.rt, Duration::from_mins(1)).await?;
    let s2_je = wait_for_job_executor(&s2.rt, Duration::from_mins(1)).await?;

    // Submit the async distributed query to s1. It registers as Running but
    // cannot complete — s1 owns no executor.
    let submitted = s1_je
        .submit(
            SubmitQueryRequest {
                sql: "SELECT COUNT(*) AS c FROM names".to_string(),
                parameters: None,
                timeout_seconds: None,
                maximum_size: None,
            },
            true,
            runtime::jobs::PUBLIC_JOB_OWNER.to_string(),
        )
        .await
        .map_err(|e| anyhow::Error::msg(format!("submit: {e}")))?;
    let job_id = submitted.job_id.clone();

    // Wait until the job is Running on s1. It cannot progress further on its own:
    // s1 has no executor to dispatch tasks to (the only executor is attached to
    // s2), so the job is held in flight until recovery.
    wait_for_job(
        &s1_je,
        &job_id,
        Duration::from_mins(1),
        "Running on s1",
        |s| s.status == JobStatus::Running,
    )
    .await?;

    // Stop s1. In-process this is a graceful deregister (SIGTERM semantics); the
    // job is left Running in the shared store for recovery (execute_job does not
    // mark it terminal on cancellation).
    s1.rt.shutdown().await;
    s1.handle.abort();

    // s2's recovery loop observes the job as orphaned (its owner is no longer a
    // live peer), resumes it, and — owning the only executor — drives it to
    // completion. Reaching Succeeded is itself the proof of recovery: s1 was shut
    // down with no executor of its own, so it cannot have completed the job —
    // only s2 can have.
    //
    // We deliberately do not assert on the job's `scheduler_node` changing:
    // recovery via `resume` re-drives the job without re-marking it Running, so
    // that field can retain the original scheduler's id even though s2 drove the
    // job to completion.
    wait_for_job(&s2_je, &job_id, recovery_timeout, "Succeeded on s2", |s| {
        s.status == JobStatus::Succeeded
    })
    .await?;

    // Results are correct: COUNT(*) == NAMES_ROWS.
    let chunks = s2_je
        .get_chunk(&job_id, 0, runtime::jobs::PUBLIC_JOB_OWNER)
        .await
        .map_err(|e| anyhow::Error::msg(format!("get_chunk: {e}")))?;
    let count: i64 = chunks
        .iter()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
        .next()
        .expect("one result row");
    assert_eq!(
        count,
        i64::try_from(NAMES_ROWS).expect("row count fits i64"),
        "recovered query should return the correct COUNT(*)"
    );

    // Cleanup.
    executor_handle.abort();
    s2.rt.shutdown().await;
    s2.handle.abort();
    Ok(())
}

/// Graceful loss (SIGTERM): the scheduler deregisters on shutdown, so recovery
/// is fast (no heartbeat-TTL wait).
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn recovers_job_after_scheduler_loss_sigterm() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async { run_failover(Duration::from_mins(1)).await })
        .await
}

/// Abrupt loss (SIGKILL). NOTE: in-process the runtime can only be stopped
/// gracefully, so this drives recovery via the same deregister path as the
/// SIGTERM case and asserts the same recovery + completion contract. A true
/// SIGKILL — heartbeat going stale without deregistration, recovery via TTL
/// expiry — would require a subprocess harness and is not covered here. The
/// larger timeout reflects the TTL-expiry budget that path would need.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn recovers_job_after_scheduler_loss_sigkill() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async { run_failover(Duration::from_secs(90)).await })
        .await
}
