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

//! Test harness for Spice cluster (scheduler + executor) integration tests.
//!
//! # Usage
//!
//! ```rust
//! let harness = ClusterHarness::builder()
//!     .scheduler(scheduler_app)
//!     .executors(2)
//!     .start()
//!     .await?;
//!
//! harness.wait_for_executors(Duration::from_secs(15)).await?;
//! sleep(Duration::from_secs(5)).await; // wait for acceleration
//!
//! let results = harness.query("SELECT * FROM my_table ORDER BY id").await?;
//! insta::assert_snapshot!(pretty_format(&results));
//!
//! let plan = harness.explain("SELECT * FROM my_table ORDER BY id").await?;
//! insta::assert_snapshot!(pretty_format(&plan));
//!
//! harness.shutdown().await;
//! ```
//!
//! Each `ClusterHarness` automatically:
//! - Initialises a per-harness PKI (CA + per-node mTLS certs)
//! - Allocates unique, non-overlapping ports for every node
//! - Starts each node's servers and waits until they are ready

use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use ballista_scheduler::state::executor_manager::ExecutorManager;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::cluster::ExecutorRegistry;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::{ClusterConfig, ClusterRole, Config};
use runtime::datafusion::query::QueryBuilder;
use runtime::{Result as RuntimeResult, auth::EndpointAuth};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::sync::Arc;
use std::time::Duration;
use test_framework::pki::init_pki;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

use crate::utils::runtime_ready_check;

// ---------------------------------------------------------------------------
// Port allocation
// ---------------------------------------------------------------------------

/// Allocate `n` free TCP ports on localhost.
///
/// Binds to port 0, reads back the OS-assigned port, then drops the listener.
/// There is an inherent time-of-check to time-of-us race, but in practice this is safe for local
/// integration tests where no other process is racing for the same ports.
fn allocate_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("failed to bind to port 0"))
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().expect("no local addr").port())
        .collect();
    drop(listeners);
    ports
}

// ---------------------------------------------------------------------------
// Node ports
// ---------------------------------------------------------------------------

/// The three ports each cluster node needs.
#[derive(Debug, Clone, Copy)]
struct NodePorts {
    http: u16,
    flight: u16,
    cluster: u16,
}

impl NodePorts {
    fn allocate() -> Self {
        let ports = allocate_ports(3);
        Self {
            http: ports[0],
            flight: ports[1],
            cluster: ports[2],
        }
    }

    fn http_addr(self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.http))
    }

    fn flight_addr(self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.flight))
    }

    fn cluster_addr(self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.cluster))
    }

    fn cluster_advertise(self) -> String {
        format!("127.0.0.1:{}", self.cluster)
    }
}

// ---------------------------------------------------------------------------
// ClusterHarness
// ---------------------------------------------------------------------------

/// A running Spice cluster (scheduler + N executors) for integration tests.
///
/// Drop aborts background server handles. Call [`shutdown`](ClusterHarness::shutdown)
/// for an orderly teardown that waits for runtimes to stop.
pub struct ClusterHarness {
    /// The scheduler runtime.
    pub scheduler: Arc<Runtime>,
    /// All executor runtimes, in the order they were added.
    pub executors: Vec<Arc<Runtime>>,
    /// Background server handles — aborted on drop.
    handles: Vec<JoinHandle<RuntimeResult<()>>>,
    executor_manager: ExecutorManager,
    /// Executor registry for checking flight SQL client readiness.
    executor_registry: Option<Arc<ExecutorRegistry>>,
    /// PKI infrastructure for creating new executor certs (late-join support).
    pki: test_framework::pki::PkiConfig,
    /// Scheduler's cluster advertise address.
    scheduler_cluster_addr: String,
    /// Keep the temp directory alive for the harness lifetime.
    _tempdir: Arc<tempfile::TempDir>,
    /// Counter for naming dynamically added executors.
    next_executor_index: usize,
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

impl ClusterHarness {
    /// Start building a harness.
    pub fn builder() -> ClusterHarnessBuilder {
        ClusterHarnessBuilder::new()
    }

    /// Block until exactly `n` executors have registered with the scheduler,
    /// or return an error after `timeout`.
    pub async fn wait_for_executors(&self, timeout: Duration) -> Result<(), anyhow::Error> {
        self.wait_until_executor_count(self.executors.len(), timeout)
            .await
    }

    /// Block until exactly `expected` executors are registered with the scheduler,
    /// or return an error after `timeout`.
    ///
    /// Unlike [`wait_for_executors`], this accepts an arbitrary target count — useful
    /// for waiting after an executor has been shut down.
    pub async fn wait_until_executor_count(
        &self,
        expected: usize,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();
        loop {
            sleep(Duration::from_millis(200)).await;
            let count = self
                .executor_manager
                .get_executor_state()
                .await
                .map_err(|e| anyhow::Error::msg(e.to_string()))?
                .len();

            // Also verify flight SQL clients are ready — an executor may be in
            // `connections` but not yet in `flight_sql_clients` during initial
            // connection setup, which causes INSERT forwarding to fail.
            let flight_clients_ready = if let Some(ref registry) = self.executor_registry {
                registry.flight_sql_clients_count().await >= expected
            } else {
                true
            };

            if count == expected && flight_clients_ready {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(anyhow::Error::msg(format!(
                    "Timed out waiting for {expected} executors; found {count}"
                )));
            }
        }
    }

    /// Run a SQL query through the scheduler and collect all result batches.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        QueryBuilder::new(sql, self.scheduler.datafusion())
            .build()
            .run()
            .await
            .map_err(|e| anyhow::Error::msg(format!("query failed: {e}")))?
            .data
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| anyhow::Error::msg(format!("query stream failed: {e}")))
    }

    /// Run `EXPLAIN <sql>` through the scheduler and collect all result batches.
    pub async fn explain(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        self.query(&format!("EXPLAIN {sql}")).await
    }

    /// Start and register a new executor after the cluster is already running.
    ///
    /// This is used to test late-join scenarios (e.g. DDL replay on join).
    /// The executor is added to `self.executors` and its server handle to `self.handles`.
    pub async fn add_executor(
        &mut self,
        app: Option<App>,
    ) -> Result<(), anyhow::Error> {
        let i = self.next_executor_index;
        self.next_executor_index += 1;
        let label = format!("executor{i}");

        let executor_app =
            app.unwrap_or_else(|| AppBuilder::new(format!("test_{label}")).build());

        let (executor_rt, executor_handle) = start_executor(
            &label,
            executor_app,
            &self.pki,
            &self.scheduler_cluster_addr,
        )
        .await?;

        self.executors.push(executor_rt);
        self.handles.push(executor_handle);
        Ok(())
    }

    /// Orderly shutdown: shut down every runtime then abort server handles.
    pub async fn shutdown(mut self) {
        // Shut down executors first so the scheduler sees them deregister.
        for rt in self.executors.drain(..) {
            rt.shutdown().await;
        }
        self.scheduler.shutdown().await;
        for h in self.handles.drain(..) {
            h.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterHarnessBuilder
// ---------------------------------------------------------------------------

/// Builder for [`ClusterHarness`].
pub struct ClusterHarnessBuilder {
    scheduler_app: Option<App>,
    executor_apps: Vec<Option<App>>,
}

impl ClusterHarnessBuilder {
    fn new() -> Self {
        Self {
            scheduler_app: None,
            executor_apps: Vec::new(),
        }
    }

    /// Set the scheduler's `App` (datasets, runtime config, etc.).
    pub fn scheduler(mut self, app: App) -> Self {
        self.scheduler_app = Some(app);
        self
    }

    /// Add `n` executors with empty apps.
    pub fn executors(mut self, n: usize) -> Self {
        for _ in 0..n {
            self.executor_apps.push(None);
        }
        self
    }

    /// Add a single executor with a specific `App` configuration.
    pub fn executor_with_app(mut self, app: App) -> Self {
        self.executor_apps.push(Some(app));
        self
    }

    /// Start all cluster nodes and return a [`ClusterHarness`] ready for queries.
    ///
    /// This:
    /// 1. Creates a shared temporary directory
    /// 2. Initialises a PKI (CA + per-node mTLS certs)
    /// 3. Allocates free ports for every node
    /// 4. Builds and starts the scheduler, then all executors in order
    /// 5. Waits for each node to become ready before moving on
    pub async fn start(self) -> Result<ClusterHarness, anyhow::Error> {
        let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());

        let tempdir = Arc::new(tempfile::tempdir().expect("should create temp dir"));

        let pki = init_pki(tempdir.path()).map_err(anyhow::Error::msg)?;

        let n_executors = self.executor_apps.len();

        // --- Scheduler ---

        let scheduler_ports = NodePorts::allocate();

        let scheduler_cert = pki
            .create_client_cert("scheduler")
            .map_err(anyhow::Error::msg)?;

        let scheduler_app = self
            .scheduler_app
            .unwrap_or_else(|| AppBuilder::new("test_scheduler").build());

        let scheduler_config = Config {
            http_bind_address: scheduler_ports.http_addr(),
            flight_bind_address: scheduler_ports.flight_addr(),
            cluster: ClusterConfig {
                role: Some(ClusterRole::Scheduler),
                node_bind_address: scheduler_ports.cluster_addr(),
                node_advertise_address: Some("127.0.0.1".to_string()),
                node_mtls_ca_certificate_file: Some(pki.ca_cert_path.to_string_lossy().to_string()),
                node_mtls_certificate_file: Some(
                    scheduler_cert.cert_path.to_string_lossy().to_string(),
                ),
                node_mtls_key_file: Some(scheduler_cert.key_path.to_string_lossy().to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let scheduler_rt = Arc::new(
            Runtime::builder()
                .with_runtime_config(scheduler_config.clone())
                .with_resolved_cluster_config(
                    ResolvedClusterConfig::try_new(scheduler_config.cluster.clone())
                        .map_err(|e| anyhow::Error::msg(format!("cluster config: {e}")))?,
                )
                .with_app(scheduler_app)
                .build()
                .await,
        );

        let mut handles: Vec<JoinHandle<RuntimeResult<()>>> = Vec::new();

        let cloned = Arc::clone(&scheduler_rt);
        handles.push(tokio::spawn(async move {
            Box::pin(cloned.start_servers(scheduler_config, None, EndpointAuth::no_auth())).await
        }));

        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(60)) => {
                return Err(anyhow::Error::msg("Timed out waiting for scheduler to start"));
            }
            () = Arc::clone(&scheduler_rt).load_components() => {}
        }

        runtime_ready_check(&scheduler_rt).await;

        // Wait for the scheduler's cluster port to be reachable.
        wait_for_tcp(
            &format!("127.0.0.1:{}", scheduler_ports.cluster),
            Duration::from_secs(30),
        )
        .await;

        let scheduler_cluster_addr = scheduler_ports.cluster_advertise();

        // --- Executors ---

        let mut executor_rts: Vec<Arc<Runtime>> = Vec::with_capacity(n_executors);

        for (i, maybe_app) in self.executor_apps.into_iter().enumerate() {
            let label = format!("executor{i}");
            let executor_app =
                maybe_app.unwrap_or_else(|| AppBuilder::new(format!("test_{label}")).build());

            let (executor_rt, executor_handle) =
                start_executor(&label, executor_app, &pki, &scheduler_cluster_addr).await?;

            executor_rts.push(executor_rt);
            handles.push(executor_handle);
        }

        // Extract the executor manager from the now-running scheduler.
        let executor_manager = scheduler_rt
            .datafusion()
            .scheduler_server
            .read()
            .expect("scheduler server lock")
            .clone()
            .expect("scheduler server should be set after start_servers")
            .state
            .executor_manager
            .clone();

        let executor_registry = scheduler_rt.datafusion().executor_registry().cloned();

        Ok(ClusterHarness {
            scheduler: scheduler_rt,
            executors: executor_rts,
            handles,
            executor_manager,
            executor_registry,
            pki,
            scheduler_cluster_addr,
            _tempdir: tempdir,
            next_executor_index: n_executors,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build, start, and wait for one executor node.
///
/// Shared by [`ClusterHarnessBuilder::start`] (initial executors) and
/// [`ClusterHarness::add_executor`] (late-joining executors).
async fn start_executor(
    label: &str,
    app: App,
    pki: &test_framework::pki::PkiConfig,
    scheduler_cluster_addr: &str,
) -> Result<(Arc<Runtime>, JoinHandle<RuntimeResult<()>>), anyhow::Error> {
    let executor_ports = NodePorts::allocate();
    let executor_cert = pki
        .create_client_cert(label)
        .map_err(anyhow::Error::msg)?;

    tracing::warn!("{label}: Ports: {executor_ports:?}. Scheduler: {scheduler_cluster_addr}");
    let executor_config = Config {
        http_bind_address: executor_ports.http_addr(),
        flight_bind_address: executor_ports.flight_addr(),
        cluster: ClusterConfig {
            role: Some(ClusterRole::Executor),
            node_bind_address: executor_ports.cluster_addr(),
            scheduler_address: Some(scheduler_cluster_addr.to_string()),
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
        ..Default::default()
    };

    let executor_rt = Arc::new(
        Runtime::builder()
            .with_runtime_config(executor_config.clone())
            .with_resolved_cluster_config(
                ResolvedClusterConfig::try_new(executor_config.cluster.clone())
                    .map_err(|e| anyhow::Error::msg(format!("cluster config: {e}")))?,
            )
            .with_app(app)
            .build()
            .await,
    );

    let cloned = Arc::clone(&executor_rt);
    let mut executor_handle = tokio::spawn(async move {
        Box::pin(cloned.start_servers(executor_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(60)) => {
            return Err(anyhow::Error::msg(format!(
                "Timed out waiting for {label} to start"
            )));
        }
        result = &mut executor_handle => {
            return Err(anyhow::Error::msg(match result {
                Ok(Ok(())) => format!("{label} server thread finished unexpectedly"),
                Ok(Err(e)) => format!("{label} server failed to start: {e}"),
                Err(e)    => format!("{label} server thread panicked: {e}"),
            }));
        }
        () = Arc::clone(&executor_rt).load_components() => {}
    }

    runtime_ready_check(&executor_rt).await;

    Ok((executor_rt, executor_handle))
}

async fn wait_for_tcp(addr: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
}
