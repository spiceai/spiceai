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
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use runtime::config::ClusterRole;
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use serde_json::json;
use spiceai::Client;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use test_framework::pki::init_pki;
use tokio::time::{Instant, sleep};
use tonic::transport::Endpoint;

// ---------------------------------------------------------------------------
// Port allocation
// ---------------------------------------------------------------------------

/// Allocate `n` free TCP ports on localhost.
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

#[derive(Debug, Clone, Copy)]
struct NodePorts {
    http: u16,
    flight: u16,
    cluster: u16,
    metrics: u16,
}

impl NodePorts {
    fn allocate() -> Self {
        let ports = allocate_ports(4);
        Self {
            http: ports[0],
            flight: ports[1],
            cluster: ports[2],
            metrics: ports[3],
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

    fn metrics_addr(self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.metrics))
    }

    fn cluster_advertise(self) -> String {
        format!("127.0.0.1:{}", self.cluster)
    }
}

// ---------------------------------------------------------------------------
// Node process
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NodeProcess {
    label: String,
    ports: NodePorts,
    child: Mutex<Child>,
    log_path: PathBuf,
}

impl NodeProcess {
    fn exited_status(&self) -> Result<Option<ExitStatus>, anyhow::Error> {
        self.child
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock child process {}: {e}", self.label))?
            .try_wait()
            .map_err(|e| anyhow::anyhow!("failed to check process status for {}: {e}", self.label))
    }

    fn terminate(&self, timeout: Duration) -> Result<(), anyhow::Error> {
        let mut child = self
            .child
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock child process {}: {e}", self.label))?;

        if child
            .try_wait()
            .map_err(|e| anyhow::anyhow!("failed to check process status for {}: {e}", self.label))?
            .is_some()
        {
            return Ok(());
        }

        child
            .kill()
            .map_err(|e| anyhow::anyhow!("failed to kill {} process: {e}", self.label))?;

        let start = std::time::Instant::now();
        while start.elapsed() <= timeout {
            if child
                .try_wait()
                .map_err(|e| {
                    anyhow::anyhow!("failed to check process status for {}: {e}", self.label)
                })?
                .is_some()
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        Err(anyhow::anyhow!(
            "timed out waiting for {} process to stop",
            self.label
        ))
    }

    fn startup_error(&self, context: &str) -> anyhow::Error {
        let log_tail = tail_log(&self.log_path, 80);
        anyhow::anyhow!(
            "{context} for {} failed. log file: {}\n--- log tail ---\n{}",
            self.label,
            self.log_path.display(),
            log_tail
        )
    }
}

// ---------------------------------------------------------------------------
// ClusterHarness
// ---------------------------------------------------------------------------

/// A running Spice cluster (scheduler + N executors) for integration tests.
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
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        for executor in &self.executors {
            let _ = executor.terminate(Duration::from_secs(1));
        }
        let _ = self.scheduler.terminate(Duration::from_secs(1));
    }
}

impl ClusterHarness {
    /// Start building a harness.
    pub fn builder() -> ClusterHarnessBuilder {
        ClusterHarnessBuilder::new()
    }

    /// Block until exactly all configured executors have registered with the scheduler.
    pub async fn wait_for_executors(&self, timeout: Duration) -> Result<(), anyhow::Error> {
        self.wait_until_executor_count(self.executors.len(), timeout)
            .await
    }

    /// Block until exactly `expected` executors are registered with the scheduler.
    pub async fn wait_until_executor_count(
        &self,
        expected: usize,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();
        let mut last_count: Option<usize> = None;

        loop {
            if self.scheduler.exited_status()?.is_some() {
                return Err(self
                    .scheduler
                    .startup_error("scheduler exited while waiting for executor count"));
            }

            if let Ok(count) = self.scheduler_executor_count().await {
                last_count = Some(count);
                if count == expected {
                    // Allow a short grace period for FlightSQL client setup.
                    sleep(Duration::from_millis(300)).await;
                    return Ok(());
                }
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "timed out waiting for {expected} executors; found {last_count:?}"
                ));
            }

            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Run a SQL statement through the scheduler and collect all result batches.
    ///
    /// For DML we always use `execute_update`. For DDL we optimistically try
    /// `execute` first and fall back to `execute_update` for FlightSQL servers
    /// that route DDL through the update path.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let first = first_sql_keyword(sql);

        if matches!(first, Some("INSERT" | "UPDATE" | "DELETE" | "MERGE")) {
            self.execute_update(sql).await?;
            return Ok(vec![]);
        }

        match self.execute_query(sql).await {
            Ok(batches) => Ok(batches),
            Err(query_err)
                if matches!(
                    first,
                    Some("CREATE" | "DROP" | "ALTER" | "TRUNCATE" | "GRANT" | "REVOKE")
                ) =>
            {
                self.execute_update(sql).await.map(|_| vec![]).map_err(|e| {
                    anyhow::anyhow!(
                        "query path failed ({query_err}); fallback update path failed: {e}"
                    )
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Run `EXPLAIN <sql>` through the scheduler and collect all result batches.
    pub async fn explain(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        self.query(&format!("EXPLAIN {sql}")).await
    }

    /// Trigger on-demand refresh for an accelerated dataset through the scheduler HTTP API.
    pub async fn refresh_table(&self, dataset_name: &TableReference) -> Result<(), anyhow::Error> {
        let url = format!(
            "http://{}/v1/datasets/{dataset_name}/acceleration/refresh",
            self.scheduler.ports.http_addr()
        );

        let response = reqwest::Client::new()
            .post(url)
            .json(&json!({}))
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed to call refresh endpoint for {dataset_name:?}: {e}")
            })?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow::anyhow!(
            "failed to refresh dataset {dataset_name:?}: {status} {body}"
        ))
    }

    /// Stop one executor process by index.
    pub async fn shutdown_executor(&self, index: usize) -> Result<(), anyhow::Error> {
        let Some(node) = self.executors.get(index).cloned() else {
            return Err(anyhow::anyhow!("executor index out of bounds: {index}"));
        };

        tokio::task::spawn_blocking(move || node.terminate(Duration::from_secs(10)))
            .await
            .map_err(|e| anyhow::anyhow!("executor shutdown task failed: {e}"))??;

        Ok(())
    }

    /// Stop the scheduler process.
    pub async fn shutdown_scheduler(&self) -> Result<(), anyhow::Error> {
        let node = Arc::clone(&self.scheduler);
        tokio::task::spawn_blocking(move || node.terminate(Duration::from_secs(10)))
            .await
            .map_err(|e| anyhow::anyhow!("scheduler shutdown task failed: {e}"))??;

        Ok(())
    }

    /// Orderly shutdown: stop executor processes first, then scheduler.
    pub async fn shutdown(self) {
        for executor in &self.executors {
            let node = Arc::clone(executor);
            let _ =
                tokio::task::spawn_blocking(move || node.terminate(Duration::from_secs(10))).await;
        }

        let scheduler = Arc::clone(&self.scheduler);
        let _ =
            tokio::task::spawn_blocking(move || scheduler.terminate(Duration::from_secs(10))).await;
    }

    async fn spice_client(&self) -> Result<Client, anyhow::Error> {
        let flight_url = format!("http://{}", self.scheduler.ports.flight_addr());
        let http_url = format!("http://{}", self.scheduler.ports.http_addr());

        Client::builder()
            .flight_url(&flight_url)
            .http_url(&http_url)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("failed to create spice client: {e}"))
    }

    async fn flightsql_client(
        &self,
    ) -> Result<FlightSqlServiceClient<tonic::transport::Channel>, anyhow::Error> {
        let endpoint =
            Endpoint::from_shared(format!("http://{}", self.scheduler.ports.flight_addr()))
                .map_err(|e| anyhow::anyhow!("invalid FlightSQL endpoint: {e}"))?
                .connect_timeout(Duration::from_secs(5));

        let channel = endpoint.connect().await.map_err(|e| {
            anyhow::anyhow!("failed to connect to scheduler FlightSQL endpoint: {e}")
        })?;

        Ok(FlightSqlServiceClient::new(channel))
    }

    async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let client = self.spice_client().await?;
        client
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("failed to execute query '{sql}': {e}"))?
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| anyhow::anyhow!("failed to collect results for '{sql}': {e}"))
    }

    async fn execute_update(&self, sql: &str) -> Result<i64, anyhow::Error> {
        let mut client = self.flightsql_client().await?;
        client
            .execute_update(sql.to_string(), None)
            .await
            .map_err(|e| anyhow::anyhow!("failed to execute update '{sql}': {e}"))
    }

    async fn scheduler_executor_count(&self) -> Result<usize, anyhow::Error> {
        let metrics = reqwest::get(format!(
            "http://{}/metrics",
            self.scheduler.ports.metrics_addr()
        ))
        .await
        .map_err(|e| anyhow::anyhow!("failed to fetch scheduler metrics: {e}"))?
        .text()
        .await
        .map_err(|e| anyhow::anyhow!("failed to read scheduler metrics body: {e}"))?;

        parse_metric_usize(&metrics, "scheduler_active_executors_count").ok_or_else(|| {
            anyhow::anyhow!(
                "'scheduler_active_executors_count' metric not found in scheduler metrics"
            )
        })
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
    pub async fn start(self) -> Result<ClusterHarness, anyhow::Error> {
        let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());

        let tempdir = tempfile::tempdir().map_err(|e| anyhow::anyhow!("tempdir: {e}"))?;
        let pki = init_pki(tempdir.path()).map_err(anyhow::Error::msg)?;

        let node_binary = runtime_cluster_node_binary()?;

        // --- Scheduler ---

        let scheduler_ports = NodePorts::allocate();
        let scheduler_cert = pki
            .create_client_cert("scheduler")
            .map_err(anyhow::Error::msg)?;

        let scheduler_app = self
            .scheduler_app
            .unwrap_or_else(|| AppBuilder::new("test_scheduler").build());

        let scheduler_app_json = tempdir.path().join("scheduler_app.json");
        write_app_json(&scheduler_app, &scheduler_app_json)?;

        let scheduler = spawn_node_process(SpicedNodeArgs {
            node_binary: &node_binary,
            label: "scheduler".to_string(),
            role: ClusterRole::Scheduler,
            ports: scheduler_ports,
            scheduler_address: None,
            app_json: Some(scheduler_app_json),
            cert_ca: pki.ca_cert_path.to_string_lossy().to_string(),
            cert: scheduler_cert.cert_path.to_string_lossy().to_string(),
            key: scheduler_cert.key_path.to_string_lossy().to_string(),
            log_dir: tempdir.path(),
            with_metrics: true,
        })?;

        wait_for_http_health(&scheduler, Duration::from_secs(60)).await?;
        wait_for_tcp(scheduler_ports.cluster_addr(), Duration::from_secs(30)).await?;

        let scheduler_cluster_addr = scheduler_ports.cluster_advertise();

        // --- Executors ---

        let mut executors = Vec::with_capacity(self.executor_apps.len());

        for (i, maybe_app) in self.executor_apps.into_iter().enumerate() {
            let label = format!("executor{i}");
            let executor_ports = NodePorts::allocate();
            let executor_cert = pki.create_client_cert(&label).map_err(anyhow::Error::msg)?;

            let executor_app =
                maybe_app.unwrap_or_else(|| AppBuilder::new(format!("test_{label}")).build());
            let executor_app_json = tempdir.path().join(format!("{label}_app.json"));
            write_app_json(&executor_app, &executor_app_json)?;

            let executor = spawn_node_process(SpicedNodeArgs {
                node_binary: &node_binary,
                label: label.clone(),
                role: ClusterRole::Executor,
                ports: executor_ports,
                scheduler_address: Some(scheduler_cluster_addr.clone()),
                app_json: Some(executor_app_json),
                cert_ca: pki.ca_cert_path.to_string_lossy().to_string(),
                cert: executor_cert.cert_path.to_string_lossy().to_string(),
                key: executor_cert.key_path.to_string_lossy().to_string(),
                log_dir: tempdir.path(),
                with_metrics: false,
            })?;

            wait_for_http_health(&executor, Duration::from_secs(60)).await?;
            executors.push(executor);
        }

        Ok(ClusterHarness {
            scheduler,
            executors,
            _tempdir: tempdir,
        })
    }
}

// ---------------------------------------------------------------------------
// Process spawning
// ---------------------------------------------------------------------------

/// The configuration required to start a spiced node within a Spice cluster.
struct SpicedNodeArgs<'a> {
    node_binary: &'a Path,
    label: String,
    role: ClusterRole,
    ports: NodePorts,
    scheduler_address: Option<String>,
    app_json: Option<PathBuf>,
    cert_ca: String,
    cert: String,
    key: String,
    log_dir: &'a Path,
    with_metrics: bool,
}

fn spawn_node_process(args: SpicedNodeArgs<'_>) -> Result<Arc<NodeProcess>, anyhow::Error> {
    let log_path = args.log_dir.join(format!("{}.log", args.label));
    let stdout = std::fs::File::create(&log_path)
        .map_err(|e| anyhow::anyhow!("failed to create {} log file: {e}", args.label))?;
    let stderr = stdout
        .try_clone()
        .map_err(|e| anyhow::anyhow!("failed to clone {} log file handle: {e}", args.label))?;

    let mut cmd = Command::new(args.node_binary);
    cmd.arg("--role")
        .arg(match args.role {
            ClusterRole::Scheduler => "scheduler",
            ClusterRole::Executor => "executor",
        })
        .arg("--http-bind")
        .arg(args.ports.http_addr().to_string())
        .arg("--flight-bind")
        .arg(args.ports.flight_addr().to_string())
        .arg("--cluster-bind")
        .arg(args.ports.cluster_addr().to_string())
        .arg("--node-advertise-address")
        .arg("127.0.0.1")
        .arg("--node-mtls-ca-certificate-file")
        .arg(args.cert_ca)
        .arg("--node-mtls-certificate-file")
        .arg(args.cert)
        .arg("--node-mtls-key-file")
        .arg(args.key)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));

    if args.with_metrics {
        cmd.arg("--metrics-bind")
            .arg(args.ports.metrics_addr().to_string());
    }

    if let Some(scheduler_address) = args.scheduler_address {
        cmd.arg("--scheduler-address").arg(scheduler_address);
    }

    if let Some(app_json) = args.app_json {
        cmd.arg("--app-json").arg(app_json);
    }

    let child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn {} process: {e}", args.label))?;

    Ok(Arc::new(NodeProcess {
        label: args.label,
        ports: args.ports,
        child: Mutex::new(child),
        log_path,
    }))
}

fn runtime_cluster_node_binary() -> Result<PathBuf, anyhow::Error> {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_runtime_cluster_node") {
        return Ok(PathBuf::from(path));
    }

    // Fallback for runners that don't set CARGO_BIN_EXE_*.
    let current = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("failed to resolve current test binary path: {e}"))?;
    let deps_dir = current
        .parent()
        .ok_or_else(|| anyhow::anyhow!("failed to resolve test binary parent directory"))?;
    let target_debug = deps_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("failed to resolve target/debug directory"))?;

    let candidate = target_debug.join(format!(
        "runtime_cluster_node{}",
        std::env::consts::EXE_SUFFIX
    ));

    if candidate.exists() {
        Ok(candidate)
    } else {
        Err(anyhow::anyhow!(
            "runtime_cluster_node binary not found. expected at {}",
            candidate.display()
        ))
    }
}

fn write_app_json(app: &App, path: &Path) -> Result<(), anyhow::Error> {
    let bytes = serde_json::to_vec(app)
        .map_err(|e| anyhow::anyhow!("failed to serialize app to JSON: {e}"))?;
    std::fs::write(path, bytes)
        .map_err(|e| anyhow::anyhow!("failed to write app JSON {}: {e}", path.display()))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn first_sql_keyword(sql: &str) -> Option<&str> {
    sql.trim_start()
        .split_whitespace()
        .next()
        .map(str::to_ascii_uppercase)
        .as_deref()
        .map(|s| match s {
            "SELECT" => "SELECT",
            "WITH" => "WITH",
            "EXPLAIN" => "EXPLAIN",
            "SHOW" => "SHOW",
            "DESCRIBE" => "DESCRIBE",
            "VALUES" => "VALUES",
            "INSERT" => "INSERT",
            "UPDATE" => "UPDATE",
            "DELETE" => "DELETE",
            "MERGE" => "MERGE",
            "CREATE" => "CREATE",
            "DROP" => "DROP",
            "ALTER" => "ALTER",
            "TRUNCATE" => "TRUNCATE",
            "GRANT" => "GRANT",
            "REVOKE" => "REVOKE",
            _ => "",
        })
        .filter(|keyword| !keyword.is_empty())
}

fn parse_metric_usize(metrics_text: &str, metric_name: &str) -> Option<usize> {
    metrics_text.lines().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || !trimmed.starts_with(metric_name) {
            return None;
        }

        let value_token = trimmed.split_whitespace().nth(1)?;
        let value = value_token.parse::<f64>().ok()?;
        if value.is_sign_negative() || !value.is_finite() {
            return None;
        }
        Some(value as usize)
    })
}

async fn wait_for_http_health(
    node: &Arc<NodeProcess>,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();
    let health_url = format!("http://{}/health", node.ports.http_addr());

    let start = Instant::now();
    loop {
        if node.exited_status()?.is_some() {
            return Err(node.startup_error("node exited before becoming healthy"));
        }

        if let Ok(resp) = client.get(&health_url).send().await
            && resp.status().is_success()
        {
            return Ok(());
        }

        if start.elapsed() > timeout {
            return Err(node.startup_error("timed out waiting for node health"));
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_tcp(addr: SocketAddr, timeout: Duration) -> Result<(), anyhow::Error> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow::anyhow!("timed out waiting for TCP port {}", addr))
}

fn tail_log(path: &Path, max_lines: usize) -> String {
    let Ok(content) = std::fs::read_to_string(path) else {
        return "<unable to read log file>".to_string();
    };

    let mut lines: Vec<&str> = content.lines().rev().take(max_lines).collect();
    lines.reverse();
    lines.join("\n")
}
