/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::Error::{self, FailedToStartClusterExecutor};
use crate::cluster::datafusion::datafusion_and_cluster_physical_optimizers;
use crate::cluster::partition::{
    executor_request_initial_partitions,
    scheduler_task::{PartitionAssignmentConfig, PartitionAssignmentTask},
};
use crate::config::{ClusterConfig, ClusterRole};
use crate::jobs::JobExecutor;
use crate::status::ComponentStatus;
use crate::{
    CLUSTER_INTERNAL_SERVER, CLUSTER_PARTITION_ASSIGNMENT_TASK, CLUSTER_SCHEDULER_REGISTRY,
    FailedToRegisterSchedulerSnafu, FailedToStartClusterExecutorSnafu,
    FailedToStartClusterSchedulerSnafu, LogErrors, Runtime, UnableToStartClusterServerSnafu,
};
use ::datafusion::optimizer::AnalyzerRule;
use ::datafusion::prelude::SessionConfig;
use ::datafusion::sql::ResolvedTableReference;
use app::App;
use ballista_core::config::ShuffleFormat as BallistaShuffleFormat;
use ballista_core::extension::SessionConfigExt;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::executor_resource::Resource;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{
    ExecutorRegistration, ExecutorResource, ExecutorSpecification,
};
use ballista_core::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_executor::execution_loop;
use ballista_executor::executor::Executor;
use ballista_scheduler::cluster::memory::{InMemoryClusterState, InMemoryJobState};
use ballista_scheduler::cluster::{BallistaCluster, ClusterState};
use ballista_scheduler::config::{OnCancelTasksFn, SchedulerConfig};
use ballista_scheduler::scheduler_process;
use ballista_scheduler::scheduler_server::SchedulerServer;
use ballista_scheduler::state::execution_graph::RunningTaskInfo;
use datafusion::codec::spice_logical_codec::SpiceLogicalCodec;
use datafusion::codec::spice_physical_codec::SpicePhysicalCodec;
use datafusion_expr::Expr;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::future::try_join_all;
use runtime_datafusion::config::cluster_config::SpiceClusterConfig;
use runtime_object_store::registry::default_runtime_env;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use runtime_proto::{
    GetAppDefinitionRequest, GetDdlCatchupRequest, GetSchedulersRequest, TaskCancelInfo,
};
use runtime_secrets::Secrets;
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use url::Url;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};
use util::session_state::builder_from_existing;
const SCHEDULER_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const SCHEDULER_BACKOFF_MAX: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub enum DistributedNode {
    Scheduler {
        peers: Arc<RwLock<SchedulerPeers>>,

        /// Job executor for async SQL query jobs (only available in cluster mode with scheduler config)
        job_executor: Arc<RwLock<Option<Arc<JobExecutor>>>>,

        /// Registry of connected executors for `FlightSQL`.
        executor_registry: Arc<ExecutorRegistry>,

        /// Shared cluster state document (cluster.json) for partition metadata and scheduler registry.
        cluster_state: Arc<ClusterStateStore>,

        /// Heartbeat store for this scheduler.
        heartbeats: Arc<SchedulerHeartbeatStore>,

        /// Partition store for accelerated table partition metadata.
        accelerations_partitions_store: Arc<AccelerationsPartitions>,

        /// Partition store for catalog/federated table partition metadata.
        catalog_partitions_store: Arc<CatalogPartitions>,

        /// Partition service for discovery, assignment, and executor notification.
        partition_service: Arc<PartitionService>,
    },
    Executor {
        /// Partition assignments for this runtime (executor) for each table.
        ///
        /// This is populated during startup when the executor registers with the scheduler.
        /// It contains the list of partition filters (expressions) that this executor is responsible for.
        partition_assignments: Arc<RwLock<HashMap<ResolvedTableReference, Vec<Expr>>>>,
    },
}

impl DistributedNode {
    #[must_use]
    pub fn is_scheduler(&self) -> bool {
        matches!(self, DistributedNode::Scheduler { .. })
    }

    #[must_use]
    pub fn is_executor(&self) -> bool {
        matches!(self, DistributedNode::Executor { .. })
    }
}

type SchedulerEndpointOverride = Arc<
    dyn Fn(Endpoint) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
>;

struct SchedulerPollHandle {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

fn normalize_scheduler_endpoint(address: &str, tls_enabled: bool) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        return address.to_string();
    }

    let scheme = if tls_enabled { "https" } else { "http" };
    format!("{scheme}://{address}")
}

/// Represents the connection state machine for the scheduler poll loop.
///
/// This enum tracks progress through connection establishment, avoiding redundant
/// work when only later stages fail (e.g., retrying `connect()` without recreating
/// the endpoint).
#[expect(clippy::large_enum_variant)]
enum SchedulerConnectionState {
    /// Initial state: need to create endpoint URL and gRPC endpoint
    NeedsEndpoint,
    /// Endpoint created and TLS configured, ready to connect
    ReadyToConnect {
        endpoint: Endpoint,
        endpoint_url: String,
    },
}

fn spawn_scheduler_poll_loop(
    scheduler_address: String,
    client_tls_config: Option<ClientTlsConfig>,
    executor: Arc<Executor>,
    codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode>,
    readiness_sender: Arc<Mutex<Option<oneshot::Sender<String>>>>,
    poll_now_notify: Option<Arc<Notify>>,
    available_task_slots: Arc<tokio::sync::Semaphore>,
) -> SchedulerPollHandle {
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let tls_enabled = client_tls_config.is_some();

    let task = tokio::spawn(async move {
        let mut backoff = FibonacciBackoffBuilder::new()
            .max_duration(Some(SCHEDULER_BACKOFF_MAX))
            .build();

        let mut state = SchedulerConnectionState::NeedsEndpoint;

        loop {
            if token.is_cancelled() {
                tracing::debug!("Stopping scheduler poll loop for {scheduler_address} (cancelled)");
                break;
            }

            // Build the endpoint if we don't have one yet
            let (endpoint, endpoint_url) = match state {
                SchedulerConnectionState::NeedsEndpoint => {
                    let endpoint_url =
                        normalize_scheduler_endpoint(&scheduler_address, tls_enabled);
                    let scheduler_endpoint = match create_grpc_client_endpoint(
                        endpoint_url.clone(),
                        Some(&GrpcClientConfig::default()),
                    ) {
                        Ok(endpoint) => endpoint,
                        Err(err) => {
                            tracing::warn!(
                                "Failed to create scheduler endpoint {endpoint_url}: {err}"
                            );
                            if let Some(delay) = backoff.next_duration() {
                                tokio::select! {
                                    () = token.cancelled() => break,
                                    () = tokio::time::sleep(delay) => {}
                                }
                            }
                            continue;
                        }
                    };

                    let scheduler_endpoint = if let Some(tls_config) = client_tls_config.clone() {
                        match scheduler_endpoint.tls_config(tls_config) {
                            Ok(endpoint) => endpoint,
                            Err(err) => {
                                tracing::warn!(
                                    "Failed to configure TLS for scheduler endpoint {endpoint_url}: {err}"
                                );
                                if let Some(delay) = backoff.next_duration() {
                                    tokio::select! {
                                        () = token.cancelled() => break,
                                        () = tokio::time::sleep(delay) => {}
                                    }
                                }
                                continue;
                            }
                        }
                    } else {
                        scheduler_endpoint
                    };

                    // Cache the endpoint for future retries
                    state = SchedulerConnectionState::ReadyToConnect {
                        endpoint: scheduler_endpoint.clone(),
                        endpoint_url: endpoint_url.clone(),
                    };
                    (scheduler_endpoint, endpoint_url)
                }
                SchedulerConnectionState::ReadyToConnect {
                    ref endpoint,
                    ref endpoint_url,
                } => (endpoint.clone(), endpoint_url.clone()),
            };

            let scheduler_connection = match endpoint.connect().await {
                Ok(connection) => connection,
                Err(err) => {
                    tracing::warn!("Unable to connect to scheduler at {endpoint_url}: {err}");
                    if let Some(delay) = backoff.next_duration() {
                        tokio::select! {
                            () = token.cancelled() => break,
                            () = tokio::time::sleep(delay) => {}
                        }
                    }
                    continue;
                }
            };

            backoff.reset();
            let scheduler = SchedulerGrpcClient::new(scheduler_connection)
                .max_encoding_message_size(usize::MAX)
                .max_decoding_message_size(usize::MAX);

            let (tx_ready, rx_ready) = oneshot::channel();
            let readiness_sender = Arc::clone(&readiness_sender);
            let readiness_task = tokio::spawn(async move {
                if let Ok(executor_id) = rx_ready.await {
                    let sender = if let Ok(mut sender) = readiness_sender.lock() {
                        sender.take()
                    } else {
                        tracing::warn!(
                            "Readiness sender lock poisoned while handling executor readiness"
                        );
                        None
                    };
                    if let Some(sender) = sender {
                        let _ = sender.send(executor_id);
                    }
                }
            });

            let poll_future = execution_loop::poll_loop(
                scheduler,
                Arc::clone(&executor),
                codec.clone(),
                Some(tx_ready),
                poll_now_notify.clone(),
                Some(Arc::clone(&available_task_slots)),
            );

            tokio::select! {
                () = token.cancelled() => {
                    readiness_task.abort();
                    tracing::debug!(
                        "Stopping scheduler poll loop for {scheduler_address} (cancelled)"
                    );
                    break;
                }
                result = poll_future => {
                    readiness_task.abort();
                    if let Err(err) = result {
                        tracing::warn!(
                            "Scheduler poll loop ended for {scheduler_address}: {err}"
                        );
                    }
                    if let Some(delay) = backoff.next_duration() {
                        tokio::select! {
                            () = token.cancelled() => break,
                            () = tokio::time::sleep(delay) => {}
                        }
                    }
                }
            }
        }
    });

    SchedulerPollHandle { cancel, task }
}

async fn fetch_scheduler_membership(
    scheduler_url: &Url,
    client_tls_config: Option<ClientTlsConfig>,
) -> Option<Vec<String>> {
    let mut cluster_client =
        match create_cluster_service_client(scheduler_url, client_tls_config.clone()).await {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!("Failed to create scheduler membership client: {err}");
                return None;
            }
        };

    match cluster_client.get_schedulers(GetSchedulersRequest {}).await {
        Ok(response) => {
            let schedulers = response.into_inner().schedulers;
            let scheduler_addresses = schedulers
                .iter()
                .map(|scheduler| scheduler.advertise_address.clone())
                .collect::<Vec<_>>();
            Some(scheduler_addresses)
        }
        Err(status) => {
            tracing::warn!("Failed to get scheduler membership from scheduler: {status}");
            None
        }
    }
}

#[expect(clippy::too_many_arguments)]
fn update_scheduler_pollers(
    pollers: &mut HashMap<String, SchedulerPollHandle>,
    known_schedulers: &mut HashSet<String>,
    addresses: Vec<String>,
    client_tls_config: Option<&ClientTlsConfig>,
    executor: &Arc<Executor>,
    codec: &BallistaCodec<LogicalPlanNode, PhysicalPlanNode>,
    readiness_sender: &Arc<Mutex<Option<oneshot::Sender<String>>>>,
    poll_now_notify: Option<&Arc<Notify>>,
    available_task_slots: &Arc<tokio::sync::Semaphore>,
) {
    let next_schedulers: HashSet<String> = addresses.into_iter().collect();

    let added: Vec<String> = next_schedulers
        .difference(known_schedulers)
        .cloned()
        .collect();
    let removed: Vec<String> = known_schedulers
        .difference(&next_schedulers)
        .cloned()
        .collect();

    if !added.is_empty() || !removed.is_empty() {
        let added_list = added.join(",");
        let removed_list = removed.join(",");
        tracing::debug!(
            "Scheduler membership updated; added=[{added_list}], removed=[{removed_list}]"
        );
    }

    for address in added {
        let handle = spawn_scheduler_poll_loop(
            address.clone(),
            client_tls_config.cloned(),
            Arc::clone(executor),
            codec.clone(),
            Arc::clone(readiness_sender),
            poll_now_notify.cloned(),
            Arc::clone(available_task_slots),
        );
        pollers.insert(address, handle);
    }

    for address in removed {
        if let Some(handle) = pollers.remove(&address) {
            handle.cancel.cancel();
            tokio::spawn(async move {
                let _ = handle.task.await;
            });
        }
    }

    *known_schedulers = next_schedulers;
}

pub(crate) mod accelerated_partition_provider;
pub(crate) use runtime_cluster::cluster_state;
mod composite_flight_service;
mod control_stream_client;
pub mod datafusion;
mod heartbeat;
pub mod metrics_collector;
pub mod partition;
pub mod pki;
mod reaper;
pub(crate) mod scheduler_registry;
mod servers;
mod service;

use crate::cluster::partition::service::PartitionService;
pub use accelerated_partition_provider::AcceleratedPartitionProvider;
pub use cluster_state::{ClusterStateStore, SchedulerEntry};
pub use control_stream_client::ControlStreamManager;
pub use heartbeat::{CLOCK_SKEW_TOLERANCE_MS, SchedulerHeartbeat, SchedulerHeartbeatStore};
pub use partition::{PartitionMetadata, PartitionStore, TablePartitionMetadata};
pub use reaper::{Reaper, ReaperOutcome};
use runtime_cluster::store::{AccelerationsPartitions, CatalogPartitions};
pub use runtime_cluster::{ExecutorRegistry, FederatedPartitionProvider, TablePartitions};
pub use scheduler_registry::SchedulerPeers;
pub use scheduler_registry::start_scheduler_registry;
pub use servers::{start_executor_flight_server, start_internal_cluster_server};
pub use service::{ClusterServiceImpl, ExecutorControlStreamRegistry};

/// mTLS configuration for cluster communications.
///
/// Holds reloadable server identity + client-CA verifier (for accepting
/// inbound mTLS connections) plus the on-disk paths required to
/// reconstruct a `tonic::transport::ClientTlsConfig` on demand for
/// outbound connections.
///
/// Hot-reload behavior:
///
/// * All three pieces (server cert, client verifier, outbound
///   `ClientTlsConfig`) live in a single
///   [`crate::cluster::pki::ClusterPkiBundle`] backed by one
///   `ArcSwap<ClusterPkiSnapshot>`. When any of CA / cert / key change
///   on disk we re-parse and validate the **whole** bundle; if anything
///   is invalid the previous snapshot is kept (last-known-good).
///   Successful rotations swap all three pointers in one operation, so
///   the runtime can never observe a partial rotation (e.g., new server
///   cert paired with stale verifier).
///
/// * Server side: the `rustls::ServerConfig` returned by
///   [`Self::server_config`] installs the bundle as both
///   `ResolvesServerCert` and `ClientCertVerifier`, so a single Arc
///   serves both rustls slots.
///
/// * Client side: [`Self::client_tls_config`] returns a fresh
///   `ClientTlsConfig` clone each call. tonic bakes the TLS config into
///   a `Channel` at connect time, so live rotation requires the
///   consumer to reconnect — the connection-rebuild loops in this
///   module already do so on every transient error.
#[derive(Debug, Clone)]
pub struct ClusterTlsConfig {
    inner: Arc<ClusterTlsConfigInner>,
}

#[derive(Debug)]
struct ClusterTlsConfigInner {
    /// Paths kept around for diagnostics / tests.
    ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    /// rustls server config (h2 ALPN, mandatory client cert). The
    /// resolver + verifier inside both delegate to `bundle`, so this
    /// `ServerConfig` is built once and never rebuilt on rotation.
    server_config: Arc<rustls::ServerConfig>,
    /// Atomic bundle of (server cert+key, client verifier, outbound
    /// `ClientTlsConfig`). All three rotate together via a single
    /// `ArcSwap` swap inside the bundle.
    bundle: Arc<crate::cluster::pki::ClusterPkiBundle>,
    /// Drop-guard for the watcher. In the centralized path the binary
    /// owns the [`crate::tls::TlsControl`] for the whole process; this
    /// `Arc` is purely a safety net so the watcher dispatcher outlives
    /// us if the caller drops their `TlsControl` first (notably the
    /// test path that constructs a transient one).
    #[expect(
        dead_code,
        reason = "drop-guard only; never read, but extends the watcher dispatcher's lifetime to match this struct"
    )]
    watcher_keepalive: Arc<crate::tls::CertWatcher>,
}

impl ClusterTlsConfig {
    /// Creates a new `ClusterTlsConfig` by loading the CA, certificate, and key files,
    /// validating their lineage, and registering them for hot-reload on
    /// the supplied process-wide [`crate::tls::TlsControl`].
    ///
    /// # Errors
    ///
    /// Returns an error if any of the files cannot be read, the certificates
    /// fail to parse / validate, or the watcher refuses the registration.
    pub fn try_new(
        ca_cert_path: &str,
        cert_path: &str,
        key_path: &str,
        control: &crate::tls::TlsControl,
    ) -> std::io::Result<Self> {
        let ca_path_buf = PathBuf::from(ca_cert_path);
        let cert_path_buf = PathBuf::from(cert_path);
        let key_path_buf = PathBuf::from(key_path);

        // Build + register the atomic bundle. `try_new` performs the
        // initial parse + chain validation before returning, so any
        // bad starting state surfaces here as an `io::Error`.
        let bundle = crate::cluster::pki::ClusterPkiBundle::try_new(
            &crate::cluster::pki::ClusterPkiPaths {
                ca: ca_path_buf.clone(),
                cert: cert_path_buf.clone(),
                key: key_path_buf.clone(),
            },
            control.watcher(),
        )?;

        // The bundle implements both `ResolvesServerCert` and
        // `ClientCertVerifier`, so a single `Arc<ClusterPkiBundle>` can
        // be installed in both slots. Server-side reads from the same
        // snapshot as outbound — no half-rotation window.
        let mut server_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(Arc::clone(&bundle) as Arc<_>)
            .with_cert_resolver(Arc::clone(&bundle) as Arc<_>);
        // Cluster gRPC is h2-only.
        server_config.alpn_protocols = vec![b"h2".to_vec()];
        let server_config = Arc::new(server_config);

        Ok(Self {
            inner: Arc::new(ClusterTlsConfigInner {
                ca_path: ca_path_buf,
                cert_path: cert_path_buf,
                key_path: key_path_buf,
                server_config,
                bundle,
                watcher_keepalive: Arc::clone(control.watcher()),
            }),
        })
    }

    /// Reloadable `rustls::ServerConfig` for accepting inbound mTLS
    /// connections (h2 ALPN, mandatory client cert).
    #[must_use]
    pub fn server_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.inner.server_config)
    }

    /// Snapshot of the current outbound `ClientTlsConfig`. Cheap to call
    /// per connection — callers should prefer doing so over caching, since
    /// only fresh snapshots reflect on-disk rotation.
    #[must_use]
    pub fn client_tls_config(&self) -> ClientTlsConfig {
        (*self.inner.bundle.client_tls_config()).clone()
    }

    /// Paths the watcher is monitoring — exposed for tests.
    #[must_use]
    #[doc(hidden)]
    pub fn watched_paths_for_tests(&self) -> (PathBuf, PathBuf, PathBuf) {
        (
            self.inner.ca_path.clone(),
            self.inner.cert_path.clone(),
            self.inner.key_path.clone(),
        )
    }

    /// Atomic PKI bundle backing this cluster TLS config. Exposed for
    /// tests that introspect the bundle (e.g. fingerprint comparisons).
    #[must_use]
    #[doc(hidden)]
    pub fn bundle_for_tests(&self) -> Arc<crate::cluster::pki::ClusterPkiBundle> {
        Arc::clone(&self.inner.bundle)
    }
}

fn io_other<E: std::fmt::Display>(err: E) -> io::Error {
    io::Error::other(err.to_string())
}

/// Cluster configuration with eagerly loaded TLS config.
///
/// This struct wraps `ClusterConfig` and caches the `ClusterTlsConfig` on creation
/// to avoid reading certificate files repeatedly.
#[derive(Debug, Default)]
pub struct ResolvedClusterConfig {
    config: ClusterConfig,
    /// Cached cluster TLS config for mTLS when configured.
    tls_config: Option<ClusterTlsConfig>,
    /// Pre-computed scheduler URL string for Ballista configuration.
    scheduler_url: Option<String>,
    /// Resolved scheduler address URL (with scheme inferred if omitted).
    scheduler_address_url: Option<Url>,
    /// Advertise address with port stripped (if present in the original input).
    node_advertise_host: Option<String>,
}

impl ResolvedClusterConfig {
    /// Creates a new `ResolvedClusterConfig` from the given `ClusterConfig`, eagerly loading
    /// the TLS configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Cluster mode is set but TLS certificates are not fully specified
    /// - Cluster mode is set but advertise address is not specified
    /// - Certificate files cannot be read
    pub fn try_new(config: ClusterConfig) -> std::io::Result<Self> {
        Self::try_new_with_tls(config, None)
    }

    /// Like [`Self::try_new`] but takes the process-wide
    /// [`crate::tls::TlsControl`] so cluster mTLS reload events flow
    /// through the same watcher as public TLS. Production callers
    /// should always go through this constructor; the no-arg
    /// `try_new` is preserved for tests that don't need centralization.
    pub fn try_new_with_tls(
        config: ClusterConfig,
        control: Option<&crate::tls::TlsControl>,
    ) -> std::io::Result<Self> {
        // Cluster mTLS configuration must be complete when provided
        let tls_config = match (
            &config.node_mtls_ca_certificate_file,
            &config.node_mtls_certificate_file,
            &config.node_mtls_key_file,
        ) {
            (Some(ca_path), Some(cert_path), Some(key_path)) => {
                // Use the shared TlsControl when available; otherwise
                // (test path) spin up a private one whose lifetime is
                // tied to the resulting `ClusterTlsConfig` via the
                // bundle's registered callback.
                let owned_control;
                let control_ref = if let Some(c) = control {
                    c
                } else {
                    owned_control =
                        crate::tls::TlsControl::new().map_err(|e| io_other(e.to_string()))?;
                    &owned_control
                };
                Some(ClusterTlsConfig::try_new(
                    ca_path,
                    cert_path,
                    key_path,
                    control_ref,
                )?)
            }
            (None, None, None) => None,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Cluster mTLS requires all of: --node-mtls-ca-certificate-file, --node-mtls-certificate-file, --node-mtls-key-file",
                ));
            }
        };

        // Determine effective cluster role (explicit or implicit from scheduler_address)
        let is_cluster_role = config.role.is_some() || config.scheduler_address.is_some();

        // Validate all cluster role requirements at once
        if is_cluster_role {
            let mut missing_flags = Vec::new();

            if tls_config.is_none() && !config.allow_insecure_connections {
                missing_flags.push("--node-mtls-ca-certificate-file, --node-mtls-certificate-file, --node-mtls-key-file (or --allow-insecure-connections)");
            }
            if config.node_advertise_address.is_none() {
                missing_flags.push("--node-advertise-address");
            }

            if !missing_flags.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Cluster mode requires the following flags: {}",
                        missing_flags.join(", ")
                    ),
                ));
            }
        }

        // Determine the scheme based on TLS config or insecure flag
        let inferred_scheme = if tls_config.is_some() {
            "https"
        } else {
            "http"
        };

        // Pre-compute scheduler URL from advertise address
        let bind_port = config.node_port();
        let node_advertise_host = config.node_advertise_address.as_ref().map(|addr| {
            // Extract just the host, ignoring any port - always use bind_port
            if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
                // Full socket address - strip the port with deprecation warning
                tracing::warn!("Port in --node-advertise-address will be ignored. Using port {bind_port} from --node-bind-address.");
                socket_addr.ip().to_string()
            } else if let Some((host_part, port_part)) = addr.rsplit_once(':') {
                // Check if this looks like host:port
                if port_part.parse::<u16>().is_ok() && !host_part.is_empty() {
                    tracing::warn!("Port in --node-advertise-address will be ignored. Using port {bind_port} from --node-bind-address.");
                    host_part.trim_matches(['[', ']']).to_string()
                } else {
                    // Not a valid port, use as-is (e.g. IPv6 without brackets)
                    addr.clone()
                }
            } else {
                // No colon - just a hostname
                addr.clone()
            }
        });
        let scheduler_url = node_advertise_host
            .as_ref()
            .map(|host| format!("{inferred_scheme}://{host}:{bind_port}"));

        // Resolve scheduler address URL, inferring scheme if omitted and default port if not provided
        let scheduler_address_url = config
            .scheduler_address
            .as_ref()
            .map(|addr| {
                // Check if scheme is already present
                let url = if addr.starts_with("http://") || addr.starts_with("https://") {
                    Url::parse(addr)
                } else {
                    // Infer scheme from TLS config
                    Url::parse(&format!("{inferred_scheme}://{addr}"))
                }?;

                // If no port is specified, use the default cluster port (50052)
                if url.port().is_none() {
                    let mut url_with_port = url;
                    url_with_port
                        .set_port(Some(50052))
                        .map_err(|()| url::ParseError::InvalidPort)?;
                    Ok(url_with_port)
                } else {
                    Ok(url)
                }
            })
            .transpose()
            .map_err(|e: url::ParseError| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid --scheduler-address URL: {e}"),
                )
            })?;

        Ok(Self {
            config,
            tls_config,
            scheduler_url,
            scheduler_address_url,
            node_advertise_host,
        })
    }

    /// Returns the cluster role.
    #[must_use]
    pub fn role(&self) -> Option<&ClusterRole> {
        self.config.role.as_ref()
    }

    /// Returns the fully qualified URL that this node advertises to other cluster nodes.
    fn node_advertise_url(&self) -> String {
        let port = self.config.node_bind_address.port();
        let protocol = if self.tls_enabled() { "https" } else { "http" };
        format!(
            "{}://{}:{}",
            protocol,
            self.node_advertise_address()
                .unwrap_or(&self.config.node_bind_address.ip().to_string()),
            port
        )
    }

    /// Returns the effective cluster role.
    ///
    /// This accounts for the implicit executor role: if `--scheduler-address` is set
    /// but no explicit `--role` is specified, this returns `ClusterRole::Executor`.
    #[must_use]
    pub fn effective_role(&self) -> Option<ClusterRole> {
        if let Some(role) = &self.config.role {
            return Some(role.clone());
        }
        // If scheduler_address is set, implicitly assume executor role
        if self.config.scheduler_address.is_some() {
            return Some(ClusterRole::Executor);
        }
        None
    }

    /// Returns the cluster bind address.
    #[must_use]
    pub fn node_bind_address(&self) -> SocketAddr {
        self.config.node_bind_address
    }

    /// Returns the scheduler URL (for executors).
    /// The scheme is inferred from TLS configuration if omitted in the original input.
    #[must_use]
    pub fn scheduler_address(&self) -> Option<&Url> {
        self.scheduler_address_url.as_ref()
    }

    /// Returns the scheduler URL as a string for use in Ballista configuration.
    ///
    /// This is constructed from the advertise address during initialization.
    /// Returns `None` if advertise address was not configured.
    #[must_use]
    pub fn scheduler_url_string(&self) -> Option<&str> {
        self.scheduler_url.as_deref()
    }

    /// Returns the advertise address (host only, with any port stripped).
    #[must_use]
    pub fn node_advertise_address(&self) -> Option<&str> {
        self.node_advertise_host.as_deref()
    }

    /// Returns the cluster TLS config if configured.
    #[must_use]
    pub fn tls_config(&self) -> Option<&ClusterTlsConfig> {
        self.tls_config.as_ref()
    }

    /// Returns whether cluster mTLS is enabled.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.tls_config.is_some()
    }

    /// Returns whether this node allows insecure cluster communication.
    #[must_use]
    pub fn allow_insecure_connections(&self) -> bool {
        self.config.allow_insecure_connections
    }

    /// Returns a fresh `ClientTlsConfig` snapshot for connecting to other
    /// cluster nodes. Reflects on-disk rotation each call.
    #[must_use]
    pub fn client_tls_config(&self) -> Option<ClientTlsConfig> {
        self.tls_config
            .as_ref()
            .map(ClusterTlsConfig::client_tls_config)
    }

    /// Reloadable rustls server config for accepting inbound mTLS
    /// connections (h2 ALPN). `None` if cluster mTLS is disabled.
    #[must_use]
    pub fn cluster_server_config(&self) -> Option<Arc<rustls::ServerConfig>> {
        self.tls_config
            .as_ref()
            .map(ClusterTlsConfig::server_config)
    }

    /// Get the node's advertise address for node identification
    pub fn node_id(&self) -> String {
        self.scheduler_url_string()
            .or_else(|| self.node_advertise_address())
            .map_or_else(|| self.node_bind_address().to_string(), str::to_string)
    }
}

/// Creates & binds a Ballista scheduler to the Runtime handle, then updates status
pub async fn initialize_cluster_scheduler(rt: &Arc<Runtime>) -> crate::Result<()> {
    let (scheduler, executor_stream_registry) = create_scheduler_server(rt).await?;

    rt.df
        .bind_scheduler_server(Arc::new(scheduler))
        .boxed()
        .context(FailedToStartClusterSchedulerSnafu)?;

    rt.df
        .bind_executor_stream_registry(executor_stream_registry)
        .boxed()
        .context(FailedToStartClusterSchedulerSnafu)?;

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    Ok(())
}

/// Bootstrap `cluster.json` and replay any DDL statements already in the log.
///
/// Must be called before the scheduler server is bound so that `cluster.json`
/// exists before any executor can connect and call `GetAppDefinition`, and
/// before partition metadata seeding runs.
///
/// For a joining scheduler (cluster.json already exists), replays the DDL log
/// so this scheduler's DataFusion context has all DDL-created tables/schemas,
/// with a single catchup pass to cover any DDL appended during replay.
async fn bootstrap_and_replay_ddl(rt: &Arc<Runtime>) -> crate::Result<()> {
    let Some(cluster_state) = rt.cluster_state() else {
        return Ok(());
    };

    cluster_state
        .bootstrap()
        .await
        .map_err(|e| crate::Error::FailedToStartClusterScheduler {
            source: Box::new(e),
        })?;

    let initial_ddl_statements = match cluster_state.read().await {
        Ok(state) if !state.ddl_log.is_empty() => {
            tracing::info!(
                "Replaying {} DDL statement(s) from cluster state",
                state.ddl_log.len()
            );
            replay_ddl_statements(rt, &state.ddl_log).await
        }
        Ok(_) => 0,
        Err(e) => {
            tracing::warn!("Failed to read cluster state for DDL replay: {e}");
            return Ok(());
        }
    };

    // Single catchup pass: cover any DDL appended by another scheduler
    // while we were replaying.
    match cluster_state.read().await {
        Ok(state) if state.ddl_log.len() > initial_ddl_statements => {
            let catchup = &state.ddl_log[initial_ddl_statements..];
            tracing::info!(
                "Replaying {} DDL catch-up statement(s) from cluster state",
                catchup.len()
            );
            replay_ddl_statements(rt, catchup).await;
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Failed to read cluster state for DDL catch-up: {e}");
        }
    }

    Ok(())
}

pub(crate) async fn initialize_cluster_scheduler_future(
    rt: &Arc<Runtime>,
    scheduler_executor_registry: Arc<ExecutorRegistry>,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
) -> crate::Result<Option<Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'static>>>> {
    bootstrap_and_replay_ddl(rt).await?;
    initialize_cluster_scheduler(rt).await?;
    // Start internal cluster server for scheduler on separate port
    let internal_server_shutdown = CancellationToken::new();
    let cloned_shutdown = internal_server_shutdown.clone();
    let internal_server_rt = Arc::clone(rt);
    let internal_server_peers = Arc::clone(&scheduler_peers);
    let scheduler_executor_registry_clone = Arc::clone(&scheduler_executor_registry);
    let internal_server_fut = async move {
        start_internal_cluster_server(
            internal_server_rt,
            Some(cloned_shutdown),
            scheduler_executor_registry_clone,
            internal_server_peers,
        )
        .await
        .context(UnableToStartClusterServerSnafu)
    };
    let self_for_task = Arc::clone(rt);
    #[expect(clippy::type_complexity)]
    let mut futures: Vec<Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>> =
        vec![Box::pin(
            self_for_task
                .start_runtime_task(
                    CLUSTER_INTERNAL_SERVER,
                    Some(internal_server_shutdown),
                    internal_server_fut,
                )
                .await,
        )];

    let Some(app) = rt.read_app().await else {
        tracing::warn!(
            "No app found in runtime during cluster scheduler initialization; skipping scheduler registry and partition manager setup"
        );
        return Ok(None);
    };

    if let Some(config) = app.runtime.scheduler.clone() {
        if rt.partition_store().is_some() {
            // Validate all accelerated datasets/views have partition keys
            // for distributed partition assignment.
            partition::validate_partition_keys(&app).map_err(|e| {
                crate::Error::FailedToStartClusterScheduler {
                    source: Box::new(e),
                }
            })?;

            // Seed partition metadata for all accelerated tables. Requires the
            // PartitionService to have been wired onto `DataFusion` during
            // builder setup.
            let df = rt.datafusion();
            if let Some(partition_service) = df.partition_service.as_ref()
                && let Err(err) =
                    partition::initialize_partition_metadata(partition_service, &df, &app).await
            {
                tracing::warn!(
                    "Failed to initialize partition metadata during scheduler startup: {err}"
                );
            } else if df.partition_service.is_none() {
                tracing::warn!(
                    "PartitionService not initialized on DataFusion; skipping partition metadata seeding"
                );
            }

            // Start partition assignment task
            let pa_shutdown = CancellationToken::new();
            let pa_config = match PartitionAssignmentConfig::try_from(config.clone()) {
                Ok(cfg) => cfg,
                Err(err) => {
                    tracing::warn!(
                        "Failed to parse partition assignment config, partition assignment task will not be started: {err}"
                    );
                    return Ok(None);
                }
            };
            // Register partition_metadata as Initializing so `/v1/ready`
            // waits for metadata seeding to complete before reporting ready.
            rt.status
                .update_component_status("partition_metadata", ComponentStatus::Initializing);

            let pa_task = PartitionAssignmentTask::new(
                rt.datafusion(),
                Arc::clone(&rt.status),
                pa_config.interval,
                pa_shutdown.clone(),
            );

            futures.push(Box::pin(
                self_for_task
                    .start_runtime_task(
                        CLUSTER_PARTITION_ASSIGNMENT_TASK,
                        Some(pa_shutdown),
                        async move {
                            pa_task
                                .run()
                                .await
                                .boxed()
                                .context(FailedToRegisterSchedulerSnafu)
                        },
                    )
                    .await,
            ));
        }

        let registry_shutdown = CancellationToken::new();
        let registry_shutdown_for_task = registry_shutdown.clone();
        let peers = Arc::clone(&scheduler_peers);
        let self_ref = Arc::clone(rt);
        let cluster_state = rt.cluster_state();
        let heartbeats = rt.scheduler_heartbeats();
        let scheduler_registry_fut = self_for_task
            .start_runtime_task(
                CLUSTER_SCHEDULER_REGISTRY,
                Some(registry_shutdown_for_task),
                async move {
                    let (Some(cluster_state), Some(heartbeats)) = (cluster_state, heartbeats)
                    else {
                        return Err(crate::Error::FailedToRegisterScheduler {
                            source: Box::new(std::io::Error::other(
                                "cluster state store not initialized for scheduler role",
                            )),
                        });
                    };
                    start_scheduler_registry(
                        self_ref,
                        &config,
                        registry_shutdown.clone(),
                        peers,
                        cluster_state,
                        heartbeats,
                    )
                    .await
                    .map_err(|err| crate::Error::FailedToRegisterScheduler {
                        source: Box::new(err),
                    })
                },
            )
            .await;
        futures.push(Box::pin(scheduler_registry_fut));
    }

    Ok(Some(Box::pin(async move {
        try_join_all(futures).await.map(|_| ())
    })))
}

/// Creates a Ballista executor, binds it to the `Runtime` handle, and returns its configured
/// work loop as a future
pub async fn initialize_cluster_executor(
    rt: Arc<Runtime>,
    shutdown_token: CancellationToken,
) -> crate::Result<impl Future<Output = crate::Result<()>>> {
    let runtime_handle = Arc::clone(&rt);

    let runtime_producer: RuntimeProducer =
        Arc::new(move |_cfg| Ok(Arc::clone(&runtime_handle.df.ctx.runtime_env())));

    // Get scheduler URL - required for executors
    let Some(scheduler_url) = rt.df.cluster_config.scheduler_address() else {
        return Err(FailedToStartClusterExecutor {
            source: "--scheduler-address is required for executor mode"
                .to_string()
                .into(),
        });
    };

    let client_tls_config = rt.df.cluster_config.client_tls_config();
    let tls_enabled = client_tls_config.is_some();

    // Use the configured node_bind_address for the executor flight server.
    // Fall back to dynamic port assignment if binding fails (e.g., port already in use).
    let cluster_bind_addr = rt.df.cluster_config.node_bind_address();
    let bind_addr = if let Ok(bound_addr) = TcpListener::bind(cluster_bind_addr)
        .await
        .and_then(|l| l.local_addr())
    {
        bound_addr
    } else if let Ok(dynamic_addr) = TcpListener::bind((cluster_bind_addr.ip(), 0))
        .await
        .and_then(|l| l.local_addr())
    {
        tracing::warn!(
            "Unable to bind executor flight server to {cluster_bind_addr}, using dynamic port {dynamic_addr}"
        );
        dynamic_addr
    } else {
        return Err(FailedToStartClusterExecutor {
            source: format!(
                "Unable to bind executor Flight service to configured address ({cluster_bind_addr}) or fallback"
            )
            .into(),
        });
    };

    // Determine the advertise host and port for executor registration
    // node_advertise_address() returns host-only (port already stripped during config resolution)
    let (advertise_host, advertise_port) =
        if let Some(advertise_host) = rt.df.cluster_config.node_advertise_address() {
            (advertise_host.to_string(), bind_addr.port())
        } else {
            // Fall back to hostname and bind_addr port
            let hostname = gethostname::gethostname().into_string().map_err(|_| {
                FailedToStartClusterExecutor {
                    source: "Unable to determine executor hostname".to_string().into(),
                }
            })?;
            (hostname, bind_addr.port())
        };

    let executor_id = format!("{advertise_host}:{advertise_port}");

    // Fetch the app definition from the scheduler to get temp_directory for the work_dir.
    // This ensures shuffle files are written to the configured directory.
    let mut cluster_client =
        create_cluster_service_client(scheduler_url, client_tls_config.clone()).await?;

    let initial_scheduler_addresses =
        match cluster_client.get_schedulers(GetSchedulersRequest {}).await {
            Ok(response) => {
                let schedulers = response.into_inner().schedulers;
                let scheduler_addresses = schedulers
                    .iter()
                    .map(|scheduler| scheduler.advertise_address.clone())
                    .collect::<Vec<_>>();
                tracing::info!("Scheduler membership: {:?}", scheduler_addresses);
                scheduler_addresses
            }
            Err(status) => {
                tracing::warn!("Failed to get scheduler membership from scheduler: {status}");
                Vec::new()
            }
        };

    let app_definition_request = GetAppDefinitionRequest {
        executor_id: executor_id.clone(),
    };

    let response = cluster_client
        .get_app_definition(app_definition_request)
        .await
        .map_err(|status| FailedToStartClusterExecutor {
            source: format!("Failed to get app definition from scheduler: {status}").into(),
        })?;

    let get_app_response = response.into_inner();
    let app_json = get_app_response.app_json;
    let ddl_statements = get_app_response.ddl_statements;
    let ddl_version = get_app_response.ddl_version;

    let app_def: App = serde_json::from_str(&app_json)
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?;

    // Resolve executor settings from the scheduler's app definition before the
    // executor Flight server starts.
    if let Some(ref telemetry_config) = rt.telemetry_config {
        let _ = telemetry_config.set(app_def.runtime.telemetry.clone());
    }
    rt.rate_limits.set_flight_write_enabled(
        app_def
            .runtime
            .flight
            .clone()
            .unwrap_or_default()
            .do_put_rate_limit_enabled,
    );

    // Get shuffle_location from app params; if set to a path (not "memory"), use it as work_dir
    // Otherwise fall back to temp_directory from query config or system temp dir
    // Note: shuffle_memory_mode and object store config is set via the scheduler's override_session_builder
    let shuffle_location = app_def.runtime.params.get("shuffle_location");

    // Determine work_dir for executor:
    // - For "memory" mode or object store paths (s3://, abfs://), use temp_directory as fallback
    // - For local disk paths, use the specified path
    let work_dir = match shuffle_location.map(String::as_str) {
        Some("memory") => {
            // Memory mode: use temp_directory as fallback for any local work
            app_def
                .runtime
                .query
                .as_ref()
                .and_then(|q| q.temp_directory.clone())
                .unwrap_or_else(|| env::temp_dir().to_string_lossy().to_string())
        }
        Some(loc)
            if loc.starts_with("s3://")
                || loc.starts_with("abfs://")
                || loc.starts_with("az://") =>
        {
            // Object store mode: shuffle data goes to object store, but executor still needs local work_dir
            app_def
                .runtime
                .query
                .as_ref()
                .and_then(|q| q.temp_directory.clone())
                .unwrap_or_else(|| env::temp_dir().to_string_lossy().to_string())
        }
        Some(loc) => {
            // Local disk mode with explicit path
            // Validate the path exists or can be created
            let path = std::path::Path::new(loc);
            if !path.exists() {
                tracing::warn!(
                    "shuffle_location '{}' does not exist. Ensure the directory exists and is writable by the executor process.",
                    loc
                );
            }
            loc.to_string()
        }
        None => {
            // Default: use temp_directory
            app_def
                .runtime
                .query
                .as_ref()
                .and_then(|q| q.temp_directory.clone())
                .unwrap_or_else(|| env::temp_dir().to_string_lossy().to_string())
        }
    };

    // Log shuffle configuration
    // Normalize shuffle_format based on feature availability
    let raw_shuffle_format = app_def
        .runtime
        .params
        .get("shuffle_format")
        .map_or("arrow_ipc", String::as_str);

    #[cfg(feature = "vortex")]
    let shuffle_format = raw_shuffle_format;

    #[cfg(not(feature = "vortex"))]
    let shuffle_format = {
        if raw_shuffle_format == "vortex" {
            tracing::warn!(
                "Vortex shuffle format requested but 'vortex' feature is not enabled. Executor will use ArrowIpc."
            );
            "arrow_ipc"
        } else {
            raw_shuffle_format
        }
    };
    let shuffle_location_display = shuffle_location.map_or("disk (temp_directory)", String::as_str);
    tracing::info!(
        "Executor shuffle configuration: shuffle_format={}, shuffle_location={}, work_dir={}",
        shuffle_format,
        shuffle_location_display,
        work_dir
    );

    let app_def = Arc::new(app_def);

    let Some(concurrent_tasks) = std::thread::available_parallelism()
        .ok()
        .and_then(|nz| u32::try_from(nz.get()).ok())
    else {
        return Err(FailedToStartClusterExecutor {
            source: "Unable to determine executor task parallelism."
                .to_string()
                .into(),
        });
    };

    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        // flight service - use advertise address for scheduler to contact this executor
        host: Some(advertise_host.clone()),
        port: u32::from(advertise_port),
        // grpc_port is used only for push mode, and not initialized for pull mode (default)
        grpc_port: 0,
        specification: Some(ExecutorSpecification {
            resources: vec![ExecutorResource {
                resource: Some(Resource::TaskSlots(concurrent_tasks)),
            }],
        }),
    };

    // Use advertise address as node_id for metrics
    let metrics_node_id = format!("{advertise_host}:{advertise_port}");

    // Configure executor session config with shuffle locality metrics callback
    let config_producer_tls = client_tls_config.clone();
    let config_producer_node_id = metrics_node_id.clone();
    let config_producer: ConfigProducer = Arc::new(move || {
        let mut config = SessionConfig::new_with_ballista()
            .with_option_extension(SpiceClusterConfig::default())
            .with_ballista_use_tls(tls_enabled)
            // Use 100MB max message size to match other gRPC configurations in the codebase.
            // The default Ballista config is 16MB which is too small for shuffle operations
            // with large batches.
            .with_ballista_grpc_client_max_message_size(100 * 1024 * 1024)
            // Enable shuffle locality metrics callback to track local vs remote shuffle reads
            .with_ballista_shuffle_read_metrics_callback(
                metrics_collector::OtelShuffleReadMetricsCallback::new_arc(
                    config_producer_node_id.clone(),
                ),
            );

        if let Some(tls_config) = config_producer_tls.clone() {
            config = config.with_ballista_override_create_grpc_client_endpoint({
                Arc::new(move |ep| ep.tls_config(tls_config.clone()).boxed())
            });
        }

        config
    });

    let metrics_collector =
        metrics_collector::OtelExecutorMetricsCollector::new(metrics_node_id.clone());

    // Record task slots capacity for utilization metrics
    crate::metrics::cluster::set_executor_task_slots(&metrics_node_id, u64::from(concurrent_tasks));

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        Arc::new(BallistaFunctionRegistry::default()),
        Arc::new(metrics_collector),
        concurrent_tasks as usize,
        None,
    ));

    let codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> = BallistaCodec::new(
        SpiceLogicalCodec::new_codec(),
        SpicePhysicalCodec::new(Arc::clone(&rt))
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?,
    );

    rt.df
        .bind_executor(Arc::clone(&executor))
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?;

    let (tx_ready, rx_ready) = oneshot::channel::<String>();
    let readiness_sender = Arc::new(Mutex::new(Some(tx_ready)));

    // Create the shared semaphore for task slot management across all scheduler poll loops.
    // This semaphore will be passed to each poll loop so the busy state can be tracked
    // and shared across nodes in the scheduler shared state location metadata.
    let available_task_slots = Arc::new(tokio::sync::Semaphore::new(concurrent_tasks as usize));

    let scheduler_url_for_manager = scheduler_url.clone();
    let client_tls_config_for_manager = client_tls_config.clone();
    let executor_for_manager = Arc::clone(&executor);
    let codec_for_manager = codec;
    let initial_scheduler_addresses_for_manager = initial_scheduler_addresses.clone();
    let available_task_slots_for_manager = Arc::clone(&available_task_slots);

    let control_stream_executor_id = executor_id.clone();
    let control_stream_ballista_id = executor_id.clone();
    let control_stream_tls_config = client_tls_config.clone();
    let control_stream_initial_schedulers = initial_scheduler_addresses.clone();
    let control_stream_metrics_reader = rt.metrics_reader().cloned();
    let shutdown_token_for_manager = shutdown_token.clone();

    let partition_update_handler_rt = Arc::clone(&rt);
    let partition_update_handler: Option<
        crate::cluster::control_stream_client::PartitionUpdateHandler,
    > = Some(Arc::new(move |new_partitions, removed_partitions| {
        let rt = Arc::clone(&partition_update_handler_rt);
        Box::pin(async move {
            rt.update_partition_assignments(new_partitions, removed_partitions)
                .await;
        })
    }));

    let refresh_dataset_handler_rt = Arc::clone(&rt);
    let refresh_dataset_handler: Option<
        crate::cluster::control_stream_client::RefreshDatasetHandler,
    > = Some(Arc::new(move |dataset_name, overrides_json| {
        let rt = Arc::clone(&refresh_dataset_handler_rt);
        Box::pin(async move {
            let dataset_ref = ::datafusion::sql::TableReference::parse_str(&dataset_name);
            let overrides = overrides_json.and_then(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| {
                        tracing::warn!(
                            "Failed to deserialize refresh overrides for {dataset_name}: {e}"
                        );
                        e
                    })
                    .ok()
            });

            match rt.datafusion().refresh_table(&dataset_ref, overrides).await {
                Ok(_) => {
                    tracing::info!("Successfully triggered refresh for dataset '{dataset_name}'");
                }
                Err(e) => {
                    tracing::error!("Failed to refresh dataset '{dataset_name}': {e}");
                }
            }
        })
    }));

    // Thread to handle:
    //  - periodic refresh of scheduler membership
    //  - spawning/stopping scheduler poll loops as membership changes
    //  - managing control streams for metrics and PollNow commands
    let poll_manager = tokio::spawn(async move {
        let mut pollers: HashMap<String, SchedulerPollHandle> = HashMap::new();
        let mut known_schedulers: HashSet<String> = HashSet::new();

        // Initialize control stream manager for metrics collection
        let mut control_stream_manager = ControlStreamManager::new(
            control_stream_executor_id,
            control_stream_ballista_id,
            control_stream_tls_config,
            control_stream_metrics_reader,
            partition_update_handler,
            Some(Arc::clone(&executor_for_manager)),
            refresh_dataset_handler,
        );

        // Get the shared poll_now notify handle from the control stream manager.
        // When any scheduler sends a PollNow command, this will wake the poll loops.
        let poll_now_notify = control_stream_manager.poll_now_notify();

        let mut current_addresses = initial_scheduler_addresses_for_manager;
        if current_addresses.is_empty() {
            current_addresses.push(scheduler_url_for_manager.to_string());
        }

        let control_stream_addresses = if control_stream_initial_schedulers.is_empty() {
            vec![scheduler_url_for_manager.to_string()]
        } else {
            control_stream_initial_schedulers
        };
        control_stream_manager.update_schedulers(control_stream_addresses);

        update_scheduler_pollers(
            &mut pollers,
            &mut known_schedulers,
            current_addresses,
            client_tls_config_for_manager.as_ref(),
            &executor_for_manager,
            &codec_for_manager,
            &readiness_sender,
            Some(&poll_now_notify),
            &available_task_slots_for_manager,
        );

        let mut refresh = tokio::time::interval(SCHEDULER_REFRESH_INTERVAL);
        loop {
            tokio::select! {
                () = shutdown_token_for_manager.cancelled() => {
                    control_stream_manager
                        .notify_shutdown("runtime shutdown")
                        .await;
                    control_stream_manager.shutdown();
                    for (_, handle) in pollers.drain() {
                        handle.cancel.cancel();
                        let _ = handle.task.await;
                    }
                    break;
                }
                _ = refresh.tick() => {
                    if let Some(addresses) = fetch_scheduler_membership(
                        &scheduler_url_for_manager,
                        client_tls_config_for_manager.clone(),
                    )
                    .await
                    {
                        if addresses.is_empty() {
                            tracing::warn!(
                                "Scheduler membership refresh returned empty list; keeping existing schedulers"
                            );
                            continue;
                        }
                        // Update control streams with new scheduler membership
                        control_stream_manager.update_schedulers(addresses.clone());

                        update_scheduler_pollers(
                            &mut pollers,
                            &mut known_schedulers,
                            addresses,
                            client_tls_config_for_manager.as_ref(),
                            &executor_for_manager,
                            &codec_for_manager,
                            &readiness_sender,
                            Some(&poll_now_notify),
                            &available_task_slots_for_manager,
                        );
                    }
                }
            }
        }
    });

    Ok(async move {
        let _ = rx_ready
            .await
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        // Get initial allocation of Accelerated table partitions.
        // This also provides scheduler with executor_id to connect over FlightSQL to fetch partitions during SQL queries.
        //
        // This must be done after executor's flight service is ready to accept connections. Otherwise the scheduler will attempt to make connection and fail. Waiting until after `rx_ready` (which is done after the executor has established a network connection to the Scheduler's control plane), should give enough time for executor to bind locally for flight.
        let initial_partitions = executor_request_initial_partitions(
            cluster_client.clone(),
            rt.datafusion().cluster_config.node_advertise_url(),
            rt.datafusion().ctx.as_ref(),
        )
        .await
        .map_err(|status| FailedToStartClusterExecutor {
            source: format!("Failed to allocate initial partitions from scheduler: {status}")
                .into(),
        })?;
        tracing::debug!(
            "For executor={:?}, initial accelerated table partitions={:?}",
            rt.datafusion().cluster_config.node_advertise_url(),
            initial_partitions.clone()
        );
        rt.set_partition_assignments(initial_partitions).await;

        // Bind the already-fetched app and initialize secrets for object store configuration
        let executor_id_for_catchup = executor_id.clone();
        executor_bind_app(&rt, executor_id, app_def, client_tls_config).await?;

        // Replay DDL statements from the scheduler to create tables/schemas
        // that were added via DDL after cluster start (e.g. CREATE TABLE on a Cayenne catalog).
        if !ddl_statements.is_empty() {
            tracing::info!(
                "Replaying {} DDL statement(s) from scheduler (version {ddl_version})",
                ddl_statements.len(),
            );
            let stmts = deserialize_ddl_statements(&ddl_statements);
            replay_ddl_statements(&rt, &stmts).await;
        }

        // Catch up any DDL created between GetAppDefinition and now (TOCTOU window).
        match cluster_client
            .get_ddl_catchup(GetDdlCatchupRequest {
                executor_id: executor_id_for_catchup,
                since_version: ddl_version,
            })
            .await
        {
            Ok(response) => {
                let catchup = response.into_inner().ddl_statements;
                if !catchup.is_empty() {
                    tracing::info!(
                        "Replaying {} DDL catch-up statement(s) from scheduler",
                        catchup.len()
                    );
                    let stmts = deserialize_ddl_statements(&catchup);
                    replay_ddl_statements(&rt, &stmts).await;
                }
            }
            Err(e) => {
                tracing::warn!("Failed to get DDL catch-up from scheduler: {e}");
            }
        }

        executor_bind_object_stores(Arc::clone(&rt)).await?;

        rt.status.update_cluster("executor", ComponentStatus::Ready);

        poll_manager
            .await
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        Ok(())
    })
}

async fn create_scheduler_server(
    rt: &Arc<Runtime>,
) -> crate::Result<(
    SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    ExecutorControlStreamRegistry,
)> {
    let bind_addr = rt.df.cluster_config.node_bind_address();

    // Bind Spice Datafusion configuration incl SpiceQueryPlanner as bound in `DataFusionBuilder`
    let current_context = Arc::clone(&rt.df.ctx);
    let io_runtime = rt.tokio_io_runtime();

    // Get shuffle format from spicepod runtime params
    let shuffle_format: String = {
        let app_ref = rt.app();
        let app_guard = app_ref.read().await;
        app_guard
            .as_ref()
            .and_then(|app| app.runtime.params.get("shuffle_format"))
            .cloned()
            .unwrap_or_else(|| "arrow_ipc".to_string())
    };

    // Get shuffle_location from spicepod runtime params
    // "memory" = in-memory shuffle, otherwise path for disk shuffle (defaults to temp_directory)
    let shuffle_location: Option<String> = {
        let app_ref = rt.app();
        let app_guard = app_ref.read().await;
        app_guard
            .as_ref()
            .and_then(|app| app.runtime.params.get("shuffle_location"))
            .cloned()
    };
    let shuffle_memory_mode = shuffle_location.as_deref() == Some("memory");

    // Determine shuffle storage type and URL from shuffle_location
    // - "memory" -> in-memory shuffle (no storage_type/storage_url needed)
    // - "s3://..." -> S3 object store
    // - "abfs://..." or "az://..." -> Azure object store
    // - other path or None -> local disk storage
    let (shuffle_storage_type, shuffle_storage_url): (Option<String>, Option<String>) =
        match shuffle_location.as_deref() {
            Some("memory") | None => (None, None), // Memory mode or default - handled separately
            Some(loc) if loc.starts_with("s3://") => {
                (Some("s3".to_string()), Some(loc.to_string()))
            }
            Some(loc) if loc.starts_with("abfs://") || loc.starts_with("az://") => {
                (Some("azure".to_string()), Some(loc.to_string()))
            }
            Some(loc) => (Some("local".to_string()), Some(loc.to_string())), // Explicit local path
        };

    let client_tls_config = rt.df.cluster_config.client_tls_config();
    let override_create_grpc_client_endpoint: Option<SchedulerEndpointOverride> =
        client_tls_config.clone().map(|tls_config| {
            Arc::new(move |ep: Endpoint| {
                ep.tls_config(tls_config.clone())
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }) as _
        });

    // Convert shuffle_format param to ballista ShuffleFormat
    #[cfg(feature = "vortex")]
    let ballista_shuffle_format = match shuffle_format.as_str() {
        "vortex" => BallistaShuffleFormat::Vortex,
        _ => BallistaShuffleFormat::ArrowIpc,
    };

    #[cfg(not(feature = "vortex"))]
    let ballista_shuffle_format = {
        if shuffle_format.as_str() == "vortex" {
            tracing::warn!(
                "Vortex shuffle format requested but 'vortex' feature is not enabled. Falling back to ArrowIpc."
            );
        }
        BallistaShuffleFormat::ArrowIpc
    };

    // Create metrics collector with the scheduler's advertise address as node_id
    let metrics_node_id = rt
        .df
        .cluster_config
        .scheduler_url_string()
        .map_or_else(|| bind_addr.to_string(), ToString::to_string);
    let scheduler_metrics_collector = Arc::new(
        metrics_collector::OtelSchedulerMetricsCollector::new(metrics_node_id.clone()),
    );

    // Create the executor stream registry for PollNow broadcasts.
    // This registry will be shared with the ClusterServiceImpl.
    let executor_stream_registry = ExecutorControlStreamRegistry::new();

    // Create callback that broadcasts PollNow to all connected executors when work is available.
    let registry_for_callback = executor_stream_registry.clone();
    let on_work_available: Arc<dyn Fn(&str) + Send + Sync> =
        Arc::new(move |reason: &str| registry_for_callback.broadcast_poll_now(reason));

    let registry_for_cancel = executor_stream_registry.clone();
    let on_cancel_tasks: OnCancelTasksFn =
        Arc::new(move |executor_id: &str, tasks: Vec<RunningTaskInfo>| {
            let tasks_to_cancel = tasks
                .into_iter()
                .filter_map(|task| {
                    let Ok(task_id) = u32::try_from(task.task_id) else {
                        tracing::warn!(
                            executor_id,
                            task_id = task.task_id,
                            "Skipping cancel task with out-of-range task_id"
                        );
                        return None;
                    };

                    let Ok(stage_id) = u32::try_from(task.stage_id) else {
                        tracing::warn!(
                            executor_id,
                            stage_id = task.stage_id,
                            "Skipping cancel task with out-of-range stage_id"
                        );
                        return None;
                    };

                    let Ok(partition_id) = u32::try_from(task.partition_id) else {
                        tracing::warn!(
                            executor_id,
                            partition_id = task.partition_id,
                            "Skipping cancel task with out-of-range partition_id"
                        );
                        return None;
                    };

                    Some(TaskCancelInfo {
                        task_id,
                        job_id: task.job_id,
                        stage_id,
                        partition_id,
                    })
                })
                .collect::<Vec<_>>();

            if !registry_for_cancel.send_cancel_tasks(executor_id, tasks_to_cancel) {
                tracing::warn!(
                    "Failed to send cancel tasks to executor {executor_id}: no control stream"
                );
            }
        });

    // Create InMemoryClusterState first so we can reference it in the config_producer
    let cluster_state: Arc<dyn ClusterState> = Arc::new(InMemoryClusterState::default());

    // Create an atomic counter for total executor slots, updated by a background task
    // This allows session_builder to read the value synchronously without blocking
    let total_executor_slots = Arc::new(AtomicUsize::new(0));

    // Spawn background task to periodically update total executor slots from cluster state
    let cluster_state_for_slots = Arc::clone(&cluster_state);
    let slots_counter = Arc::clone(&total_executor_slots);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let metadata = cluster_state_for_slots.registered_executor_metadata().await;
            let total: usize = metadata
                .iter()
                .map(|m| m.specification.task_slots as usize)
                .sum();
            let prev = slots_counter.swap(total, Ordering::Relaxed);
            if total != prev {
                tracing::info!(
                    executor_count = metadata.len(),
                    total_slots = total,
                    "Cluster executor slots updated"
                );
            }
        }
    });

    // Create the session builder that will build SessionState from SessionConfig
    // Uses the dynamic total_executor_slots to set target_partitions
    let slots_for_session = Arc::clone(&total_executor_slots);
    let session_builder: ballista_scheduler::scheduler_server::SessionBuilder =
        Arc::new(move |_cfg| {
            // Get dynamic target_partitions based on cluster capacity
            let total_slots = slots_for_session.load(Ordering::Relaxed);
            let target_partitions = if total_slots > 0 { total_slots } else { 16 };

            tracing::debug!(
                total_slots,
                target_partitions,
                "Cluster session_builder: setting target_partitions based on cluster capacity"
            );

            let mut cfg = current_context
                .copied_config()
                .with_target_partitions(target_partitions)
                .with_option_extension(SpiceClusterConfig::default())
                .with_ballista_shuffle_format(ballista_shuffle_format)
                .with_ballista_shuffle_memory_mode(shuffle_memory_mode);

            // Apply object store shuffle configuration if specified
            if let Some(ref storage_type) = shuffle_storage_type {
                cfg = cfg.with_ballista_shuffle_storage_type(storage_type);
            }
            if let Some(ref storage_url) = shuffle_storage_url {
                cfg = cfg.with_ballista_shuffle_storage_url(storage_url);
            }

            // Filter out PartitionedTableScanRewrite from analyzer rules.
            // That rule rewrites TableScans into UNION ALL of FlightSQL partitions
            // for sync query distribution; Ballista handles distribution natively
            // for async queries, and FlightSqlExec has no codec support.
            let spice_state = current_context.as_ref().state();
            let distributed_analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = spice_state
                .analyzer()
                .rules
                .iter()
                .filter(|r| r.name() != "PartitionedTableScanRewrite")
                .map(Arc::clone)
                .collect();

            Ok(builder_from_existing(&spice_state)
                .with_config(cfg)
                .with_runtime_env(default_runtime_env(io_runtime.clone()))
                .with_analyzer_rules(distributed_analyzer_rules)
                .with_physical_optimizer_rules(datafusion_and_cluster_physical_optimizers())
                .build())
        });

    // Create config_producer that dynamically sets target_partitions based on cluster capacity
    // Reads from the atomic counter updated by the background task above
    let slots_for_config = Arc::clone(&total_executor_slots);
    let config_producer: ConfigProducer = Arc::new(move || {
        let total_slots = slots_for_config.load(Ordering::Relaxed);

        // Use total slots if executors have registered, otherwise fall back to default
        let target_partitions = if total_slots > 0 { total_slots } else { 16 };

        tracing::debug!(
            total_slots,
            target_partitions,
            "Cluster config_producer: setting target_partitions based on cluster capacity"
        );

        SessionConfig::new_with_ballista()
            .with_target_partitions(target_partitions)
            .with_option_extension(SpiceClusterConfig::default())
            .with_ballista_shuffle_format(ballista_shuffle_format)
            .with_ballista_shuffle_memory_mode(shuffle_memory_mode)
    });

    // Manually create the BallistaCluster with our custom config_producer
    let job_state = Arc::new(InMemoryJobState::new(
        metrics_node_id,
        session_builder,
        config_producer,
    ));
    let cluster = BallistaCluster::new(cluster_state, job_state);

    let scheduler_config = SchedulerConfig {
        bind_host: bind_addr.ip().to_string(),
        bind_port: bind_addr.port(),

        override_logical_codec: Some(SpiceLogicalCodec::new_with_runtime(Arc::clone(rt))),
        override_physical_codec: Some(
            SpicePhysicalCodec::new(Arc::clone(rt))
                .boxed()
                .context(FailedToStartClusterSchedulerSnafu)?,
        ),

        grpc_server_max_decoding_message_size: u32::MAX,
        grpc_server_max_encoding_message_size: u32::MAX,

        override_create_grpc_client_endpoint,
        override_metrics_collector: Some(scheduler_metrics_collector),
        on_work_available: Some(on_work_available),
        on_cancel_tasks: Some(on_cancel_tasks),

        // Faster failure detection: 30s timeout with 10s heartbeat interval
        executor_timeout_seconds: 30,

        // The Spice executor uses pull-based polling (execution_loop::poll_loop),
        // so the scheduler must use PullStaged to register executors via PollWork RPCs.
        scheduling_policy: ballista_core::config::TaskSchedulingPolicy::PullStaged,
        ..Default::default()
    };

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    let shuffle_location_display = shuffle_location
        .as_deref()
        .unwrap_or("disk (temp_directory)");
    tracing::info!(
        "Starting Ballista scheduler on {} (shuffle_format={}, shuffle_location={})",
        bind_addr,
        shuffle_format,
        shuffle_location_display
    );

    let scheduler = scheduler_process::create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(
        cluster,
        scheduler_config.into(),
    )
    .await
    .boxed()
    .context(FailedToStartClusterSchedulerSnafu)?;

    Ok((scheduler, executor_stream_registry))
}

/// Creates a gRPC client for the scheduler's internal cluster service.
async fn create_cluster_service_client(
    scheduler_url: &Url,
    client_tls_config: Option<ClientTlsConfig>,
) -> crate::Result<ClusterServiceClient<Channel>> {
    let endpoint_url = scheduler_url.to_string();
    let mut endpoint = Endpoint::from_shared(endpoint_url.clone())
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?;
    if let Some(tls_config) = client_tls_config {
        endpoint = endpoint
            .tls_config(tls_config)
            .map_err(|e| FailedToStartClusterExecutor {
                source: Box::new(e),
            })?;
    }

    let channel = endpoint
        .connect()
        .await
        .map_err(|e| FailedToStartClusterExecutor {
            source: format!(
                "Unable to connect to scheduler cluster service at {endpoint_url}: {e}"
            )
            .into(),
        })?;

    Ok(ClusterServiceClient::new(channel))
}

/// Wrapper struct that implements `ClusterSecretExpander` for the gRPC cluster client.
pub struct ClusterSecretExpanderImpl {
    client: ClusterServiceClient<Channel>,
}

impl ClusterSecretExpanderImpl {
    #[must_use]
    pub fn new(client: ClusterServiceClient<Channel>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl runtime_secrets::ClusterSecretExpander for ClusterSecretExpanderImpl {
    async fn expand_secret(
        &self,
        executor_id: &str,
        key: &str,
    ) -> Result<secrecy::SecretString, String> {
        let request = runtime_proto::ExpandSecretRequest {
            executor_id: executor_id.to_string(),
            key: key.to_string(),
        };

        let response = self
            .client
            .clone()
            .expand_secret(request)
            .await
            .map_err(|status| format!("Failed to expand secret from scheduler: {status}"))?;

        // Wrap at the earliest point we own the plaintext so downstream code
        // cannot accidentally stash it in a non-zeroizing buffer.
        Ok(secrecy::SecretString::from(response.into_inner().value))
    }
}

/// - Binds the pre-fetched `App` to the runtime
/// - Initializes and binds `SchedulerRPCSecretStore`
/// - Loads catalogs, embeddings, models, and tools
async fn executor_bind_app(
    rt: &Arc<Runtime>,
    executor_id: String,
    app_def: Arc<App>,
    client_tls_config: Option<ClientTlsConfig>,
) -> crate::Result<()> {
    let Some(scheduler_url) = rt.df.cluster_config.scheduler_address() else {
        return Err(FailedToStartClusterExecutor {
            source: "--scheduler-address is required for executor mode"
                .to_string()
                .into(),
        });
    };

    *rt.app.write().await = Some(app_def);

    // Create a cluster client for secrets
    let secrets_cluster_client =
        create_cluster_service_client(scheduler_url, client_tls_config).await?;

    let expander = Box::new(ClusterSecretExpanderImpl::new(secrets_cluster_client));
    *rt.secrets.write().await = Secrets::new_for_cluster_executor(expander, executor_id);

    Arc::clone(rt).load_catalogs().await;
    rt.load_embeddings().await;
    rt.load_rerankers().await;
    Arc::clone(rt).load_models().await;
    Arc::clone(rt).load_tools().await;
    Arc::clone(rt).load_datasets().await;

    Ok(())
}

/// Replays DDL SQL statements on the executor's local `DataFusion` context.
///
/// Statements are replayed in order. If any statement fails, remaining
/// statements are skipped because later DDL may depend on earlier ones
/// (e.g. `CREATE TABLE` depends on `CREATE SCHEMA`).
///
/// Uses the Spice `QueryBuilder` path (not `ctx.sql()` directly) so that
/// `DdlAnalyzerRule` runs — routing DDL through the correct Cayenne/Iceberg
/// physical-plan handlers rather than `DataFusion`'s built-in DDL handlers,
/// which don't know about custom catalogs and would fail with errors like
/// "failed to resolve schema" or "Registering new schemas is not supported".
///
/// Returns the number of successfully replayed statements.
async fn replay_ddl_statements(rt: &Runtime, statements: &[datafusion_ddl::DdlStatement]) -> usize {
    use futures::TryStreamExt as _;
    let df = rt.datafusion();
    for (i, stmt) in statements.iter().enumerate() {
        let sql = stmt.to_sql();
        let error: Option<String> = match df.query_builder(&sql).build().run().await {
            Err(e) => Some(e.to_string()),
            Ok(query_result) => query_result
                .data
                .try_collect::<Vec<_>>()
                .await
                .err()
                .map(|e| e.to_string()),
        };
        if let Some(e) = error {
            tracing::warn!(
                sql,
                "Failed to replay DDL statement ({}/{}) — skipping remaining: {e}",
                i + 1,
                statements.len()
            );
            return i;
        }
    }
    statements.len()
}

/// Deserializes a slice of JSON-encoded [`DdlStatement`] strings (as received
/// over gRPC) into structured [`DdlStatement`] values for replay.
/// Strings that fail to deserialize are warned about and skipped.
fn deserialize_ddl_statements(json_stmts: &[String]) -> Vec<datafusion_ddl::DdlStatement> {
    json_stmts
        .iter()
        .filter_map(
            |s| match serde_json::from_str::<datafusion_ddl::DdlStatement>(s) {
                Ok(stmt) => Some(stmt),
                Err(e) => {
                    tracing::warn!(
                        "Failed to deserialize DDL statement from scheduler: {e}; raw={s}"
                    );
                    None
                }
            },
        )
        .collect()
}

/// For each registered dataset on the cluster executor, asks its data
/// connector to register any object stores it needs against the executor's
/// runtime env.
///
/// On the executor, decoded `ParquetSource` (and other file-source) plans
/// arrive without their `parquet_file_reader_factory`, so `DataFusion` falls
/// back to `runtime_env().object_store(url)`. This function gives each
/// connector a chance to populate that registry using the dataset's
/// already-secret-expanded params.
async fn executor_bind_object_stores(rt: Arc<Runtime>) -> crate::Result<()> {
    let app = rt.app();
    let app = app.read().await;
    let Some(ref app) = *app else {
        return Err(FailedToStartClusterExecutor {
            source: "Runtime did not bind an App.".into(),
        });
    };
    let runtime_env = rt.df.ctx.runtime_env();
    for dataset in Arc::clone(&rt).get_valid_datasets(app, LogErrors(true)) {
        let connector = match Arc::clone(&rt)
            .get_dataconnector_from_dataset(Arc::clone(&dataset))
            .await
        {
            Ok(connector) => connector,
            Err(error) => {
                tracing::warn!(
                    "Skipping object store registration for dataset {}: {error}",
                    dataset.name
                );
                continue;
            }
        };

        if let Err(error) = connector
            .register_object_stores(&dataset, &runtime_env)
            .await
        {
            tracing::warn!(
                "Failed to register object stores for dataset {}: {error}",
                dataset.name
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ClusterTlsConfig;
    use bcder::{Mode, encode::Values, string::BitString};
    use bytes::Bytes;
    use chrono::{Duration, Utc};
    use tempfile::TempDir;
    use x509_certificate::asn1time::Time;
    use x509_certificate::rfc3280::Name;
    use x509_certificate::rfc5280;
    use x509_certificate::{
        CapturedX509Certificate, EcdsaCurve, InMemorySigningKeyPair, KeyAlgorithm, Sign, Signer,
        X509Certificate,
    };

    fn create_signed_certificate(
        subject_cn: &str,
        issuer_cn: &str,
        subject_key: &InMemorySigningKeyPair,
        issuer_key: &InMemorySigningKeyPair,
    ) -> CapturedX509Certificate {
        let mut subject = Name::default();
        subject
            .append_common_name_utf8_string(subject_cn)
            .expect("subject CN should be valid utf8");

        let mut issuer = Name::default();
        issuer
            .append_common_name_utf8_string(issuer_cn)
            .expect("issuer CN should be valid utf8");

        let not_before = Utc::now();
        let not_after = not_before + Duration::hours(1);

        let signature_algorithm = issuer_key
            .signature_algorithm()
            .expect("issuer key should have signature algorithm");
        let subject_key_algorithm = subject_key
            .key_algorithm()
            .expect("subject key should have key algorithm");

        let tbs_certificate = rfc5280::TbsCertificate {
            version: Some(rfc5280::Version::V3),
            serial_number: 1.into(),
            signature: signature_algorithm.into(),
            issuer,
            validity: rfc5280::Validity {
                not_before: Time::from(not_before),
                not_after: Time::from(not_after),
            },
            subject,
            subject_public_key_info: rfc5280::SubjectPublicKeyInfo {
                algorithm: subject_key_algorithm.into(),
                subject_public_key: BitString::new(0, subject_key.public_key_data()),
            },
            issuer_unique_id: None,
            subject_unique_id: None,
            extensions: None,
            raw_data: None,
        };

        let mut tbs_der = Vec::new();
        tbs_certificate
            .encode_ref()
            .write_encoded(Mode::Der, &mut tbs_der)
            .expect("tbs certificate should encode");

        let signature = issuer_key
            .try_sign(&tbs_der)
            .expect("issuer key should sign certificate");
        let signature_algorithm = issuer_key
            .signature_algorithm()
            .expect("issuer key should have signature algorithm");

        let cert = rfc5280::Certificate {
            tbs_certificate,
            signature_algorithm: signature_algorithm.into(),
            signature: BitString::new(0, Bytes::copy_from_slice(signature.as_ref())),
        };

        let cert = X509Certificate::from(cert);
        let cert_der = cert.encode_der().expect("certificate should encode");
        CapturedX509Certificate::from_der(cert_der).expect("certificate should parse")
    }

    fn write_cert(path: &std::path::Path, cert: &CapturedX509Certificate) {
        std::fs::write(path, cert.encode_pem()).expect("certificate should write");
    }

    fn write_key(path: &std::path::Path, key: &InMemorySigningKeyPair) {
        let key_der = key.to_pkcs8_one_asymmetric_key_der();
        let key_pem = pem::Pem::new("PRIVATE KEY", key_der.as_slice().to_vec());
        std::fs::write(path, key_pem.to_string()).expect("key should write");
    }

    fn generate_key() -> InMemorySigningKeyPair {
        InMemorySigningKeyPair::generate_random(KeyAlgorithm::Ecdsa(EcdsaCurve::Secp256r1))
            .expect("key generation should succeed")
    }

    fn install_crypto_provider() {
        // The new ClusterTlsConfig builds a `rustls::ServerConfig`, which
        // requires a process-wide CryptoProvider. Tests may run in any
        // order so install idempotently.
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
    }

    #[test]
    fn cluster_tls_config_accepts_valid_node_certificate() {
        install_crypto_provider();
        let temp_dir = TempDir::new().expect("temp dir should create");
        let ca_key = generate_key();
        let ca_cert = create_signed_certificate("Spice Test CA", "Spice Test CA", &ca_key, &ca_key);

        let node_key = generate_key();
        let node_cert =
            create_signed_certificate("Spice Test Node", "Spice Test CA", &node_key, &ca_key);

        let ca_path = temp_dir.path().join("ca.pem");
        let node_cert_path = temp_dir.path().join("node.pem");
        let node_key_path = temp_dir.path().join("node.key");

        write_cert(&ca_path, &ca_cert);
        write_cert(&node_cert_path, &node_cert);
        write_key(&node_key_path, &node_key);

        let control = crate::tls::TlsControl::new().expect("watcher");
        ClusterTlsConfig::try_new(
            ca_path.to_str().expect("ca path should be utf8"),
            node_cert_path
                .to_str()
                .expect("node cert path should be utf8"),
            node_key_path
                .to_str()
                .expect("node key path should be utf8"),
            &control,
        )
        .expect("valid certificates should be accepted");
    }

    #[test]
    fn cluster_tls_config_rejects_mismatched_issuer_name() {
        install_crypto_provider();
        let temp_dir = TempDir::new().expect("temp dir should create");
        let ca_key = generate_key();
        let ca_cert = create_signed_certificate("Spice Test CA", "Spice Test CA", &ca_key, &ca_key);

        let node_key = generate_key();
        let node_cert =
            create_signed_certificate("Spice Test Node", "Other CA", &node_key, &ca_key);

        let ca_path = temp_dir.path().join("ca.pem");
        let node_cert_path = temp_dir.path().join("node.pem");
        let node_key_path = temp_dir.path().join("node.key");

        write_cert(&ca_path, &ca_cert);
        write_cert(&node_cert_path, &node_cert);
        write_key(&node_key_path, &node_key);

        let control = crate::tls::TlsControl::new().expect("watcher");
        let err = ClusterTlsConfig::try_new(
            ca_path.to_str().expect("ca path should be utf8"),
            node_cert_path
                .to_str()
                .expect("node cert path should be utf8"),
            node_key_path
                .to_str()
                .expect("node key path should be utf8"),
            &control,
        )
        .expect_err("mismatched issuer should be rejected");

        assert!(
            err.to_string()
                .contains("was not issued by the provided CA"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cluster_tls_config_rejects_invalid_signature() {
        install_crypto_provider();
        let temp_dir = TempDir::new().expect("temp dir should create");
        let ca_key = generate_key();
        let ca_cert = create_signed_certificate("Spice Test CA", "Spice Test CA", &ca_key, &ca_key);

        let node_key = generate_key();
        let bad_signing_key = generate_key();
        let node_cert = create_signed_certificate(
            "Spice Test Node",
            "Spice Test CA",
            &node_key,
            &bad_signing_key,
        );

        let ca_path = temp_dir.path().join("ca.pem");
        let node_cert_path = temp_dir.path().join("node.pem");
        let node_key_path = temp_dir.path().join("node.key");

        write_cert(&ca_path, &ca_cert);
        write_cert(&node_cert_path, &node_cert);
        write_key(&node_key_path, &node_key);

        let control = crate::tls::TlsControl::new().expect("watcher");
        let err = ClusterTlsConfig::try_new(
            ca_path.to_str().expect("ca path should be utf8"),
            node_cert_path
                .to_str()
                .expect("node cert path should be utf8"),
            node_key_path
                .to_str()
                .expect("node key path should be utf8"),
            &control,
        )
        .expect_err("invalid signature should be rejected");

        assert!(
            err.to_string().contains("signature verification failed"),
            "unexpected error: {err}"
        );
    }
}
