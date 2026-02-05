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

use crate::Error::{FailedToStartClusterExecutor, FailedToStartClusterScheduler};
use crate::cluster::datafusion::datafusion_and_cluster_physical_optimizers;
use crate::config::{ClusterConfig, ClusterRole};
use crate::dataconnector::listing;
use crate::dataconnector::parameters::ConnectorParamsBuilder;
use crate::jobs::JobExecutor;
use crate::status::ComponentStatus;
use crate::{
    CLUSTER_INTERNAL_SERVER, CLUSTER_SCHEDULER_REGISTRY, FailedToStartClusterExecutorSnafu,
    FailedToStartClusterSchedulerSnafu, LogErrors, Runtime, UnableToStartClusterServerSnafu,
};
use ::datafusion::error::DataFusionError;
use ::datafusion::execution::SessionStateBuilder;
use ::datafusion::prelude::SessionConfig;
use ::datafusion::sql::TableReference;
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
use ballista_core::utils::create_grpc_client_endpoint;
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_executor::execution_loop;
use ballista_executor::executor::Executor;
use ballista_scheduler::cluster::memory::{InMemoryClusterState, InMemoryJobState};
use ballista_scheduler::cluster::{BallistaCluster, ClusterState};
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::codec::spice_logical_codec::SpiceLogicalCodec;
use datafusion::codec::spice_physical_codec::SpicePhysicalCodec;
use datafusion_datasource::ListingTableUrl;
use datafusion_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use runtime_datafusion::config::cluster_config::SpiceClusterConfig;
use runtime_object_store::registry::default_runtime_env;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use runtime_proto::{
    AllocateInitialPartitionsRequest, GetAppDefinitionRequest, GetSchedulersRequest,
};
use runtime_secrets::Secrets;
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};
use x509_certificate::CapturedX509Certificate;
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
    },
    Executor,
}

impl DistributedNode {
    #[must_use]
    pub fn is_scheduler(&self) -> bool {
        matches!(self, DistributedNode::Scheduler { .. })
    }

    #[must_use]
    pub fn is_executor(&self) -> bool {
        matches!(self, DistributedNode::Executor)
    }
}

type SchedulerEndpointOverride =
    Arc<dyn Fn(Endpoint) -> Result<Endpoint, tonic::transport::Error> + Send + Sync>;

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
                    let scheduler_endpoint = match create_grpc_client_endpoint(endpoint_url.clone())
                    {
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

mod composite_flight_service;
mod control_stream_client;
pub mod datafusion;
mod executor_registry;
pub mod metrics_collector;
mod scheduler_registry;
mod servers;
mod service;

pub use control_stream_client::ControlStreamManager;
pub use executor_registry::ExecutorRegistry;
pub use scheduler_registry::start_scheduler_registry;
pub use scheduler_registry::{SchedulerPeers, SchedulerRecord};
pub use servers::{start_executor_flight_server, start_internal_cluster_server};
pub use service::{ClusterServiceImpl, ExecutorControlStreamRegistry};

/// mTLS configuration for cluster communications.
///
/// This holds the loaded certificates and keys for both server and client TLS,
/// enabling mutual TLS authentication between cluster nodes.
#[derive(Debug, Clone)]
pub struct ClusterTlsConfig {
    /// CA certificate used to validate other cluster nodes
    pub ca_certificate: Certificate,
    /// Client TLS config with CA and client identity for mTLS
    pub client_tls_config: ClientTlsConfig,
    /// Server identity (cert + key) for serving TLS
    pub server_identity: Identity,
}

impl ClusterTlsConfig {
    /// Creates a new `ClusterTlsConfig` by loading the CA, certificate, and key files.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the files cannot be read.
    pub fn try_new(ca_cert_path: &str, cert_path: &str, key_path: &str) -> std::io::Result<Self> {
        let ca_cert_pem = std::fs::read(ca_cert_path)?;
        let cert_pem = std::fs::read(cert_path)?;
        let key_pem = std::fs::read(key_path)?;

        let ca_x509 = CapturedX509Certificate::from_pem(&ca_cert_pem).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse cluster CA certificate at {ca_cert_path}: {err}"),
            )
        })?;
        let node_x509 = CapturedX509Certificate::from_pem(&cert_pem).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse cluster node certificate at {cert_path}: {err}"),
            )
        })?;

        let ca_name = ca_x509.subject_name().user_friendly_str().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Failed to read subject name from cluster CA certificate at {ca_cert_path}: {err}"
                ),
            )
        })?;
        let node_issuer = node_x509.issuer_name().user_friendly_str().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Failed to read issuer name from cluster node certificate at {cert_path}: {err}"
                ),
            )
        })?;

        let node_cn = node_x509
            .subject_common_name()
            .unwrap_or_else(|| "unknown".to_string());

        tracing::info!(
            "Cluster mTLS configured with CA {ca_name} and node certificate CN {node_cn}"
        );

        if node_issuer != ca_name {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "The node certificate was not issued by the provided CA, expected {ca_name} but found issuer {node_issuer}"
                ),
            ));
        }

        if let Err(err) = node_x509.verify_signed_by_certificate(&ca_x509) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "The node certificate was not issued by the provided CA, signature verification failed for issuer {node_issuer}: {err}"
                ),
            ));
        }

        let ca_certificate = Certificate::from_pem(&ca_cert_pem);

        // Client TLS config with mTLS: CA for server validation + client identity
        let client_tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&ca_cert_pem))
            .identity(Identity::from_pem(&cert_pem, &key_pem));

        // Server identity for TLS
        let server_identity = Identity::from_pem(&cert_pem, &key_pem);

        Ok(Self {
            ca_certificate,
            client_tls_config,
            server_identity,
        })
    }
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
        // Cluster mTLS configuration must be complete when provided
        let tls_config = match (
            &config.node_mtls_ca_certificate_file,
            &config.node_mtls_certificate_file,
            &config.node_mtls_key_file,
        ) {
            (Some(ca_path), Some(cert_path), Some(key_path)) => {
                Some(ClusterTlsConfig::try_new(ca_path, cert_path, key_path)?)
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

    /// Returns the client TLS config for connecting to other cluster nodes.
    #[must_use]
    pub fn client_tls_config(&self) -> Option<&ClientTlsConfig> {
        self.tls_config.as_ref().map(|t| &t.client_tls_config)
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
        .map_err(|e| FailedToStartClusterScheduler {
            source: Box::new(e),
        })?;

    rt.df
        .bind_executor_stream_registry(executor_stream_registry)
        .map_err(|e| FailedToStartClusterScheduler {
            source: Box::new(e),
        })?;

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    Ok(())
}

pub(crate) async fn initialize_cluster_scheduler_future(
    rt: &Arc<Runtime>,
    scheduler_executor_registry: Arc<ExecutorRegistry>,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
) -> crate::Result<Option<Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'static>>>> {
    initialize_cluster_scheduler(rt).await?;
    // Start internal cluster server for scheduler on separate port
    let internal_server_shutdown = CancellationToken::new();
    let cloned_shutdown = internal_server_shutdown.clone();
    let internal_server_rt = Arc::clone(rt);
    let internal_server_peers = Arc::clone(&scheduler_peers);
    let internal_server_fut = async move {
        start_internal_cluster_server(
            internal_server_rt,
            Some(cloned_shutdown),
            Arc::clone(&scheduler_executor_registry),
            internal_server_peers,
        )
        .await
        .context(UnableToStartClusterServerSnafu)
    };
    let self_for_task = Arc::clone(rt);
    let internal_server_future = self_for_task
        .start_runtime_task(
            CLUSTER_INTERNAL_SERVER,
            Some(internal_server_shutdown),
            internal_server_fut,
        )
        .await;

    let scheduler_registry_future = {
        let app = rt.app.read().await;
        let config = app.as_ref().and_then(|app| app.runtime.scheduler.clone());
        if let Some(config) = config {
            let registry_shutdown = CancellationToken::new();
            let registry_shutdown_for_task = registry_shutdown.clone();
            let peers = Arc::clone(&scheduler_peers);
            let self_ref = Arc::clone(rt);
            let registry_task = async move {
                start_scheduler_registry(self_ref, &config, registry_shutdown.clone(), peers)
                    .await
                    .map_err(|err| crate::Error::FailedToRegisterScheduler {
                        source: Box::new(err),
                    })
            };
            Some(
                self_for_task
                    .start_runtime_task(
                        CLUSTER_SCHEDULER_REGISTRY,
                        Some(registry_shutdown_for_task),
                        registry_task,
                    )
                    .await,
            )
        } else {
            None
        }
    };

    let cluster_future = async move {
        if let Some(registry_future) = scheduler_registry_future {
            tokio::try_join!(internal_server_future, registry_future).map(|_| ())
        } else {
            internal_server_future.await
        }
    };

    Ok(Some(Box::pin(cluster_future)))
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

    let client_tls_config = rt.df.cluster_config.client_tls_config().cloned();
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

    let app_json = response.into_inner().app_json;

    let app_def: App = serde_json::from_str(&app_json)
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?;

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
        let _ = cluster_client
            .allocate_initial_partitions(AllocateInitialPartitionsRequest {
                executor_id: rt.datafusion().cluster_config.node_advertise_url(),
            })
            .await
            .map_err(|status| FailedToStartClusterExecutor {
                source: format!("Failed to allocate initial partitions from scheduler: {status}")
                    .into(),
            })?
            .into_inner()
            .table_partitions
            .into_iter()
            .map(|(table_id, partitions)| {
                Ok((
                    TableReference::parse_str(&table_id),
                    partitions
                        .items
                        .into_iter()
                        .map(|e| Expr::from_bytes(&e.into_bytes()))
                        .collect::<Result<Vec<Expr>, _>>()?,
                ))
            })
            .collect::<Result<HashMap<TableReference, Vec<Expr>>, DataFusionError>>()
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        // Bind the already-fetched app and initialize secrets for object store configuration
        executor_bind_app(&rt, executor_id, app_def, client_tls_config).await?;

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

    let client_tls_config = rt.df.cluster_config.client_tls_config().cloned();
    let override_create_grpc_client_endpoint: Option<SchedulerEndpointOverride> = client_tls_config
        .clone()
        .map(|tls_config| Arc::new(move |ep: Endpoint| ep.tls_config(tls_config.clone())) as _);

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

            Ok(
                SessionStateBuilder::new_from_existing(current_context.as_ref().state())
                    .with_config(cfg)
                    .with_runtime_env(default_runtime_env(io_runtime.clone()))
                    .with_physical_optimizer_rules(datafusion_and_cluster_physical_optimizers())
                    .build(),
            )
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

        // Faster failure detection: 30s timeout with 10s heartbeat interval
        executor_timeout_seconds: 30,
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
    async fn expand_secret(&self, executor_id: &str, key: &str) -> Result<String, String> {
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

        Ok(response.into_inner().value)
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
    Arc::clone(rt).load_models().await;
    Arc::clone(rt).load_tools().await;

    Ok(())
}

/// Traverses dataset definitions and reifies `ListingTableUrl`s, triggering object store
/// registration for each.
async fn executor_bind_object_stores(rt: Arc<Runtime>) -> crate::Result<()> {
    let app = rt.app();
    let app = app.read().await;
    let Some(ref app) = *app else {
        return Err(FailedToStartClusterExecutor {
            source: "Runtime did not bind an App.".into(),
        });
    };
    for dataset in Arc::clone(&rt).get_valid_datasets(app, LogErrors(true)) {
        let mut params = ConnectorParamsBuilder::new(dataset.source().into(), (&dataset).into())
            .build(Arc::clone(&rt.secrets), rt.tokio_io_runtime())
            .await
            .context(FailedToStartClusterExecutorSnafu)?;

        // Either this is a URL with a scheme, or a URL with a connector name prefixing it
        let url = match dataset.from.as_str().split_once(':') {
            Some((_, rest)) if !rest.starts_with("//") => rest,
            _ => dataset.from.as_str(),
        };

        let Ok(mut parsed) = Url::parse(url) else {
            tracing::warn!("Unable to configure Dataset URL {}", url);
            continue;
        };

        if parsed.scheme() == "file" {
            tracing::warn!(
                "Dataset {} has a file:// scheme and may not be resolvable without a shared mount.",
                dataset.name
            );
            continue;
        }

        // Not all connectors have the same parameter structures for S3 -- this makes all fragment
        // keys match the spec expected by the S3 connector and `SpiceObjectRegistry`.
        params.parameters.canonicalize_s3_fragments();

        // Canonicalize Azure parameters (e.g., `azure_storage_account_name` -> `account`)
        // for Delta Lake and other connectors that use Azure-prefixed parameter names.
        params.parameters.canonicalize_azure_fragments();

        // Canonicalize GCS parameters (e.g., `google_service_account` -> `service_account`)
        // for Delta Lake and other connectors that use GCS-prefixed parameter names.
        params.parameters.canonicalize_gcs_fragments();

        let unprefixed = params
            .parameters
            .into_iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>();

        parsed.set_fragment(Some(
            listing::build_fragments(&params.parameters, unprefixed).as_str(),
        ));

        let listing_table_url = ListingTableUrl::parse(parsed)
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        let _ = rt
            .df
            .ctx
            .runtime_env()
            .object_store(listing_table_url)
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        tracing::info!("Configured object storage for Dataset {}", dataset.name);
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

    #[test]
    fn cluster_tls_config_accepts_valid_node_certificate() {
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

        ClusterTlsConfig::try_new(
            ca_path.to_str().expect("ca path should be utf8"),
            node_cert_path
                .to_str()
                .expect("node cert path should be utf8"),
            node_key_path
                .to_str()
                .expect("node key path should be utf8"),
        )
        .expect("valid certificates should be accepted");
    }

    #[test]
    fn cluster_tls_config_rejects_mismatched_issuer_name() {
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

        let err = ClusterTlsConfig::try_new(
            ca_path.to_str().expect("ca path should be utf8"),
            node_cert_path
                .to_str()
                .expect("node cert path should be utf8"),
            node_key_path
                .to_str()
                .expect("node key path should be utf8"),
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

        let err = ClusterTlsConfig::try_new(
            ca_path.to_str().expect("ca path should be utf8"),
            node_cert_path
                .to_str()
                .expect("node cert path should be utf8"),
            node_key_path
                .to_str()
                .expect("node key path should be utf8"),
        )
        .expect_err("invalid signature should be rejected");

        assert!(
            err.to_string().contains("signature verification failed"),
            "unexpected error: {err}"
        );
    }
}
