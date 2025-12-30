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
use crate::status::ComponentStatus;
use crate::{
    FailedToStartClusterExecutorSnafu, FailedToStartClusterSchedulerSnafu, LogErrors, Runtime,
};
use ::datafusion::execution::SessionStateBuilder;
use ::datafusion::prelude::SessionConfig;
use app::App;
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
use ballista_executor::metrics::LoggingMetricsCollector;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::codec::spice_logical_codec::SpiceLogicalCodec;
use datafusion::codec::spice_physical_codec::SpicePhysicalCodec;
use datafusion_datasource::ListingTableUrl;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::TryFutureExt;
use runtime_datafusion::config::cluster_config::SpiceClusterConfig;
use runtime_object_store::registry::default_runtime_env;
use runtime_proto::GetAppDefinitionRequest;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use runtime_secrets::Secrets;
use snafu::ResultExt;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

type SchedulerEndpointOverride =
    Arc<dyn Fn(Endpoint) -> Result<Endpoint, tonic::transport::Error> + Send + Sync>;

pub mod datafusion;
mod servers;
mod service;

pub use servers::{start_executor_flight_server, start_internal_cluster_server};
pub use service::ClusterServiceImpl;

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

        // Validate cluster role requirements
        if is_cluster_role {
            if tls_config.is_none() && !config.insecure {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Cluster mode requires mTLS configuration or the --insecure flag. Provide --node-mtls-ca-certificate-file, --node-mtls-certificate-file, and --node-mtls-key-file, or use --insecure.",
                ));
            }
            if config.node_advertise_address.is_none() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Multi-node clusters require --node-advertise-address. Set this to the hostname or IP address other cluster nodes can use to reach this node.",
                ));
            }
        }

        // Pre-compute scheduler URL from advertise address
        let scheme = if tls_config.is_some() {
            "https"
        } else {
            "http"
        };
        let scheduler_url = config
            .node_advertise_address
            .as_ref()
            .map(|addr| format!("{scheme}://{addr}"));

        Ok(Self {
            config,
            tls_config,
            scheduler_url,
        })
    }

    /// Returns the cluster role.
    #[must_use]
    pub fn role(&self) -> Option<&ClusterRole> {
        self.config.role.as_ref()
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
    #[must_use]
    pub fn scheduler_address(&self) -> Option<&Url> {
        self.config.scheduler_address.as_ref()
    }

    /// Returns the scheduler URL as a string for use in Ballista configuration.
    ///
    /// This is constructed from the advertise address during initialization.
    /// Returns `None` if advertise address was not configured.
    #[must_use]
    pub fn scheduler_url_string(&self) -> Option<&str> {
        self.scheduler_url.as_deref()
    }

    /// Returns the advertise address.
    #[must_use]
    pub fn node_advertise_address(&self) -> Option<&str> {
        self.config.node_advertise_address.as_deref()
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
    pub fn insecure(&self) -> bool {
        self.config.insecure
    }

    /// Returns the client TLS config for connecting to other cluster nodes.
    #[must_use]
    pub fn client_tls_config(&self) -> Option<&ClientTlsConfig> {
        self.tls_config.as_ref().map(|t| &t.client_tls_config)
    }

    /// Creates a new `ResolvedClusterConfig` from a `ClusterConfig`.
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS configuration is invalid or files cannot be read.
    pub fn from_config_and_app(config: ClusterConfig, _app: Option<&App>) -> std::io::Result<Self> {
        Self::try_new(config)
    }
}

/// Creates & binds a Ballista scheduler to the Runtime handle, then updates status
pub async fn initialize_cluster_scheduler(rt: &Arc<Runtime>) -> crate::Result<()> {
    let scheduler = create_scheduler_server(rt).await?;

    rt.df
        .bind_scheduler_server(Arc::new(scheduler))
        .map_err(|e| FailedToStartClusterScheduler {
            source: Box::new(e),
        })?;

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    Ok(())
}

/// Creates a Ballista executor, binds it to the `Runtime` handle, and returns its configured
/// work loop as a future
pub async fn initialize_cluster_executor(
    rt: Arc<Runtime>,
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
    let config_producer_tls = client_tls_config.clone();

    // Configure mTLS for executor-to-executor gRPC connections (e.g., shuffle fetch)
    let config_producer: ConfigProducer = Arc::new(move || {
        let mut config = SessionConfig::new_with_ballista()
            .with_option_extension(SpiceClusterConfig::default())
            .with_ballista_use_tls(tls_enabled);

        if let Some(tls_config) = config_producer_tls.clone() {
            config = config.with_ballista_override_create_grpc_client_endpoint({
                Arc::new(move |ep| ep.tls_config(tls_config.clone()).boxed())
            });
        }

        config
    });

    let work_dir = rt
        .df
        .temp_directory
        .clone()
        .unwrap_or(env::temp_dir().to_string_lossy().to_string());

    let scheduler_endpoint = create_grpc_client_endpoint(scheduler_url.to_string())
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?;
    let scheduler_endpoint = if let Some(tls_config) = client_tls_config.clone() {
        scheduler_endpoint
            .tls_config(tls_config)
            .map_err(|e| FailedToStartClusterExecutor {
                source: Box::new(e),
            })?
    } else {
        scheduler_endpoint
    };

    let scheduler_connection =
        scheduler_endpoint
            .connect()
            .await
            .map_err(|e| FailedToStartClusterExecutor {
                source: format!("Unable to connect to scheduler at {scheduler_url}: {e}").into(),
            })?;

    let scheduler = SchedulerGrpcClient::new(scheduler_connection)
        .max_encoding_message_size(usize::MAX)
        .max_decoding_message_size(usize::MAX);

    // Try to bind the same flight port Spice usually does, but if we cannot, bind a different
    // port to allow for easy local deployments
    let bind_addr = if let Ok(flight_bind_addr) = TcpListener::bind(rt.config.flight_bind_address)
        .await
        .and_then(|l| l.local_addr())
    {
        flight_bind_addr
    } else if let Ok(dynamic_addr) = TcpListener::bind("127.0.0.1:0")
        .await
        .and_then(|l| l.local_addr())
    {
        dynamic_addr
    } else {
        return Err(FailedToStartClusterExecutor {
            source: format!(
                "Unable to bind Flight service to configured address ({}) or fallback",
                rt.config.flight_bind_address
            )
            .into(),
        });
    };

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

    // Determine the advertise host and port for executor registration
    let (advertise_host, advertise_port) = if let Some(advertise_addr) =
        rt.df.cluster_config.node_advertise_address()
    {
        // Parse the advertise address (format: "host:port" or "[ipv6]:port")
        if let Ok(socket_addr) = advertise_addr.parse::<SocketAddr>() {
            (socket_addr.ip().to_string(), socket_addr.port())
        } else if let Some((host_part, port_part)) = advertise_addr.rsplit_once(':') {
            let port = port_part
                .parse::<u16>()
                .map_err(|_| FailedToStartClusterExecutor {
                    source: format!("Invalid port in --node-advertise-address: {advertise_addr}")
                        .into(),
                })?;
            let host = host_part.trim_matches(['[', ']']).to_string();
            (host, port)
        } else {
            return Err(FailedToStartClusterExecutor {
                    source: format!(
                        "Invalid --node-advertise-address format: {advertise_addr}. Expected 'host:port' (IPv6 must be in [addr]:port form)"
                    )
                    .into(),
                });
        }
    } else {
        // Fall back to hostname and bind_addr port
        let hostname =
            gethostname::gethostname()
                .into_string()
                .map_err(|_| FailedToStartClusterExecutor {
                    source: "Unable to determine executor hostname".to_string().into(),
                })?;
        (hostname, bind_addr.port())
    };

    let executor_id = Uuid::new_v4().to_string();
    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        // flight service - use advertise address for scheduler to contact this executor
        host: Some(advertise_host),
        port: u32::from(advertise_port),
        // grpc_port is used only for push mode, and not initialized for pull mode (default)
        grpc_port: 0,
        specification: Some(ExecutorSpecification {
            resources: vec![ExecutorResource {
                resource: Some(Resource::TaskSlots(concurrent_tasks)),
            }],
        }),
    };

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        Arc::new(BallistaFunctionRegistry::default()),
        Arc::new(LoggingMetricsCollector::default()),
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

    let executor_poll_loop = tokio::spawn(
        execution_loop::poll_loop(scheduler, Arc::clone(&executor), codec, Some(tx_ready)).map_err(
            |e| FailedToStartClusterExecutor {
                source: Box::new(e),
            },
        ),
    );

    Ok(async move {
        let _ = rx_ready
            .await
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        // Initialize secrets first so they're available for object store configuration
        executor_bind_app(&rt, executor_id, client_tls_config).await?;

        executor_bind_object_stores(Arc::clone(&rt)).await?;

        rt.status.update_cluster("executor", ComponentStatus::Ready);

        executor_poll_loop
            .await
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?
    })
}

async fn create_scheduler_server(
    rt: &Arc<Runtime>,
) -> crate::Result<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
    let bind_addr = rt.df.cluster_config.node_bind_address();

    // Bind Spice Datafusion configuration incl SpiceQueryPlanner as bound in `DataFusionBuilder`
    let current_context = Arc::clone(&rt.df.ctx);
    let io_runtime = rt.tokio_io_runtime();

    let client_tls_config = rt.df.cluster_config.client_tls_config().cloned();
    let override_create_grpc_client_endpoint: Option<SchedulerEndpointOverride> = client_tls_config
        .clone()
        .map(|tls_config| Arc::new(move |ep: Endpoint| ep.tls_config(tls_config.clone())) as _);

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

        override_session_builder: Some(Arc::new(move |_cfg| {
            let cfg = current_context
                .copied_config()
                .with_option_extension(SpiceClusterConfig::default());

            Ok(
                SessionStateBuilder::new_from_existing(current_context.as_ref().state())
                    .with_config(cfg)
                    .with_runtime_env(default_runtime_env(io_runtime.clone()))
                    .with_physical_optimizer_rules(datafusion_and_cluster_physical_optimizers())
                    .build(),
            )
        })),
        override_create_grpc_client_endpoint,
        ..Default::default()
    };

    let cluster = BallistaCluster::new_from_config(&scheduler_config)
        .await
        .boxed()
        .context(FailedToStartClusterSchedulerSnafu)?;

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    tracing::info!("Starting Ballista scheduler on {}", bind_addr);

    scheduler_process::create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(
        cluster,
        scheduler_config.into(),
    )
    .await
    .boxed()
    .context(FailedToStartClusterSchedulerSnafu)
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

/// - Initializes relevant `App` runtime components retrieved from the scheduler node
/// - Initializes and binds `SchedulerRPCSecretStore`
async fn executor_bind_app(
    rt: &Arc<Runtime>,
    executor_id: String,
    client_tls_config: Option<ClientTlsConfig>,
) -> crate::Result<()> {
    let Some(scheduler_url) = rt.df.cluster_config.scheduler_address() else {
        return Err(FailedToStartClusterExecutor {
            source: "--scheduler-address is required for executor mode"
                .to_string()
                .into(),
        });
    };
    let mut cluster_client =
        create_cluster_service_client(scheduler_url, client_tls_config.clone()).await?;

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

    *rt.app.write().await = Some(Arc::new(app_def));

    // Create a new cluster client for secrets
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
