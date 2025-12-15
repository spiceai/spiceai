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
use crate::config::{ClusterConfig, ClusterMode};
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
use flight_client::arrow_flight_factory::make_arrow_flight_client;
use futures::{StreamExt, TryFutureExt};
use prost::Message;
use runtime_datafusion::config::cluster_config::SpiceClusterConfig;
use runtime_object_store::registry::default_runtime_env;
use runtime_proto::GetAppDefinitionRequest;
use runtime_secrets::Secrets;
use snafu::ResultExt;
use spicepod::component::runtime::{ApiKey, ApiKeyAuth, Auth};
use std::env;
use std::sync::{Arc, OnceLock};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::{Certificate, ClientTlsConfig};
use url::Url;
use uuid::Uuid;

pub mod datafusion;

/// Cluster configuration with lazily loaded TLS config.
///
/// This struct wraps `ClusterConfig` and caches the `ClientTlsConfig` on first access
/// to avoid reading the CA certificate file on every query.
#[derive(Debug)]
pub struct ResolvedClusterConfig {
    config: ClusterConfig,
    /// Cached client TLS config, loaded lazily from `cluster_ca_certificate_file`.
    client_tls_config: OnceLock<Option<ClientTlsConfig>>,
}

impl ResolvedClusterConfig {
    /// Creates a new `ResolvedClusterConfig` from the given `ClusterConfig`, eagerly loading
    /// the TLS configuration if a CA certificate file is specified.
    ///
    /// # Errors
    ///
    /// Returns an error if the CA certificate file cannot be read.
    pub fn try_new(config: ClusterConfig) -> std::io::Result<Self> {
        let client_tls_config = OnceLock::new();

        // Eagerly load the TLS config if a CA certificate file is specified
        if let Some(ref ca_path) = config.cluster_ca_certificate_file {
            let ca_certificate = std::fs::read(ca_path)?;
            let tls_config =
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_certificate));
            // This cannot fail since we just created the OnceLock
            let _ = client_tls_config.set(Some(tls_config));
        } else {
            let _ = client_tls_config.set(None);
        }

        Ok(Self {
            config,
            client_tls_config,
        })
    }

    /// Returns the cluster mode.
    #[must_use]
    pub fn mode(&self) -> Option<&ClusterMode> {
        self.config.mode.as_ref()
    }

    /// Returns the scheduler URL.
    #[must_use]
    pub fn scheduler_url(&self) -> &Url {
        &self.config.scheduler_url
    }

    /// Returns the cluster API key.
    #[must_use]
    pub fn cluster_api_key(&self) -> Option<&String> {
        self.config.cluster_api_key.as_ref()
    }

    /// Returns whether insecure connections are allowed.
    #[must_use]
    pub fn allow_insecure_connections(&self) -> bool {
        self.config.allow_insecure_connections
    }

    /// Returns the path to the CA certificate file.
    #[must_use]
    pub fn cluster_ca_certificate_file(&self) -> Option<&String> {
        self.config.cluster_ca_certificate_file.as_ref()
    }

    /// Returns the cached client TLS config, if configured.
    #[must_use]
    pub fn client_tls_config(&self) -> Option<&ClientTlsConfig> {
        self.client_tls_config
            .get()
            .and_then(std::option::Option::as_ref)
    }

    /// Creates a new `ResolvedClusterConfig` from a `ClusterConfig`, optionally merging
    /// the API key from the app's auth configuration if no cluster API key is already set.
    ///
    /// # Errors
    ///
    /// Returns an error if the CA certificate file cannot be read.
    pub fn from_config_and_app(
        mut config: ClusterConfig,
        app: Option<&App>,
    ) -> std::io::Result<Self> {
        // If no cluster API key is set, try to use the app's auth API key
        if config.cluster_api_key.is_none()
            && let Some(api_key) = app
                .and_then(|a| a.runtime.auth.as_ref())
                .and_then(|a| a.api_key.as_ref())
                .and_then(|ak| {
                    if ak.enabled {
                        ak.keys.first().cloned()
                    } else {
                        None
                    }
                })
        {
            let (ApiKey::ReadOnly { key } | ApiKey::ReadWrite { key }) = api_key;
            config.cluster_api_key = Some(key);
        }

        Self::try_new(config)
    }
}

impl Default for ResolvedClusterConfig {
    fn default() -> Self {
        let client_tls_config = OnceLock::new();
        let _ = client_tls_config.set(None);
        Self {
            config: ClusterConfig::default(),
            client_tls_config,
        }
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

    let config_producer: ConfigProducer = Arc::new(move || {
        SessionConfig::new_with_ballista().with_option_extension(SpiceClusterConfig::default())
    });

    let work_dir = rt
        .df
        .temp_directory
        .clone()
        .unwrap_or(env::temp_dir().to_string_lossy().to_string());

    let mut scheduler_endpoint =
        create_grpc_client_endpoint(rt.config.cluster.scheduler_url.clone().to_string())
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

    let maybe_client_tls_config = runtime_tls_configuration(rt.as_ref()).await?;

    if let Some(tls_config) = &maybe_client_tls_config {
        scheduler_endpoint = scheduler_endpoint
            .tls_config(tls_config.clone())
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;
    }

    let scheduler_connection =
        scheduler_endpoint
            .connect()
            .await
            .map_err(|_| FailedToStartClusterExecutor {
                source: format!(
                    "Unable to connect to scheduler at {}",
                    rt.config.cluster.scheduler_url
                )
                .into(),
            })?;

    let Some(api_key) = rt.config.cluster.cluster_api_key.clone() else {
        return Err(FailedToStartClusterExecutor {
            source: "Unable to start executor without an API key".into(),
        });
    };

    let interceptor = move |mut req: tonic::Request<()>| {
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {api_key}")
                .parse()
                .map_err(|_| tonic::Status::invalid_argument("Invalid API key"))?,
        );

        Ok(req)
    };

    let scheduler = SchedulerGrpcClient::with_interceptor(scheduler_connection, interceptor)
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

    let Ok(hostname) = gethostname::gethostname().into_string() else {
        return Err(FailedToStartClusterExecutor {
            source: "Unable to determine executor hostname".to_string().into(),
        });
    };

    let executor_id = Uuid::new_v4().to_string();
    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        // flight service
        host: Some(hostname),
        port: u32::from(bind_addr.port()),
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
        executor_bind_app(
            &rt,
            rt.config.cluster.scheduler_url.to_string(),
            executor_id,
            maybe_client_tls_config,
        )
        .await?;

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
    let bind_addr = rt.config.flight_bind_address;

    // Bind Spice Datafusion configuration incl SpiceQueryPlanner as bound in `DataFusionBuilder`
    let current_context = Arc::clone(&rt.df.ctx);
    let io_runtime = rt.tokio_io_runtime();

    let maybe_client_tls_config = runtime_tls_configuration(rt.as_ref()).await?;

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
        override_create_grpc_client_endpoint: Some(Arc::new(move |ep| {
            if let Some(ref tls_config) = maybe_client_tls_config {
                ep.tls_config(tls_config.clone())
            } else {
                Ok(ep)
            }
        })),
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

/// - Initializes relevant `App` runtime components retrieved from the scheduler node
/// - Initializes and binds `SchedulerRPCSecretStore`
async fn executor_bind_app(
    rt: &Arc<Runtime>,
    scheduler_flight_url: String,
    executor_id: String,
    client_tls_config: Option<ClientTlsConfig>,
) -> crate::Result<()> {
    let Some(api_key) = rt.config.cluster.cluster_api_key.clone() else {
        return Err(FailedToStartClusterExecutor {
            source: "Unable to start executor without an API key".into(),
        });
    };

    let mut flight_client = make_arrow_flight_client(
        &scheduler_flight_url,
        Some(api_key.clone()),
        client_tls_config,
    )
    .await
    .boxed()
    .context(FailedToStartClusterExecutorSnafu)?;

    let app_definition_request = GetAppDefinitionRequest {
        executor_id: executor_id.clone(),
    };

    let action = arrow_flight::Action {
        r#type: "GetAppDefinition".to_string(),
        body: bytes::Bytes::from(app_definition_request.encode_to_vec()),
    };

    let response = flight_client
        .do_action(action)
        .await
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?
        .next()
        .await;

    if let Some(Ok(bytes)) = response {
        let mut app_def: App = serde_json::from_slice(&bytes)
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        app_def.runtime.auth = Some(Auth {
            api_key: Some(ApiKeyAuth {
                enabled: true,
                keys: vec![ApiKey::ReadOnly { key: api_key }],
            }),
        });

        *rt.app.write().await = Some(Arc::new(app_def));
    }

    *rt.secrets.write().await = Secrets::new_for_cluster_executor(flight_client, executor_id);

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

async fn runtime_tls_configuration(rt: &Runtime) -> crate::Result<Option<ClientTlsConfig>> {
    if let Some(ref ca_path) = rt.config.cluster.cluster_ca_certificate_file {
        let ca_certificate = tokio::fs::read(ca_path)
            .await
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;
        Ok(Some(
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_certificate)),
        ))
    } else {
        Ok(None)
    }
}
