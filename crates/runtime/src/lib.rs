/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
#![allow(clippy::missing_errors_doc)]

use async_stream::stream;
use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;

use crate::{
    auth::EndpointAuth, dataconnector::DataConnector, datafusion::DataFusion,
    internal_table::Error as InternalTableError, model::ENABLE_MODEL_SUPPORT_MESSAGE,
};

use ::datafusion::error::DataFusionError;
use ::datafusion::sql::{TableReference, sqlparser};
use app::App;
use builder::RuntimeBuilder;
use cancellable_task::{CancellableTaskHandle, spawn_cancellable_task};
use config::Config;
use dataconnector::ConnectorComponent;
use datasets_health_monitor::DatasetsHealthMonitor;
use extension::ExtensionFactory;
use flight::RateLimits;
use futures::Stream;
use futures::future::{join_all, try_join_all};
#[cfg(feature = "openapi")]
pub use http::get_api_doc;
use model::{EmbeddingModelStore, EvalScorerRegistry, LLMModelStore};

use crate::tools::{SpiceModelTool, with_name};
use model_components::model::Model;
pub use notify::Error as NotifyError;
use secrecy::SecretString;
use secrets::{ParamStr, Secrets};
use snafu::prelude::*;
use spicepod::component::eval::Eval;
use status::ComponentStatus;
use tls::TlsConfig;
use tokio::sync::{RwLock, oneshot::error::RecvError};
use tokio_util::sync::CancellationToken;
use tools::factory::default_available_catalogs;
use tools::{Tooling, catalog::SpiceToolCatalog};
pub use util::shutdown_signal;

use crate::extension::Extension;
pub mod accelerated_table;
pub mod auth;
mod builder;
mod cancellable_task;
pub mod catalogconnector;
pub mod component;
pub mod config;
pub mod dataaccelerator;
pub mod dataconnector;
pub mod datafusion;
pub mod datasets_health_monitor;
pub mod dataupdate;
pub mod embeddings;
pub mod execution_plan;
pub mod extension;
pub mod federated_table;
pub mod flight;
mod http;
mod init;
pub mod internal_table;
mod metrics;
mod metrics_server;
pub mod model;
pub mod object_store_registry;
pub mod objectstore;
mod opentelemetry;
pub mod parameters;
pub mod podswatcher;
pub mod request;
pub mod secrets;
pub mod spice_metrics;
pub mod status;
pub mod task_history;
pub mod timing;
pub mod tls;
pub mod tools;
pub mod topological_ordering;
pub(crate) mod tracers;
mod tracing_util;
mod view;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: http::Error },

    #[snafu(display(
        "Task execution failed: {source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToExecuteTask { source: tokio::task::JoinError },

    #[snafu(display("Unable to start Prometheus metrics server: {source}"))]
    UnableToStartMetricsServer { source: metrics_server::Error },

    #[snafu(display("Unable to start Flight server: {source}"))]
    UnableToStartFlightServer { source: flight::Error },

    #[snafu(display("Unable to start OpenTelemetry server: {source}"))]
    UnableToStartOpenTelemetryServer { source: opentelemetry::Error },

    #[snafu(display("Unknown data source: {data_source}"))]
    UnknownDataSource { data_source: String },

    #[snafu(display("Unable to create data backend: {source}"))]
    UnableToCreateBackend { source: datafusion::Error },

    #[snafu(display("Unable to attach view: {source}"))]
    UnableToAttachView { source: datafusion::Error },

    #[snafu(display("Failed to start pods watcher: {source}"))]
    UnableToInitializePodsWatcher { source: NotifyError },

    #[snafu(display("{source}"))]
    UnableToInitializeDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToInitializeCatalogConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToInitializeLlm {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToInitializeEmbeddingModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToInitializeLlmTool {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unknown data connector: {data_connector}.\nSpecify a valid data connector and retry. For details, visit: https://spiceai.org/docs/components/data-connectors"
    ))]
    UnknownDataConnector { data_connector: String },

    #[snafu(display(
        "Unknown catalog connector: {catalog_connector}.\nSpecify a valid catalog connector and retry. For details, visit: https://spiceai.org/docs/components/catalogs"
    ))]
    UnknownCatalogConnector { catalog_connector: String },

    #[snafu(display(
        "The runtime is built without ODBC support.\nBuild Spice.ai OSS with the `odbc` feature enabled or use the Docker image that includes ODBC support.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/odbc"
    ))]
    OdbcNotInstalled,

    #[snafu(display("Unable to load secrets for data connector: {data_connector}"))]
    UnableToLoadDataConnectorSecrets { data_connector: String },

    #[snafu(display("Unable to get secret for data connector {data_connector}: {source}"))]
    UnableToGetSecretForDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
        data_connector: String,
    },

    #[snafu(display("Unable to get secret for LLM: {source}"))]
    UnableToGetSecretForLLM {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to setup the {connector_component} ({data_connector}).\n{source}"))]
    UnableToAttachDataConnector {
        source: datafusion::Error,
        connector_component: ConnectorComponent,
        data_connector: String,
    },

    #[snafu(display("Unable to load SQL file {file}: {source}"))]
    UnableToLoadSqlFile {
        file: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to parse SQL: {source}"))]
    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Unable to create view: {reason}"))]
    UnableToCreateView { reason: String },

    #[snafu(display(
        "Specify the SQL string for view {name} using either `sql: SELECT * FROM...` inline or as a file reference with `sql_ref: my_view.sql`"
    ))]
    NeedToSpecifySQLView { name: String },

    #[snafu(display(
        "An accelerated table was configured as read_write without setting replication.enabled = true"
    ))]
    AcceleratedReadWriteTableWithoutReplication,

    #[snafu(display(
        "An accelerated table for {dataset_name} was configured with 'refresh_mode = changes', but the data connector doesn't support a changes stream."
    ))]
    AcceleratedTableInvalidChanges { dataset_name: String },

    #[snafu(display("Expected acceleration settings for {name}, found None"))]
    ExpectedAccelerationSettings { name: String },

    #[snafu(display(
        "The accelerator engine {name} is not available. Valid engines are arrow, duckdb, sqlite, and postgres."
    ))]
    AcceleratorEngineNotAvailable { name: String },

    #[snafu(display("The accelerator engine {name} failed to initialize: {source}"))]
    AcceleratorInitializationFailed {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Dataset names should not include a catalog. Unexpected '{}' in '{}'. Remove '{}' from the dataset name and try again.",
        catalog,
        name,
        catalog,
    ))]
    DatasetNameIncludesCatalog { catalog: Arc<str>, name: Arc<str> },

    #[snafu(display("Unable to load dataset connector: {dataset}"))]
    UnableToLoadDatasetConnector { dataset: TableReference },

    #[snafu(display("Unable to load data connector for catalog {catalog}: {source}"))]
    UnableToLoadCatalogConnector {
        catalog: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("The data connector {dataconnector} doesn't support catalogs."))]
    DataConnectorDoesntSupportCatalogs { dataconnector: String },

    #[snafu(display("Unable to create accelerated table: {dataset}, {source}"))]
    UnableToCreateAcceleratedTable {
        dataset: TableReference,
        source: datafusion::Error,
    },

    #[snafu(display("Unable to receive accelerated table status: {source}"))]
    UnableToReceiveAcceleratedTableStatus { source: RecvError },

    #[snafu(display("Unable to start local metrics: {source}"))]
    UnableToStartLocalMetrics { source: spice_metrics::Error },

    #[snafu(display("Unable to track task history: {source}"))]
    UnableToTrackTaskHistory { source: task_history::Error },

    #[snafu(display("Unable to create metrics table: {source}"))]
    UnableToCreateMetricsTable { source: DataFusionError },

    #[snafu(display("Unable to create eval runs table: {source}"))]
    UnableToCreateEvalRunsTable { source: InternalTableError },

    #[snafu(display("Unable to register metrics table: {source}"))]
    UnableToRegisterMetricsTable { source: datafusion::Error },

    #[snafu(display("Invalid dataset defined in Spicepod: {source}"))]
    InvalidSpicepodDataset {
        source: crate::component::dataset::Error,
    },

    #[snafu(display("Invalid glob pattern {pattern}: {source}"))]
    InvalidGlobPattern {
        pattern: String,
        source: globset::Error,
    },

    #[snafu(display("Error converting GlobSet to Regex: {source}"))]
    ErrorConvertingGlobSetToRegex { source: globset::Error },

    #[snafu(display("Unable to create directory: {source}"))]
    UnableToCreateDirectory { source: std::io::Error },

    #[snafu(display("{source}"))]
    ComponentError { source: component::Error },

    #[snafu(display("{source}"))]
    ComponentsInitializationFailed { source: tokio::task::JoinError },

    #[snafu(display("Initialization has been cancelled"))]
    ComponentsInitializationCancelled,
}

const HTTP_SERVER: &str = "http_server";
const METRICS_SERVER: &str = "metrics_server";
const FLIGHT_SERVER: &str = "flight_server";
const OPENTELEMETRY_SERVER: &str = "opentelemetry_server";
const PODS_WATCHER: &str = "pods_watcher";
const COMPONENTS_INITIAL_LOAD: &str = "components_initial_load";

// Allow 30 seconds for tasks for graceful shutdown
const RUNTIME_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy)]
pub struct LogErrors(pub bool);

#[derive(Clone)]
pub struct Runtime {
    app: Arc<RwLock<Option<Arc<App>>>>,
    df: Arc<DataFusion>,
    models: Arc<RwLock<HashMap<String, Model>>>,
    llms: Arc<RwLock<LLMModelStore>>,
    embeds: Arc<RwLock<EmbeddingModelStore>>,
    tools: Arc<RwLock<HashMap<String, Tooling>>>,
    evals: Arc<RwLock<Vec<Eval>>>,
    eval_scorers: EvalScorerRegistry,
    pods_watcher: Arc<RwLock<Option<podswatcher::PodsWatcher>>>,
    secrets: Arc<RwLock<secrets::Secrets>>,
    datasets_health_monitor: Option<Arc<DatasetsHealthMonitor>>,
    metrics_endpoint: Option<SocketAddr>,
    prometheus_registry: Option<prometheus::Registry>,
    rate_limits: Arc<RateLimits>,

    autoload_extensions: Arc<HashMap<String, Box<dyn ExtensionFactory>>>,
    extensions: Arc<RwLock<HashMap<String, Arc<dyn Extension>>>>,
    spaced_tracer: Arc<tracers::SpacedTracer>,

    status: Arc<status::RuntimeStatus>,

    runtime_tasks: Arc<RwLock<HashMap<String, CancellableTaskHandle>>>,
}

impl Runtime {
    #[must_use]
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::new()
    }

    #[must_use]
    pub fn datafusion(&self) -> Arc<DataFusion> {
        Arc::clone(&self.df)
    }

    #[must_use]
    pub fn secrets(&self) -> Arc<RwLock<secrets::Secrets>> {
        Arc::clone(&self.secrets)
    }

    #[must_use]
    pub fn status(&self) -> Arc<status::RuntimeStatus> {
        Arc::clone(&self.status)
    }

    #[must_use]
    pub fn embeds(&self) -> Arc<RwLock<EmbeddingModelStore>> {
        Arc::clone(&self.embeds)
    }

    #[must_use]
    pub fn app(&self) -> Arc<RwLock<Option<Arc<App>>>> {
        Arc::clone(&self.app)
    }

    /// Requests a loaded extension, or will attempt to load it if part of the autoloaded extensions.
    pub async fn extension(&self, name: &str) -> Option<Arc<dyn Extension>> {
        let extensions = self.extensions.read().await;

        if let Some(extension) = extensions.get(name) {
            return Some(Arc::clone(extension));
        }
        drop(extensions);

        if let Some(autoload_factory) = self.autoload_extensions.get(name) {
            let mut extensions = self.extensions.write().await;
            let mut extension = autoload_factory.create();
            let extension_name = extension.name().to_string();
            if let Err(err) = extension.initialize(self).await {
                tracing::error!("Unable to initialize extension {extension_name}: {err}");
                return None;
            }

            if let Err(err) = extension.on_start(self).await {
                tracing::error!("Unable to start extension {extension_name}: {err}");
                return None;
            }

            extensions.insert(extension_name.clone(), extension.into());
            return extensions.get(&extension_name).cloned();
        }

        None
    }

    /// Starts the HTTP, Flight, OpenTelemetry and Metrics servers all listening on the ports specified in the given `Config`.
    ///
    /// The future returned by this function drives the individual server futures and will only return once the servers are shutdown.
    ///
    /// It is recommended to start the servers in parallel to loading the Runtime components to speed up startup.
    #[allow(clippy::too_many_lines)]
    pub async fn start_servers(
        self: Arc<Self>,
        config: Config,
        tls_config: Option<Arc<TlsConfig>>,
        endpoint_auth: EndpointAuth,
    ) -> Result<()> {
        self.register_metrics_table(self.prometheus_registry.is_some())
            .await?;

        // Start Http server
        let cloned_tls_config = tls_config.clone();
        let cloned_config = config.clone();
        let http_auth = endpoint_auth.http_auth.clone();
        let self_ref = Arc::clone(&self);
        let http_shutdown = CancellationToken::new();

        let http_future = self
            .start_runtime_task(HTTP_SERVER, Some(http_shutdown.clone()), async move {
                http::start(
                    cloned_config.http_bind_address,
                    self_ref,
                    cloned_config.into(),
                    cloned_tls_config,
                    http_auth,
                    Some(http_shutdown),
                )
                .await
                .context(UnableToStartHttpServerSnafu)
            })
            .await;

        // Start Metrics server
        let metrics_endpoint = self.metrics_endpoint;
        let prometheus_registry = self.prometheus_registry.clone();
        let cloned_tls_config = tls_config.clone();

        let metrics_future = self
            .start_runtime_task(METRICS_SERVER, None, async move {
                metrics_server::start(metrics_endpoint, prometheus_registry, cloned_tls_config)
                    .await
                    .context(UnableToStartMetricsServerSnafu)
            })
            .await;

        // Start Flight server
        let flight_shutdown = CancellationToken::new();
        let self_ref = Arc::clone(&self);
        let cloned_tls_config = tls_config.clone();
        let cloned_endpoint_auth = endpoint_auth.clone();
        let cloned_app_ref = self_ref.app.read().await.as_ref().map(Arc::clone);

        let flight_future = self
            .start_runtime_task(FLIGHT_SERVER, Some(flight_shutdown.clone()), async move {
                flight::start(
                    config.flight_bind_address,
                    cloned_app_ref,
                    Arc::clone(&self_ref.df),
                    cloned_tls_config,
                    cloned_endpoint_auth,
                    Arc::clone(&self_ref.rate_limits),
                    Some(flight_shutdown),
                )
                .await
                .context(UnableToStartFlightServerSnafu)
            })
            .await;

        // Start OpenTelemetry server
        let opentelemetry_graceful_shutdown = CancellationToken::new();
        let df_ref = Arc::clone(&self.df);
        let cloned_tls_config = tls_config.clone();
        let grpc_auth = endpoint_auth.grpc_auth.clone();

        let opentelemetry_future = self
            .start_runtime_task(
                OPENTELEMETRY_SERVER,
                Some(opentelemetry_graceful_shutdown.clone()),
                async move {
                    opentelemetry::start(
                        config.open_telemetry_bind_address,
                        df_ref,
                        cloned_tls_config,
                        grpc_auth,
                        Some(opentelemetry_graceful_shutdown),
                    )
                    .await
                    .context(UnableToStartOpenTelemetryServerSnafu)
                },
            )
            .await;

        if let Some(tls_config) = tls_config {
            match tls_config.subject_name() {
                Some(subject_name) => {
                    tracing::info!("Endpoints secured with TLS using certificate: {subject_name}");
                }
                None => {
                    tracing::info!("Endpoints secured with TLS");
                }
            }
        }

        // Start Spicepod watcher
        let self_ref = Arc::clone(&self);
        let pods_watcher_future = self
            .start_runtime_task(PODS_WATCHER, None, async move {
                self_ref
                    .start_pods_watcher()
                    .await
                    .context(UnableToInitializePodsWatcherSnafu)
            })
            .await;

        // Shutdown signal
        let shutdown_signal_future = async {
            shutdown_signal().await;
            tracing::debug!("Shutdown signal received.");
            self.shutdown().await;
            Ok(())
        };

        // wait for all servers to shut down or if any of the servers fail to start
        match tokio::try_join!(
            http_future,
            flight_future,
            metrics_future,
            opentelemetry_future,
            pods_watcher_future,
            shutdown_signal_future
        ) {
            Err(err) => Err(err),
            _ => Ok(()),
        }
    }

    /// Updates all of the component statuses to `Initializing`.
    pub async fn set_components_initializing(&self) {
        let app_lock = self.app.read().await;
        let Some(app) = app_lock.as_ref() else {
            return;
        };

        let valid_datasets = Self::get_valid_datasets(app, LogErrors(false));
        for ds in &valid_datasets {
            self.status
                .update_dataset(&ds.name, ComponentStatus::Initializing);
        }

        if cfg!(feature = "models") {
            for embedding in &app.embeddings {
                self.status
                    .update_embedding(&embedding.name, ComponentStatus::Initializing);
            }

            for model in &app.models {
                self.status
                    .update_model(&model.name, ComponentStatus::Initializing);
            }

            for tool in &app.tools {
                self.status
                    .update_tool(&tool.name, ComponentStatus::Initializing);
            }

            for tool_catalog in default_available_catalogs() {
                self.status
                    .update_tool_catalog(tool_catalog.name(), ComponentStatus::Initializing);
            }
        }

        let valid_catalogs = Self::get_valid_catalogs(app, LogErrors(false));
        for catalog in valid_catalogs {
            self.status
                .update_catalog(&catalog.name, ComponentStatus::Initializing);
        }

        let valid_views = Self::get_valid_views(app, LogErrors(false));
        for view in valid_views {
            self.status
                .update_view(&view.name, ComponentStatus::Initializing);
        }
    }

    /// Will load all of the components of the Runtime, including `secret_stores`, `catalogs`, `datasets`, `models`, and `embeddings`.
    ///
    /// The future returned by this function will not resolve until all components have been loaded and marked as ready.
    /// This includes waiting for the first refresh of any accelerated tables to complete.
    #[allow(clippy::too_many_lines)]
    pub async fn load_components(self: Arc<Self>) {
        self.set_components_initializing().await;

        self.start_extensions().await;

        // Must be loaded before datasets
        self.load_embeddings().await;

        // Spawn each component load in its own task to run in parallel
        let task_history = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                if let Err(err) = self_clone.init_task_history().await {
                    tracing::warn!("Creating internal task history table: {err}");
                }
            }
        });

        let results_cache = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                self_clone.init_results_cache().await;
            }
        });

        let datasets = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                self_clone.load_datasets().await;
            }
        });

        let catalogs = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                self_clone.load_catalogs().await;
            }
        });

        let models_and_evals = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                self_clone.load_models().await;

                let app_lock = self_clone.app.read().await;

                if !cfg!(feature = "models")
                    && app_lock.as_ref().is_some_and(|s| !s.evals.is_empty())
                {
                    tracing::error!(
                        "Cannot load evals without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}"
                    );
                }

                #[cfg(feature = "models")]
                {
                    self_clone.load_eval_scorer().await;
                    let () = self_clone.verify_evals().await;
                    let an_eval_exists = app_lock.as_ref().is_some_and(|app| !app.evals.is_empty());
                    if !an_eval_exists {
                        tracing::trace!(
                            "No eval spice components defined. Therefore not loading eval tables into database."
                        );
                    } else if let Err(err) = self_clone.load_eval_tables().await {
                        tracing::warn!("Creating internal eval run table: {err}");
                    }
                }
            }
        });

        let components = vec![
            task_history,
            results_cache,
            datasets,
            catalogs,
            models_and_evals,
        ];

        // Signal that the load must be canceled if the runtime is shut down before the components are loaded
        let cancel_loading = CancellationToken::new();

        // Wait for all components to load returning the first error
        // or canceling spawned tokio tasks if the runtime is shutting down
        let load_result = self
            .start_runtime_task(
                COMPONENTS_INITIAL_LOAD,
                Some(cancel_loading.clone()),
                async move {
                    let abort_handlers = components
                        .iter()
                        .map(JoinHandle::abort_handle)
                        .collect::<Vec<_>>();

                    tokio::select! {
                        load_result = try_join_all(components) => {
                            load_result.map(|_| ()).context(ComponentsInitializationFailedSnafu)
                        }
                        () = cancel_loading.cancelled() => {
                            for handle in abort_handlers {
                                handle.abort();
                            }
                            ComponentsInitializationCancelledSnafu.fail()
                        }
                    }
                },
            )
            .await;

        if let Err(err) = load_result.await {
            if !matches!(err, Error::ComponentsInitializationCancelled) {
                tracing::error!("Could not start the Spice runtime: {err}");
            }
        } else {
            // Create a background task to report once all components are marked as `Ready`
            let status = self.status();
            tokio::spawn({
                async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                        if status.is_shutdown() {
                            break;
                        }
                        if status.is_ready() {
                            tracing::info!("All components are loaded. Spice runtime is ready!");
                            break;
                        }
                    }
                }
            });
        }
    }

    // Closes and deallocates all resources (including the static registries)
    pub async fn shutdown(&self) {
        if self.status.is_shutdown() {
            return;
        }

        tracing::info!("Shutting down runtime...");
        self.status.mark_shutdown();

        // shutdown all running components except the HTTP and Metrics servers
        let mut runtime_tasks = self.runtime_tasks.write().await;

        // HTTP and METRICS servers must be shutdown last
        let mut first_shutdown_group = Vec::new();
        let mut last_shutdown_group = Vec::new();

        for (name, handle) in runtime_tasks.drain() {
            match name.as_str() {
                HTTP_SERVER | METRICS_SERVER => last_shutdown_group.push((name, handle)),
                _ => first_shutdown_group.push((name, handle)),
            }
        }

        let shutdown_futures: Vec<_> = first_shutdown_group
            .into_iter()
            .filter_map(|(name, handle)| {
                if handle.is_finished() {
                    None
                } else {
                    tracing::debug!("Shutting down {name}");
                    Some(handle.cancel(RUNTIME_TASK_SHUTDOWN_TIMEOUT))
                }
            })
            .collect();

        join_all(shutdown_futures).await;

        // Clean up DataFusion first as there could be datasets loading and accessing registries below.
        self.df.shutdown().await;
        dataconnector::unregister_all().await;
        catalogconnector::unregister_all().await;
        dataaccelerator::unregister_all().await;
        tools::factory::unregister_all_factories().await;
        document_parse::unregister_all().await;

        // Shutdown HTTP & Metrics servers last
        let shutdown_futures: Vec<_> = last_shutdown_group
            .into_iter()
            .map(|(name, handle)| {
                tracing::debug!("Shutting down {name}");
                handle.cancel(RUNTIME_TASK_SHUTDOWN_TIMEOUT)
            })
            .collect();

        join_all(shutdown_futures).await;

        tracing::debug!("Shutdown completed");
    }

    /// Spawns and registers a runtime task with optional cancellation support.
    async fn start_runtime_task<F>(
        self: &Arc<Self>,
        component_name: &str,
        cancellation_token: Option<CancellationToken>,
        task_fn: F,
    ) -> impl Future<Output = Result<(), Error>>
    where
        F: Future<Output = Result<(), Error>> + Send + 'static,
    {
        let (future, handle) = spawn_cancellable_task(cancellation_token, task_fn);

        self.runtime_tasks
            .write()
            .await
            .insert(component_name.to_string(), handle);

        future
    }

    /// List all tools available in the runtime, either within a catalog or standalone.
    ///
    /// Tools from default catalogs are also loaded individually, so the default catalogs must be ignored.
    ///
    /// For tools from catalog, the name is prefixed with the catalog name. e.g. `catalog_name/tool_name`.
    fn list_all_tools(self: &Arc<Self>) -> impl Stream<Item = Arc<dyn SpiceModelTool>> {
        let default_catalogs = default_available_catalogs();
        let stream_self = Arc::clone(self);
        stream! {
            let tool_lock = stream_self.tools.read().await;
            let default_catalog_names = default_catalogs
                .iter()
                .map(|c| c.name())
                .collect::<HashSet<_>>();
            for (name, tooling) in tool_lock.iter() {
                match tooling {
                    Tooling::Tool(tool) => {
                        yield Arc::clone(tool);
                    }
                    Tooling::Catalog(catalog) => {
                        // Do not list tools from default catalogs. They are already listed individually as tools.
                        if default_catalog_names.contains(&name.as_str()) {
                            continue;
                        }
                        let all = catalog.all().await;
                        for tool in all {
                            yield with_name(&tool, format!("{}/{}", catalog.name(), tool.name()).as_str());
                        }
                    }
                }
            }
        }
    }

    pub async fn get_tool(self: &Arc<Self>, tool_name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        let tools = self.tools.read().await;
        let tool: Arc<dyn SpiceModelTool> =
            if let Some((catalog_name, name)) = tool_name.split_once('/') {
                let Some(Tooling::Catalog(catalog)) = tools.get(catalog_name) else {
                    return None;
                };
                return catalog.get(name).await;
            } else {
                let Some(Tooling::Tool(tool)) = tools.get(tool_name) else {
                    return None;
                };
                Arc::clone(tool)
            };
        Some(tool)
    }
}

#[allow(clippy::implicit_hasher)]
pub async fn get_params_with_secrets(
    secrets: Arc<RwLock<Secrets>>,
    params: &HashMap<String, String>,
) -> HashMap<String, SecretString> {
    let secrets = secrets.read().await;

    let mut params_with_secrets: HashMap<String, SecretString> = HashMap::new();

    // Inject secrets from the user-supplied params.
    // This will replace any instances of `${ store:key }` with the actual secret value.
    for (k, v) in params {
        let secret = secrets.inject_secrets(k, ParamStr(v)).await;
        params_with_secrets.insert(k.clone(), secret);
    }

    params_with_secrets
}

#[must_use]
pub fn spice_data_base_path() -> String {
    let Ok(working_dir) = std::env::current_dir() else {
        return ".".to_string();
    };

    let base_folder = working_dir.join(".spice/data");
    base_folder.to_str().unwrap_or(".").to_string()
}

pub(crate) fn make_spice_data_directory() -> Result<()> {
    let base_folder = spice_data_base_path();
    std::fs::create_dir_all(base_folder).context(UnableToCreateDirectorySnafu)
}
