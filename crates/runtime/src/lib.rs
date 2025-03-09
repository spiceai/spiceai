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

use std::net::SocketAddr;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use crate::{
    auth::EndpointAuth, dataconnector::DataConnector, datafusion::DataFusion,
    internal_table::Error as InternalTableError, model::ENABLE_MODEL_SUPPORT_MESSAGE,
};

use ::datafusion::error::DataFusionError;
use ::datafusion::sql::{sqlparser, TableReference};
use app::App;
use builder::RuntimeBuilder;
use config::Config;
use dataconnector::ConnectorComponent;
use datasets_health_monitor::DatasetsHealthMonitor;
use extension::ExtensionFactory;
use flight::RateLimits;
#[cfg(feature = "openapi")]
pub use http::ApiDoc;
use managed_task::{spawn_managed_task, ManagedTaskHandle};
use model::{EmbeddingModelStore, EvalScorerRegistry, LLMModelStore};

use model_components::model::Model;
pub use notify::Error as NotifyError;
use secrecy::SecretString;
use secrets::{ParamStr, Secrets};
use snafu::prelude::*;
use spicepod::component::eval::Eval;
use status::ComponentStatus;
use tls::TlsConfig;
use tokio::sync::{oneshot::error::RecvError, RwLock};
use tokio_util::sync::CancellationToken;
use tools::factory::default_available_catalogs;
use tools::{catalog::SpiceToolCatalog, Tooling};
pub use util::shutdown_signal;

use crate::extension::Extension;
pub mod accelerated_table;
pub mod auth;
mod builder;
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
mod managed_task;
mod metrics;
mod metrics_server;
pub mod model;
pub mod object_store_registry;
pub mod objectstore;
mod opentelemetry;
mod parameters;
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

    #[snafu(display("Task execution failed: {source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
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

    #[snafu(display("Unknown data connector: {data_connector}.\nSpecify a valid data connector and retry. For details, visit: https://spiceai.org/docs/components/data-connectors"))]
    UnknownDataConnector { data_connector: String },

    #[snafu(display("Unknown catalog connector: {catalog_connector}.\nSpecify a valid catalog connector and retry. For details, visit: https://spiceai.org/docs/components/catalogs"))]
    UnknownCatalogConnector { catalog_connector: String },

    #[snafu(display("The runtime is built without ODBC support.\nBuild Spice.ai OSS with the `odbc` feature enabled or use the Docker image that includes ODBC support.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/odbc"))]
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

    #[snafu(display("Specify the SQL string for view {name} using either `sql: SELECT * FROM...` inline or as a file reference with `sql_ref: my_view.sql`"))]
    NeedToSpecifySQLView { name: String },

    #[snafu(display("An accelerated table was configured as read_write without setting replication.enabled = true"))]
    AcceleratedReadWriteTableWithoutReplication,

    #[snafu(display("An accelerated table for {dataset_name} was configured with 'refresh_mode = changes', but the data connector doesn't support a changes stream."))]
    AcceleratedTableInvalidChanges { dataset_name: String },

    #[snafu(display("Expected acceleration settings for {name}, found None"))]
    ExpectedAccelerationSettings { name: String },

    #[snafu(display("The accelerator engine {name} is not available. Valid engines are arrow, duckdb, sqlite, and postgres."))]
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
}

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

    running_components: Arc<RwLock<HashMap<String, ManagedTaskHandle>>>,
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

        let cloned_self = Arc::clone(&self);
        let cloned_config = config.clone();
        let cloned_tls_config = tls_config.clone();

        // Start HTTP server
        let http_auth = endpoint_auth.http_auth.clone();
        let (http_future, http_handle) = spawn_managed_task(
            async move {
                http::start(
                    cloned_config.http_bind_address,
                    cloned_self,
                    cloned_config.into(),
                    cloned_tls_config,
                    http_auth,
                )
                .await
                .context(UnableToStartHttpServerSnafu)
            },
            None,
        );

        self.running_components
            .write()
            .await
            .insert("http_server".to_string(), http_handle);

        // Start Metrics server
        let metrics_endpoint = self.metrics_endpoint;
        let prometheus_registry = self.prometheus_registry.clone();
        let cloned_tls_config = tls_config.clone();

        let (metrics_future, metrics_handle) = spawn_managed_task(
            async move {
                metrics_server::start(metrics_endpoint, prometheus_registry, cloned_tls_config)
                    .await
                    .context(UnableToStartMetricsServerSnafu)
            },
            None,
        );

        self.running_components
            .write()
            .await
            .insert("metrics_server".to_string(), metrics_handle);

        let flight_graceful_shutdown = CancellationToken::new();
        let flight_graceful_shutdown_clone = flight_graceful_shutdown.clone();

        let cloned_self = Arc::clone(&self);
        let cloned_tls_config = tls_config.clone();
        let cloned_endpoint_auth = endpoint_auth.clone();

        // Start Flight server
        let (flight_future, flight_handle) = spawn_managed_task(
            async move {
                flight::start(
                    config.flight_bind_address,
                    cloned_self.app.read().await.as_ref().map(Arc::clone),
                    Arc::clone(&cloned_self.df),
                    cloned_tls_config,
                    cloned_endpoint_auth,
                    Arc::clone(&cloned_self.rate_limits),
                    Some(flight_graceful_shutdown_clone),
                )
                .await
                .context(UnableToStartFlightServerSnafu)
            },
            Some(flight_graceful_shutdown),
        );

        self.running_components
            .write()
            .await
            .insert("flight_server".to_string(), flight_handle);

        // Start OpenTelemetry server
        let opentelemetry_graceful_shutdown = CancellationToken::new();
        let opentelemetry_graceful_shutdown_clone = opentelemetry_graceful_shutdown.clone();
        let cloned_self = Arc::clone(&self);
        let cloned_tls_config = tls_config.clone();
        let grpc_auth = endpoint_auth.grpc_auth.clone();

        let (opentelemetry_future, opentelemetry_handle) = spawn_managed_task(
            async move {
                opentelemetry::start(
                    config.open_telemetry_bind_address,
                    Arc::clone(&cloned_self.df),
                    cloned_tls_config,
                    grpc_auth,
                    Some(opentelemetry_graceful_shutdown_clone),
                )
                .await
                .context(UnableToStartOpenTelemetryServerSnafu)
            },
            Some(opentelemetry_graceful_shutdown),
        );
        self.running_components
            .write()
            .await
            .insert("opentelemetry_server".to_string(), opentelemetry_handle);

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

        // Pods watcher
        let cloned_self = Arc::clone(&self);
        let (pods_watcher_future, posd_watcher_handle) = spawn_managed_task(
            async move {
                cloned_self
                    .start_pods_watcher()
                    .await
                    .context(UnableToInitializePodsWatcherSnafu)
            },
            None,
        );

        self.running_components
            .write()
            .await
            .insert("pods_watcher".to_string(), posd_watcher_handle);

        // Start Spicepod watcher

        let shutdown_signnal_future = async {
            shutdown_signal().await;
            tracing::debug!("Shutdown signal received.");
            self.shutdown().await;
            tracing::info!("Goodbye!");
            Ok(())
        };

        match tokio::try_join!(
            http_future,
            flight_future,
            metrics_future,
            opentelemetry_future,
            pods_watcher_future,
            shutdown_signnal_future
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
    pub async fn load_components(&self) {
        self.set_components_initializing().await;

        self.start_extensions().await;

        // Must be loaded before datasets
        self.load_embeddings().await;

        // Spawn each component load in its own task to run in parallel
        let task_history = tokio::spawn({
            let self_clone = self.clone();
            async move {
                if let Err(err) = self_clone.init_task_history().await {
                    tracing::warn!("Creating internal task history table: {err}");
                }
            }
        });

        let results_cache = tokio::spawn({
            let self_clone = self.clone();
            async move {
                self_clone.init_results_cache().await;
            }
        });

        let datasets = tokio::spawn({
            let self_clone = Arc::new(self.clone());
            async move {
                self_clone.load_datasets().await;
            }
        });

        let catalogs = tokio::spawn({
            let self_clone = self.clone();
            async move {
                self_clone.load_catalogs().await;
            }
        });

        let models = tokio::spawn({
            let self_clone = self.clone();
            async move {
                self_clone.load_models().await;
            }
        });

        let eval_scorer = tokio::spawn({
            let self_clone = self.clone();
            async move {
                let app_lock = self_clone.app.read().await;

                if !cfg!(feature = "models")
                    && app_lock.as_ref().is_some_and(|s| !s.evals.is_empty())
                {
                    tracing::error!("Cannot load evals without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}");
                }

                #[cfg(feature = "models")]
                {
                    self_clone.load_eval_scorer().await;
                    let an_eval_exists = app_lock.as_ref().is_some_and(|app| !app.evals.is_empty());
                    if !an_eval_exists {
                        tracing::trace!("No eval spice components defined. Therefore not loading eval tables into database.");
                    } else if let Err(err) = self_clone.load_eval_tables().await {
                        tracing::warn!("Creating internal eval run table: {err}");
                    }
                }
            }
        });

        // Wait for all tasks to complete
        let load_result = tokio::try_join!(
            task_history,
            results_cache,
            datasets,
            catalogs,
            models,
            eval_scorer
        );

        if let Err(err) = load_result {
            tracing::error!("Could not start the Spice runtime: {err}");
        }
    }

    // Closes and deallocates all resources (including the static registries)
    pub async fn shutdown(&self) {
        if self.status.is_shutdown() {
            return;
        }

        tracing::info!("Shutting down...");
        self.status.mark_shutdown();

        // shutdown all running components except the HTTP and Metrics servers
        let mut running_components = self.running_components.write().await;
        for (name, handle) in running_components.drain() {
            // if name == "http_server" || name == "metrics_server" {
            //     continue;
            // }
            handle.shutdown(Duration::from_secs(10)).await;
            tracing::debug!("Shutdown component: {name}");
        }

        // Clean up DataFusion first as there could be datasets loading and accessing registries below.
        self.df.shutdown().await;
        dataconnector::unregister_all().await;
        catalogconnector::unregister_all().await;
        dataaccelerator::unregister_all().await;
        tools::factory::unregister_all_factories().await;
        document_parse::unregister_all().await;
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
