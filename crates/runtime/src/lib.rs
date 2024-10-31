/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::{collections::HashMap, sync::Arc};

use crate::{dataconnector::DataConnector, datafusion::DataFusion};
use ::datafusion::datasource::TableProvider;
use ::datafusion::error::DataFusionError;
use ::datafusion::sql::{sqlparser, TableReference};
use ::opentelemetry::KeyValue;
use accelerated_table::AcceleratedTable;
use app::App;
use builder::RuntimeBuilder;
use cache::QueryResultsCacheProvider;
use component::catalog::Catalog;
use component::dataset::acceleration::RefreshMode;
use component::dataset::{self, Dataset};
use component::view::View;
use config::Config;
use dataconnector::localpod::{LocalPodConnector, LOCALPOD_DATACONNECTOR};
use datafusion::SPICE_RUNTIME_SCHEMA;
use datasets_health_monitor::DatasetsHealthMonitor;
use embeddings::connector::EmbeddingConnector;
use embeddings::task::TaskEmbed;
use extension::ExtensionFactory;
use futures::future::join_all;
use futures::{Future, StreamExt};
use llms::chat::Chat;
use llms::embeddings::Embed;
use model::{
    try_to_chat_model, try_to_embedding, EmbeddingModelStore, LLMModelStore,
    ENABLE_MODEL_SUPPORT_MESSAGE,
};
use model_components::model::Model;
pub use notify::Error as NotifyError;
use secrecy::SecretString;
use secrets::ParamStr;
use snafu::prelude::*;
use spice_metrics::get_metrics_table_reference;
use spicepod::component::embeddings::Embeddings;
use spicepod::component::model::{Model as SpicepodModel, ModelType};
use spicepod::component::tool::Tool;
use timing::TimeMeasurement;
use tls::TlsConfig;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::RwLock;
use tools::builtin::get_builtin_tool_spec;
use tools::factory as tool_factory;
use tools::SpiceModelTool;
use tracing_util::dataset_registered_trace;
use util::fibonacci_backoff::FibonacciBackoffBuilder;
pub use util::shutdown_signal;
use util::{retry, RetryError};

use crate::extension::Extension;
pub mod accelerated_table;
mod builder;
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
mod flight;
mod http;
pub mod internal_table;
mod metrics;
mod metrics_server;
pub mod model;
pub mod object_store_registry;
pub mod objectstore;
mod opentelemetry;
mod parameters;
pub mod podswatcher;
pub mod secrets;
pub mod spice_metrics;
pub mod status;
pub mod task_history;
pub mod timing;
pub mod tls;
pub mod tools;
pub(crate) mod tracers;
mod tracing_util;
mod view;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: http::Error },

    #[snafu(display("{source}"))]
    UnableToJoinTask { source: tokio::task::JoinError },

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

    #[snafu(display("Unknown data connector: {data_connector}"))]
    UnknownDataConnector { data_connector: String },

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

    #[snafu(display("Unable to attach data connector {data_connector}: {source}"))]
    UnableToAttachDataConnector {
        source: datafusion::Error,
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

    #[snafu(display(
        "A federated table was configured as read_write without setting replication.enabled = true"
    ))]
    FederatedReadWriteTableWithoutReplication,

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
    tools: Arc<RwLock<HashMap<String, Arc<dyn SpiceModelTool>>>>,
    pods_watcher: Arc<RwLock<Option<podswatcher::PodsWatcher>>>,
    secrets: Arc<RwLock<secrets::Secrets>>,
    datasets_health_monitor: Option<Arc<DatasetsHealthMonitor>>,
    metrics_endpoint: Option<SocketAddr>,
    prometheus_registry: Option<prometheus::Registry>,

    autoload_extensions: Arc<HashMap<String, Box<dyn ExtensionFactory>>>,
    extensions: Arc<RwLock<HashMap<String, Arc<dyn Extension>>>>,
    spaced_tracer: Arc<tracers::SpacedTracer>,

    status: Arc<status::RuntimeStatus>,
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
    pub async fn start_servers(
        &self,
        config: Config,
        tls_config: Option<Arc<TlsConfig>>,
    ) -> Result<()> {
        self.register_metrics_table(self.prometheus_registry.is_some())
            .await?;

        let http_server_future = tokio::spawn(http::start(
            config.http_bind_address,
            Arc::clone(&self.app),
            Arc::clone(&self.df),
            Arc::clone(&self.models),
            Arc::clone(&self.llms),
            Arc::clone(&self.embeds),
            config.clone().into(),
            self.metrics_endpoint,
            tls_config.clone(),
        ));

        // Spawn the metrics server in the background
        let metrics_endpoint = self.metrics_endpoint;
        let prometheus_registry = self.prometheus_registry.clone();
        let cloned_tls_config = tls_config.clone();
        tokio::spawn(async move {
            if let Err(e) =
                metrics_server::start(metrics_endpoint, prometheus_registry, cloned_tls_config)
                    .await
            {
                tracing::error!("Prometheus metrics server error: {e}");
            }
        });

        let flight_server_future = tokio::spawn(flight::start(
            config.flight_bind_address,
            Arc::clone(&self.df),
            tls_config.clone(),
        ));
        let open_telemetry_server_future = tokio::spawn(opentelemetry::start(
            config.open_telemetry_bind_address,
            Arc::clone(&self.df),
            tls_config.clone(),
        ));

        let pods_watcher_future = if self.pods_watcher.read().await.is_some() {
            Some(self.start_pods_watcher())
        } else {
            None
        };

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

        tokio::select! {
            http_res = http_server_future => {
                match http_res {
                    Ok(http_res) => http_res.context(UnableToStartHttpServerSnafu),
                    Err(source) => {
                        Err(Error::UnableToJoinTask { source })
                    }
                }
             },
            flight_res = flight_server_future => {
                match flight_res {
                    Ok(flight_res) => flight_res.context(UnableToStartFlightServerSnafu),
                    Err(source) => {
                        Err(Error::UnableToJoinTask { source })
                    }
                }
            },
            open_telemetry_res = open_telemetry_server_future => {
                match open_telemetry_res {
                    Ok(open_telemetry_res) => open_telemetry_res.context(UnableToStartOpenTelemetryServerSnafu),
                    Err(source) => {
                        Err(Error::UnableToJoinTask { source })
                    }
                }
            },
            pods_watcher_res = async {
                if let Some(fut) = pods_watcher_future {
                    fut.await
                } else {
                    futures::future::pending().await
                }
            } => {
                pods_watcher_res.context(UnableToInitializePodsWatcherSnafu)
            },
            () = shutdown_signal() => {
                tracing::info!("Goodbye!");
                Ok(())
            },
        }
    }

    /// Will load all of the components of the Runtime, including `secret_stores`, `catalogs`, `datasets`, `models`, and `embeddings`.
    ///
    /// The future returned by this function will not resolve until all components have been loaded and marked as ready.
    /// This includes waiting for the first refresh of any accelerated tables to complete.
    pub async fn load_components(&self) {
        self.start_extensions().await;

        self.load_embeddings().await; // Must be loaded before datasets

        let mut futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![
            Box::pin(async {
                if let Err(err) = self.init_task_history().await {
                    tracing::warn!("Creating internal task history table: {err}");
                };
            }),
            Box::pin(self.init_results_cache()),
            Box::pin(self.load_datasets()),
            Box::pin(self.load_catalogs()),
        ];

        futures.push(Box::pin(self.load_models()));

        join_all(futures).await;
    }

    pub async fn get_params_with_secrets(
        &self,
        params: &HashMap<String, String>,
    ) -> HashMap<String, SecretString> {
        let shared_secrets = Arc::clone(&self.secrets);
        let secrets = shared_secrets.read().await;

        let mut params_with_secrets: HashMap<String, SecretString> = HashMap::new();

        // Inject secrets from the user-supplied params.
        // This will replace any instances of `${ store:key }` with the actual secret value.
        for (k, v) in params {
            let secret = secrets.inject_secrets(k, ParamStr(v)).await;
            params_with_secrets.insert(k.clone(), secret);
        }

        params_with_secrets
    }

    async fn start_extensions(&self) {
        let mut extensions = self.extensions.write().await;
        for (name, extension) in extensions.iter_mut() {
            if let Err(err) = extension.on_start(self).await {
                tracing::warn!("Failed to start extension {name}: {err}");
            }
        }
    }

    fn datasets_iter(app: &Arc<App>) -> impl Iterator<Item = Result<Dataset>> + '_ {
        app.datasets
            .clone()
            .into_iter()
            .map(Dataset::try_from)
            .map(move |ds| ds.map(|ds| Dataset::with_app(ds, Arc::clone(app))))
    }

    fn catalogs_iter<'a>(app: &Arc<App>) -> impl Iterator<Item = Result<Catalog>> + 'a {
        app.catalogs.clone().into_iter().map(Catalog::try_from)
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    fn get_valid_datasets(app: &Arc<App>, log_errors: LogErrors) -> Vec<Arc<Dataset>> {
        Self::datasets_iter(app)
            .zip(&app.datasets)
            .filter_map(|(ds, spicepod_ds)| match ds {
                Ok(ds) => Some(Arc::new(ds)),
                Err(e) => {
                    if log_errors.0 {
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        tracing::error!(dataset = &spicepod_ds.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    /// Returns a list of valid catalogs from the given App, skipping any that fail to parse and logging an error for them.
    fn get_valid_catalogs(app: &Arc<App>, log_errors: LogErrors) -> Vec<Catalog> {
        Self::catalogs_iter(app)
            .zip(&app.catalogs)
            .filter_map(|(catalog, spicepod_catalog)| match catalog {
                Ok(catalog) => Some(catalog),
                Err(e) => {
                    if log_errors.0 {
                        metrics::catalogs::LOAD_ERROR.add(1, &[]);
                        tracing::error!(catalog = &spicepod_catalog.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    /// Returns a list of valid views from the given App, skipping any that fail to parse and logging an error for them.
    fn get_valid_views(app: &Arc<App>, log_errors: LogErrors) -> Vec<View> {
        let datasets = Self::get_valid_datasets(app, log_errors)
            .iter()
            .map(|ds| ds.name.clone())
            .collect::<HashSet<_>>();

        app.views
            .iter()
            .cloned()
            .map(View::try_from)
            .zip(&app.views)
            .filter_map(|(view, spicepod_view)| match view {
                Ok(view) => {
                    // only load this view if the name isn't used by an existing dataset
                    if datasets.contains(&view.name) {
                        if log_errors.0 {
                            metrics::views::LOAD_ERROR.add(1, &[]);
                            tracing::error!(
                                view = &spicepod_view.name,
                                "View name is already in use by a dataset."
                            );
                        }
                        None
                    } else {
                        Some(view)
                    }
                }
                Err(e) => {
                    if log_errors.0 {
                        metrics::views::LOAD_ERROR.add(1, &[]);
                        tracing::error!(view = &spicepod_view.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    async fn load_catalogs(&self) {
        let app_lock = self.app.read().await;
        let Some(app) = app_lock.as_ref() else {
            return;
        };

        let valid_catalogs = Self::get_valid_catalogs(app, LogErrors(true));
        let mut futures = vec![];
        for catalog in &valid_catalogs {
            self.status
                .update_catalog(&catalog.name, status::ComponentStatus::Initializing);
            futures.push(self.load_catalog(catalog));
        }

        let _ = join_all(futures).await;
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    async fn get_initialized_datasets(app: &Arc<App>, log_errors: LogErrors) -> Vec<Arc<Dataset>> {
        let valid_datasets = Self::get_valid_datasets(app, log_errors);
        futures::stream::iter(valid_datasets)
            .filter_map(|ds| async move {
                match (ds.is_accelerated(), ds.is_accelerator_initialized().await) {
                    (true, true) | (false, _) => Some(Arc::clone(&ds)),
                    (true, false) => {
                        if log_errors.0 {
                            metrics::datasets::LOAD_ERROR.add(1, &[]);
                            tracing::error!(
                                dataset = &ds.name.to_string(),
                                "Dataset is accelerated but the accelerator failed to initialize."
                            );
                        }
                        None
                    }
                }
            })
            .collect()
            .await
    }

    /// Initialize datasets configured with accelerators before registering the datasets.
    /// This ensures that the required resources for acceleration are available before registration,
    /// which is important for acceleration federation for some acceleration engines (e.g. `SQLite`).
    async fn initialize_accelerators(&self, datasets: &[Arc<Dataset>]) -> Vec<Arc<Dataset>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let mut initialized_datasets = vec![];
        for ds in datasets {
            if let Some(acceleration) = &ds.acceleration {
                let accelerator = match dataaccelerator::get_accelerator_engine(acceleration.engine)
                    .await
                    .context(AcceleratorEngineNotAvailableSnafu {
                        name: acceleration.engine.to_string(),
                    }) {
                    Ok(accelerator) => accelerator,
                    Err(err) => {
                        let ds_name = &ds.name;
                        self.status
                            .update_dataset(ds_name, status::ComponentStatus::Error);
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                        continue;
                    }
                };

                match accelerator
                    .init(ds)
                    .await
                    .context(AcceleratorInitializationFailedSnafu {
                        name: acceleration.engine.to_string(),
                    }) {
                    Ok(()) => {
                        initialized_datasets.push(Arc::clone(ds));
                    }
                    Err(err) => {
                        let ds_name = &ds.name;
                        self.status
                            .update_dataset(ds_name, status::ComponentStatus::Error);
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                    }
                }
            } else {
                initialized_datasets.push(Arc::clone(ds)); // non-accelerated datasets are always successfully initialized
            }
        }

        initialized_datasets
    }

    async fn load_datasets(&self) {
        let app_lock = self.app.read().await;
        let Some(app) = app_lock.as_ref() else {
            return;
        };

        let valid_datasets = Self::get_valid_datasets(app, LogErrors(true));
        let initialized_datasets = self.initialize_accelerators(&valid_datasets).await;

        // Separate datasets into localpod and non-localpod
        let (localpod_datasets, non_localpod_datasets): (Vec<_>, Vec<_>) = initialized_datasets
            .into_iter()
            .partition(|ds| ds.source() == LOCALPOD_DATACONNECTOR);
        let mut futures = vec![];

        // Load non-localpod datasets first in parallel
        for ds in non_localpod_datasets {
            self.status
                .update_dataset(&ds.name, status::ComponentStatus::Initializing);
            futures.push(self.load_dataset(ds));
        }

        if let Some(parallel_num) = app.runtime.num_of_parallel_loading_at_start_up {
            let stream = futures::stream::iter(futures).buffer_unordered(parallel_num);
            let _ = stream.collect::<Vec<_>>().await;
        } else {
            let _ = join_all(futures).await;
        }

        // Load localpod datasets
        for ds in localpod_datasets {
            self.status
                .update_dataset(&ds.name, status::ComponentStatus::Initializing);
            self.load_dataset(ds).await;
        }

        // After all datasets have loaded, load the views.
        self.load_views(app);
    }

    fn load_views(&self, app: &Arc<App>) {
        let views: Vec<View> = Self::get_valid_views(app, LogErrors(true));

        for view in &views {
            if let Err(e) = self.load_view(view) {
                tracing::error!("Unable to load view: {e}");
            };
        }
    }

    async fn load_catalog(&self, catalog: &Catalog) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

        let _ = retry(retry_strategy, || async {
            let connector = match self.load_catalog_connector(catalog).await {
                Ok(connector) => connector,
                Err(err) => {
                    let catalog_name = &catalog.name;
                    self.status
                        .update_catalog(catalog_name, status::ComponentStatus::Error);
                    metrics::catalogs::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = self.register_catalog(catalog, connector).await {
                tracing::error!("Unable to register catalog {}: {err}", &catalog.name);
                return Err(RetryError::transient(err));
            };

            self.status
                .update_catalog(&catalog.name, status::ComponentStatus::Ready);

            Ok(())
        })
        .await;
    }

    /// Caller must set `status::update_dataset(...` before calling `load_dataset`. This function will set error/ready statuses appropriately.
    async fn load_dataset(&self, ds: Arc<Dataset>) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

        let _ = retry(retry_strategy, || async {
            let connector = match self.load_dataset_connector(Arc::clone(&ds)).await {
                Ok(connector) => connector,
                Err(err) => {
                    let ds_name = &ds.name;
                    self.status
                        .update_dataset(ds_name, status::ComponentStatus::Error);
                    metrics::datasets::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = self
                .register_loaded_dataset(Arc::clone(&ds), connector, None)
                .await
            {
                return Err(RetryError::transient(err));
            };

            Ok(())
        })
        .await;
    }

    fn load_view(&self, view: &View) -> Result<()> {
        let df = Arc::clone(&self.df);
        df.register_view(view.name.clone(), view.sql.clone())
            .context(UnableToAttachViewSnafu)?;

        Ok(())
    }

    async fn load_catalog_connector(&self, catalog: &Catalog) -> Result<Arc<dyn DataConnector>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let catalog = catalog.clone();

        let source = catalog.provider;
        let params = catalog.params.clone();
        let data_connector: Arc<dyn DataConnector> = match self
            .get_dataconnector_from_source(&source, params, None)
            .await
        {
            Ok(data_connector) => data_connector,
            Err(err) => {
                let catalog_name = &catalog.name;
                self.status
                    .update_catalog(catalog_name, status::ComponentStatus::Error);
                metrics::catalogs::LOAD_ERROR.add(1, &[]);
                warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
                return UnableToLoadDatasetConnectorSnafu {
                    dataset: catalog_name.clone(),
                }
                .fail();
            }
        };

        Ok(data_connector)
    }

    async fn load_dataset_connector(&self, ds: Arc<Dataset>) -> Result<Arc<dyn DataConnector>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let source = ds.source();
        let params = ds.params.clone();
        let metadata = ds.metadata.clone();
        let data_connector: Arc<dyn DataConnector> = match self
            .get_dataconnector_from_source(&source, params, Some(metadata))
            .await
        {
            Ok(data_connector) => data_connector,
            Err(err) => {
                let ds_name = &ds.name;
                self.status
                    .update_dataset(ds_name, status::ComponentStatus::Error);
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                warn_spaced!(
                    spaced_tracer,
                    "Error initializing dataset {}. {err}",
                    ds_name.table()
                );
                return UnableToLoadDatasetConnectorSnafu {
                    dataset: ds.name.clone(),
                }
                .fail();
            }
        };

        Ok(data_connector)
    }

    async fn register_catalog(
        &self,
        catalog: &Catalog,
        data_connector: Arc<dyn DataConnector>,
    ) -> Result<()> {
        tracing::info!(
            "Registering catalog '{}' for {}",
            &catalog.name,
            &catalog.provider
        );
        let catalog_provider = data_connector
            .catalog_provider(self, catalog)
            .await
            .context(DataConnectorDoesntSupportCatalogsSnafu {
                dataconnector: catalog.provider.clone(),
            })?
            .boxed()
            .context(UnableToLoadCatalogConnectorSnafu {
                catalog: catalog.name.clone(),
            })?;
        let num_schemas = catalog_provider
            .schema_names()
            .iter()
            .fold(0, |acc, schema| {
                acc + catalog_provider
                    .schema(schema)
                    .map_or(0, |s| i32::from(!s.table_names().is_empty()))
            });
        let num_tables = catalog_provider
            .schema_names()
            .iter()
            .fold(0, |acc, schema| {
                acc + catalog_provider
                    .schema(schema)
                    .map_or(0, |s| s.table_names().len())
            });

        self.df
            .register_catalog(&catalog.name, catalog_provider)
            .boxed()
            .context(UnableToLoadCatalogConnectorSnafu {
                catalog: catalog.name.clone(),
            })?;

        tracing::info!(
            "Registered catalog '{}' with {num_schemas} schema{} and {num_tables} table{}",
            &catalog.name,
            if num_schemas == 1 { "" } else { "s" },
            if num_tables == 1 { "" } else { "s" },
        );

        Ok(())
    }

    async fn register_loaded_dataset(
        &self,
        ds: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        accelerated_table: Option<AcceleratedTable>,
    ) -> Result<()> {
        let source = ds.source();
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        if let Some(acceleration) = &ds.acceleration {
            if data_connector.resolve_refresh_mode(acceleration.refresh_mode)
                == RefreshMode::Changes
                && !data_connector.supports_changes_stream()
            {
                let err = AcceleratedTableInvalidChangesSnafu {
                    dataset_name: ds.name.to_string(),
                }
                .build();
                warn_spaced!(spaced_tracer, "{}{err}", "");
                return Err(err);
            }
        }

        // test dataset connectivity by attempting to get a read provider
        let read_provider = match data_connector.read_provider(&ds).await {
            Ok(provider) => provider,
            Err(err) => {
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Error);
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                warn_spaced!(spaced_tracer, "{}{err}", "");
                return UnableToLoadDatasetConnectorSnafu {
                    dataset: ds.name.clone(),
                }
                .fail();
            }
        };

        match self
            .register_dataset(
                Arc::clone(&ds),
                RegisterDatasetContext {
                    data_connector: Arc::clone(&data_connector),
                    federated_read_table: read_provider,
                    source,
                    accelerated_table,
                },
            )
            .await
        {
            Ok(()) => {
                tracing::info!(
                    "{}",
                    dataset_registered_trace(
                        &data_connector,
                        &ds,
                        self.df.cache_provider().is_some()
                    )
                );
                if let Some(datasets_health_monitor) = &self.datasets_health_monitor {
                    if let Err(err) = datasets_health_monitor.register_dataset(&ds).await {
                        tracing::warn!(
                            "Unable to add dataset {} for availability monitoring: {err}",
                            &ds.name
                        );
                    };
                }
                let engine = ds.acceleration.as_ref().map_or_else(
                    || "None".to_string(),
                    |acc| {
                        if acc.enabled {
                            acc.engine.to_string()
                        } else {
                            "None".to_string()
                        }
                    },
                );
                metrics::datasets::COUNT.add(1, &[KeyValue::new("engine", engine)]);

                Ok(())
            }
            Err(err) => {
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Error);
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                if let Error::UnableToAttachDataConnector {
                    source: datafusion::Error::RefreshSql { source },
                    data_connector: _,
                } = &err
                {
                    tracing::error!("{source}");
                }
                warn_spaced!(spaced_tracer, "{}{err}", "");

                Err(err)
            }
        }
    }

    async fn remove_dataset(&self, ds: &Dataset) {
        if self.df.table_exists(ds.name.clone()) {
            if let Some(datasets_health_monitor) = &self.datasets_health_monitor {
                datasets_health_monitor
                    .deregister_dataset(&ds.name.to_string())
                    .await;
            }

            if let Err(e) = self.df.remove_table(&ds.name).await {
                tracing::warn!("Unable to unload dataset {}: {}", &ds.name, e);
                return;
            }
        }

        tracing::info!("Unloaded dataset {}", &ds.name);
        let engine = ds.acceleration.as_ref().map_or_else(
            || "None".to_string(),
            |acc| {
                if acc.enabled {
                    acc.engine.to_string()
                } else {
                    "None".to_string()
                }
            },
        );
        metrics::datasets::COUNT.add(-1, &[KeyValue::new("engine", engine)]);
    }

    async fn update_dataset(&self, ds: Arc<Dataset>) {
        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        match self.load_dataset_connector(Arc::clone(&ds)).await {
            Ok(connector) => {
                // File accelerated datasets don't support hot reload.
                if Self::accelerated_dataset_supports_hot_reload(&ds, &*connector) {
                    tracing::info!("Updating accelerated dataset {}...", &ds.name);
                    if let Ok(()) = &self
                        .reload_accelerated_dataset(Arc::clone(&ds), Arc::clone(&connector))
                        .await
                    {
                        self.status
                            .update_dataset(&ds.name, status::ComponentStatus::Ready);
                        return;
                    }
                    tracing::debug!("Failed to create accelerated table for dataset {}, falling back to full dataset reload", ds.name);
                }

                self.remove_dataset(&ds).await;

                // Initialize file mode accelerator when reloading with file mode acceleration
                // Fail when there's no successfully initiated dataset
                if ds.is_file_accelerated() {
                    let datasets = self.initialize_accelerators(&[Arc::clone(&ds)]).await;
                    if datasets.is_empty() {
                        return;
                    }
                }

                if (self
                    .register_loaded_dataset(Arc::clone(&ds), Arc::clone(&connector), None)
                    .await)
                    .is_err()
                {
                    self.status
                        .update_dataset(&ds.name, status::ComponentStatus::Error);
                }
            }
            Err(e) => {
                tracing::error!("Unable to update dataset {}: {e}", ds.name);
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Error);
            }
        }
    }

    fn accelerated_dataset_supports_hot_reload(
        ds: &Dataset,
        connector: &dyn DataConnector,
    ) -> bool {
        let Some(acceleration) = &ds.acceleration else {
            return false;
        };

        if !acceleration.enabled {
            return false;
        }

        // Datasets that configure changes and are file-accelerated automatically keep track of changes that survive restarts.
        // Thus we don't need to "hot reload" them to try to keep their data intact.
        if connector.supports_changes_stream()
            && ds.is_file_accelerated()
            && connector.resolve_refresh_mode(acceleration.refresh_mode) == RefreshMode::Changes
        {
            return false;
        }

        // File accelerated datasets don't support hot reload.
        if ds.is_file_accelerated() {
            return false;
        }

        true
    }

    async fn reload_accelerated_dataset(
        &self,
        ds: Arc<Dataset>,
        connector: Arc<dyn DataConnector>,
    ) -> Result<()> {
        let read_table = connector.read_provider(&ds).await.map_err(|_| {
            UnableToLoadDatasetConnectorSnafu {
                dataset: ds.name.clone(),
            }
            .build()
        })?;

        // create new accelerated table for updated data connector
        let (accelerated_table, is_ready) = self
            .df
            .create_accelerated_table(&ds, Arc::clone(&connector), read_table, self.secrets())
            .await
            .context(UnableToCreateAcceleratedTableSnafu {
                dataset: ds.name.clone(),
            })?;

        // wait for accelerated table to be ready
        is_ready
            .await
            .context(UnableToReceiveAcceleratedTableStatusSnafu)?;

        tracing::debug!("Accelerated table for dataset {} is ready", ds.name);

        self.register_loaded_dataset(ds, Arc::clone(&connector), Some(accelerated_table))
            .await?;

        Ok(())
    }

    async fn get_dataconnector_from_source(
        &self,
        source: &str,
        params: HashMap<String, String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Arc<dyn DataConnector>> {
        let secret_map = self.get_params_with_secrets(&params).await;

        // Unlike most other data connectors, the localpod connector needs a reference to the current DataFusion instance.
        if source == LOCALPOD_DATACONNECTOR {
            return Ok(Arc::new(LocalPodConnector::new(Arc::clone(&self.df))));
        }

        match dataconnector::create_new_connector(source, secret_map, self.secrets(), metadata)
            .await
        {
            Some(dc) => dc.context(UnableToInitializeDataConnectorSnafu {}),
            None => UnknownDataConnectorSnafu {
                data_connector: source,
            }
            .fail(),
        }
    }

    async fn register_dataset(
        &self,
        ds: Arc<Dataset>,
        register_dataset_ctx: RegisterDatasetContext,
    ) -> Result<()> {
        let RegisterDatasetContext {
            data_connector,
            mut federated_read_table,
            source,
            accelerated_table,
        } = register_dataset_ctx;

        let replicate = ds.replication.as_ref().map_or(false, |r| r.enabled);

        // Only wrap data connector when necessary.
        let connector = if ds.embeddings.is_empty() {
            data_connector
        } else {
            let connector = EmbeddingConnector::new(data_connector, Arc::clone(&self.embeds));
            federated_read_table = connector
                .wrap_table(federated_read_table, &ds)
                .await
                .boxed()
                .context(UnableToInitializeDataConnectorSnafu)?;
            Arc::new(connector) as Arc<dyn DataConnector>
        };

        // FEDERATED TABLE
        if !ds.is_accelerated() {
            if ds.mode() == dataset::Mode::ReadWrite && !replicate {
                // A federated dataset was configured as ReadWrite, but the replication setting wasn't set - error out.
                FederatedReadWriteTableWithoutReplicationSnafu.fail()?;
            }

            let ds_name: TableReference = ds.name.clone();
            self.df
                .register_table(
                    ds,
                    datafusion::Table::Federated {
                        data_connector: connector,
                        federated_read_table,
                    },
                )
                .await
                .context(UnableToAttachDataConnectorSnafu {
                    data_connector: source,
                })?;

            self.status
                .update_dataset(&ds_name, status::ComponentStatus::Ready);
            return Ok(());
        }

        // ACCELERATED TABLE
        let acceleration_settings =
            ds.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: ds.name.to_string(),
                })?;
        let accelerator_engine = acceleration_settings.engine;
        let replicate = ds.replication.as_ref().map_or(false, |r| r.enabled);

        if ds.mode() == dataset::Mode::ReadWrite && !replicate {
            AcceleratedReadWriteTableWithoutReplicationSnafu.fail()?;
        }

        dataaccelerator::get_accelerator_engine(accelerator_engine)
            .await
            .context(AcceleratorEngineNotAvailableSnafu {
                name: accelerator_engine.to_string(),
            })?;

        // The accelerated refresh task will set the dataset status to `Ready` once it finishes loading.
        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        self.df
            .register_table(
                ds,
                datafusion::Table::Accelerated {
                    source: connector,
                    federated_read_table,
                    accelerated_table,
                    secrets: self.secrets(),
                },
            )
            .await
            .context(UnableToAttachDataConnectorSnafu {
                data_connector: source,
            })
    }

    /// Loads a specific LLM from the spicepod. If an error occurs, no retry attempt is made.
    async fn load_llm(
        &self,
        m: SpicepodModel,
        params: HashMap<String, SecretString>,
    ) -> Result<Box<dyn Chat>> {
        let l = try_to_chat_model(&m, &params, Arc::new(self.clone()))
            .await
            .boxed()
            .context(UnableToInitializeLlmSnafu)?;

        l.health()
            .await
            .boxed()
            .context(UnableToInitializeLlmSnafu)?;
        Ok(l)
    }

    #[allow(dead_code)]
    /// Loads a specific Embedding model from the spicepod. If an error occurs, no retry attempt is made.
    async fn load_embedding(&self, in_embed: &Embeddings) -> Result<TaskEmbed> {
        let params_with_secrets = self.get_params_with_secrets(&in_embed.params).await;

        let l = try_to_embedding(in_embed, &params_with_secrets)
            .boxed()
            .context(UnableToInitializeEmbeddingModelSnafu)?;
        l.health()
            .await
            .boxed()
            .context(UnableToInitializeEmbeddingModelSnafu)?;

        TaskEmbed::new(l)
            .await
            .boxed()
            .context(UnableToInitializeEmbeddingModelSnafu)
    }

    #[allow(dead_code)]
    async fn load_embeddings(&self) {
        let app_opt = self.app.read().await;

        if !cfg!(feature = "models") && app_opt.as_ref().is_some_and(|s| !s.embeddings.is_empty()) {
            tracing::error!("Cannot load embedding models without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}");
            return;
        };

        if let Some(app) = app_opt.as_ref() {
            for in_embed in &app.embeddings {
                self.status
                    .update_embedding(&in_embed.name, status::ComponentStatus::Initializing);
                match self.load_embedding(in_embed).await {
                    Ok(e) => {
                        let mut embeds_map = self.embeds.write().await;

                        embeds_map.insert(in_embed.name.clone(), Box::new(e) as Box<dyn Embed>);

                        tracing::info!("Embedding [{}] ready to embed", in_embed.name);
                        metrics::embeddings::COUNT.add(
                            1,
                            &[
                                KeyValue::new("embeddings", in_embed.name.clone()),
                                KeyValue::new(
                                    "source",
                                    in_embed
                                        .get_prefix()
                                        .map(|x| x.to_string())
                                        .unwrap_or_default(),
                                ),
                            ],
                        );
                        self.status
                            .update_embedding(&in_embed.name, status::ComponentStatus::Ready);
                    }
                    Err(e) => {
                        metrics::embeddings::LOAD_ERROR.add(1, &[]);
                        self.status
                            .update_embedding(&in_embed.name, status::ComponentStatus::Error);
                        tracing::warn!(
                            "Unable to load embedding from spicepod {}, error: {}",
                            in_embed.name,
                            e,
                        );
                    }
                }
            }
        }
    }

    async fn load_models(&self) {
        let app_lock = self.app.read().await;

        if !cfg!(feature = "models") && app_lock.as_ref().is_some_and(|s| !s.models.is_empty()) {
            tracing::error!("Cannot load models without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}");
            return;
        }

        // Load tools before loading models.
        self.load_tools().await;

        if let Some(app) = app_lock.as_ref() {
            for model in &app.models {
                self.status
                    .update_model(&model.name, status::ComponentStatus::Initializing);
                self.load_model(model).await;
            }
        }
    }

    #[allow(clippy::implicit_hasher)]
    async fn load_tools(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for tool in &app.tools {
                self.load_tool(tool).await;
            }
        }

        // Load all built-in tools, regardless if they are in the spicepod
        for tool in get_builtin_tool_spec() {
            self.load_tool(&tool).await;
        }
    }

    async fn load_tool(&self, tool: &Tool) {
        self.status
            .update_tool(&tool.name, status::ComponentStatus::Initializing);
        let params_with_secrets: HashMap<String, SecretString> =
            self.get_params_with_secrets(&tool.params).await;

        match tool_factory::forge(tool, params_with_secrets)
            .await
            .context(UnableToInitializeLlmToolSnafu)
        {
            Ok(t) => {
                let mut tools_map = self.tools.write().await;
                tools_map.insert(tool.name.clone(), t);
                tracing::debug!("Tool {} ready to use", tool.name);
                metrics::tools::COUNT.add(1, &[KeyValue::new("tool", tool.name.clone())]);
                self.status
                    .update_tool(&tool.name, status::ComponentStatus::Ready);
            }
            Err(e) => {
                metrics::tools::LOAD_ERROR.add(1, &[]);
                self.status
                    .update_tool(&tool.name, status::ComponentStatus::Error);
                tracing::warn!(
                    "Unable to load tool from spicepod {}, error: {}",
                    tool.name,
                    e,
                );
            }
        }
    }

    // Caller must set `status::update_model(...` before calling `load_model`. This function will set error/ready statues appropriately.`
    async fn load_model(&self, m: &SpicepodModel) {
        let source = m.get_source();
        let source_str = source.clone().map(|s| s.to_string()).unwrap_or_default();
        let model = m.clone();
        let _guard = TimeMeasurement::new(
            &metrics::models::LOAD_DURATION_MS,
            &[
                KeyValue::new("model", m.name.clone()),
                KeyValue::new("source", source_str.clone()),
            ],
        );

        tracing::info!("Loading model [{}] from {}...", m.name, m.from);

        // TODO: Have downstream code using model parameters to accept `Hashmap<String, Value>`.
        // This will require handling secrets with `Value` type.
        let p = m
            .params
            .clone()
            .iter()
            .map(|(k, v)| {
                let k = k.clone();
                match v.as_str() {
                    Some(s) => (k, s.to_string()),
                    None => (k, v.to_string()),
                }
            })
            .collect::<HashMap<_, _>>();
        let params = self.get_params_with_secrets(&p).await;

        let model_type = m.model_type();
        tracing::trace!("Model type for {} is {:#?}", m.name, model_type.clone());
        let result: Result<(), String> = match model_type {
            Some(ModelType::Llm) => match self.load_llm(m.clone(), params).await {
                Ok(l) => {
                    let mut llm_map = self.llms.write().await;
                    llm_map.insert(m.name.clone(), l);
                    Ok(())
                }
                Err(e) => Err(format!(
                    "Unable to load LLM from spicepod {}, error: {}",
                    m.name, e,
                )),
            },
            Some(ModelType::Ml) => match Model::load(m.clone(), params).await {
                Ok(in_m) => {
                    let mut model_map = self.models.write().await;
                    model_map.insert(m.name.clone(), in_m);
                    Ok(())
                }
                Err(e) => Err(format!(
                    "Unable to load runnable model from spicepod {}, error: {}",
                    m.name, e,
                )),
            },
            None => Err(format!(
                "Unable to load model {} from spicepod. Unable to determine model type.",
                m.name,
            )),
        };
        match result {
            Ok(()) => {
                tracing::info!("Model [{}] deployed, ready for inferencing", m.name);
                metrics::models::COUNT.add(
                    1,
                    &[
                        KeyValue::new("model", m.name.clone()),
                        KeyValue::new("source", source_str),
                    ],
                );
                self.status
                    .update_model(&model.name, status::ComponentStatus::Ready);
            }
            Err(e) => {
                metrics::models::LOAD_ERROR.add(1, &[]);
                self.status
                    .update_model(&model.name, status::ComponentStatus::Error);
                tracing::warn!(e);
            }
        }
    }

    async fn remove_model(&self, m: &SpicepodModel) {
        match m.model_type() {
            Some(ModelType::Ml) => {
                let mut ml_map = self.models.write().await;
                ml_map.remove(&m.name);
            }
            Some(ModelType::Llm) => {
                let mut llm_map = self.llms.write().await;
                llm_map.remove(&m.name);
            }
            None => return,
        };

        tracing::info!("Model [{}] has been unloaded", m.name);
        let source_str = m.get_source().map(|s| s.to_string()).unwrap_or_default();
        metrics::models::COUNT.add(
            -1,
            &[
                KeyValue::new("model", m.name.clone()),
                KeyValue::new("source", source_str),
            ],
        );
    }

    async fn update_model(&self, m: &SpicepodModel) {
        self.status
            .update_model(&m.name, status::ComponentStatus::Refreshing);
        self.remove_model(m).await;
        self.load_model(m).await;
    }

    async fn register_metrics_table(&self, metrics_enabled: bool) -> Result<()> {
        if metrics_enabled {
            let table_reference = get_metrics_table_reference();
            let metrics_table = self.df.get_table(&table_reference).await;

            if metrics_table.is_none() {
                tracing::debug!("Registering local metrics table");
                spice_metrics::register_metrics_table(&self.df)
                    .await
                    .context(UnableToStartLocalMetricsSnafu)?;
            }
        }

        Ok(())
    }

    async fn start_pods_watcher(&self) -> notify::Result<()> {
        let mut pods_watcher = self.pods_watcher.write().await;
        let Some(mut pods_watcher) = pods_watcher.take() else {
            return Ok(());
        };
        let mut rx = pods_watcher.watch()?;

        while let Some(new_app) = rx.recv().await {
            let mut app_lock = self.app.write().await;
            if let Some(current_app) = app_lock.as_mut() {
                let new_app = Arc::new(new_app);
                if *current_app == new_app {
                    continue;
                }

                tracing::debug!("Updated pods information: {:?}", new_app);
                tracing::debug!("Previous pods information: {:?}", current_app);

                // Check for new and updated catalogs
                let valid_catalogs = Self::get_valid_catalogs(&new_app, LogErrors(true));
                let existing_catalogs = Self::get_valid_catalogs(current_app, LogErrors(false));

                for catalog in &valid_catalogs {
                    if let Some(current_catalog) =
                        existing_catalogs.iter().find(|c| c.name == catalog.name)
                    {
                        if catalog != current_catalog {
                            // It isn't currently possible to remove catalogs once they have been loaded in DataFusion. `load_catalog` will overwrite the existing catalog.
                            self.load_catalog(catalog).await;
                        }
                    } else {
                        self.status
                            .update_catalog(&catalog.name, status::ComponentStatus::Initializing);
                        self.load_catalog(catalog).await;
                    }
                }

                // Check for new and updated datasets
                let valid_datasets = Self::get_valid_datasets(&new_app, LogErrors(true));
                let existing_datasets = Self::get_valid_datasets(current_app, LogErrors(false));

                for ds in valid_datasets {
                    if let Some(current_ds) = existing_datasets.iter().find(|d| d.name == ds.name) {
                        if ds != *current_ds {
                            self.update_dataset(ds).await;
                        }
                    } else {
                        self.status
                            .update_dataset(&ds.name, status::ComponentStatus::Initializing);
                        self.load_dataset(ds).await;
                    }
                }

                // Remove datasets that are no longer in the app
                for ds in &current_app.datasets {
                    if !new_app.datasets.iter().any(|d| d.name == ds.name) {
                        let ds = match Dataset::try_from(ds.clone()) {
                            Ok(ds) => ds,
                            Err(e) => {
                                tracing::error!("Could not remove dataset {}: {e}", ds.name);
                                continue;
                            }
                        };
                        self.status
                            .update_dataset(&ds.name, status::ComponentStatus::Disabled);
                        self.remove_dataset(&ds).await;
                    }
                }

                // check for new and updated models
                for model in &new_app.models {
                    if let Some(current_model) =
                        current_app.models.iter().find(|m| m.name == model.name)
                    {
                        if current_model != model {
                            self.update_model(model).await;
                        }
                    } else {
                        self.status
                            .update_model(&model.name, status::ComponentStatus::Initializing);
                        self.load_model(model).await;
                    }
                }

                // Remove models that are no longer in the app
                for model in &current_app.models {
                    if !new_app.models.iter().any(|m| m.name == model.name) {
                        self.status
                            .update_model(&model.name, status::ComponentStatus::Disabled);
                        self.remove_model(model).await;
                    }
                }

                *current_app = new_app;
            } else {
                *app_lock = Some(Arc::new(new_app));
            }
        }

        Ok(())
    }

    pub async fn init_results_cache(&self) {
        let app = self.app.read().await;
        let Some(app) = app.as_ref() else { return };

        let cache_config = &app.runtime.results_cache;

        if !cache_config.enabled {
            return;
        }

        match QueryResultsCacheProvider::try_new(
            cache_config,
            Box::new([SPICE_RUNTIME_SCHEMA.into(), "information_schema".into()]),
        ) {
            Ok(cache_provider) => {
                tracing::info!("Initialized results cache; {cache_provider}");
                self.datafusion().set_cache_provider(cache_provider);
            }
            Err(e) => {
                tracing::warn!("Failed to initialize results cache: {e}");
            }
        };
    }

    pub async fn init_task_history(&self) -> Result<()> {
        let app = self.app.read().await;

        if let Some(app) = app.as_ref() {
            if !app.runtime.task_history.enabled {
                tracing::debug!("Task history is disabled via configuration.");
                return Ok(());
            }
        }

        let (retention_period_secs, retention_check_interval_secs) = match app.as_ref() {
            Some(app) => (
                app.runtime
                    .task_history
                    .retention_period_as_secs()
                    .map_err(|e| Error::UnableToTrackTaskHistory {
                        source: task_history::Error::InvalidConfiguration { source: e }, // keeping the spicepod detached but still want to return snafu errors
                    })?,
                app.runtime
                    .task_history
                    .retention_check_interval_as_secs()
                    .map_err(|e| Error::UnableToTrackTaskHistory {
                        source: task_history::Error::InvalidConfiguration { source: e },
                    })?,
            ),
            None => (
                task_history::DEFAULT_TASK_HISTORY_RETENTION_PERIOD_SECS,
                task_history::DEFAULT_TASK_HISTORY_RETENTION_CHECK_INTERVAL_SECS,
            ),
        };

        match task_history::TaskSpan::instantiate_table(
            self.status(),
            retention_period_secs,
            retention_check_interval_secs,
        )
        .await
        {
            Ok(table) => self
                .df
                .register_runtime_table(
                    TableReference::partial(
                        SPICE_RUNTIME_SCHEMA,
                        task_history::DEFAULT_TASK_HISTORY_TABLE,
                    ),
                    table,
                )
                .context(UnableToCreateBackendSnafu),
            Err(source) => Err(Error::UnableToTrackTaskHistory { source }),
        }
    }
}

pub struct RegisterDatasetContext {
    data_connector: Arc<dyn DataConnector>,
    federated_read_table: Arc<dyn TableProvider>,
    source: String,
    accelerated_table: Option<AcceleratedTable>,
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
