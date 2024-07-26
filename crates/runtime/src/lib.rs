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

use std::borrow::Borrow;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::{collections::HashMap, sync::Arc};

use crate::spice_metrics::MetricsRecorder;
use crate::{dataconnector::DataConnector, datafusion::DataFusion};
use ::datafusion::datasource::TableProvider;
use ::datafusion::error::DataFusionError;
use ::datafusion::sql::parser::{self, DFParser};
use ::datafusion::sql::sqlparser::ast::{SetExpr, TableFactor};
use ::datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use ::datafusion::sql::sqlparser::{self, ast};
use ::datafusion::sql::TableReference;
use accelerated_table::AcceleratedTable;
use app::App;
use builder::RuntimeBuilder;
use cache::QueryResultsCacheProvider;
use component::catalog::Catalog;
use component::dataset::acceleration::RefreshMode;
use component::dataset::{self, Dataset};
use component::view::View;
use config::Config;
use datafusion::query::query_history;
use datafusion::SPICE_RUNTIME_SCHEMA;
use datasets_health_monitor::DatasetsHealthMonitor;
use embeddings::connector::EmbeddingConnector;
use extension::ExtensionFactory;
use futures::future::join_all;
use futures::{Future, StreamExt};
use llms::chat::Chat;
use llms::embeddings::Embed;
use metrics::SetRecorderError;
use metrics_exporter_prometheus::PrometheusHandle;
use model::{try_to_chat_model, try_to_embedding, LLMModelStore};
use model_components::model::Model;
pub use notify::Error as NotifyError;
use secrecy::SecretString;
use secrets::ParamStr;
use snafu::prelude::*;
use spice_metrics::get_metrics_table_reference;
use spicepod::component::embeddings::Embeddings;
use spicepod::component::model::{Model as SpicepodModel, ModelType};
use tls::TlsConfig;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::RwLock;
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
mod metrics_server;
pub mod model;
pub mod object_store_registry;
pub mod objectstore;
mod opentelemetry;
pub mod podswatcher;
pub mod secrets;
pub mod spice_metrics;
pub mod status;
pub mod timing;
pub mod tls;
pub(crate) mod tracers;
mod tracing_util;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: http::Error },

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

    #[snafu(display("Unable to install metrics recorder: {source}"))]
    UnableToInstallMetricsServer {
        source: SetRecorderError<MetricsRecorder>,
    },

    #[snafu(display("Unable to start local metrics: {source}"))]
    UnableToStartLocalMetrics { source: spice_metrics::Error },

    #[snafu(display("Unable to track query history: {source}"))]
    UnableToTrackQueryHistory { source: query_history::Error },

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type EmbeddingModelStore = HashMap<String, RwLock<Box<dyn Embed>>>;

#[derive(Clone, Copy)]
pub struct LogErrors(pub bool);

#[derive(Clone)]
pub struct Runtime {
    instance_name: String,
    app: Arc<RwLock<Option<App>>>,
    df: Arc<DataFusion>,
    models: Arc<RwLock<HashMap<String, Model>>>,
    llms: Arc<RwLock<LLMModelStore>>,
    embeds: Arc<RwLock<EmbeddingModelStore>>,
    pods_watcher: Arc<RwLock<Option<podswatcher::PodsWatcher>>>,
    secrets: Arc<RwLock<secrets::Secrets>>,
    datasets_health_monitor: Option<Arc<DatasetsHealthMonitor>>,
    metrics_endpoint: Option<SocketAddr>,
    metrics_handle: Option<PrometheusHandle>,
    tls_config: Option<Arc<TlsConfig>>,

    autoload_extensions: Arc<HashMap<String, Box<dyn ExtensionFactory>>>,
    extensions: Arc<RwLock<HashMap<String, Arc<dyn Extension>>>>,
    spaced_tracer: Arc<tracers::SpacedTracer>,
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
    pub async fn start_servers(&self, config: Config) -> Result<()> {
        self.register_metrics_table(self.metrics_handle.clone())
            .await?;

        let http_server_future = http::start(
            config.http_bind_address,
            Arc::clone(&self.app),
            Arc::clone(&self.df),
            Arc::clone(&self.models),
            Arc::clone(&self.llms),
            Arc::clone(&self.embeds),
            config.clone().into(),
            self.metrics_endpoint,
            self.tls_config.clone(),
        );

        // Spawn the metrics server in the background
        let metrics_endpoint = self.metrics_endpoint;
        let metrics_handle = self.metrics_handle.clone();
        let tls_config = self.tls_config.clone();
        tokio::spawn(async move {
            if let Err(e) =
                metrics_server::start(metrics_endpoint, metrics_handle, tls_config).await
            {
                tracing::error!("Prometheus metrics server error: {e}");
            }
        });

        let flight_server_future = flight::start(
            config.flight_bind_address,
            Arc::clone(&self.df),
            self.tls_config.clone(),
        );
        let open_telemetry_server_future = opentelemetry::start(
            config.open_telemetry_bind_address,
            Arc::clone(&self.df),
            self.tls_config.clone(),
        );
        let pods_watcher_future = self.start_pods_watcher();

        tokio::select! {
            http_res = http_server_future => http_res.context(UnableToStartHttpServerSnafu),
            flight_res = flight_server_future => flight_res.context(UnableToStartFlightServerSnafu),
            open_telemetry_res = open_telemetry_server_future => open_telemetry_res.context(UnableToStartOpenTelemetryServerSnafu),
            pods_watcher_res = pods_watcher_future => pods_watcher_res.context(UnableToInitializePodsWatcherSnafu),
            () = shutdown_signal() => {
                tracing::info!("Goodbye!");
                Ok(())
            },
        }
    }

    /// Will load all of the components of the Runtime, including `secret_stores`, `catalogs`, `datasets`, `models`, and `embeddings`.
    ///
    /// The future returned by this function will not resolve until all components have been loaded.
    pub async fn load_components(&self) {
        self.load_secrets().await;
        self.start_extensions().await;

        #[cfg(feature = "models")]
        self.load_embeddings().await; // Must be loaded before datasets

        let mut futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![
            Box::pin(async {
                if let Err(err) = self.init_query_history().await {
                    tracing::warn!("Creating internal query history table: {err}");
                };
            }),
            Box::pin(self.init_results_cache()),
            Box::pin(self.load_datasets()),
            Box::pin(self.load_catalogs()),
        ];

        if cfg!(feature = "models") {
            futures.push(Box::pin(self.load_models()));
        }

        join_all(futures).await;

        self.df.mark_initial_load_complete();
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

    async fn load_secrets(&self) {
        measure_scope_ms!("load_secret_stores");
        let mut secrets = self.secrets.write().await;

        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            if let Err(e) = secrets.load_from(&app.secrets).await {
                tracing::error!("Error loading secret stores: {e}");
            };
        }
    }

    fn datasets_iter(app: &App) -> impl Iterator<Item = Result<Dataset>> + '_ {
        app.datasets.iter().cloned().map(Dataset::try_from)
    }

    fn catalogs_iter(app: &App) -> impl Iterator<Item = Result<Catalog>> + '_ {
        app.catalogs.iter().cloned().map(Catalog::try_from)
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    fn get_valid_datasets(app: &App, log_errors: LogErrors) -> Vec<Dataset> {
        Self::datasets_iter(app)
            .zip(&app.datasets)
            .filter_map(|(ds, spicepod_ds)| match ds {
                Ok(ds) => Some(ds),
                Err(e) => {
                    if log_errors.0 {
                        status::update_dataset(
                            &TableReference::parse_str(&spicepod_ds.name),
                            status::ComponentStatus::Error,
                        );
                        metrics::counter!("datasets_load_error").increment(1);
                        tracing::error!(dataset = &spicepod_ds.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    /// Returns a list of valid catalogs from the given App, skipping any that fail to parse and logging an error for them.
    fn get_valid_catalogs(app: &App, log_errors: LogErrors) -> Vec<Catalog> {
        Self::catalogs_iter(app)
            .zip(&app.catalogs)
            .filter_map(|(catalog, spicepod_catalog)| match catalog {
                Ok(catalog) => Some(catalog),
                Err(e) => {
                    if log_errors.0 {
                        status::update_catalog(
                            &spicepod_catalog.name,
                            status::ComponentStatus::Error,
                        );
                        metrics::counter!("catalogs_load_error").increment(1);
                        tracing::error!(catalog = &spicepod_catalog.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    /// Returns a list of valid views from the given App, skipping any that fail to parse and logging an error for them.
    fn get_valid_views(app: &App, log_errors: LogErrors) -> Vec<View> {
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
                            metrics::counter!("views_load_error").increment(1);
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
                        metrics::counter!("views_load_error").increment(1);
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
            status::update_catalog(&catalog.name, status::ComponentStatus::Initializing);
            futures.push(self.load_catalog(catalog));
        }

        let _ = join_all(futures).await;
    }

    async fn load_datasets(&self) {
        let app_lock = self.app.read().await;
        let Some(app) = app_lock.as_ref() else {
            return;
        };

        let valid_datasets = Self::get_valid_datasets(app, LogErrors(true));
        let mut futures = vec![];
        for ds in &valid_datasets {
            status::update_dataset(&ds.name, status::ComponentStatus::Initializing);
            futures.push(self.load_dataset(ds));
        }

        if let Some(parallel_num) = app.runtime.num_of_parallel_loading_at_start_up {
            let stream = futures::stream::iter(futures).buffer_unordered(parallel_num);
            let _ = stream.collect::<Vec<_>>().await;
            return;
        }

        let _ = join_all(futures).await;

        // After all datasets have loaded, load the views.
        self.load_views(app, &valid_datasets);
    }

    fn load_views(&self, app: &App, valid_datasets: &[Dataset]) {
        let views: Vec<View> = Self::get_valid_views(app, LogErrors(true));

        for view in &views {
            if let Err(e) = self.load_view(view, valid_datasets) {
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
                    status::update_catalog(catalog_name, status::ComponentStatus::Error);
                    metrics::counter!("catalogs_load_error").increment(1);
                    warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = self.register_catalog(catalog, connector).await {
                tracing::error!("Unable to register catalog {}: {err}", &catalog.name);
                return Err(RetryError::transient(err));
            };

            status::update_catalog(&catalog.name, status::ComponentStatus::Ready);

            Ok(())
        })
        .await;
    }

    // Caller must set `status::update_dataset(...` before calling `load_dataset`. This function will set error/ready statuses appropriately.`
    async fn load_dataset(&self, ds: &Dataset) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

        let _ = retry(retry_strategy, || async {
            let connector = match self.load_dataset_connector(ds).await {
                Ok(connector) => connector,
                Err(err) => {
                    let ds_name = &ds.name;
                    status::update_dataset(ds_name, status::ComponentStatus::Error);
                    metrics::counter!("datasets_load_error").increment(1);
                    warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = self.register_loaded_dataset(ds, connector, None).await {
                return Err(RetryError::transient(err));
            };

            status::update_dataset(&ds.name, status::ComponentStatus::Ready);

            Ok(())
        })
        .await;
    }

    fn load_view(&self, view: &View, all_datasets: &[Dataset]) -> Result<()> {
        let existing_tables = all_datasets
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<TableReference>>();

        if !verify_dependent_tables(view, &existing_tables) {
            return UnableToCreateViewSnafu {
                reason: "One or more tables in the view's SQL statement do not exist.".to_string(),
            }
            .fail();
        }

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
        let data_connector: Arc<dyn DataConnector> =
            match self.get_dataconnector_from_source(&source, params).await {
                Ok(data_connector) => data_connector,
                Err(err) => {
                    let catalog_name = &catalog.name;
                    status::update_catalog(catalog_name, status::ComponentStatus::Error);
                    metrics::counter!("catalogs_load_error").increment(1);
                    warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
                    return UnableToLoadDatasetConnectorSnafu {
                        dataset: catalog_name.clone(),
                    }
                    .fail();
                }
            };

        Ok(data_connector)
    }

    async fn load_dataset_connector(&self, ds: &Dataset) -> Result<Arc<dyn DataConnector>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let ds = ds.clone();

        let source = ds.source();
        let params = ds.params.clone();
        let data_connector: Arc<dyn DataConnector> =
            match self.get_dataconnector_from_source(&source, params).await {
                Ok(data_connector) => data_connector,
                Err(err) => {
                    let ds_name = &ds.name;
                    status::update_dataset(ds_name, status::ComponentStatus::Error);
                    metrics::counter!("datasets_load_error").increment(1);
                    warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
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
        ds: &Dataset,
        data_connector: Arc<dyn DataConnector>,
        accelerated_table: Option<AcceleratedTable>,
    ) -> Result<()> {
        let ds = ds.clone();
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
                status::update_dataset(&ds.name, status::ComponentStatus::Error);
                metrics::counter!("datasets_load_error").increment(1);
                warn_spaced!(spaced_tracer, "{}{err}", "");
                return UnableToLoadDatasetConnectorSnafu {
                    dataset: ds.name.clone(),
                }
                .fail();
            }
        };

        match self
            .register_dataset(
                &ds,
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
                let engine = ds.acceleration.map_or_else(
                    || "None".to_string(),
                    |acc| {
                        if acc.enabled {
                            acc.engine.to_string()
                        } else {
                            "None".to_string()
                        }
                    },
                );
                metrics::gauge!("datasets_count", "engine" => engine).increment(1.0);
                status::update_dataset(&ds.name, status::ComponentStatus::Ready);

                Ok(())
            }
            Err(err) => {
                status::update_dataset(&ds.name, status::ComponentStatus::Error);
                metrics::counter!("datasets_load_error").increment(1);
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

            if let Err(e) = self.df.remove_table(&ds.name) {
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
        metrics::gauge!("datasets_count", "engine" => engine).decrement(1.0);
    }

    async fn update_dataset(&self, ds: &Dataset) {
        status::update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        match self.load_dataset_connector(ds).await {
            Ok(connector) => {
                // File accelerated datasets don't support hot reload.
                if Self::accelerated_dataset_supports_hot_reload(ds, &*connector) {
                    tracing::info!("Updating accelerated dataset {}...", &ds.name);
                    if let Ok(()) = &self
                        .reload_accelerated_dataset(ds, Arc::clone(&connector))
                        .await
                    {
                        status::update_dataset(&ds.name, status::ComponentStatus::Ready);
                        return;
                    }
                    tracing::debug!("Failed to create accelerated table for dataset {}, falling back to full dataset reload", ds.name);
                }

                self.remove_dataset(ds).await;

                if let Ok(()) = self
                    .register_loaded_dataset(ds, Arc::clone(&connector), None)
                    .await
                {
                    status::update_dataset(&ds.name, status::ComponentStatus::Ready);
                } else {
                    status::update_dataset(&ds.name, status::ComponentStatus::Error);
                }
            }
            Err(e) => {
                tracing::error!("Unable to update dataset {}: {e}", ds.name);
                status::update_dataset(&ds.name, status::ComponentStatus::Error);
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
        ds: &Dataset,
        connector: Arc<dyn DataConnector>,
    ) -> Result<()> {
        let read_table = connector.read_provider(ds).await.map_err(|_| {
            UnableToLoadDatasetConnectorSnafu {
                dataset: ds.name.clone(),
            }
            .build()
        })?;

        // create new accelerated table for updated data connector
        let (accelerated_table, is_ready) = self
            .df
            .create_accelerated_table(ds, Arc::clone(&connector), read_table, self.secrets())
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
    ) -> Result<Arc<dyn DataConnector>> {
        let secret_map = self.get_params_with_secrets(&params).await;

        match dataconnector::create_new_connector(source, secret_map, self.secrets()).await {
            Some(dc) => dc.context(UnableToInitializeDataConnectorSnafu {}),
            None => UnknownDataConnectorSnafu {
                data_connector: source,
            }
            .fail(),
        }
    }

    async fn register_dataset(
        &self,
        ds: impl Borrow<Dataset>,
        register_dataset_ctx: RegisterDatasetContext,
    ) -> Result<()> {
        let RegisterDatasetContext {
            data_connector,
            federated_read_table,
            source,
            accelerated_table,
        } = register_dataset_ctx;

        let ds = ds.borrow();

        let replicate = ds.replication.as_ref().map_or(false, |r| r.enabled);

        // Only wrap data connector when necessary.
        let connector = if ds.embeddings.is_empty() {
            data_connector
        } else {
            Arc::new(EmbeddingConnector::new(
                data_connector,
                Arc::clone(&self.embeds),
            )) as Arc<dyn DataConnector>
        };

        // FEDERATED TABLE
        if !ds.is_accelerated() {
            if ds.mode() == dataset::Mode::ReadWrite && !replicate {
                // A federated dataset was configured as ReadWrite, but the replication setting wasn't set - error out.
                FederatedReadWriteTableWithoutReplicationSnafu.fail()?;
            }

            return self
                .df
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
                });
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
        let mut l = try_to_chat_model(&m, &params)
            .boxed()
            .context(UnableToInitializeLlmSnafu)?;

        l.health()
            .await
            .boxed()
            .context(UnableToInitializeLlmSnafu)?;
        Ok(l)
    }

    /// Loads a specific Embedding model from the spicepod. If an error occurs, no retry attempt is made.
    async fn load_embedding(&self, in_embed: &Embeddings) -> Result<Box<dyn Embed>> {
        let params_with_secrets = self.get_params_with_secrets(&in_embed.params).await;

        let mut l = try_to_embedding(in_embed, &params_with_secrets)
            .boxed()
            .context(UnableToInitializeEmbeddingModelSnafu)?;
        l.health()
            .await
            .boxed()
            .context(UnableToInitializeEmbeddingModelSnafu)?;
        Ok(l)
    }

    async fn load_embeddings(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for in_embed in &app.embeddings {
                status::update_embedding(&in_embed.name, status::ComponentStatus::Initializing);
                match self.load_embedding(in_embed).await {
                    Ok(e) => {
                        let mut embeds_map = self.embeds.write().await;
                        embeds_map.insert(in_embed.name.clone(), e.into());
                        tracing::info!("Embedding [{}] ready to embed", in_embed.name);
                        metrics::gauge!("embeddings_count", "embeddings" => in_embed.name.clone(), "source" => in_embed.get_prefix().map(|x| x.to_string()).unwrap_or_default()).increment(1.0);
                        status::update_embedding(&in_embed.name, status::ComponentStatus::Ready);
                    }
                    Err(e) => {
                        metrics::counter!("embeddings_load_error").increment(1);
                        status::update_embedding(&in_embed.name, status::ComponentStatus::Error);
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
        if let Some(app) = app_lock.as_ref() {
            for model in &app.models {
                status::update_model(&model.name, status::ComponentStatus::Initializing);
                self.load_model(model).await;
            }
        }
    }

    // Caller must set `status::update_model(...` before calling `load_model`. This function will set error/ready statues appropriately.`
    async fn load_model(&self, m: &SpicepodModel) {
        let source = m.get_source();
        let source_str = source.clone().map(|s| s.to_string()).unwrap_or_default();
        let model = m.clone();
        measure_scope_ms!("load_model", "model" => m.name, "source" => source_str);
        tracing::info!("Loading model [{}] from {}...", m.name, m.from);

        let params = self.get_params_with_secrets(&m.params).await;

        let model_type = m.model_type();
        tracing::trace!("Model type for {} is {:#?}", m.name, model_type.clone());
        let result: Result<(), String> = match model_type {
            Some(ModelType::Llm) => match self.load_llm(m.clone(), params).await {
                Ok(l) => {
                    let mut llm_map = self.llms.write().await;
                    llm_map.insert(m.name.clone(), l.into());
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
                metrics::gauge!("models_count", "model" => m.name.clone(), "source" => source_str)
                    .increment(1.0);
                status::update_model(&model.name, status::ComponentStatus::Ready);
            }
            Err(e) => {
                metrics::counter!("models_load_error").increment(1);
                status::update_model(&model.name, status::ComponentStatus::Error);
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
        metrics::gauge!("models_count", "llm" => m.name.clone(), "source" => m.get_source().map(|s| s.to_string()).unwrap_or_default())
            .decrement(1.0);
    }

    async fn update_model(&self, m: &SpicepodModel) {
        status::update_model(&m.name, status::ComponentStatus::Refreshing);
        self.remove_model(m).await;
        self.load_model(m).await;
    }

    async fn register_metrics_table(&self, with_metrics: Option<PrometheusHandle>) -> Result<()> {
        if let Some(handle) = with_metrics {
            let mut recorder = MetricsRecorder::new(handle);

            let table_reference = get_metrics_table_reference();
            let metrics_table = self.df.get_table(table_reference).await;

            if let Some(metrics_table) = metrics_table {
                recorder.set_remote_schema(Arc::new(Some(metrics_table.schema())));
            } else {
                tracing::debug!("Registering local metrics table");
                MetricsRecorder::register_metrics_table(&Arc::clone(&self.df))
                    .await
                    .context(UnableToStartLocalMetricsSnafu)?;
            }

            recorder.start(self.instance_name.clone(), &self.df);
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
                        status::update_catalog(
                            &catalog.name,
                            status::ComponentStatus::Initializing,
                        );
                        self.load_catalog(catalog).await;
                    }
                }

                // Check for new and updated datasets
                let valid_datasets = Self::get_valid_datasets(&new_app, LogErrors(true));
                let existing_datasets = Self::get_valid_datasets(current_app, LogErrors(false));

                for ds in &valid_datasets {
                    if let Some(current_ds) = existing_datasets.iter().find(|d| d.name == ds.name) {
                        if ds != current_ds {
                            self.update_dataset(ds).await;
                        }
                    } else {
                        status::update_dataset(&ds.name, status::ComponentStatus::Initializing);
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
                        status::update_dataset(&ds.name, status::ComponentStatus::Disabled);
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
                        status::update_model(&model.name, status::ComponentStatus::Initializing);
                        self.load_model(model).await;
                    }
                }

                // Remove models that are no longer in the app
                for model in &current_app.models {
                    if !new_app.models.iter().any(|m| m.name == model.name) {
                        status::update_model(&model.name, status::ComponentStatus::Disabled);
                        self.remove_model(model).await;
                    }
                }

                *current_app = new_app;
            } else {
                *app_lock = Some(new_app);
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

        match QueryResultsCacheProvider::new(cache_config) {
            Ok(cache_provider) => {
                tracing::info!("Initialized results cache; {cache_provider}");
                self.datafusion().set_cache_provider(cache_provider);
            }
            Err(e) => {
                tracing::warn!("Failed to initialize results cache: {e}");
            }
        };
    }

    pub async fn init_query_history(&self) -> Result<()> {
        let query_history_table_reference = TableReference::partial(
            SPICE_RUNTIME_SCHEMA,
            query_history::DEFAULT_QUERY_HISTORY_TABLE,
        );
        match query_history::instantiate_query_history_table().await {
            Ok(table) => self
                .df
                .register_runtime_table(query_history_table_reference, table)
                .context(UnableToCreateBackendSnafu),
            Err(err) => Err(Error::UnableToTrackQueryHistory { source: err }),
        }
    }
}

fn verify_dependent_tables(view: &View, existing_tables: &[TableReference]) -> bool {
    let dependent_tables = match get_view_dependent_tables(view) {
        Ok(tables) => tables,
        Err(err) => {
            tracing::error!(
                "Failed to get dependent tables for view {}: {}",
                &view.name,
                err
            );
            return false;
        }
    };

    for tbl in &dependent_tables {
        if !existing_tables.contains(tbl) {
            tracing::error!(
                "Failed to load view {}. Dependent table {} not found",
                &view.name,
                &tbl
            );
            return false;
        }
    }

    true
}

fn get_view_dependent_tables(view: impl Borrow<View>) -> Result<Vec<TableReference>> {
    let view = view.borrow();

    let statements = DFParser::parse_sql_with_dialect(view.sql.as_str(), &PostgreSqlDialect {})
        .context(UnableToParseSqlSnafu)?;

    if statements.len() != 1 {
        return UnableToCreateViewSnafu {
            reason: format!(
                "Expected 1 statement to create view from, received {}",
                statements.len()
            )
            .to_string(),
        }
        .fail();
    }

    Ok(get_dependent_table_names(&statements[0]))
}

fn get_dependent_table_names(statement: &parser::Statement) -> Vec<TableReference> {
    let mut table_names = Vec::new();
    let mut cte_names = HashSet::new();

    if let parser::Statement::Statement(statement) = statement.clone() {
        if let ast::Statement::Query(statement) = *statement {
            // Collect names of CTEs
            if let Some(with) = statement.with {
                for table in with.cte_tables {
                    cte_names.insert(TableReference::bare(table.alias.name.to_string()));
                    let cte_table_names = get_dependent_table_names(&parser::Statement::Statement(
                        Box::new(ast::Statement::Query(table.query)),
                    ));
                    // Extend table_names with names found in CTEs if they reference actual tables
                    table_names.extend(cte_table_names);
                }
            }
            // Process the main query body
            if let SetExpr::Select(select_statement) = *statement.body {
                for from in select_statement.from {
                    let mut relations = vec![];
                    relations.push(from.relation.clone());
                    for join in from.joins {
                        relations.push(join.relation.clone());
                    }

                    for relation in relations {
                        match relation {
                            TableFactor::Table {
                                name,
                                alias: _,
                                args: _,
                                with_hints: _,
                                version: _,
                                partitions: _,
                            } => {
                                table_names.push(name.to_string().into());
                            }
                            TableFactor::Derived {
                                lateral: _,
                                subquery,
                                alias: _,
                            } => {
                                table_names.extend(get_dependent_table_names(
                                    &parser::Statement::Statement(Box::new(ast::Statement::Query(
                                        subquery,
                                    ))),
                                ));
                            }
                            _ => (),
                        }
                    }
                }
            }
        }
    }

    // Filter out CTEs and temporary views (aliases of subqueries)
    table_names
        .into_iter()
        .filter(|name| !cte_names.contains(name))
        .collect()
}

pub struct RegisterDatasetContext {
    data_connector: Arc<dyn DataConnector>,
    federated_read_table: Arc<dyn TableProvider>,
    source: String,
    accelerated_table: Option<AcceleratedTable>,
}
