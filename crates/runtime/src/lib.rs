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
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use crate::spice_metrics::MetricsRecorder;
use crate::{dataconnector::DataConnector, datafusion::DataFusion};
use ::datafusion::error::DataFusionError;
use ::datafusion::sql::parser::{self, DFParser};
use ::datafusion::sql::sqlparser::ast::{SetExpr, TableFactor};
use ::datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use ::datafusion::sql::sqlparser::{self, ast};
use ::datafusion::sql::TableReference;
use accelerated_table::AcceleratedTable;
use app::App;
use cache::QueryResultsCacheProvider;
use component::dataset::{self, Dataset};
use config::Config;
use datafusion::SPICE_RUNTIME_SCHEMA;
use llms::nql::Nql;
use metrics::SetRecorderError;
use model::try_to_nql;
use model_components::{model::Model, modelsource::source as model_source};
pub use notify::Error as NotifyError;
use secrets::{spicepod_secret_store_type, Secret};
use snafu::prelude::*;
use spice_metrics::get_metrics_table_reference;
use spicepod::component::model::Model as SpicepodModel;
use tokio::sync::oneshot::error::RecvError;
use tokio::time::sleep;
use tokio::{signal, sync::RwLock};

use crate::extension::{Extension, ExtensionFactory};
pub mod accelerated_table;
pub mod component;
pub mod config;
pub mod dataaccelerator;
pub mod dataconnector;
pub mod datafusion;
pub mod dataupdate;
pub mod execution_plan;
pub mod extension;
mod flight;
mod http;
pub mod internal_table;
pub mod model;
pub mod object_store_registry;
pub mod objectstore;
mod opentelemetry;
pub mod podswatcher;
pub mod query_history;
pub mod spice_metrics;
pub mod status;
pub mod timing;
pub(crate) mod tracers;

pub mod pg_server;


#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: http::Error },

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

    #[snafu(display("Unknown data connector: {data_connector}"))]
    UnknownDataConnector { data_connector: String },

    #[snafu(display("Unable to load secrets for data connector: {data_connector}"))]
    UnableToLoadDataConnectorSecrets { data_connector: String },

    #[snafu(display("Unable to get secret for data connector {data_connector}: {source}"))]
    UnableToGetSecretForDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
        data_connector: String,
    },

    #[snafu(display("Unable to create view: {source}"))]
    InvalidSQLView {
        source: crate::component::dataset::Error,
    },

    #[snafu(display("Unable to attach data connector {data_connector}: {source}"))]
    UnableToAttachDataConnector {
        source: datafusion::Error,
        data_connector: String,
    },

    #[snafu(display("Expected a SQL view statement, received nothing."))]
    ExpectedSqlView,

    #[snafu(display("Unable to parse SQL: {source}"))]
    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Unable to create view: {reason}"))]
    UnableToCreateView { reason: String },

    #[snafu(display(
        "A federated table was configured as read_write without setting replication.enabled = true"
    ))]
    FederatedReadWriteTableWithoutReplication,

    #[snafu(display("An accelerated table was configured as read_write without setting replication.enabled = true"))]
    AcceleratedReadWriteTableWithoutReplication,

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

    //#[cfg(feature = "pg-server")]
    #[snafu(display("Unable to start postgres compatible access layer: {source}"))]
    UnableToStartPGServer { source: pg_server::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type LLMModelStore = HashMap<String, RwLock<Box<dyn Nql>>>;

pub struct Runtime {
    pub app: Arc<RwLock<Option<App>>>,
    pub df: Arc<RwLock<DataFusion>>,
    pub models: Arc<RwLock<HashMap<String, Model>>>,
    pub llms: Arc<RwLock<LLMModelStore>>,
    pub pods_watcher: Option<podswatcher::PodsWatcher>,
    pub secrets_provider: Arc<RwLock<secrets::SecretsProvider>>,

    extensions: Vec<Box<dyn Extension>>,
    spaced_tracer: Arc<tracers::SpacedTracer>,
}

impl Runtime {
    #[must_use]
    pub async fn new(
        app: Option<app::App>,
        df: Arc<RwLock<DataFusion>>,
        extension_factories: Arc<Vec<Box<dyn ExtensionFactory>>>,
    ) -> Self {
        dataconnector::register_all().await;
        dataaccelerator::register_all().await;

        let mut rt = Runtime {
            app: Arc::new(RwLock::new(app)),
            df,
            models: Arc::new(RwLock::new(HashMap::new())),
            llms: Arc::new(RwLock::new(HashMap::new())),
            pods_watcher: None,
            secrets_provider: Arc::new(RwLock::new(secrets::SecretsProvider::new())),
            spaced_tracer: Arc::new(tracers::SpacedTracer::new(Duration::from_secs(15))),
            extensions: vec![],
        };

        let mut extensions: Vec<Box<dyn Extension>> = vec![];
        for factory in extension_factories.iter() {
            let mut extension = factory.create();
            let extension_name = extension.name();
            if let Err(err) = extension.initialize(&mut rt).await {
                tracing::warn!("Failed to initialize extension {extension_name}: {err}");
            } else {
                extensions.push(extension);
            };
        }

        rt.extensions = extensions;

        rt
    }

    #[must_use]
    pub fn datafusion(&self) -> Arc<RwLock<DataFusion>> {
        Arc::clone(&self.df)
    }

    pub async fn start_extensions(&mut self) {
        let mut extensions: Vec<Box<dyn Extension>> = std::mem::take(&mut self.extensions);
        for extension in &mut extensions {
            if let Err(err) = extension.on_start(self).await {
                tracing::warn!("Failed to start extension: {err}");
            }
        }

        self.extensions = extensions;
    }

    pub fn with_pods_watcher(&mut self, pods_watcher: podswatcher::PodsWatcher) {
        self.pods_watcher = Some(pods_watcher);
    }

    pub async fn load_secrets(&self) {
        measure_scope_ms!("load_secrets");
        let mut secret_store = self.secrets_provider.write().await;

        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            let Some(secret_store_type) = spicepod_secret_store_type(&app.secrets.store) else {
                return;
            };

            secret_store.store = secret_store_type;
        }

        if let Err(e) = secret_store.load_secrets().await {
            tracing::warn!("Unable to load secrets: {}", e);
        }
    }

    fn datasets_iter(app: &App) -> impl Iterator<Item = Result<Dataset>> + '_ {
        app.datasets.iter().cloned().map(Dataset::try_from)
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    fn get_valid_datasets(app: &App, log_failures: bool) -> Vec<Dataset> {
        Self::datasets_iter(app)
            .zip(&app.datasets)
            .filter_map(|(ds, spicepod_ds)| match ds {
                Ok(ds) => Some(ds),
                Err(e) => {
                    if log_failures {
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

    pub async fn load_datasets(&self) {
        let app_lock = self.app.read().await;
        let Some(app) = app_lock.as_ref() else {
            return;
        };

        let valid_datasets = Self::get_valid_datasets(app, true);
        for ds in &valid_datasets {
            status::update_dataset(&ds.name, status::ComponentStatus::Initializing);
            self.load_dataset(ds, &valid_datasets).await;
        }
    }

    // Caller must set `status::update_dataset(...` before calling `load_dataset`. This function will set error/ready statuses appropriately.`
    pub async fn load_dataset(&self, ds: &Dataset, all_datasets: &[Dataset]) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        loop {
            let connector = match self.load_dataset_connector(ds, all_datasets).await {
                Ok(connector) => connector,
                Err(err) => {
                    status::update_dataset(&ds.name, status::ComponentStatus::Error);
                    metrics::counter!("datasets_load_error").increment(1);
                    warn_spaced!(spaced_tracer, "{}{err}", "");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            if let Ok(()) = self.register_loaded_dataset(ds, connector, None).await {
            } else {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            status::update_dataset(&ds.name, status::ComponentStatus::Ready);
            break;
        }
    }

    pub async fn load_dataset_connector(
        &self,
        ds: &Dataset,
        all_datasets: &[Dataset],
    ) -> Result<Arc<dyn DataConnector>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let shared_secrets_provider: Arc<RwLock<secrets::SecretsProvider>> =
            Arc::clone(&self.secrets_provider);

        let ds = ds.clone();

        let existing_tables = all_datasets
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<TableReference>>();

        let secrets_provider = shared_secrets_provider.read().await;

        if !verify_dependent_tables(&ds, &existing_tables) {
            status::update_dataset(&ds.name, status::ComponentStatus::Error);
            metrics::counter!("datasets_load_error").increment(1);
            return UnableToLoadDatasetConnectorSnafu {
                dataset: ds.name.clone(),
            }
            .fail();
        }

        let source = ds.source();
        let params = Arc::new(ds.params.clone());
        let data_connector: Arc<dyn DataConnector> = match Runtime::get_dataconnector_from_source(
            &source,
            &secrets_provider,
            Arc::clone(&params),
        )
        .await
        {
            Ok(data_connector) => data_connector,
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

        Ok(data_connector)
    }

    pub async fn register_loaded_dataset(
        &self,
        ds: &Dataset,
        data_connector: Arc<dyn DataConnector>,
        accelerated_table: Option<AcceleratedTable>,
    ) -> Result<()> {
        let df = Arc::clone(&self.df);
        let ds = ds.clone();
        let source = ds.source();
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let shared_secrets_provider: Arc<RwLock<secrets::SecretsProvider>> =
            Arc::clone(&self.secrets_provider);

        // test dataset connectivity by attempting to get a read provider
        if let Err(err) = data_connector.read_provider(&ds).await {
            status::update_dataset(&ds.name, status::ComponentStatus::Error);
            metrics::counter!("datasets_load_error").increment(1);
            warn_spaced!(spaced_tracer, "{}{err}", "");
            return UnableToLoadDatasetConnectorSnafu {
                dataset: ds.name.clone(),
            }
            .fail();
        }

        let data_connector = Arc::clone(&data_connector);
        match Runtime::register_dataset(
            &ds,
            data_connector,
            Arc::clone(&df),
            &source,
            Arc::clone(&shared_secrets_provider),
            accelerated_table,
        )
        .await
        {
            Ok(()) => {
                tracing::info!("Registered dataset {}", &ds.name);
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

    pub async fn remove_dataset(&self, ds: &Dataset) {
        let mut df = self.df.write().await;

        if df.table_exists(ds.name.clone()) {
            if let Err(e) = df.remove_table(&ds.name) {
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

    pub async fn update_dataset(&self, ds: &Dataset, all_datasets: &[Dataset]) {
        status::update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        if let Ok(connector) = self.load_dataset_connector(ds, all_datasets).await {
            tracing::info!("Updating accelerated dataset {}...", &ds.name);

            // File accelerated datasets don't support hot reload.
            if ds.is_accelerated() {
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
        } else {
            status::update_dataset(&ds.name, status::ComponentStatus::Error);
        }
    }

    async fn reload_accelerated_dataset(
        &self,
        ds: &Dataset,
        connector: Arc<dyn DataConnector>,
    ) -> Result<()> {
        let acceleration_secret =
            Runtime::get_acceleration_secret(ds, Arc::clone(&self.secrets_provider)).await?;

        // create new accelerated table for updated data connector
        let (accelerated_table, is_ready) = self
            .df
            .read()
            .await
            .create_accelerated_table(ds, Arc::clone(&connector), acceleration_secret)
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
        source: &str,
        secrets_provider: &secrets::SecretsProvider,
        params: Arc<HashMap<String, String>>,
    ) -> Result<Arc<dyn DataConnector>> {
        let secret = secrets_provider.get_secret(source).await.context(
            UnableToGetSecretForDataConnectorSnafu {
                data_connector: source,
            },
        )?;

        match dataconnector::create_new_connector(source, secret, params).await {
            Some(dc) => dc.context(UnableToInitializeDataConnectorSnafu {}),
            None => UnknownDataConnectorSnafu {
                data_connector: source,
            }
            .fail(),
        }
    }

    async fn get_acceleration_secret(
        ds: &Dataset,
        secrets_provider: Arc<RwLock<secrets::SecretsProvider>>,
    ) -> Result<Option<Secret>> {
        let source = ds.source();
        let replicate = ds.replication.as_ref().map_or(false, |r| r.enabled);

        let acceleration_settings =
            ds.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: ds.name.to_string(),
                })?;

        if ds.mode() == dataset::Mode::ReadWrite && !replicate {
            AcceleratedReadWriteTableWithoutReplicationSnafu.fail()?;
        }

        let accelerator_engine = acceleration_settings.engine;
        let secret_key = acceleration_settings
            .engine_secret
            .clone()
            .unwrap_or(format!("{accelerator_engine}_engine").to_lowercase());

        let secrets_provider_read_guard = secrets_provider.read().await;
        let acceleration_secret = secrets_provider_read_guard
            .get_secret(&secret_key)
            .await
            .context(UnableToGetSecretForDataConnectorSnafu {
                data_connector: source,
            })?;

        drop(secrets_provider_read_guard);

        Ok(acceleration_secret)
    }

    async fn register_dataset(
        ds: impl Borrow<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        df: Arc<RwLock<DataFusion>>,
        source: &str,
        secrets_provider: Arc<RwLock<secrets::SecretsProvider>>,
        accelerated_table: Option<AcceleratedTable>,
    ) -> Result<()> {
        let ds = ds.borrow();

        // VIEW
        if let Some(view_sql) = ds.view_sql() {
            let view_sql = view_sql.context(InvalidSQLViewSnafu)?;
            df.write()
                .await
                .register_table(ds, datafusion::Table::View(view_sql))
                .await
                .context(UnableToAttachViewSnafu)?;
            return Ok(());
        }

        let replicate = ds.replication.as_ref().map_or(false, |r| r.enabled);

        // FEDERATED TABLE
        if !ds.is_accelerated() {
            if ds.mode() == dataset::Mode::ReadWrite && !replicate {
                // A federated dataset was configured as ReadWrite, but the replication setting wasn't set - error out.
                FederatedReadWriteTableWithoutReplicationSnafu.fail()?;
            }

            return Runtime::register_table(
                df,
                ds,
                datafusion::Table::Federated(data_connector),
                source,
            )
            .await;
        }

        // ACCELERATED TABLE
        let acceleration_settings =
            ds.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: ds.name.to_string(),
                })?;
        let accelerator_engine = acceleration_settings.engine;
        let acceleration_secret = Runtime::get_acceleration_secret(ds, secrets_provider).await?;

        dataaccelerator::get_accelerator_engine(accelerator_engine)
            .await
            .context(AcceleratorEngineNotAvailableSnafu {
                name: accelerator_engine.to_string(),
            })?;

        Runtime::register_table(
            df,
            ds,
            datafusion::Table::Accelerated {
                source: data_connector,
                acceleration_secret,
                accelerated_table,
            },
            source,
        )
        .await
    }

    async fn register_table(
        df: Arc<RwLock<DataFusion>>,
        ds: &Dataset,
        table: datafusion::Table,
        source: &str,
    ) -> Result<()> {
        df.write().await.register_table(ds, table).await.context(
            UnableToAttachDataConnectorSnafu {
                data_connector: source,
            },
        )?;

        Ok(())
    }

    pub async fn load_llms(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for in_llm in &app.llms {
                status::update_llm(&in_llm.name, status::ComponentStatus::Initializing);
                match try_to_nql(in_llm) {
                    Ok(l) => {
                        let mut llm_map = self.llms.write().await;
                        llm_map.insert(in_llm.name.clone(), l.into());
                        tracing::info!("Llm [{}] deployed, ready for inferencing", in_llm.name);
                        metrics::gauge!("llms_count", "llm" => in_llm.name.clone(), "source" => in_llm.get_prefix().map(|x| x.to_string()).unwrap_or_default()).increment(1.0);
                        status::update_llm(&in_llm.name, status::ComponentStatus::Ready);
                    }
                    Err(e) => {
                        metrics::counter!("llms_load_error").increment(1);
                        status::update_llm(&in_llm.name, status::ComponentStatus::Error);
                        tracing::warn!(
                            "Unable to load LLM from spicepod {}, error: {}",
                            in_llm.name,
                            e,
                        );
                    }
                }
            }
        }
    }

    pub async fn load_models(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for model in &app.models {
                status::update_model(&model.name, status::ComponentStatus::Initializing);
                self.load_model(model).await;
            }
        }
    }

    // Caller must set `status::update_model(...` before calling `load_model`. This function will set error/ready statues appropriately.`
    pub async fn load_model(&self, m: &SpicepodModel) {
        measure_scope_ms!("load_model", "model" => m.name, "source" => model_source(&m.from));
        tracing::info!("Loading model [{}] from {}...", m.name, m.from);

        let model = m.clone();
        let source = model_source(model.from.as_str());

        let shared_secrets_provider = Arc::clone(&self.secrets_provider);
        let secrets_provider = shared_secrets_provider.read().await;

        let secret = match secrets_provider
            .get_secret(source.to_string().as_str())
            .await
        {
            Ok(s) => s,
            Err(e) => {
                metrics::counter!("models_load_error").increment(1);
                status::update_model(&model.name, status::ComponentStatus::Error);
                tracing::warn!(
                    "Unable to load runnable model from spicepod {}, error: {}",
                    m.name,
                    e,
                );
                return;
            }
        };

        match Model::load(m.clone(), secret).await {
            Ok(in_m) => {
                let mut model_map = self.models.write().await;
                model_map.insert(m.name.clone(), in_m);
                tracing::info!("Model [{}] deployed, ready for inferencing", m.name);
                metrics::gauge!("models_count", "model" => m.name.clone(), "source" => model_source(&m.from).to_string()).increment(1.0);
                status::update_model(&model.name, status::ComponentStatus::Ready);
            }
            Err(e) => {
                metrics::counter!("models_load_error").increment(1);
                status::update_model(&model.name, status::ComponentStatus::Error);
                tracing::warn!(
                    "Unable to load runnable model from spicepod {}, error: {}",
                    m.name,
                    e,
                );
            }
        }
    }

    pub async fn remove_model(&self, m: &SpicepodModel) {
        let mut model_map = self.models.write().await;
        if !model_map.contains_key(&m.name) {
            tracing::warn!(
                "Unable to unload runnable model {}: model not found",
                m.name,
            );
            return;
        }
        model_map.remove(&m.name);
        tracing::info!("Model [{}] has been unloaded", m.name);
        metrics::gauge!("models_count", "model" => m.name.clone(), "source" => model_source(&m.from).to_string()).decrement(1.0);
    }

    pub async fn update_model(&self, m: &SpicepodModel) {
        status::update_model(&m.name, status::ComponentStatus::Refreshing);
        self.remove_model(m).await;
        self.load_model(m).await;
    }

    pub async fn start_metrics(&mut self, with_metrics: Option<SocketAddr>) -> Result<()> {
        if let Some(metrics_socket) = with_metrics {
            let mut recorder = MetricsRecorder::new(metrics_socket);

            let table_reference = get_metrics_table_reference();
            let metrics_table = self.df.read().await.get_table(table_reference).await;

            if let Some(metrics_table) = metrics_table {
                recorder.set_remote_schema(Arc::new(Some(metrics_table.schema())));
            } else {
                tracing::debug!("Registering local metrics table");
                MetricsRecorder::register_metrics_table(&self.df)
                    .await
                    .context(UnableToStartLocalMetricsSnafu)?;
            }

            recorder.start(&Arc::clone(&self.df));
        }

        Ok(())
    }

    pub async fn start_servers(
        &mut self,
        config: Config,
        with_metrics: Option<SocketAddr>,
    ) -> Result<()> {
        let http_server_future = http::start(
            config.http_bind_address,
            Arc::clone(&self.app),
            Arc::clone(&self.df),
            Arc::clone(&self.models),
            Arc::clone(&self.llms),
            config.clone().into(),
            with_metrics,
        );

        let flight_server_future = flight::start(config.flight_bind_address, Arc::clone(&self.df));
        let open_telemetry_server_future =
            opentelemetry::start(config.open_telemetry_bind_address, Arc::clone(&self.df));
        
        let pg_server_future = pg_server::start(config.pg_bind_address, Arc::clone(&self.df));
        
        
        let pods_watcher_future = self.start_pods_watcher();

        // let df_session = Arc::clone(&self.df.read().await.ctx);


        // #[cfg(feature = "pg-server")]



        tokio::select! {
            http_res = http_server_future => http_res.context(UnableToStartHttpServerSnafu),
            flight_res = flight_server_future => flight_res.context(UnableToStartFlightServerSnafu),
            open_telemetry_res = open_telemetry_server_future => open_telemetry_res.context(UnableToStartOpenTelemetryServerSnafu),
            pods_watcher_res = pods_watcher_future => pods_watcher_res.context(UnableToInitializePodsWatcherSnafu),
            pg_server_res = pg_server_future => pg_server_res.context(UnableToStartPGServerSnafu),
            () = shutdown_signal() => {
                tracing::info!("Goodbye!");
                Ok(())
            },
        }
    }

    pub async fn start_pods_watcher(&mut self) -> notify::Result<()> {
        let Some(mut pods_watcher) = self.pods_watcher.take() else {
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

                // check for new and updated datasets
                let valid_datasets = Self::get_valid_datasets(&new_app, true);
                for ds in &valid_datasets {
                    if let Some(current_ds) = current_app
                        .datasets
                        .iter()
                        .find(|d| TableReference::parse_str(&d.name) == ds.name)
                    {
                        if TableReference::parse_str(&current_ds.name) != ds.name {
                            self.update_dataset(ds, &valid_datasets).await;
                        }
                    } else {
                        status::update_dataset(&ds.name, status::ComponentStatus::Initializing);
                        self.load_dataset(ds, &valid_datasets).await;
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

                *current_app = new_app;
            } else {
                *app_lock = Some(new_app);
            }
        }

        Ok(())
    }

    pub async fn init_results_cache(&self) {
        let app_lock = self.app.read().await;

        let Some(app) = app_lock.as_ref() else {
            return;
        };

        let cache_config = &app.runtime.results_cache;

        if !cache_config.enabled {
            return;
        }

        let cache_provider = match QueryResultsCacheProvider::new(cache_config) {
            Ok(cache_provider) => cache_provider,
            Err(e) => {
                tracing::warn!("Failed to initialize results cache: {e}");
                return;
            }
        };

        tracing::info!("Initialized results cache; {cache_provider}");

        self.df
            .write()
            .await
            .set_cache_provider(Some(Arc::new(cache_provider)));
    }

    pub async fn init_query_history(&self) -> Result<()> {
        let query_history_table_reference = TableReference::partial(
            SPICE_RUNTIME_SCHEMA,
            query_history::DEFAULT_QUERY_HISTORY_TABLE,
        );
        match query_history::instantiate_query_history_table().await {
            Ok(table) => self
                .df
                .write()
                .await
                .register_runtime_table(query_history_table_reference, table)
                .context(UnableToCreateBackendSnafu),
            Err(err) => Err(Error::UnableToTrackQueryHistory { source: err }),
        }
    }
}

fn verify_dependent_tables(ds: &Dataset, existing_tables: &[TableReference]) -> bool {
    if !ds.is_view() {
        return true;
    }

    let dependent_tables = match get_view_dependent_tables(ds) {
        Ok(tables) => tables,
        Err(err) => {
            tracing::error!(
                "Failed to get dependent tables for view {}: {}",
                &ds.name,
                err
            );
            return false;
        }
    };

    for tbl in &dependent_tables {
        if !existing_tables.contains(tbl) {
            tracing::error!(
                "Failed to load {}. Dependent table {} not found",
                &ds.name,
                &tbl
            );
            return false;
        }
    }

    true
}

fn get_view_dependent_tables(dataset: impl Borrow<Dataset>) -> Result<Vec<TableReference>> {
    let ds = dataset.borrow();

    let Some(sql) = ds.view_sql() else {
        return ExpectedSqlViewSnafu.fail();
    };

    let sql = sql.context(InvalidSQLViewSnafu)?;

    let statements = DFParser::parse_sql_with_dialect(sql.as_str(), &PostgreSqlDialect {})
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

async fn shutdown_signal() {
    let ctrl_c = async {
        let signal_result = signal::ctrl_c().await;
        if let Err(err) = signal_result {
            tracing::error!("Failed to listen to shutdown signal: {err}");
        }
    };

    tokio::select! {
        () = ctrl_c => {},
    }
}
