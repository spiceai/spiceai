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
use std::{collections::HashMap, sync::Arc};

use ::datafusion::sql::parser::{self, DFParser};
use ::datafusion::sql::sqlparser::ast::{SetExpr, TableFactor};
use ::datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use ::datafusion::sql::sqlparser::{self, ast};
use app::App;
use config::Config;
use model::Model;
pub use notify::Error as NotifyError;
use secrets::spicepod_secret_store_type;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use spicepod::component::dataset::Mode;
use spicepod::component::model::Model as SpicepodModel;
use std::time::Duration;
use tokio::time::sleep;
use tokio::{signal, sync::RwLock};

use crate::{dataconnector::DataConnector, datafusion::DataFusion};
mod accelerated_table;
pub mod config;
pub mod dataaccelerator;
pub mod dataconnector;
pub mod datafusion;
pub mod dataupdate;
mod flight;
mod http;
pub mod model;
pub mod modelformat;
pub mod modelruntime;
pub mod modelsource;
mod opentelemetry;
pub mod podswatcher;
pub mod status;
pub mod timing;
pub(crate) mod tracers;

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

    #[snafu(display("Unable to initialize data connector {data_connector}: {source}"))]
    UnableToInitializeDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
        data_connector: String,
    },

    #[snafu(display("Unknown data connector: {data_connector}"))]
    UnknownDataConnector { data_connector: String },

    #[snafu(display("Unable to load secrets for data connector: {data_connector}"))]
    UnableToLoadDataConnectorSecrets { data_connector: String },

    #[snafu(display("Unable to create view: {source}"))]
    InvalidSQLView {
        source: spicepod::component::dataset::Error,
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

    #[snafu(display("The accelerator engine {name} is not available"))]
    AcceleratorEngineNotAvailable { name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Runtime {
    pub app: Arc<RwLock<Option<App>>>,
    pub config: config::Config,
    pub df: Arc<RwLock<DataFusion>>,
    pub models: Arc<RwLock<HashMap<String, Model>>>,
    pub pods_watcher: podswatcher::PodsWatcher,
    pub secrets_provider: Arc<RwLock<secrets::SecretsProvider>>,

    spaced_tracer: Arc<tracers::SpacedTracer>,
}

impl Runtime {
    #[must_use]
    pub async fn new(
        config: Config,
        app: Arc<RwLock<Option<app::App>>>,
        df: Arc<RwLock<DataFusion>>,
        pods_watcher: podswatcher::PodsWatcher,
    ) -> Self {
        dataconnector::register_all().await;
        Runtime {
            app,
            config,
            df,
            models: Arc::new(RwLock::new(HashMap::new())),
            pods_watcher,
            secrets_provider: Arc::new(RwLock::new(secrets::SecretsProvider::new())),
            spaced_tracer: Arc::new(tracers::SpacedTracer::new(Duration::from_secs(15))),
        }
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

        if let Err(e) = secret_store.load_secrets() {
            tracing::warn!("Unable to load secrets: {}", e);
        }
    }

    pub async fn load_datasets(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for ds in &app.datasets {
                status::update_dataset(&ds.name, status::ComponentStatus::Initializing);
                self.load_dataset(ds, &app.datasets);
            }
        }
    }

    // Caller must set `status::update_dataset(...` before calling `load_dataset`. This function will set error/ready statuses appropriately.`
    pub fn load_dataset(&self, ds: &Dataset, all_datasets: &[Dataset]) {
        let df = Arc::clone(&self.df);
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let shared_secrets_provider: Arc<RwLock<secrets::SecretsProvider>> =
            Arc::clone(&self.secrets_provider);

        let ds = ds.clone();

        let existing_tables = all_datasets
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<String>>();

        tokio::spawn(async move {
            loop {
                let secrets_provider = shared_secrets_provider.read().await;

                if !verify_dependent_tables(&ds, &existing_tables).await {
                    status::update_dataset(&ds.name, status::ComponentStatus::Error);
                    metrics::counter!("datasets_load_error").increment(1);
                    return;
                }

                let source = ds.source();

                let params = Arc::new(ds.params.clone());
                let data_connector: Arc<dyn DataConnector> =
                    match Runtime::get_dataconnector_from_source(
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
                            warn_spaced!(
                                spaced_tracer,
                                "Failed to get data connector from source for dataset {}, retrying: {err}",
                                &ds.name
                            );
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                match Runtime::register_dataset(
                    &ds,
                    data_connector,
                    Arc::clone(&df),
                    &source,
                    Arc::clone(&shared_secrets_provider),
                )
                .await
                {
                    Ok(()) => (),
                    Err(err) => {
                        status::update_dataset(&ds.name, status::ComponentStatus::Error);
                        metrics::counter!("datasets_load_error").increment(1);
                        warn_spaced!(
                            spaced_tracer,
                            "Failed to initialize data connector for dataset {}, retrying: {err}",
                            &ds.name
                        );
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                tracing::info!("Loaded dataset: {}", &ds.name);
                let engine = ds.acceleration.map_or_else(
                    || "None".to_string(),
                    |acc| {
                        if acc.enabled {
                            acc.engine().to_string()
                        } else {
                            "None".to_string()
                        }
                    },
                );
                metrics::gauge!("datasets_count", "engine" => engine).increment(1.0);
                status::update_dataset(&ds.name, status::ComponentStatus::Ready);
                break;
            }
        });
    }

    pub async fn remove_dataset(&self, ds: &Dataset) {
        let mut df = self.df.write().await;

        if df.table_exists(&ds.name) {
            if let Err(e) = df.remove_table(&ds.name) {
                tracing::warn!("Unable to unload dataset {}: {}", &ds.name, e);
                return;
            }
        }

        tracing::info!("Unloaded dataset: {}", &ds.name);
        let engine = ds.acceleration.as_ref().map_or_else(
            || "None".to_string(),
            |acc| {
                if acc.enabled {
                    acc.engine().to_string()
                } else {
                    "None".to_string()
                }
            },
        );
        metrics::gauge!("datasets_count", "engine" => engine).decrement(1.0);
    }

    pub async fn update_dataset(&self, ds: &Dataset, all_datasets: &[Dataset]) {
        status::update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        self.remove_dataset(ds).await;
        self.load_dataset(ds, all_datasets);
    }

    async fn get_dataconnector_from_source(
        source: &str,
        secrets_provider: &secrets::SecretsProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Arc<dyn DataConnector>> {
        let secret = secrets_provider.get_secret(source).await;

        match source {
            _ => match dataconnector::create_new_connector(source, secret, params).await {
                Some(dc) => dc.context(UnableToInitializeDataConnectorSnafu {
                    data_connector: source,
                }),
                None => UnknownDataConnectorSnafu {
                    data_connector: source,
                }
                .fail(),
            },
        }
    }

    async fn register_dataset(
        ds: impl Borrow<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        df: Arc<RwLock<DataFusion>>,
        source: &str,
        secrets_provider: Arc<RwLock<secrets::SecretsProvider>>,
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

        let secrets_provider_read_guard = secrets_provider.read().await;
        let source_secret = secrets_provider_read_guard.get_secret(&ds.source()).await;
        drop(secrets_provider_read_guard);

        let replicate = ds.replication.as_ref().map_or(false, |r| r.enabled);

        // FEDERATED TABLE
        if ds.acceleration.is_none() || ds.acceleration.as_ref().map_or(false, |acc| !acc.enabled) {
            if ds.mode() == Mode::ReadWrite && !replicate {
                // A federated dataset was configured as ReadWrite, but the replication setting wasn't set - error out.
                FederatedReadWriteTableWithoutReplicationSnafu.fail()?;
            }

            df.write()
                .await
                .register_table(
                    ds,
                    datafusion::Table::Federated {
                        source: data_connector,
                        source_secret,
                    },
                )
                .await
                .context(UnableToAttachDataConnectorSnafu {
                    data_connector: source,
                })?;
            return Ok(());
        }

        // ACCELERATED TABLE
        let acceleration_settings =
            ds.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: ds.name.to_string(),
                })?;

        if ds.mode() == Mode::ReadWrite && !replicate {
            AcceleratedReadWriteTableWithoutReplicationSnafu.fail()?;
        }

        let accelerator_engine = acceleration_settings.engine();
        let secret_key = acceleration_settings
            .engine_secret
            .clone()
            .unwrap_or(format!("{:?}_engine", accelerator_engine).to_lowercase());

        let secrets_provider_read_guard = secrets_provider.read().await;
        let acceleration_secret = secrets_provider_read_guard.get_secret(&secret_key).await;
        drop(secrets_provider_read_guard);

        let Some(accelerator) = dataaccelerator::get_accelerator_engine(&accelerator_engine).await
        else {
            return Err(Error::AcceleratorEngineNotAvailable {
                name: accelerator_engine,
            });
        };

        df.write()
            .await
            .register_table(
                ds,
                datafusion::Table::Accelerated {
                    source: data_connector,
                    source_secret,
                    accelerator,
                    acceleration_secret,
                },
            )
            .await
            .context(UnableToAttachDataConnectorSnafu {
                data_connector: source,
            })?;

        Ok(())
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
        measure_scope_ms!("load_model", "model" => m.name, "source" => model::source(&m.from));
        tracing::info!("Loading model [{}] from {}...", m.name, m.from);
        let mut model_map = self.models.write().await;

        let model = m.clone();
        let source = model::source(&model.from);

        let shared_secrets_provider = Arc::clone(&self.secrets_provider);
        let secrets_provider = shared_secrets_provider.read().await;

        match Model::load(
            m.clone(),
            secrets_provider.get_secret(source.as_str()).await,
        )
        .await
        {
            Ok(in_m) => {
                model_map.insert(m.name.clone(), in_m);
                tracing::info!("Model [{}] deployed, ready for inferencing", m.name);
                metrics::gauge!("models_count", "model" => m.name.clone(), "source" => model::source(&m.from)).increment(1.0);
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
        metrics::gauge!("models_count", "model" => m.name.clone(), "source" => model::source(&m.from)).decrement(1.0);
    }

    pub async fn update_model(&self, m: &SpicepodModel) {
        status::update_model(&m.name, status::ComponentStatus::Refreshing);
        self.remove_model(m).await;
        self.load_model(m).await;
    }

    pub async fn start_servers(&mut self, with_metrics: Option<SocketAddr>) -> Result<()> {
        let http_server_future = http::start(
            self.config.http_bind_address,
            self.app.clone(),
            self.df.clone(),
            self.models.clone(),
            self.config.clone().into(),
            with_metrics,
        );

        let flight_server_future = flight::start(self.config.flight_bind_address, self.df.clone());
        let open_telemetry_server_future =
            opentelemetry::start(self.config.open_telemetry_bind_address, self.df.clone());
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

    pub async fn start_pods_watcher(&mut self) -> notify::Result<()> {
        let mut rx = self.pods_watcher.watch()?;

        while let Some(new_app) = rx.recv().await {
            let mut app_lock = self.app.write().await;
            if let Some(current_app) = app_lock.as_mut() {
                if *current_app == new_app {
                    continue;
                }

                tracing::debug!("Updated pods information: {:?}", new_app);
                tracing::debug!("Previous pods information: {:?}", current_app);

                // check for new and updated datasets
                for ds in &new_app.datasets {
                    if let Some(current_ds) =
                        current_app.datasets.iter().find(|d| d.name == ds.name)
                    {
                        if current_ds != ds {
                            self.update_dataset(ds, &new_app.datasets).await;
                        }
                    } else {
                        status::update_dataset(&ds.name, status::ComponentStatus::Initializing);
                        self.load_dataset(ds, &new_app.datasets);
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
                        status::update_dataset(&ds.name, status::ComponentStatus::Disabled);
                        self.remove_dataset(ds).await;
                    }
                }

                *current_app = new_app;
            } else {
                *app_lock = Some(new_app);
            }
        }

        Ok(())
    }
}

async fn verify_dependent_tables(ds: &Dataset, existing_tables: &[String]) -> bool {
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

fn get_view_dependent_tables(dataset: impl Borrow<Dataset>) -> Result<Vec<String>> {
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

fn get_dependent_table_names(statement: &parser::Statement) -> Vec<String> {
    let mut table_names = Vec::new();
    let mut cte_names = HashSet::new();

    if let parser::Statement::Statement(statement) = statement.clone() {
        if let ast::Statement::Query(statement) = *statement {
            // Collect names of CTEs
            if let Some(with) = statement.with {
                for table in with.cte_tables {
                    cte_names.insert(table.alias.name.to_string());
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
                                table_names.push(name.to_string());
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
