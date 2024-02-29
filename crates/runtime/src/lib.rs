#![allow(clippy::missing_errors_doc)]

use std::borrow::Borrow;
use std::{collections::HashMap, sync::Arc};

use app::App;
use config::Config;
use model::Model;
pub use notify::Error as NotifyError;
use secretstore::file::FileSecretStore;
use secretstore::{Secret, SecretStore, SecretStores};
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use spicepod::component::dataset::Mode;
use spicepod::component::model::Model as SpicepodModel;
use spicepod::component::secret;
use std::time::Duration;
use tokio::time::sleep;
use tokio::{signal, sync::RwLock};

use crate::{dataconnector::DataConnector, datafusion::DataFusion};

pub mod config;
pub mod databackend;
pub mod dataconnector;
pub mod datafusion;
pub mod datapublisher;
pub mod dataupdate;
mod flight;
mod http;
pub mod model;
pub mod modelformat;
pub mod modelruntime;
pub mod modelsource;
mod opentelemetry;
pub mod podswatcher;
pub mod secretstore;
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

    #[snafu(display("Unable to attach data source {data_source}: {source}"))]
    UnableToAttachDataSource {
        source: datafusion::Error,
        data_source: String,
    },

    #[snafu(display("Unable to attach view: {source}"))]
    UnableToAttachView { source: datafusion::Error },

    #[snafu(display("Failed to start pods watcher: {source}"))]
    UnableToInitializePodsWatcher { source: NotifyError },

    #[snafu(display("Unable to initialize data connector {data_connector}: {source}"))]
    UnableToInitializeDataConnector {
        source: dataconnector::Error,
        data_connector: String,
    },

    #[snafu(display("Unknown data connector: {data_connector}"))]
    UnknownDataConnector { data_connector: String },

    #[snafu(display("Unable to create view: {source}"))]
    InvalidSQLView {
        source: spicepod::component::dataset::Error,
    },

    #[snafu(display("Unable to attach data connector {data_connector}: {source}"))]
    UnableToAttachDataConnector {
        source: datafusion::Error,
        data_connector: String,
    },

    #[snafu(display("Unable to load secret store: {secret_store}"))]
    UnableToLoadSecretStore { secret_store: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Runtime {
    pub app: Arc<RwLock<Option<App>>>,
    pub config: config::Config,
    pub df: Arc<RwLock<DataFusion>>,
    pub models: Arc<RwLock<HashMap<String, Model>>>,
    pub pods_watcher: podswatcher::PodsWatcher,
    pub secret_stores: Arc<RwLock<SecretStores>>,

    spaced_tracer: Arc<tracers::SpacedTracer>,
}

impl Runtime {
    #[must_use]
    pub fn new(
        config: Config,
        app: Arc<RwLock<Option<app::App>>>,
        df: Arc<RwLock<DataFusion>>,
        pods_watcher: podswatcher::PodsWatcher,
        secret_stores: Arc<RwLock<SecretStores>>,
    ) -> Self {
        Runtime {
            app,
            config,
            df,
            models: Arc::new(RwLock::new(HashMap::new())),
            pods_watcher,
            secret_stores,
            spaced_tracer: Arc::new(tracers::SpacedTracer::new(Duration::from_secs(15))),
        }
    }

    pub async fn load_datasets(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for ds in &app.datasets {
                self.load_dataset(ds);
            }
        }
    }

    pub fn load_dataset(&self, ds: &Dataset) {
        let df = Arc::clone(&self.df);
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let shared_secret_stores = Arc::clone(&self.secret_stores);

        let ds = ds.clone();

        // let secret_store_key = match ds.auth {
        //     Some(ref auth) => auth.secret_store.clone(),
        //     None => "file".to_string(),
        // };

        tokio::spawn(async move {
            loop {
                let source = ds.source();

                let dataset_secrets = Runtime::load_component_secrets(
                    shared_secret_stores.clone(),
                    ds.secrets.clone(),
                )
                .await;

                // let secret_key = match ds.auth {
                //     Some(ref auth) => auth.secret_key.clone(),
                //     None => source.clone(),
                // };

                // let secret_store =
                //     match secret_stores.get_store(&secret_store_key).ok_or_else(|| {
                //         UnableToLoadSecretStoreSnafu {
                //             secret_store: secret_key.clone(),
                //         }
                //     }) {
                //         Ok(s) => s,
                //         Err(_) => {
                //             tracing::error!("Unable to load secret store: {:?}", ds.auth);
                //             break;
                //         }
                //     };

                let params = Arc::new(ds.params.clone());
                let data_connector: Option<Box<dyn DataConnector + Send>> =
                    match Runtime::get_dataconnector_from_source(
                        &source,
                        dataset_secrets,
                        // &secret_store.get_secret(secret_key.as_str().clone()),
                        Arc::clone(&params),
                    )
                    .await
                    {
                        Ok(data_connector) => data_connector,
                        Err(err) => {
                            warn_spaced!(
                                spaced_tracer,
                                "Unable to get data connector from source for dataset {}, retrying: {err:?}",
                                &ds.name
                            );
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                if ds.acceleration.is_none()
                    && !ds.is_view()
                    && !has_table_provider(&data_connector)
                {
                    tracing::warn!("No acceleration specified for dataset: {}", ds.name);
                    break;
                };

                match Runtime::initialize_dataconnector(
                    data_connector,
                    Arc::clone(&df),
                    &source,
                    &ds,
                )
                .await
                {
                    Ok(()) => (),
                    Err(err) => {
                        warn_spaced!(
                            spaced_tracer,
                            "Unable to initialize data connector for dataset {}, retrying: {err:?}",
                            &ds.name
                        );
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                tracing::info!("Loaded dataset: {}", &ds.name);
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
    }

    pub async fn update_dataset(&self, ds: &Dataset) {
        self.remove_dataset(ds).await;
        self.load_dataset(ds);
    }

    async fn get_dataconnector_from_source(
        source: &str,
        // secret: &Secret,
        dataset_secrets: HashMap<String, String>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Option<Box<dyn DataConnector + Send>>> {
        match source {
            "spiceai" => {
                println!("spiceai");
                // TODO: fallback to default file auth

                Ok(Some(Box::new(
                    dataconnector::spiceai::SpiceAI::new(Secret::new(dataset_secrets), params)
                        .await
                        .context(UnableToInitializeDataConnectorSnafu {
                            data_connector: source,
                        })?,
                )))
            }
            "dremio" => {
                println!("dremio");
                // TODO: fallback to default file auth

                Ok(Some(Box::new(
                    dataconnector::dremio::Dremio::new(Secret::new(dataset_secrets), params)
                        .await
                        .context(UnableToInitializeDataConnectorSnafu {
                            data_connector: source,
                        })?,
                )))
            }
            "localhost" => Ok(None),
            "debug" => Ok(Some(Box::new(dataconnector::debug::DebugSource {}))),
            _ => UnknownDataConnectorSnafu {
                data_connector: source,
            }
            .fail()?,
        }
    }

    async fn initialize_dataconnector(
        data_connector: Option<Box<dyn DataConnector + Send>>,
        df: Arc<RwLock<DataFusion>>,
        source: &str,
        ds: impl Borrow<Dataset>,
    ) -> Result<()> {
        let ds = ds.borrow();
        let view_sql = ds.view_sql().context(InvalidSQLViewSnafu)?;
        let data_backend_publishing_enabled =
            ds.mode() == Mode::ReadWrite || data_connector.is_none();

        if view_sql.is_some() {
            df.read()
                .await
                .attach_view(ds)
                .context(UnableToAttachViewSnafu)?;
            return Ok(());
        }

        if ds.acceleration.is_none() {
            if let Some(data_connector) = data_connector {
                df.read()
                    .await
                    .attach_mesh(ds, data_connector)
                    .await
                    .context(UnableToAttachViewSnafu)?;
                return Ok(());
            }
        }

        let data_backend = df
            .read()
            .await
            .new_accelerated_backend(ds)
            .context(UnableToCreateBackendSnafu)?;
        let data_backend = Arc::new(data_backend);

        if data_backend_publishing_enabled {
            df.write()
                .await
                .attach_publisher(&ds.name.clone(), ds.clone(), Arc::clone(&data_backend))
                .await
                .context(UnableToAttachDataConnectorSnafu {
                    data_connector: source,
                })?;
        }

        if let Some(data_connector) = data_connector {
            let replicate = ds.replication.as_ref().map_or(false, |r| r.enabled);

            // Attach data publisher only if replicate is true and mode is ReadWrite
            if replicate && ds.mode() == Mode::ReadWrite {
                if let Some(data_publisher) = data_connector.get_data_publisher() {
                    df.write()
                        .await
                        .attach_publisher(&ds.name.clone(), ds.clone(), Arc::new(data_publisher))
                        .await
                        .context(UnableToAttachDataConnectorSnafu {
                            data_connector: source,
                        })?;
                } else {
                    tracing::warn!(
                        "Data connector {source} does not support writes, but dataset {ds_name} is configured to replicate",
                        ds_name = ds.name
                    );
                }
            }

            df.write()
                .await
                .attach_connector_to_publisher(
                    ds.clone(),
                    data_connector,
                    Arc::clone(&data_backend),
                )
                .context(UnableToAttachDataConnectorSnafu {
                    data_connector: source,
                })?;
        }

        Ok(())
    }

    pub async fn load_models(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for model in &app.models {
                self.load_model(model).await;
            }
        }
    }

    pub async fn load_model(&self, m: &SpicepodModel) {
        tracing::info!("Loading model [{}] from {}...", m.name, m.from);
        let mut model_map = self.models.write().await;

        let source = model::source(&m.from);
        let secret_stores = self.secret_stores.read().await;
        let secret_store =
            match secret_stores
                .get_store("files")
                .ok_or_else(|| UnableToLoadSecretStoreSnafu {
                    secret_store: source.clone(),
                }) {
                Ok(s) => s,
                Err(_) => return,
            };

        match Model::load(m, secret_store.get_secret(source.as_str())).await {
            Ok(in_m) => {
                model_map.insert(m.name.clone(), in_m);
                tracing::info!("Model [{}] deployed, ready for inferencing", m.name);
            }
            Err(e) => {
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
    }

    pub async fn update_model(&self, m: &SpicepodModel) {
        self.remove_model(m).await;
        self.load_model(m).await;
    }

    pub async fn start_servers(&mut self) -> Result<()> {
        let http_server_future = http::start(
            self.config.http_bind_address,
            self.app.clone(),
            self.df.clone(),
            self.models.clone(),
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
                            self.update_dataset(ds).await;
                        }
                    } else {
                        self.load_dataset(ds);
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
                        self.load_model(model).await;
                    }
                }

                // Remove models that are no longer in the app
                for model in &current_app.models {
                    if !new_app.models.iter().any(|m| m.name == model.name) {
                        self.remove_model(model).await;
                    }
                }

                // Remove datasets that are no longer in the app
                for ds in &current_app.datasets {
                    if !new_app.datasets.iter().any(|d| d.name == ds.name) {
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

    pub async fn load_component_secrets(
        secret_stores: Arc<RwLock<SecretStores>>,
        secrets: Vec<secret::Secret>,
    ) -> HashMap<String, String> {
        let mut component_secrets: HashMap<String, String> = HashMap::new();

        let shared_secret_stores = Arc::clone(&secret_stores);
        let secret_stores = shared_secret_stores.read().await;

        for s in secrets {
            let secret_store = match secret_stores.get_store(&s.store).ok_or_else(|| {
                UnableToLoadSecretStoreSnafu {
                    secret_store: &s.store,
                }
            }) {
                Ok(s) => s,
                Err(_) => {
                    tracing::error!("Unable to load secret store: {:?}", &s.store);
                    break;
                }
            };

            let secret = secret_store.get_secret(&s.store_key.as_str().clone());
            let value = match secret.get_secret(&s.data_key) {
                Some(v) => v,
                None => {
                    tracing::error!("Unable to load secret: {:?}", &s.key);
                    break;
                }
            };

            component_secrets.insert(s.key, value.to_string());
        }

        component_secrets
    }
}

fn has_table_provider(data_connector: &Option<Box<dyn DataConnector + Send>>) -> bool {
    data_connector.is_some()
        && data_connector
            .as_ref()
            .is_some_and(|dc| dc.has_table_provider())
}

pub fn initialize_secret_stores() -> secretstore::SecretStores {
    let mut stores = secretstore::SecretStores::new();

    let mut file_secret_store = FileSecretStore::new();
    match file_secret_store.init() {
        Ok(()) => {}
        Err(err) => {
            tracing::error!("Unable to initialize file secret store: {err:?}");
        }
    }

    stores.add_store("file".to_string(), Box::new(file_secret_store));

    let mut keyring_secret_store = secretstore::keyring::KeyringSecretStore::new();
    match keyring_secret_store.init() {
        Ok(()) => {}
        Err(err) => {
            tracing::error!("Unable to initialize keyring secret store: {err:?}");
        }
    }

    stores.add_store("keyring".to_string(), Box::new(keyring_secret_store));

    stores
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let signal_result = signal::ctrl_c().await;
        if let Err(err) = signal_result {
            tracing::error!("Unable to listen to shutdown signal: {err:?}");
        }
    };

    tokio::select! {
        () = ctrl_c => {},
    }
}
