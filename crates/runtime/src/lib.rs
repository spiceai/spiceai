#![allow(clippy::missing_errors_doc)]

use std::{collections::HashMap, sync::Arc};

use config::Config;
use model::Model;
pub use notify::Error as NotifyError;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use spicepod::component::dataset::Mode;
use std::time::Duration;
use tokio::time::sleep;
use tokio::{signal, sync::RwLock};

use crate::{dataconnector::DataConnector, datafusion::DataFusion};

pub mod auth;
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Runtime {
    pub app: Arc<app::App>,
    pub config: config::Config,
    pub df: Arc<RwLock<DataFusion>>,
    pub models: Arc<HashMap<String, Model>>,
    pub pods_watcher: podswatcher::PodsWatcher,
    pub auth: Arc<auth::AuthProviders>,
}

impl Runtime {
    #[must_use]
    pub fn new(
        config: Config,
        app: Arc<app::App>,
        df: Arc<RwLock<DataFusion>>,
        models: HashMap<String, Model>,
        pods_watcher: podswatcher::PodsWatcher,
        auth: Arc<auth::AuthProviders>,
    ) -> Self {
        Runtime {
            app,
            config,
            df,
            models: Arc::new(models),
            pods_watcher,
            auth,
        }
    }

    pub fn load_datasets(&self, auth: &Arc<auth::AuthProviders>) {
        for ds in self.app.datasets.clone() {
            self.load_dataset(&ds, auth);
        }
    }

    pub fn load_dataset(&self, ds: &Dataset, auth: &Arc<auth::AuthProviders>) {
        let ds = ds.clone();
        let df = self.df.clone();
        let auth = auth.clone();
        let retries = self.config.dataset_load_retries;
        tokio::spawn(async move {
            for _i in 0..retries {
                let ds = Arc::new(ds.clone());
                if ds.acceleration.is_none() && !ds.is_view() {
                    tracing::warn!("No acceleration specified for dataset: {}", ds.name);
                    break;
                };

                let source = ds.source();
                let source = source.as_str();
                let params = Arc::new(ds.params.clone());
                let data_connector: Option<Box<dyn DataConnector + Send>> =
                    match Runtime::get_dataconnector_from_source(source, &auth, Arc::clone(&params))
                        .await
                    {
                        Ok(data_connector) => data_connector,
                        Err(err) => {
                            tracing::warn!(
                                "Unable to get data connector from source for dataset {}, retrying: {err:?}", ds.name
                            );
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                match Runtime::initialize_dataconnector(data_connector, df.clone(), source, &ds)
                    .await
                {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::warn!(
                            "Unable to initialize data connector for dataset {}, retrying: {err:?}",
                            ds.name
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

    async fn get_dataconnector_from_source(
        source: &str,
        auth: &auth::AuthProviders,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Option<Box<dyn DataConnector + Send>>> {
        match source {
            "spice.ai" => Ok(Some(Box::new(
                dataconnector::spiceai::SpiceAI::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataConnectorSnafu {
                        data_connector: source,
                    })?,
            ))),
            "dremio" => Ok(Some(Box::new(
                dataconnector::dremio::Dremio::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataConnectorSnafu {
                        data_connector: source,
                    })?,
            ))),
            "localhost" | "" => Ok(None),
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
        ds: &Arc<Dataset>,
    ) -> Result<()> {
        let view_sql = ds.view_sql().context(InvalidSQLViewSnafu)?;

        match data_connector {
            Some(data_connector) => {
                let data_backend = df
                    .read()
                    .await
                    .new_accelerated_backend(Arc::clone(ds))
                    .context(UnableToCreateBackendSnafu)?;
                let data_backend = Arc::new(data_backend);

                if ds.mode() == Mode::ReadWrite {
                    if let Some(data_publisher) = data_connector.get_data_publisher() {
                        df.write()
                            .await
                            .attach_publisher(
                                &ds.name.clone(),
                                Arc::clone(ds),
                                Arc::new(data_publisher),
                            )
                            .await
                            .context(UnableToAttachDataConnectorSnafu {
                                data_connector: source,
                            })?;
                    } else {
                        tracing::warn!(
                            "Data connector {source} does not support writes, but dataset {ds_name} is in read-write mode",
                            ds_name = ds.name
                        );
                    }

                    df.write()
                        .await
                        .attach_publisher(
                            &ds.name.clone(),
                            Arc::clone(ds),
                            Arc::clone(&data_backend),
                        )
                        .await
                        .context(UnableToAttachDataConnectorSnafu {
                            data_connector: source,
                        })?;
                }

                df.write()
                    .await
                    .attach_connector_to_publisher(
                        Arc::clone(ds),
                        data_connector,
                        Arc::clone(&data_backend),
                    )
                    .context(UnableToAttachDataConnectorSnafu {
                        data_connector: source,
                    })?;
            }
            None => {
                if view_sql.is_some() {
                    df.read()
                        .await
                        .attach_view(ds)
                        .context(UnableToAttachViewSnafu)?;
                } else {
                    let data_backend = df
                        .read()
                        .await
                        .new_accelerated_backend(Arc::clone(ds))
                        .context(UnableToCreateBackendSnafu)?;
                    df.write()
                        .await
                        .attach_publisher(&ds.name.clone(), Arc::clone(ds), Arc::new(data_backend))
                        .await
                        .context(UnableToAttachDataConnectorSnafu {
                            data_connector: source,
                        })?;
                }
            }
        }

        Ok(())
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
        let mut current_app = Arc::clone(&self.app);

        let mut rx = self.pods_watcher.watch()?;

        while let Some(new_app) = rx.recv().await {
            tracing::debug!("Updated pods information: {:?}", new_app);
            tracing::debug!("Previous pods information: {:?}", current_app);

            let mut auth = auth::AuthProviders::default();
            if let Err(e) = auth.parse_from_config() {
                tracing::warn!(
                    "Unable to parse auth from config, proceeding without auth: {}",
                    e
                );
            }
            let auth_arc = Arc::new(auth);

            let existing_dataset_names = current_app
                .datasets
                .iter()
                .map(|ds| ds.name.clone())
                .collect::<Vec<String>>();

            for ds in &new_app.datasets {
                if !existing_dataset_names.contains(&ds.name) {
                    self.load_dataset(ds, &auth_arc);
                }
            }

            current_app = Arc::new(new_app);
        }

        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let signal_result = signal::ctrl_c().await;
        if let Err(err) = signal_result {
            tracing::error!("Unable to listen to shutdown signal: {err:?}");
        }
    };

    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => signal.recv().await,
            Err(err) => {
                tracing::error!("Unable to listen to shutdown signal: {err:?}");
                None
            }
        }
    };

    tokio::select! {
        () = ctrl_c => {},
        _ = terminate => {},
    }
}
