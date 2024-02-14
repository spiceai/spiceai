#![allow(clippy::missing_errors_doc)]

use std::{collections::HashMap, sync::Arc};

use config::Config;
use model::Model;
use snafu::prelude::*;
use spicepod::component::dataset;
use tokio::{signal, sync::RwLock};

use crate::{dataconnector::DataConnector, datafusion::DataFusion};
pub use notify::Error as NotifyError;

pub mod auth;
pub mod config;
pub mod databackend;
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
pub(crate) mod tracers;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server"))]
    UnableToStartHttpServer { source: http::Error },

    #[snafu(display("Unable to start Flight server"))]
    UnableToStartFlightServer { source: flight::Error },

    #[snafu(display("Unable to start OpenTelemetry server"))]
    UnableToStartOpenTelemetryServer { source: opentelemetry::Error },

    #[snafu(display("Unknown data source: {data_source}"))]
    UnknownDataSource { data_source: String },

    #[snafu(display("Unable to create data backend"))]
    UnableToCreateBackend { source: datafusion::Error },

    #[snafu(display("Unable to attach data source: {data_source}"))]
    UnableToAttachDataSource {
        source: datafusion::Error,
        data_source: String,
    },

    #[snafu(display("Unable to attach view"))]
    UnableToAttachView { source: datafusion::Error },

    #[snafu(display("Failed to start pods watcher: {source}"))]
    UnableToInitializePodsWatcher { source: NotifyError },

    #[snafu(display("Unable to initialize data connector: {data_connector}"))]
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

    #[snafu(display("Unable to attach data connector: {data_connector}"))]
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
}

impl Runtime {
    #[must_use]
    pub fn new(
        config: Config,
        app: app::App,
        df: DataFusion,
        models: HashMap<String, Model>,
        pods_watcher: podswatcher::PodsWatcher,
    ) -> Self {
        Runtime {
            app: Arc::new(app),
            config,
            df: Arc::new(RwLock::new(df)),
            models: Arc::new(models),
            pods_watcher,
        }
    }

    pub async fn load_dataset(
        ds: &dataset::Dataset,
        auth: &auth::AuthProviders,
        df: &mut datafusion::DataFusion,
    ) -> Result<()> {
        if ds.acceleration.is_none() && !ds.is_view() {
            tracing::warn!("No acceleration specified for dataset: {}", ds.name);
            return Ok(());
        };

        let source = ds.source();
        let source = source.as_str();
        let params = Arc::new(ds.params.clone());
        let data_connector: Option<Box<dyn DataConnector>> = match source {
            "spice.ai" => Some(Box::new(
                dataconnector::spiceai::SpiceAI::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataConnectorSnafu {
                        data_connector: source,
                    })?,
            )),
            "dremio" => Some(Box::new(
                dataconnector::dremio::Dremio::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataConnectorSnafu {
                        data_connector: source,
                    })?,
            )),
            "localhost" | "" => None,
            "debug" => Some(Box::new(dataconnector::debug::DebugSource {})),
            _ => UnknownDataConnectorSnafu {
                data_connector: source,
            }
            .fail()?,
        };

        let view_sql = ds.view_sql().context(InvalidSQLViewSnafu)?;

        match data_connector {
            Some(data_connector) => {
                let data_connector = Box::leak(data_connector);
                let data_backend = df.new_backend(ds).context(UnableToCreateBackendSnafu)?;

                df.attach(ds, data_connector, data_backend).context(
                    UnableToAttachDataConnectorSnafu {
                        data_connector: source,
                    },
                )?;
            }
            None => {
                if view_sql.is_some() {
                    df.attach_view(ds).context(UnableToAttachViewSnafu)?;
                } else {
                    let data_backend = df.new_backend(ds).context(UnableToCreateBackendSnafu)?;
                    df.attach_backend(&ds.name, data_backend).context(
                        UnableToAttachDataConnectorSnafu {
                            data_connector: source,
                        },
                    )?;
                }
            }
        }

        tracing::info!("Loaded dataset: {}", ds.name);

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

            // TODO: This is a placeholder to demo/test that datasets could be loaded here
            // To be replaced with actual implementation using shared ds/datafusion instance

            // let mut df = datafusion::DataFusion::new();
            // let auth = auth::AuthProviders::default();

            // if let Err(err) = Runtime::load_dataset(&new_app.datasets[0], &auth, &mut df).await {
            //     tracing::error!("Unable to load dataset: {err:?}");
            // }

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
