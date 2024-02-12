#![allow(clippy::missing_errors_doc)]

use std::sync::Arc;

use config::Config;
use snafu::prelude::*;
use tokio::signal;

use crate::datafusion::DataFusion;

pub mod auth;
pub mod config;
pub mod databackend;
pub mod datafusion;
pub mod datasource;
pub mod dataupdate;
mod flight;
mod http;
pub mod modelformat;
mod opentelemetry;
pub mod podswatcher;
pub use notify::Error as NotifyError;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server"))]
    UnableToStartHttpServer { source: http::Error },

    #[snafu(display("Unable to start Flight server"))]
    UnableToStartFlightServer { source: flight::Error },

    #[snafu(display("Unable to start OpenTelemetry server"))]
    UnableToStartOpenTelemetryServer { source: opentelemetry::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Runtime {
    pub app: Arc<app::App>,
    pub config: config::Config,
    pub df: Arc<DataFusion>,
    pub pods_watcher: podswatcher::PodsWatcher,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config, app: app::App, df: DataFusion, pods_watcher: podswatcher::PodsWatcher) -> Self {
        Runtime {
            app: Arc::new(app),
            config,
            df: Arc::new(df),
            pods_watcher: pods_watcher,
        }
    }

    pub async fn start_servers(&self) -> Result<()> {
        let http_server_future = http::start(self.config.http_bind_address, self.app.clone());
        let flight_server_future = flight::start(self.config.flight_bind_address, self.df.clone());
        let open_telemetry_server_future =
            opentelemetry::start(self.config.open_telemetry_bind_address);

        tokio::select! {
            http_res = http_server_future => http_res.context(UnableToStartHttpServerSnafu),
            flight_res = flight_server_future => flight_res.context(UnableToStartFlightServerSnafu),
            open_telemetry_res = open_telemetry_server_future => open_telemetry_res.context(UnableToStartOpenTelemetryServerSnafu),
            () = shutdown_signal() => {
                tracing::info!("Goodbye!");
                Ok(())
            },
        }
    }

    pub fn start_pods_watcher(&mut self) -> notify::Result<()> {
        let mut current_app = Arc::clone(&self.app);

        let handle_event = move |event: podswatcher::PodsWatcherEvent| {
            match event {
                podswatcher::PodsWatcherEvent::PodsUpdated(new_app) => {
                    tracing::debug!("updated pods information: {:?}", new_app);
                    tracing::debug!("previous pods information: {:?}", current_app);

                    // TODO: update runtime based on current_app vs new_app info

                    current_app = Arc::new(new_app);
                }
            }
        };

        self.pods_watcher.watch(handle_event)?;

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
