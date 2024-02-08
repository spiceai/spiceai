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
pub mod model;
pub mod modelformat;
pub mod modelruntime;
pub mod modelsource;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server"))]
    UnableToStartHttpServer { source: http::Error },

    #[snafu(display("Unable to start Flight server"))]
    UnableToStartFlightServer { source: flight::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Runtime {
    pub app: Arc<app::App>,
    pub config: config::Config,
    pub df: Arc<DataFusion>,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config, app: app::App, df: DataFusion) -> Self {
        Runtime {
            app: Arc::new(app),
            config,
            df: Arc::new(df),
        }
    }

    pub async fn start_servers(&self) -> Result<()> {
        let http_server_future = http::start(self.config.http_bind_address, self.app.clone());
        let flight_server_future = flight::start(self.config.flight_bind_address, self.df.clone());

        tokio::select! {
            http_res = http_server_future => http_res.context(UnableToStartHttpServerSnafu),
            flight_res = flight_server_future => flight_res.context(UnableToStartFlightServerSnafu),
            () = shutdown_signal() => {
                tracing::info!("Goodbye!");
                Ok(())
            },
        }
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
