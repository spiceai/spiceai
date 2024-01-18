#![allow(clippy::missing_errors_doc)]

use config::Config;
use snafu::prelude::*;
use tokio::signal;

pub mod config;
mod http;
mod flight;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server"))]
    UnableToStartHttpServer { source: http::Error },

    #[snafu(display("Unable to start Flight server"))]
    UnableToStartFlightServer { source: Box<dyn snafu::Error>},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Runtime {
    pub app: app::App,
    pub config: config::Config,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config, app: app::App) -> Self {
        Runtime { app, config }
    }

    pub async fn start_servers(&self) -> Result<()> {
        let http_server_future = http::start(shutdown_signal(), self.config.http_bind_address);
        let flight_server_future = flight::start(self.config.flight_bind_address);

        tokio::select! {
            http_res = http_server_future => http_res.context(UnableToStartHttpServerSnafu),
            flight_res = flight_server_future => flight_res.context(UnableToStartFlightServerSnafu),
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
