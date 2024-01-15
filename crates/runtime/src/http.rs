use std::{fmt::Debug, future::Future};

use axum::{routing::get, Router};
use snafu::prelude::*;
use tokio::net::{TcpListener, ToSocketAddrs};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to address"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Unable to start HTTP server"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn start<F, A>(shutdown_signal: F, bind_address: A) -> Result<()>
where
    F: Future<Output = ()> + Send + Sync + 'static,
    A: ToSocketAddrs + Debug,
{
    let routes = Router::new().route("/", get(|| async { "Hello, World!" }));

    let listener = TcpListener::bind(&bind_address)
        .await
        .context(UnableToBindServerToPortSnafu)?;
    tracing::info!("Spice Runtime listening on {bind_address:?}");

    axum::serve(listener, routes)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .context(UnableToStartHttpServerSnafu)?;
    Ok(())
}
