use std::fmt::Debug;

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

pub(crate) async fn start<A>(bind_address: A) -> Result<()>
where
    A: ToSocketAddrs + Debug,
{
    let routes = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/health", get(|| async { "ok\n" }));

    let listener = TcpListener::bind(&bind_address)
        .await
        .context(UnableToBindServerToPortSnafu)?;
    tracing::info!("Spice Runtime HTTP listening on {bind_address:?}");

    metrics::counter!("spiced_runtime_http_server_start").increment(1);

    axum::serve(listener, routes)
        .await
        .context(UnableToStartHttpServerSnafu)?;
    Ok(())
}
