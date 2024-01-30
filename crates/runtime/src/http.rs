use std::{fmt::Debug, sync::Arc};

use app::App;
use snafu::prelude::*;
use tokio::net::{TcpListener, ToSocketAddrs};

mod routes;
mod v1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to address"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Unable to start HTTP server"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn start<A>(bind_address: A, app: Arc<App>) -> Result<()>
where
    A: ToSocketAddrs + Debug,
{
    let routes = routes::routes(app);

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
