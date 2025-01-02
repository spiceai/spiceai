/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{borrow::Cow, fmt::Debug, sync::Arc};

use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
    service::TowerToHyperService,
};
use runtime_auth::{layer::http::AuthLayer, HttpAuth};
use snafu::prelude::*;
use spicepod::component::runtime::CorsConfig;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_rustls::TlsAcceptor;

use crate::{
    config,
    embeddings::vector_search::{self, parse_explicit_primary_keys},
    metrics as runtime_metrics,
    tls::TlsConfig,
    Runtime,
};

#[cfg(feature = "openapi")]
pub use routes::ApiDoc;
mod metrics;
mod routes;
mod traceparent;

pub mod v1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to address: {source}"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn start<A>(
    bind_address: A,
    rt: Arc<Runtime>,
    config: Arc<config::Config>,
    tls_config: Option<Arc<TlsConfig>>,
    auth_provider: Option<Arc<dyn HttpAuth + Send + Sync>>,
) -> Result<()>
where
    A: ToSocketAddrs + Debug,
{
    let vsearch = Arc::new(vector_search::VectorSearch::new(
        Arc::clone(&rt.df),
        Arc::clone(&rt.embeds),
        parse_explicit_primary_keys(Arc::clone(&rt.app)).await,
    ));
    let app = rt.app.as_ref().read().await;
    let cors_config: Cow<'_, CorsConfig> = match app.as_ref() {
        Some(app) => Cow::Borrowed(&app.runtime.cors),
        None => Cow::Owned(CorsConfig::default()),
    };
    let routes = routes::routes(
        &rt,
        config,
        vsearch,
        auth_provider.map(AuthLayer::new),
        &cors_config,
    );
    drop(app);

    let listener = TcpListener::bind(&bind_address)
        .await
        .context(UnableToBindServerToPortSnafu)?;
    tracing::info!("Spice Runtime HTTP listening on {bind_address:?}");

    runtime_metrics::spiced_runtime::HTTP_SERVER_START.add(1, &[]);

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::debug!("Error accepting connection to serve HTTP request: {e}");
                continue;
            }
        };

        match tls_config {
            Some(ref config) => {
                let acceptor = TlsAcceptor::from(Arc::clone(&config.server_config));
                process_tls_tcp_stream(stream, acceptor, routes.clone());
            }
            None => {
                process_tcp_stream(stream, routes.clone());
            }
        };
    }
}

fn process_tls_tcp_stream(stream: TcpStream, acceptor: TlsAcceptor, routes: Router) {
    tokio::spawn(async move {
        let stream = acceptor.accept(stream).await;
        match stream {
            Ok(stream) => {
                serve_connection(stream, routes).await;
            }
            Err(e) => {
                tracing::debug!("Error accepting TLS connection: {e}");
            }
        }
    });
}

fn process_tcp_stream(stream: TcpStream, routes: Router) {
    tokio::spawn(serve_connection(stream, routes));
}

async fn serve_connection<S>(stream: S, service: Router)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let hyper_service = TowerToHyperService::new(service);
    if let Err(err) = Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(stream), hyper_service)
        .await
    {
        tracing::debug!(error = ?err, "Error serving HTTP connection.");
    }
}
