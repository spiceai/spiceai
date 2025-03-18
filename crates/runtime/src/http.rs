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
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::{Builder, Connection};
use hyper_util::service::TowerToHyperService;
use runtime_auth::{layer::http::AuthLayer, HttpAuth};
use snafu::prelude::*;
use spicepod::component::runtime::CorsConfig;
use tokio::net::TcpStream;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::watch::{self, Receiver};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use crate::{
    config,
    embeddings::vector_search::{self, parse_explicit_primary_keys},
    metrics as runtime_metrics,
    tls::TlsConfig,
    Runtime,
};

#[cfg(feature = "openapi")]
pub use routes::get_api_doc;
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
    shutdown_signal: Option<CancellationToken>,
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

    let shutdown_signal = shutdown_signal.unwrap_or_default();

    let (shutdown_notify, _) = watch::channel(());

    loop {
        tokio::select! {
            conn = listener.accept() => {
                let stream = match conn {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        tracing::debug!("Error accepting connection to serve HTTP request: {e}");
                        continue;
                    }
                };

                match tls_config {
                    Some(ref config) => {
                        let acceptor = TlsAcceptor::from(Arc::clone(&config.server_config));
                        process_tls_tcp_stream(stream, acceptor, routes.clone(), shutdown_notify.subscribe());
                    }
                    None => {
                        process_tcp_stream(stream, routes.clone(), shutdown_notify.subscribe());
                    }
                };
            },
            () = shutdown_signal.cancelled() => {
                tracing::debug!("Received shutdown signal");
                drop(listener); // stop accepting new connections while shutting down
                let num_active = shutdown_notify.receiver_count();
                if num_active > 0 {
                    tracing::info!(
                        "Detected {num_active} active requests. Waiting for completion before shutting down..."
                    );
                }
                shutdown_notify.send(()).ok();
                // Wait for all active connections to close
                shutdown_notify.closed().await;
                break;
            }
        }
    }

    tracing::debug!("Stopped");

    Ok(())
}

fn process_tls_tcp_stream(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    routes: Router,
    on_shutdown: Receiver<()>,
) {
    tokio::spawn(async move {
        match acceptor.accept(stream).await {
            Ok(tls_stream) => {
                let conn = serve_connection(tls_stream, routes);
                handle_connection(conn, on_shutdown).await;
            }
            Err(e) => {
                tracing::debug!("Error accepting TLS connection: {e}");
            }
        }
    });
}

fn process_tcp_stream(stream: TcpStream, routes: Router, on_shutdown: Receiver<()>) {
    tokio::spawn({
        let conn = serve_connection(stream, routes);
        async move { handle_connection(conn, on_shutdown).await }
    });
}

fn serve_connection<S>(
    stream: S,
    service: Router,
) -> Connection<'static, TokioIo<S>, TowerToHyperService<Router>, TokioExecutor>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let hyper_service = TowerToHyperService::new(service);
    Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(stream), hyper_service)
        .into_owned()
}

async fn handle_connection<S>(
    conn: Connection<'static, TokioIo<S>, TowerToHyperService<Router>, TokioExecutor>,
    mut on_shutdown: Receiver<()>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // Implementation is inspired by axum WithGracefulShutdown:
    // https://github.com/tokio-rs/axum/blob/main/axum/src/serve/mod.rs#L344

    tokio::pin!(conn);
    tokio::select! {
        result = &mut conn.as_mut() => {
            if let Err(e) = result {
                tracing::debug!(error = ?e, "Error serving HTTP connection.");
            }
        },
        _ = on_shutdown.changed() => {
            tracing::trace!("Received shutdown signal, starting graceful connection shutdown");
            conn.as_mut().graceful_shutdown();
            let _ = conn.as_mut().await;
        }
    }
}
