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

use std::{borrow::Cow, fmt::Debug, sync::Arc, time::Duration};

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::{Builder, Connection};
use hyper_util::service::TowerToHyperService;
use runtime_auth::{HttpAuth, IdentitySource, layer::http::AuthLayer};
use snafu::prelude::*;
use spicepod::component::runtime::CorsConfig;
use tokio::net::TcpStream;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::watch::{self, Receiver};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use crate::{Runtime, config};
use runtime_search::search_engine::SearchEngine;
use runtime_tls::TlsConfig;

#[cfg(feature = "openapi")]
pub use routes::get_api_doc;
#[cfg(test)]
#[cfg(feature = "models")]
mod gemini_tests;
pub(crate) mod host_policy;
#[cfg(feature = "mcp")]
mod mcp_transport;
#[cfg(test)]
#[cfg(feature = "models")]
mod messages_tests;
#[cfg(test)]
#[cfg(feature = "models")]
mod model_capability_tests;
mod mtls;
pub(crate) mod optional_json;
#[cfg(test)]
mod refresh_sql_depth_tests;
mod response_outcome;
mod routes;
mod server_timing;
#[cfg(test)]
mod write_access_tests;

pub mod v1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to address: {source}"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Starts a minimal HTTP server that only serves the `/health` endpoint.
///
/// This is used by cluster executors which need a health endpoint for Kubernetes
/// probes but don't require the full HTTP API.
pub(crate) async fn start_health_only<A>(
    bind_address: A,
    shutdown_signal: Option<CancellationToken>,
) -> Result<()>
where
    A: ToSocketAddrs + Debug,
{
    use axum::routing::get;

    let routes = Router::new().route("/health", get(|| async { "ok\n" }));

    let listener = TcpListener::bind(&bind_address)
        .await
        .context(UnableToBindServerToPortSnafu)?;
    tracing::info!("Spice Runtime HTTP health endpoint listening on {bind_address:?}");

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

                process_tcp_stream(stream, routes.clone(), shutdown_notify.subscribe());
            },
            () = shutdown_signal.cancelled() => {
                tracing::debug!("Health HTTP server received shutdown signal");
                drop(listener);
                let num_active = shutdown_notify.receiver_count();
                if num_active > 0 {
                    tracing::info!(
                        "Detected {num_active} active health check requests. Waiting for completion before shutting down..."
                    );
                }
                shutdown_notify.send(()).ok();
                shutdown_notify.closed().await;
                break;
            }
        }
    }

    tracing::debug!("Health HTTP server stopped");

    Ok(())
}

pub(crate) async fn start<A>(
    bind_address: A,
    rt: Arc<Runtime>,
    config: Arc<config::Config>,
    tls_config: Option<Arc<TlsConfig>>,
    auth_provider: Option<Arc<dyn HttpAuth + Send + Sync>>,
    identity_source: IdentitySource,
    shutdown_signal: Option<CancellationToken>,
) -> Result<()>
where
    A: ToSocketAddrs + Debug,
{
    let vsearch = Arc::new(
        SearchEngine::new(
            Arc::clone(&rt.df) as Arc<dyn runtime_query_engine::query_engine::QueryEngine>,
            rt.search_primary_keys.clone(),
            crate::search::util::RuntimeTableProviderExplorer,
        )
        .with_search_cache(rt.df.search_cache_provider()),
    );
    let app = rt.app.as_ref().read().await;
    let cors_config: Cow<'_, CorsConfig> = match app.as_ref() {
        Some(app) => Cow::Borrowed(&app.runtime.cors),
        None => Cow::Owned(CorsConfig::default()),
    };
    // `runtime.allowed_hosts` follows the same pattern as builder.rs: Config.runtime,
    // if present, replaces the whole runtime. MCP Origin CORS is resolved from the
    // same override in `routes::mcp_origin_cors`. HTTP CORS stays on the app
    // spicepod (a cache-only Config.runtime override must not disable browser CORS).
    // The runtime requires authentication when it checks API keys or client
    // certificates on every request; otherwise the listener answers only to
    // local hosts and origins (see `host_policy`).
    let requires_auth = auth_provider.is_some()
        || tls_config
            .as_ref()
            .is_some_and(|tls| tls.client_auth == runtime_tls::ClientAuthEnforcement::Required);
    let effective_runtime = config
        .runtime
        .as_ref()
        .or_else(|| app.as_ref().map(|app| &app.runtime));
    let request_origin = Arc::new(host_policy::RequestOriginPolicy::new(
        requires_auth,
        effective_runtime.and_then(spicepod::component::runtime::Runtime::effective_allowed_hosts),
        &cors_config,
        tls_config.is_some(),
    ));
    let auth_layer = auth_provider.map(AuthLayer::new);
    let routes = routes::routes(
        &rt,
        config,
        vsearch,
        auth_layer,
        &cors_config,
        tls_config.is_some(),
        request_origin,
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
                        let acceptor = TlsAcceptor::from(Arc::clone(&config.http_server_config));
                        process_tls_tcp_stream(
                            stream,
                            acceptor,
                            routes.clone(),
                            identity_source,
                            config.client_auth,
                            shutdown_notify.subscribe(),
                            runtime_tls::TLS_HANDSHAKE_TIMEOUT,
                        );
                    }
                    None => {
                        process_tcp_stream(stream, routes.clone(), shutdown_notify.subscribe());
                    }
                }
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

/// Disables Nagle's algorithm on an accepted connection.
///
/// hyper emits a response as several small writes (head, then body or chunk
/// frames). With Nagle enabled, every write after the first is held until the
/// client acknowledges the previous one, and a client in delayed-ACK mode
/// withholds that acknowledgement for tens of milliseconds — so a small query
/// result lands ~40ms after the query finished, while a large one that fills a
/// full segment arrives immediately. The Flight server gets the same treatment
/// from tonic's transport defaults.
///
/// Failing to set the option costs latency but not correctness, so the
/// connection is still served.
fn set_tcp_nodelay(stream: &TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!(
            "Unable to disable Nagle's algorithm on an HTTP connection, so small responses on it may be delayed by tens of milliseconds: {e}"
        );
    }
}

fn process_tls_tcp_stream(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    routes: Router,
    identity_source: IdentitySource,
    client_auth: runtime_tls::ClientAuthEnforcement,
    on_shutdown: Receiver<()>,
    handshake_timeout: Duration,
) {
    set_tcp_nodelay(&stream);
    tokio::spawn(async move {
        match runtime_tls::accept_within(&acceptor, stream, handshake_timeout).await {
            Ok(tls_stream) => {
                // Pre-compute the per-connection mTLS identities
                // (`ChannelIdentity` always; `MtlsPrincipal` only in
                // `Channel` mode) once at accept-time so per-request
                // middleware does no X.509 parsing on the hot path.
                // Every request served on this TLS connection then
                // sees the same `Arc<PerConnTls>` in its extensions.
                let per_conn = mtls::PerConnTls::build(
                    tls_stream.get_ref().1.peer_certificates(),
                    identity_source,
                    client_auth,
                );
                let routes = mtls::install_per_conn_extension(routes, per_conn);
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
    set_tcp_nodelay(&stream);
    tokio::spawn({
        let conn = serve_connection(stream, routes);
        async move { handle_connection(conn, on_shutdown).await }
    });
}

/// Maximum time a client may take to send the full request-header block on an
/// HTTP/1 connection. Bounds Slowloris-style header-dribble attacks that would
/// otherwise hold connections (and their spawned tasks/FDs) open indefinitely.
const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn serve_connection<S>(
    stream: S,
    service: Router,
) -> Connection<'static, TokioIo<S>, TowerToHyperService<Router>, TokioExecutor>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let hyper_service = TowerToHyperService::new(service);
    let mut builder = Builder::new(TokioExecutor::new());
    // Apply an HTTP/1 header-read timeout (auto HTTP/1+2 serving is preserved).
    // HTTP/2 has its own framing/flow-control limits, so this targets the
    // plaintext/HTTP-1 Slowloris vector.
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HTTP_HEADER_READ_TIMEOUT);
    builder
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

/// A TLS acceptor for the repository's test certificate.
#[cfg(test)]
pub(crate) fn test_tls_acceptor() -> TlsAcceptor {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let cert =
        CertificateDer::from_pem_slice(include_bytes!("../../../../test/tls/spiced_cert.pem"))
            .expect("the fixture is a PEM certificate");
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("../../../../test/tls/spiced_key.pem"))
        .expect("the fixture is a PEM private key");
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .expect("the fixture certificate and key match");
    TlsAcceptor::from(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get};
    use http::StatusCode;
    use std::time::Duration;
    use tokio::{io::AsyncReadExt, net::TcpListener, sync::watch, time::timeout};

    // Router that immediately responds with "ok"
    fn ok_router() -> Router {
        Router::new().route("/", get(|| async { "ok" }))
    }

    #[tokio::test]
    async fn test_process_tcp_stream_request_completed() {
        // Bind a listener on a system assigned available port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("to create listener");
        let addr = listener.local_addr().expect("to get local addr");
        let (shutdown_notify, shutdown_rx) = watch::channel(());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("to accept connection");
            process_tcp_stream(stream, ok_router(), shutdown_rx);
        });
        let client = build_test_http_client();
        let resp = timeout(
            Duration::from_secs(2),
            client.get(format!("http://{addr}/")).send(),
        )
        .await
        .expect("to complete request before timeout")
        .expect("to get response");

        assert_eq!(resp.status(), StatusCode::OK);

        drop(client);
        // The connection task holds a shutdown receiver for as long as the
        // connection is open, so `closed()` resolves exactly when it has ended.
        timeout(Duration::from_secs(2), shutdown_notify.closed())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the connection should close once the client is dropped; {} connection(s) still open",
                    shutdown_notify.receiver_count()
                )
            });

        assert_eq!(
            shutdown_notify.receiver_count(),
            0,
            "Should be no active connections"
        );

        // Verify that the shutdown does not fail if there are no active connections
        shutdown_notify.send(()).ok();
        timeout(Duration::from_secs(1), shutdown_notify.closed())
            .await
            .expect("should complete shutdown notification");
    }

    /// A shutdown closes a connection that has no request in flight, and the connection's task
    /// then drops its shutdown receiver.
    #[tokio::test]
    async fn test_process_tcp_stream_graceful_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("to create listener");
        let addr = listener.local_addr().expect("to get local addr");
        let (shutdown_notify, shutdown_rx) = watch::channel(());

        let mut client = TcpStream::connect(addr).await.expect("to connect");
        let (stream, _) = listener.accept().await.expect("to accept connection");
        process_tcp_stream(stream, ok_router(), shutdown_rx);
        assert_eq!(
            shutdown_notify.receiver_count(),
            1,
            "Must be one active connection"
        );

        shutdown_notify.send(()).ok();
        let mut byte = [0u8; 1];
        let read = timeout(Duration::from_secs(5), client.read(&mut byte))
            .await
            .expect("the server should close an idle connection after shutdown")
            .expect("to read from the connection");
        assert_eq!(read, 0, "the server should close the connection, not write");
        timeout(Duration::from_secs(1), shutdown_notify.closed())
            .await
            .expect("should complete shutdown notification");
    }

    /// A shutdown lets a request that is already being handled finish: the client gets its
    /// response, marked `connection: close`, and then the server closes the connection and its
    /// task drops its shutdown receiver.
    #[tokio::test]
    async fn test_process_tcp_stream_graceful_shutdown_finishes_the_request_in_flight() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("to create listener");
        let addr = listener.local_addr().expect("to get local addr");
        let (shutdown_notify, shutdown_rx) = watch::channel(());

        let arrived = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let router = {
            let (arrived, release) = (Arc::clone(&arrived), Arc::clone(&release));
            Router::new().route(
                "/",
                get(move || async move {
                    arrived.notify_one();
                    release.notified().await;
                    "finished"
                }),
            )
        };
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("to accept connection");
            process_tcp_stream(stream, router, shutdown_rx);
        });
        // The test holds a client of the same pool until the end, so only the server can close
        // the connection.
        let client = build_test_http_client();
        let request = tokio::spawn({
            let client = client.clone();
            async move {
                let response = client.get(format!("http://{addr}/")).send().await?;
                let status = response.status();
                let connection = response.headers().get(http::header::CONNECTION).cloned();
                Ok::<_, reqwest::Error>((status, connection, response.text().await?))
            }
        });
        timeout(Duration::from_secs(5), arrived.notified())
            .await
            .expect("the request should reach its handler");

        // The handler cannot finish before `release`, so when the connection's task next runs,
        // the shutdown is the only branch of its select that is ready. On this current-thread
        // runtime, `yield_now` runs every queued task, the connection's among them, before the
        // test resumes, so the shutdown reaches the connection before the handler is released.
        shutdown_notify.send(()).ok();
        tokio::task::yield_now().await;
        assert!(
            !request.is_finished(),
            "the shutdown must not end a request in flight"
        );

        release.notify_one();
        let (status, connection, body) = timeout(Duration::from_secs(5), request)
            .await
            .expect("the request should finish once its handler does")
            .expect("the request task should not panic")
            .expect("the request in flight should get its response");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "finished");
        // hyper marks the response `connection: close` only when the shutdown reached the
        // connection while its request was in flight.
        assert_eq!(
            connection.as_ref().map(http::HeaderValue::as_bytes),
            Some(&b"close"[..]),
            "the shutdown should end keep-alive on the connection whose request was in flight"
        );
        timeout(Duration::from_secs(5), shutdown_notify.closed())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the server should close the connection after its request in flight; {} connection(s) still open",
                    shutdown_notify.receiver_count()
                )
            });
        drop(client);
    }

    /// A small `/v1/sql` result leaves the server as several short writes (the
    /// head, a chunk per record batch, the chunked terminator). With Nagle's
    /// algorithm enabled, every write after the first waits for the client to
    /// acknowledge the previous one, and a client in delayed-ACK mode withholds
    /// that acknowledgement for tens of milliseconds.
    ///
    /// regression test for #13867
    #[tokio::test]
    async fn test_process_tcp_stream_disables_nagle() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("to create listener");
        let addr = listener.local_addr().expect("to get local addr");
        let (_shutdown_notify, shutdown_rx) = watch::channel(());

        let accepted = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("to accept connection");
            // Duplicate the accepted socket, so its options stay readable after
            // the connection itself is handed to the server. Both refer to the
            // same socket, so they report the same option values.
            let stream = stream.into_std().expect("to unwrap accepted stream");
            let observer = stream.try_clone().expect("to duplicate accepted stream");
            assert!(
                !observer
                    .nodelay()
                    .expect("to read the accepted socket options"),
                "a freshly accepted connection must still have Nagle's algorithm enabled, otherwise this test cannot tell whether the server disables it"
            );
            let stream = TcpStream::from_std(stream).expect("to rewrap accepted stream");

            process_tcp_stream(stream, ok_router(), shutdown_rx);
            observer
        });

        let client = build_test_http_client();
        let resp = timeout(
            Duration::from_secs(2),
            client.get(format!("http://{addr}/")).send(),
        )
        .await
        .expect("to complete request before timeout")
        .expect("to get response");
        assert_eq!(resp.status(), StatusCode::OK);

        let observer = accepted.await.expect("to finish accepting the connection");
        assert!(
            observer
                .nodelay()
                .expect("to read the accepted socket options"),
            "the server must disable Nagle's algorithm on connections it accepts"
        );
    }

    #[tokio::test]
    async fn a_client_that_never_completes_the_tls_handshake_is_disconnected() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("to create listener");
        let addr = listener.local_addr().expect("to get local addr");
        let (shutdown_notify, shutdown_rx) = watch::channel(());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("to accept connection");
            process_tls_tcp_stream(
                stream,
                test_tls_acceptor(),
                ok_router(),
                IdentitySource::Anonymous,
                runtime_tls::ClientAuthEnforcement::Disabled,
                shutdown_rx,
                Duration::from_millis(200),
            );
        });

        // Connects and never sends a `ClientHello`.
        let mut silent = TcpStream::connect(addr).await.expect("to connect");
        let mut buf = [0u8; 1];
        let read = timeout(Duration::from_secs(5), silent.read(&mut buf))
            .await
            .expect("the server closes the connection once its handshake deadline passes");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "the server closes the connection without sending anything: {read:?}"
        );
        // The connection task holds a shutdown receiver until it ends, so a
        // graceful shutdown does not wait for this connection.
        timeout(Duration::from_secs(5), shutdown_notify.closed())
            .await
            .expect("the connection task ends");
    }

    fn build_test_http_client() -> reqwest::Client {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|err| panic!("failed to build test http client: {err}"))
    }
}
