/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `Stream<Item = Result<TlsStream<TcpStream>>>` adaptor for tonic's
//! `serve_with_incoming` API. Used by the Flight server (and by the cluster
//! gRPC server in a follow-up) so we can run TLS through `tokio_rustls`
//! against a hot-swappable [`rustls::ServerConfig`] instead of tonic's
//! `ServerTlsConfig` (which bakes the cert in once and has no rotation hook).
//!
//! Each incoming TCP connection is handed off to a per-connection task that
//! drives the TLS handshake. Handshake failures are logged at `debug` and
//! the stream continues; this matches the existing HTTP server's behavior
//! and prevents one bad client from killing the listener.

use std::{io, pin::Pin, sync::Arc, task::Poll};

use futures::{Stream, StreamExt};
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tokio_stream::wrappers::TcpListenerStream;

/// Build a stream of accepted+handshaked TLS connections suitable for
/// `tonic::transport::Server::serve_with_incoming`.
pub fn tls_incoming(
    listener: TcpListener,
    server_config: Arc<ServerConfig>,
) -> impl Stream<Item = io::Result<TlsStream<TcpStream>>> + Send + 'static {
    let acceptor = TlsAcceptor::from(server_config);
    let tcp = TcpListenerStream::new(listener);

    // Per-connection TLS handshake. We spawn one task per accept so a slow
    // handshake from one client does not block accepting the next.
    let handshakes = tcp
        .filter_map(move |conn| {
            let acceptor = acceptor.clone();
            async move {
                let stream = match conn {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::debug!("Flight: TCP accept error: {e}");
                        return None;
                    }
                };
                Some(tokio::spawn(async move { acceptor.accept(stream).await }))
            }
        })
        .filter_map(|join| async move {
            match join.await {
                Ok(Ok(tls)) => Some(Ok(tls)),
                Ok(Err(e)) => {
                    tracing::debug!("Flight: TLS handshake error: {e}");
                    // Yielding an `Err` here would terminate the tonic server.
                    // Drop the bad connection silently instead.
                    None
                }
                Err(join_err) => {
                    tracing::debug!("Flight: TLS handshake task panicked: {join_err}");
                    None
                }
            }
        });

    // Box-pin via a tiny adaptor so the `impl Stream` is `Unpin`.
    BoxIncoming {
        inner: Box::pin(handshakes),
    }
}

struct BoxIncoming<S> {
    inner: Pin<Box<S>>,
}

impl<S, I> Stream for BoxIncoming<S>
where
    S: Stream<Item = I>,
{
    type Item = I;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
