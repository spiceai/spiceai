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

use std::{io, pin::Pin, sync::Arc, task::Poll, time::Duration};

use futures::{Stream, StreamExt};
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tokio_stream::wrappers::TcpListenerStream;

/// Maximum number of TLS handshakes to drive concurrently off the listener.
/// Generous because handshakes are I/O-bound and cheap to hold; the goal is to
/// keep the TCP accept queue drained under a connection burst (e.g. a
/// distributed shuffle where many peers each open dozens of fetch connections
/// at once).
const MAX_CONCURRENT_TLS_HANDSHAKES: usize = 1024;

/// How long a client has to complete the TLS handshake. Without a deadline, a
/// connection that never sends a `ClientHello` holds a handshake slot for as
/// long as it stays open, and once every slot is held the listener stops
/// accepting connections.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Build a stream of accepted+handshaked TLS connections suitable for
/// `tonic::transport::Server::serve_with_incoming`.
pub fn tls_incoming(
    listener: TcpListener,
    server_config: Arc<ServerConfig>,
) -> impl Stream<Item = io::Result<TlsStream<TcpStream>>> + Send + 'static {
    tls_incoming_with(
        listener,
        server_config,
        MAX_CONCURRENT_TLS_HANDSHAKES,
        TLS_HANDSHAKE_TIMEOUT,
    )
}

fn tls_incoming_with(
    listener: TcpListener,
    server_config: Arc<ServerConfig>,
    max_concurrent_handshakes: usize,
    handshake_timeout: Duration,
) -> impl Stream<Item = io::Result<TlsStream<TcpStream>>> + Send + 'static {
    let acceptor = TlsAcceptor::from(server_config);
    let tcp = TcpListenerStream::new(listener);

    // Drive many TLS handshakes concurrently. A burst of incoming connections —
    // e.g. a distributed shuffle where each reducer opens up to 64 fetch
    // connections per peer with no client-side pooling — must not back up behind
    // a single in-flight handshake. The previous implementation spawned a task
    // per accept but then `join.await`ed each one inline in the stream, which
    // serialized acceptance: the next TCP connection was not accepted until the
    // current handshake finished, overflowing the listener accept queue under
    // load so peers saw `connection reset by peer`. `buffer_unordered` keeps
    // accepting (draining the backlog) while up to `MAX_CONCURRENT_TLS_HANDSHAKES`
    // handshakes proceed at once. A failed handshake is logged and dropped —
    // yielding `Err` would terminate the tonic server.
    let handshakes = tcp
        .filter_map(|conn| async move {
            match conn {
                Ok(stream) => Some(stream),
                Err(e) => {
                    tracing::debug!("Flight: TCP accept error: {e}");
                    None
                }
            }
        })
        .map(move |stream| {
            let acceptor = acceptor.clone();
            async move {
                tokio::time::timeout(handshake_timeout, acceptor.accept(stream))
                    .await
                    .unwrap_or_else(|_| {
                        Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "the client did not complete the TLS handshake in time",
                        ))
                    })
            }
        })
        .buffer_unordered(max_concurrent_handshakes)
        .filter_map(|res| async move {
            match res {
                Ok(tls) => Some(Ok(tls)),
                Err(e) => {
                    tracing::debug!("Flight: TLS handshake error: {e}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::{
        ClientConfig, RootCertStore,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
    };
    use tokio_rustls::TlsConnector;

    #[tokio::test]
    async fn a_silent_connection_does_not_hold_the_last_handshake_slot() {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
        let cert =
            CertificateDer::from_pem_slice(include_bytes!("../../../test/tls/spiced_cert.pem"))
                .expect("the fixture is a PEM certificate");
        let key = PrivateKeyDer::from_pem_slice(include_bytes!("../../../test/tls/spiced_key.pem"))
            .expect("the fixture is a PEM private key");
        let root = CertificateDer::from_pem_slice(include_bytes!(
            "../../../test/tls/spiced_root_cert.pem"
        ))
        .expect("the fixture is a PEM certificate");

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .expect("the fixture certificate and key match");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binds a loopback port");
        let addr = listener.local_addr().expect("the listener has an address");
        let mut incoming = Box::pin(tls_incoming_with(
            listener,
            Arc::new(server_config),
            1,
            Duration::from_millis(200),
        ));

        // Takes the only handshake slot and never sends a `ClientHello`.
        let _silent = TcpStream::connect(addr).await.expect("connects");

        let mut roots = RootCertStore::empty();
        roots.add(root).expect("the root certificate is valid");
        let connector = TlsConnector::from(Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ));
        let client = tokio::spawn(async move {
            let tcp = TcpStream::connect(addr).await?;
            let name = ServerName::try_from("localhost").map_err(io::Error::other)?;
            connector.connect(name, tcp).await
        });

        let accepted = tokio::time::timeout(Duration::from_secs(5), incoming.next())
            .await
            .expect("the silent connection releases its slot when its handshake deadline passes");
        assert!(
            matches!(accepted, Some(Ok(_))),
            "the waiting client is accepted"
        );
        client
            .await
            .expect("the client task does not panic")
            .expect("the client completes its handshake");
    }
}
