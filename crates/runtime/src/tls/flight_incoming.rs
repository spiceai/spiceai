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

/// Maximum number of TLS handshakes to drive concurrently off the listener.
/// Generous because handshakes are I/O-bound and cheap to hold; the goal is to
/// keep the TCP accept queue drained under a connection burst (e.g. a
/// distributed shuffle where many peers each open dozens of fetch connections
/// at once).
const MAX_CONCURRENT_TLS_HANDSHAKES: usize = 1024;

/// Build a stream of accepted+handshaked TLS connections suitable for
/// `tonic::transport::Server::serve_with_incoming`.
pub fn tls_incoming(
    listener: TcpListener,
    server_config: Arc<ServerConfig>,
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
    // TEMP INSTRUMENTATION (cluster shuffle-fetch "connection reset by peer"
    // diagnosis): concurrent buffer_unordered accept is already present, yet
    // peers still reset under SF10 load. Track accept + handshake rates and the
    // ticker's own scheduling lag to distinguish: accept-runtime STARVATION
    // (accepted stalls + ticker_lag grows -> the runtime isn't polling the
    // accept stream so the kernel backlog overflows and RSTs) vs handshake
    // failure (hs_err grows) vs a downstream/serving cause (accept+hs healthy,
    // clients still reset). REVERT before the final PR.
    let accept_n = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let hs_ok = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let hs_err = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let a = Arc::clone(&accept_n);
        let ok = Arc::clone(&hs_ok);
        let err = Arc::clone(&hs_err);
        tokio::spawn(async move {
            use std::sync::atomic::Ordering::Relaxed;
            let mut pa = 0u64;
            let mut pok = 0u64;
            let mut perr = 0u64;
            loop {
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let lag_ms = start.elapsed().as_millis().saturating_sub(5000);
                let na = a.load(Relaxed);
                let nok = ok.load(Relaxed);
                let nerr = err.load(Relaxed);
                tracing::warn!(
                    "TLS_ACCEPT_STATS accepted={na}(+{}) hs_ok={nok}(+{}) hs_err={nerr}(+{}) ticker_lag_ms={lag_ms}",
                    na.saturating_sub(pa),
                    nok.saturating_sub(pok),
                    nerr.saturating_sub(perr),
                );
                pa = na;
                pok = nok;
                perr = nerr;
            }
        });
    }

    let handshakes = tcp
        .filter_map({
            let accept_n = Arc::clone(&accept_n);
            move |conn| {
                let accept_n = Arc::clone(&accept_n);
                async move {
                    match conn {
                        Ok(stream) => {
                            accept_n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Some(stream)
                        }
                        Err(e) => {
                            tracing::debug!("Flight: TCP accept error: {e}");
                            None
                        }
                    }
                }
            }
        })
        .map(move |stream| {
            let acceptor = acceptor.clone();
            async move {
                // TEMP: time the handshake to detect CPU-bound slowness under
                // load. REVERT before PR.
                let t = std::time::Instant::now();
                let r = acceptor.accept(stream).await;
                let ms = t.elapsed().as_millis();
                if ms > 500 {
                    tracing::warn!("TLS_HS_SLOW {ms}ms ok={}", r.is_ok());
                }
                r
            }
        })
        .buffer_unordered(MAX_CONCURRENT_TLS_HANDSHAKES)
        .filter_map({
            let hs_ok = Arc::clone(&hs_ok);
            let hs_err = Arc::clone(&hs_err);
            move |res| {
                let hs_ok = Arc::clone(&hs_ok);
                let hs_err = Arc::clone(&hs_err);
                async move {
                    match res {
                        Ok(tls) => {
                            hs_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Some(Ok(tls))
                        }
                        Err(e) => {
                            let n = hs_err
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // TEMP: sample the handshake error reason at WARN
                            // (first 30 + every 5000th) to diagnose the ~16%
                            // failure rate. REVERT before PR.
                            if n < 30 || n % 5000 == 0 {
                                tracing::warn!("TLS_HS_ERR n={n}: {e:?}");
                            } else {
                                tracing::debug!("Flight: TLS handshake error: {e}");
                            }
                            None
                        }
                    }
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
