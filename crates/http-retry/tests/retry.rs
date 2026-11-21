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

//! Which failed requests a client from `http_retry::client_builder` sends again, against a
//! local TLS and HTTP/2 server that fails connections the way a network or a server does.
//!
//! The server counts what reached it, so each test can check how many times a request was
//! sent as well as what the caller got back.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use h2::server::SendResponse;
use rstest::rstest;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

const ANSWER: &[u8] = b"answered";

/// What the server does with one TCP connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Connection {
    /// Reads the client's TLS `ClientHello` and closes the connection.
    CloseDuringHandshake,
    /// Reads the client's TLS `ClientHello` and resets the connection.
    ResetDuringHandshake,
    /// Completes TLS and HTTP/2 and answers every request.
    Serve,
    /// As [`Connection::Serve`], but refuses the first stream with `REFUSED_STREAM`.
    RefuseFirstStream,
    /// Completes TLS and HTTP/2, reads a request whole, and closes without answering.
    CloseAfterRequest,
    /// Completes TLS and, when the first request's `HEADERS` arrive, sends a graceful `GOAWAY`
    /// (`NO_ERROR`) whose last stream ID is 0: the server processed no stream.
    GoAwayBeforeStream,
    /// Completes TLS and HTTP/2, reads a request whole, then sends a graceful `GOAWAY`
    /// (`NO_ERROR`) whose last stream ID covers that request, and closes without answering.
    GoAwayAfterRequest,
}

/// What reached the server.
#[derive(Debug, Default)]
struct Counts {
    connections: AtomicUsize,
    streams: AtomicUsize,
}

struct Server {
    url: String,
    certificate: CertificateDer<'static>,
    counts: Arc<Counts>,
}

impl Server {
    /// A server that treats its first connection as `first` says, when given, and every other
    /// one as `then`.
    async fn start(first: Option<Connection>, then: Connection) -> Self {
        let (certificate, key) = self_signed();
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("the default TLS versions")
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)
        .expect("the server certificate");
        config.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a local port");
        let port = listener.local_addr().expect("the local address").port();
        let counts = Arc::new(Counts::default());
        tokio::spawn({
            let counts = Arc::clone(&counts);
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let index = counts.connections.fetch_add(1, Ordering::SeqCst);
                    let behavior = first.filter(|_| index == 0).unwrap_or(then);
                    tokio::spawn(handle(
                        stream,
                        behavior,
                        acceptor.clone(),
                        Arc::clone(&counts),
                    ));
                }
            }
        });

        Self {
            url: format!("https://127.0.0.1:{port}/v1/chat/completions"),
            certificate,
            counts,
        }
    }

    /// A client from `http_retry::client_builder` that trusts only `certificate`.
    fn client_trusting(certificate: &CertificateDer<'static>) -> reqwest::Client {
        http_retry::client_builder()
            .tls_certs_only([
                reqwest::Certificate::from_der(certificate).expect("a DER certificate")
            ])
            .no_proxy()
            .build()
            .expect("the client")
    }

    fn client(&self) -> reqwest::Client {
        Self::client_trusting(&self.certificate)
    }

    /// Sends a chat-completions-shaped POST and returns the body of a successful answer.
    async fn send(&self, client: &reqwest::Client) -> reqwest::Result<Bytes> {
        client
            .post(&self.url)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(r#"{"model":"m","messages":[{"role":"user","content":"Say Hello"}]}"#)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await
    }

    /// `[connections, streams]`.
    fn counts(&self) -> [usize; 2] {
        [
            self.counts.connections.load(Ordering::SeqCst),
            self.counts.streams.load(Ordering::SeqCst),
        ]
    }
}

fn self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let identity = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])
        .expect("a self-signed certificate");
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        identity.signing_key.serialize_der(),
    ));
    (identity.cert.der().clone(), key)
}

async fn handle(
    mut stream: TcpStream,
    behavior: Connection,
    acceptor: TlsAcceptor,
    counts: Arc<Counts>,
) {
    match behavior {
        Connection::CloseDuringHandshake => {
            read_client_hello(&mut stream).await;
        }
        Connection::ResetDuringHandshake => {
            read_client_hello(&mut stream).await;
            stream.set_zero_linger().expect("a zero linger");
        }
        Connection::GoAwayBeforeStream => {
            if let Ok(tls) = acceptor.accept(stream).await {
                go_away_before_any_stream(tls).await;
            }
        }
        Connection::Serve
        | Connection::RefuseFirstStream
        | Connection::CloseAfterRequest
        | Connection::GoAwayAfterRequest => {
            // A client that does not trust the certificate ends the handshake here.
            if let Ok(tls) = acceptor.accept(stream).await {
                serve(tls, behavior, &counts).await;
            }
        }
    }
}

/// Speaks HTTP/2 frames directly, because `h2`'s server cannot send a `GOAWAY` whose last
/// stream ID is below a stream it has already read: it sends its own `SETTINGS`, reads the
/// client's frames up to the first `HEADERS`, answers with `GOAWAY` (last stream ID 0,
/// `NO_ERROR`), and reads until the client closes, so the connection ends with a FIN.
async fn go_away_before_any_stream(mut tls: TlsStream<TcpStream>) {
    const SETTINGS: [u8; 9] = [0, 0, 0, 0x4, 0, 0, 0, 0, 0];
    const HEADERS: u8 = 0x1;
    #[rustfmt::skip]
    const GOAWAY_NO_STREAM: [u8; 17] = [
        0, 0, 8, 0x7, 0, 0, 0, 0, 0, // an 8-byte GOAWAY frame on stream 0
        0, 0, 0, 0, // last stream ID: 0
        0, 0, 0, 0, // error code: NO_ERROR
    ];

    let mut preface = [0_u8; 24];
    if tls.read_exact(&mut preface).await.is_err() || tls.write_all(&SETTINGS).await.is_err() {
        return;
    }
    loop {
        let mut header = [0_u8; 9];
        if tls.read_exact(&mut header).await.is_err() {
            return;
        }
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        let mut payload = vec![0_u8; length];
        if tls.read_exact(&mut payload).await.is_err() {
            return;
        }
        if header[3] == HEADERS {
            break;
        }
    }
    if tls.write_all(&GOAWAY_NO_STREAM).await.is_err() || tls.flush().await.is_err() {
        return;
    }
    let mut rest = Vec::new();
    let _ = tls.read_to_end(&mut rest).await;
}

/// Reads the first TLS record whole, so the connection closes with nothing left unread: a
/// close then sends a FIN, not a reset.
async fn read_client_hello(stream: &mut TcpStream) {
    let mut header = [0_u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .expect("a TLS record header");
    let mut record = vec![0_u8; usize::from(u16::from_be_bytes([header[3], header[4]]))];
    stream
        .read_exact(&mut record)
        .await
        .expect("the ClientHello");
}

async fn serve(tls: TlsStream<TcpStream>, behavior: Connection, counts: &Counts) {
    let Ok(mut connection) = h2::server::handshake(tls).await else {
        return;
    };
    // A stream's body arrives only while the connection is polled, so each stream is read on
    // a task of its own. One whose request is to go unanswered is handed back here, so the
    // connection closes before anything else is written to it.
    let (unanswered_tx, mut unanswered_rx) = mpsc::unbounded_channel::<SendResponse<Bytes>>();
    loop {
        tokio::select! {
            next = connection.accept() => {
                let Some(Ok((request, mut respond))) = next else {
                    return;
                };
                let first = counts.streams.fetch_add(1, Ordering::SeqCst) == 0;
                if behavior == Connection::RefuseFirstStream && first {
                    respond.send_reset(h2::Reason::REFUSED_STREAM);
                    continue;
                }
                let unanswered_tx = unanswered_tx.clone();
                tokio::spawn(async move {
                    let mut body = request.into_body();
                    while let Some(Ok(chunk)) = body.data().await {
                        let _ = body.flow_control().release_capacity(chunk.len());
                    }
                    if matches!(
                        behavior,
                        Connection::CloseAfterRequest | Connection::GoAwayAfterRequest
                    ) {
                        let _ = unanswered_tx.send(respond);
                        return;
                    }
                    let response = http::Response::builder()
                        .status(200)
                        .body(())
                        .expect("a response");
                    if let Ok(mut send) = respond.send_response(response, false) {
                        let _ = send.send_data(Bytes::from_static(ANSWER), true);
                    }
                });
            }
            Some(unanswered) = unanswered_rx.recv() => {
                if behavior == Connection::GoAwayAfterRequest {
                    // The `GOAWAY` names the last stream the server read, this one. Polling
                    // the connection sends it and then closes the connection. The stream is
                    // held open meanwhile, so it ends with the connection, not with a reset.
                    connection.abrupt_shutdown(h2::Reason::NO_ERROR);
                    while connection.accept().await.is_some() {}
                    drop(unanswered);
                }
                return;
            }
        }
    }
}

/// The failure seen in CI against Azure `OpenAI` (`tls handshake eof`): the network closed a
/// new connection during the TLS handshake. The request was never sent, so it is sent again on
/// a new connection and answered.
#[rstest]
#[tokio::test]
async fn a_connection_closed_during_the_tls_handshake_is_sent_again(
    #[values(Connection::CloseDuringHandshake, Connection::ResetDuringHandshake)]
    failure: Connection,
) {
    let server = Server::start(Some(failure), Connection::Serve).await;

    let answer = server.send(&server.client()).await;

    assert_eq!(
        answer.as_deref().ok(),
        Some(ANSWER),
        "after {failure:?}: {answer:?}"
    );
    assert_eq!(
        server.counts(),
        [2, 1],
        "the first connection failed and the second carried the only request"
    );
}

/// A server that closes every handshake gets the request `1 + MAX_RETRIES_PER_REQUEST` times
/// and the caller gets the connection error.
#[tokio::test]
async fn a_connection_that_closes_every_time_fails_after_the_last_retry() {
    let server = Server::start(None, Connection::CloseDuringHandshake).await;

    let error = server
        .send(&server.client())
        .await
        .expect_err("no connection completes its handshake");

    assert!(error.is_connect(), "{error:?}");
    assert!(
        format!("{error:?}").contains("tls handshake eof"),
        "{error:?}"
    );
    let attempts = usize::try_from(1 + http_retry::MAX_RETRIES_PER_REQUEST).expect("a count");
    assert_eq!(server.counts(), [attempts, 0]);
}

/// The server read the request, so it may have acted on it: the request is not sent again,
/// even though the next connection would answer it.
#[tokio::test]
async fn a_request_the_server_received_is_not_sent_again() {
    let server = Server::start(Some(Connection::CloseAfterRequest), Connection::Serve).await;

    let error = server
        .send(&server.client())
        .await
        .expect_err("the server closed the connection without answering");

    assert!(!error.is_connect(), "{error:?}");
    assert_eq!(
        server.counts(),
        [1, 1],
        "the request was sent once: {error:?}"
    );
}

/// A certificate the client does not trust fails the same way on every connection, so the
/// handshake is not tried again.
#[tokio::test]
async fn an_untrusted_certificate_is_not_tried_again() {
    let server = Server::start(None, Connection::Serve).await;
    let (other_certificate, _) = self_signed();

    let error = server
        .send(&Server::client_trusting(&other_certificate))
        .await
        .expect_err("the server's certificate is not trusted");

    assert!(error.is_connect(), "{error:?}");
    assert_eq!(server.counts(), [1, 0], "{error:?}");
}

/// `reqwest` retries a stream the server refused without processing it. Setting a retry policy
/// replaces `reqwest`'s default one, so this checks the policy keeps that retry.
#[tokio::test]
async fn a_stream_the_server_refused_is_sent_again() {
    let server = Server::start(None, Connection::RefuseFirstStream).await;

    let answer = server.send(&server.client()).await;

    assert_eq!(answer.as_deref().ok(), Some(ANSWER), "{answer:?}");
    assert_eq!(
        server.counts(),
        [1, 2],
        "one connection carried the refused stream and the answered one"
    );
}

/// A graceful `GOAWAY` whose last stream ID is below the request's stream says the server did
/// not process that stream (RFC 9113, section 6.8), so the request is sent again on a new
/// connection. `reqwest` retries this by default; this checks the policy keeps that retry.
#[tokio::test]
async fn a_stream_after_a_graceful_goaway_is_sent_again() {
    let server = Server::start(Some(Connection::GoAwayBeforeStream), Connection::Serve).await;

    let answer = server.send(&server.client()).await;

    assert_eq!(answer.as_deref().ok(), Some(ANSWER), "{answer:?}");
    assert_eq!(
        server.counts(),
        [2, 1],
        "the first connection processed no stream and the second answered the request"
    );
}

/// A graceful `GOAWAY` whose last stream ID covers the request says the server may have
/// processed it, so when the connection then closes without an answer, the request is not sent
/// again.
#[tokio::test]
async fn a_stream_a_graceful_goaway_covers_is_not_sent_again() {
    let server = Server::start(Some(Connection::GoAwayAfterRequest), Connection::Serve).await;

    let error = server
        .send(&server.client())
        .await
        .expect_err("the server closed the connection without answering");

    assert!(!error.is_connect(), "{error:?}");
    assert_eq!(
        server.counts(),
        [1, 1],
        "the request was sent once: {error:?}"
    );
}
