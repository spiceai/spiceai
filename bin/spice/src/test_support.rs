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

//! Test-only servers for exercising the CLI's HTTP clients.
//!
//! A client's deadline is not readable off the client, so the only way to tell a
//! whole-request deadline from a silence deadline is to answer a real request slowly and
//! see which one survives. `wiremock` delays a response as a unit, so it cannot express
//! "still arriving"; these servers speak enough HTTP/1.1 to do so.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// How the server answers a connection.
#[derive(Debug, Clone)]
enum Behaviour {
    /// Send the response head, then each body chunk with `gap` of quiet before it, and
    /// finish the way `ending` says.
    Dribble {
        chunks: Vec<Vec<u8>>,
        gap: Duration,
        ending: Ending,
    },
    /// Send the response head and then nothing at all, holding the connection open.
    StallAfterHead,
    /// Send nothing at all, not even the response head, holding the connection open.
    StallBeforeHead,
}

/// How a dribbling response ends once its chunks are sent.
#[derive(Debug, Clone, Copy)]
enum Ending {
    /// End the chunked body properly and hang up.
    Close,
    /// Hold the connection open, so a client that waits for EOF rather than for the
    /// protocol's own terminator is left waiting.
    Hold,
    /// Hang up without the terminating chunk, so the body the client is reading stops
    /// part-way through — a truncated response on an already-successful status.
    Truncate,
}

/// An HTTP/1.1 server that answers every connection the same slow way.
///
/// The listener is closed when this is dropped, so a test that returns early does not leave
/// a thread accepting.
pub(crate) struct SlowServer {
    url: String,
    running: Arc<AtomicBool>,
    /// The request targets seen so far, so a test can assert which endpoint was called. Every
    /// path is *answered* identically, so without this a test would pass against any route.
    targets: Arc<Mutex<Vec<String>>>,
}

impl SlowServer {
    /// Answer each request with `chunks`, waiting `gap` before every one of them.
    ///
    /// The response is therefore never quiet for longer than `gap`, but takes
    /// `gap * chunks.len()` in total — the shape that separates the two deadlines.
    pub(crate) fn dribbling(chunks: Vec<String>, gap: Duration) -> Self {
        Self::dribble(chunks, gap, Ending::Close)
    }

    /// As [`SlowServer::dribbling`], but with chunks that need not each be valid UTF-8 — the
    /// shape a test needs to split a multi-byte character across two reads.
    pub(crate) fn dribbling_bytes(chunks: Vec<Vec<u8>>, gap: Duration) -> Self {
        Self::start(Behaviour::Dribble {
            chunks,
            gap,
            ending: Ending::Close,
        })
    }

    fn dribble(chunks: Vec<String>, gap: Duration, ending: Ending) -> Self {
        Self::start(Behaviour::Dribble {
            chunks: chunks.into_iter().map(String::into_bytes).collect(),
            gap,
            ending,
        })
    }

    /// As [`SlowServer::dribbling`], but hold the connection open once the chunks are sent
    /// rather than closing it — a server under no obligation to hang up promptly.
    pub(crate) fn dribbling_then_holding(chunks: Vec<String>, gap: Duration) -> Self {
        Self::dribble(chunks, gap, Ending::Hold)
    }

    /// Send `chunks` and then hang up without ending the chunked body, so the client's read
    /// of an otherwise-successful response fails part-way through.
    pub(crate) fn truncating(chunks: Vec<String>, gap: Duration) -> Self {
        Self::dribble(chunks, gap, Ending::Truncate)
    }

    /// Answer each request with a response head and then silence, so a deadline is exercised
    /// during the body.
    pub(crate) fn stalling_after_head() -> Self {
        Self::start(Behaviour::StallAfterHead)
    }

    /// Accept the connection and the request, then send nothing, so a deadline is exercised
    /// while waiting for the response head — after the connect timeout has already been
    /// satisfied and so can no longer end the wait.
    pub(crate) fn stalling_before_head() -> Self {
        Self::start(Behaviour::StallBeforeHead)
    }

    /// The server's base URL. Every path is answered identically.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// The request targets received so far, in arrival order.
    pub(crate) fn targets(&self) -> Vec<String> {
        self.targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn start(behaviour: Behaviour) -> Self {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test server should bind a loopback port");
        let port = listener
            .local_addr()
            .expect("bound listener should have an address")
            .port();
        listener
            .set_nonblocking(true)
            .expect("test server listener should accept non-blocking");

        let running = Arc::new(AtomicBool::new(true));
        let accepting = Arc::clone(&running);
        let targets = Arc::new(Mutex::new(Vec::new()));
        let recording = Arc::clone(&targets);

        thread::spawn(move || {
            while accepting.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let behaviour = behaviour.clone();
                        let serving = Arc::clone(&accepting);
                        let recording = Arc::clone(&recording);
                        thread::spawn(move || serve(&stream, &behaviour, &serving, &recording));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            url: format!("http://127.0.0.1:{port}"),
            running,
            targets,
        }
    }
}

impl Drop for SlowServer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Answer one connection.
///
/// The request is read to its end before anything is written: replying to a socket that
/// still holds unread request bytes can surface to the client as a connection reset rather
/// than as the response, which would make a deadline test fail for the wrong reason.
fn serve(
    stream: &TcpStream,
    behaviour: &Behaviour,
    running: &AtomicBool,
    targets: &Mutex<Vec<String>>,
) {
    stream
        .set_nonblocking(false)
        .expect("accepted connection should be blocking");

    let Ok(target) = read_request(stream) else {
        return;
    };
    targets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(target);

    if matches!(behaviour, Behaviour::StallBeforeHead) {
        hold_open(running);
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }

    let mut writer = stream;
    if writer
        .write_all(
            b"HTTP/1.1 200 OK\r\n\
              Content-Type: text/event-stream\r\n\
              Transfer-Encoding: chunked\r\n\
              \r\n",
        )
        .is_err()
        || writer.flush().is_err()
    {
        return;
    }

    match behaviour {
        Behaviour::Dribble {
            chunks,
            gap,
            ending,
        } => {
            for chunk in chunks {
                thread::sleep(*gap);
                // Chunked transfer encoding: the length in hex, then the bytes.
                let mut framed = format!("{:x}\r\n", chunk.len()).into_bytes();
                framed.extend_from_slice(chunk);
                framed.extend_from_slice(b"\r\n");
                if writer.write_all(&framed).is_err() || writer.flush().is_err() {
                    return;
                }
            }
            match ending {
                Ending::Hold => hold_open(running),
                Ending::Close => {
                    let _ = writer.write_all(b"0\r\n\r\n");
                    let _ = writer.flush();
                }
                // Nothing more is written: the body stops mid-stream.
                Ending::Truncate => {}
            }
        }
        Behaviour::StallAfterHead => hold_open(running),
        // Handled before the response head was written.
        Behaviour::StallBeforeHead => {}
    }

    let _ = stream.shutdown(Shutdown::Both);
}

/// Hold the connection open, sending nothing, until the server is dropped. The client's
/// deadline is what has to end this.
fn hold_open(running: &AtomicBool) {
    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(10));
    }
}

/// Read a request's head and, when it declares one, its body, and return its target.
///
/// Only `Content-Length` bodies are understood, which is all `reqwest` sends here. A body
/// declared with any other framing would leave unread bytes on the socket, so an unparseable
/// length is an error rather than a silent zero.
fn read_request(stream: &TcpStream) -> std::io::Result<String> {
    let mut reader = BufReader::new(stream);
    let mut content_length = 0usize;
    let mut target = String::new();

    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let line = line.trim_end();
        if line.is_empty() {
            break;
        }

        if target.is_empty() {
            // The request line: METHOD SP target SP HTTP/1.1
            target = line.split(' ').nth(1).unwrap_or_default().to_string();
            continue;
        }

        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unparseable Content-Length: {value:?}"),
                )
            })?;
        }
    }

    if content_length > 0 {
        let mut body = vec![0u8; content_length];
        reader.read_exact(&mut body)?;
    }

    Ok(target)
}

/// An HTTPS server whose certificate is issued by a CA made for the test.
///
/// It answers every request with `200 OK` and a fixed JSON body, so a test can tell "the
/// client trusted the certificate and got an answer" from "the handshake failed". The CA
/// certificate is written to disk for `--tls-root-certificate-file`, alongside the certificate
/// of an unrelated CA, which did not issue the server's certificate.
pub(crate) struct TlsServer {
    url: String,
    running: Arc<AtomicBool>,
    dir: tempfile::TempDir,
}

impl TlsServer {
    /// Answer every request with `body` as `application/json`.
    pub(crate) fn answering(body: &'static str) -> Self {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};

        fn ca(name: &str) -> (rcgen::Certificate, KeyPair) {
            let mut params =
                CertificateParams::new(Vec::<String>::new()).expect("CA params should build");
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.distinguished_name.push(DnType::CommonName, name);
            let key = KeyPair::generate().expect("CA key should generate");
            let cert = params.self_signed(&key).expect("CA should self-sign");
            (cert, key)
        }

        let (ca_cert, ca_key) = ca("spice cli test CA");
        let (other_ca_cert, _) = ca("spice cli unrelated CA");

        let leaf_key = KeyPair::generate().expect("leaf key should generate");
        let leaf_cert =
            CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()])
                .expect("leaf params should build")
                .signed_by(
                    &leaf_key,
                    &Issuer::from_ca_cert_pem(&ca_cert.pem(), ca_key)
                        .expect("CA should become an issuer"),
                )
                .expect("CA should sign the leaf");

        let dir = tempfile::TempDir::new().expect("temp dir should be created");
        std::fs::write(dir.path().join("ca.pem"), ca_cert.pem()).expect("CA should be written");
        std::fs::write(dir.path().join("other-ca.pem"), other_ca_cert.pem())
            .expect("unrelated CA should be written");

        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("default protocol versions should be supported")
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf_cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into(),
        )
        .expect("server certificate should be accepted");
        let config = Arc::new(config);

        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test server should bind a loopback port");
        let port = listener
            .local_addr()
            .expect("bound listener should have an address")
            .port();
        listener
            .set_nonblocking(true)
            .expect("test server listener should accept non-blocking");

        let running = Arc::new(AtomicBool::new(true));
        let accepting = Arc::clone(&running);
        thread::spawn(move || {
            while accepting.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let config = Arc::clone(&config);
                        thread::spawn(move || serve_tls(stream, config, body));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            url: format!("https://127.0.0.1:{port}"),
            running,
            dir,
        }
    }

    /// The server's base URL. Every path is answered identically.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// The PEM file of the CA that issued the server's certificate.
    pub(crate) fn ca_file(&self) -> String {
        self.dir.path().join("ca.pem").display().to_string()
    }

    /// The PEM file of a CA that did not issue the server's certificate.
    pub(crate) fn unrelated_ca_file(&self) -> String {
        self.dir.path().join("other-ca.pem").display().to_string()
    }
}

impl Drop for TlsServer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Answer one TLS connection with `body`. A handshake the client rejects ends it quietly.
fn serve_tls(stream: TcpStream, config: Arc<rustls::ServerConfig>, body: &str) {
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    let Ok(conn) = rustls::ServerConnection::new(config) else {
        return;
    };
    let mut tls = rustls::StreamOwned::new(conn, stream);

    // Read the request head; the CLI's requests carry no body this server needs.
    let mut request = Vec::new();
    let mut buf = [0_u8; 1024];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        match tls.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => request.extend_from_slice(&buf[..n]),
        }
    }

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    if tls.write_all(response.as_bytes()).is_ok() {
        tls.conn.send_close_notify();
        let _ = tls.flush();
    }
}
