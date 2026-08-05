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
    /// Send the response head, then each body chunk with `gap` of quiet before it.
    ///
    /// When `then_hold` is set the connection is held open afterwards instead of being
    /// closed, so a client that waits for EOF rather than for the protocol's own terminator
    /// is left waiting.
    Dribble {
        chunks: Vec<String>,
        gap: Duration,
        then_hold: bool,
    },
    /// Send the response head and then nothing at all, holding the connection open.
    StallAfterHead,
    /// Send nothing at all, not even the response head, holding the connection open.
    StallBeforeHead,
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
        Self::start(Behaviour::Dribble {
            chunks,
            gap,
            then_hold: false,
        })
    }

    /// As [`SlowServer::dribbling`], but hold the connection open once the chunks are sent
    /// rather than closing it — a server under no obligation to hang up promptly.
    pub(crate) fn dribbling_then_holding(chunks: Vec<String>, gap: Duration) -> Self {
        Self::start(Behaviour::Dribble {
            chunks,
            gap,
            then_hold: true,
        })
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
            then_hold,
        } => {
            for chunk in chunks {
                thread::sleep(*gap);
                // Chunked transfer encoding: the length in hex, then the bytes.
                let framed = format!("{:x}\r\n{chunk}\r\n", chunk.len());
                if writer.write_all(framed.as_bytes()).is_err() || writer.flush().is_err() {
                    return;
                }
            }
            if *then_hold {
                hold_open(running);
            } else {
                let _ = writer.write_all(b"0\r\n\r\n");
                let _ = writer.flush();
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
