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

//! A minimal HTTP server for tests that need to observe what a client actually sends.
//!
//! Shared by the redirect-policy tests, which check where a credential is allowed to
//! travel, and by the client-construction tests, which check that a constructed client
//! refuses a plain-HTTP origin even when one is answering.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// A one-connection-at-a-time HTTP/1.1 stub, enough to answer a scripted exchange.
///
/// The workspace `tokio` carries no `net` feature, so this is a blocking `std::net`
/// listener on its own thread. Every response closes the connection, so each request
/// arrives as its own accept and the recorded order is the request order.
pub struct Stub {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    worker: Option<JoinHandle<()>>,
}

impl Stub {
    /// Answer each request with `respond(nth)`, where `nth` is 1-based, until dropped.
    pub fn serve(respond: impl Fn(usize) -> String + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("stub should bind a loopback port");
        let addr = listener
            .local_addr()
            .expect("stub should report its address");
        let requests = Arc::new(Mutex::new(Vec::new()));

        let recorded = Arc::clone(&requests);
        let worker = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let head = read_request_head(&mut stream);
                // The drop poke connects and sends nothing; that is the stop signal.
                if head.is_empty() {
                    break;
                }

                let nth = {
                    let mut recorded = recorded.lock().expect("request log should not be poisoned");
                    recorded.push(head);
                    recorded.len()
                };

                let _ = stream.write_all(respond(nth).as_bytes());
                let _ = stream.flush();
            }
        });

        Self {
            addr,
            requests,
            worker: Some(worker),
        }
    }

    /// The stub's own `http://` URL for `path`.
    pub fn url(&self, path: &str) -> String {
        let addr = self.addr;
        format!("http://{addr}{path}")
    }

    /// The request heads seen so far, in order, lower-cased so an assertion does not
    /// depend on how the client happens to case a header name on the wire.
    pub fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("request log should not be poisoned")
            .clone()
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        // Unblock the accept the worker is parked in, then let it finish.
        let _ = TcpStream::connect(self.addr);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Read one request's head, stopping at the blank line that ends it. These requests carry
/// no body, so nothing after it needs consuming.
fn read_request_head(stream: &mut TcpStream) -> String {
    /// Far above any head these tests produce, so reaching it means the peer is not
    /// sending one and the read should stop rather than accumulate without a bound.
    const MAX_HEAD_BYTES: usize = 16 * 1024;

    let mut head = Vec::new();
    let mut chunk = [0_u8; 256];

    loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                head.extend_from_slice(&chunk[..read]);
                if head.len() >= MAX_HEAD_BYTES
                    || head.windows(4).any(|window| window == b"\r\n\r\n")
                {
                    break;
                }
            }
        }
    }

    String::from_utf8_lossy(&head).to_lowercase()
}

/// A `307`, which preserves both the method and the body across the hop.
pub fn redirect_to(location: &str) -> String {
    format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n"
    )
}

pub fn ok_with(body: &str) -> String {
    let length = body.len();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\n\
         Connection: close\r\n\r\n{body}"
    )
}
