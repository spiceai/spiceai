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

//! Port forwards into a rootless Docker daemon's network namespace.
//!
//! A rootless daemon runs inside `RootlessKit`, and its `host` network is
//! `RootlessKit`'s namespace rather than this machine's. A container there
//! listening on `127.0.0.1` cannot be reached from this machine, and
//! `docker run -p` does nothing on the `host` network. `RootlessKit`'s own API
//! (the one the daemon uses to publish ports) forwards a port of this
//! machine's loopback to the same port on the namespace's loopback, which is
//! what a host-network container needs to be reached at the address it
//! advertises.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

/// How long one API call may take. The API answers from memory.
const TIMEOUT: Duration = Duration::from_secs(10);

/// `RootlessKit`'s API socket for the daemon `docker` talks to, if there is
/// one. `dockerd-rootless.sh` keeps its state in
/// `$XDG_RUNTIME_DIR/dockerd-rootless`, beside the daemon's own socket, so
/// the daemon's socket (`DOCKER_HOST`) leads to it even from another container
/// that mounts that directory elsewhere.
pub fn api_socket() -> Option<PathBuf> {
    let beside_daemon = std::env::var("DOCKER_HOST")
        .ok()
        .and_then(|host| host.strip_prefix("unix://").map(PathBuf::from))
        .and_then(|socket| socket.parent().map(Path::to_path_buf));
    let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from);
    [beside_daemon, runtime_dir]
        .into_iter()
        .flatten()
        .map(|dir| dir.join("dockerd-rootless").join("api.sock"))
        .find(|socket| socket.exists())
}

/// A forward from `127.0.0.1:<port>` on this machine to `127.0.0.1:<port>` in
/// the daemon's namespace, removed when dropped.
#[derive(Debug)]
pub struct Forward {
    socket: PathBuf,
    id: u64,
}

impl Forward {
    /// Forwards `port`. Fails if this machine's `127.0.0.1:<port>` is taken.
    pub fn add(socket: &Path, port: u16) -> Result<Self> {
        let body = json!({
            "proto": "tcp",
            "parentIP": "127.0.0.1",
            "parentPort": port,
            "childIP": "127.0.0.1",
            "childPort": port,
        });
        let (status, reply) = call(socket, "POST", "/v1/ports", Some(&body))
            .with_context(|| format!("Failed to forward port {port} into the rootless Docker daemon's network through {}", socket.display()))?;
        if status != 200 {
            bail!(
                "Failed to forward port {port} into the rootless Docker daemon's network: {} answered {status}: {}",
                socket.display(),
                reply.trim()
            );
        }
        let id = serde_json::from_str::<Value>(&reply)
            .ok()
            .and_then(|v| v.get("id").and_then(Value::as_u64))
            .with_context(|| {
                format!(
                    "Failed to forward port {port} into the rootless Docker daemon's network: {} answered without a forward id: {}",
                    socket.display(),
                    reply.trim()
                )
            })?;
        Ok(Self {
            socket: socket.to_path_buf(),
            id,
        })
    }
}

impl Drop for Forward {
    fn drop(&mut self) {
        // The forward holds this machine's port until removed; a failure
        // here leaves it held for the daemon's lifetime, which only takes
        // that port out of `free_port`'s choices.
        let _ = call(
            &self.socket,
            "DELETE",
            &format!("/v1/ports/{}", self.id),
            None,
        );
    }
}

/// One HTTP/1.0 request to the API, returning the status code and body.
fn call(socket: &Path, method: &str, path: &str, body: Option<&Value>) -> Result<(u16, String)> {
    let mut stream = UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    stream.write_all(request(method, path, body).as_bytes())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    parse_response(&response)
}

/// The request bytes. HTTP/1.0 so the server closes the connection after
/// answering, and the reply is read to its end.
fn request(method: &str, path: &str, body: Option<&Value>) -> String {
    let body = body.map(Value::to_string).unwrap_or_default();
    format!(
        "{method} {path} HTTP/1.0\r\nHost: rootlesskit\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn parse_response(response: &str) -> Result<(u16, String)> {
    let (head, body) = response
        .split_once("\r\n\r\n")
        .context("the API's reply has no end of headers")?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .with_context(|| format!("the API's reply has no status line: {head}"))?;
    Ok((status, body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    /// A fresh directory for one test's socket, short enough for a Unix
    /// socket path.
    fn socket_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rlk-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the socket directory");
        dir
    }

    /// A stand-in for `RootlessKit`'s API: answers each connection with the
    /// next reply and records what it was sent.
    fn fake_api(dir: &Path, replies: Vec<&'static str>) -> std::thread::JoinHandle<Vec<String>> {
        let listener = UnixListener::bind(dir.join("api.sock")).expect("bind the fake API");
        std::thread::spawn(move || {
            let mut seen = Vec::new();
            for reply in replies {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = vec![0; 4096];
                let n = stream.read(&mut buf).expect("read the request");
                seen.push(String::from_utf8_lossy(&buf[..n]).into_owned());
                stream.write_all(reply.as_bytes()).expect("reply");
            }
            seen
        })
    }

    #[test]
    fn a_forward_asks_for_the_same_loopback_port_on_both_sides_and_removes_it_when_dropped() {
        let dir = socket_dir("add");
        let api = fake_api(
            &dir,
            vec![
                "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{\"id\":7,\"spec\":{}}",
                "HTTP/1.0 200 OK\r\n\r\n",
            ],
        );
        let forward = Forward::add(&dir.join("api.sock"), 41234).expect("add");
        drop(forward);
        let seen = api.join().expect("fake API");
        let (head, body) = seen[0].split_once("\r\n\r\n").expect("request body");
        assert_eq!(head.lines().next(), Some("POST /v1/ports HTTP/1.0"));
        assert_eq!(
            serde_json::from_str::<Value>(body).expect("JSON body"),
            json!({"proto": "tcp", "parentIP": "127.0.0.1", "parentPort": 41234,
                   "childIP": "127.0.0.1", "childPort": 41234})
        );
        assert_eq!(seen[1].lines().next(), Some("DELETE /v1/ports/7 HTTP/1.0"));
    }

    #[test]
    fn a_refused_forward_names_the_port_and_the_api_answer() {
        let dir = socket_dir("refused");
        let _api = fake_api(
            &dir,
            vec![
                "HTTP/1.0 500 Internal Server Error\r\n\r\nlisten tcp 127.0.0.1:41235: bind: address already in use",
            ],
        );
        let error = Forward::add(&dir.join("api.sock"), 41235)
            .expect_err("a 500 is refused")
            .to_string();
        assert!(
            error.starts_with(
                "Failed to forward port 41235 into the rootless Docker daemon's network: "
            ) && error.ends_with(
                "answered 500: listen tcp 127.0.0.1:41235: bind: address already in use"
            ),
            "{error}"
        );
    }
}
