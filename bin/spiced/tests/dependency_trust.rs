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

//! A Spicepod dependency starts a program on the host only when the root
//! `spicepod.yaml` trusts it.
//!
//! The dependency declares a view and a stdio MCP tool whose "server" is
//! `touch <marker>`, so a started program leaves a file behind. These run the
//! real binary: what matters is whether a process is ever spawned.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("read ephemeral address")
}

/// An app whose one dependency, `thirdparty`, declares a view and a stdio MCP
/// tool. `entry` is the `dependencies:` entry naming it.
fn app(entry: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let root = tempfile::tempdir().expect("create the test root");
    let instance = root
        .path()
        .canonicalize()
        .expect("resolve the test root")
        .join("app");
    let dependency = instance.join("spicepods").join("thirdparty");
    std::fs::create_dir_all(&dependency).expect("create the dependency");
    let marker = instance.join("host-program-ran");
    std::fs::write(
        instance.join("spicepod.yaml"),
        format!("version: v2\nkind: Spicepod\nname: app\ndependencies:\n  - {entry}\n"),
    )
    .expect("write spicepod.yaml");
    std::fs::write(
        dependency.join("spicepod.yaml"),
        format!(
            "version: v2\nkind: Spicepod\nname: thirdparty\nviews:\n  - name: answer\n    sql: SELECT 42 AS answer\ntools:\n  - name: fetch\n    from: mcp:/usr/bin/touch\n    params:\n      mcp_args: {}\n",
            marker.display()
        ),
    )
    .expect("write the dependency");
    (root, instance, marker)
}

struct Spiced {
    child: Child,
    http: SocketAddr,
    log: PathBuf,
}

impl Spiced {
    fn start(instance: &Path) -> Self {
        let log = instance.join("spiced.log");
        let file = std::fs::File::create(&log).expect("create the log");
        let http = unused_local_addr();
        let child = Command::new(env!("CARGO_BIN_EXE_spiced"))
            .current_dir(instance)
            .args(["--http", &http.to_string()])
            .args(["--flight", &unused_local_addr().to_string()])
            .args(["--metrics", &unused_local_addr().to_string()])
            .args(["--open_telemetry", &unused_local_addr().to_string()])
            .stdout(Stdio::from(file.try_clone().expect("share the log")))
            .stderr(Stdio::from(file))
            .spawn()
            .expect("start spiced");
        Self { child, http, log }
    }

    fn printed(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Wait until `done` holds, or fail with the log after two minutes.
    fn wait_for(&self, what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_mins(2);
        while !done() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: {}",
                self.printed()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// `POST /v1/sql`, returning the response body.
    fn sql(&self, query: &str) -> String {
        let mut stream = TcpStream::connect(self.http).expect("connect to spiced");
        write!(
            stream,
            "POST /v1/sql HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{query}",
            self.http,
            query.len()
        )
        .expect("send the query");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("read the response");
        response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or_default()
    }

    fn ready(&self) -> bool {
        let Ok(mut stream) = TcpStream::connect(self.http) else {
            return false;
        };
        let _ = write!(
            stream,
            "GET /v1/ready HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.http
        );
        let mut response = String::new();
        let _ = stream.read_to_string(&mut response);
        response.starts_with("HTTP/1.1 200")
    }
}

impl Drop for Spiced {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The short form is `restricted`: the dependency's view answers, its tool is
/// not loaded, the ERROR names the fix, and the program never starts.
#[test]
fn a_restricted_dependency_does_not_start_its_stdio_tool() {
    let (_root, instance, marker) = app("thirdparty");
    let spiced = Spiced::start(&instance);

    let refusal = "ERROR runtime: Failed to load tool 'fetch' from Spicepod dependency 'thirdparty': the tool starts the program '/usr/bin/touch' on this host, and a dependency may do that only when spicepod.yaml trusts it. To trust it, replace `- thirdparty` under `dependencies:` with `- { from: thirdparty, trust: full }`. See: https://spiceai.org/docs/reference/spicepod#dependencies";
    spiced.wait_for("the refusal", || spiced.printed().contains(refusal));
    spiced.wait_for("readiness", || spiced.ready());
    let answer = spiced.sql("SELECT answer FROM answer");
    assert!(
        answer.contains("\"answer\":42"),
        "the rest of the dependency loads: {answer}"
    );
    // Tools load in the background; give a started program time to show.
    std::thread::sleep(Duration::from_secs(3));
    assert!(
        !marker.exists(),
        "the program must never start: {}",
        spiced.printed()
    );
}

/// `trust: full` lets the same dependency start its tool's program.
#[test]
fn a_trusted_dependency_starts_its_stdio_tool() {
    let (_root, instance, marker) = app("{ from: thirdparty, trust: full }");
    let spiced = Spiced::start(&instance);

    spiced.wait_for("the program to start", || marker.exists());
    assert!(
        !spiced.printed().contains("from Spicepod dependency 'thirdparty'"),
        "a trusted dependency is not refused: {}",
        spiced.printed()
    );
}
