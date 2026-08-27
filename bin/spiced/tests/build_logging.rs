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

//! Events emitted while the runtime is built are not dropped.
//!
//! Regression test for #13537: the build ran between the temporary subscriber
//! `main` ends and the global one `init_tracing` installs, so its warnings —
//! the coordinated accelerator memory budget, invalid `runtime.query.*` values
//! — were dropped at every log level.
//!
//! Two halves, one test each: that the build's events reach a subscriber
//! already installed, and that a real `spiced` process — where none is, until
//! after the build — still prints them.

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// The warning `Runtime::build()` emits for an unparseable `runtime.query.timeout`.
const TIMEOUT_WARNING: &str = "No query timeout will be applied.";

const SPICEPOD: &str = "version: v1\nkind: Spicepod\nname: build-logging\nruntime:\n  query:\n    timeout: notaduration\n";

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("read ephemeral address")
}

/// Writes the spicepod under `dir` and returns the directory to run against.
fn spicepod_dir(dir: &tempfile::TempDir) -> &std::path::Path {
    std::fs::write(dir.path().join("spicepod.yaml"), SPICEPOD).expect("writes the spicepod");
    dir.path()
}

/// Sink for the probe subscriber, so the test can assert on what reached it.
#[derive(Clone, Default)]
struct ProbeWriter(Arc<Mutex<Vec<u8>>>);

impl ProbeWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("probe buffer poisoned")).into_owned()
    }
}

impl std::io::Write for ProbeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("probe buffer poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for ProbeWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn build_time_warnings_reach_the_installed_subscriber() {
    let probe = ProbeWriter::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(probe.clone()),
        ),
    )
    .expect("installs the global subscriber");

    let dir = tempfile::tempdir().expect("creates the spicepod directory");
    let app = app::AppBuilder::build_from_path(spicepod_dir(&dir))
        .await
        .expect("loads the spicepod");
    let _rt = runtime::Runtime::builder().with_app(app).build().await;

    let logged = probe.contents();
    assert!(
        logged.contains(TIMEOUT_WARNING),
        "the invalid-timeout warning must reach the installed subscriber, got: {logged}"
    );
    // Emitted through `in_tracing_context`, which must defer to the subscriber
    // already installed rather than shadow it with a temporary one.
    assert!(
        logged.contains("Initialized sql results cache"),
        "the caching line must reach the installed subscriber, got: {logged}"
    );
}

/// The build window itself. Nothing installs a subscriber until after
/// `Runtime::build()` returns, so only a real process proves the build's own
/// events still reach the console.
#[test]
fn a_spiced_process_prints_what_the_runtime_build_emits() {
    let dir = tempfile::tempdir().expect("creates the spicepod directory");
    let mut child = Command::new(env!("CARGO_BIN_EXE_spiced"))
        .arg(spicepod_dir(&dir))
        .args(["--http", &unused_local_addr().to_string()])
        .args(["--flight", &unused_local_addr().to_string()])
        .args(["--metrics", &unused_local_addr().to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start spiced");

    let stdout = child.stdout.take().expect("piped stdout");
    let (lines_tx, lines_rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });

    // A default-feature `spiced` binary is large enough that cold macOS
    // loading/signature checks can take tens of seconds on a busy builder.
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut printed = Vec::new();
    let found = loop {
        match lines_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => {
                let matched = line.contains(TIMEOUT_WARNING);
                printed.push(line);
                if matched {
                    break true;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break false,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= deadline {
            break false;
        }
    };

    child.kill().expect("stop spiced");
    child.wait().expect("reap spiced");

    assert!(
        found,
        "spiced printed nothing from the runtime build, got:\n{}",
        printed.join("\n")
    );
}
