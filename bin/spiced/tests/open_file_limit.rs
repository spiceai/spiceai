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

//! A `spiced` process raises its soft open file limit to the hard limit.
//!
//! Regression test for #15338: containers start `spiced` with a soft
//! `RLIMIT_NOFILE` of 1024 under a much higher hard limit, and client
//! connections and acceleration files exhausted it. Each test starts a real
//! `spiced` under a chosen limit and checks what it reports and, on Linux, the
//! limit the kernel actually holds for it.

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const SPICEPOD: &str = "version: v1\nkind: Spicepod\nname: open-file-limit\n";

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("read ephemeral address")
}

/// A `spiced` started under the soft and hard open file limits given.
struct Spiced {
    child: Child,
    lines: mpsc::Receiver<String>,
    printed: Vec<String>,
    _dir: tempfile::TempDir,
}

impl Spiced {
    fn start(soft: u64, hard: u64) -> Self {
        let (_, current_hard) =
            nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE)
                .expect("read this process's open file limit");
        assert!(
            current_hard >= hard,
            "the test needs a hard open file limit of at least {hard} to lower it from, but this process has {current_hard}"
        );

        let dir = tempfile::tempdir().expect("creates the spicepod directory");
        std::fs::write(dir.path().join("spicepod.yaml"), SPICEPOD).expect("writes the spicepod");
        let mut command = Command::new(env!("CARGO_BIN_EXE_spiced"));
        // Each `spiced` claims its working directory as its instance directory, so
        // the tests start in their own directories to run side by side.
        command
            .current_dir(dir.path())
            .arg(dir.path())
            // The outcome is logged at DEBUG; `-v` enables it for `spiced`.
            .arg("-v")
            .args(["--http", &unused_local_addr().to_string()])
            .args(["--flight", &unused_local_addr().to_string()])
            .args(["--metrics", &unused_local_addr().to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        // SAFETY: `setrlimit` is async-signal-safe, and the closure touches
        // nothing but its two copied integers.
        unsafe {
            command.pre_exec(move || {
                nix::sys::resource::setrlimit(
                    nix::sys::resource::Resource::RLIMIT_NOFILE,
                    soft,
                    hard,
                )
                .map_err(std::io::Error::from)
            });
        }
        let mut child = command.spawn().expect("start spiced");

        let stdout = child.stdout.take().expect("piped stdout");
        let (lines_tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if lines_tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            lines,
            printed: Vec::new(),
            _dir: dir,
        }
    }

    /// The first line `spiced::open_file_limit` logs, or a failure naming what
    /// `spiced` printed instead.
    fn open_file_limit_line(&mut self) -> String {
        // A default-feature `spiced` binary is large enough that cold macOS
        // loading/signature checks can take tens of seconds on a busy builder.
        let deadline = Instant::now() + Duration::from_mins(1);
        loop {
            match self.lines.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    let matched = line.contains("spiced::open_file_limit");
                    self.printed.push(line.clone());
                    if matched {
                        return line;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        let state = match self.child.try_wait().expect("poll spiced") {
            Some(status) => format!("exited: {status}"),
            None => "was still running".to_string(),
        };
        panic!(
            "spiced logged nothing about its open file limit; spiced {state}\nspiced stdout:\n{}",
            self.printed.join("\n")
        );
    }

    /// The soft and hard open file limits the kernel holds for this `spiced`.
    #[cfg(target_os = "linux")]
    fn kernel_limits(&self) -> (u64, u64) {
        let limits = std::fs::read_to_string(format!("/proc/{}/limits", self.child.id()))
            .expect("read spiced's /proc limits");
        let line = limits
            .lines()
            .find(|line| line.starts_with("Max open files"))
            .unwrap_or_else(|| panic!("no 'Max open files' row in:\n{limits}"));
        let mut values = line["Max open files".len()..]
            .split_whitespace()
            .map(|value| value.parse::<u64>().expect("a numeric limit"));
        (
            values.next().expect("a soft limit"),
            values.next().expect("a hard limit"),
        )
    }
}

impl Drop for Spiced {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn spiced_raises_its_soft_open_file_limit_to_the_hard_limit() {
    let mut spiced = Spiced::start(256, 4096);
    let line = spiced.open_file_limit_line();
    assert!(
        line.ends_with("Raised the open file limit from 256 to 4096"),
        "unexpected line: {line}"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(spiced.kernel_limits(), (4096, 4096));
}

#[test]
fn spiced_reports_a_soft_limit_already_at_the_hard_limit() {
    let mut spiced = Spiced::start(256, 256);
    let line = spiced.open_file_limit_line();
    assert!(
        line.ends_with("Open file limit is 256"),
        "unexpected line: {line}"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(spiced.kernel_limits(), (256, 256));
}
