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

//! Spec for the `SFTP` buffered read.
//!
//! `SFTPObjectStore::get_opts` sizes its buffer from the `SFTP` `FSTAT` size.
//! These tests point that call at a local server whose stat size is chosen by
//! the test and whose readable bytes are a 16-byte payload. A resolved length
//! above [`BUFFERED_READ_CAP`] (the same two-gibibyte cap the `SMB` store
//! enforces before it allocates) must come back as an error that names both
//! that length and the cap. The process must not ask the allocator for a
//! buffer that large first: the probe's allocator refuses that request, the
//! runtime aborts, and the parent treats the abort as a failure.
//!
//! A later stat of the same object is not a second chance to allocate. The
//! grow server answers the first stat with the payload length and every later
//! stat with the over-cap size. The read must still not allocate the over-cap
//! size.
//!
//! The same unbounded buffer exists in `FTPObjectStore::get_opts`
//! (`Vec::with_capacity` of the `SIZE` reply) and `NFSObjectStore::get_opts`
//! (`pread` of the reported `nfs_size`). Those stores are not covered here.
//!
//! The server is the `paramiko` 4.0.0 `SFTP` implementation. Install it with
//! `python3 -m pip install --user 'paramiko==4.0.0'` before running:
//!
//! ```text
//! cargo test -p runtime-object-store --features sftp --test sftp_buffered_read_cap
//! ```

#![cfg(feature = "sftp")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use std::{env, fs, thread};

use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use runtime_object_store::store::sftp::SFTPObjectStore;

/// Same ceiling `SMB` uses for one buffered read: two gibibytes.
const BUFFERED_READ_CAP: u64 = 2 * 1024 * 1024 * 1024;

/// Reported size used by the over-cap server. The extra 32 bytes let an
/// offset of 32 still resolve to one byte past the cap.
const REPORTED_SIZE: u64 = BUFFERED_READ_CAP + 1 + 32;

const PAYLOAD: &[u8] = b"abcdefghijklmnop";

fn payload_len() -> u64 {
    u64::try_from(PAYLOAD.len()).expect("the payload length fits in u64")
}

const PLACEHOLDER_USER: &str = "placeholder-user";
const PLACEHOLDER_PASSWORD: &str = "placeholder-password";

const PROBE_TEST: &str = "sftp_buffered_read_cap_probe";

struct GuardAllocator;

/// Forwards ordinary allocations and refuses any single request at or above
/// [`BUFFERED_READ_CAP`]. A null return makes the probe's alloc-error hook
/// exit before the bytes are touched.
///
/// # Safety
///
/// The methods honor the [`GlobalAlloc`] contract: pointers passed back to
/// `dealloc` and `realloc` are ones this allocator returned, and a refused
/// request returns null without creating a pointer.
unsafe impl GlobalAlloc for GuardAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if exceeds_cap(layout.size()) {
            return std::ptr::null_mut();
        }
        // SAFETY: `layout` is a non-zero layout from the allocator, and the
        // system allocator accepts it.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if exceeds_cap(layout.size()) {
            return std::ptr::null_mut();
        }
        // SAFETY: same layout contract as `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was allocated by `System` for `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if exceeds_cap(new_size) {
            return std::ptr::null_mut();
        }
        // SAFETY: `ptr` was allocated by `System` for `layout`, and `new_size`
        // is non-zero when the caller asks to grow a buffer.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn exceeds_cap(size: usize) -> bool {
    u64::try_from(size).unwrap_or(u64::MAX) >= BUFFERED_READ_CAP
}

#[global_allocator]
static ALLOCATOR: GuardAllocator = GuardAllocator;

#[test]
#[ignore = "spawned by the buffered-read cap tests with SFTP_CAP_CASE set"]
fn sftp_buffered_read_cap_probe() {
    let case = env::var("SFTP_CAP_CASE").expect("SFTP_CAP_CASE is set by the parent test");
    let port = env::var("SFTP_CAP_PORT").expect("SFTP_CAP_PORT is set by the parent test");
    let location =
        env::var("SFTP_CAP_LOCATION").expect("SFTP_CAP_LOCATION is set by the parent test");
    let store = SFTPObjectStore::new(
        PLACEHOLDER_USER.to_string(),
        PLACEHOLDER_PASSWORD.to_string(),
        "127.0.0.1".to_string(),
        port,
        Some(Duration::from_secs(20)),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the probe runtime should start");
    runtime.block_on(run_probe(&store, &case, &location));
}

async fn run_probe(store: &SFTPObjectStore, case: &str, location: &str) {
    let path = ObjectPath::from(location);
    let outcome = match case {
        "get" => read_outcome(ObjectStoreExt::get(store, &path).await).await,
        "offset-zero" => {
            read_outcome(
                store
                    .get_opts(
                        &path,
                        GetOptions::new().with_range(Some(GetRange::Offset(0))),
                    )
                    .await,
            )
            .await
        }
        "offset-remainder" => {
            read_outcome(
                store
                    .get_opts(
                        &path,
                        GetOptions::new().with_range(Some(GetRange::Offset(32))),
                    )
                    .await,
            )
            .await
        }
        "suffix" => {
            read_outcome(
                store
                    .get_opts(
                        &path,
                        GetOptions::new().with_range(Some(GetRange::Suffix(REPORTED_SIZE))),
                    )
                    .await,
            )
            .await
        }
        "bounded" => {
            read_outcome(
                store
                    .get_opts(
                        &path,
                        GetOptions::new().with_range(Some(GetRange::Bounded(0..REPORTED_SIZE))),
                    )
                    .await,
            )
            .await
        }
        "bounded-overshoot" => {
            read_outcome(
                store
                    .get_opts(
                        &path,
                        GetOptions::new()
                            .with_range(Some(GetRange::Bounded(0..REPORTED_SIZE + 100))),
                    )
                    .await,
            )
            .await
        }
        "get-range" => match ObjectStoreExt::get_range(store, &path, 0..REPORTED_SIZE).await {
            Ok(bytes) => Ok(format!(
                "len={} body={}",
                bytes.len(),
                String::from_utf8_lossy(&bytes)
            )),
            Err(error) => Err(error.to_string()),
        },
        "bounded-small" => {
            read_outcome(
                store
                    .get_opts(
                        &path,
                        GetOptions::new().with_range(Some(GetRange::Bounded(0..payload_len()))),
                    )
                    .await,
            )
            .await
        }
        "get-range-small" => {
            match ObjectStoreExt::get_range(store, &path, 0..payload_len()).await {
                Ok(bytes) => Ok(format!(
                    "len={} body={}",
                    bytes.len(),
                    String::from_utf8_lossy(&bytes)
                )),
                Err(error) => Err(error.to_string()),
            }
        }
        "offset-tail" => {
            read_outcome(
                store
                    .get_opts(
                        &path,
                        GetOptions::new()
                            .with_range(Some(GetRange::Offset(REPORTED_SIZE - payload_len()))),
                    )
                    .await,
            )
            .await
        }
        "head" => match store
            .get_opts(&path, GetOptions::new().with_head(true))
            .await
        {
            Ok(result) => {
                let size = result.meta.size;
                match result.bytes().await {
                    Ok(bytes) => Ok(format!("head size={size} len={}", bytes.len())),
                    Err(error) => Err(error.to_string()),
                }
            }
            Err(error) => Err(error.to_string()),
        },
        other => panic!("unknown probe case {other}"),
    };

    match outcome {
        Ok(detail) => println!("RESULT ok {detail}"),
        Err(error) => println!("RESULT err {error}"),
    }
}

async fn read_outcome(
    result: object_store::Result<object_store::GetResult>,
) -> Result<String, String> {
    let result = result.map_err(|error| error.to_string())?;
    let bytes = result.bytes().await.map_err(|error| error.to_string())?;
    Ok(format!(
        "len={} body={}",
        bytes.len(),
        String::from_utf8_lossy(&bytes)
    ))
}

struct RunningServer {
    child: std::process::Child,
    port: String,
    open_log: PathBuf,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server(mode: &str) -> RunningServer {
    let directory = env::temp_dir().join(format!("sftp-read-cap-{}-{}", mode, std::process::id()));
    fs::create_dir_all(&directory).expect("the server directory should be created");
    let script = directory.join("server.py");
    let open_log = directory.join("opens.log");
    fs::write(&script, SERVER_SCRIPT).expect("the server script should be written");
    fs::write(&open_log, "").expect("the open log should be created");

    let mut child = Command::new("python3")
        .arg(&script)
        .env("SFTP_SERVER_MODE", mode)
        .env("SFTP_REPORTED_SIZE", REPORTED_SIZE.to_string())
        .env("SFTP_PAYLOAD_HEX", hex_encode(PAYLOAD))
        .env("SFTP_OPEN_LOG", &open_log)
        .env("SFTP_USER", PLACEHOLDER_USER)
        .env("SFTP_PASSWORD", PLACEHOLDER_PASSWORD)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3 should start the local SFTP server");

    let stdout = child
        .stdout
        .take()
        .expect("the server stdout should be piped");
    let stderr = child
        .stderr
        .take()
        .expect("the server stderr should be piped");
    let (port_tx, port_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = port_tx.send(line);
    });
    let (err_tx, err_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut collected = String::new();
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            collected.push_str(&line);
            line.clear();
        }
        let _ = err_tx.send(collected);
    });

    let port = port_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the SFTP server should print its port")
        .trim()
        .to_string();
    assert!(
        port.parse::<u16>().is_ok(),
        "the server should print a port, got {port:?}. stderr: {}",
        err_rx.try_recv().unwrap_or_default()
    );

    RunningServer {
        child,
        port,
        open_log,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn probe(server: &RunningServer, case: &str, location: &str) -> std::process::Output {
    let executable = env::current_exe().expect("the test executable path");
    let mut command = Command::new(executable);
    command
        .env("SFTP_CAP_CASE", case)
        .env("SFTP_CAP_PORT", &server.port)
        .env("SFTP_CAP_LOCATION", location)
        .args([
            "--ignored",
            "--exact",
            PROBE_TEST,
            "--nocapture",
            "--test-threads=1",
        ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the probe process should start");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    match rx.recv_timeout(Duration::from_secs(90)) {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => panic!("the probe process failed to report its output: {error}"),
        Err(_) => panic!("probe {case} on {location} did not finish within 90s"),
    }
}

fn opened_log(server: &RunningServer) -> String {
    fs::read_to_string(&server.open_log).expect("the open log should be readable")
}

fn assert_opened(server: &RunningServer, location: &str) {
    let log = opened_log(server);
    let expected = format!("open /{location}");
    assert!(
        log.contains(&expected),
        "the server should have opened the requested object {expected}, log:\n{log}"
    );
}

fn whole_numbers(text: &str) -> Vec<u64> {
    let mut numbers = Vec::new();
    let mut current: Option<u64> = None;
    for byte in text.bytes() {
        if byte.is_ascii_digit() {
            let digit = u64::from(byte - b'0');
            current = Some(
                current
                    .unwrap_or(0)
                    .saturating_mul(10)
                    .saturating_add(digit),
            );
        } else if let Some(value) = current.take() {
            numbers.push(value);
        }
    }
    if let Some(value) = current {
        numbers.push(value);
    }
    numbers
}

fn aborted_on_allocation(output: &std::process::Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("memory allocation of") {
        return true;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if output.status.signal() == Some(6) {
            return true;
        }
    }
    false
}

fn assert_did_not_allocate(case: &str, output: &std::process::Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !aborted_on_allocation(output),
        "{case} allocated a buffer at or above the {BUFFERED_READ_CAP}-byte cap. \
         stderr:\n{stderr}\nstdout:\n{stdout}"
    );
}

fn assert_refused(server: &RunningServer, case: &str, location: &str, resolved_length: u64) {
    let output = probe(server, case, location);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_opened(server, location);
    assert_did_not_allocate(case, &output);
    assert!(
        output.status.success(),
        "{case} should return a cap error for a resolved length of {resolved_length}, \
         status {:?}. stderr:\n{stderr}\nstdout:\n{stdout}",
        output.status.code()
    );
    assert!(
        stdout.contains("RESULT err"),
        "{case} should fail the read, stdout:\n{stdout}"
    );
    let numbers = whole_numbers(&stdout);
    assert!(
        numbers.contains(&resolved_length),
        "{case} error should name the resolved length {resolved_length}, stdout:\n{stdout}"
    );
    assert!(
        numbers.contains(&BUFFERED_READ_CAP),
        "{case} error should name the {BUFFERED_READ_CAP}-byte cap, stdout:\n{stdout}"
    );
    assert!(
        !stderr.contains("huge-allocation"),
        "{case} allocated before returning the error. stderr:\n{stderr}"
    );
}

fn assert_read(server: &RunningServer, case: &str, location: &str, body: &str, len: usize) {
    let output = probe(server, case, location);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_opened(server, location);
    assert_did_not_allocate(case, &output);
    assert!(
        output.status.success(),
        "{case} should read the object, status {:?}. stderr:\n{stderr}\nstdout:\n{stdout}",
        output.status.code()
    );
    let expected = format!("RESULT ok len={len} body={body}");
    assert!(
        stdout.contains(&expected),
        "{case} should report {expected}, stdout:\n{stdout}"
    );
}

fn refuse_case(case: &str, location: &str, resolved_length: u64) {
    let server = start_server("consistent");
    assert_refused(&server, case, location, resolved_length);
}

#[tokio::test]
async fn sftp_get_refuses_a_reported_size_above_the_buffered_read_cap() {
    refuse_case("get", "full.csv", REPORTED_SIZE);
}

#[tokio::test]
async fn sftp_get_opts_offset_zero_refuses_a_resolved_length_above_the_cap() {
    refuse_case("offset-zero", "offset-zero.csv", REPORTED_SIZE);
}

#[tokio::test]
async fn sftp_get_opts_offset_remainder_refuses_when_the_remainder_exceeds_the_cap() {
    refuse_case(
        "offset-remainder",
        "offset-remainder.csv",
        REPORTED_SIZE - 32,
    );
}

#[tokio::test]
async fn sftp_get_opts_suffix_refuses_a_resolved_length_above_the_cap() {
    refuse_case("suffix", "suffix.csv", REPORTED_SIZE);
}

#[tokio::test]
async fn sftp_get_opts_bounded_range_refuses_a_resolved_length_above_the_cap() {
    refuse_case("bounded", "bounded.csv", REPORTED_SIZE);
}

#[tokio::test]
async fn sftp_get_opts_bounded_range_past_the_end_still_refuses_the_clamped_length() {
    refuse_case("bounded-overshoot", "bounded-overshoot.csv", REPORTED_SIZE);
}

#[tokio::test]
async fn sftp_get_range_refuses_a_resolved_length_above_the_cap() {
    refuse_case("get-range", "get-range.csv", REPORTED_SIZE);
}

#[tokio::test]
async fn sftp_reads_under_the_cap_return_the_object_bytes() {
    let over_cap = start_server("consistent");
    let body = std::str::from_utf8(PAYLOAD).expect("the payload is utf-8");
    assert_read(
        &over_cap,
        "bounded-small",
        "bounded-small.csv",
        body,
        PAYLOAD.len(),
    );
    assert_read(
        &over_cap,
        "get-range-small",
        "get-range-small.csv",
        body,
        PAYLOAD.len(),
    );
    assert_read(&over_cap, "offset-tail", "offset-tail.csv", "", 0);

    let output = probe(&over_cap, "head", "head.csv");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_opened(&over_cap, "head.csv");
    assert!(
        output.status.success(),
        "head should succeed, status {:?}. stderr:\n{stderr}\nstdout:\n{stdout}",
        output.status.code()
    );
    assert!(
        stdout.contains(&format!("RESULT ok head size={REPORTED_SIZE} len=0")),
        "head should report the server size without a body, stdout:\n{stdout}"
    );

    let honest = start_server("honest");
    assert_read(&honest, "get", "honest.csv", body, PAYLOAD.len());
}

#[tokio::test]
async fn sftp_read_does_not_allocate_a_size_taken_from_a_later_stat() {
    let server = start_server("grow");
    let output = probe(&server, "get", "grow.csv");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_opened(&server, "grow.csv");
    assert_did_not_allocate("grow", &output);
    assert!(
        output.status.success(),
        "the grow probe should finish, status {:?}. stderr:\n{stderr}\nstdout:\n{stdout}",
        output.status.code()
    );
    let body = std::str::from_utf8(PAYLOAD).expect("the payload is utf-8");
    let read_small = stdout.contains(&format!("RESULT ok len={} body={body}", PAYLOAD.len()));
    let numbers = whole_numbers(&stdout);
    let refused = stdout.contains("RESULT err")
        && numbers.contains(&REPORTED_SIZE)
        && numbers.contains(&BUFFERED_READ_CAP);
    assert!(
        read_small || refused,
        "the read should return the payload or refuse the later over-cap stat. stdout:\n{stdout}"
    );
}

const SERVER_SCRIPT: &str = r#"
import os
import socket
import sys
import threading
import traceback

import paramiko

MODE = os.environ["SFTP_SERVER_MODE"]
REPORTED = int(os.environ["SFTP_REPORTED_SIZE"])
PAYLOAD = bytes.fromhex(os.environ["SFTP_PAYLOAD_HEX"])
OPEN_LOG = os.environ["SFTP_OPEN_LOG"]
USER = os.environ["SFTP_USER"]
PASSWORD = os.environ["SFTP_PASSWORD"]
STATE = {"stats": 0}
LOCK = threading.Lock()


def note(line):
    with open(OPEN_LOG, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()


def next_reported_size():
    with LOCK:
        STATE["stats"] += 1
        which = STATE["stats"]
    if MODE == "honest":
        return len(PAYLOAD)
    if MODE == "grow" and which == 1:
        return len(PAYLOAD)
    return REPORTED


def attributes(size):
    attr = paramiko.SFTPAttributes()
    attr.st_size = size
    attr.st_mode = 0o100644
    attr.st_uid = 0
    attr.st_gid = 0
    attr.st_atime = 1700000000
    attr.st_mtime = 1700000000
    return attr


class Handle(paramiko.SFTPHandle):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def stat(self):
        size = next_reported_size()
        note(f"fstat {self.path} size={size}")
        return attributes(size)

    def read(self, offset, length):
        if offset >= len(PAYLOAD):
            return b""
        end = min(len(PAYLOAD), offset + int(length))
        return PAYLOAD[offset:end]


class Interface(paramiko.SFTPServerInterface):
    def open(self, path, flags, attr):
        note(f"open {path}")
        return Handle(path)

    def stat(self, path):
        size = next_reported_size()
        note(f"stat {path} size={size}")
        return attributes(size)

    def lstat(self, path):
        size = next_reported_size()
        note(f"lstat {path} size={size}")
        return attributes(size)


class Server(paramiko.ServerInterface):
    def get_allowed_auths(self, username):
        return "password"

    def check_auth_password(self, username, password):
        if username == USER and password == PASSWORD:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def check_channel_request(self, kind, chanid):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


def serve(client, host_key):
    transport = paramiko.Transport(client)
    try:
        transport.add_server_key(host_key)
        transport.set_subsystem_handler("sftp", paramiko.SFTPServer, Interface)
        transport.start_server(server=Server())
        channel = transport.accept(30)
        while transport.is_active():
            if channel is not None and channel.closed:
                break
            threading.Event().wait(0.1)
    except Exception:
        traceback.print_exc(file=sys.stderr)
    finally:
        transport.close()
        client.close()


def main():
    host_key = paramiko.RSAKey.generate(2048)
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    print(listener.getsockname()[1], flush=True)
    while True:
        client, _addr = listener.accept()
        threading.Thread(target=serve, args=(client, host_key), daemon=True).start()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
"#;
