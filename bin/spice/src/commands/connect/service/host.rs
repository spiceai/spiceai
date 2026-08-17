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

//! What a back end does outside its own process, and the one health probe both
//! back ends gate an install on.
//!
//! Each supervisor keeps its own host trait, because the tools it runs and the
//! way it is asked about a job are its own. What is shared is everything that
//! is *not* supervisor-specific: how a completed command is described when it
//! failed, and what "this instance is serving" means. A second copy of the
//! health probe would be a second answer to that question, and the two would
//! drift.

use std::io::{Read as _, Write as _};
use std::net::{TcpStream, ToSocketAddrs as _};

use std::time::Duration;

/// How long the health probe waits for the instance to accept a connection and
/// answer. Short: it is retried for the whole of an install's health gate.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Bytes of a health response read before giving up on finding a status line.
const PROBE_READ_LIMIT: usize = 512;

/// One completed supervisor command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommandOutput {
    /// Whether the command reported success.
    pub(super) success: bool,
    /// The exit code, when the command exited on its own.
    pub(super) code: Option<i32>,
    pub(super) stdout: String,
    pub(super) stderr: String,
}

impl CommandOutput {
    /// How a failure is named in an error message: the supervisor's own words
    /// when it said any, because they diagnose the problem far better than an
    /// exit code.
    ///
    /// `launchctl` reports some failures on stdout, so both streams are
    /// considered.
    pub(super) fn describe_failure(&self) -> String {
        let said = folded(self.stderr.trim());
        if !said.is_empty() {
            return said;
        }
        let said = folded(self.stdout.trim());
        if !said.is_empty() {
            return said;
        }
        match self.code {
            Some(code) => format!("exit status {code}"),
            None => "terminated by a signal".to_string(),
        }
    }
}

/// A health endpoint can be absent while the service starts or deliberately
/// configured elsewhere; that is different from a runtime that answered with
/// an explicit unhealthy status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HealthProbe {
    Healthy,
    Unhealthy,
    Unreachable,
}

/// Whether a health URL is one a back end can probe.
///
/// Only plain HTTP is: the recorded URL is the loopback endpoint the runtime
/// serves, and a gate that cannot reach an unusual one must not fail an install
/// that is fine — it gates on what the supervisor reports instead.
pub(super) fn is_probeable(url: &str) -> bool {
    url.strip_prefix("http://")
        .is_some_and(|rest| !rest.is_empty() && !rest.starts_with('/'))
}

/// `GET` the health URL and report whether it answered `2xx`.
///
/// Blocking and dependency-free on purpose: this runs inside the installer's
/// own thread, between two filesystem operations, and answers one question
/// about one loopback address.
pub(super) fn probe_http_health(url: &str) -> HealthProbe {
    let Some(rest) = url.strip_prefix("http://") else {
        return HealthProbe::Unreachable;
    };
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let authority = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };

    let Ok(mut addrs) = authority.to_socket_addrs() else {
        return HealthProbe::Unreachable;
    };
    let Some(addr) = addrs.next() else {
        return HealthProbe::Unreachable;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, PROBE_TIMEOUT) else {
        return HealthProbe::Unreachable;
    };
    if stream.set_read_timeout(Some(PROBE_TIMEOUT)).is_err()
        || stream.set_write_timeout(Some(PROBE_TIMEOUT)).is_err()
    {
        return HealthProbe::Unreachable;
    }

    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: spice\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return HealthProbe::Unreachable;
    }

    let mut response = Vec::with_capacity(PROBE_READ_LIMIT);
    let mut chunk = [0_u8; 128];
    while response.len() < PROBE_READ_LIMIT {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&chunk[..read]),
            Err(_) => return HealthProbe::Unreachable,
        }
        if response.contains(&b'\n') {
            break;
        }
    }
    status_line_probe(&String::from_utf8_lossy(&response))
}

/// Classify a complete HTTP status line without treating transport failures as
/// an explicit unhealthy response.
///
/// The terminator is required: a response that ended before one arrived is a
/// connection that was cut mid-answer, and reading `HTTP/1.1 20` as a success
/// would report an instance healthy on the strength of a truncated read.
pub(super) fn status_line_probe(response: &str) -> HealthProbe {
    let Some((line, _)) = response.split_once('\n') else {
        return HealthProbe::Unreachable;
    };
    let mut fields = line.split_whitespace();
    let Some(version) = fields.next() else {
        return HealthProbe::Unreachable;
    };
    if !version.starts_with("HTTP/") {
        return HealthProbe::Unreachable;
    }
    let Some(code) = fields.next().and_then(|code| code.parse::<u16>().ok()) else {
        return HealthProbe::Unreachable;
    };
    if (200..300).contains(&code) {
        HealthProbe::Healthy
    } else {
        HealthProbe::Unhealthy
    }
}

/// One line, whatever the source said.
///
/// Supervisor output arrives with newlines in it, and a log line that carries
/// them is two records only one of which can be searched for.
pub(super) fn folded(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_plain_http_authority_is_probeable() {
        assert!(is_probeable("http://127.0.0.1:8090/health"));
        assert!(is_probeable("http://localhost:8090"));
        assert!(!is_probeable("https://127.0.0.1:8090/health"));
        assert!(!is_probeable("http:///health"));
        assert!(!is_probeable("http://"));
        assert!(!is_probeable("127.0.0.1:8090"));
    }

    #[test]
    fn a_truncated_status_line_is_unreachable_rather_than_healthy() {
        // Without the terminator this is a connection cut mid-answer, and
        // reading it as a success would report an instance healthy on a partial
        // read.
        assert_eq!(status_line_probe("HTTP/1.1 20"), HealthProbe::Unreachable);
        assert_eq!(
            status_line_probe("HTTP/1.1 200 OK\r\nDate: now\r\n"),
            HealthProbe::Healthy
        );
        assert_eq!(
            status_line_probe("HTTP/1.0 204 No Content\r\n"),
            HealthProbe::Healthy
        );
        assert_eq!(
            status_line_probe("HTTP/1.1 503 Service Unavailable\r\n"),
            HealthProbe::Unhealthy
        );
        assert_eq!(status_line_probe("garbage\n"), HealthProbe::Unreachable);
        assert_eq!(status_line_probe(""), HealthProbe::Unreachable);
    }

    #[test]
    fn a_failure_is_described_by_what_the_supervisor_said() {
        let stderr = CommandOutput {
            success: false,
            code: Some(1),
            stdout: String::new(),
            stderr: "Operation not permitted\nwhile bootstrapping".to_string(),
        };
        assert_eq!(
            stderr.describe_failure(),
            "Operation not permitted while bootstrapping"
        );

        // `launchctl` reports some refusals on stdout, so a silent stderr is
        // not the end of the search.
        let stdout = CommandOutput {
            success: false,
            code: Some(113),
            stdout: "Could not find service".to_string(),
            stderr: String::new(),
        };
        assert_eq!(stdout.describe_failure(), "Could not find service");

        let silent = CommandOutput {
            success: false,
            code: Some(78),
            stdout: String::new(),
            stderr: String::new(),
        };
        assert_eq!(silent.describe_failure(), "exit status 78");

        let signalled = CommandOutput {
            success: false,
            code: None,
            stdout: String::new(),
            stderr: String::new(),
        };
        assert_eq!(signalled.describe_failure(), "terminated by a signal");
    }

    #[test]
    fn folding_puts_a_multi_line_message_on_one_line() {
        assert_eq!(folded("first\nsecond   third\n"), "first second third");
        assert_eq!(folded(""), "");
    }
}
