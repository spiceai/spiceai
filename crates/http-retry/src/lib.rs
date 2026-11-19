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

//! A retry policy for `reqwest` clients that sends a request again only when the server never
//! received it.
//!
//! A request can fail before any of it is sent. The network or a load balancer can close a new
//! connection while TLS is still being negotiated, and the client then reads an early end of
//! stream (`tls handshake eof`) or a reset. A request is written only after the handshake
//! completes, so the server never saw it, and sending it again on a new connection is safe
//! whatever its method. [`policy`] sends such a request again.
//!
//! Setting a policy replaces the one `reqwest` uses by default, so [`policy`] also keeps the
//! retries `reqwest` makes by default. Those are for an HTTP/2 stream the server refused
//! without processing it: a `REFUSED_STREAM` reset, or a graceful `GOAWAY` that came before
//! the stream.
//!
//! It does not send a request again in these cases:
//! - The request was sent, because the server may have acted on it.
//! - The connection timed out, because each retry would wait as long again.
//! - The connection was refused, the host did not resolve, or the certificate was not trusted,
//!   because a retry made at once would fail the same way.
//!
//! A retry is sent at once, on a new connection. Each request is sent at most
//! [`MAX_RETRIES_PER_REQUEST`] more times. Each client allows retries up to 20% of its
//! requests, with a floor of 10 retries a second, so an outage cannot multiply its load.

use std::error::Error as StdError;
use std::io;

/// The most times a request is sent again after its first attempt.
pub const MAX_RETRIES_PER_REQUEST: u32 = 2;

/// A [`reqwest::ClientBuilder`] whose client retries with [`policy`].
pub fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().retry(policy())
}

/// The retry policy described in the [crate documentation](crate).
#[must_use]
pub fn policy() -> reqwest::retry::Builder {
    reqwest::retry::for_host(EveryHost)
        .max_retries_per_request(MAX_RETRIES_PER_REQUEST)
        .classify_fn(|attempt| {
            let Some(reason) = attempt.error().and_then(retry_reason) else {
                return attempt.success();
            };
            tracing::debug!(
                "Sending a request to '{}' again: {}",
                attempt.uri().host().unwrap_or_default(),
                reason.describe()
            );
            attempt.retryable()
        })
}

/// A request scope that holds every host. `reqwest` budgets retries per client, and each
/// client here serves one provider or service.
struct EveryHost;

impl PartialEq<&str> for EveryHost {
    fn eq(&self, _host: &&str) -> bool {
        true
    }
}

/// Why a failed attempt can be sent again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryReason {
    /// The connection closed before the request was written to it.
    ConnectionClosedBeforeRequest,
    /// The server refused the HTTP/2 stream without processing it.
    StreamRefused,
}

impl RetryReason {
    fn describe(self) -> &'static str {
        match self {
            Self::ConnectionClosedBeforeRequest => {
                "the connection closed before the request was sent"
            }
            Self::StreamRefused => "the server refused the request without processing it",
        }
    }
}

fn retry_reason(error: &(dyn StdError + 'static)) -> Option<RetryReason> {
    // A connect error is one hyper reports while it establishes the connection, before it
    // writes any part of the request. A closed or reset connection at any later point may
    // come after the server has read the request.
    let connecting = error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_connect);
    if connecting && any_cause(error, &is_closed_connection) {
        return Some(RetryReason::ConnectionClosedBeforeRequest);
    }
    if any_cause(error, &is_refused_stream) {
        return Some(RetryReason::StreamRefused);
    }
    None
}

/// Whether `found` holds for `error` or any error that caused it.
fn any_cause(
    error: &(dyn StdError + 'static),
    found: &impl Fn(&(dyn StdError + 'static)) -> bool,
) -> bool {
    if found(error) {
        return true;
    }
    // `io::Error::source` returns the source of the error an `io::Error` wraps, and skips that
    // error itself. A TLS handshake that reads end of stream is wrapped that way.
    if let Some(wrapped) = error
        .downcast_ref::<io::Error>()
        .and_then(io::Error::get_ref)
        && any_cause(wrapped, found)
    {
        return true;
    }
    error
        .source()
        .is_some_and(|source| any_cause(source, found))
}

/// The peer closed or reset the connection. This is not a refused connection, a timeout, or an
/// untrusted certificate (`InvalidData`).
fn is_closed_connection(error: &(dyn StdError + 'static)) -> bool {
    error.downcast_ref::<io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
        )
    })
}

/// The HTTP/2 refusals `reqwest` retries by default (RFC 9113, section 8.7).
fn is_refused_stream(error: &(dyn StdError + 'static)) -> bool {
    error.downcast_ref::<h2::Error>().is_some_and(|error| {
        error.is_remote()
            && ((error.is_go_away() && error.reason() == Some(h2::Reason::NO_ERROR))
                || (error.is_reset() && error.reason() == Some(h2::Reason::REFUSED_STREAM)))
    })
}
