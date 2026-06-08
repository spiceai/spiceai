/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Resilience primitives for the replication path.
//!
//! Everything in this module is about one job: turn the inevitable
//! network / database blips into a delay rather than a user-visible error.
//! The replication stream reconnects internally; the runtime only sees
//! `StreamError` once a failure has been *classified as fatal* (auth, slot
//! dropped, schema mismatch, etc.).
//!
//! Design:
//!   * `is_transient_pgwire` / `is_transient_pg` classify client errors by
//!     kind (IO, EOF, TLS reset, connection-closed) — these are the failures
//!     that a reconnect is likely to fix.
//!   * `Backoff` is an exponential-backoff helper with ±20% jitter, capped at
//!     a configurable max delay. Reset on every successful use.
//!   * `retry_async` runs an async closure with `Backoff`, classifying errors
//!     via a caller-supplied predicate.

use std::time::Duration;

/// Defaults picked to tolerate short network blips (≤ a minute of Postgres
/// unavailability) without being user-visibly disruptive, while giving up on
/// anything longer so an operator can intervene.
pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Maximum time we'll keep retrying a single setup/bootstrap attempt before
/// giving up. The WAL stream uses the same attempts budget per *reconnect*
/// but reconnects indefinitely once it has been healthy once — the stream is
/// meant to run forever.
pub const DEFAULT_SETUP_MAX_ELAPSED: Duration = Duration::from_mins(2);

/// Exponential backoff with full jitter (±20%).
#[derive(Debug)]
pub struct Backoff {
    current: Duration,
    max: Duration,
    initial: Duration,
}

impl Backoff {
    #[must_use]
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            current: initial,
            max,
            initial,
        }
    }

    #[must_use]
    pub fn default_for_stream() -> Self {
        Self::new(DEFAULT_INITIAL_BACKOFF, DEFAULT_MAX_BACKOFF)
    }

    /// Sleep for the current delay, then double it (capped at `max`). The
    /// actual delay is randomised ±20% so N replicas reconnecting after a
    /// network split don't synchronise into a thundering herd.
    pub async fn wait(&mut self) {
        let jittered = jitter(self.current);
        tokio::time::sleep(jittered).await;
        let next = self.current.saturating_mul(2);
        self.current = if next > self.max { self.max } else { next };
    }

    pub fn reset(&mut self) {
        self.current = self.initial;
    }

    #[must_use]
    pub fn current(&self) -> Duration {
        self.current
    }
}

fn jitter(d: Duration) -> Duration {
    // Cheap PRNG without pulling in a dep here: use the current nanos.
    let nanos = u64::from(d.subsec_nanos())
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |e| u64::from(e.subsec_nanos()));
    // Map to [-20%, +20%] of `d`. The casts below are intentional — we're
    // computing a small bounded signed offset to add to a positive base and
    // clamping back to a u64 millis. Out-of-range inputs would already be
    // unsafe at this layer (an hours-long jitter base doesn't make sense).
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        reason = "bounded millis arithmetic: span fits in i64, sign-bit cast is scoped to a jitter delta"
    )]
    {
        let span = (d.as_millis() as i64) / 5;
        let delta = if span == 0 {
            0
        } else {
            (nanos as i64) % (2 * span) - span
        };
        let base = d.as_millis() as i64;
        Duration::from_millis((base + delta).max(1) as u64)
    }
}

/// Classify a `pgwire_replication::PgWireError` as transient (worth
/// reconnecting) or fatal (propagate to the user).
///
/// We look at the *error path* rather than specific variants, since
/// pgwire-replication may add variants over time. The heuristic: anything
/// that looks like an IO / connection / EOF error is transient; authentication,
/// protocol, slot-not-found, or decoding errors are fatal.
#[must_use]
pub fn is_transient_pgwire(err: &pgwire_replication::PgWireError) -> bool {
    is_transient_by_display(&err.to_string())
}

/// Same classifier for tokio-postgres (used by setup + bootstrap).
#[must_use]
pub fn is_transient_pg(err: &tokio_postgres::Error) -> bool {
    if err.is_closed() {
        return true;
    }
    // Postgres SQLSTATE classes: 08xxx = connection exception (transient),
    // 57P0x = admin shutdown / cannot-connect-now (transient).
    if let Some(db_err) = err.as_db_error() {
        let code = db_err.code().code();
        if code.starts_with("08") || code == "57P01" || code == "57P02" || code == "57P03" {
            return true;
        }
        // Anything else from the server is a structured error (permission,
        // syntax, constraint). Don't retry those.
        return false;
    }
    is_transient_by_display(&err.to_string())
}

/// Shared string-heuristic fallback used by both classifiers.
fn is_transient_by_display(msg: &str) -> bool {
    // All markers are lowercase; we compare against `lower.contains(m)`.
    const TRANSIENT_MARKERS: &[&str] = &[
        "connection closed",
        "connection reset",
        "connection refused",
        "broken pipe",
        "unexpected eof",
        "unexpected end of file",
        "early eof",
        "temporarily unavailable",
        "timed out",
        "timeout",
        "network is unreachable",
        "host unreachable",
        "operation interrupted",
    ];
    let lower = msg.to_ascii_lowercase();
    TRANSIENT_MARKERS.iter().any(|m| lower.contains(m))
}

/// Run `op` with exponential backoff, retrying any error for which `classify`
/// returns true, up to `max_elapsed`. Returns the first success or the most
/// recent error once the budget is exhausted.
pub async fn retry_async<T, E, F, Fut, C>(
    label: &str,
    max_elapsed: Duration,
    mut classify: C,
    mut op: F,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
    C: FnMut(&E) -> bool,
    E: std::fmt::Display,
{
    let mut backoff = Backoff::new(DEFAULT_INITIAL_BACKOFF, DEFAULT_MAX_BACKOFF);
    let deadline = std::time::Instant::now() + max_elapsed;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !classify(&e) {
                    return Err(e);
                }
                if std::time::Instant::now() >= deadline {
                    tracing::error!(
                        op = %label,
                        error = %e,
                        "transient error budget exhausted; giving up"
                    );
                    return Err(e);
                }
                tracing::warn!(
                    op = %label,
                    error = %e,
                    retry_in_ms = %backoff.current().as_millis(),
                    "transient error; retrying"
                );
                backoff.wait().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = Backoff::new(Duration::from_millis(10), Duration::from_millis(80));
        assert_eq!(b.current().as_millis(), 10);
        // manually advance
        let next = b.current.saturating_mul(2);
        b.current = if next > b.max { b.max } else { next };
        assert_eq!(b.current().as_millis(), 20);
        let next = b.current.saturating_mul(2);
        b.current = if next > b.max { b.max } else { next };
        assert_eq!(b.current().as_millis(), 40);
        let next = b.current.saturating_mul(2);
        b.current = if next > b.max { b.max } else { next };
        assert_eq!(b.current().as_millis(), 80);
        let next = b.current.saturating_mul(2);
        b.current = if next > b.max { b.max } else { next };
        assert_eq!(b.current().as_millis(), 80, "should cap at max");
        b.reset();
        assert_eq!(b.current().as_millis(), 10);
    }

    #[test]
    fn transient_classifier_catches_common_failures() {
        assert!(is_transient_by_display("connection closed"));
        assert!(is_transient_by_display("Connection reset by peer"));
        assert!(is_transient_by_display("broken pipe"));
        assert!(is_transient_by_display("unexpected EOF"));
        assert!(is_transient_by_display("operation timed out"));
        assert!(!is_transient_by_display("syntax error at or near"));
        assert!(!is_transient_by_display(
            "permission denied for table users"
        ));
        assert!(!is_transient_by_display(
            "replication slot \"foo\" does not exist"
        ));
    }

    #[tokio::test]
    async fn retry_async_succeeds_after_transient_failures() {
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result: Result<&'static str, &'static str> = retry_async(
            "unit_test",
            Duration::from_secs(5),
            |e| *e == "flake",
            || async {
                let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n < 2 { Err("flake") } else { Ok("success") }
            },
        )
        .await;
        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_async_gives_up_on_fatal_immediately() {
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result: Result<(), &'static str> = retry_async(
            "unit_test",
            Duration::from_secs(5),
            |_e| false,
            || async {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err("fatal")
            },
        )
        .await;
        assert_eq!(result, Err("fatal"));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
