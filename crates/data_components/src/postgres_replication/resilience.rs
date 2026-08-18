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
//!   * `is_slot_unusable` picks out the failures that are neither: a slot the
//!     server will never stream from again, recovered by replacing the slot
//!     rather than by retrying or by ending the dataset.
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
/// protocol, and decoding errors are not. A slot that can no longer supply changes
/// is neither — see [`is_slot_unusable`], which this defers to first.
#[must_use]
pub fn is_transient_pgwire(err: &pgwire_replication::PgWireError) -> bool {
    // A slot that can no longer supply changes refuses every attempt identically,
    // so it is never worth retrying — and it has to be excluded here rather than
    // just ordered around at each call site, because its message trips the generic
    // markers below: `PostgreSQL` 18's idle-invalidation detail names
    // `idle_replication_slot_timeout`, and "timeout" is a transient marker. Left in,
    // that reads as a network blip and reconnects against a slot that will never
    // accept another `START_REPLICATION`, forever.
    if is_slot_unusable(err) {
        return false;
    }
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

/// `PostgreSQL`'s message when `START_REPLICATION` names a slot the server has
/// invalidated, whatever invalidated it (exhausted `max_slot_wal_keep_size`, or
/// the `PostgreSQL` 18 idle timeout).
///
/// Matched on the message rather than its SQLSTATE. 55000
/// (`object_not_in_prerequisite_state`) is overloaded — slot *creation* returns
/// the same code when `wal_level` is not `logical`, which is a configuration
/// error and must not be answered by replacing a slot. The message is the part
/// that identifies the situation, and it has been stable since the invalidation
/// machinery was introduced.
const SLOT_NO_LONGER_STREAMABLE: &str = "can no longer get changes from replication slot";
/// `PostgreSQL`'s message when the slot is simply gone — an operator ran
/// `pg_drop_replication_slot`, or a failover left the replica without it.
///
/// Both halves are required. `does not exist` on its own also describes a missing
/// *publication*, which needs its own fix and must never be answered by dropping
/// and recreating a slot.
const SLOT_DOES_NOT_EXIST: (&str, &str) = ("replication slot", "does not exist");

/// Whether the named slot can no longer supply changes and has to be replaced.
///
/// A third classification, distinct from both others: reconnecting cannot help
/// (every attempt hits the same refusal), but this is not the dataset's end
/// either — the slot can be replaced and the accelerations on it rebuilt from the
/// source. See `shared::recover_unusable_slot`.
///
/// Covers the two ways a slot stops being usable while Spice is running, because
/// they have one remedy between them: the server invalidated it (its WAL is gone),
/// or it is no longer there at all. What distinguishes them is only what to tell
/// the operator afterwards, which the replacement path logs from the catalog.
#[must_use]
pub fn is_slot_unusable(err: &pgwire_replication::PgWireError) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    if msg.contains(SLOT_NO_LONGER_STREAMABLE) {
        return true;
    }
    let (subject, state) = SLOT_DOES_NOT_EXIST;
    msg.contains(subject) && msg.contains(state)
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
        // SQLSTATE 55006 (object_in_use): "replication slot ... is active for
        // PID ...". A reconnect can race the previous walsender's teardown —
        // the server releases the slot within moments, so retry with backoff
        // instead of failing the stream. (Two *distinct* consumers fighting
        // over one slot keep erroring through the retry budget and still
        // surface, just slower.)
        "sqlstate 55006",
        "is active for pid",
        // SQLSTATE 53300 (too_many_connections) for walsenders: "number of
        // requested standby connections exceeds max_wal_senders". During a
        // rolling deploy the outgoing instance still holds its walsender, so
        // a capped server can momentarily have no free slots. Retry with
        // backoff; if the server is genuinely over-subscribed the stream
        // keeps retrying visibly (reconnect logs + metrics) instead of
        // fatally ending every dataset on the shared slot.
        "sqlstate 53300",
        "max_wal_senders",
        "too many connections",
        // SQLSTATE 57P01 (admin_shutdown): "terminating connection due to
        // administrator command". An orderly server restart raises it, and so
        // does `PostgreSQL` killing the walsender that holds a slot it is
        // invalidating ("terminating process N to release replication slot").
        // Reconnecting is right in both cases: a restarted server resumes the
        // stream, and a released slot surfaces its invalidation on the next
        // `START_REPLICATION`, where `is_slot_unusable` can act on it. The
        // tokio-postgres classifier already treats 57P01 as transient by code;
        // this is the same judgement for the replication client, which only
        // exposes the rendered message.
        //
        // Reconnecting rather than ending the stream does make a pre-existing race
        // reachable: a dataset joining while the slot is invalidated recreates it
        // through `slot::ensure_slot`, and members already streaming are not told
        // that their recorded positions no longer reach the replacement (#13229).
        // The previous fatal classification masked that by killing the stream first.
        "sqlstate 57p01",
        "terminating connection due to administrator command",
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
    #[test]
    fn slot_contention_errors_are_transient() {
        // Both raced-walsender shapes observed during rolling deploys: the
        // outgoing instance still holds the slot / a walsender seat for a
        // moment. These MUST retry — on a shared slot a fatal classification
        // terminates every member dataset.
        for msg in [
            "server error: replication slot \"s\" is active for PID 123 (SQLSTATE 55006)",
            "server error: number of requested standby connections exceeds max_wal_senders (currently 5) (SQLSTATE 53300)",
        ] {
            assert!(
                is_transient_pgwire(&pgwire_replication::PgWireError::Server(msg.to_string())),
                "must be transient: {msg}"
            );
        }
        // A structured server error (permission denied) stays fatal.
        assert!(!is_transient_pgwire(
            &pgwire_replication::PgWireError::Server(
                "server error: permission denied for database app (SQLSTATE 42501)".to_string()
            )
        ));
    }

    use super::*;

    #[test]
    fn a_dropped_slot_is_replaceable_but_a_missing_publication_is_not() {
        // An operator running `pg_drop_replication_slot` is the same situation as
        // an invalidation and has the same remedy.
        assert!(is_slot_unusable(&pgwire_replication::PgWireError::Server(
            "replication slot \"spice_x\" does not exist (SQLSTATE 42704)".to_string()
        )));
        // A missing publication shares the "does not exist" wording and must not
        // be answered by dropping and recreating the operator's slot.
        assert!(!is_slot_unusable(&pgwire_replication::PgWireError::Server(
            "publication \"spice_pub\" does not exist (SQLSTATE 42704)".to_string()
        )));
        // Neither is worth a retry: both refuse every attempt identically.
        assert!(!is_transient_pgwire(
            &pgwire_replication::PgWireError::Server(
                "replication slot \"spice_x\" does not exist (SQLSTATE 42704)".to_string()
            )
        ));
    }

    #[test]
    fn an_invalidated_slot_is_recognised_but_is_not_transient() {
        // Every invalidation cause reaches the client as the same message, which
        // is what this keys on; the detail line is what differs.
        for detail in [
            "This slot has been invalidated because it exceeded the maximum reserved size.",
            "This slot has been invalidated because it was inactive for longer than the amount of time specified by \"idle_replication_slot_timeout\".",
        ] {
            let err = pgwire_replication::PgWireError::Server(format!(
                "can no longer get changes from replication slot \"spice_x\" (SQLSTATE 55000) — {detail}"
            ));
            assert!(is_slot_unusable(&err), "must be recognised: {detail}");
            assert!(
                !is_transient_pgwire(&err),
                "an invalidated slot must never be retried against: {detail}"
            );
        }
    }

    /// The invalidation detail `PostgreSQL` 18 attaches names
    /// `idle_replication_slot_timeout`, and "timeout" is one of the transient
    /// markers. The classifier has to exclude an unusable slot explicitly for that
    /// reason; without it this message reads as a network blip and the stream
    /// reconnects against a slot that will never accept it again.
    #[test]
    fn an_idle_invalidation_is_not_mistaken_for_a_timeout() {
        let err = pgwire_replication::PgWireError::Server(
            "can no longer get changes from replication slot \"spice_x\" (SQLSTATE 55000) — This slot has been invalidated because it was inactive for longer than the amount of time specified by \"idle_replication_slot_timeout\"."
                .to_string(),
        );
        assert!(
            err.to_string().to_ascii_lowercase().contains("timeout"),
            "the premise of this test: the message contains a transient marker"
        );
        assert!(!is_transient_pgwire(&err));
    }

    #[test]
    fn wal_level_misconfiguration_shares_the_sqlstate_but_is_not_an_invalidation() {
        // Slot creation returns 55000 when `wal_level` is not `logical`. Keying
        // recovery on the code alone would answer a configuration error by
        // dropping and recreating the operator's slot.
        let err = pgwire_replication::PgWireError::Server(
            "logical decoding requires wal_level >= logical (SQLSTATE 55000)".to_string(),
        );
        assert!(!is_slot_unusable(&err));
    }

    #[test]
    fn a_walsender_killed_to_release_a_slot_reconnects() {
        // PostgreSQL SIGTERMs the walsender holding a slot it is invalidating, so
        // the invalidation is first seen as an admin shutdown on the *recv* path.
        // Classifying that as fatal would end the stream before the reconnect
        // could observe the invalidation and recover from it.
        for msg in [
            "server error: terminating connection due to administrator command (SQLSTATE 57P01)",
            "server error: terminating connection due to administrator command",
        ] {
            assert!(
                is_transient_pgwire(&pgwire_replication::PgWireError::Server(msg.to_string())),
                "must reconnect: {msg}"
            );
        }
    }

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
