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

//! testoperator's own `/health` and `/v1/ready` — the same paths, and the same
//! meanings, spiced serves, so a harness already probing a runtime can probe the
//! load generator with the same code.
//!
//! **Why the generator needs them.** From outside the process, a run that is
//! seeding an SF1000 source for forty minutes and a run wedged on a source
//! connection look identical: the process exists, it burns little CPU, and its
//! log says nothing. `pgrep testoperator` answers "a process is there", which is
//! not the question — the questions are whether it is still responsive, and
//! whether it has started applying load yet.
//!
//! - `/health` is liveness: 200 for as long as the process is up. It is served
//!   from the same Tokio runtime that drives the OLTP terminals and analytical
//!   clients, deliberately — its *latency* is then a reading on whether that
//!   runtime is still scheduling promptly, which a probe isolated on a private
//!   runtime could not tell you. (spiced makes the opposite choice for the
//!   opposite reason: there, a probe that stalls gets the pod killed.)
//! - `/v1/ready` is readiness, and readiness here means **the measured workload
//!   is running**: 200 only during [`Phase::Running`], 503 with the current
//!   phase otherwise. A watcher can then tell seeding from waiting-on-spiced
//!   from finished, rather than reading "not 200" as "broken".
//!
//! Both bodies are one line — a leading word, then `key=value` tokens — so a
//! shell probe can pull fields out of them without a JSON parser:
//!
//! ```text
//! $ curl -s localhost:8099/health
//! ok
//! $ curl -s localhost:8099/v1/ready
//! not_ready phase=preparing_source phase_s=412
//! ```
//!
//! The phase lives in an atomic rather than behind a lock: a readiness probe
//! must never queue behind whatever the run is doing, least of all when the run
//! is in trouble.

use std::net::SocketAddr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};

/// Where the endpoints bind unless `--health-listen` says otherwise. Loopback,
/// because the harness that reads them runs *on* the generator host (over ssh)
/// and these endpoints are unauthenticated. The port stays clear of spiced's
/// own (8090 HTTP, 9090 metrics, 50051 Flight), so a single-host run can serve
/// both.
pub(crate) const DEFAULT_LISTEN: &str = "127.0.0.1:8099";

/// What the run is doing, as `/v1/ready` reports it.
///
/// Ordered as a run passes through them. Only [`Phase::Running`] is ready: it is
/// the window in which load is actually being applied, which is the only window
/// a measurement taken from outside is valid in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Phase {
    /// Process up, command not yet started doing anything.
    Starting = 0,
    /// Seeding (or verifying) the source database — minutes to hours at scale.
    PreparingSource = 1,
    /// Source ready; spiced started or connected to, and not yet ready.
    WaitingForSpiced = 2,
    /// The measured workload is running: OLTP load applied, queries in flight.
    Running = 3,
    /// Workload stopped; gates, correctness checks and reporting still to run.
    Finalizing = 4,
    /// Everything is done and the process is about to exit.
    Finished = 5,
}

impl Phase {
    /// Stable token for the `phase=` field. Snake case and space-free so a
    /// whitespace-splitting shell probe can carry it through unquoted.
    #[must_use]
    pub(crate) fn slug(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::PreparingSource => "preparing_source",
            Phase::WaitingForSpiced => "waiting_for_spiced",
            Phase::Running => "running",
            Phase::Finalizing => "finalizing",
            Phase::Finished => "finished",
        }
    }

    /// Whether `/v1/ready` answers 200 in this phase.
    #[must_use]
    pub(crate) fn is_ready(self) -> bool {
        self == Phase::Running
    }

    fn from_u8(value: u8) -> Phase {
        match value {
            1 => Phase::PreparingSource,
            2 => Phase::WaitingForSpiced,
            3 => Phase::Running,
            4 => Phase::Finalizing,
            5 => Phase::Finished,
            // Anything else can only be a store this match has not been taught
            // about; reporting the earliest phase understates progress, which is
            // the safe direction for a readiness signal.
            _ => Phase::Starting,
        }
    }
}

/// Process start, the origin for `phase_s`. An `Instant` cannot be stored in an
/// atomic, so elapsed whole seconds are what the atomics carry.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);
static PHASE: AtomicU8 = AtomicU8::new(Phase::Starting as u8);
/// Seconds since [`START`] at which the current phase was entered.
static PHASE_AT_S: AtomicU64 = AtomicU64::new(0);

/// Records the phase the run has just entered. Two stores, so cheap enough to
/// call from anywhere, including a hot path.
pub(crate) fn set_phase(phase: Phase) {
    // The timestamp is written first, and the phase is *released* after it, so
    // a reader that acquires the new phase is guaranteed to see the timestamp
    // that goes with it. Relaxed on both would let the two stores be observed
    // out of order, and a probe that saw the new phase with the previous
    // phase's start time would report a phase that had already been running
    // for an hour — the exact reading this ordering exists to prevent.
    PHASE_AT_S.store(START.elapsed().as_secs(), Ordering::Relaxed);
    PHASE.store(phase as u8, Ordering::Release);
}

/// The current phase, and how many whole seconds the run has been in it.
pub(crate) fn phase() -> (Phase, u64) {
    // Acquire pairs with the release in [`set_phase`]: seeing a phase means
    // seeing the timestamp stored before it, so the two can never be read from
    // different transitions.
    let phase = Phase::from_u8(PHASE.load(Ordering::Acquire));
    let entered_at = PHASE_AT_S.load(Ordering::Relaxed);
    (phase, START.elapsed().as_secs().saturating_sub(entered_at))
}

/// The body `/v1/ready` returns, and the status that goes with it.
fn ready_response() -> (StatusCode, String) {
    let (phase, seconds) = phase();
    let (status, word) = if phase.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not_ready")
    };
    (
        status,
        format!("{word} phase={} phase_s={seconds}\n", phase.slug()),
    )
}

fn router() -> Router {
    Router::new()
        // "ok\n", byte for byte what spiced's /health returns.
        .route("/health", get(|| async { "ok\n" }))
        .route(
            "/v1/ready",
            get(|| async { ready_response().into_response() }),
        )
}

/// Binds the endpoints and serves them in the background for the rest of the
/// process's life. Returns the bound address, or `None` if they are switched
/// off or the bind failed.
///
/// A bind failure is a warning, never an error: a port already in use is not a
/// reason to lose a benchmark run that was otherwise about to succeed. The
/// harness reading the endpoints sees "nothing answering", which is the truth.
pub(crate) async fn serve(listen: &str) -> Option<SocketAddr> {
    let listen = listen.trim();
    if listen.is_empty() || listen.eq_ignore_ascii_case("off") {
        return None;
    }

    let listener = match tokio::net::TcpListener::bind(listen).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("liveness endpoints disabled: could not bind {listen}: {e}");
            return None;
        }
    };
    let addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("liveness endpoints disabled: bound {listen} but could not read it: {e}");
            return None;
        }
    };

    // `LazyLock` initialises on first touch, so the clock `phase_s` counts from
    // starts here rather than at whatever moment the first probe happens to
    // arrive.
    let _ = *START;
    println!("Liveness endpoints on http://{addr} (/health, /v1/ready)");
    // Nothing authenticates these, so anything but loopback publishes the run's
    // phase to whoever can reach the port. That is a legitimate choice (a
    // container's loopback is not reachable from the harness outside it), but
    // it should never happen without the run log saying so.
    if !addr.ip().is_loopback() {
        eprintln!(
            "WARNING: liveness endpoints are unauthenticated and bound off-loopback on {addr}"
        );
    }

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router()).await {
            eprintln!("liveness endpoint server stopped: {e}");
        }
    });
    Some(addr)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_LISTEN, Phase, phase, ready_response, serve, set_phase};
    use axum::http::StatusCode;
    use std::sync::{Mutex, MutexGuard, PoisonError};

    /// Held by every test that moves the phase. The phase is process-global and
    /// the harness runs tests on several threads, so without this two of them
    /// interleave their `set_phase` calls and each reads the other's phase.
    static PHASE_LOCK: Mutex<()> = Mutex::new(());

    /// Takes [`PHASE_LOCK`], recovering it if an earlier test panicked while
    /// holding it: poisoning would otherwise turn one failure into two.
    fn phase_guard() -> MutexGuard<'static, ()> {
        PHASE_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The phases a run passes through, in order.
    const EVERY_PHASE: [Phase; 6] = [
        Phase::Starting,
        Phase::PreparingSource,
        Phase::WaitingForSpiced,
        Phase::Running,
        Phase::Finalizing,
        Phase::Finished,
    ];

    #[test]
    fn only_the_running_phase_is_ready() {
        for p in EVERY_PHASE {
            assert_eq!(
                p.is_ready(),
                p == Phase::Running,
                "{} must not be ready: load is not being applied in it",
                p.slug()
            );
        }
    }

    #[test]
    fn every_phase_round_trips_through_its_atomic() {
        for p in EVERY_PHASE {
            assert_eq!(
                Phase::from_u8(p as u8),
                p,
                "{} did not round-trip",
                p.slug()
            );
        }
        assert_eq!(
            Phase::from_u8(200),
            Phase::Starting,
            "an unknown value must understate progress, not overstate it"
        );
    }

    #[test]
    fn slugs_are_shell_safe_and_distinct() {
        let mut seen: Vec<&str> = Vec::new();
        for p in EVERY_PHASE {
            let slug = p.slug();
            assert!(
                !slug.contains(char::is_whitespace) && !slug.is_empty(),
                "{slug:?} would break a whitespace-split probe line"
            );
            assert!(!seen.contains(&slug), "duplicate slug {slug}");
            seen.push(slug);
        }
    }

    /// The status/body pairing the harness keys off.
    #[test]
    fn the_ready_body_names_the_phase_and_its_status_follows_it() {
        let _guard = phase_guard();
        set_phase(Phase::PreparingSource);
        let (status, body) = ready_response();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            body.starts_with("not_ready phase=preparing_source phase_s="),
            "{body:?}"
        );
        assert!(
            body.ends_with('\n'),
            "one line, newline-terminated: {body:?}"
        );

        set_phase(Phase::Running);
        let (status, body) = ready_response();
        assert_eq!(status, StatusCode::OK);
        assert!(body.starts_with("ready phase=running phase_s="), "{body:?}");

        let (current, seconds) = phase();
        assert_eq!(current, Phase::Running);
        assert!(seconds < 5, "the phase was just entered, got {seconds}s");

        // Leave the global back where the other tests expect it.
        set_phase(Phase::Starting);
    }

    #[test]
    fn switching_the_endpoints_off_binds_nothing() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        assert!(runtime.block_on(serve("off")).is_none());
        assert!(runtime.block_on(serve("")).is_none());
    }

    #[test]
    fn an_unbindable_address_warns_instead_of_failing_the_run() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        // Port 1 on loopback needs privileges this test does not have; a
        // hostname that resolves to nothing fails earlier still. Either way the
        // call must return rather than propagate.
        assert!(runtime.block_on(serve("192.0.2.1:9")).is_none());
    }

    /// The endpoints as a probe actually sees them: over HTTP, with the phase
    /// moving underneath.
    ///
    /// A `block_on` inside a plain test rather than `#[tokio::test]`, so the
    /// phase guard is held across the runtime call in synchronous code — an
    /// async test would hold a `MutexGuard` across its awaits.
    #[test]
    fn the_endpoints_answer_over_http_and_track_the_phase() {
        let _guard = phase_guard();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            set_phase(Phase::PreparingSource);
            let addr = serve("127.0.0.1:0").await.expect("bound an ephemeral port");
            let base = format!("http://{addr}");
            let client = reqwest::Client::new();

            let health = client
                .get(format!("{base}/health"))
                .send()
                .await
                .expect("health answered");
            assert_eq!(health.status(), 200);
            assert_eq!(health.text().await.unwrap_or_default(), "ok\n");

            let not_ready = client
                .get(format!("{base}/v1/ready"))
                .send()
                .await
                .expect("ready answered");
            assert_eq!(not_ready.status(), 503, "seeding is not ready");
            assert!(
                not_ready
                    .text()
                    .await
                    .unwrap_or_default()
                    .contains("phase=preparing_source")
            );

            set_phase(Phase::Running);
            let ready = client
                .get(format!("{base}/v1/ready"))
                .send()
                .await
                .expect("ready answered");
            assert_eq!(ready.status(), 200, "load is being applied");

            // A path neither endpoint owns must not be answered as if it were
            // one.
            let unknown = client
                .get(format!("{base}/v1/nope"))
                .send()
                .await
                .expect("answered");
            assert_eq!(unknown.status(), 404);

            set_phase(Phase::Starting);
        });
    }

    #[test]
    fn the_default_listen_is_loopback_and_clear_of_spiceds_ports() {
        let addr: std::net::SocketAddr = DEFAULT_LISTEN.parse().expect("a parseable address");
        assert!(addr.ip().is_loopback(), "unauthenticated: loopback only");
        assert!(
            ![8090, 9090, 50051, 50052].contains(&addr.port()),
            "{DEFAULT_LISTEN} collides with a spiced port on a single-host run"
        );
    }
}
