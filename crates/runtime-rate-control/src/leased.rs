/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Leased token-bucket rate limiter backed by an object store.
//!
//! Each replica leases a slice of a cluster-wide budget for a fixed-length
//! window (window length = `refresh_interval`). Leases are negotiated through
//! `object_store` conditional writes. Within a window a replica may consume up
//! to its lease, paced locally with a GCRA/TAT-style scheduler so a long global
//! lease window does not burst all tokens into the upstream backend at once.
//!
//! Schema is `PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION = 3`. Older state is
//! treated as empty (with a warning); the previous PR was never shipped so no
//! migration is required.
//!
//! ## Adaptive demand-weighted leasing
//!
//! Each lease record carries the replica's observed *demand* (count of
//! `acquire()` calls during the window, regardless of grant). When refreshing
//! its lease for window N, a replica reads recent completed windows from the
//! shared state, computes an exponentially weighted moving average (EWMA) of
//! saturation-classified demand, and claims a proportional slice of the
//! cluster burst:
//!
//! ```text
//! my_share = burst_per_window * my_ewma_demand / sum_ewma_demand
//! ```
//!
//! This converges within a few windows: if A wants 10 RPS and B wants 5 RPS
//! against a 5 RPS cluster cap, A claims `5 * 10/15 ≈ 3` and B claims
//! `5 * 5/15 ≈ 2`. Idle replicas decay toward `min_lease`, freeing budget for
//! hot replicas. EWMA smooths over occasional missing/zero demand samples so a
//! hot replica does not fall back to fair-share for a full window.
//!
//! Bootstrap: with no prior demand data, a replica with local in-progress
//! demand claims up to `max_lease_per_replica`; an idle replica claims only
//! `min_lease`.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use object_store::ObjectStore;
use object_store_occ::{InsertResult, ObjectState, UpdateResult};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify};

pub(crate) const PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION: u32 = 3;

const MAX_LEASE_RETRIES: usize = 3;
/// Number of windows of history to retain in the persisted file.
const STALE_WINDOW_RETENTION: u64 = 60;
/// Number of completed windows included in the demand EWMA.
const DEMAND_EWMA_LOOKBACK_WINDOWS: u64 = 5;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read persisted rate-control state for origin {origin}. {source}"))]
    Read {
        origin: String,
        source: Box<object_store_occ::Error>,
    },

    #[snafu(display("Failed to write persisted rate-control state for origin {origin}. {source}"))]
    Write {
        origin: String,
        source: Box<object_store_occ::Error>,
    },

    #[snafu(display(
        "Conflict exhausted writing persisted rate-control state for origin {origin}"
    ))]
    ConflictExhausted { origin: String },

    #[snafu(display(
        "Cluster rate-control budget exhausted for origin {origin}; persisted store is unavailable and last lease has expired"
    ))]
    FailClosed { origin: String },

    #[snafu(display(
        "The shared rate-control state for origin {origin} was written by a newer Spice version (state version {found}; this version understands {supported}), so this instance does not overwrite it. Upgrade this instance to match the others sharing the rate-control state location."
    ))]
    NewerStateVersion {
        origin: String,
        found: u32,
        supported: u32,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Per-window state persisted in object store.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct PersistedRateControlState {
    pub schema_version: u32,
    pub updated_at_unix_ms: u64,
    pub window_ms: u64,
    pub limiters: HashMap<String, PersistedLimiter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct PersistedLimiter {
    pub burst_per_window: u64,
    pub windows: HashMap<String, PersistedWindow>, // window_id stringified for stable JSON
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct PersistedWindow {
    pub budget_remaining: u64,
    pub leases: HashMap<String, PersistedLease>, // instance_id -> lease
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PersistedLease {
    pub granted: u64,
    /// Last reported within-window consumption; used by peers' demand
    /// estimates (informational only).
    #[serde(default)]
    pub consumed: u64,
    /// Last reported within-window demand (count of `acquire()` calls
    /// regardless of grant). Peers read this from the most recently completed
    /// window to compute their proportional lease share for the next window.
    #[serde(default)]
    pub attempted: u64,
    pub expires_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

/// Configuration for a single leased rate limiter (one quota on one origin).
#[derive(Clone)]
pub(crate) struct LeasedBucketConfig {
    pub store: Arc<dyn ObjectStore>,
    /// Object-store prefix (already normalized).
    pub prefix: String,
    /// Object key (one file per origin holds all limiters for that origin).
    pub object_key: String,
    /// Origin URL string, used for log/error context.
    pub origin: String,
    /// Identifier for this replica.
    pub instance_id: String,
    /// Window length (= `refresh_interval`).
    pub window_duration: Duration,
    /// Persistence-key for this limiter (e.g.
    /// `requests_per_second:burst=4:replenish_ns=250000000`).
    pub limiter_key: String,
    /// Cluster-wide burst budget per window.
    pub burst_per_window: u64,
}

impl std::fmt::Debug for LeasedBucketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeasedBucketConfig")
            .field("prefix", &self.prefix)
            .field("object_key", &self.object_key)
            .field("origin", &self.origin)
            .field("instance_id", &self.instance_id)
            .field("window_duration", &self.window_duration)
            .field("limiter_key", &self.limiter_key)
            .field("burst_per_window", &self.burst_per_window)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct LeasedBucketMetrics {
    /// Tokens granted by the most recent successful lease.
    pub lease_granted: AtomicU64,
    /// Tokens remaining in the cluster budget for the current window after the
    /// most recent successful lease.
    pub cluster_budget_remaining: AtomicU64,
    /// Wall-clock micros taken by the most recent lease acquisition.
    pub last_lease_acquire_micros: AtomicU64,
    /// Total OCC conflicts encountered during lease acquisition.
    pub lease_acquire_conflicts_total: AtomicU64,
    /// Total times a request was denied because the lease was exhausted and
    /// the persisted store was unreachable.
    pub fail_closed_total: AtomicU64,
    /// Total times a lease refresh failed to talk to the store.
    pub lease_refresh_errors_total: AtomicU64,
}

impl LeasedBucketMetrics {
    #[must_use]
    pub fn lease_granted(&self) -> u64 {
        self.lease_granted.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn cluster_budget_remaining(&self) -> u64 {
        self.cluster_budget_remaining.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn last_lease_acquire_micros(&self) -> u64 {
        self.last_lease_acquire_micros.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn lease_acquire_conflicts_total(&self) -> u64 {
        self.lease_acquire_conflicts_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn fail_closed_total(&self) -> u64 {
        self.fail_closed_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn lease_refresh_errors_total(&self) -> u64 {
        self.lease_refresh_errors_total.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct LeasedBucketInner {
    current_window_id: u64,
    granted_this_window: u64,
    consumed_this_window: u64,
    /// Count of `acquire()` calls registered during the current window
    /// (regardless of grant). Drives the demand signal published to peers.
    attempted_this_window: u64,
    /// Pre-leased slot for the *upcoming* window. On window roll the contents
    /// are promoted into the current-window fields so consumers never wait
    /// for a lease at a window boundary.
    next_window_id: u64,
    granted_next_window: u64,
    consumed_next_window: u64,
    /// Final demand and consumption of a window that rolled before its tail
    /// was published. `(window_id, attempted_count, consumed_count)`. The
    /// next refresh writes these back to the rolled-out window's lease
    /// record so peers see the full counts. Cleared on successful publish.
    pending_window_publish: Option<(u64, u64, u64)>,
    /// Local GCRA/TAT pacing state for the current window. A successful
    /// acquire advances this theoretical arrival time by
    /// `window_duration / granted_this_window`, spreading the leased tokens
    /// across the whole window instead of allowing an immediate burst.
    pacing_tat_ns: u64,
    /// Timestamp the current lease was last refreshed (ms).
    last_lease_refresh_ms: u64,
    /// Wall-clock end of the latest pre-leased window (ms).
    lease_expires_at_ms: u64,
    /// Set when the most recent lease attempt failed.
    last_attempt_failed: bool,
}

/// A leased token bucket that gates requests against a cluster-wide budget.
///
/// Within any `window_duration` the cluster as a whole consumes at most
/// `burst_per_window` permits; this is enforced via OCC writes to the
/// `object_store`-backed file shared by all replicas.
#[derive(Debug)]
pub(crate) struct LeasedBucket {
    config: LeasedBucketConfig,
    object_state: Arc<ObjectState<PersistedRateControlState>>,
    inner: Mutex<LeasedBucketInner>,
    notify: Notify,
    metrics: Arc<LeasedBucketMetrics>,
}

impl LeasedBucket {
    pub fn new(config: LeasedBucketConfig) -> Arc<Self> {
        let object_state = Arc::new(
            ObjectState::new(Arc::clone(&config.store)).with_prefix(config.prefix.clone()),
        );
        let now_ms = unix_millis_now();
        let window_id = window_id_for(now_ms, &config.window_duration);
        Arc::new(Self {
            object_state,
            inner: Mutex::new(LeasedBucketInner {
                current_window_id: window_id,
                granted_this_window: 0,
                consumed_this_window: 0,
                attempted_this_window: 0,
                next_window_id: window_id + 1,
                granted_next_window: 0,
                consumed_next_window: 0,
                pending_window_publish: None,
                pacing_tat_ns: 0,
                last_lease_refresh_ms: 0,
                lease_expires_at_ms: 0,
                last_attempt_failed: false,
            }),
            notify: Notify::new(),
            metrics: Arc::new(LeasedBucketMetrics::default()),
            config,
        })
    }

    pub fn metrics(&self) -> Arc<LeasedBucketMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn limiter_key(&self) -> &str {
        &self.config.limiter_key
    }

    pub fn origin(&self) -> &str {
        &self.config.origin
    }

    /// Wait until a permit is available, then consume it. Returns
    /// `Error::FailClosed` if the persisted store is unreachable and the last
    /// lease has expired.
    pub async fn acquire(self: &Arc<Self>) -> Result<()> {
        // Register one unit of demand against whichever window we're in at
        // call entry, before any waiting/looping. This counts every caller
        // exactly once regardless of how long it spends waiting for tokens,
        // so peers' demand-weighted lease calculations see true demand rather
        // than the throttled effective rate.
        {
            let now_ms = unix_millis_now();
            let now_window = window_id_for(now_ms, &self.config.window_duration);
            let mut inner = self.inner.lock().await;
            Self::roll_window_locked(&mut inner, now_window);
            inner.attempted_this_window = inner.attempted_this_window.saturating_add(1);
        }

        loop {
            let current_time_ms = unix_millis_now();
            let now_window = window_id_for(current_time_ms, &self.config.window_duration);

            let wait = {
                let mut inner = self.inner.lock().await;

                Self::roll_window_locked(&mut inner, now_window);

                let wait = if inner.consumed_this_window < inner.granted_this_window {
                    let pacing_now_ns = unix_nanos_now();
                    let interval_ns =
                        pacing_interval_ns(self.config.window_duration, inner.granted_this_window);
                    if inner.pacing_tat_ns <= pacing_now_ns {
                        inner.consumed_this_window += 1;
                        inner.pacing_tat_ns = pacing_now_ns
                            .max(inner.pacing_tat_ns)
                            .saturating_add(interval_ns);
                        return Ok(());
                    }

                    let wait_ns = inner.pacing_tat_ns.saturating_sub(pacing_now_ns);
                    nanos_to_duration(wait_ns).min(self.config.window_duration / 4)
                } else {
                    // Out of local lease.
                    self.config.window_duration / 4
                };

                // Fail-closed condition: store is failing and our lease has fully expired.
                if inner.last_attempt_failed && current_time_ms >= inner.lease_expires_at_ms {
                    self.metrics
                        .fail_closed_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(Error::FailClosed {
                        origin: self.config.origin.clone(),
                    });
                }

                // Otherwise wait. Cap wait at a fraction of the window so we
                // re-check window roll even if no notify arrives (e.g. replica
                // is the only one consuming and the persistence task is slow).
                wait
            };

            let _ = tokio::time::timeout(wait, self.notify.notified()).await;
        }
    }

    /// Roll over per-window state if the window has changed. Caller must hold
    /// the inner lock.
    ///
    /// Promotes the pre-leased `next_window` into the current slot when the
    /// clock advances by exactly one window — eliminating the dead-zone that
    /// would otherwise occur while the persistence task fetches a fresh
    /// lease at each window boundary. If the clock has skipped by more than
    /// one window the pre-leased slot is discarded and both slots reset.
    fn roll_window_locked(inner: &mut LeasedBucketInner, now_window: u64) {
        if now_window == inner.current_window_id {
            return;
        }
        // Capture the final demand and consumption of the window we're
        // rolling out of so the next refresh can publish them to peers.
        // Otherwise counts registered between the last refresh tick and the
        // window roll would be lost.
        if inner.attempted_this_window > 0 || inner.consumed_this_window > 0 {
            inner.pending_window_publish = Some((
                inner.current_window_id,
                inner.attempted_this_window,
                inner.consumed_this_window,
            ));
        }
        if now_window == inner.next_window_id {
            inner.current_window_id = inner.next_window_id;
            inner.granted_this_window = inner.granted_next_window;
            inner.consumed_this_window = inner.consumed_next_window;
            inner.attempted_this_window = 0;
            inner.pacing_tat_ns = 0;
            inner.next_window_id = now_window + 1;
            inner.granted_next_window = 0;
            inner.consumed_next_window = 0;
        } else {
            inner.current_window_id = now_window;
            inner.granted_this_window = 0;
            inner.consumed_this_window = 0;
            inner.attempted_this_window = 0;
            inner.pacing_tat_ns = 0;
            inner.next_window_id = now_window + 1;
            inner.granted_next_window = 0;
            inner.consumed_next_window = 0;
        }
        // last_lease_refresh_ms / lease_expires_at_ms preserved so
        // fail-closed logic can compare against the old expiry.
    }

    /// Refresh the lease for the current window **and** pre-lease the next
    /// window. Called by the persistence task on a timer and at
    /// controller-build time.
    ///
    /// Pre-leasing the next window keeps S3 latency off the critical path:
    /// when the window rolls, [`Self::acquire`] promotes the pre-leased slot
    /// into the current slot via [`Self::roll_window_locked`] without
    /// blocking on a fresh OCC round-trip.
    pub async fn refresh_lease(self: &Arc<Self>) -> Result<()> {
        let started = std::time::Instant::now();
        let now_ms = unix_millis_now();
        let window_ms = duration_millis_u64(self.config.window_duration);
        let now_window = window_id_for(now_ms, &self.config.window_duration);

        let (last_consumed_for_publish, last_attempted_for_publish, pending_publish) = {
            let mut inner = self.inner.lock().await;
            (
                inner.consumed_this_window,
                inner.attempted_this_window,
                inner.pending_window_publish.take(),
            )
        };

        for attempt in 0..MAX_LEASE_RETRIES {
            let read = self.read_state().await;
            let mut state = match read {
                Ok(state) => state.unwrap_or_else(|| fresh_state(window_ms)),
                Err(e) => {
                    self.note_failure();
                    return Err(e);
                }
            };

            // State from a newer version holds leases this version cannot interpret;
            // resetting it would wipe every newer peer's grants, and during a rolling
            // upgrade the two versions would keep resetting each other and over-admit.
            if state.schema_version > PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION {
                self.note_failure();
                return Err(Error::NewerStateVersion {
                    origin: self.config.origin.clone(),
                    found: state.schema_version,
                    supported: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
                });
            }
            // State from an older version is replaced: its leases use a layout this
            // version no longer reads. The warning waits for the write that replaces it,
            // since a write that loses the race replaced nothing.
            let replaced_version = (state.schema_version
                < PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION)
                .then_some(state.schema_version);
            if replaced_version.is_some() {
                state = fresh_state(window_ms);
            }
            state.window_ms = window_ms;
            state.schema_version = PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION;
            state.updated_at_unix_ms = now_ms;

            // Drop windows older than retention horizon.
            if let Some(limiter) = state.limiters.get_mut(&self.config.limiter_key) {
                limiter.windows.retain(|id, _| {
                    id.parse::<u64>()
                        .is_ok_and(|id| id + STALE_WINDOW_RETENTION >= now_window)
                });
            }

            // Publish any pending counts that were latched at a window roll
            // between refresh ticks. Best-effort: if the rolled-out window
            // has already been pruned from state, drop the pending values
            // and move on.
            if let Some((pending_window, pending_attempted, pending_consumed)) = pending_publish
                && let Some(limiter) = state.limiters.get_mut(&self.config.limiter_key)
                && let Some(window) = limiter.windows.get_mut(&pending_window.to_string())
                && let Some(lease) = window.leases.get_mut(&self.config.instance_id)
            {
                let updated =
                    pending_attempted > lease.attempted || pending_consumed > lease.consumed;
                if updated {
                    lease.attempted = lease.attempted.max(pending_attempted);
                    lease.consumed = lease.consumed.max(pending_consumed);
                    lease.updated_at_unix_ms = now_ms;
                }
            }

            let current = self.process_window(
                &mut state,
                now_window,
                now_ms,
                last_consumed_for_publish,
                last_attempted_for_publish,
                last_attempted_for_publish,
            );
            let next = self.process_window(
                &mut state,
                now_window + 1,
                now_ms,
                0,
                0,
                last_attempted_for_publish,
            );

            let dirty = current.dirty || next.dirty;

            if !dirty {
                self.apply_local_lease(
                    now_window,
                    now_ms,
                    current.granted,
                    next.granted,
                    next.expires_at_ms,
                )
                .await;
                self.metrics
                    .lease_granted
                    .store(current.granted, Ordering::Relaxed);
                self.metrics
                    .cluster_budget_remaining
                    .store(current.budget_remaining_after, Ordering::Relaxed);
                self.metrics.last_lease_acquire_micros.store(
                    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                return Ok(());
            }

            match self.write_state(state).await {
                Ok(WriteOutcome::Written) => {
                    if let Some(replaced_version) = replaced_version {
                        tracing::warn!(
                            origin = %self.config.origin,
                            "Replacing shared rate-control state for origin {} written by an older Spice version (state version {}), so leases granted by instances still on that version are reset. Finish upgrading every instance that shares the rate-control state location.",
                            self.config.origin,
                            replaced_version
                        );
                    }
                    self.apply_local_lease(
                        now_window,
                        now_ms,
                        current.granted,
                        next.granted,
                        next.expires_at_ms,
                    )
                    .await;
                    self.metrics
                        .lease_granted
                        .store(current.granted, Ordering::Relaxed);
                    self.metrics
                        .cluster_budget_remaining
                        .store(current.budget_remaining_after, Ordering::Relaxed);
                    self.metrics.last_lease_acquire_micros.store(
                        u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    self.notify.notify_waiters();
                    return Ok(());
                }
                Ok(WriteOutcome::Conflict) => {
                    self.metrics
                        .lease_acquire_conflicts_total
                        .fetch_add(1, Ordering::Relaxed);
                    if attempt + 1 == MAX_LEASE_RETRIES {
                        self.note_failure();
                        return Err(Error::ConflictExhausted {
                            origin: self.config.origin.clone(),
                        });
                    }
                    // retry
                }
                Err(e) => {
                    self.note_failure();
                    return Err(e);
                }
            }
        }

        unreachable!("loop body always returns within MAX_LEASE_RETRIES iterations")
    }

    /// Apply lease grants for the current and next windows to local state.
    async fn apply_local_lease(
        &self,
        now_window: u64,
        now_ms: u64,
        current_grant: u64,
        next_grant: u64,
        next_expires_at_ms: u64,
    ) {
        let mut inner = self.inner.lock().await;
        Self::roll_window_locked(&mut inner, now_window);
        inner.granted_this_window = current_grant;
        // Refresh the next-window slot. The window id may already match if a
        // previous tick already pre-leased it; either way we adopt the latest
        // grant value.
        inner.next_window_id = now_window + 1;
        inner.granted_next_window = next_grant;
        inner.lease_expires_at_ms = next_expires_at_ms;
        inner.last_lease_refresh_ms = now_ms;
        inner.last_attempt_failed = false;
    }

    /// Compute and apply this replica's lease for a single window inside the
    /// shared persisted state. Mutates `state` in place; returns the resulting
    /// grant, the post-write `budget_remaining`, and whether the state was
    /// modified (and therefore must be written back).
    fn process_window(
        &self,
        state: &mut PersistedRateControlState,
        window_id: u64,
        now_ms: u64,
        last_consumed: u64,
        last_attempted: u64,
        local_demand_hint: u64,
    ) -> WindowOutcome {
        let window_ms = duration_millis_u64(self.config.window_duration);
        let window_end_ms = (window_id + 1).saturating_mul(window_ms);
        let now_window = window_id_for(now_ms, &self.config.window_duration);
        // Use only *fully completed* windows as the demand signal source for
        // both the current and next windows. Sourcing from the in-progress
        // window's partial data caused leases to oscillate wildly within a
        // window as different ticks observed different partial counts. EWMA
        // over completed windows gives a stable, smoothed signal that is
        // resilient to a single missing/zero publish.

        let limiter = state
            .limiters
            .entry(self.config.limiter_key.clone())
            .or_insert_with(|| PersistedLimiter {
                burst_per_window: self.config.burst_per_window,
                windows: HashMap::new(),
            });
        limiter.burst_per_window = self.config.burst_per_window;

        // Read the smoothed demand signal from recent completed windows before
        // taking a mutable borrow on `limiter.windows` for the entry below.
        //
        // Demand classification: a replica that consumed everything it was
        // granted is **saturated** — we don't know how much more it would
        // have used, so we treat its demand as the full cluster burst. A
        // replica that did not consume its full grant signals demand equal
        // to its observed `attempted` (true `acquire()` call rate). EWMA is
        // computed over this classified demand, not raw `attempted`, so a
        // grant-bound hot replica remains visible as high demand.
        let burst = self.config.burst_per_window;
        let classify = |lease: &PersistedLease| -> u64 {
            if lease.granted > 0 && lease.consumed >= lease.granted {
                burst
            } else {
                lease.attempted.max(lease.consumed)
            }
        };
        let mut my_ewma_demand: u128 = 0;
        let mut total_ewma_demand: u128 = 0;
        for age in 0..DEMAND_EWMA_LOOKBACK_WINDOWS {
            let Some(source_window) = now_window.saturating_sub(1).checked_sub(age) else {
                break;
            };
            let weight_shift = DEMAND_EWMA_LOOKBACK_WINDOWS - 1 - age;
            let weight = 1_u128 << weight_shift; // 16, 8, 4, 2, 1 for lookback=5.
            if let Some(window) = limiter.windows.get(&source_window.to_string()) {
                my_ewma_demand += u128::from(
                    window
                        .leases
                        .get(&self.config.instance_id)
                        .map_or(0, classify),
                ) * weight;
                total_ewma_demand += window
                    .leases
                    .values()
                    .map(classify)
                    .map(u128::from)
                    .sum::<u128>()
                    * weight;
            }
        }

        let window = limiter
            .windows
            .entry(window_id.to_string())
            .or_insert_with(|| PersistedWindow {
                budget_remaining: self.config.burst_per_window,
                leases: HashMap::new(),
            });

        // Drop expired leases in this window (only relevant for the current
        // window; a future window's lease cannot have expired).
        window
            .leases
            .retain(|_, lease| lease.expires_at_unix_ms > now_ms);

        // Recompute budget_remaining from surviving leases (defensive).
        let leased: u64 = window.leases.values().map(|l| l.granted).sum();
        window.budget_remaining = self.config.burst_per_window.saturating_sub(leased);

        let my_existing = window
            .leases
            .get(&self.config.instance_id)
            .map_or(0, |l| l.granted);
        let others_leased: u64 = window
            .leases
            .iter()
            .filter(|(k, _)| k.as_str() != self.config.instance_id.as_str())
            .map(|(_, l)| l.granted)
            .sum();
        let max_possible_for_me = self.config.burst_per_window.saturating_sub(others_leased);

        // Demand-weighted share: every replica claims a slice of the cluster
        // burst proportional to its EWMA-smoothed, saturation-aware demand.
        // If there is no persisted demand yet, use local in-progress demand
        // as a bootstrap signal instead of fair-share: a hot replica should
        // reclaim idle budget, while an idle replica should hold only the
        // minimum lease.
        let demand_signal = if total_ewma_demand > 0 && my_ewma_demand > 0 {
            let share = (u128::from(burst) * my_ewma_demand) / total_ewma_demand;
            u64::try_from(share).unwrap_or(burst)
        } else if total_ewma_demand > 0 {
            min_lease(burst)
        } else if local_demand_hint > 0 {
            burst
        } else {
            min_lease(burst)
        };

        let demand = demand_signal
            .max(min_lease(burst))
            .min(max_lease_per_replica(burst));

        // Pick the new grant.
        //
        // Both the current and pre-leased windows are **first-write-wins**:
        // the first tick to lease a given window/replica establishes the
        // grant, and subsequent ticks only refresh `consumed`/`attempted`.
        // This eliminates within-window oscillation that would otherwise
        // occur as demand-weighted recomputation ping-pongs leases between
        // replicas. Adjustments to demand show up in the *next* window's
        // pre-lease.
        let new_grant = if my_existing > 0 {
            my_existing
        } else {
            demand.min(max_possible_for_me)
        };

        // Always re-publish so peers see updated `consumed`/`attempted`
        // counters even when our `granted` value is unchanged.
        let needs_publish = match window.leases.get(&self.config.instance_id) {
            Some(existing) => {
                existing.granted != new_grant
                    || existing.consumed != last_consumed
                    || existing.attempted != last_attempted
            }
            None => true,
        };

        if needs_publish {
            window.leases.insert(
                self.config.instance_id.clone(),
                PersistedLease {
                    granted: new_grant,
                    consumed: last_consumed,
                    attempted: last_attempted,
                    expires_at_unix_ms: window_end_ms,
                    updated_at_unix_ms: now_ms,
                },
            );
        }

        // Recompute budget_remaining defensively from final lease state.
        let total_leased: u64 = window.leases.values().map(|l| l.granted).sum();
        window.budget_remaining = burst.saturating_sub(total_leased);

        WindowOutcome {
            granted: new_grant,
            budget_remaining_after: window.budget_remaining,
            expires_at_ms: window_end_ms,
            dirty: needs_publish,
        }
    }

    fn note_failure(&self) {
        self.metrics
            .lease_refresh_errors_total
            .fetch_add(1, Ordering::Relaxed);
        // Mark inner state as failing so acquire() can fail closed once the
        // lease expires. We don't need the inner lock in async to set this
        // since `last_attempt_failed` is set under the next lease attempt's
        // lock; for now, we use a try_lock-like approach via blocking_lock
        // which is safe because this is only called from refresh_lease.
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.last_attempt_failed = true;
        }
    }

    async fn read_state(&self) -> Result<Option<PersistedRateControlState>> {
        self.object_state
            .get(self.config.object_key.as_str())
            .await
            .map_err(|source| Error::Read {
                origin: self.config.origin.clone(),
                source: Box::new(source),
            })
    }

    async fn write_state(&self, state: PersistedRateControlState) -> Result<WriteOutcome> {
        // First try update (assumes file exists); if NotFound, fall back to insert.
        match self
            .object_state
            .update(self.config.object_key.as_str(), &state)
            .await
            .map_err(|source| Error::Write {
                origin: self.config.origin.clone(),
                source: Box::new(source),
            })? {
            UpdateResult::Ok => Ok(WriteOutcome::Written),
            UpdateResult::Conflict { .. } => Ok(WriteOutcome::Conflict),
            UpdateResult::NotFound => {
                match self
                    .object_state
                    .insert(self.config.object_key.as_str(), &state)
                    .await
                    .map_err(|source| Error::Write {
                        origin: self.config.origin.clone(),
                        source: Box::new(source),
                    })? {
                    InsertResult::Ok => Ok(WriteOutcome::Written),
                    InsertResult::AlreadyExists => Ok(WriteOutcome::Conflict),
                }
            }
        }
    }
}

enum WriteOutcome {
    Written,
    Conflict,
}

/// Result of computing this replica's lease for a single window.
struct WindowOutcome {
    granted: u64,
    budget_remaining_after: u64,
    expires_at_ms: u64,
    /// Whether the persisted state was modified and must therefore be written
    /// back. Two reasons we'd skip a write: (a) we already had a lease at the
    /// desired size in this window from a previous tick, or (b) demand is
    /// zero. Combined-window dirty status drives the OCC write decision.
    dirty: bool,
}

fn fresh_state(window_ms: u64) -> PersistedRateControlState {
    PersistedRateControlState {
        schema_version: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
        updated_at_unix_ms: unix_millis_now(),
        window_ms,
        limiters: HashMap::new(),
    }
}

fn window_id_for(now_ms: u64, window: &Duration) -> u64 {
    let w = duration_millis_u64(*window).max(1);
    now_ms / w
}

fn min_lease(burst_per_window: u64) -> u64 {
    let floor = burst_per_window / 100;
    floor.max(1)
}

fn max_lease_per_replica(burst_per_window: u64) -> u64 {
    // Allow a single replica to claim almost all the budget when no peer is
    // demanding any. We always reserve at least `min_lease` so a newcomer can
    // grab a starter slice in its first window.
    burst_per_window
        .saturating_sub(min_lease(burst_per_window))
        .max(1)
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn pacing_interval_ns(window: Duration, granted: u64) -> u64 {
    if granted == 0 {
        return duration_nanos_u64(window).max(1);
    }
    (duration_nanos_u64(window) / granted).max(1)
}

fn nanos_to_duration(nanos: u64) -> Duration {
    Duration::from_nanos(nanos)
}

fn unix_millis_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => duration_millis_u64(d),
        Err(_) => 0,
    }
}

fn unix_nanos_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => duration_nanos_u64(d),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use object_store::memory::InMemory;

    fn config_for(burst: u64, instance: &str, window: Duration) -> LeasedBucketConfig {
        LeasedBucketConfig {
            store: Arc::new(InMemory::new()),
            prefix: String::new(),
            object_key: "test/origin".to_string(),
            origin: "https://example.com".to_string(),
            instance_id: instance.to_string(),
            window_duration: window,
            limiter_key: "rps:burst=10".to_string(),
            burst_per_window: burst,
        }
    }

    /// State written by a newer version is left alone: resetting it would wipe the
    /// newer peers' grants, and two versions resetting each other over-admit.
    #[tokio::test]
    async fn state_from_a_newer_version_is_not_overwritten() {
        use object_store::ObjectStoreExt;

        let store = Arc::new(InMemory::new());
        let mut config = config_for(10, "a", Duration::from_secs(1));
        config.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut newer = fresh_state(1_000);
        newer.schema_version = PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION + 1;
        let path = object_store::path::Path::from("test/origin.json");
        let bytes = serde_json::to_vec(&newer).expect("serialize");
        store
            .put(&path, bytes.clone().into())
            .await
            .expect("seed newer state");

        let bucket = LeasedBucket::new(config);
        let err = bucket
            .refresh_lease()
            .await
            .expect_err("a newer state version must not be overwritten");
        assert!(matches!(err, Error::NewerStateVersion { .. }), "{err}");

        let stored = store
            .get(&path)
            .await
            .expect("state still there")
            .bytes()
            .await
            .expect("body");
        assert_eq!(
            stored.as_ref(),
            bytes.as_slice(),
            "the newer state is untouched"
        );
    }

    /// Newer-version state is unavailable state: the lease granted before it appeared
    /// is still honored, and once that lease expires `acquire` fails closed instead of
    /// waiting on a lease that can never be renewed.
    #[tokio::test]
    async fn state_from_a_newer_version_fails_closed_once_the_current_lease_expires() {
        use object_store::ObjectStoreExt;

        let store = Arc::new(InMemory::new());
        let mut config = config_for(10, "a", Duration::from_secs(1));
        config.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let bucket = LeasedBucket::new(config);
        bucket
            .refresh_lease()
            .await
            .expect("lease from an empty state location");

        // A newer instance rewrites the shared document after this one holds a lease.
        let mut newer = fresh_state(1_000);
        newer.schema_version = PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION + 1;
        let path = object_store::path::Path::from("test/origin.json");
        let bytes = serde_json::to_vec(&newer).expect("serialize");
        store
            .put(&path, bytes.clone().into())
            .await
            .expect("seed newer state");

        let err = bucket
            .refresh_lease()
            .await
            .expect_err("a newer state version must not be overwritten");
        assert!(
            matches!(
                err,
                Error::NewerStateVersion { found, supported, .. }
                    if found == PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION + 1
                        && supported == PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION
            ),
            "{err}"
        );

        // The lease covers the current and the next window, so this permit comes from
        // it even if the window rolled since the first refresh.
        bucket
            .acquire()
            .await
            .expect("the lease granted before the newer state appeared is still honored");

        // Lease expiry is wall-clock time, so wait for the instant it passes: the end of
        // the next 1 s window, at most 2 s away.
        let lease_expires_at_ms = bucket.inner.lock().await.lease_expires_at_ms;
        let now_ms = unix_millis_now();
        assert!(
            lease_expires_at_ms > now_ms && lease_expires_at_ms <= now_ms + 2_000,
            "the lease must end with the next window: expires at {lease_expires_at_ms} ms, now {now_ms} ms"
        );
        // A monotonic deadline bounds the wait even if the wall clock steps backwards.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while unix_millis_now() <= lease_expires_at_ms {
            assert!(
                std::time::Instant::now() < deadline,
                "the wall clock did not pass the lease expiry within 5 s: expires at {lease_expires_at_ms} ms, now {} ms",
                unix_millis_now()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let result = tokio::time::timeout(Duration::from_secs(5), bucket.acquire())
            .await
            .expect("acquire must fail closed after the lease expires, not wait for a renewal");
        assert!(
            matches!(&result, Err(Error::FailClosed { origin }) if origin == "https://example.com"),
            "{result:?}"
        );
        assert_eq!(bucket.metrics.fail_closed_total(), 1);

        let stored = store
            .get(&path)
            .await
            .expect("state still there")
            .bytes()
            .await
            .expect("body");
        assert_eq!(
            stored.as_ref(),
            bytes.as_slice(),
            "the newer state is untouched"
        );
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("log buffer lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl CapturedLogs {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("log buffer lock")).into_owned()
        }

        fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
            tracing_subscriber::fmt()
                .with_ansi(false)
                .without_time()
                .with_writer(self.clone())
                .finish()
        }
    }

    /// Store on which an instance still on the older schema version rewrites the
    /// shared document just before each conditional update, so every write this
    /// instance attempts finds the document changed under it.
    #[derive(Debug)]
    struct OlderWriterBeforeEachUpdate {
        inner: Arc<dyn ObjectStore>,
        older: Vec<u8>,
    }

    impl std::fmt::Display for OlderWriterBeforeEachUpdate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "OlderWriterBeforeEachUpdate")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for OlderWriterBeforeEachUpdate {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if matches!(opts.mode, object_store::PutMode::Update(_)) {
                self.inner
                    .put_opts(
                        location,
                        self.older.clone().into(),
                        object_store::PutOptions::from(object_store::PutMode::Overwrite),
                    )
                    .await?;
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The warning that older-version state was replaced is logged only once the
    /// replacement is written: a write that loses to an instance still on the older
    /// version replaced nothing, so it must not tell the user the leases were reset.
    #[tokio::test]
    async fn older_state_version_is_reported_replaced_only_once_the_write_lands() {
        use object_store::ObjectStoreExt;

        let mut older = fresh_state(1_000);
        older.schema_version = PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION - 1;
        let older = serde_json::to_vec(&older).expect("serialize");
        let path = object_store::path::Path::from("test/origin.json");
        let replaced = format!(
            "Replacing shared rate-control state for origin https://example.com written by an older Spice version (state version {}), so leases granted by instances still on that version are reset. Finish upgrading every instance that shares the rate-control state location.",
            PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION - 1
        );

        // Every write loses to the older instance, so nothing is replaced.
        let contended = Arc::new(InMemory::new());
        contended
            .put(&path, older.clone().into())
            .await
            .expect("seed older state");
        let mut config = config_for(10, "a", Duration::from_secs(1));
        config.store = Arc::new(OlderWriterBeforeEachUpdate {
            inner: Arc::clone(&contended) as Arc<dyn ObjectStore>,
            older: older.clone(),
        });
        let logs = CapturedLogs::default();
        let err = {
            let _guard = tracing::subscriber::set_default(logs.subscriber());
            LeasedBucket::new(config)
                .refresh_lease()
                .await
                .expect_err("every write loses to the older instance")
        };
        assert!(matches!(err, Error::ConflictExhausted { .. }), "{err}");
        let stored = contended
            .get(&path)
            .await
            .expect("state")
            .bytes()
            .await
            .expect("body");
        assert_eq!(
            stored.as_ref(),
            older.as_slice(),
            "the older state is still in place"
        );
        assert_eq!(
            logs.text().matches(replaced.as_str()).count(),
            0,
            "{}",
            logs.text()
        );

        // Uncontended, the replacement lands and the warning is logged once.
        let store = Arc::new(InMemory::new());
        store
            .put(&path, older.clone().into())
            .await
            .expect("seed older state");
        let mut config = config_for(10, "a", Duration::from_secs(1));
        config.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let logs = CapturedLogs::default();
        {
            let _guard = tracing::subscriber::set_default(logs.subscriber());
            LeasedBucket::new(config)
                .refresh_lease()
                .await
                .expect("the replacement is written");
        }
        let stored: PersistedRateControlState = serde_json::from_slice(
            &store
                .get(&path)
                .await
                .expect("state")
                .bytes()
                .await
                .expect("body"),
        )
        .expect("deserialize");
        assert_eq!(
            stored.schema_version,
            PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION
        );
        assert_eq!(
            logs.text().matches(replaced.as_str()).count(),
            1,
            "{}",
            logs.text()
        );
    }

    #[tokio::test]
    async fn single_replica_lease_grants_bootstrap_share() {
        let bucket = LeasedBucket::new(config_for(10, "a", Duration::from_secs(1)));
        bucket.refresh_lease().await.expect("lease should succeed");
        let inner = bucket.inner.lock().await;
        // Single replica with no demand history and no local demand holds
        // only the minimum lease. Once it observes local demand it can grow
        // to MAX_LEASE_PER_REPLICA = 9.
        assert_eq!(inner.granted_this_window, 1);
        assert_eq!(bucket.metrics.lease_granted(), 1);
    }

    #[tokio::test]
    async fn demand_weighted_proportional_share_after_one_window() {
        // Cluster cap = 5, two replicas. With A reporting demand=10 and
        // B reporting demand=5 every window, leases should converge to a
        // proportional split: A ~= 3 of the 5-token budget, B ~= 1
        // (floored at min_lease). Convergence takes a couple of windows
        // because the current-window grant is locked at its pre-leased
        // value.
        let store = Arc::new(InMemory::new());
        let mut cfg_a = config_for(5, "a", Duration::from_millis(150));
        let mut cfg_b = cfg_a.clone();
        cfg_a.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_b.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_b.instance_id = "b".to_string();

        let a = LeasedBucket::new(cfg_a);
        let b = LeasedBucket::new(cfg_b);

        a.refresh_lease()
            .await
            .expect("lease refresh should succeed");
        b.refresh_lease()
            .await
            .expect("lease refresh should succeed");

        for _ in 0..4 {
            {
                let mut inner = a.inner.lock().await;
                inner.attempted_this_window = 10;
            }
            {
                let mut inner = b.inner.lock().await;
                inner.attempted_this_window = 5;
            }
            a.refresh_lease()
                .await
                .expect("lease refresh should succeed");
            b.refresh_lease()
                .await
                .expect("lease refresh should succeed");
            tokio::time::sleep(Duration::from_millis(170)).await;
        }
        a.refresh_lease()
            .await
            .expect("lease refresh should succeed");
        b.refresh_lease()
            .await
            .expect("lease refresh should succeed");

        let granted_a = a.metrics.lease_granted();
        let granted_b = b.metrics.lease_granted();

        // A had 2x B's demand, so should get a strictly larger lease.
        assert!(
            granted_a > granted_b,
            "A demand=10 should outrank B demand=5; got A={granted_a} B={granted_b}"
        );
        // Total never exceeds cluster cap.
        assert!(
            granted_a + granted_b <= 5,
            "sum {granted_a}+{granted_b} > cap 5"
        );
        // A should claim the lion's share — expect at least 3 of the 5.
        assert!(
            granted_a >= 3,
            "A should get >= 3 of 5 with 2:1 demand ratio, got A={granted_a} B={granted_b}"
        );
    }

    #[tokio::test]
    async fn single_demanding_replica_reclaims_budget_after_idle_peer_drops() {
        // Two replicas, cluster cap = 10. A demands 20 per window; B demands
        // 0. After several windows of demand-weighted convergence, A's lease
        // should grow to MAX_LEASE_PER_REPLICA and B should drop to
        // min_lease.
        let store = Arc::new(InMemory::new());
        let mut cfg_a = config_for(10, "a", Duration::from_millis(150));
        let mut cfg_b = cfg_a.clone();
        cfg_a.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_b.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_b.instance_id = "b".to_string();

        let a = LeasedBucket::new(cfg_a);
        let b = LeasedBucket::new(cfg_b);

        // Bootstrap.
        a.refresh_lease()
            .await
            .expect("lease refresh should succeed");
        b.refresh_lease()
            .await
            .expect("lease refresh should succeed");

        // Drive several windows where A reports demand=20 and B reports 0.
        // The current-window grant is locked at the pre-leased value, so
        // convergence to MAX_LEASE_PER_REPLICA takes a couple of pre-lease
        // cycles as A's share grows and B's shrinks toward min_lease.
        for _ in 0..4 {
            // Re-inject demand at the start of each window (the previous
            // window roll reset attempted_this_window to 0).
            {
                let mut inner = a.inner.lock().await;
                inner.attempted_this_window = 20;
            }
            // B stays idle.
            a.refresh_lease()
                .await
                .expect("lease refresh should succeed");
            b.refresh_lease()
                .await
                .expect("lease refresh should succeed");
            tokio::time::sleep(Duration::from_millis(170)).await;
        }
        // Final refresh after the last window roll so the pre-leased lease
        // for the now-current window is reflected in metrics.
        a.refresh_lease()
            .await
            .expect("lease refresh should succeed");
        b.refresh_lease()
            .await
            .expect("lease refresh should succeed");

        let granted_a = a.metrics.lease_granted();
        let granted_b = b.metrics.lease_granted();

        assert_eq!(
            granted_a,
            max_lease_per_replica(10),
            "A with sole demand should converge to MAX_LEASE_PER_REPLICA, got A={granted_a} B={granted_b}"
        );
        assert_eq!(
            granted_b,
            min_lease(10),
            "B with zero demand should converge to min_lease, got A={granted_a} B={granted_b}"
        );
    }

    #[tokio::test]
    async fn two_replicas_share_one_file() {
        let store = Arc::new(InMemory::new());
        let mut cfg_a = config_for(10, "a", Duration::from_secs(1));
        let mut cfg_b = cfg_a.clone();
        cfg_a.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_b.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_b.instance_id = "b".to_string();

        let a = LeasedBucket::new(cfg_a);
        let b = LeasedBucket::new(cfg_b);
        a.refresh_lease().await.expect("a lease");
        b.refresh_lease().await.expect("b lease");

        let granted_a = a.metrics.lease_granted();
        let granted_b = b.metrics.lease_granted();
        assert!(granted_a + granted_b <= 10, "{granted_a}+{granted_b} > 10");
    }

    #[tokio::test]
    async fn acquire_yields_when_lease_exhausted_and_recovers_on_window_roll() {
        let bucket = LeasedBucket::new(config_for(2, "a", Duration::from_millis(100)));
        // Force grant of 1 token by setting EWMA low.
        bucket.refresh_lease().await.expect("lease");
        // Manually overwrite granted to 1 so we exhaust quickly.
        {
            let mut inner = bucket.inner.lock().await;
            inner.granted_this_window = 1;
            inner.consumed_this_window = 0;
        }
        bucket.acquire().await.expect("first ok");
        // Second acquire should block briefly until window rolls + lease refreshes.
        let bucket2 = Arc::clone(&bucket);
        let handle = tokio::spawn(async move { bucket2.acquire().await });
        // Drive a refresh on the new window.
        tokio::time::sleep(Duration::from_millis(150)).await;
        bucket.refresh_lease().await.expect("refresh");
        let res = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("did not block forever");
        res.expect("join").expect("acquire ok");
    }

    /// Polls the wall clock the bucket windows by until window `target` has
    /// begun. Bounded, so a window that never arrives fails the test instead
    /// of hanging it, and a clock that skipped the window entirely is named.
    async fn wait_for_window(target: u64, window: Duration) {
        let deadline = tokio::time::Instant::now() + window * 10;
        loop {
            let now_window = window_id_for(unix_millis_now(), &window);
            if now_window >= target {
                assert_eq!(now_window, target, "the clock skipped past window {target}");
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "window {target} never began; still in window {now_window}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[tokio::test]
    async fn pre_lease_lands_in_next_window_and_promotes_on_roll() {
        // 200 ms windows make the test quick; burst=20 so MAX_LEASE_PER_REPLICA = 19.
        let window = Duration::from_millis(200);
        let bucket = LeasedBucket::new(config_for(20, "a", window));

        // Lease at the top of a fresh window, so the refresh and the drain
        // finish inside it. Refreshing at the very end of a window could let
        // the clock skip the pre-leased window, which is then discarded.
        let start = window_id_for(unix_millis_now(), &window);
        wait_for_window(start + 1, window).await;
        bucket.refresh_lease().await.expect("lease");

        // After refresh, both current and next slots are granted: a replica
        // with no demand history holds the minimum lease of one in each.
        let (cur_window, cur_grant, next_window, next_grant) = {
            let inner = bucket.inner.lock().await;
            (
                inner.current_window_id,
                inner.granted_this_window,
                inner.next_window_id,
                inner.granted_next_window,
            )
        };
        assert_eq!(next_window, cur_window + 1);
        assert_eq!(cur_grant, 1, "current window holds the minimum lease");
        assert_eq!(
            next_grant, 1,
            "next-window pre-lease holds the minimum lease"
        );

        // Drain the current window's lease so the next acquire would block on
        // a roll.
        for _ in 0..cur_grant {
            bucket.acquire().await.expect("acquire ok");
        }

        // Once the window rolls, acquire() must NOT block on a fresh OCC
        // round-trip: the next-window slot is promoted within its first poll,
        // so it completes without waiting at all.
        wait_for_window(next_window, window).await;
        bucket
            .acquire()
            .now_or_never()
            .expect(
                "post-roll acquire must complete without waiting: the pre-leased slot is promoted",
            )
            .expect("post-roll acquire ok");

        // Inner state should reflect that the previous next-window is now current.
        let inner = bucket.inner.lock().await;
        assert_eq!(inner.current_window_id, next_window);
        assert_eq!(inner.granted_this_window, next_grant);
        assert_eq!(inner.consumed_this_window, 1);
    }
}
