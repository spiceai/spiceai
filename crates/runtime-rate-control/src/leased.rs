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
//! Schema is `PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION = 4`. Older state is
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
//!
//! ## Cluster adaptive throttling
//!
//! Demand-weighted leasing divides a *fixed* budget. Adaptive throttling lowers
//! the budget itself while the origin fails, and raises it again as the origin
//! recovers, so the whole cluster backs off together rather than each replica
//! backing off on its own view (which would only move load between replicas,
//! not off the origin).
//!
//! Each replica publishes the upstream outcomes it saw in a window (`ok` /
//! `failed`) alongside its lease. Every replica then reads the same shared
//! counts and derives the same coefficient, with no replica-to-replica traffic:
//!
//! ```text
//! requests    = Σ_age weight(age) · Σ_leases (ok + failed)
//! accepts     = Σ_age weight(age) · Σ_leases  ok
//! coefficient = min( (K · accepts + 1) / (requests + 1),  1 )
//! effective_burst = max(1, round(burst_per_window · coefficient))
//! ```
//!
//! `K = 1 / (1 - failure_threshold)`, so the coefficient is exactly 1 at or
//! below the configured error rate and falls below it above. The floor of one
//! request per window keeps recovery observable at any error rate.
//!
//! Unlike the single-node controller, outcome counts are bucketed into whole
//! windows rather than timestamped, so the decay runs on window identifiers:
//! a source window `s` ends at `(s+1)·W` and the target window `t` starts at
//! `t·W`, giving `age = t - s - 1` whole windows of separation. Every replica
//! leasing window `t` therefore applies the same weights to the same source
//! windows, whatever its local clock reads.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use object_store::ObjectStore;
use object_store_occ::{InsertResult, ObjectState, UpdateResult};
use parking_lot::Mutex as SyncMutex;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify};

use crate::adaptive::{RequestOutcome, ThrottleState};
use crate::phase_change_log::{Damping, PhaseChangeLog};

/// Bumped to 4 for the `ok` / `failed` / `effective_burst` fields of cluster
/// adaptive throttling. [`PersistedRateControlState::is_current_schema`] tests
/// for an exact match and a reader discards a mismatch, so a fleet spanning two
/// schema versions erases the shared state on every tick, in both directions.
/// The `rate-control` feature is absent from the release build, so the bump is
/// safe now and will not be once it ships.
pub(crate) const PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION: u32 = 4;

const MAX_LEASE_RETRIES: usize = 3;
/// Number of windows of history to retain in the persisted file.
const STALE_WINDOW_RETENTION: u64 = 60;
/// Number of completed windows included in the demand EWMA.
const DEMAND_EWMA_LOOKBACK_WINDOWS: u64 = 5;
/// Number of completed windows included in the upstream-outcome EWMA that
/// drives the cluster adaptive coefficient.
const OUTCOME_EWMA_LOOKBACK_WINDOWS: u64 = 5;
/// The coefficient of a healthy origin: the configured cluster budget applies
/// in full. Mirrors the single-node controller's `FULL_ADMISSION_COEFFICIENT`.
const FULL_ADMISSION_COEFFICIENT: f64 = 1.0;

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
    /// Cluster budget for this window after the adaptive coefficient. The first
    /// replica to write the window fixes this value; later replicas read it and
    /// do not calculate their own, so two replicas reading the file at
    /// different moments cannot size their grants against different budgets.
    /// `None` on a window written before cluster adaptive throttling, which
    /// reads as the configured `burst_per_window`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_burst: Option<u64>,
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
    /// Upstream requests in this window that answered with a recorded success.
    /// `None` means this replica did not report, and the lease is dropped from
    /// the cluster error-rate estimate rather than read as zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<u64>,
    /// Upstream requests in this window that answered with a recorded failure.
    /// See [`PersistedLease::ok`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed: Option<u64>,
    pub expires_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

impl PersistedRateControlState {
    fn fresh(window: Duration) -> Self {
        Self {
            schema_version: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
            updated_at_unix_ms: unix_millis(SystemTime::now()),
            window_ms: duration_millis_u64(window),
            limiters: HashMap::new(),
        }
    }

    fn is_current_schema(&self) -> bool {
        self.schema_version == PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION
    }

    /// Mark the state as written now, by this schema version, for this window
    /// length.
    fn stamp(&mut self, now: SystemTime, window: Duration) {
        self.schema_version = PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION;
        self.updated_at_unix_ms = unix_millis(now);
        self.window_ms = duration_millis_u64(window);
    }

    fn limiter_mut(&mut self, key: &str) -> Option<&mut PersistedLimiter> {
        self.limiters.get_mut(key)
    }

    /// The limiter for `key`, created at `burst` if absent. The stored burst
    /// always tracks the live config.
    fn limiter_entry(&mut self, key: &str, burst: u64) -> &mut PersistedLimiter {
        let limiter = self
            .limiters
            .entry(key.to_string())
            .or_insert_with(|| PersistedLimiter::new(burst));
        limiter.burst_per_window = burst;
        limiter
    }
}

impl PersistedLimiter {
    fn new(burst: u64) -> Self {
        Self {
            burst_per_window: burst,
            windows: HashMap::new(),
        }
    }

    /// Drop windows older than the retention horizon.
    fn retain_recent_windows(&mut self, now_window: u64) {
        self.windows.retain(|id, _| {
            id.parse::<u64>()
                .ok()
                .is_some_and(|id| id + STALE_WINDOW_RETENTION >= now_window)
        });
    }

    fn window_mut(&mut self, window_id: u64) -> Option<&mut PersistedWindow> {
        self.windows.get_mut(&window_id.to_string())
    }

    /// The record for `window_id`, created with `burst_if_absent` budget if absent.
    fn window_entry(&mut self, window_id: u64, burst_if_absent: u64) -> &mut PersistedWindow {
        self.windows
            .entry(window_id.to_string())
            .or_insert_with(|| PersistedWindow::new(burst_if_absent))
    }

    /// Exponentially weighted (recent weighted highest) demand over the previous `lookback_windows`
    ///  completed windows.
    ///
    /// Only fully completed windows contribute: sourcing the signal from the
    /// in-progress window's partial data made leases oscillate within a window
    /// as different ticks observed different partial counts. The EWMA also
    /// keeps a hot replica visible across a single missing or zero publish.
    fn ewma_demand(
        &self,
        instance: &str,
        now_window: u64,
        burst: u64,
        lookback_windows: u64,
    ) -> DemandSample {
        let mut demand = DemandSample::default();
        for age in 0..lookback_windows {
            let Some(source_window) = now_window.saturating_sub(1).checked_sub(age) else {
                break;
            };
            // e.g. 16, 8, 4, 2, 1 for lookback=5.
            let weight = 1_u128 << (lookback_windows - 1 - age);
            if let Some(window) = self.windows.get(&source_window.to_string()) {
                demand.accumulate(window.classified_demand(instance, burst).weighted(weight));
            }
        }
        demand
    }

    /// The budget a peer already fixed for `window_id`, if any.
    fn effective_burst_of(&self, window_id: u64) -> Option<u64> {
        self.windows.get(&window_id.to_string())?.effective_burst
    }

    /// Weighted upstream request and success counts over the completed windows
    /// before `target_window`.
    ///
    /// Anchored on `target_window`, never on the clock: two replicas leasing the
    /// same window must weight the same source windows the same way, or they
    /// derive different budgets from the same file.
    ///
    /// A source window `s` covers `[s·W, (s+1)·W)` and the target window starts
    /// at `t·W`, so the gap between them is `(t − s − 1)·W` — `age` below is
    /// that gap in whole windows, and the weight halves every
    /// `half_life_windows` of it.
    fn ewma_outcomes(
        &self,
        target_window: u64,
        lookback_windows: u64,
        half_life_windows: u64,
    ) -> OutcomeSample {
        let mut outcomes = OutcomeSample::default();
        #[expect(
            clippy::cast_precision_loss,
            reason = "half-life is a small window count; f64 represents it exactly"
        )]
        let half_life = half_life_windows.max(1) as f64;
        for age in 0..lookback_windows {
            let Some(source_window) = target_window.checked_sub(age + 1) else {
                break; // Before the epoch's first window: nothing older exists.
            };
            let Some(window) = self.windows.get(&source_window.to_string()) else {
                continue;
            };
            #[expect(
                clippy::cast_precision_loss,
                reason = "age is bounded by the lookback; f64 represents it exactly"
            )]
            let weight = 0.5_f64.powf(age as f64 / half_life);
            outcomes.accumulate(window.final_outcomes(), weight);
        }
        outcomes
    }
}

impl PersistedWindow {
    fn new(burst: u64) -> Self {
        Self {
            // Left unset so the caller can tell a window it just created from
            // one a peer created, and fix the budget exactly once.
            effective_burst: None,
            budget_remaining: burst,
            leases: HashMap::new(),
        }
    }

    fn total_granted(&self) -> u64 {
        self.leases.values().map(|lease| lease.granted).sum()
    }

    fn granted_for(&self, instance: &str) -> u64 {
        self.leases.get(instance).map_or(0, |lease| lease.granted)
    }

    fn granted_by_others(&self, instance: &str) -> u64 {
        self.leases
            .iter()
            .filter(|(id, _)| id.as_str() != instance)
            .map(|(_, lease)| lease.granted)
            .sum()
    }

    fn lease_mut(&mut self, instance: &str) -> Option<&mut PersistedLease> {
        self.leases.get_mut(instance)
    }

    fn drop_expired(&mut self, now: SystemTime) {
        let now_ms = unix_millis(now);
        self.leases
            .retain(|_, lease| lease.expires_at_unix_ms > now_ms);
    }

    /// `budget_remaining` is a cache of `burst - total_granted`; recompute it
    /// from the surviving leases rather than trust the stored value.
    fn recompute_budget_remaining(&mut self, burst: u64) {
        self.budget_remaining = burst.saturating_sub(self.total_granted());
    }

    /// Demand this window signals for `instance`, and for the cluster.
    fn classified_demand(&self, instance: &str, burst: u64) -> DemandSample {
        DemandSample {
            mine: u128::from(
                self.leases
                    .get(instance)
                    .map_or(0, |lease| lease.classified_demand(burst)),
            ),
            total: self
                .leases
                .values()
                .map(|lease| u128::from(lease.classified_demand(burst)))
                .sum(),
        }
    }

    /// Upstream outcomes this window holds evidence for, summed over the
    /// replicas whose counts are final.
    ///
    /// A replica publishes a snapshot taken at its own tick phase, so two
    /// replicas cover different parts of the same window; summing them all
    /// would weight each replica by how completely it reported rather than by
    /// how much traffic it sent. A lease written back after its window ended
    /// carries a timestamp past the window end, and that timestamp is the proof
    /// the counts are final. The newest completed window often fails this test
    /// for some replicas, so the lookback starts one window further back in
    /// practice; that is the intended behaviour, not a defect.
    fn final_outcomes(&self) -> OutcomeCounts {
        let mut counts = OutcomeCounts::default();
        for lease in self.leases.values() {
            // A replica that did not report is dropped from the estimate. It is
            // not read as zero: zero successes out of zero requests is evidence
            // of nothing, but it would still dilute the weights.
            let (Some(ok), Some(failed)) = (lease.ok, lease.failed) else {
                continue;
            };
            if lease.updated_at_unix_ms <= lease.expires_at_unix_ms {
                continue;
            }
            counts.requests = counts.requests.saturating_add(ok).saturating_add(failed);
            counts.accepts = counts.accepts.saturating_add(ok);
        }
        counts
    }

    /// Record `lease` for `instance`. Returns whether the window changed, and
    /// therefore whether the shared state must be written back.
    fn publish(&mut self, instance: &str, lease: PersistedLease) -> bool {
        if self
            .leases
            .get(instance)
            .is_some_and(|existing| existing.matches_counts(&lease))
        {
            return false;
        }
        self.leases.insert(instance.to_string(), lease);
        true
    }
}

impl PersistedLease {
    /// Demand this lease signals to peers.
    ///
    /// A replica that consumed everything it was granted is **saturated** — we
    /// don't know how much more it would have used, so it counts as the full
    /// cluster burst. A replica that did not consume its full grant signals
    /// demand equal to its observed `acquire()` call rate.
    fn classified_demand(&self, burst: u64) -> u64 {
        if self.granted > 0 && self.consumed >= self.granted {
            burst
        } else {
            self.attempted.max(self.consumed)
        }
    }

    /// Whether two leases carry the same published values. Timestamps are
    /// excluded: re-stamping identical counts on every tick would dirty the
    /// shared state and force a needless OCC write.
    fn matches_counts(&self, other: &Self) -> bool {
        self.granted == other.granted
            && self.consumed == other.consumed
            && self.attempted == other.attempted
            && self.ok == other.ok
            && self.failed == other.failed
    }

    /// Raise every published count to at least `counts`, and mark the record
    /// final by re-stamping `updated_at_unix_ms`. Returns whether anything
    /// changed, and therefore whether the shared state must be written back.
    ///
    /// Called only for a window that has already ended. The stamp is what makes
    /// the counts usable: [`PersistedWindow::final_outcomes`] reads a lease only
    /// once its timestamp passes the window end, so a replica whose last
    /// in-window tick happened to publish the right numbers must still re-stamp
    /// them. Without that, a replica that went quiet before the roll would be
    /// dropped from the cluster estimate while its busier peers were kept —
    /// biasing the measured error rate toward whoever was still failing.
    fn merge_counts(&mut self, counts: WindowCounts, now: SystemTime) -> bool {
        let was_final = self.updated_at_unix_ms > self.expires_at_unix_ms;
        let ok = self.ok.unwrap_or(0);
        let failed = self.failed.unwrap_or(0);
        let raised = counts.attempted > self.attempted
            || counts.consumed > self.consumed
            || counts.ok > ok
            || counts.failed > failed;
        if !raised && was_final {
            return false;
        }
        self.attempted = self.attempted.max(counts.attempted);
        self.consumed = self.consumed.max(counts.consumed);
        // Both or neither, and only when there is something to report: the
        // estimate reads a lease only when the pair is present, and a replica
        // with nothing to say should stay absent rather than assert it saw none.
        if let Some((reported_ok, reported_failed)) = counts.reported_outcomes() {
            self.ok = Some(ok.max(reported_ok));
            self.failed = Some(failed.max(reported_failed));
        }
        self.updated_at_unix_ms = unix_millis(now);
        true
    }
}

/// Weighted demand over a span of windows: this replica's share against the
/// cluster total. Weighted sums exceed `u64` range, hence `u128`.
#[derive(Debug, Clone, Copy, Default)]
struct DemandSample {
    mine: u128,
    total: u128,
}

impl DemandSample {
    fn weighted(self, weight: u128) -> Self {
        Self {
            mine: self.mine * weight,
            total: self.total * weight,
        }
    }

    fn accumulate(&mut self, other: Self) {
        self.mine += other.mine;
        self.total += other.total;
    }

    /// Tokens this replica should claim out of `burst`.
    ///
    /// Every replica claims a slice proportional to its demand. With no
    /// persisted demand yet, `local_demand_hint` — this replica's in-progress
    /// `acquire()` count — decides instead of fair-share: a hot replica
    /// reclaims idle budget, an idle one holds only the minimum lease.
    fn demand_signal(self, burst: u64, local_demand_hint: u64) -> u64 {
        match (self.total, self.mine) {
            (0, _) if local_demand_hint > 0 => burst,
            (0, _) | (_, 0) => min_lease(burst),
            (total, mine) => u64::try_from((u128::from(burst) * mine) / total).unwrap_or(burst),
        }
    }
}

/// Counts a replica publishes for a window alongside its lease.
#[derive(Debug, Clone, Copy, Default)]
struct WindowCounts {
    consumed: u64,
    attempted: u64,
    /// Upstream requests that answered with a recorded success.
    ok: u64,
    /// Upstream requests that answered with a recorded failure.
    failed: u64,
}

impl WindowCounts {
    /// The outcome pair to publish, or `None` when this replica recorded
    /// nothing for the window.
    ///
    /// Published as a pair because the cluster estimate reads a lease only when
    /// both fields are present. A window with no recorded outcome is evidence of
    /// nothing — in static mode that is every window, and its leases stay
    /// exactly as they were before cluster adaptive throttling existed.
    fn reported_outcomes(self) -> Option<(u64, u64)> {
        (self.ok > 0 || self.failed > 0).then_some((self.ok, self.failed))
    }
}

/// Upstream outcomes one window holds evidence for, summed across replicas.
#[derive(Debug, Clone, Copy, Default)]
struct OutcomeCounts {
    /// Recorded upstream requests: successes plus failures. Not every consumed
    /// token appears here — a token may be spent on a request that never
    /// reaches the origin, or on one the classification discards (a
    /// non-retryable 4xx), which is why this cannot be derived from `consumed`.
    requests: u64,
    /// The subset of `requests` that succeeded.
    accepts: u64,
}

/// Time-weighted upstream outcomes over a span of completed windows, and the
/// admission coefficient they imply.
#[derive(Debug, Clone, Copy, Default)]
struct OutcomeSample {
    requests: f64,
    accepts: f64,
}

impl OutcomeSample {
    fn accumulate(&mut self, counts: OutcomeCounts, weight: f64) {
        #[expect(
            clippy::cast_precision_loss,
            reason = "counts far below 2^53 in any realistic window; the ratio is what matters"
        )]
        {
            self.requests += weight * counts.requests as f64;
            self.accepts += weight * counts.accepts as f64;
        }
    }

    /// The fraction of the configured cluster budget to admit, in `[0, 1]`.
    ///
    /// `k` is `1 / (1 - failure_threshold)`. Both `+1`s sit inside the fraction,
    /// which gives the three properties the single-node controller also has:
    /// at 100% success the ratio is `(k·r + 1) / (r + 1) >= 1` for `k > 1`, so a
    /// healthy origin is never throttled; the coefficient falls below 1 exactly
    /// when `accepts / requests < 1 / k`; and with no evidence at all it is
    /// exactly 1 rather than a division by zero.
    fn coefficient(self, k: f64) -> f64 {
        ((k * self.accepts + 1.0) / (self.requests + 1.0)).clamp(0.0, FULL_ADMISSION_COEFFICIENT)
    }

    /// Whether one more recorded outcome could flip the throttling state back.
    ///
    /// Decided on `k · accepts` against `requests`, never on the coefficient:
    /// the coefficient is clamped to 1, so a large success margin and a
    /// knife-edge recovery both read exactly 1.0. Mirrors the single-node
    /// controller so both log lines damp on the same rule.
    fn is_near_boundary(self, k: f64, throttling: bool) -> bool {
        let weighted_accepts = k * self.accepts;
        if throttling {
            // One success adds a request and `k` weighted accepts.
            weighted_accepts >= self.requests + 1.0 - k
        } else {
            // One failure adds a request but no accepts.
            weighted_accepts < self.requests + 1.0
        }
    }
}

/// Final demand and consumption of a window that rolled before its tail was
/// published.
#[derive(Debug, Clone, Copy)]
struct PendingPublish {
    window_id: u64,
    counts: WindowCounts,
}

/// Cluster adaptive parameters for one leased bucket.
///
/// Absent in static mode, where the coefficient is always
/// [`FULL_ADMISSION_COEFFICIENT`] and the effective budget is the configured
/// one.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeasedAdaptiveConfig {
    /// `1 / (1 - failure_threshold)`. The weighted accept count is multiplied by
    /// this, so the coefficient falls below 1 exactly when the error rate passes
    /// the configured threshold.
    pub k: f64,
    /// The configured failure threshold, quoted in the throttling log line.
    pub failure_threshold: f64,
    /// Decay half-life, in whole windows. One window is the smallest unit of
    /// time the shared file records, so a shorter half-life cannot be expressed;
    /// the caller quantises `rate_control_window` against `refresh_interval` and
    /// logs the rounded value.
    pub half_life_windows: u64,
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
    /// Cluster adaptive throttling parameters, or `None` in static mode.
    pub adaptive: Option<LeasedAdaptiveConfig>,
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
            .field("adaptive", &self.adaptive)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
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
    /// The cluster admission coefficient of the current window, as
    /// [`f64::to_bits`]. `None` in static mode, encoded as
    /// [`NO_COEFFICIENT_BITS`] so a mode that cannot throttle reports no series
    /// rather than a constant `1` that reads like a live measurement.
    adaptive_admission_ratio_bits: AtomicU64,
    /// The cluster budget of the current window after the coefficient.
    cluster_effective_burst: AtomicU64,
}

/// The encoding [`LeasedBucketMetrics::adaptive_admission_ratio_bits`] uses for
/// "static mode, no coefficient". A NaN payload, which no real coefficient can
/// take: the coefficient is clamped to `[0, 1]`.
const NO_COEFFICIENT_BITS: u64 = u64::MAX;

impl Default for LeasedBucketMetrics {
    /// All counters at zero, and the coefficient absent: a bucket reports no
    /// admission ratio until a lease refresh establishes one.
    fn default() -> Self {
        Self {
            lease_granted: AtomicU64::new(0),
            cluster_budget_remaining: AtomicU64::new(0),
            last_lease_acquire_micros: AtomicU64::new(0),
            lease_acquire_conflicts_total: AtomicU64::new(0),
            fail_closed_total: AtomicU64::new(0),
            lease_refresh_errors_total: AtomicU64::new(0),
            adaptive_admission_ratio_bits: AtomicU64::new(NO_COEFFICIENT_BITS),
            cluster_effective_burst: AtomicU64::new(0),
        }
    }
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

    /// The cluster admission coefficient of the most recently leased window, in
    /// `[0, 1]`. `None` in static mode.
    #[must_use]
    pub fn adaptive_admission_ratio(&self) -> Option<f64> {
        match self.adaptive_admission_ratio_bits.load(Ordering::Relaxed) {
            NO_COEFFICIENT_BITS => None,
            bits => Some(f64::from_bits(bits)),
        }
    }

    /// The cluster budget of the most recently leased window, after the
    /// coefficient. Equal to the configured burst while the origin is healthy.
    #[must_use]
    pub fn cluster_effective_burst(&self) -> u64 {
        self.cluster_effective_burst.load(Ordering::Relaxed)
    }

    fn record_effective_burst(&self, effective_burst: u64, coefficient: Option<f64>) {
        self.cluster_effective_burst
            .store(effective_burst, Ordering::Relaxed);
        self.adaptive_admission_ratio_bits.store(
            coefficient.map_or(NO_COEFFICIENT_BITS, f64::to_bits),
            Ordering::Relaxed,
        );
    }

    /// Record the outcome of a successful lease acquisition.
    fn record_lease(&self, granted: u64, cluster_budget_remaining: u64, elapsed: Duration) {
        self.lease_granted.store(granted, Ordering::Relaxed);
        self.cluster_budget_remaining
            .store(cluster_budget_remaining, Ordering::Relaxed);
        self.last_lease_acquire_micros.store(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
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
    /// Upstream requests in the current window that answered with a recorded
    /// success, and with a recorded failure. Published to peers, who derive the
    /// cluster admission coefficient from the whole fleet's counts.
    ///
    /// Tracked apart from `consumed_this_window` because the two measure
    /// different things: a consumed token may never reach the origin (an
    /// acquire or connection timeout), or may buy a response the classification
    /// discards (a non-retryable 4xx).
    ok_this_window: u64,
    failed_this_window: u64,
    /// Pre-leased slot for the *upcoming* window. On window roll the contents
    /// are promoted into the current-window fields so consumers never wait
    /// for a lease at a window boundary.
    next_window_id: u64,
    granted_next_window: u64,
    consumed_next_window: u64,
    /// Counts latched at a window roll. The next refresh writes these back
    /// to the rolled-out window's lease record so peers see the full counts.
    /// Cleared on successful publish.
    pending_window_publish: Option<PendingPublish>,
    /// Local GCRA/TAT pacing state for the current window. A successful
    /// acquire advances this theoretical arrival time by
    /// `window_duration / granted_this_window`, spreading the leased tokens
    /// across the whole window instead of allowing an immediate burst.
    /// `None` until the first acquire of a window, and reset on every roll.
    ///
    /// `Instant`, not `SystemTime`: this measures elapsed time on one replica
    /// and is never shared, so it must not follow wall-clock adjustments. A
    /// backward step would stall a replica that still holds lease; a forward
    /// step would release the burst that pacing exists to prevent.
    pacing_tat: Option<Instant>,
    /// Highest window this replica holds a grant for. `None` until the first
    /// successful lease, which counts as expired.
    ///
    /// Deliberately *not* advanced by a window roll: once the clock passes
    /// this window and the store is unreachable, the replica has no budget
    /// left to spend and must fail closed.
    leased_through: Option<u64>,
    /// Set when the most recent lease attempt failed.
    last_attempt_failed: bool,
}

impl LeasedBucketInner {
    fn new(window_id: u64) -> Self {
        Self {
            current_window_id: window_id,
            granted_this_window: 0,
            consumed_this_window: 0,
            attempted_this_window: 0,
            ok_this_window: 0,
            failed_this_window: 0,
            next_window_id: window_id + 1,
            granted_next_window: 0,
            consumed_next_window: 0,
            pending_window_publish: None,
            pacing_tat: None,
            leased_through: None,
            last_attempt_failed: false,
        }
    }

    /// Counts to publish for the in-progress window.
    fn live_counts(&self) -> WindowCounts {
        WindowCounts {
            consumed: self.consumed_this_window,
            attempted: self.attempted_this_window,
            ok: self.ok_this_window,
            failed: self.failed_this_window,
        }
    }

    /// Record one upstream outcome against the window that is open now.
    fn register_outcome(&mut self, outcome: RequestOutcome) {
        match outcome {
            RequestOutcome::Success => {
                self.ok_this_window = self.ok_this_window.saturating_add(1);
            }
            RequestOutcome::Failure => {
                self.failed_this_window = self.failed_this_window.saturating_add(1);
            }
        }
    }

    /// Register one unit of demand, whether or not a token is ever granted.
    fn register_demand(&mut self) {
        self.attempted_this_window = self.attempted_this_window.saturating_add(1);
    }

    /// Roll over per-window state if the window has changed.
    ///
    /// Promotes the pre-leased next window into the current slot when the
    /// clock advances by exactly one window — eliminating the dead-zone that
    /// would otherwise occur while the persistence task fetches a fresh lease
    /// at each window boundary. If the clock has skipped by more than one
    /// window the pre-leased slot is discarded and both slots reset.
    fn roll_to(&mut self, now_window: u64) {
        if now_window == self.current_window_id {
            return;
        }
        self.latch_pending_publish();
        if now_window == self.next_window_id {
            self.promote_next();
        } else {
            self.reset_to(now_window);
        }
        // leased_through is preserved so the fail-closed check can see that
        // the clock has outrun the last lease.
    }

    /// Capture the final counts of the window being rolled out of, so the
    /// next refresh can publish them to peers. Counts registered between the
    /// last refresh tick and the roll would otherwise be lost.
    fn latch_pending_publish(&mut self) {
        if self.attempted_this_window > 0
            || self.consumed_this_window > 0
            || self.ok_this_window > 0
            || self.failed_this_window > 0
        {
            self.pending_window_publish = Some(PendingPublish {
                window_id: self.current_window_id,
                counts: self.live_counts(),
            });
        }
    }

    fn promote_next(&mut self) {
        self.current_window_id = self.next_window_id;
        self.granted_this_window = self.granted_next_window;
        self.consumed_this_window = self.consumed_next_window;
        self.start_window();
    }

    fn reset_to(&mut self, now_window: u64) {
        self.current_window_id = now_window;
        self.granted_this_window = 0;
        self.consumed_this_window = 0;
        self.start_window();
    }

    /// Tail shared by both roll paths: clear the demand and pacing counters,
    /// and open an empty pre-lease slot for the window after the current one.
    fn start_window(&mut self) {
        self.attempted_this_window = 0;
        self.ok_this_window = 0;
        self.failed_this_window = 0;
        self.pacing_tat = None;
        self.next_window_id = self.current_window_id + 1;
        self.granted_next_window = 0;
        self.consumed_next_window = 0;
    }

    /// Adopt the grants from a successful lease refresh.
    fn adopt_lease(&mut self, now_window: u64, lease: LeaseGrants) {
        self.roll_to(now_window);
        self.granted_this_window = lease.current;
        // The next-window id may already match if an earlier tick pre-leased
        // it; either way we adopt the latest grant value.
        self.next_window_id = now_window + 1;
        self.granted_next_window = lease.next;
        self.leased_through = Some(now_window + 1);
        self.last_attempt_failed = false;
    }

    /// Whether the store is failing and the lease has fully expired, so no
    /// further request may be admitted.
    fn is_fail_closed(&self, now_window: u64) -> bool {
        self.last_attempt_failed
            && self
                .leased_through
                .is_none_or(|leased_through| now_window > leased_through)
    }

    /// Take one token from the local lease if the GCRA pacer allows it at
    /// `now`.
    fn try_take_paced_token(&mut self, now: Instant, window: Duration) -> PacingStep {
        // Cap every wait at a fraction of the window so the caller re-checks
        // for a window roll even if no notify arrives (e.g. this replica is
        // the only consumer and the persistence task is slow).
        let max_wait = window / 4;
        if self.consumed_this_window >= self.granted_this_window {
            return PacingStep::Wait(max_wait); // Out of local lease.
        }

        if let Some(tat) = self.pacing_tat
            && let Some(remaining) = tat.checked_duration_since(now)
            && !remaining.is_zero()
        {
            return PacingStep::Wait(remaining.min(max_wait));
        }

        // The pacer has caught up with the clock, so the next theoretical
        // arrival is one interval from now.
        self.consumed_this_window += 1;
        let interval = pacing_interval(window, self.granted_this_window);
        self.pacing_tat = Some(now.checked_add(interval).unwrap_or(now));
        PacingStep::Granted
    }
}

/// Grants adopted from one successful lease refresh: the tokens for the
/// current window and the pre-lease for the next.
#[derive(Debug, Clone, Copy)]
struct LeaseGrants {
    current: u64,
    next: u64,
}

/// Outcome of one pacing attempt against the local lease.
enum PacingStep {
    /// A token was taken; the caller may proceed.
    Granted,
    /// No token is available yet. Re-check after this long.
    Wait(Duration),
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
    /// Debounces the throttling log so a reading that sits on the boundary does
    /// not oscillate. Separate from `inner` and synchronous: it is touched once
    /// per refresh tick, never on the acquire path.
    throttle_log: SyncMutex<PhaseChangeLog<ThrottleState>>,
}

impl LeasedBucket {
    pub fn new(config: LeasedBucketConfig) -> Arc<Self> {
        let object_state = Arc::new(
            ObjectState::new(Arc::clone(&config.store)).with_prefix(config.prefix.clone()),
        );
        let window_id = window_id_for(SystemTime::now(), config.window_duration);
        // Hold a boundary reading for one half-life before reporting it, the
        // same rule the single-node controller uses.
        let hold = config.window_duration.saturating_mul(
            u32::try_from(
                config
                    .adaptive
                    .map_or(1, |adaptive| adaptive.half_life_windows.max(1)),
            )
            .unwrap_or(u32::MAX),
        );
        Arc::new(Self {
            object_state,
            inner: Mutex::new(LeasedBucketInner::new(window_id)),
            notify: Notify::new(),
            metrics: Arc::new(LeasedBucketMetrics::default()),
            throttle_log: SyncMutex::new(PhaseChangeLog::new(ThrottleState::Healthy, hold)),
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

    /// The cluster admission coefficient in force, or `None` in static mode.
    pub fn admission_coefficient(&self) -> Option<f64> {
        self.metrics.adaptive_admission_ratio()
    }

    /// Whether the cluster budget is currently below the configured limit.
    pub fn is_throttling(&self) -> bool {
        self.admission_coefficient()
            .is_some_and(|ratio| ratio < FULL_ADMISSION_COEFFICIENT)
    }

    /// Record one upstream outcome against the window that is open now, for
    /// peers to read on their next refresh.
    ///
    /// Called from a synchronous path, so the lock is taken with `try_lock`:
    /// losing a count under contention is acceptable, because the coefficient
    /// reads a ratio and contention drops successes and failures alike.
    pub fn record_outcome(&self, outcome: RequestOutcome) {
        if self.config.adaptive.is_none() {
            return;
        }
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.roll_to(window_id_for(
                SystemTime::now(),
                self.config.window_duration,
            ));
            inner.register_outcome(outcome);
        }
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
            let now_window = window_id_for(SystemTime::now(), self.config.window_duration);
            let mut inner = self.inner.lock().await;
            inner.roll_to(now_window);
            inner.register_demand();
        }

        loop {
            // Two clocks, deliberately: `SystemTime` places us in a
            // cluster-wide window, `Instant` paces tokens within it.
            let now = SystemTime::now();
            let now_window = window_id_for(now, self.config.window_duration);

            let wait = {
                let mut inner = self.inner.lock().await;
                inner.roll_to(now_window);

                match inner.try_take_paced_token(Instant::now(), self.config.window_duration) {
                    PacingStep::Granted => return Ok(()),
                    PacingStep::Wait(wait) => {
                        if inner.is_fail_closed(now_window) {
                            self.metrics
                                .fail_closed_total
                                .fetch_add(1, Ordering::Relaxed);
                            return Err(Error::FailClosed {
                                origin: self.config.origin.clone(),
                            });
                        }
                        wait
                    }
                }
            };

            let _ = tokio::time::timeout(wait, self.notify.notified()).await;
        }
    }

    /// Refresh the lease for the current window **and** pre-lease the next
    /// window. Called by the persistence task on a timer and at
    /// controller-build time.
    ///
    /// Pre-leasing the next window keeps S3 latency off the critical path:
    /// when the window rolls, [`Self::acquire`] promotes the pre-leased slot
    /// into the current slot via [`LeasedBucketInner::roll_to`] without
    /// blocking on a fresh OCC round-trip.
    pub async fn refresh_lease(self: &Arc<Self>) -> Result<()> {
        let started = std::time::Instant::now();
        let now = SystemTime::now();
        let window = self.config.window_duration;
        let now_window = window_id_for(now, window);

        let (live_counts, pending_publish) = {
            let mut inner = self.inner.lock().await;
            (inner.live_counts(), inner.pending_window_publish.take())
        };

        for attempt in 0..MAX_LEASE_RETRIES {
            // State written under an older schema is treated as empty; the
            // previous PR was never shipped so no migration is required.
            let mut state = match self.read_state().await {
                Ok(state) => state
                    .filter(PersistedRateControlState::is_current_schema)
                    .unwrap_or_else(|| PersistedRateControlState::fresh(window)),
                Err(e) => {
                    self.note_failure();
                    return Err(e);
                }
            };
            state.stamp(now, window);

            // Publish any counts latched at a window roll between refresh
            // ticks. Best-effort: if the rolled-out window has already been
            // pruned from state, drop the pending values and move on.
            //
            // This write-back is the only thing that marks a window's counts
            // final, and the cluster coefficient reads nothing else, so it has
            // to force a write of its own. Folding it into the grant's dirty
            // flag would drop a whole window of upstream outcomes whenever
            // neither the current nor the pre-leased window changed.
            let mut tail_published = false;
            if let Some(limiter) = state.limiter_mut(&self.config.limiter_key) {
                limiter.retain_recent_windows(now_window);

                if let Some(pending) = pending_publish
                    && let Some(lease) = limiter
                        .window_mut(pending.window_id)
                        .and_then(|window| window.lease_mut(&self.config.instance_id))
                {
                    tail_published = lease.merge_counts(pending.counts, now);
                }
            }

            let current = self.process_window(
                &mut state,
                now_window,
                now,
                live_counts,
                live_counts.attempted,
            );
            let next = self.process_window(
                &mut state,
                now_window + 1,
                now,
                WindowCounts::default(),
                live_counts.attempted,
            );

            let dirty = tail_published || current.dirty || next.dirty;
            if dirty {
                match self.write_state(state).await {
                    Ok(WriteOutcome::Written) => {}
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
                        continue;
                    }
                    Err(e) => {
                        self.note_failure();
                        return Err(e);
                    }
                }
            }

            {
                let mut inner = self.inner.lock().await;
                inner.adopt_lease(
                    now_window,
                    LeaseGrants {
                        current: current.granted,
                        next: next.granted,
                    },
                );
            }
            self.metrics.record_lease(
                current.granted,
                current.budget_remaining_after,
                started.elapsed(),
            );
            self.report_throttle(current.throttle);
            if dirty {
                // Only a write can have changed what waiters are owed.
                self.notify.notify_waiters();
            }
            return Ok(());
        }

        unreachable!("loop body always returns within MAX_LEASE_RETRIES iterations")
    }

    /// Compute and apply this replica's lease for a single window inside the
    /// shared persisted state. Mutates `state` in place; returns the resulting
    /// grant, the post-write `budget_remaining`, and whether the state was
    /// modified (and therefore must be written back).
    fn process_window(
        &self,
        state: &mut PersistedRateControlState,
        window_id: u64,
        now: SystemTime,
        counts: WindowCounts,
        local_demand_hint: u64,
    ) -> WindowOutcome {
        let instance = self.config.instance_id.as_str();
        let burst = self.config.burst_per_window;
        let window_ms = duration_millis_u64(self.config.window_duration);
        let window_end_ms = (window_id + 1).saturating_mul(window_ms);
        let now_window = window_id_for(now, self.config.window_duration);

        let limiter = state.limiter_entry(&self.config.limiter_key, burst);

        // The budget this window is leased against. First write wins, one level
        // above the grant: the replica that creates a window fixes the budget
        // every replica then leases against. Two replicas can read the file at
        // different moments — before and after a late tail publish, say — and
        // would otherwise derive different budgets from the same window and
        // break `Σ granted <= budget`.
        let peer_burst = limiter.effective_burst_of(window_id);
        // This replica's own reading of the shared counts: `Some` exactly in
        // adaptive mode. Taken even when a peer already fixed the budget, since
        // it costs a few map lookups and is what tells this replica's log line
        // how firm the state is — so every replica reports for itself, not only
        // the one that happened to create the window.
        let reading = self.config.adaptive.map(|adaptive| {
            let outcomes = limiter.ewma_outcomes(
                window_id,
                OUTCOME_EWMA_LOOKBACK_WINDOWS,
                adaptive.half_life_windows,
            );
            (adaptive, outcomes)
        });
        let effective_burst = peer_burst.unwrap_or_else(|| {
            reading.map_or(burst, |(adaptive, outcomes)| {
                scale_burst(burst, outcomes.coefficient(adaptive.k))
            })
        });

        // Read the smoothed demand signal before taking a mutable borrow on
        // the window below.
        //
        // `classified_demand` measures want against the *configured* burst, not
        // the throttled one — a saturated replica wants at least a full budget
        // whatever the cluster is currently allowed. The share it wins, and the
        // clamps around it, are of the effective budget: that is the permission
        // actually being divided up.
        let demand = limiter
            .ewma_demand(instance, now_window, burst, DEMAND_EWMA_LOOKBACK_WINDOWS)
            .demand_signal(effective_burst, local_demand_hint)
            .max(min_lease(effective_burst))
            .min(max_lease_per_replica(effective_burst));

        let window = limiter.window_entry(window_id, effective_burst);
        // `replace` rather than `get_or_insert`: the value is already this
        // window's own, so this only reports whether *we* fixed it, which the
        // write-back decision below needs.
        let budget_newly_fixed = window.effective_burst.replace(effective_burst).is_none();

        // Only the current window can hold expired leases; a future window's
        // lease cannot have expired yet.
        window.drop_expired(now);
        window.recompute_budget_remaining(effective_burst);

        let my_existing = window.granted_for(instance);
        let max_possible_for_me =
            effective_burst.saturating_sub(window.granted_by_others(instance));

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

        // Always re-publish so peers see updated `consumed`/`attempted`/
        // `ok`/`failed` counters even when our `granted` value is unchanged.
        let reported = counts.reported_outcomes();
        let published = window.publish(
            instance,
            PersistedLease {
                granted: new_grant,
                consumed: counts.consumed,
                attempted: counts.attempted,
                ok: reported.map(|(ok, _)| ok),
                failed: reported.map(|(_, failed)| failed),
                expires_at_unix_ms: window_end_ms,
                updated_at_unix_ms: unix_millis(now),
            },
        );

        window.recompute_budget_remaining(effective_burst);

        WindowOutcome {
            granted: new_grant,
            budget_remaining_after: window.budget_remaining,
            throttle: reading.map(|(adaptive, outcomes)| ClusterThrottle {
                effective_burst,
                // Reported from the budget actually in force, not from this
                // replica's own arithmetic, so every replica leasing a window
                // reports the same ratio.
                admission_ratio: admission_ratio(effective_burst, burst),
                near_boundary: outcomes.is_near_boundary(adaptive.k, effective_burst < burst),
            }),
            dirty: published || budget_newly_fixed,
        }
    }

    /// Publish the adaptive state of the window just leased, and log a change
    /// of state.
    ///
    /// Every replica reports for itself. They share one budget, so their lines
    /// agree — and a replica that adopted a peer's budget still reports, rather
    /// than only the replica that happened to create the window.
    fn report_throttle(&self, throttle: Option<ClusterThrottle>) {
        let Some(throttle) = throttle else {
            return; // Static mode: nothing to report, and no series to emit.
        };
        self.metrics
            .record_effective_burst(throttle.effective_burst, Some(throttle.admission_ratio));

        let changed = self.throttle_log.lock().observe(
            throttle.state(),
            tokio::time::Instant::now(),
            throttle.damping(),
        );
        if let Some(state) = changed {
            state.report(
                &self.config.origin,
                self.config
                    .adaptive
                    .map_or(0.0, |adaptive| adaptive.failure_threshold),
            );
        }
    }

    fn note_failure(&self) {
        self.metrics
            .lease_refresh_errors_total
            .fetch_add(1, Ordering::Relaxed);
        // Mark inner state as failing so acquire() can fail closed once the
        // lease expires. Best-effort: `try_lock` keeps this synchronous, and
        // a contended lock means another lease attempt is already in flight
        // and will set the flag itself.
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
    /// The adaptive state of the window that was just leased, or `None` in
    /// static mode.
    throttle: Option<ClusterThrottle>,
    /// Whether the persisted state was modified and must therefore be written
    /// back. Two reasons we'd skip a write: (a) we already had a lease at the
    /// desired size in this window from a previous tick, or (b) demand is
    /// zero. Combined-window dirty status drives the OCC write decision.
    dirty: bool,
}

/// The adaptive state of one leased window: what the cluster is allowed to send
/// and how confident the reading behind it is.
#[derive(Debug, Clone, Copy)]
struct ClusterThrottle {
    /// The cluster budget for the window after the coefficient.
    effective_burst: u64,
    /// `effective_burst / burst_per_window`, in `[0, 1]`.
    admission_ratio: f64,
    /// Whether one more recorded outcome could flip the state back, so the log
    /// has to hold the reading for a window before reporting it.
    near_boundary: bool,
}

impl ClusterThrottle {
    fn state(self) -> ThrottleState {
        if self.admission_ratio >= FULL_ADMISSION_COEFFICIENT {
            ThrottleState::Healthy
        } else {
            ThrottleState::Throttling
        }
    }

    fn damping(self) -> Damping {
        if self.near_boundary {
            Damping::AfterHold
        } else {
            Damping::Immediate
        }
    }
}

/// The cluster budget for one window after the adaptive coefficient.
///
/// Floored at one: the cluster always sends at least one request per window,
/// whatever the error rate, so recovery is always tested and the coefficient can
/// rise again. Capped at the configured burst, because adaptive control is a
/// modifier on a static limit and never raises it.
fn scale_burst(burst: u64, coefficient: f64) -> u64 {
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "burst is a per-window request count; the coefficient is clamped to [0, 1], so the product is non-negative and well within u64"
    )]
    let scaled = (burst as f64 * coefficient).round() as u64;
    scaled.clamp(1, burst.max(1))
}

/// The fraction of the configured budget a window is allowed to spend.
fn admission_ratio(effective_burst: u64, burst: u64) -> f64 {
    if burst == 0 {
        return FULL_ADMISSION_COEFFICIENT;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "per-window request counts; the ratio is what matters, not the last bit"
    )]
    let ratio = effective_burst as f64 / burst as f64;
    ratio.clamp(0.0, FULL_ADMISSION_COEFFICIENT)
}

/// The window containing `now`.
///
/// Both operands are truncated to whole milliseconds before dividing. Window
/// ids are JSON map keys in the shared file, so this truncation is part of the
/// wire contract: dividing at finer resolution would place replicas of
/// different versions in different windows.
fn window_id_for(now: SystemTime, window: Duration) -> u64 {
    unix_millis(now) / duration_millis_u64(window).max(1)
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

/// Spacing between paced tokens: one window divided by the tokens leased for
/// it. An unleased replica paces at the full window length.
fn pacing_interval(window: Duration, granted: u64) -> Duration {
    let interval = if granted == 0 {
        window
    } else {
        window / u32::try_from(granted).unwrap_or(u32::MAX)
    };
    interval.max(Duration::from_nanos(1))
}

/// Unix-epoch milliseconds, the encoding the shared JSON file uses. The only
/// place time crosses from the typed domain into the persisted one.
///
/// A clock before the epoch reads as 0, matching the persisted encoding's
/// floor rather than failing a lease refresh.
fn unix_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, duration_millis_u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;
    use object_store::memory::InMemory;
    use serde_json::Value;

    /// A populated state covering every persisted field: a limiter with one
    /// window and two leases, one reporting counts and one idle.
    fn wire_format_fixture() -> PersistedRateControlState {
        let mut leases = HashMap::new();
        leases.insert(
            "replica-a".to_string(),
            PersistedLease {
                granted: 7,
                consumed: 5,
                attempted: 12,
                ok: Some(3),
                failed: Some(1),
                expires_at_unix_ms: 1_700_000_001_000,
                // Past the window end: this replica's counts are final.
                updated_at_unix_ms: 1_700_000_001_500,
            },
        );
        leases.insert(
            "replica-b".to_string(),
            PersistedLease {
                granted: 2,
                consumed: 0,
                attempted: 0,
                // An idle replica that has not reported: absent on the wire,
                // and dropped from the error-rate estimate rather than read as
                // zero.
                ok: None,
                failed: None,
                expires_at_unix_ms: 1_700_000_001_000,
                updated_at_unix_ms: 1_700_000_000_500,
            },
        );

        let mut windows = HashMap::new();
        windows.insert(
            "1700000000".to_string(),
            PersistedWindow {
                effective_burst: Some(9),
                budget_remaining: 1,
                leases,
            },
        );

        let mut limiters = HashMap::new();
        limiters.insert(
            "requests_per_second:burst=10:replenish_ns=100000000".to_string(),
            PersistedLimiter {
                burst_per_window: 10,
                windows,
            },
        );

        PersistedRateControlState {
            schema_version: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
            updated_at_unix_ms: 1_700_000_000_500,
            window_ms: 1_000,
            limiters,
        }
    }

    /// Serialize with every object's keys sorted. The persisted types are
    /// `HashMap`-backed and this workspace builds `serde_json` with
    /// `preserve_order`, so unsorted output would vary run to run.
    fn canonical_json(state: &PersistedRateControlState) -> String {
        fn sort_keys(value: Value) -> Value {
            match value {
                Value::Object(map) => {
                    let mut entries: Vec<(String, Value)> = map.into_iter().collect();
                    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
                    Value::Object(
                        entries
                            .into_iter()
                            .map(|(key, value)| (key, sort_keys(value)))
                            .collect(),
                    )
                }
                Value::Array(items) => Value::Array(items.into_iter().map(sort_keys).collect()),
                scalar => scalar,
            }
        }

        let value = serde_json::to_value(state).expect("state serializes to JSON");
        serde_json::to_string_pretty(&sort_keys(value)).expect("JSON pretty-prints")
    }

    /// Pins the on-disk JSON. Replicas of different runtime versions read and
    /// write one shared file per origin, so any change to a field name, a
    /// field's presence or a map key encoding breaks a mixed-version cluster.
    /// Update this snapshot only alongside a `schema_version` bump.
    #[test]
    fn persisted_state_json_is_stable() {
        assert_snapshot!(canonical_json(&wire_format_fixture()), @r###"
        {
          "limiters": {
            "requests_per_second:burst=10:replenish_ns=100000000": {
              "burst_per_window": 10,
              "windows": {
                "1700000000": {
                  "budget_remaining": 1,
                  "effective_burst": 9,
                  "leases": {
                    "replica-a": {
                      "attempted": 12,
                      "consumed": 5,
                      "expires_at_unix_ms": 1700000001000,
                      "failed": 1,
                      "granted": 7,
                      "ok": 3,
                      "updated_at_unix_ms": 1700000001500
                    },
                    "replica-b": {
                      "attempted": 0,
                      "consumed": 0,
                      "expires_at_unix_ms": 1700000001000,
                      "granted": 2,
                      "updated_at_unix_ms": 1700000000500
                    }
                  }
                }
              }
            }
          },
          "schema_version": 4,
          "updated_at_unix_ms": 1700000000500,
          "window_ms": 1000
        }
        "###);
    }

    #[test]
    fn persisted_state_round_trips() {
        let state = wire_format_fixture();
        let encoded = serde_json::to_string(&state).expect("state serializes");
        let decoded: PersistedRateControlState =
            serde_json::from_str(&encoded).expect("state deserializes");
        assert_eq!(state, decoded);
    }

    /// `consumed`, `attempted`, `ok`, `failed` and `effective_burst` are all
    /// optional on the wire. A file written by a replica that omits them must
    /// still load: the counts at zero, and the two `Option`s absent rather than
    /// `Some(0)` — "did not report" is not "reported none".
    #[test]
    fn persisted_state_reads_lease_without_optional_counts() {
        const ON_DISK: &str = r#"{
            "schema_version": 4,
            "updated_at_unix_ms": 1700000000500,
            "window_ms": 1000,
            "limiters": {
                "requests_per_second:burst=10:replenish_ns=100000000": {
                    "burst_per_window": 10,
                    "windows": {
                        "1700000000": {
                            "budget_remaining": 3,
                            "leases": {
                                "replica-a": {
                                    "granted": 7,
                                    "expires_at_unix_ms": 1700000001000,
                                    "updated_at_unix_ms": 1700000000500
                                }
                            }
                        }
                    }
                }
            }
        }"#;

        let state: PersistedRateControlState =
            serde_json::from_str(ON_DISK).expect("older v3 file deserializes");

        assert_eq!(
            state.schema_version,
            PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION
        );
        assert_eq!(state.window_ms, 1000);

        let limiter = state
            .limiters
            .get("requests_per_second:burst=10:replenish_ns=100000000")
            .expect("limiter present");
        assert_eq!(limiter.burst_per_window, 10);

        let window = limiter.windows.get("1700000000").expect("window present");
        assert_eq!(window.budget_remaining, 3);

        let lease = window.leases.get("replica-a").expect("lease present");
        assert_eq!(lease.granted, 7);
        assert_eq!(lease.expires_at_unix_ms, 1_700_000_001_000);
        assert_eq!(lease.updated_at_unix_ms, 1_700_000_000_500);
        assert_eq!(lease.consumed, 0, "missing `consumed` defaults to zero");
        assert_eq!(lease.attempted, 0, "missing `attempted` defaults to zero");
        assert_eq!(lease.ok, None, "missing `ok` is absent, not zero");
        assert_eq!(lease.failed, None, "missing `failed` is absent, not zero");
        assert_eq!(
            window.effective_burst, None,
            "a window written before cluster adaptive throttling fixes no budget"
        );
    }

    /// A file written under schema 3 is discarded, not read field by field. The
    /// reader tests for an exact schema match, so a v3 window cannot leak into a
    /// v4 replica's budget arithmetic.
    #[test]
    fn schema_three_state_is_not_current() {
        const ON_DISK: &str = r#"{
            "schema_version": 3,
            "updated_at_unix_ms": 1700000000500,
            "window_ms": 1000,
            "limiters": {
                "requests_per_second:burst=10:replenish_ns=100000000": {
                    "burst_per_window": 10,
                    "windows": {
                        "1700000000": {
                            "budget_remaining": 3,
                            "leases": {
                                "replica-a": {
                                    "granted": 7,
                                    "expires_at_unix_ms": 1700000001000,
                                    "updated_at_unix_ms": 1700000000500
                                }
                            }
                        }
                    }
                }
            }
        }"#;

        let state: PersistedRateControlState =
            serde_json::from_str(ON_DISK).expect("a v3 file still parses");
        assert!(
            !state.is_current_schema(),
            "a v3 file must be discarded, so its windows never reach the budget arithmetic"
        );
    }

    /// `lookback_windows` bounds how far back the EWMA reaches, and the
    /// weights halve with age. A lookback of 1 sees only the window that just
    /// completed; a wider lookback picks up the older one at half the weight.
    #[test]
    fn ewma_demand_honours_lookback_windows() {
        let lease = |granted, attempted| PersistedLease {
            granted,
            consumed: 0,
            attempted,
            ok: None,
            failed: None,
            expires_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let window = |attempted| PersistedWindow {
            effective_burst: None,
            budget_remaining: 0,
            leases: HashMap::from([("a".to_string(), lease(0, attempted))]),
        };

        // Windows 8 and 9 completed; window 10 is in progress.
        let limiter = PersistedLimiter {
            burst_per_window: 100,
            windows: HashMap::from([("8".to_string(), window(1)), ("9".to_string(), window(4))]),
        };

        // Lookback 1: only window 9, at weight 1.
        assert_eq!(limiter.ewma_demand("a", 10, 100, 1).mine, 4);
        // Lookback 2: window 9 at weight 2, window 8 at weight 1.
        assert_eq!(limiter.ewma_demand("a", 10, 100, 2).mine, 4 * 2 + 1);
        // The in-progress window never contributes.
        assert_eq!(limiter.ewma_demand("a", 9, 100, 1).mine, 1);
    }

    /// A failure threshold of 50% (`k = 2`), the value the cluster tests below
    /// use: throttling starts above a 50% error rate.
    const HALF_THRESHOLD: f64 = 0.5;
    const K_AT_HALF_THRESHOLD: f64 = 2.0;

    fn adaptive_config(half_life_windows: u64) -> LeasedAdaptiveConfig {
        LeasedAdaptiveConfig {
            k: K_AT_HALF_THRESHOLD,
            failure_threshold: HALF_THRESHOLD,
            half_life_windows,
        }
    }

    /// A lease whose counts are final: its timestamp is past the window end.
    fn reported_lease(window_id: u64, window_ms: u64, ok: u64, failed: u64) -> PersistedLease {
        let end_ms = (window_id + 1) * window_ms;
        PersistedLease {
            granted: 0,
            consumed: 0,
            attempted: 0,
            ok: Some(ok),
            failed: Some(failed),
            expires_at_unix_ms: end_ms,
            updated_at_unix_ms: end_ms + 1,
        }
    }

    fn outcome_window(leases: Vec<(&str, PersistedLease)>) -> PersistedWindow {
        PersistedWindow {
            effective_burst: None,
            budget_remaining: 0,
            leases: leases
                .into_iter()
                .map(|(id, lease)| (id.to_string(), lease))
                .collect(),
        }
    }

    /// One limiter holding `windows` keyed by window id.
    fn outcome_limiter(windows: Vec<(u64, PersistedWindow)>) -> PersistedLimiter {
        PersistedLimiter {
            burst_per_window: 10,
            windows: windows
                .into_iter()
                .map(|(id, window)| (id.to_string(), window))
                .collect(),
        }
    }

    /// `merge_counts` only ever raises a count, so a stale tick that reports
    /// less than a peer already published cannot erase evidence.
    #[test]
    fn merge_counts_raises_outcome_counts_and_never_lowers_them() {
        let now = UNIX_EPOCH + Duration::from_secs(5);
        let mut lease = reported_lease(0, 1_000, 4, 2);

        assert!(lease.merge_counts(
            WindowCounts {
                consumed: 0,
                attempted: 0,
                ok: 7,
                failed: 3,
            },
            now,
        ));
        assert_eq!((lease.ok, lease.failed), (Some(7), Some(3)));

        assert!(!lease.merge_counts(
            WindowCounts {
                consumed: 0,
                attempted: 0,
                ok: 1,
                failed: 1,
            },
            now,
        ));
        assert_eq!(
            (lease.ok, lease.failed),
            (Some(7), Some(3)),
            "a lower report must not lower the published counts"
        );
    }

    /// The write-back that follows a window roll is the only thing that marks a
    /// window final, so it must re-stamp even when it raises nothing — or a
    /// replica that went quiet before the roll drops out of the estimate.
    #[test]
    fn merge_counts_stamps_a_window_whose_counts_were_already_published() {
        let window_ms = 1_000;
        let end_ms = window_ms; // Window 0 ends at 1000ms.
        let mut lease = PersistedLease {
            granted: 2,
            consumed: 2,
            attempted: 2,
            ok: Some(2),
            failed: Some(0),
            expires_at_unix_ms: end_ms,
            // Published mid-window by the last refresh tick before the roll.
            updated_at_unix_ms: end_ms - 100,
        };

        let counts = WindowCounts {
            consumed: 2,
            attempted: 2,
            ok: 2,
            failed: 0,
        };
        assert!(
            lease.merge_counts(counts, UNIX_EPOCH + Duration::from_millis(end_ms + 50)),
            "the stamp itself is a change that has to be written back"
        );
        assert!(lease.updated_at_unix_ms > lease.expires_at_unix_ms);

        // Idempotent once final.
        assert!(!lease.merge_counts(counts, UNIX_EPOCH + Duration::from_millis(end_ms + 90)));
    }

    /// A tick that changed only the outcome counts still has to be written, or
    /// the evidence never reaches the peers that need it.
    #[test]
    fn matches_counts_separates_leases_that_differ_only_in_outcomes() {
        let lease = reported_lease(0, 1_000, 4, 1);
        let mut with_more_failures = lease.clone();
        with_more_failures.failed = Some(2);

        assert!(lease.matches_counts(&lease.clone()));
        assert!(!lease.matches_counts(&with_more_failures));
    }

    /// At a half-life of one window the weights halve with each window of age:
    /// 1, 1/2, 1/4, 1/8, 1/16 over a five-window lookback.
    #[test]
    fn ewma_outcomes_halves_the_weight_every_half_life() {
        // Windows 5..=9 completed, each holding exactly one request. Window 10
        // is the target.
        let limiter = outcome_limiter(
            (5..=9)
                .map(|id| {
                    (
                        id,
                        outcome_window(vec![("a", reported_lease(id, 1_000, 1, 0))]),
                    )
                })
                .collect(),
        );

        let sample = limiter.ewma_outcomes(10, 5, 1);
        let expected = 1.0 + 0.5 + 0.25 + 0.125 + 0.0625;
        assert!(
            (sample.requests - expected).abs() < 1e-9,
            "expected {expected}, got {}",
            sample.requests
        );
        assert!((sample.accepts - expected).abs() < 1e-9);

        // A five-window half-life flattens the weights toward 1.
        let flat = limiter.ewma_outcomes(10, 5, 5);
        assert!(flat.requests > sample.requests);
    }

    /// The decay is anchored on the window being leased, not on a clock read.
    /// Two replicas whose clocks disagree must still weight the same windows the
    /// same way, or they size their grants against different budgets.
    #[test]
    fn ewma_outcomes_anchors_on_the_target_window() {
        let limiter = outcome_limiter(vec![
            (
                8,
                outcome_window(vec![("a", reported_lease(8, 1_000, 1, 0))]),
            ),
            (
                9,
                outcome_window(vec![("a", reported_lease(9, 1_000, 4, 0))]),
            ),
        ]);

        // Target 10: window 9 at weight 1, window 8 at weight 1/2.
        assert!((limiter.ewma_outcomes(10, 5, 1).requests - (4.0 + 0.5)).abs() < 1e-9);
        // Target 11, same file and no clock involved: both windows age by one.
        assert!((limiter.ewma_outcomes(11, 5, 1).requests - (2.0 + 0.25)).abs() < 1e-9);
    }

    /// A replica that did not report is dropped from the estimate. Reading it as
    /// zero would dilute the ratio with evidence nobody produced.
    #[test]
    fn ewma_outcomes_skips_a_lease_that_did_not_report() {
        let mut silent = reported_lease(9, 1_000, 0, 0);
        silent.ok = None;
        silent.failed = None;

        let limiter = outcome_limiter(vec![(
            9,
            outcome_window(vec![("a", reported_lease(9, 1_000, 1, 1)), ("b", silent)]),
        )]);

        let sample = limiter.ewma_outcomes(10, 5, 1);
        assert!(
            (sample.requests - 2.0).abs() < 1e-9,
            "only replica a counts"
        );
        assert!((sample.accepts - 1.0).abs() < 1e-9);
    }

    /// A lease still stamped inside its own window is a partial report: two
    /// replicas publish at different tick phases, so summing partial reports
    /// weights each replica by how completely it reported rather than by how
    /// much traffic it sent.
    #[test]
    fn ewma_outcomes_skips_a_lease_whose_window_has_not_been_written_back() {
        let mut mid_window = reported_lease(9, 1_000, 5, 5);
        mid_window.updated_at_unix_ms = mid_window.expires_at_unix_ms; // Not past the end.

        let limiter = outcome_limiter(vec![(
            9,
            outcome_window(vec![
                ("a", reported_lease(9, 1_000, 1, 0)),
                ("b", mid_window),
            ]),
        )]);

        let sample = limiter.ewma_outcomes(10, 5, 1);
        assert!(
            (sample.requests - 1.0).abs() < 1e-9,
            "only the final lease counts"
        );
    }

    /// No evidence is not a 100% error rate. An empty lookback must read as a
    /// healthy origin rather than divide by zero.
    #[test]
    fn coefficient_of_an_empty_sample_is_full_admission() {
        let limiter = outcome_limiter(vec![]);
        let coefficient = limiter
            .ewma_outcomes(10, 5, 1)
            .coefficient(K_AT_HALF_THRESHOLD);
        assert!((coefficient - FULL_ADMISSION_COEFFICIENT).abs() < f64::EPSILON);
    }

    /// A healthy origin is never throttled, and an error rate exactly at the
    /// threshold is still healthy: throttling starts strictly above it.
    #[test]
    fn coefficient_throttles_only_above_the_failure_threshold() {
        let all_ok = OutcomeSample {
            requests: 100.0,
            accepts: 100.0,
        };
        assert!(
            (all_ok.coefficient(K_AT_HALF_THRESHOLD) - FULL_ADMISSION_COEFFICIENT).abs()
                < f64::EPSILON
        );

        // Exactly at a 50% error rate: k * accepts == requests, so the ratio is
        // exactly 1.
        let at_threshold = OutcomeSample {
            requests: 100.0,
            accepts: 50.0,
        };
        assert!(
            (at_threshold.coefficient(K_AT_HALF_THRESHOLD) - FULL_ADMISSION_COEFFICIENT).abs()
                < f64::EPSILON
        );

        // Above it, the coefficient settles at k * success rate.
        let above_threshold = OutcomeSample {
            requests: 100.0,
            accepts: 20.0,
        };
        let coefficient = above_threshold.coefficient(K_AT_HALF_THRESHOLD);
        assert!(coefficient < FULL_ADMISSION_COEFFICIENT);
        assert!(
            (coefficient - 0.406).abs() < 0.01,
            "expected about k * 0.2, got {coefficient}"
        );
    }

    /// The cluster always sends at least one request per window, whatever the
    /// error rate, so recovery is always tested.
    #[test]
    fn effective_burst_never_falls_below_one() {
        assert_eq!(scale_burst(10, 0.0), 1);
        assert_eq!(scale_burst(10, 0.01), 1);
        assert_eq!(scale_burst(1, 0.0), 1);
        // And never rises above the configured limit.
        assert_eq!(scale_burst(10, 1.0), 10);
        assert_eq!(scale_burst(10, 0.5), 5);
    }

    /// The budget of a window is fixed by whoever writes it first. Two replicas
    /// reading the file at different moments would otherwise derive different
    /// budgets from the same counts and break `sum(granted) <= budget`.
    #[tokio::test]
    async fn second_replica_adopts_the_effective_burst_of_the_first() {
        let store = Arc::new(InMemory::new());
        let window = Duration::from_millis(200);
        let mut cfg_a = config_for(10, "a", window);
        cfg_a.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_a.adaptive = Some(adaptive_config(1));
        let mut cfg_b = cfg_a.clone();
        cfg_b.instance_id = "b".to_string();

        let a = LeasedBucket::new(cfg_a);
        let b = LeasedBucket::new(cfg_b);

        a.refresh_lease().await.expect("a leases");
        // Overwrite the budget A fixed, to a value B could not have calculated.
        let window_id = window_id_for(SystemTime::now(), window);
        let mut state = a.read_state().await.expect("read").expect("state exists");
        let limiter = state
            .limiter_mut(&a.config.limiter_key)
            .expect("limiter present");
        limiter
            .window_mut(window_id)
            .expect("current window present")
            .effective_burst = Some(3);
        a.write_state(state).await.expect("write");

        b.refresh_lease().await.expect("b leases");

        let adopted = b
            .read_state()
            .await
            .expect("read")
            .expect("state exists")
            .limiters
            .get(&b.config.limiter_key)
            .and_then(|limiter| limiter.windows.get(&window_id.to_string()))
            .and_then(|window| window.effective_burst);
        assert_eq!(
            adopted,
            Some(3),
            "B must lease against the budget A fixed, not recalculate one"
        );
        assert!(
            b.metrics.lease_granted() <= 3,
            "B leased {} against a budget of 3",
            b.metrics.lease_granted()
        );
    }

    /// End to end through the shared file: a failing cluster leases a smaller
    /// budget, and both replicas agree on it.
    #[tokio::test]
    async fn a_failing_origin_shrinks_the_cluster_budget_for_every_replica() {
        let store = Arc::new(InMemory::new());
        let window = Duration::from_millis(150);
        let mut cfg_a = config_for(20, "a", window);
        cfg_a.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg_a.adaptive = Some(adaptive_config(1));
        let mut cfg_b = cfg_a.clone();
        cfg_b.instance_id = "b".to_string();

        let a = LeasedBucket::new(cfg_a);
        let b = LeasedBucket::new(cfg_b);

        // Three windows of a consistently failing origin: 2 successes to 8
        // failures on each replica, well past the 50% threshold.
        for _ in 0..3 {
            for bucket in [&a, &b] {
                bucket.refresh_lease().await.expect("lease");
                for _ in 0..2 {
                    bucket.record_outcome(RequestOutcome::Success);
                }
                for _ in 0..8 {
                    bucket.record_outcome(RequestOutcome::Failure);
                }
            }
            tokio::time::sleep(window + Duration::from_millis(20)).await;
        }
        // One more pass so the tail counts of the last window are written back
        // and the next window is leased against them.
        for bucket in [&a, &b] {
            bucket.refresh_lease().await.expect("lease");
        }
        tokio::time::sleep(window + Duration::from_millis(20)).await;
        for bucket in [&a, &b] {
            bucket.refresh_lease().await.expect("lease");
        }

        let burst_a = a.metrics.cluster_effective_burst();
        let burst_b = b.metrics.cluster_effective_burst();
        assert_eq!(
            burst_a, burst_b,
            "replicas must agree on the cluster budget"
        );
        assert!(
            burst_a < 20,
            "a 80% error rate must shrink the budget below the configured 20, got {burst_a}"
        );
        assert!(burst_a >= 1, "the budget never falls below one request");
        assert!(
            a.is_throttling() && b.is_throttling(),
            "both replicas must report the throttle"
        );
    }

    /// Static cluster mode reports no admission ratio at all: a mode that cannot
    /// throttle must not emit a series that reads like a live measurement.
    #[tokio::test]
    async fn static_mode_reports_no_admission_ratio() {
        let bucket = LeasedBucket::new(config_for(10, "a", Duration::from_millis(200)));
        bucket.refresh_lease().await.expect("lease");
        assert_eq!(bucket.admission_coefficient(), None);
        assert!(!bucket.is_throttling());
    }

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
            adaptive: None,
        }
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

    #[tokio::test]
    async fn pre_lease_lands_in_next_window_and_promotes_on_roll() {
        // 200 ms windows make the test quick; burst=20 so MAX_LEASE_PER_REPLICA = 19.
        let bucket = LeasedBucket::new(config_for(20, "a", Duration::from_millis(200)));
        bucket.refresh_lease().await.expect("lease");

        // After refresh, both current and next slots should be granted.
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
        assert!(cur_grant > 0, "current grant should be non-zero");
        assert!(next_grant > 0, "next-window pre-lease should be non-zero");

        // Drain the current window's lease so the next acquire would block on
        // a roll.
        for _ in 0..cur_grant {
            bucket.acquire().await.expect("acquire ok");
        }

        // Wait for the window to roll. With pre-leasing, acquire() must NOT
        // block on a fresh OCC round-trip — the next-window slot is promoted
        // immediately.
        tokio::time::sleep(Duration::from_millis(220)).await;

        let started = std::time::Instant::now();
        bucket.acquire().await.expect("post-roll acquire ok");
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "post-roll acquire took {:?}, expected pre-leased slot to be promoted",
            started.elapsed()
        );

        // Inner state should reflect that the previous next-window is now current.
        let inner = bucket.inner.lock().await;
        assert_eq!(inner.current_window_id, next_window);
        assert_eq!(inner.granted_this_window, next_grant);
        assert_eq!(inner.consumed_this_window, 1);
    }
}
