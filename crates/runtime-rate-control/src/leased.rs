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
//! Schema is `PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION = 3`. State written
//! under a different schema is treated as empty (with a warning).
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
//! budget      = burst_per_window · coefficient
//! ```
//!
//! `K = 1 / (1 - failure_threshold)`, so the coefficient is exactly 1 at or
//! below the configured error rate and falls below it above.
//!
//! ### Carrying the fraction a lease cannot hold
//!
//! A lease is whole tokens, so the budget has to be split:
//!
//! ```text
//! whole     = floor(budget)
//! remainder = budget - whole        // in [0, 1)
//! ```
//!
//! Dropping the remainder every window leaves two bands in which the control
//! has no effect at all:
//!
//! 1. A small configured burst. At `burst_per_window = 1` the whole part is
//!    only ever 0 or 1, so a floor of one token let the cluster send at its
//!    configured rate however badly the origin failed.
//! 2. A coefficient above `1 - 1/(2 · burst_per_window)`, where rounding
//!    returned the configured budget in full and the throttle did nothing.
//!
//! Each replica therefore banks `remainder · its share of cluster demand` once
//! per window, and spends one whole token when its bank holds one. The shares
//! of a window sum to one, so the banks of the fleet hold exactly one window's
//! remainder between them: the carry needs no replica-to-replica traffic and
//! cannot raise the cluster above its budget. A bank is local to one replica
//! and is never written to the shared file.
//!
//! Unlike the single-node controller, outcome counts are bucketed into whole
//! windows rather than timestamped, so the decay runs on window identifiers:
//! a source window `s` ends at `(s+1)·W` and the target window `t` starts at
//! `t·W`, giving `age = t - s - 1` whole windows of separation. Every replica
//! leasing window `t` therefore applies the same weights to the same source
//! windows, whatever its local clock reads.
//!
//! The budget itself is never written back. Only the counts are shared; each
//! replica derives the budget from them and holds it for the life of the
//! window, so a tick cannot move the budget under a grant already issued.
//!
//! ## Replicas that configure different limits
//!
//! A limiter is keyed by its quota name *and* its configured limit
//! (`requests_per_second:burst=10:replenish_ns=100000000`), so replicas that
//! set different limits for one quota — a rolling deployment that changes
//! `requests_per_second_limit`, say — each write their own limiter into the
//! same file. Each replica therefore reads every limiter of its quota, not
//! only its own, and treats them as one budget:
//!
//! ```text
//! burst   = min(own burst, burst of every sibling limiter some replica is leasing)
//! granted = Σ grants of the window across own and sibling limiters
//! demand  = own demand / Σ demand across own and sibling limiters
//! ```
//!
//! The lowest limit is the only one that exceeds no replica's configuration.
//! A sibling counts while a replica holds a lease under it within two windows
//! of the window being leased (see [`PersistedLimiter::is_leasing`]), so a
//! replica that stops stops holding the cluster to its limit three windows
//! after its last refresh. Grants already written stand, so a replica that
//! starts with a lower limit holds the cluster to it from the second window
//! after it first leases. Each replica logs a warning naming the origin and
//! the limits when they start to differ, and a note when they agree again.
//! A limiter no replica has written for as many windows as the file retains
//! is dropped, so limit changes leave no trail in the file.
//!
//! Window ids come from each replica's own clock, so the budget of a window
//! holds in real time only while the replicas' clocks agree to well within a
//! window, whether they share one limit or several: a replica a window ahead
//! spends the next window's budget while its peers spend this one's.
//!
//! Replicas that agree on a limit share one key, and so one limiter. A replica
//! of a version that does not read siblings sees only its own limiter: it is
//! not held to a lower limit, and the replicas that read siblings count its
//! grants and give way to them.

use std::{
    collections::{BTreeSet, HashMap},
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

/// The format stays at 3. The cluster outcome counters (`ok`, `failed`) are
/// optional and additive, and no persisted struct denies unknown fields, so one
/// file serves a mixed-version fleet: an older instance ignores the counters and
/// keeps applying the configured limits, and a newer instance reads an older file
/// with the counters absent.
///
/// Bump this only for a change an older reader would misread.
/// [`PersistedRateControlState::is_current_schema`] tests for an exact match and
/// the reader then discards the whole state, so a fleet spanning two schema
/// versions erases the shared state on every tick, in both directions.
pub(crate) const PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION: u32 = 3;

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
/// The most fractional tokens one replica may hold in its bank.
///
/// A carried token the cluster cap refuses stays banked for a later window, but
/// a replica refused for a long run must not store those windows up and release
/// them together. Two tokens holds a full token plus one more window of accrual,
/// which is all the normal refuse-then-spend path needs.
const MAX_BANKED_TOKENS: f64 = 2.0;

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

    /// Drop every limiter but `keep` that no replica has written a lease
    /// under since `cutoff_unix_ms`.
    ///
    /// A replica prunes the windows of its own limiter only, so a limiter whose
    /// replicas have all stopped or changed their limit would otherwise stay in
    /// the file for good, and every replica would read it on every refresh.
    /// Judged by the leases' write times rather than by window ids, so a
    /// limiter written under a different window length is judged the same way.
    /// A running replica writes a lease at least once per window.
    fn drop_retired_limiters(&mut self, keep: &str, cutoff_unix_ms: u64) {
        self.limiters
            .retain(|key, limiter| key == keep || limiter.last_written_unix_ms() >= cutoff_unix_ms);
    }

    /// The other limiters of `own_key`'s quota: the same quota name, written
    /// by replicas that configure a different limit for it.
    fn siblings<'a>(
        &'a self,
        own_key: &'a str,
    ) -> impl Iterator<Item = (LimiterKey<'a>, &'a PersistedLimiter)> + 'a {
        let own_name = LimiterKey::parse(own_key).map(|key| key.name);
        self.limiters.iter().filter_map(move |(key, limiter)| {
            if key == own_key {
                return None;
            }
            let parsed = LimiterKey::parse(key)?;
            (Some(parsed.name) == own_name).then_some((parsed, limiter))
        })
    }
}

/// A persisted limiter key, `{name}:burst={limit}:replenish_ns={interval}`,
/// taken apart.
///
/// The key carries the configured limit, so replicas that set different limits
/// for one quota write different keys into one file. The name is what ties them
/// back together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LimiterKey<'a> {
    /// The quota, e.g. `requests_per_second`.
    name: &'a str,
    /// The configured limit: the quota's burst size, which is the value the
    /// user set (`requests_per_second_limit: 10` is a burst of 10).
    limit: u64,
}

impl<'a> LimiterKey<'a> {
    fn parse(key: &'a str) -> Option<Self> {
        let (name, rest) = key.split_once(":burst=")?;
        let limit = rest.split(':').next()?.parse().ok()?;
        Some(Self { name, limit })
    }

    /// The setting the user configures this quota with. The quota names come
    /// from HTTP rate control, the only caller that leases quotas: the quota
    /// `requests_per_second` is set by `requests_per_second_limit`.
    fn setting(self) -> String {
        format!("{}_limit", self.name)
    }
}

/// What the sibling limiters of a quota hold for one window: the replicas that
/// lease the same quota under a different configured limit.
#[derive(Debug)]
struct SiblingReading {
    /// The per-window burst the cluster is held to: the lowest of this
    /// replica's and every live sibling's.
    burst: u64,
    /// The distinct limits live siblings configure, for the log line.
    limits: BTreeSet<u64>,
    /// Tokens live siblings granted in the window.
    granted: u64,
    /// Live siblings' demand over the lookback. `mine` is zero: this replica
    /// leases only under its own limiter.
    demand: DemandSample,
    /// Upstream outcomes recorded under every sibling, live or not: they were
    /// observed against the same origin.
    outcomes: OutcomeSample,
}

impl SiblingReading {
    /// Read the siblings of `own_key` for `window_id`. `own_burst` is this
    /// replica's configured burst; the demand of live siblings is classified
    /// against the burst the cluster is held to, which every replica derives
    /// the same way.
    fn read(
        state: &PersistedRateControlState,
        own_key: &str,
        own_burst: u64,
        instance: &str,
        window_id: u64,
        now_window: u64,
        adaptive: Option<LeasedAdaptiveConfig>,
    ) -> Self {
        let siblings: Vec<_> = state.siblings(own_key).collect();
        let live: Vec<_> = siblings
            .iter()
            .filter(|(_, limiter)| limiter.is_leasing(window_id))
            .collect();

        let burst = live
            .iter()
            .map(|(_, limiter)| limiter.burst_per_window)
            .fold(own_burst, u64::min);

        let mut demand = DemandSample::default();
        for (_, limiter) in &live {
            demand.accumulate(limiter.ewma_demand(
                instance,
                now_window,
                burst,
                DEMAND_EWMA_LOOKBACK_WINDOWS,
            ));
        }

        let mut outcomes = OutcomeSample::default();
        if let Some(adaptive) = adaptive {
            for (_, limiter) in &siblings {
                outcomes.merge(limiter.ewma_outcomes(
                    window_id,
                    OUTCOME_EWMA_LOOKBACK_WINDOWS,
                    adaptive.half_life_windows,
                ));
            }
        }

        Self {
            burst,
            limits: live.iter().map(|(key, _)| key.limit).collect(),
            granted: live
                .iter()
                .map(|(_, limiter)| limiter.granted_in(window_id))
                .sum(),
            demand,
            outcomes,
        }
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
                .is_ok_and(|id| id + STALE_WINDOW_RETENTION >= now_window)
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

    /// When a replica last wrote a lease under this limiter, by that replica's
    /// clock; `0` when none has.
    fn last_written_unix_ms(&self) -> u64 {
        self.windows
            .values()
            .flat_map(|window| window.leases.values())
            .map(|lease| lease.updated_at_unix_ms)
            .max()
            .unwrap_or(0)
    }

    /// Tokens granted under this limiter in `window_id`, across replicas.
    fn granted_in(&self, window_id: u64) -> u64 {
        self.windows
            .get(&window_id.to_string())
            .map_or(0, PersistedWindow::total_granted)
    }

    /// Whether a replica is leasing under this limiter for `window_id`.
    ///
    /// Every refresh leases the current window and pre-leases the next, so a
    /// running replica holds a lease in `window_id` or the window before it,
    /// whichever window it last refreshed in. Windows up to two either side
    /// count, so a replica whose clock runs up to a window ahead of or behind
    /// this one's still counts, a newly started one included. A replica that
    /// stopped drops out three windows after its last refresh.
    fn is_leasing(&self, window_id: u64) -> bool {
        (window_id.saturating_sub(2)..=window_id.saturating_add(2)).any(|id| {
            self.windows
                .get(&id.to_string())
                .is_some_and(|window| !window.leases.is_empty())
        })
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
    /// `half_life_windows` of it. The half-life itself need not be a whole
    /// number of windows.
    fn ewma_outcomes(
        &self,
        target_window: u64,
        lookback_windows: u64,
        half_life_windows: f64,
    ) -> OutcomeSample {
        let mut outcomes = OutcomeSample::default();
        let half_life = half_life_windows.max(1.0);
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
    /// `granted_by_siblings` is what the window's sibling limiters hold out of
    /// the same budget.
    fn recompute_budget_remaining(&mut self, burst: u64, granted_by_siblings: u64) {
        self.budget_remaining = burst
            .saturating_sub(self.total_granted())
            .saturating_sub(granted_by_siblings);
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

    /// This replica's share of cluster demand, in `[0, 1]`, or `None` when the
    /// sample cannot divide one.
    ///
    /// Shares taken from one sample sum to 1 across the replicas in it, and that
    /// identity is what keeps the banked remainder of a window summing to one
    /// window's remainder. Where the identity cannot hold there is no share to
    /// give: with no cluster demand recorded at all, every replica would read a
    /// share of 1 and the fleet would bank the remainder once per replica, so
    /// the caller banks nothing instead. A replica of its own zero demand needs
    /// no carry either.
    #[expect(
        clippy::cast_precision_loss,
        reason = "weighted per-window request counts; the ratio is what matters, not the last bit"
    )]
    fn share_fraction(self) -> Option<f64> {
        if self.total == 0 || self.mine == 0 {
            return None;
        }
        Some((self.mine as f64 / self.total as f64).clamp(0.0, 1.0))
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
    /// nothing — for a bucket without adaptive settings that is every window,
    /// and its leases stay exactly as they were before cluster adaptive
    /// throttling existed.
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

    /// Add a sample weighted over the same windows, such as a sibling
    /// limiter's.
    fn merge(&mut self, other: Self) {
        self.requests += other.requests;
        self.accepts += other.accepts;
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
/// Absent for a bucket built without adaptive settings, where the coefficient
/// is always
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
    /// Decay half-life, as a count of windows. One window is the smallest unit
    /// of time the shared file records, so a shorter half-life cannot be
    /// expressed; the caller divides `rate_control_window` by `refresh_interval`
    /// and floors the ratio at one window.
    pub half_life_windows: f64,
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
    /// Cluster adaptive throttling parameters, or `None` to apply the
    /// configured budget unchanged.
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
    /// [`f64::to_bits`]. `None` without adaptive settings, encoded as
    /// [`NO_COEFFICIENT_BITS`] so a bucket that cannot throttle reports no series
    /// rather than a constant `1` that reads like a live measurement.
    adaptive_admission_ratio_bits: AtomicU64,
    /// The whole part of the cluster budget of the current window, after the
    /// coefficient.
    cluster_effective_burst: AtomicU64,
}

/// The encoding [`LeasedBucketMetrics::adaptive_admission_ratio_bits`] uses for
/// "no adaptive settings, no coefficient". A NaN payload, which no real coefficient can
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
    /// `[0, 1]`. `None` without adaptive settings, or before the first lease.
    #[must_use]
    pub fn adaptive_admission_ratio(&self) -> Option<f64> {
        match self.adaptive_admission_ratio_bits.load(Ordering::Relaxed) {
            NO_COEFFICIENT_BITS => None,
            bits => Some(f64::from_bits(bits)),
        }
    }

    /// The whole part of the cluster budget of the most recently leased window,
    /// after the coefficient. Equal to the lowest burst the replicas leasing the
    /// quota configure while the origin is healthy. The fraction the whole part
    /// drops is carried by the replicas and is reported by
    /// [`Self::adaptive_admission_ratio`], so a deeply throttled cluster can
    /// read zero here and still send.
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
    /// The budget this replica has already fixed for the windows it is
    /// currently leasing. Touched once per refresh tick, like `throttle_log`.
    budget_memo: SyncMutex<BudgetMemo>,
    /// The fraction of the cluster budget this replica carries between windows.
    /// Local to this replica, and never written to the shared file.
    remainder_bank: SyncMutex<RemainderBank>,
    /// The limits other replicas configure for this quota, as last logged.
    /// Empty while every replica agrees. Touched once per refresh tick.
    reported_sibling_limits: SyncMutex<BTreeSet<u64>>,
}

/// The cluster budget for one window after the adaptive coefficient, split into
/// the whole tokens a lease can carry and the fraction it cannot.
///
/// Capped at the configured burst, because adaptive control modifies a static
/// limit and never raises it. Deliberately *not* floored at one: a floor of one
/// token per window is itself a dead zone, because at a configured burst of one
/// no error rate could then throttle the cluster at all. The fraction is not
/// lost either — [`RemainderBank`] carries it.
#[derive(Debug, Clone, Copy, Default)]
struct ClusterBudget {
    /// `floor(burst · coefficient)`: what a lease can hold.
    whole: u64,
    /// What the floor dropped, in `[0, 1)`.
    remainder: f64,
}

impl ClusterBudget {
    /// The configured budget in full: without adaptive settings there is no
    /// coefficient and so nothing to carry.
    fn full(burst: u64) -> Self {
        Self {
            whole: burst,
            remainder: 0.0,
        }
    }

    /// Split `burst · coefficient`, with `coefficient` in `[0, 1]`.
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "burst is a per-window request count; the coefficient is clamped to [0, 1], so the product is non-negative and well within u64"
    )]
    fn scaled(burst: u64, coefficient: f64) -> Self {
        let scaled = burst as f64 * coefficient.clamp(0.0, FULL_ADMISSION_COEFFICIENT);
        let whole = (scaled.floor() as u64).min(burst);
        Self {
            whole,
            // A product that reaches the cap has nothing left to carry.
            remainder: if whole >= burst {
                0.0
            } else {
                scaled - scaled.floor()
            },
        }
    }

    /// The whole part as a real number, for the arithmetic the carry needs.
    #[expect(
        clippy::cast_precision_loss,
        reason = "a per-window request count, far below 2^53 in any real configuration"
    )]
    fn whole_tokens(self) -> f64 {
        self.whole as f64
    }

    /// Tokens this window targets, the carried fraction included. `whole` is
    /// what one lease can hold; this is what the cluster spends over time.
    fn target(self) -> f64 {
        self.whole_tokens() + self.remainder
    }
}

/// Carries the fraction of the cluster budget that a whole-token lease drops.
///
/// Each window the replica banks its own share of that fraction and may spend a
/// whole token once the bank holds one. The shares of a window sum to one, so
/// the fleet's banks hold exactly one window's fraction between them without any
/// replica-to-replica traffic.
#[derive(Debug, Default)]
struct RemainderBank {
    /// Fractional tokens held, in `[0, MAX_BANKED_TOKENS]`.
    banked: f64,
    /// Highest window already banked for. The lease path visits a window many
    /// times — once per refresh tick, as the current window and again as the
    /// pre-leased one — and the fraction may be banked only once per window, or
    /// a fast tick rate would multiply the budget.
    highest_banked_window: Option<u64>,
}

impl RemainderBank {
    /// Bank `remainder · share` for `window_id` if that window has not banked
    /// yet, then report how many whole tokens the bank can fund.
    ///
    /// `share` is `None` when the demand sample cannot divide a share; the
    /// window then banks nothing rather than have every replica bank the whole
    /// fraction. See [`DemandSample::share_fraction`].
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the bank is non-negative and clamped to MAX_BANKED_TOKENS"
    )]
    fn accrue(&mut self, window_id: u64, remainder: f64, share: Option<f64>) -> u64 {
        if self
            .highest_banked_window
            .is_none_or(|highest| window_id > highest)
        {
            self.highest_banked_window = Some(window_id);
            if let Some(share) = share {
                self.banked = (self.banked + remainder * share).clamp(0.0, MAX_BANKED_TOKENS);
            }
        }
        self.banked.floor() as u64
    }

    /// Debit the carried tokens a lease actually granted.
    ///
    /// Only what was granted. A carried token the cluster cap refused stays
    /// banked for a later window; debiting the request instead would make the
    /// fleet admit steadily less than the budget it derived.
    #[expect(
        clippy::cast_precision_loss,
        reason = "at most MAX_BANKED_TOKENS whole tokens"
    )]
    fn debit(&mut self, tokens: u64) {
        self.banked = (self.banked - tokens as f64).max(0.0);
    }
}

/// What a window's budget is derived from, as this replica first read it.
#[derive(Debug, Clone, Copy)]
struct HeldBudget {
    /// The burst the cluster was held to.
    burst: u64,
    /// The adaptive coefficient, or `None` without adaptive settings.
    coefficient: Option<f64>,
}

impl HeldBudget {
    /// The window's budget, given the burst the cluster is held to now.
    ///
    /// Never above the held burst, so a sibling that stops mid-window does not
    /// raise a budget already leased against. A replica that configures a
    /// lower limit can start leasing mid-window, though, and the budget then
    /// falls to it at once — at the held coefficient, so an adaptive throttle
    /// carries over to the lower burst.
    fn at(self, burst: u64) -> ClusterBudget {
        let burst = self.burst.min(burst);
        self.coefficient.map_or_else(
            || ClusterBudget::full(burst),
            |coefficient| ClusterBudget::scaled(burst, coefficient),
        )
    }
}

/// Holds what this replica derived each window's budget from for the life of
/// the window.
///
/// Two slots: a tick leases the current window and pre-leases the next, so a
/// third window is never live at once.
#[derive(Debug, Default)]
struct BudgetMemo {
    slots: [Option<(u64, HeldBudget)>; 2],
}

impl BudgetMemo {
    /// The budget already held for `window_id`, else `derive()`'s value,
    /// stored against it. Evicts the older window when both slots are taken.
    fn get_or_derive(&mut self, window_id: u64, derive: impl FnOnce() -> HeldBudget) -> HeldBudget {
        if let Some((_, held)) = self.slots.iter().flatten().find(|(id, _)| *id == window_id) {
            return *held;
        }
        let held = derive();
        // A held budget holds a float and so is not ordered; compare the slots
        // on window id, which is what "older" meant all along.
        let window_of = |slot: &Option<(u64, HeldBudget)>| slot.map_or(0, |(id, _)| id);
        let victim = self
            .slots
            .iter()
            .position(Option::is_none)
            .unwrap_or_else(|| usize::from(window_of(&self.slots[1]) < window_of(&self.slots[0])));
        self.slots[victim] = Some((window_id, held));
        held
    }
}

impl LeasedBucket {
    pub fn new(config: LeasedBucketConfig) -> Arc<Self> {
        let object_state = Arc::new(
            ObjectState::new(Arc::clone(&config.store)).with_prefix(config.prefix.clone()),
        );
        let window_id = window_id_for(SystemTime::now(), config.window_duration);
        // Hold a boundary reading for one half-life before reporting it, the
        // same rule the single-node controller uses.
        let half_life_windows = config
            .adaptive
            .map_or(1.0, |adaptive| adaptive.half_life_windows.max(1.0));
        let hold =
            Duration::try_from_secs_f64(config.window_duration.as_secs_f64() * half_life_windows)
                .unwrap_or(Duration::MAX);
        Arc::new(Self {
            object_state,
            inner: Mutex::new(LeasedBucketInner::new(window_id)),
            notify: Notify::new(),
            metrics: Arc::new(LeasedBucketMetrics::default()),
            throttle_log: SyncMutex::new(PhaseChangeLog::new(ThrottleState::Healthy, hold)),
            budget_memo: SyncMutex::new(BudgetMemo::default()),
            remainder_bank: SyncMutex::new(RemainderBank::default()),
            reported_sibling_limits: SyncMutex::new(BTreeSet::new()),
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

    /// The cluster admission coefficient in force, or `None` without adaptive
    /// settings or before the first lease.
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
            let existing = match self.read_state().await {
                Ok(state) => state,
                Err(e) => {
                    self.note_failure();
                    return Err(e);
                }
            };
            // State from a newer version holds leases this version cannot interpret;
            // resetting it would wipe every newer peer's grants, and during a rolling
            // upgrade the two versions would keep resetting each other and over-admit.
            // State from an older version is replaced: its leases use a layout this
            // version no longer reads. The warning waits for the write that replaces it,
            // since a write that loses the race replaced nothing.
            let (mut state, replaced_version) = match existing {
                Some(state)
                    if state.schema_version > PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION =>
                {
                    self.note_failure();
                    return Err(Error::NewerStateVersion {
                        origin: self.config.origin.clone(),
                        found: state.schema_version,
                        supported: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
                    });
                }
                Some(state) if !state.is_current_schema() => {
                    let replaced = state.schema_version;
                    (PersistedRateControlState::fresh(window), Some(replaced))
                }
                Some(state) => (state, None),
                None => (PersistedRateControlState::fresh(window), None),
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
            // Limiters no replica writes any more go after the same horizon as
            // the windows of a live one.
            state.drop_retired_limiters(
                &self.config.limiter_key,
                unix_millis(now).saturating_sub(
                    STALE_WINDOW_RETENTION.saturating_mul(duration_millis_u64(window)),
                ),
            );
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
                    Ok(WriteOutcome::Written) => {
                        if let Some(replaced_version) = replaced_version {
                            tracing::warn!(
                                origin = %self.config.origin,
                                "Replacing shared rate-control state for origin {} written by an older Spice version (state version {}), so leases granted by instances still on that version are reset. Finish upgrading every instance that shares the rate-control state location.",
                                self.config.origin,
                                replaced_version
                            );
                        }
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
            self.report_sibling_limits(&current.sibling_limits);
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
        let configured_burst = self.config.burst_per_window;
        let window_ms = duration_millis_u64(self.config.window_duration);
        let window_end_ms = (window_id + 1).saturating_mul(window_ms);
        let now_window = window_id_for(now, self.config.window_duration);

        // Replicas that configure a different limit for this quota lease under
        // their own limiters. Read them before taking a mutable borrow on ours:
        // together they are one budget, held to the lowest configured limit.
        let siblings = SiblingReading::read(
            state,
            &self.config.limiter_key,
            configured_burst,
            instance,
            window_id,
            now_window,
            self.config.adaptive,
        );
        let burst = siblings.burst;

        let limiter = state.limiter_entry(&self.config.limiter_key, configured_burst);

        // This replica's reading of the shared counts: `Some` exactly when the
        // bucket has adaptive settings. Every replica reads the same published outcomes, so
        // they converge on the same coefficient without any of it being
        // written back — the counts are the shared state, the budget is not.
        let reading = self.config.adaptive.map(|adaptive| {
            let mut outcomes = limiter.ewma_outcomes(
                window_id,
                OUTCOME_EWMA_LOOKBACK_WINDOWS,
                adaptive.half_life_windows,
            );
            outcomes.merge(siblings.outcomes);
            (adaptive, outcomes)
        });

        // Read the smoothed demand signal before taking a mutable borrow on
        // the window below.
        //
        // `classified_demand` measures want against the configured burst the
        // cluster is held to, not the throttled one — a saturated replica wants
        // at least a full budget whatever the cluster is currently allowed. The share it wins, and the
        // clamps around it, are of the effective budget: that is the permission
        // actually being divided up.
        let mut demand_sample =
            limiter.ewma_demand(instance, now_window, burst, DEMAND_EWMA_LOOKBACK_WINDOWS);
        demand_sample.accumulate(siblings.demand);

        // The budget this window is leased against, derived once and then held
        // for the life of the window. A later tick must not move it: this
        // replica's grant is already in the file, and a budget recomputed from
        // fresher counts would republish a `budget_remaining` that contradicts
        // the leases already written. Same reason the grant itself is
        // first-write-wins. The one exception is a lower limit appearing
        // mid-window, which lowers the budget at once; see [`HeldBudget::at`].
        let budget = self
            .budget_memo
            .lock()
            .get_or_derive(window_id, || HeldBudget {
                burst,
                coefficient: reading.map(|(adaptive, outcomes)| outcomes.coefficient(adaptive.k)),
            })
            .at(burst);

        // Bank this replica's share of the fraction the whole part dropped, and
        // ask what the bank can fund on top of the demand-weighted slice. The
        // bank accrues once per window however many ticks reach this line.
        let carried = self.remainder_bank.lock().accrue(
            window_id,
            budget.remainder,
            demand_sample.share_fraction(),
        );

        let demand = demand_sample
            .demand_signal(budget.whole, local_demand_hint)
            .max(min_lease(budget.whole))
            .min(max_lease_per_replica(budget.whole));

        let window = limiter.window_entry(window_id, budget.whole);

        // Only the current window can hold expired leases; a future window's
        // lease cannot have expired yet.
        window.drop_expired(now);
        window.recompute_budget_remaining(budget.whole, siblings.granted);

        let my_existing = window.granted_for(instance);
        let max_possible_for_me = replica_ceiling(
            budget.whole,
            carried,
            burst,
            window
                .granted_by_others(instance)
                .saturating_add(siblings.granted),
        );

        // Pick the new grant.
        //
        // Both the current and pre-leased windows are **first-write-wins**:
        // the first tick to lease a given window/replica establishes the
        // grant, and subsequent ticks only refresh `consumed`/`attempted`.
        // This eliminates within-window oscillation that would otherwise
        // occur as demand-weighted recomputation ping-pongs leases between
        // replicas. Adjustments to demand show up in the *next* window's
        // pre-lease.
        let (new_grant, carry_spent) = if my_existing > 0 {
            (my_existing, 0)
        } else {
            let granted = demand.saturating_add(carried).min(max_possible_for_me);
            (granted, granted.saturating_sub(demand))
        };

        // Debit the bank by what the grant actually carried, never by what it
        // asked for. A carried token the cluster cap refused has to stay banked
        // for a later window, or the fleet admits less than its own budget.
        self.remainder_bank.lock().debit(carry_spent);

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

        window.recompute_budget_remaining(budget.whole, siblings.granted);

        // Decided on the ratio, not on the whole part: with the fraction
        // carried, a budget whose whole part equals the configured burst can
        // still be a throttled one.
        let ratio = admission_ratio(budget, burst);
        WindowOutcome {
            granted: new_grant,
            budget_remaining_after: window.budget_remaining,
            throttle: reading.map(|(adaptive, outcomes)| ClusterThrottle {
                effective_burst: budget.whole,
                admission_ratio: ratio,
                near_boundary: outcomes
                    .is_near_boundary(adaptive.k, ratio < FULL_ADMISSION_COEFFICIENT),
            }),
            sibling_limits: siblings.limits,
            dirty: published,
        }
    }

    /// Publish the adaptive state of the window just leased, and log a change
    /// of state.
    ///
    /// Every replica reports for itself. They read the same published counts,
    /// so their lines agree once those counts have settled.
    fn report_throttle(&self, throttle: Option<ClusterThrottle>) {
        let Some(throttle) = throttle else {
            return; // No adaptive settings: nothing to report, and no series to emit.
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

    /// Log a change in the limits other replicas configure for this quota.
    fn report_sibling_limits(&self, sibling_limits: &BTreeSet<u64>) {
        match self.sibling_limits_change(sibling_limits) {
            Some(SiblingLimitsChange::Differ(warning)) => tracing::warn!("{warning}"),
            Some(SiblingLimitsChange::Agree(notice)) => tracing::info!("{notice}"),
            None => {}
        }
    }

    /// What to log about the limits other replicas configure for this quota,
    /// or `None` when they have not changed since the last report.
    ///
    /// Warns when they start to differ from this replica's, or change while
    /// they do, naming every value, and notes once they agree again. Reported
    /// on a change only, not on every tick.
    fn sibling_limits_change(&self, sibling_limits: &BTreeSet<u64>) -> Option<SiblingLimitsChange> {
        {
            let mut reported = self.reported_sibling_limits.lock();
            if *reported == *sibling_limits {
                return None;
            }
            reported.clone_from(sibling_limits);
        }
        let key = LimiterKey::parse(&self.config.limiter_key)?;
        Some(if sibling_limits.is_empty() {
            SiblingLimitsChange::Agree(limits_agree_notice(&self.config.origin, key))
        } else {
            SiblingLimitsChange::Differ(limits_differ_warning(
                &self.config.origin,
                key,
                sibling_limits,
            ))
        })
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
    /// The adaptive state of the window that was just leased, or `None`
    /// without adaptive settings.
    throttle: Option<ClusterThrottle>,
    /// The limits replicas leasing under sibling limiters configure for the
    /// window. Empty while every replica configures this one's.
    sibling_limits: BTreeSet<u64>,
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
    /// The whole part of the cluster budget for the window: what the leases of
    /// the window can hold between them. The fraction the whole part drops is
    /// carried by the replicas and shows up in `admission_ratio`.
    effective_burst: u64,
    /// The fractional budget over the configured burst, in `[0, 1]`. Equal to
    /// the coefficient, bar the cap at the configured burst.
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

/// A change in the limits other replicas configure for a quota, with the line
/// that reports it.
#[derive(Debug, PartialEq, Eq)]
enum SiblingLimitsChange {
    /// They differ from this replica's limit: a warning.
    Differ(String),
    /// They agree with it again: a note.
    Agree(String),
}

/// The warning a replica logs while other replicas sharing the state location
/// configure a different limit for one of its quotas.
fn limits_differ_warning(origin: &str, key: LimiterKey<'_>, others: &BTreeSet<u64>) -> String {
    let setting = key.setting();
    let own = key.limit;
    let lowest = others.iter().copied().fold(own, u64::min);
    let others = list_values(others);
    format!(
        "Instances sharing cluster rate control for origin '{origin}' set `{setting}` to different values (this instance: {own}; other instances: {others}), so the cluster is held to the lowest value, {lowest}, until every instance sets the same one. Set the same `{setting}` on every instance that shares `runtime.state.location`. See: https://spiceai.org/docs/reference/spicepod/runtime#runtimesource_rate_control"
    )
}

/// The note a replica logs once every replica it shares a quota with agrees on
/// the limit again, closing the warning [`limits_differ_warning`] opened.
fn limits_agree_notice(origin: &str, key: LimiterKey<'_>) -> String {
    let setting = key.setting();
    let limit = key.limit;
    format!(
        "Instances sharing cluster rate control for origin '{origin}' now all set `{setting}` to {limit}, so the cluster is held to {limit}."
    )
}

/// `10`, `10 and 15`, `5, 10 and 15`.
fn list_values(values: &BTreeSet<u64>) -> String {
    let values: Vec<String> = values.iter().map(u64::to_string).collect();
    match values.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        Some((last, _)) => last.clone(),
        None => String::new(),
    }
}

/// The fraction of the configured budget a window is allowed to spend.
///
/// Measured against the fractional budget, not the whole part the lease path
/// enforces. The remainder is spent as well — banked by each replica and
/// released one whole token at a time — so reporting `whole / burst` would read
/// as a deeper throttle than the cluster applies, and at a configured burst of
/// one it would report 0 for every coefficient below 1.
fn admission_ratio(budget: ClusterBudget, burst: u64) -> f64 {
    if burst == 0 {
        return FULL_ADMISSION_COEFFICIENT;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "per-window request counts; the ratio is what matters, not the last bit"
    )]
    let ratio = budget.target() / burst as f64;
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

/// The most tokens this replica may hold in a window.
///
/// A carried token is funded by the replica's own bank rather than out of the
/// cluster whole part, so it raises this replica's ceiling — but never above
/// the configured burst. The bank outlives the window that filled it, so a
/// replica whose carry the cluster refused can still hold a whole token when
/// the coefficient recovers and the whole part is already the configured
/// burst. Without the cap the pair would lease `burst + 1`, and adaptive
/// control would admit more than the static limit it only ever modifies down.
fn replica_ceiling(whole: u64, carried: u64, burst: u64, granted_by_others: u64) -> u64 {
    whole
        .saturating_add(carried)
        .min(burst)
        .saturating_sub(granted_by_others)
}

/// The smallest slice a replica may lease: one percent of the budget, and never
/// less than one token.
///
/// A budget of zero is the exception, and the reason the floor is spelled out
/// rather than left as a `.max(1)`: a budget the coefficient has closed must
/// grant nothing. One token there would give every replica a token per window
/// and put back the band the carried remainder exists to remove.
fn min_lease(budget_per_window: u64) -> u64 {
    if budget_per_window == 0 {
        return 0;
    }
    (budget_per_window / 100).max(1)
}

/// The largest slice one replica may lease. A single replica can claim almost
/// the whole budget when no peer demands any; `min_lease` stays reserved so a
/// newcomer can take a starter slice in its first window. Zero on a zero budget,
/// for the reason [`min_lease`] gives.
fn max_lease_per_replica(budget_per_window: u64) -> u64 {
    if budget_per_window == 0 {
        return 0;
    }
    budget_per_window
        .saturating_sub(min_lease(budget_per_window))
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
    use futures::FutureExt;
    use insta::assert_snapshot;
    use object_store::memory::InMemory;
    use serde_json::Value;
    use std::num::NonZeroU32;

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
    /// Update it only with a deliberate wire-format change, and bump
    /// `schema_version` as well when an older reader would misread that change.
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
          "schema_version": 3,
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

    /// `consumed`, `attempted`, `ok` and `failed` are all optional on the wire. A file written by a replica that omits them must
    /// still load: the counts at zero, and the two `Option`s absent rather than
    /// `Some(0)` — "did not report" is not "reported none".
    #[test]
    fn persisted_state_reads_lease_without_optional_counts() {
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
    }

    /// A file from an instance that predates the outcome counters stays usable:
    /// it is current, so its limiters, windows and leases are kept rather than
    /// discarded, and its leases carry no outcome evidence, so they drop out of
    /// the cluster error-rate estimate instead of reading as zero.
    #[test]
    fn state_written_without_the_outcome_counters_is_current_and_holds_no_evidence() {
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
                                    "consumed": 5,
                                    "attempted": 12,
                                    "expires_at_unix_ms": 1700000001000,
                                    "updated_at_unix_ms": 1700000001500
                                }
                            }
                        }
                    }
                }
            }
        }"#;

        let state: PersistedRateControlState =
            serde_json::from_str(ON_DISK).expect("a file without the counters deserializes");
        assert!(
            state.is_current_schema(),
            "the counters are additive, so an older file is still read, not discarded"
        );

        let limiter = state
            .limiters
            .get("requests_per_second:burst=10:replenish_ns=100000000")
            .expect("limiter present");
        let lease = limiter
            .windows
            .get("1700000000")
            .expect("window present")
            .leases
            .get("replica-a")
            .expect("lease present");
        assert_eq!(lease.granted, 7, "the lease survives the read");
        assert_eq!((lease.ok, lease.failed), (None, None));

        // The lease is final (written back past its window end), so only the
        // absent counters keep it out of the estimate.
        let sample = limiter.ewma_outcomes(1_700_000_001, OUTCOME_EWMA_LOOKBACK_WINDOWS, 1.0);
        assert!(
            sample.requests.abs() < 1e-9,
            "a lease without counters is dropped from the cluster estimate, not read as zero"
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

    fn adaptive_config(half_life_windows: f64) -> LeasedAdaptiveConfig {
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

        let sample = limiter.ewma_outcomes(10, 5, 1.0);
        let expected = 1.0 + 0.5 + 0.25 + 0.125 + 0.0625;
        assert!(
            (sample.requests - expected).abs() < 1e-9,
            "expected {expected}, got {}",
            sample.requests
        );
        assert!((sample.accepts - expected).abs() < 1e-9);

        // A five-window half-life flattens the weights toward 1.
        let flat = limiter.ewma_outcomes(10, 5, 5.0);
        assert!(flat.requests > sample.requests);
    }

    /// The half-life is a real number of windows, not a whole one: at 1.5
    /// windows the weight at age `a` is `0.5 ^ (a / 1.5)`, which sits between
    /// the one-window and two-window curves.
    #[test]
    fn ewma_outcomes_accepts_a_fractional_half_life() {
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

        let sample = limiter.ewma_outcomes(10, 5, 1.5);
        let expected: f64 = (0..5).map(|age| 0.5_f64.powf(f64::from(age) / 1.5)).sum();
        assert!(
            (sample.requests - expected).abs() < 1e-9,
            "expected {expected}, got {}",
            sample.requests
        );

        let one_window = limiter.ewma_outcomes(10, 5, 1.0);
        let two_windows = limiter.ewma_outcomes(10, 5, 2.0);
        assert!(sample.requests > one_window.requests);
        assert!(sample.requests < two_windows.requests);
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
        assert!((limiter.ewma_outcomes(10, 5, 1.0).requests - (4.0 + 0.5)).abs() < 1e-9);
        // Target 11, same file and no clock involved: both windows age by one.
        assert!((limiter.ewma_outcomes(11, 5, 1.0).requests - (2.0 + 0.25)).abs() < 1e-9);
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

        let sample = limiter.ewma_outcomes(10, 5, 1.0);
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

        let sample = limiter.ewma_outcomes(10, 5, 1.0);
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
            .ewma_outcomes(10, 5, 1.0)
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

    /// Tokens one replica admits per window, averaged over `windows` windows,
    /// for a replica that holds the whole demand share and that the cluster cap
    /// never refuses.
    fn mean_admitted(burst: u64, coefficient: f64, windows: u32) -> f64 {
        let budget = ClusterBudget::scaled(burst, coefficient);
        let mut bank = RemainderBank::default();
        let mut carried = 0_u32;
        for window in 0..u64::from(windows) {
            let offered = bank.accrue(window, budget.remainder, Some(1.0));
            bank.debit(offered);
            carried += u32::try_from(offered).unwrap_or(u32::MAX);
        }
        budget.whole_tokens() + f64::from(carried) / f64::from(windows)
    }

    /// The carry is what holds the throttle on target: over a few hundred
    /// windows the cluster admits `burst · coefficient` per window, at a
    /// configured burst of one as well as at six hundred.
    #[test]
    fn the_carried_remainder_converges_on_the_fractional_budget() {
        const WINDOWS: u32 = 400;
        for (burst, coefficient) in [
            (1_u32, 0.3_f64),
            (1, 0.9),
            (5, 0.3),
            (5, 0.9),
            (600, 0.3),
            (600, 0.9),
        ] {
            let target = f64::from(burst) * coefficient;
            let mean = mean_admitted(u64::from(burst), coefficient, WINDOWS);
            assert!(
                (mean - target).abs() < 0.01,
                "burst {burst} at coefficient {coefficient} should admit about {target} per window, admitted {mean}"
            );
        }
    }

    /// The first dead zone: at a configured burst of one the whole part is only
    /// ever 0 or 1, so with a floor of one token no error rate could throttle
    /// the cluster at all.
    #[test]
    fn a_configured_burst_of_one_can_be_throttled() {
        let budget = ClusterBudget::scaled(1, 0.3);
        assert_eq!(
            budget.whole, 0,
            "a lease at a burst of one carries no whole token"
        );
        assert!((budget.remainder - 0.3).abs() < 1e-9);
        assert!(
            admission_ratio(budget, 1) < FULL_ADMISSION_COEFFICIENT,
            "and the window has to report as throttled"
        );
        assert!((mean_admitted(1, 0.3, 400) - 0.3).abs() < 0.01);
    }

    /// The second dead zone: above `1 − 1/(2·B)` rounding returned the
    /// configured budget in full, so a cluster that had started to fail kept
    /// sending at its configured rate.
    #[test]
    fn a_coefficient_inside_the_old_rounding_band_still_throttles() {
        let burst = 10_u64;
        // 1 − 1/(2·10) = 0.95, and rounding 9.6 gave back all ten tokens.
        let budget = ClusterBudget::scaled(burst, 0.96);
        assert_eq!(
            budget.whole, 9,
            "the lease may carry only nine whole tokens"
        );
        assert!((budget.remainder - 0.6).abs() < 1e-9);
        assert!(
            admission_ratio(budget, burst) < FULL_ADMISSION_COEFFICIENT,
            "and the window has to report as throttled"
        );
        assert!((mean_admitted(burst, 0.96, 400) - 9.6).abs() < 0.01);
    }

    /// A healthy origin is unchanged at any burst: the configured budget in
    /// full, nothing carried, and an admission ratio of one.
    #[test]
    fn a_healthy_origin_keeps_the_whole_configured_budget() {
        for burst in [1_u64, 5, 600] {
            let budget = ClusterBudget::scaled(burst, FULL_ADMISSION_COEFFICIENT);
            assert_eq!(budget.whole, burst);
            assert!(budget.remainder.abs() < f64::EPSILON);
            assert!(
                (admission_ratio(budget, burst) - FULL_ADMISSION_COEFFICIENT).abs() < f64::EPSILON
            );
        }
        // And the budget never rises above the configured limit.
        assert_eq!(ClusterBudget::scaled(10, 2.0).whole, 10);
    }

    /// A carried token the cluster cap refuses stays banked and is spent in a
    /// later window.
    #[test]
    fn a_refused_carried_token_stays_banked() {
        let mut bank = RemainderBank::default();
        assert_eq!(bank.accrue(0, 0.5, Some(1.0)), 0);
        assert_eq!(bank.accrue(1, 0.5, Some(1.0)), 1);
        bank.debit(0); // The cluster cap refused it.
        assert_eq!(
            bank.accrue(2, 0.5, Some(1.0)),
            1,
            "a refused token has to stay banked"
        );
        bank.debit(1);
        assert_eq!(bank.accrue(3, 0.5, Some(1.0)), 1);
        bank.debit(1);
        assert_eq!(bank.accrue(4, 0.5, Some(1.0)), 0);
    }

    /// Debiting the request rather than the grant loses the fraction every time
    /// the cluster cap refuses a token, and the fleet then admits steadily less
    /// than the budget it derived.
    #[test]
    fn debiting_a_request_instead_of_a_grant_under_admits() {
        const WINDOWS: u64 = 200;
        const REMAINDER: f64 = 0.5;
        // The cluster cap refuses the carried token in one window of every
        // eight.
        let refused = |window: u64| window % 8 == 1;

        let mut honest = RemainderBank::default();
        let mut naive = RemainderBank::default();
        let (mut honest_total, mut naive_total) = (0_u32, 0_u32);
        for window in 0..WINDOWS {
            let offered = honest.accrue(window, REMAINDER, Some(1.0));
            let granted = if refused(window) { 0 } else { offered };
            honest.debit(granted);
            honest_total += u32::try_from(granted).unwrap_or(0);

            let offered = naive.accrue(window, REMAINDER, Some(1.0));
            let granted = if refused(window) { 0 } else { offered };
            naive.debit(offered); // The mistake: the request, not the grant.
            naive_total += u32::try_from(granted).unwrap_or(0);
        }

        assert_eq!(
            honest_total, 100,
            "an honest debit admits the whole remainder: 200 windows of 0.5"
        );
        assert!(
            naive_total < honest_total,
            "debiting the request under-admits: {naive_total} against {honest_total}"
        );
    }

    /// Shares taken from one demand sample sum to one, so the banks of the fleet
    /// hold exactly one window's remainder between them.
    #[test]
    fn demand_shares_of_one_window_bank_its_whole_remainder() {
        const REMAINDER: f64 = 0.4;
        let total = 100_u128;
        let mines = [50_u128, 30, 20];

        let shares: f64 = mines
            .iter()
            .filter_map(|mine| DemandSample { mine: *mine, total }.share_fraction())
            .sum();
        assert!((shares - 1.0).abs() < 1e-9, "the shares sum to {shares}");

        let banked: f64 = mines
            .iter()
            .map(|mine| {
                let mut bank = RemainderBank::default();
                bank.accrue(
                    0,
                    REMAINDER,
                    DemandSample { mine: *mine, total }.share_fraction(),
                );
                bank.banked
            })
            .sum();
        assert!(
            (banked - REMAINDER).abs() < 1e-9,
            "the fleet banked {banked}, not one window's {REMAINDER}"
        );
    }

    /// A sample that cannot divide a share banks nothing. Reading a share of one
    /// there would have every replica bank the whole remainder, and the fleet
    /// would admit it once per replica.
    #[test]
    fn a_sample_that_cannot_divide_a_share_banks_nothing() {
        assert!(DemandSample::default().share_fraction().is_none());
        assert!(
            DemandSample { mine: 5, total: 0 }
                .share_fraction()
                .is_none(),
            "no cluster demand recorded: there is no share to take"
        );
        assert!(
            DemandSample { mine: 0, total: 9 }
                .share_fraction()
                .is_none(),
            "this replica asked for nothing"
        );

        let mut bank = RemainderBank::default();
        assert_eq!(bank.accrue(0, 0.9, None), 0);
        assert_eq!(bank.accrue(1, 0.9, None), 0);
        assert!(bank.banked.abs() < f64::EPSILON);
    }

    /// The lease path reaches a window on every refresh tick, and again as the
    /// pre-leased window. The remainder may be banked only once per window, or a
    /// fast tick rate would multiply the budget.
    #[test]
    fn a_window_banks_its_remainder_once_however_many_ticks_reach_it() {
        let mut bank = RemainderBank::default();
        for _ in 0..10 {
            assert_eq!(bank.accrue(7, 0.5, Some(1.0)), 0);
        }
        assert!((bank.banked - 0.5).abs() < 1e-9);
        // The pre-leased window is a window of its own and banks its own share.
        assert_eq!(bank.accrue(8, 0.5, Some(1.0)), 1);
    }

    /// A budget the coefficient has closed leases nothing. Every floor on the
    /// lease path has to agree: the one token `min_lease` used to guarantee is
    /// exactly the band the carry exists to remove.
    #[test]
    fn a_zero_budget_leases_no_tokens() {
        assert_eq!(min_lease(0), 0);
        assert_eq!(max_lease_per_replica(0), 0);
        assert_eq!(DemandSample::default().demand_signal(0, 10), 0);
        assert_eq!(DemandSample { mine: 3, total: 4 }.demand_signal(0, 0), 0);
        // A live budget keeps the floors it had.
        assert_eq!(min_lease(10), 1);
        assert_eq!(max_lease_per_replica(10), 9);
        assert_eq!(min_lease(600), 6);
    }

    /// Adaptive control only ever lowers the static limit, so a carried token
    /// must never lift the cluster above the configured burst.
    ///
    /// The bank outlives the window that filled it. A replica can therefore
    /// accrue a whole token while throttled, have the cluster refuse it, and
    /// still hold it in the window the coefficient recovers in — where the
    /// whole part is already the configured burst and there is no room for it.
    #[test]
    fn a_carried_token_never_lifts_the_cluster_above_the_configured_burst() {
        // Recovered: the whole part is the full burst and a token is banked.
        assert_eq!(replica_ceiling(1, 1, 1, 0), 1, "a carry cannot make it two");
        assert_eq!(replica_ceiling(600, 2, 600, 0), 600);

        // Throttled: the carry is what the bank exists for, so it still lands.
        assert_eq!(
            replica_ceiling(0, 1, 1, 0),
            1,
            "a closed budget still probes"
        );
        assert_eq!(replica_ceiling(4, 1, 10, 0), 5);

        // A peer that spent first closes the ceiling behind it.
        assert_eq!(replica_ceiling(4, 1, 10, 5), 0);
        assert_eq!(replica_ceiling(1, 1, 1, 1), 0);
    }

    /// End to end: a wholly failing origin at a configured burst of one leases
    /// nothing at all. `min_lease` used to floor the grant at one token, so the
    /// cluster kept sending at its configured rate however badly it failed.
    #[tokio::test]
    async fn a_wholly_failing_origin_at_a_burst_of_one_grants_nothing() {
        let window = Duration::from_millis(150);
        let mut cfg = config_for(1, "a", window);
        cfg.adaptive = Some(adaptive_config(1.0));
        let bucket = LeasedBucket::new(cfg);

        // Three windows in which every recorded request failed.
        for _ in 0..3 {
            bucket.refresh_lease().await.expect("lease");
            for _ in 0..20 {
                bucket.record_outcome(RequestOutcome::Failure);
            }
            tokio::time::sleep(window + Duration::from_millis(20)).await;
        }
        // One pass writes the tail counts back, the next leases against them.
        for _ in 0..2 {
            bucket.refresh_lease().await.expect("lease");
            tokio::time::sleep(window + Duration::from_millis(20)).await;
        }
        bucket.refresh_lease().await.expect("lease");

        assert_eq!(
            bucket.metrics.cluster_effective_burst(),
            0,
            "a total failure has to close the budget, not floor it at one"
        );
        assert_eq!(
            bucket.metrics.lease_granted(),
            0,
            "and the lease has to grant nothing"
        );
        assert!(bucket.is_throttling());
    }

    /// The budget is derived per replica and never written back, so it must
    /// not move under grants this replica has already issued: a second tick
    /// inside the same window leases against the budget of the first.
    #[tokio::test]
    async fn the_budget_of_a_window_is_held_for_the_life_of_the_window() {
        let store = Arc::new(InMemory::new());
        let window = Duration::from_millis(400);
        let mut cfg = config_for(10, "a", window);
        cfg.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        cfg.adaptive = Some(adaptive_config(1.0));

        let bucket = LeasedBucket::new(cfg);
        bucket.refresh_lease().await.expect("first tick leases");
        let first = bucket.metrics.cluster_effective_burst();
        let granted = bucket.metrics.lease_granted();

        // Enough failures to shrink the budget, were it recomputed now.
        for _ in 0..40 {
            bucket.record_outcome(RequestOutcome::Failure);
        }
        bucket.refresh_lease().await.expect("second tick leases");

        assert_eq!(
            bucket.metrics.cluster_effective_burst(),
            first,
            "the budget of a live window must not move under a grant already issued"
        );
        assert_eq!(
            bucket.metrics.lease_granted(),
            granted,
            "and the grant leased against it must not change either"
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
        cfg_a.adaptive = Some(adaptive_config(1.0));
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
        // Two passes: the first settles the tail counts in the file, the second
        // has both replicas read that same settled state.
        for _ in 0..2 {
            for bucket in [&a, &b] {
                bucket.refresh_lease().await.expect("lease");
            }
        }

        // Each replica fixes a window's budget when it first leases it, usually
        // as the pre-lease of the window before. The two read the shared counts
        // at different moments, so they can differ by one token, not more.
        let burst_a = a.metrics.cluster_effective_burst();
        let burst_b = b.metrics.cluster_effective_burst();
        assert!(
            burst_a.abs_diff(burst_b) <= 1,
            "replicas reading the same counts must derive budgets within one token, got {burst_a} and {burst_b}"
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

    /// A bucket without adaptive settings reports no admission ratio at all: a
    /// bucket that cannot throttle must not emit a series that reads like a
    /// live measurement.
    #[tokio::test]
    async fn a_bucket_without_adaptive_settings_reports_no_admission_ratio() {
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

    /// State written by a newer version is left alone: resetting it would wipe the
    /// newer peers' grants, and two versions resetting each other over-admit.
    #[tokio::test]
    async fn state_from_a_newer_version_is_not_overwritten() {
        use object_store::ObjectStoreExt;

        let store = Arc::new(InMemory::new());
        let mut config = config_for(10, "a", Duration::from_secs(1));
        config.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut newer = PersistedRateControlState::fresh(Duration::from_millis(1_000));
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
        let mut newer = PersistedRateControlState::fresh(Duration::from_millis(1_000));
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

        // The lease expires when the wall clock passes its last window, so wait for
        // that window to end: at most 2 s away with 1 s windows.
        let window = Duration::from_secs(1);
        let leased_through = bucket
            .inner
            .lock()
            .await
            .leased_through
            .expect("a lease was granted");
        assert!(
            window_id_for(SystemTime::now(), window) <= leased_through,
            "the lease must still cover the current window"
        );
        // A monotonic deadline bounds the wait even if the wall clock steps backwards.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while window_id_for(SystemTime::now(), window) <= leased_through {
            assert!(
                std::time::Instant::now() < deadline,
                "the wall clock did not pass window {leased_through} within 5 s"
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

        let mut older = PersistedRateControlState::fresh(Duration::from_millis(1_000));
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

    /// The key a quota is persisted under parses back to the quota's name and
    /// the configured limit: the name is what ties replicas that configure
    /// different limits back to one budget.
    #[test]
    fn limiter_keys_parse_back_to_the_quota_and_the_configured_limit() {
        for (name, quota, limit) in [
            (
                "requests_per_second",
                governor::Quota::per_second(NonZeroU32::new(10).expect("non-zero")),
                10,
            ),
            (
                "requests_per_minute",
                governor::Quota::per_minute(NonZeroU32::new(600).expect("non-zero")),
                600,
            ),
        ] {
            let key = crate::QuotaDefinition::new(Some(name.to_string()), quota)
                .persistence_key("unused");
            assert_eq!(
                LimiterKey::parse(&key),
                Some(LimiterKey { name, limit }),
                "{key}"
            );
        }
        assert_eq!(LimiterKey::parse("requests_per_second"), None);
        assert_eq!(
            LimiterKey::parse("requests_per_second:burst=ten:replenish_ns=1"),
            None
        );
    }

    /// Only limiters of the same quota are siblings: a per-minute limiter in
    /// the same file is a separate budget and must never cap the per-second one.
    #[test]
    fn siblings_are_the_other_limits_of_the_same_quota_only() {
        let mut state = PersistedRateControlState::fresh(Duration::from_secs(1));
        for (key, burst) in [
            ("requests_per_second:burst=20:replenish_ns=50000000", 20),
            ("requests_per_second:burst=10:replenish_ns=100000000", 10),
            ("requests_per_minute:burst=60:replenish_ns=1000000000", 1),
        ] {
            state.limiter_entry(key, burst);
        }

        let siblings: Vec<_> = state
            .siblings("requests_per_second:burst=20:replenish_ns=50000000")
            .map(|(key, limiter)| (key, limiter.burst_per_window))
            .collect();
        assert_eq!(
            siblings,
            vec![(
                LimiterKey {
                    name: "requests_per_second",
                    limit: 10
                },
                10
            )]
        );
    }

    /// The warning names the origin, the setting, this replica's value and every
    /// other one, and the value the cluster is held to; the notice that closes
    /// it names the value the replicas agree on.
    #[test]
    fn limit_log_lines_name_the_origin_the_setting_and_every_value() {
        let origin = "http://127.0.0.1:37081";
        let key = LimiterKey {
            name: "requests_per_second",
            limit: 20,
        };
        assert_eq!(
            limits_differ_warning(origin, key, &BTreeSet::from([10])),
            "Instances sharing cluster rate control for origin 'http://127.0.0.1:37081' set `requests_per_second_limit` to different values (this instance: 20; other instances: 10), so the cluster is held to the lowest value, 10, until every instance sets the same one. Set the same `requests_per_second_limit` on every instance that shares `runtime.state.location`. See: https://spiceai.org/docs/reference/spicepod/runtime#runtimesource_rate_control"
        );
        assert_eq!(
            limits_differ_warning(origin, key, &BTreeSet::from([30, 25, 40])),
            "Instances sharing cluster rate control for origin 'http://127.0.0.1:37081' set `requests_per_second_limit` to different values (this instance: 20; other instances: 25, 30 and 40), so the cluster is held to the lowest value, 20, until every instance sets the same one. Set the same `requests_per_second_limit` on every instance that shares `runtime.state.location`. See: https://spiceai.org/docs/reference/spicepod/runtime#runtimesource_rate_control"
        );
        assert_eq!(
            limits_agree_notice(origin, key),
            "Instances sharing cluster rate control for origin 'http://127.0.0.1:37081' now all set `requests_per_second_limit` to 20, so the cluster is held to 20."
        );
    }

    /// A change in the other replicas' limits is reported once: a warning
    /// naming them when they start to differ or change, a note when they agree
    /// again, and nothing on the ticks in between.
    #[tokio::test]
    async fn limit_changes_are_reported_once() {
        let bucket = per_second_bucket(&Arc::new(InMemory::new()), "a", 20, Duration::from_secs(1));
        let key = LimiterKey {
            name: "requests_per_second",
            limit: 20,
        };
        let differ = |others: &[u64]| {
            Some(SiblingLimitsChange::Differ(limits_differ_warning(
                "https://example.com",
                key,
                &others.iter().copied().collect(),
            )))
        };
        let agree = Some(SiblingLimitsChange::Agree(limits_agree_notice(
            "https://example.com",
            key,
        )));

        let reports: Vec<_> = [&[][..], &[10], &[10], &[10, 15], &[10, 15], &[], &[]]
            .into_iter()
            .map(|others| bucket.sibling_limits_change(&others.iter().copied().collect()))
            .collect();
        assert_eq!(
            reports,
            vec![
                None,
                differ(&[10]),
                None,
                differ(&[10, 15]),
                None,
                agree,
                None
            ]
        );
    }

    /// The config of a bucket for the per-second quota at `limit`, keyed the
    /// way the controller keys it, leasing `limit` tokens per window from
    /// `store`.
    fn per_second_config(
        store: &Arc<InMemory>,
        instance: &str,
        limit: u32,
        window: Duration,
    ) -> LeasedBucketConfig {
        let quota = crate::QuotaDefinition::new(
            Some("requests_per_second".to_string()),
            governor::Quota::per_second(NonZeroU32::new(limit).expect("non-zero limit")),
        );
        let mut config = config_for(u64::from(limit), instance, window);
        config.store = Arc::clone(store) as Arc<dyn ObjectStore>;
        config.limiter_key = quota.persistence_key("unused");
        config
    }

    fn per_second_bucket(
        store: &Arc<InMemory>,
        instance: &str,
        limit: u32,
        window: Duration,
    ) -> Arc<LeasedBucket> {
        LeasedBucket::new(per_second_config(store, instance, limit, window))
    }

    /// Tokens every limiter in the shared file holds for `window_id`.
    async fn cluster_granted(bucket: &LeasedBucket, window_id: u64) -> u64 {
        bucket
            .read_state()
            .await
            .expect("read the shared state")
            .expect("the shared state exists")
            .limiters
            .values()
            .map(|limiter| limiter.granted_in(window_id))
            .sum()
    }

    /// Lease as a replica with requests waiting on it.
    async fn refresh_with_demand(bucket: &Arc<LeasedBucket>) {
        bucket.inner.lock().await.attempted_this_window = 50;
        bucket.refresh_lease().await.expect("lease refresh");
    }

    /// Tokens `instance` holds in `window_id`, under whichever limiter.
    async fn granted_to(bucket: &LeasedBucket, window_id: u64, instance: &str) -> u64 {
        bucket
            .read_state()
            .await
            .expect("read the shared state")
            .expect("the shared state exists")
            .limiters
            .values()
            .filter_map(|limiter| limiter.windows.get(&window_id.to_string()))
            .map(|window| window.granted_for(instance))
            .sum()
    }

    /// A rolling deployment that lowers a limit: replicas configured at 20 and
    /// at 10 for one quota lease under different keys of one file, and are held
    /// together to the lower limit rather than each to its own, sharing it by
    /// demand. Once the replica at 10 stops, the one at 20 leases against its
    /// own limit again.
    ///
    /// The two refresh in alternating order: when the replica at 20 pre-leases
    /// first, the one at 10 holds a lease only in the window before, and must
    /// still count.
    ///
    /// Regression test for #14913: each configuration leased only against its
    /// own key, so the cluster sent the sum of both limits.
    #[tokio::test]
    async fn replicas_configuring_different_limits_are_held_to_the_lowest() {
        let window = Duration::from_millis(200);
        let store = Arc::new(InMemory::new());
        let high = per_second_bucket(&store, "high", 20, window);
        let low = per_second_bucket(&store, "low", 10, window);

        // The replica at 20 runs alone and holds most of its own limit.
        let start = window_id_for(SystemTime::now(), window) + 1;
        for target in start..start + 2 {
            wait_for_window(target, window).await;
            refresh_with_demand(&high).await;
        }
        assert_eq!(
            cluster_granted(&high, start + 1).await,
            max_lease_per_replica(20)
        );

        // The replica at 10 joins. Grants already issued stand, so the window
        // it joins in keeps the earlier grant; from the next window on the two
        // together hold at most 10. Each replica's demand-weighted share rounds
        // down, so one token can go unclaimed.
        let joined = start + 2;
        for target in joined..joined + 6 {
            wait_for_window(target, window).await;
            let order = if (target - joined).is_multiple_of(2) {
                [&low, &high]
            } else {
                [&high, &low]
            };
            for bucket in order {
                refresh_with_demand(bucket).await;
            }
            for window_id in [target, target + 1] {
                if window_id == joined {
                    continue;
                }
                let granted = cluster_granted(&high, window_id).await;
                assert!(
                    (9..=10).contains(&granted),
                    "window {window_id}: replicas configured at 20 and 10 hold {granted} tokens, expected 9 or 10"
                );
            }
        }

        // Each replica has reported the other's limit.
        assert_eq!(*high.reported_sibling_limits.lock(), BTreeSet::from([10]));
        assert_eq!(*low.reported_sibling_limits.lock(), BTreeSet::from([20]));

        // Both have asked for the same since the replica at 10 joined, so
        // neither is left with the scraps of the other's grant.
        let low_stopped = joined + 5;
        for instance in ["high", "low"] {
            let granted = granted_to(&high, low_stopped + 1, instance).await;
            assert!(
                granted >= 3,
                "{instance} holds {granted} of the 10 tokens of window {}",
                low_stopped + 1
            );
        }

        // The replica at 10 stops. Three windows after its last refresh it no
        // longer holds the cluster to its limit.
        for target in low_stopped + 1..=low_stopped + 4 {
            wait_for_window(target, window).await;
            refresh_with_demand(&high).await;
        }
        assert_eq!(high.metrics.lease_granted(), max_lease_per_replica(20));

        // The replica left reports that every replica it shares the quota with
        // agrees with it again; see `limit_changes_are_reported_once`.
        assert_eq!(*high.reported_sibling_limits.lock(), BTreeSet::new());
    }

    /// Replicas that start together, with requests waiting and no history,
    /// each claim the whole budget they see. The second to lease counts what
    /// the first, under the other limit, already holds.
    #[tokio::test]
    async fn replicas_starting_together_count_each_others_grants() {
        let window = Duration::from_millis(200);
        let store = Arc::new(InMemory::new());
        let high = per_second_bucket(&store, "high", 20, window);
        let low = per_second_bucket(&store, "low", 10, window);

        let start = window_id_for(SystemTime::now(), window) + 1;
        wait_for_window(start, window).await;
        refresh_with_demand(&low).await;
        refresh_with_demand(&high).await;

        for window_id in [start, start + 1] {
            assert_eq!(
                (
                    granted_to(&high, window_id, "low").await,
                    granted_to(&high, window_id, "high").await
                ),
                (max_lease_per_replica(10), 10 - max_lease_per_replica(10)),
                "window {window_id}"
            );
        }
    }

    /// Upstream failures recorded under one limit throttle the replicas under
    /// the other as well. They were observed against the same origin, and a
    /// replica that ignored them would keep leasing the budget the failing one
    /// gave up.
    #[tokio::test]
    async fn outcomes_recorded_under_one_limit_throttle_the_other() {
        let window = Duration::from_millis(150);
        let store = Arc::new(InMemory::new());
        let adaptive_bucket = |instance, limit| {
            let mut config = per_second_config(&store, instance, limit, window);
            config.adaptive = Some(adaptive_config(1.0));
            LeasedBucket::new(config)
        };
        let high = adaptive_bucket("high", 20);
        let low = adaptive_bucket("low", 10);

        // Three windows in which only the replica at 10 reaches the origin, and
        // sees 2 successes to 8 failures: well past the 50% threshold.
        for _ in 0..3 {
            for bucket in [&low, &high] {
                bucket.refresh_lease().await.expect("lease");
            }
            for _ in 0..2 {
                low.record_outcome(RequestOutcome::Success);
            }
            for _ in 0..8 {
                low.record_outcome(RequestOutcome::Failure);
            }
            tokio::time::sleep(window + Duration::from_millis(20)).await;
        }
        // Write back the tail counts of the last window, then have both
        // replicas lease against the same settled state.
        for bucket in [&low, &high] {
            bucket.refresh_lease().await.expect("lease");
        }
        tokio::time::sleep(window + Duration::from_millis(20)).await;
        for _ in 0..2 {
            for bucket in [&low, &high] {
                bucket.refresh_lease().await.expect("lease");
            }
        }

        assert!(
            high.is_throttling() && low.is_throttling(),
            "both replicas must throttle: high {:?}, low {:?}",
            high.admission_coefficient(),
            low.admission_coefficient()
        );
        let (burst_high, burst_low) = (
            high.metrics.cluster_effective_burst(),
            low.metrics.cluster_effective_burst(),
        );
        assert!(
            burst_high.abs_diff(burst_low) <= 1 && burst_high < 10,
            "both replicas lease the throttled lower limit, got high {burst_high} and low {burst_low}"
        );
    }

    /// A held budget falls to a lower burst at the coefficient it was derived
    /// at, and never rises above the burst it was derived at.
    #[test]
    fn a_held_budget_follows_a_lower_burst_at_its_coefficient() {
        let throttled = HeldBudget {
            burst: 20,
            coefficient: Some(0.45),
        };
        let lowered = throttled.at(10);
        assert_eq!(lowered.whole, 4);
        assert!((lowered.remainder - 0.5).abs() < 1e-9, "{lowered:?}");
        let raised = throttled.at(30);
        assert_eq!(raised.whole, 9);
        assert!(raised.remainder.abs() < 1e-9, "{raised:?}");

        let unthrottled = HeldBudget {
            burst: 20,
            coefficient: None,
        };
        assert_eq!(unthrottled.at(10).whole, 10);
        assert_eq!(unthrottled.at(30).whole, 20);
    }

    /// A sibling counts while some replica holds a lease under it within two
    /// windows of the one being leased, either side, and not further away.
    #[test]
    fn a_sibling_is_leasing_within_two_windows_of_a_lease() {
        let lease = PersistedLease {
            granted: 0,
            consumed: 0,
            attempted: 0,
            ok: None,
            failed: None,
            expires_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let mut limiter = PersistedLimiter::new(10);
        limiter.window_entry(100, 10);
        assert!(
            !limiter.is_leasing(100),
            "a window without a lease is not evidence of a replica"
        );
        limiter
            .window_entry(100, 10)
            .leases
            .insert("a".to_string(), lease);

        let leasing: Vec<u64> = (96..=104)
            .filter(|window_id| limiter.is_leasing(*window_id))
            .collect();
        assert_eq!(leasing, vec![98, 99, 100, 101, 102]);
    }

    /// A limiter no replica has written for the retention horizon is dropped,
    /// whatever its window ids; the replica's own limiter and every limiter
    /// written since are kept.
    #[test]
    fn retired_limiters_are_dropped_after_the_retention_horizon() {
        let lease_written_at = |updated_at_unix_ms| PersistedLease {
            granted: 1,
            consumed: 0,
            attempted: 0,
            ok: None,
            failed: None,
            expires_at_unix_ms: updated_at_unix_ms,
            updated_at_unix_ms,
        };
        let mut state = PersistedRateControlState::fresh(Duration::from_secs(1));
        for (key, window_id, written_at) in [
            (
                "requests_per_second:burst=10:replenish_ns=100000000",
                100,
                100_000,
            ),
            (
                "requests_per_second:burst=20:replenish_ns=50000000",
                159,
                159_000,
            ),
            (
                "requests_per_second:burst=30:replenish_ns=33333333",
                7,
                160_000,
            ),
            (
                "requests_per_minute:burst=60:replenish_ns=1000000000",
                99,
                99_999,
            ),
        ] {
            state
                .limiter_entry(key, 1)
                .window_entry(window_id, 1)
                .leases
                .insert("replica".to_string(), lease_written_at(written_at));
        }
        state.limiter_entry("requests_per_second:burst=40:replenish_ns=25000000", 1);

        state.drop_retired_limiters(
            "requests_per_second:burst=40:replenish_ns=25000000",
            100_000,
        );

        let mut kept: Vec<&str> = state.limiters.keys().map(String::as_str).collect();
        kept.sort_unstable();
        assert_eq!(
            kept,
            vec![
                "requests_per_second:burst=10:replenish_ns=100000000",
                "requests_per_second:burst=20:replenish_ns=50000000",
                "requests_per_second:burst=30:replenish_ns=33333333",
                "requests_per_second:burst=40:replenish_ns=25000000",
            ]
        );
    }

    /// Repeated limit changes leave no trail: each refresh drops the limiters
    /// of configurations that stopped leasing more than the retention horizon
    /// ago, so the shared file holds only the live ones.
    #[tokio::test]
    async fn limiters_of_retired_limits_leave_the_shared_file() {
        let window = Duration::from_millis(20);
        let horizon =
            window * u32::try_from(STALE_WINDOW_RETENTION).expect("retention fits in u32");
        let store = Arc::new(InMemory::new());
        let live = per_second_bucket(&store, "live", 10, window);

        // Five rolling changes, each leasing once under its own limit and then
        // stopping.
        for limit in 11..16 {
            refresh_with_demand(&per_second_bucket(&store, "retired", limit, window)).await;
        }
        refresh_with_demand(&live).await;
        let limiters = |state: PersistedRateControlState| {
            let mut keys: Vec<String> = state.limiters.into_keys().collect();
            keys.sort_unstable();
            keys
        };
        let state = live
            .read_state()
            .await
            .expect("read the shared state")
            .expect("the shared state exists");
        assert_eq!(limiters(state).len(), 6);

        // The live replica keeps leasing past the horizon.
        let deadline = tokio::time::Instant::now() + horizon + window * 10;
        while tokio::time::Instant::now() < deadline {
            refresh_with_demand(&live).await;
            tokio::time::sleep(window).await;
        }
        let state = live
            .read_state()
            .await
            .expect("read the shared state")
            .expect("the shared state exists");
        assert_eq!(
            limiters(state),
            vec!["requests_per_second:burst=10:replenish_ns=100000000".to_string()]
        );
    }

    /// Replicas whose clocks run a window apart, each leasing the windows its
    /// own clock reads, still find each other: the one under the higher limit
    /// is held to the lower one in every window it leases once it has seen the
    /// other, whichever replica runs ahead.
    #[test]
    fn replicas_a_window_apart_are_held_to_the_lowest() {
        let window = Duration::from_secs(1);
        let start = 100;
        let start_time = UNIX_EPOCH + Duration::from_millis(100_300);
        let busy = WindowCounts {
            attempted: 50,
            ..WindowCounts::default()
        };

        for (ahead_limit, behind_limit) in [(10, 20), (20, 10)] {
            let store = Arc::new(InMemory::new());
            let ahead = per_second_bucket(&store, "ahead", ahead_limit, window);
            let behind = per_second_bucket(&store, "behind", behind_limit, window);
            let mut state = PersistedRateControlState::fresh(window);

            // Each round, both lease their current window and pre-lease the
            // next, the replica ahead first, by clocks a window apart.
            for round in 0..5 {
                for (bucket, current, now) in [
                    (&ahead, start + round + 1, start_time + window * (round + 1)),
                    (&behind, start + round, start_time + window * round),
                ] {
                    bucket.process_window(&mut state, u64::from(current), now, busy, 50);
                    bucket.process_window(
                        &mut state,
                        u64::from(current) + 1,
                        now,
                        WindowCounts::default(),
                        50,
                    );
                }
            }

            // A replica at 20 that leased before the one at 10 existed keeps
            // those grants; every window either leased after that holds at most
            // 10. When the replica at 10 runs ahead, the one at 20 has seen it
            // before its first lease.
            let first_held = if ahead_limit == 10 { start } else { start + 3 };
            for window_id in u64::from(first_held)..=u64::from(start) + 5 {
                let granted: u64 = state
                    .limiters
                    .values()
                    .map(|limiter| limiter.granted_in(window_id))
                    .sum();
                assert!(
                    granted <= 10,
                    "replica at {ahead_limit} a window ahead of one at {behind_limit}: window {window_id} holds {granted} tokens, above the lower limit of 10"
                );
            }
        }
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
            let now_window = window_id_for(SystemTime::now(), window);
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
        let start = window_id_for(SystemTime::now(), window);
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
