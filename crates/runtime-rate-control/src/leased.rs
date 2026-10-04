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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
    }

    /// Raise the published counts to at least `attempted`/`consumed`. Returns
    /// whether anything changed.
    fn merge_counts(&mut self, counts: WindowCounts, now: SystemTime) -> bool {
        if counts.attempted <= self.attempted && counts.consumed <= self.consumed {
            return false;
        }
        self.attempted = self.attempted.max(counts.attempted);
        self.consumed = self.consumed.max(counts.consumed);
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
}

/// Final demand and consumption of a window that rolled before its tail was
/// published.
#[derive(Debug, Clone, Copy)]
struct PendingPublish {
    window_id: u64,
    counts: WindowCounts,
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
        if self.attempted_this_window > 0 || self.consumed_this_window > 0 {
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
}

impl LeasedBucket {
    pub fn new(config: LeasedBucketConfig) -> Arc<Self> {
        let object_state = Arc::new(
            ObjectState::new(Arc::clone(&config.store)).with_prefix(config.prefix.clone()),
        );
        let window_id = window_id_for(SystemTime::now(), config.window_duration);
        Arc::new(Self {
            object_state,
            inner: Mutex::new(LeasedBucketInner::new(window_id)),
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

            if let Some(limiter) = state.limiter_mut(&self.config.limiter_key) {
                limiter.retain_recent_windows(now_window);

                // Publish any counts latched at a window roll between refresh
                // ticks. Best-effort: if the rolled-out window has already
                // been pruned from state, drop the pending values and move on.
                if let Some(pending) = pending_publish
                    && let Some(lease) = limiter
                        .window_mut(pending.window_id)
                        .and_then(|window| window.lease_mut(&self.config.instance_id))
                {
                    lease.merge_counts(pending.counts, now);
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

            let dirty = current.dirty || next.dirty;
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

        // Read the smoothed demand signal before taking a mutable borrow on
        // the window below.
        let demand = limiter
            .ewma_demand(instance, now_window, burst, DEMAND_EWMA_LOOKBACK_WINDOWS)
            .demand_signal(burst, local_demand_hint)
            .max(min_lease(burst))
            .min(max_lease_per_replica(burst));

        let window = limiter.window_entry(window_id, burst);

        // Only the current window can hold expired leases; a future window's
        // lease cannot have expired yet.
        window.drop_expired(now);
        window.recompute_budget_remaining(burst);

        let my_existing = window.granted_for(instance);
        let max_possible_for_me = burst.saturating_sub(window.granted_by_others(instance));

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
        let dirty = window.publish(
            instance,
            PersistedLease {
                granted: new_grant,
                consumed: counts.consumed,
                attempted: counts.attempted,
                expires_at_unix_ms: window_end_ms,
                updated_at_unix_ms: unix_millis(now),
            },
        );

        window.recompute_budget_remaining(burst);

        WindowOutcome {
            granted: new_grant,
            budget_remaining_after: window.budget_remaining,
            dirty,
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
    /// Whether the persisted state was modified and must therefore be written
    /// back. Two reasons we'd skip a write: (a) we already had a lease at the
    /// desired size in this window from a previous tick, or (b) demand is
    /// zero. Combined-window dirty status drives the OCC write decision.
    dirty: bool,
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
        let nanos = window.as_nanos() / u128::from(granted);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
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
                expires_at_unix_ms: 1_700_000_001_000,
                updated_at_unix_ms: 1_700_000_000_500,
            },
        );
        leases.insert(
            "replica-b".to_string(),
            PersistedLease {
                granted: 2,
                consumed: 0,
                attempted: 0,
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
    /// Update this snapshot only alongside a `schema_version` bump.
    #[test]
    fn persisted_state_json_is_stable() {
        assert_snapshot!(
            "persisted_state_json_is_stable",
            canonical_json(&wire_format_fixture())
        );
    }

    /// A grant wider than `u32` must still divide the whole window: narrowing
    /// it would widen the spacing and pace a replica below its lease.
    #[test]
    fn pacing_interval_divides_by_the_full_grant() {
        let window = Duration::from_secs(10);
        assert_eq!(
            pacing_interval(window, 10_000_000_000),
            Duration::from_nanos(1)
        );
        assert_eq!(
            pacing_interval(window, u64::from(u32::MAX) + 1),
            Duration::from_nanos(2)
        );
        assert_eq!(pacing_interval(window, 4), Duration::from_millis(2500));
        assert_eq!(pacing_interval(window, 0), window);
    }

    #[test]
    fn persisted_state_round_trips() {
        let state = wire_format_fixture();
        let encoded = serde_json::to_string(&state).expect("state serializes");
        let decoded: PersistedRateControlState =
            serde_json::from_str(&encoded).expect("state deserializes");
        assert_eq!(state, decoded);
    }

    /// `consumed` and `attempted` are `#[serde(default)]`. A file written by a
    /// replica that predates them must still load, with the counts at zero.
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
