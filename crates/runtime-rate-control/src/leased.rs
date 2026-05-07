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
//! to its lease at any pace; once exhausted it waits for the next window or a
//! lease refresh (whichever happens first).
//!
//! Schema is `PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION = 2`. v1 state is
//! treated as empty (with a warning); the previous PR was never shipped so no
//! migration is required.

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

pub(crate) const PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION: u32 = 2;

const MAX_LEASE_RETRIES: usize = 3;
/// Number of windows of history to retain in the persisted file.
const STALE_WINDOW_RETENTION: u64 = 60;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to read persisted rate-control state for origin {origin}. {source}"
    ))]
    Read {
        origin: String,
        source: Box<object_store_occ::Error>,
    },

    #[snafu(display(
        "Failed to write persisted rate-control state for origin {origin}. {source}"
    ))]
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
    /// Last reported within-window consumption; used by peers' EWMA estimates
    /// (informational only).
    #[serde(default)]
    pub consumed: u64,
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
            .finish()
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
    /// Timestamp the current lease was last refreshed (ms).
    last_lease_refresh_ms: u64,
    /// Wall-clock end of the current lease (ms).
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
        let object_state =
            Arc::new(ObjectState::new(Arc::clone(&config.store)).with_prefix(config.prefix.clone()));
        let now_ms = unix_millis_now();
        let window_id = window_id_for(now_ms, &config.window_duration);
        Arc::new(Self {
            object_state,
            inner: Mutex::new(LeasedBucketInner {
                current_window_id: window_id,
                granted_this_window: 0,
                consumed_this_window: 0,
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
        loop {
            let now_ms = unix_millis_now();
            let now_window = window_id_for(now_ms, &self.config.window_duration);

            let wait = {
                let mut inner = self.inner.lock().await;

                self.roll_window_locked(&mut inner, now_window);

                if inner.consumed_this_window < inner.granted_this_window {
                    inner.consumed_this_window += 1;
                    return Ok(());
                }

                // Out of local lease.
                // Fail-closed condition: store is failing and our lease has fully expired.
                if inner.last_attempt_failed && now_ms >= inner.lease_expires_at_ms {
                    self.metrics.fail_closed_total.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::FailClosed {
                        origin: self.config.origin.clone(),
                    });
                }

                // Otherwise wait. Cap wait at a fraction of the window so we
                // re-check window roll even if no notify arrives (e.g. replica
                // is the only one consuming and the persistence task is slow).
                self.config.window_duration / 4
            };

            let _ = tokio::time::timeout(wait, self.notify.notified()).await;
        }
    }

    /// Roll over per-window state if the window has changed. Caller must hold
    /// the inner lock.
    fn roll_window_locked(&self, inner: &mut LeasedBucketInner, now_window: u64) {
        if now_window != inner.current_window_id {
            inner.current_window_id = now_window;
            inner.consumed_this_window = 0;
            inner.granted_this_window = 0;
            // last_lease_refresh_ms / lease_expires_at_ms preserved so
            // fail-closed logic can compare against the old expiry.
        }
    }

    /// Refresh the lease for the current window. Called by the persistence
    /// task on a timer and at controller-build time.
    pub async fn refresh_lease(self: &Arc<Self>) -> Result<()> {
        let started = std::time::Instant::now();
        let now_ms = unix_millis_now();
        let window_ms = duration_millis_u64(self.config.window_duration);
        let now_window = window_id_for(now_ms, &self.config.window_duration);
        let window_end_ms = (now_window + 1).saturating_mul(window_ms);

        let last_consumed_for_publish = {
            let inner = self.inner.lock().await;
            inner.consumed_this_window
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

            // If existing schema is not v2, treat as empty (logged once in caller).
            if state.schema_version != PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION {
                state = fresh_state(window_ms);
            }
            state.window_ms = window_ms;
            state.schema_version = PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION;
            state.updated_at_unix_ms = now_ms;

            let limiter = state
                .limiters
                .entry(self.config.limiter_key.clone())
                .or_insert_with(|| PersistedLimiter {
                    burst_per_window: self.config.burst_per_window,
                    windows: HashMap::new(),
                });
            limiter.burst_per_window = self.config.burst_per_window;

            // Drop windows older than retention horizon.
            limiter
                .windows
                .retain(|id, _| id.parse::<u64>().ok().is_some_and(|id| id + STALE_WINDOW_RETENTION >= now_window));

            let window_key = now_window.to_string();
            let window = limiter.windows.entry(window_key.clone()).or_insert_with(|| {
                PersistedWindow {
                    budget_remaining: self.config.burst_per_window,
                    leases: HashMap::new(),
                }
            });

            // Drop expired leases in this window.
            window.leases.retain(|_, lease| lease.expires_at_unix_ms > now_ms);

            // Recompute budget_remaining from surviving leases (defensive).
            let leased: u64 = window.leases.values().map(|l| l.granted).sum();
            window.budget_remaining = self.config.burst_per_window.saturating_sub(leased);

            let my_existing = window
                .leases
                .get(&self.config.instance_id)
                .map(|l| l.granted)
                .unwrap_or(0);

            // Fair-share demand: divide remaining cluster budget evenly across
            // active replicas (including self). Clamped to
            // [MIN_LEASE, MAX_LEASE_PER_REPLICA].
            let n_peers = window
                .leases
                .keys()
                .filter(|id| id.as_str() != self.config.instance_id.as_str())
                .count() as u64;
            let n_active = n_peers + 1; // including self
            let fair_share = self.config.burst_per_window / n_active;
            let demand = fair_share
                .max(min_lease(self.config.burst_per_window))
                .min(max_lease_per_replica(self.config.burst_per_window));

            let want_extra = demand
                .saturating_sub(my_existing)
                .min(window.budget_remaining);
            let new_grant = my_existing.saturating_add(want_extra);

            let must_write = want_extra > 0
                || my_existing == 0
                || !window.leases.contains_key(&self.config.instance_id);

            if !must_write {
                // Update local granted in case window rolled silently.
                let mut inner = self.inner.lock().await;
                self.roll_window_locked(&mut inner, now_window);
                inner.granted_this_window = new_grant;
                inner.lease_expires_at_ms = window_end_ms;
                inner.last_lease_refresh_ms = now_ms;
                inner.last_attempt_failed = false;
                drop(inner);
                self.metrics
                    .lease_granted
                    .store(new_grant, Ordering::Relaxed);
                self.metrics
                    .cluster_budget_remaining
                    .store(window.budget_remaining, Ordering::Relaxed);
                self.metrics.last_lease_acquire_micros.store(
                    started.elapsed().as_micros() as u64,
                    Ordering::Relaxed,
                );
                return Ok(());
            }

            window.budget_remaining = window.budget_remaining.saturating_sub(want_extra);
            window.leases.insert(
                self.config.instance_id.clone(),
                PersistedLease {
                    granted: new_grant,
                    consumed: last_consumed_for_publish,
                    expires_at_unix_ms: window_end_ms,
                    updated_at_unix_ms: now_ms,
                },
            );

            let remaining_after = window.budget_remaining;

            match self.write_state(state).await {
                Ok(WriteOutcome::Written) => {
                    let mut inner = self.inner.lock().await;
                    self.roll_window_locked(&mut inner, now_window);
                    inner.granted_this_window = new_grant;
                    inner.lease_expires_at_ms = window_end_ms;
                    inner.last_lease_refresh_ms = now_ms;
                    inner.last_attempt_failed = false;
                    drop(inner);

                    self.metrics
                        .lease_granted
                        .store(new_grant, Ordering::Relaxed);
                    self.metrics
                        .cluster_budget_remaining
                        .store(remaining_after, Ordering::Relaxed);
                    self.metrics.last_lease_acquire_micros.store(
                        started.elapsed().as_micros() as u64,
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
    // Half the budget — leaves room for at least one peer to grab a lease in
    // the next OCC round even after a single greedy first-mover.
    (burst_per_window / 2).max(1)
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn unix_millis_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => duration_millis_u64(d),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn single_replica_lease_grants_max_per_replica() {
        let bucket = LeasedBucket::new(config_for(10, "a", Duration::from_secs(1)));
        bucket.refresh_lease().await.expect("lease should succeed");
        let inner = bucket.inner.lock().await;
        // Single replica: fair share = burst / 1, clamped to MAX_LEASE_PER_REPLICA = burst/2.
        assert_eq!(inner.granted_this_window, max_lease_per_replica(10));
        assert_eq!(
            bucket.metrics.lease_granted(),
            max_lease_per_replica(10)
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
}
