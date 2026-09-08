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

//! Per-origin HTTP rate-controller used by data connectors.
//!
//! Two modes:
//! * **In-memory** (default): a local `governor` rate limiter per quota, plus
//!   an optional concurrency semaphore. No coordination across replicas.
//! * **Cluster** (when `with_object_store_persistence_for_instance` is
//!   configured): each named quota is enforced by a `LeasedBucket` which
//!   negotiates a per-window slice of the cluster-wide budget through OCC
//!   writes to the configured `object_store`. See [`leased`].
//!
//! When cluster mode is enabled, the configured `requests_per_second_limit` /
//! `requests_per_minute_limit` is interpreted as the **cluster-wide** limit,
//! not per replica. This is a deliberate semantics change from the standalone
//! mode and is documented in the user-facing docs.

use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use std::sync::atomic::{AtomicU64, Ordering};

use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use object_store::ObjectStore;
use snafu::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

mod leased;

use leased::{LeasedBucket, LeasedBucketConfig, LeasedBucketMetrics};

const DEFAULT_PERSISTED_INSTANCE_TTL: Duration = Duration::from_secs(90);

type GovernorRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to acquire semaphore permit. {source}"))]
    SemaphoreAcquireError { source: tokio::sync::AcquireError },

    #[snafu(display(
        "Cluster rate-control budget exhausted for origin {origin}; persisted store is unavailable and the lease has expired"
    ))]
    ClusterBudgetExhausted { origin: String },

    #[snafu(display("Failed to refresh cluster rate-control lease for origin {origin}. {source}"))]
    LeaseRefresh {
        origin: String,
        source: Box<leased::Error>,
    },

    #[snafu(display(
        "The rate limiter has insufficient capacity for a request with weight '{weight}'. Reduce the request size, or increase the rate limit, and try again."
    ))]
    InsufficientCapacity { weight: u32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
struct QuotaDefinition {
    name: Option<String>,
    quota: Quota,
}

impl QuotaDefinition {
    fn new(name: Option<String>, quota: Quota) -> Self {
        Self { name, quota }
    }

    fn persistence_key(&self, fallback_name: &str) -> String {
        let name = self.name.as_deref().unwrap_or(fallback_name);
        format!(
            "{name}:burst={}:replenish_ns={}",
            self.quota.burst_size().get(),
            self.quota.replenish_interval().as_nanos()
        )
    }

    /// Cluster-wide tokens permitted in one `window_duration`.
    fn burst_per_window(&self, window: Duration) -> u64 {
        // Quota replenishes one token every `replenish_interval`. So in
        // `window` time, `window / replenish_interval` tokens are permitted.
        // Round to nearest, but at least 1.
        let replenish_ns = self.quota.replenish_interval().as_nanos().max(1);
        let window_ns = window.as_nanos();
        let tokens = window_ns / replenish_ns;
        u64::try_from(tokens).unwrap_or(u64::MAX).max(1)
    }
}

#[derive(Clone)]
struct PersistenceConfig {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    object_key: String,
    origin: String,
    instance_id: String,
    /// Window length for the leased bucket model. Set from
    /// `runtime.source_rate_control.refresh_interval`. Reused as the lease
    /// refresh cadence — one read/write per window per replica per origin.
    window_duration: Duration,
}

impl std::fmt::Debug for PersistenceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistenceConfig")
            .field("prefix", &self.prefix)
            .field("object_key", &self.object_key)
            .field("origin", &self.origin)
            .field("instance_id", &self.instance_id)
            .field("window_duration", &self.window_duration)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct JitterConfig {
    min: Duration,
    max: Duration,
}

impl JitterConfig {
    #[must_use]
    pub fn new(min: Duration, max: Duration) -> Self {
        Self { min, max }
    }

    #[must_use]
    pub fn zero() -> Self {
        Self::new(Duration::ZERO, Duration::ZERO)
    }
}

#[derive(Debug, Default)]
pub struct RateControllerBuilder {
    jitter: Option<JitterConfig>,
    max_concurrent_requests: Option<usize>,
    quotas: Vec<QuotaDefinition>,
    weighted_quota: Option<QuotaDefinition>,
    metrics: Option<Arc<RateControllerMetrics>>,
    persistence: Option<PersistenceConfig>,
}

impl RateControllerBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_weighted_quota(mut self, quota: Quota) -> Self {
        self.weighted_quota = Some(QuotaDefinition::new(Some("weighted".to_string()), quota));
        self
    }

    #[must_use]
    pub fn with_jitter(mut self, jitter: JitterConfig) -> Self {
        self.jitter = Some(jitter);
        self
    }

    #[must_use]
    pub fn with_max_concurrent_requests(mut self, max_concurrent_requests: usize) -> Self {
        self.max_concurrent_requests = Some(max_concurrent_requests);
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<RateControllerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn add_quota(mut self, quota: Quota) -> Self {
        self.quotas.push(QuotaDefinition::new(None, quota));
        self
    }

    #[must_use]
    pub fn add_quota_with_name(mut self, name: impl Into<String>, quota: Quota) -> Self {
        self.quotas
            .push(QuotaDefinition::new(Some(name.into()), quota));
        self
    }

    #[must_use]
    pub fn with_quotas(mut self, quotas: Vec<Quota>) -> Self {
        self.quotas = quotas
            .into_iter()
            .map(|quota| QuotaDefinition::new(None, quota))
            .collect();
        self
    }

    /// Configure cluster-mode persistence for this controller. The configured
    /// quotas become **cluster-wide** budgets enforced via OCC writes to the
    /// supplied object store.
    #[must_use]
    pub fn with_object_store_persistence(
        self,
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        object_key: impl Into<String>,
        origin: impl Into<String>,
    ) -> Self {
        self.with_object_store_persistence_for_instance(
            store,
            prefix,
            object_key,
            origin,
            "default",
            DEFAULT_PERSISTED_INSTANCE_TTL,
        )
    }

    /// Configure cluster-mode persistence for this controller. `instance_ttl`
    /// is reinterpreted as the lease window length (= refresh interval).
    #[must_use]
    pub fn with_object_store_persistence_for_instance(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        object_key: impl Into<String>,
        origin: impl Into<String>,
        instance_id: impl Into<String>,
        window_duration: Duration,
    ) -> Self {
        let instance_id = instance_id.into();
        self.persistence = Some(PersistenceConfig {
            store,
            prefix: normalize_object_state_prefix(&prefix.into()),
            object_key: object_key.into(),
            origin: origin.into(),
            instance_id: if instance_id.trim().is_empty() {
                "default".to_string()
            } else {
                instance_id
            },
            window_duration: if window_duration.is_zero() {
                Duration::from_secs(1)
            } else {
                window_duration
            },
        });
        self
    }

    #[must_use]
    pub fn build(self) -> Arc<RateController> {
        let jitter = self.jitter;
        let metrics = self.metrics.unwrap_or_default();

        let semaphore = self
            .max_concurrent_requests
            .map(|max_concurrent_requests| Arc::new(Semaphore::new(max_concurrent_requests)));

        let weighted_rate_limiter = self
            .weighted_quota
            .as_ref()
            .map(|q| Arc::new(GovernorRateLimiter::direct(q.quota)));

        // Persistence path: each named quota becomes a LeasedBucket. We do NOT
        // also build a local governor limiter for that quota — the lease
        // strictly bounds the per-replica budget per window already.
        //
        // No-persistence path: each quota becomes a local governor limiter.
        let mut local_limiters: Vec<Arc<GovernorRateLimiter>> = Vec::new();
        let mut leased_buckets: Vec<Arc<LeasedBucket>> = Vec::new();

        for (index, quota_def) in self.quotas.into_iter().enumerate() {
            let fallback_name = format!("quota-{index}");
            let limiter_key = quota_def.persistence_key(&fallback_name);

            if let Some(persistence) = &self.persistence {
                let burst_per_window = quota_def.burst_per_window(persistence.window_duration);
                leased_buckets.push(LeasedBucket::new(LeasedBucketConfig {
                    store: Arc::clone(&persistence.store),
                    prefix: persistence.prefix.clone(),
                    object_key: persistence.object_key.clone(),
                    origin: persistence.origin.clone(),
                    instance_id: persistence.instance_id.clone(),
                    window_duration: persistence.window_duration,
                    limiter_key,
                    burst_per_window,
                }));
            } else {
                local_limiters.push(Arc::new(GovernorRateLimiter::direct(quota_def.quota)));
            }
        }

        let persistence_origin = self.persistence.as_ref().map(|p| p.origin.clone());

        RateController::new(
            jitter,
            local_limiters,
            leased_buckets,
            weighted_rate_limiter,
            semaphore,
            metrics,
            persistence_origin,
        )
    }
}

#[derive(Debug, Default)]
pub struct RateControllerMetrics {
    permits_acquired_total: AtomicU64,
    acquire_errors_total: AtomicU64,
    wait_duration_ms_total: AtomicU64,
    inflight_permits: AtomicU64,
}

impl RateControllerMetrics {
    #[must_use]
    pub fn permits_acquired_total(&self) -> u64 {
        self.permits_acquired_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn acquire_errors_total(&self) -> u64 {
        self.acquire_errors_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn wait_duration_ms_total(&self) -> u64 {
        self.wait_duration_ms_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn inflight_permits(&self) -> u64 {
        self.inflight_permits.load(Ordering::Relaxed)
    }

    fn record_wait_duration(&self, duration: Duration) {
        self.wait_duration_ms_total
            .fetch_add(duration_millis_u64(duration), Ordering::Relaxed);
    }

    fn record_acquire_success(&self, duration: Duration) {
        self.permits_acquired_total.fetch_add(1, Ordering::Relaxed);
        self.inflight_permits.fetch_add(1, Ordering::Relaxed);
        self.record_wait_duration(duration);
    }

    fn record_acquire_error(&self, duration: Duration) {
        self.acquire_errors_total.fetch_add(1, Ordering::Relaxed);
        self.record_wait_duration(duration);
    }

    fn record_permit_drop(&self) {
        self.inflight_permits.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct RateController {
    jitter_config: JitterConfig,
    /// Local-only governor limiters (in-memory mode).
    local_limiters: Vec<Arc<GovernorRateLimiter>>,
    /// Cluster-wide leased token buckets (cluster mode).
    leased_buckets: Vec<Arc<LeasedBucket>>,
    weighted_rate_limiter: Option<Arc<GovernorRateLimiter>>,
    semaphore: Option<Arc<Semaphore>>,
    metrics: Arc<RateControllerMetrics>,
    /// Origin string used for log/error context when in cluster mode.
    persistence_origin: Option<String>,
}

impl std::fmt::Debug for RateController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RateController")
            .field("jitter_config", &self.jitter_config)
            .field("local_limiters", &self.local_limiters.len())
            .field("leased_buckets", &self.leased_buckets.len())
            .field(
                "weighted_rate_limiter",
                &self.weighted_rate_limiter.is_some(),
            )
            .field("semaphore", &self.semaphore.is_some())
            .field("metrics", &self.metrics)
            .field("persistence_origin", &self.persistence_origin)
            .finish()
    }
}

#[derive(Debug)]
pub struct Permit {
    semaphore: Option<OwnedSemaphorePermit>,
    weight: Option<u32>,
    rate_controller: Arc<RateController>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.rate_controller.metrics.record_permit_drop();
        if let Some(permit) = self.semaphore.take() {
            drop(permit);
        }
    }
}

impl Permit {
    /// Re-check the quotas from an existing permit. The caller retains its
    /// permit but acquires fresh rate-limit budget — used on retry paths.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`RateController::acquire`].
    pub async fn until_ready(&self) -> Result<()> {
        let wait_start = tokio::time::Instant::now();
        let result = self
            .rate_controller
            .wait_for_rate_limiters(self.weight)
            .await;

        let wait_duration = wait_start.elapsed();
        match result {
            Ok(()) => {
                self.rate_controller
                    .metrics
                    .record_wait_duration(wait_duration);
                Ok(())
            }
            Err(error) => {
                self.rate_controller
                    .metrics
                    .record_acquire_error(wait_duration);
                Err(error)
            }
        }
    }
}

impl RateController {
    #[must_use]
    pub fn builder() -> RateControllerBuilder {
        RateControllerBuilder::new()
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<RateControllerMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Snapshot of all per-bucket leased metrics, for telemetry. Returns
    /// `(limiter_key, metrics)` pairs.
    #[must_use]
    pub fn leased_bucket_metrics(&self) -> Vec<(String, Arc<LeasedBucketMetrics>)> {
        self.leased_buckets
            .iter()
            .map(|b| (b.limiter_key().to_string(), b.metrics()))
            .collect()
    }

    #[must_use]
    pub fn available_permits(&self) -> Option<usize> {
        self.semaphore
            .as_ref()
            .map(|semaphore| semaphore.available_permits())
    }

    /// Refresh leases for all leased buckets (no-op if persistence is
    /// disabled). Called by the persistence task on `refresh_interval` ticks
    /// and once at controller-build time.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered. Other buckets are still attempted.
    pub async fn refresh_and_persist_state_snapshot(&self) -> Result<()> {
        let mut first_error: Option<Error> = None;
        for bucket in &self.leased_buckets {
            if let Err(e) = bucket.refresh_lease().await {
                let origin = bucket.origin().to_string();
                tracing::warn!(
                    origin = origin.as_str(),
                    limiter = bucket.limiter_key(),
                    "Failed to refresh cluster rate-control lease: {e}"
                );
                if first_error.is_none() {
                    first_error = Some(Error::LeaseRefresh {
                        origin,
                        source: Box::new(e),
                    });
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        Ok(())
    }

    /// Read-only snapshot refresh — no-op for the leased model (every refresh
    /// is read+write). Kept for API compatibility with the previous design.
    ///
    /// # Errors
    ///
    /// Currently never returns an error.
    pub fn refresh_persisted_state(&self) -> Result<()> {
        Ok(())
    }

    async fn until_ready(self: Arc<Self>) -> Result<()> {
        // Local in-memory limiters: pace per replica.
        for limiter in &self.local_limiters {
            limiter.until_ready().await;
        }
        // Cluster leased buckets: each acquire consumes one token, may wait.
        for bucket in &self.leased_buckets {
            bucket.acquire().await.map_err(|e| match e {
                leased::Error::FailClosed { origin } => Error::ClusterBudgetExhausted { origin },
                other => Error::LeaseRefresh {
                    origin: other_origin(&other),
                    source: Box::new(other),
                },
            })?;
        }
        Ok(())
    }

    async fn until_weighted_ready(self: Arc<Self>, weight: Option<u32>) -> Result<()> {
        Arc::clone(&self).until_ready().await?;

        if let Some(weight) = weight
            && let Some(weighted_limiter) = &self.weighted_rate_limiter
            && let Some(nonzero_weight) = NonZeroU32::new(weight)
        {
            tracing::debug!("Acquiring weighted rate limiter for weight {weight}");

            weighted_limiter
                .until_n_ready(nonzero_weight)
                .await
                .map_err(|_| Error::InsufficientCapacity { weight })?;
        }

        Ok(())
    }

    fn new(
        jitter: Option<JitterConfig>,
        local_limiters: Vec<Arc<GovernorRateLimiter>>,
        leased_buckets: Vec<Arc<LeasedBucket>>,
        weighted_rate_limiter: Option<Arc<GovernorRateLimiter>>,
        semaphore: Option<Arc<Semaphore>>,
        metrics: Arc<RateControllerMetrics>,
        persistence_origin: Option<String>,
    ) -> Arc<Self> {
        let jitter_config = jitter.unwrap_or(JitterConfig {
            min: Duration::ZERO,
            max: Duration::ZERO,
        });

        Arc::new(Self {
            jitter_config,
            local_limiters,
            leased_buckets,
            weighted_rate_limiter,
            semaphore,
            metrics,
            persistence_origin,
        })
    }

    async fn wait_for_rate_limiters(self: &Arc<Self>, weight: Option<u32>) -> Result<()> {
        Arc::clone(self).until_weighted_ready(weight).await
    }

    /// Acquire a permit with a specific weight. See [`Self::acquire`] for
    /// notes on cluster-mode failure semantics.
    ///
    /// # Errors
    ///
    /// - [`Error::SemaphoreAcquireError`] if the concurrency semaphore is closed.
    /// - [`Error::InsufficientCapacity`] if the weighted quota cannot satisfy `weight`.
    /// - [`Error::ClusterBudgetExhausted`] if cluster mode is fail-closed.
    pub async fn acquire_weighted(self: &Arc<Self>, weight: u32) -> Result<Permit> {
        self.acquire_weighted_opt(Some(weight)).await
    }

    /// Acquire a permit. In cluster mode, may wait until the next window if
    /// the local lease is exhausted. Returns
    /// [`Error::ClusterBudgetExhausted`] only when the persisted state store
    /// is unreachable AND the last lease has expired (fail-closed semantics).
    ///
    /// # Errors
    ///
    /// See [`Self::acquire_weighted`].
    pub async fn acquire(self: &Arc<Self>) -> Result<Permit> {
        self.acquire_weighted_opt(None).await
    }

    /// Acquire a permit with an optional weight.
    ///
    /// # Errors
    ///
    /// See [`Self::acquire_weighted`].
    pub async fn acquire_weighted_opt(self: &Arc<Self>, weight: Option<u32>) -> Result<Permit> {
        let self_cloned = Arc::clone(self);
        let wait_start = tokio::time::Instant::now();

        // Concurrency cap first — we may end up waiting long enough that
        // rate-limiter slots open up.
        let semaphore = if let Some(semaphore) = &self.semaphore {
            match Arc::clone(semaphore).acquire_owned().await {
                Ok(permit) => Some(permit),
                Err(source) => {
                    self.metrics.record_acquire_error(wait_start.elapsed());
                    return Err(Error::SemaphoreAcquireError { source });
                }
            }
        } else {
            None
        };

        if let Err(error) = self.wait_for_rate_limiters(weight).await {
            self.metrics.record_acquire_error(wait_start.elapsed());
            return Err(error);
        }

        let jitter_wait = rand::random_range(self.jitter_config.min..=self.jitter_config.max);
        tokio::time::sleep(jitter_wait).await;

        self.metrics.record_acquire_success(wait_start.elapsed());

        Ok(Permit {
            semaphore,
            weight,
            rate_controller: self_cloned,
        })
    }
}

fn other_origin(e: &leased::Error) -> String {
    use leased::Error::{ConflictExhausted, FailClosed, Read, Write};
    match e {
        Read { origin, .. }
        | Write { origin, .. }
        | ConflictExhausted { origin }
        | FailClosed { origin } => origin.clone(),
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn normalize_object_state_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

#[expect(dead_code)]
fn unix_millis_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_millis_u64(duration),
        Err(error) => {
            tracing::warn!("Failed to read system time for rate-control state: {error}");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn in_memory_acquire_and_concurrency_cap() {
        let rate_controller = RateControllerBuilder::new()
            .with_jitter(JitterConfig::zero())
            .with_max_concurrent_requests(5)
            .add_quota(Quota::per_second(NonZeroU32::new(10).expect("non-zero")))
            .build();

        let permit = rate_controller.acquire().await.expect("acquire");
        assert!(permit.semaphore.is_some());
        drop(permit);

        let permits = (0..5)
            .map(|_| rate_controller.acquire())
            .collect::<Vec<_>>();
        let mut results = futures::future::try_join_all(permits)
            .await
            .expect("acquire all");

        tokio::select! {
            _ = rate_controller.acquire() => panic!("semaphore should have blocked"),
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }

        drop(results.pop().expect("at least one"));

        tokio::time::timeout(Duration::from_secs(1), rate_controller.acquire())
            .await
            .expect("should not time out")
            .expect("acquire ok");
    }

    #[tokio::test]
    async fn cluster_mode_two_replicas_share_budget() {
        // Cluster budget = 10 RPS = 10 tokens / 1s window.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let make = |instance: &str| {
            RateControllerBuilder::new()
                .with_jitter(JitterConfig::zero())
                .add_quota_with_name(
                    "requests_per_second",
                    Quota::per_second(NonZeroU32::new(10).expect("non-zero")),
                )
                .with_object_store_persistence_for_instance(
                    Arc::clone(&store),
                    "",
                    "test/origin",
                    "https://example.com".to_string(),
                    instance.to_string(),
                    Duration::from_secs(1),
                )
                .build()
        };

        let a = make("a");
        let b = make("b");

        // Refresh leases for both replicas.
        a.refresh_and_persist_state_snapshot()
            .await
            .expect("a refresh");
        b.refresh_and_persist_state_snapshot()
            .await
            .expect("b refresh");

        // Sum of granted leases must not exceed cluster budget.
        let a_granted = a.leased_bucket_metrics()[0].1.lease_granted();
        let b_granted = b.leased_bucket_metrics()[0].1.lease_granted();
        assert!(
            a_granted + b_granted <= 10,
            "leases {a_granted}+{b_granted} exceed cluster budget 10"
        );
    }

    #[tokio::test]
    async fn cluster_mode_acquire_blocks_when_lease_exhausted() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let controller = RateControllerBuilder::new()
            .with_jitter(JitterConfig::zero())
            .add_quota_with_name(
                "requests_per_second",
                Quota::per_second(NonZeroU32::new(2).expect("non-zero")),
            )
            .with_object_store_persistence_for_instance(
                Arc::clone(&store),
                "",
                "test/origin",
                "https://example.com".to_string(),
                "a".to_string(),
                Duration::from_millis(200),
            )
            .build();

        controller
            .refresh_and_persist_state_snapshot()
            .await
            .expect("first lease");

        // Drain the lease.
        let granted = controller.leased_bucket_metrics()[0].1.lease_granted();
        for _ in 0..granted {
            controller.acquire().await.expect("acquire within lease");
        }

        // Next acquire must block.
        tokio::select! {
            _ = controller.acquire() => panic!("should have blocked, lease was {granted}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}
