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
use tokio::time::Instant;

mod adaptive;
mod leased;
mod phase_change_log;

pub use adaptive::{
    AdaptiveController, AdaptiveRateControl, AdaptiveRateControlError,
    DEFAULT_ADAPTIVE_FAILURE_THRESHOLD, DEFAULT_ADAPTIVE_WINDOW, RequestOutcome,
};
use leased::{LeasedBucket, LeasedBucketConfig, LeasedBucketMetrics};

const DEFAULT_PERSISTED_INSTANCE_TTL: Duration = Duration::from_secs(90);

/// Resolution multiplier for adaptive weighting. When adaptive control is
/// enabled, every limiter is built at a [`ADAPTIVE_WEIGHT_RESOLUTION`]
/// multiple of its configured capacity and a healthy
/// request charges [`ADAPTIVE_WEIGHT_RESOLUTION`] cells instead of 1. This enables
/// fractional weights (to 0.01 resolution) for a integer based bucket. Therefore, the
///  per-request charge `round(M / coefficient)` has ~`1/M` resolution. Without it,
///  the integer weight `round(1 / coefficient)` stays 1 for any coefficient above ~0.67,
/// leaving mild throttling a no-op. This is a purely internal scale: the buckets stay
/// logically the same size and every public metric reports logical (unscaled)
/// units.
const ADAPTIVE_WEIGHT_RESOLUTION: u32 = 100;

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

    #[snafu(display(
        "Timed out after {waited:?} waiting for rate-control capacity{target} to admit the request. The configured rate limit could not free a slot in time. Increase the rate limit, raise `rate_control_acquire_timeout`, or lower request concurrency, then try again. See: https://spiceai.org/docs/reference/spicepod/runtime#http-rate-control"
    ))]
    AcquireTimeout {
        target: RateControlTarget,
        waited: Duration,
    },
}

/// The rate-limited upstream named in user-facing errors. HTTP rate control
/// knows the origin; other callers (models, UDFs) have none to name, so the
/// type — not the message — decides whether the clause appears.
///
/// [`Display`](std::fmt::Display) writes a leading-space clause (` for origin
/// 'x'`), or nothing when the origin is unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RateControlTarget(Option<String>);

impl RateControlTarget {
    /// A target with no known origin.
    #[must_use]
    pub fn unknown() -> Self {
        Self(None)
    }

    /// The origin this controller limits, e.g. `https://api.example.com`.
    #[must_use]
    pub fn origin(origin: impl Into<String>) -> Self {
        Self(Some(origin.into()))
    }

    /// The origin, when one is known.
    #[must_use]
    pub fn as_origin(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

impl std::fmt::Display for RateControlTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(origin) => write!(formatter, " for origin '{origin}'"),
            None => Ok(()),
        }
    }
}

/// Bound one wait for rate-control capacity. `None` waits indefinitely.
///
/// The bound is per attempt: one call covers every wait source inside
/// `future`, and a later attempt gets the full bound again. This is the only
/// place [`Error::AcquireTimeout`] is built, so the acquire and the retry
/// re-check report it the same way.
async fn within<T, F>(bound: Option<Duration>, target: &RateControlTarget, future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let Some(bound) = bound else {
        return future.await;
    };

    match tokio::time::timeout(bound, future).await {
        Ok(result) => result,
        Err(_elapsed) => Err(Error::AcquireTimeout {
            target: target.clone(),
            waited: bound,
        }),
    }
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
    adaptive: Option<AdaptiveRateControl>,
    acquire_timeout: Option<Duration>,
    /// The single origin this builder knows. It names the target in
    /// [`Error::AcquireTimeout`] and the adaptive log lines.
    origin: Option<String>,
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

    /// Bound how long one acquire attempt may wait for capacity before failing
    /// with [`Error::AcquireTimeout`]. The bound covers every wait source of
    /// that attempt — the semaphore, the quotas, the cluster leased buckets and
    /// jitter. Each attempt gets the bound again, so a caller that retries can
    /// wait up to the bound per attempt. Unset means wait indefinitely.
    ///
    /// Cluster mode: leased-bucket tokens already spent when the bound fires
    /// are not returned to the window, so the bound is exact for single-node
    /// (local) rate control and best-effort for cluster rate control.
    #[must_use]
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = Some(timeout);
        self
    }

    /// Name the upstream origin this controller limits, so user-facing errors
    /// can identify it. Cluster mode supplies it from the persistence config.
    #[must_use]
    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = Some(origin.into());
        self
    }

    /// Attach an adaptive controller that dynamically scales every configured
    /// limit by its admission coefficient. `origin` names the upstream in the
    /// controller's throttling and recovery log lines, and becomes the
    /// controller's origin, exactly as [`Self::with_origin`] sets it.
    #[must_use]
    pub fn with_adaptive(self, control: AdaptiveRateControl, origin: impl Into<String>) -> Self {
        let mut builder = self.with_origin(origin);
        builder.adaptive = Some(control);
        builder
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

        // With adaptive control, every limiter is built at `ADAPTIVE_WEIGHT_RESOLUTION` x capacity
        // and a healthy request charges `ADAPTIVE_WEIGHT_RESOLUTION` cells, so the adaptive weight
        // has sub-integer resolution (see [`ADAPTIVE_WEIGHT_RESOLUTION`]). Without
        // adaptive control the resolution is 1 and nothing is scaled.
        let resolution = if self.adaptive.is_some() {
            ADAPTIVE_WEIGHT_RESOLUTION
        } else {
            1
        };

        // Each limiter carries its own capacity so the adaptive weight is clamped
        // per limiter: a weighted acquire never asks a limiter for more than it can
        // hold, and one small limit never bounds how deeply a larger one throttles.
        let semaphore = self.max_concurrent_requests.map(|max_concurrent_requests| {
            let scaled = max_concurrent_requests.saturating_mul(resolution as usize);
            (
                Arc::new(Semaphore::new(scaled)),
                u32::try_from(scaled).unwrap_or(u32::MAX),
            )
        });

        let weighted_rate_limiter = self
            .weighted_quota
            .as_ref()
            .map(|q| Arc::new(GovernorRateLimiter::direct(q.quota)));

        // Persistence path: each named quota becomes a LeasedBucket. We do NOT
        // also build a local governor limiter for that quota — the lease
        // strictly bounds the per-replica budget per window already.
        //
        // No-persistence path: each quota becomes a local governor limiter.
        let mut local_limiters: Vec<(Arc<GovernorRateLimiter>, u32)> = Vec::new();
        let mut leased_buckets: Vec<Arc<LeasedBucket>> = Vec::new();

        for (index, quota_def) in self.quotas.into_iter().enumerate() {
            let fallback_name = format!("quota-{index}");
            let limiter_key = quota_def.persistence_key(&fallback_name);

            if let Some(persistence) = &self.persistence {
                // Leased buckets are deliberately NOT `resolution`-scaled. Their
                // `acquire()` registers one unit of cluster demand per call, so
                // charging `resolution` tokens would inflate the demand-weighted
                // lease sharing across replicas. Adaptive mode is rejected at
                // configuration time when cluster rate control is set, so a leased
                // bucket never has an adaptive weight to apply.
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
                let quota = scale_quota_rate(quota_def.quota, resolution);
                let capacity = quota.burst_size().get();
                local_limiters.push((Arc::new(GovernorRateLimiter::direct(quota)), capacity));
            }
        }

        // An explicit origin wins; cluster mode already carries one.
        let target = self
            .origin
            .or_else(|| self.persistence.as_ref().map(|p| p.origin.clone()))
            .map_or_else(RateControlTarget::unknown, RateControlTarget::origin);

        // The adaptive controller names the same origin in its log lines, so it
        // takes it from the target. `with_adaptive` always sets an origin, so
        // the fallback is unreachable.
        let adaptive = self.adaptive.map(|control| {
            Arc::new(AdaptiveController::new(
                control,
                target.as_origin().unwrap_or_default(),
            ))
        });

        RateController::new(
            jitter,
            local_limiters,
            leased_buckets,
            weighted_rate_limiter,
            semaphore,
            metrics,
            target,
            adaptive,
            resolution,
            self.acquire_timeout,
        )
    }
}

#[derive(Debug, Default)]
pub struct RateControllerMetrics {
    permits_acquired_total: AtomicU64,
    acquire_errors_total: AtomicU64,
    wait_duration_ms_total: AtomicU64,
    inflight_permits: AtomicU64,
    adaptive_throttled_total: AtomicU64,
    adaptive_outcomes_total: [AtomicU64; 3],
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

    /// Total requests adaptive rate control actually throttled — charged an
    /// above-normal weight (`> 1`) because the origin was failing. `0` when
    /// adaptive control is disabled or the origin has stayed healthy. Unlike the
    /// admission-coefficient gauge (throttle *intensity*), this counts the
    /// *volume* of requests that paid.
    #[must_use]
    pub fn adaptive_throttled_total(&self) -> u64 {
        self.adaptive_throttled_total.load(Ordering::Relaxed)
    }

    /// Recorded outcomes by kind. Only applicable when adaptive control is enabled.
    #[must_use]
    pub fn adaptive_outcomes_total(&self, outcome: RequestOutcome) -> u64 {
        self.adaptive_outcomes_total[outcome.index()].load(Ordering::Relaxed)
    }

    fn record_adaptive_throttle(&self) {
        self.adaptive_throttled_total
            .fetch_add(1, Ordering::Relaxed);
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

/// A rate controller with its known maximum capacity limit.
/// Useful for adapative rate controls on static data structures by using inverse
/// weight acquisition.
pub type MaxCapacityLimits<T, L = u32> = (Arc<T>, L);

pub struct RateController {
    jitter_config: JitterConfig,
    /// Local-only governor limiters (in-memory mode).
    local_limiters: Vec<MaxCapacityLimits<GovernorRateLimiter>>,
    /// Cluster-wide leased token buckets (cluster mode).
    leased_buckets: Vec<Arc<LeasedBucket>>,
    weighted_rate_limiter: Option<Arc<GovernorRateLimiter>>,
    semaphore: Option<MaxCapacityLimits<Semaphore>>,
    metrics: Arc<RateControllerMetrics>,
    /// The upstream this controller limits, named in user-facing errors.
    target: RateControlTarget,
    /// When present, scales each limit down by an admission coefficient in
    /// `[0, 1.0]` based on recent [`RequestOutcome`]. See [`Self::record_outcome`].
    /// Buckets, semaphores and limits stay static; each request charges an
    /// inversely-scaled weight ([`AdaptiveController::acquire_weight`]) that every
    /// limiter clamps to its own capacity — so a small concurrency cap never bounds
    /// how deeply the per-second/per-minute quotas can throttle.
    adaptive: Option<Arc<AdaptiveController>>,
    /// Cells a healthy request charges (and the factor every limiter's capacity
    /// is scaled by). [`ADAPTIVE_WEIGHT_RESOLUTION`] with adaptive control, else
    /// `1`. Purely internal — divided back out of any logical metric.
    resolution: u32,
    /// Upper bound on how long one acquire attempt waits for capacity before
    /// returning [`Error::AcquireTimeout`]. `None` = wait indefinitely (the
    /// legacy behaviour). Applied per attempt: each `acquire*` call, and each
    /// [`Permit::until_ready`] re-check, gets the whole bound for the
    /// semaphore, the governor quotas, the leased buckets and jitter.
    acquire_timeout: Option<Duration>,
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
            .field("target", &self.target)
            .field("adaptive", &self.adaptive.is_some())
            .field("resolution", &self.resolution)
            .field("acquire_timeout", &self.acquire_timeout)
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
    /// This re-check is its own attempt, so it gets the whole
    /// `rate_control_acquire_timeout` again rather than what an earlier acquire
    /// left over.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`RateController::acquire`].
    pub async fn until_ready(&self) -> Result<()> {
        let controller = &self.rate_controller;
        let wait_start = Instant::now();
        let result = within(
            controller.acquire_timeout,
            &controller.target,
            controller.wait_for_rate_limiters(self.weight),
        )
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
            .map(|bucket| (bucket.limiter_key().to_string(), bucket.metrics()))
            .collect()
    }

    #[must_use]
    pub fn available_permits(&self) -> Option<usize> {
        // The semaphore is sized at `resolution`x and a healthy request holds
        // `resolution` permits, so divide back to report LOGICAL permits (the
        // user's configured concurrency), not the internal scaled count.
        let resolution = (self.resolution as usize).max(1);
        self.semaphore
            .as_ref()
            .map(|(semaphore, _)| semaphore.available_permits() / resolution)
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
                first_error = first_error.or_else(|| {
                    Some(Error::LeaseRefresh {
                        origin,
                        source: Box::new(e),
                    })
                });
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn until_ready(self: Arc<Self>) -> Result<()> {
        // Adaptive control scales every configured limit by charging `weight`
        // cells/tokens per request (the healthy baseline is `resolution` cells for
        // the `resolution`-scaled local limiters, `1` when disabled). Each limiter
        // clamps the weight to its own capacity, so one small limit never caps how
        // deeply another can throttle.
        let desired = self.adaptive_desired_weight();

        // Local in-memory limiters: pace per replica.
        for (limiter, capacity) in &self.local_limiters {
            let weight = clamp_weight(desired, *capacity);
            if weight <= 1 {
                limiter.until_ready().await;
            } else if let Some(nonzero_weight) = NonZeroU32::new(weight) {
                limiter
                    .until_n_ready(nonzero_weight)
                    .await
                    .map_err(|_| Error::InsufficientCapacity { weight })?;
            }
        }
        // Cluster leased buckets: each acquire consumes one token, may wait.
        // Adaptive mode and cluster rate control are mutually exclusive (rejected
        // at configuration time), so each request charges exactly one token.
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

    #[expect(
        clippy::too_many_arguments,
        reason = "internal constructor fed by the builder"
    )]
    fn new(
        jitter: Option<JitterConfig>,
        local_limiters: Vec<(Arc<GovernorRateLimiter>, u32)>,
        leased_buckets: Vec<Arc<LeasedBucket>>,
        weighted_rate_limiter: Option<Arc<GovernorRateLimiter>>,
        semaphore: Option<(Arc<Semaphore>, u32)>,
        metrics: Arc<RateControllerMetrics>,
        target: RateControlTarget,
        adaptive: Option<Arc<AdaptiveController>>,
        resolution: u32,
        acquire_timeout: Option<Duration>,
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
            target,
            adaptive,
            resolution,
            acquire_timeout,
        })
    }

    /// Record the outcome of one request, feeding the adaptive controller (a
    /// no-op when adaptive control is disabled).
    pub fn record_outcome(&self, outcome: RequestOutcome) {
        if let Some(adaptive) = &self.adaptive {
            self.metrics.adaptive_outcomes_total[outcome.index()].fetch_add(1, Ordering::Relaxed);
            adaptive.record(outcome);
        }
    }

    /// Whether adaptive admission has stayed reduced for the given duration.
    #[must_use]
    pub fn throttled_for(&self, duration: Duration) -> bool {
        self.adaptive
            .as_ref()
            .is_some_and(|adaptive| adaptive.throttled_for(duration))
    }

    /// The adaptive decay window, or `None` in static mode.
    #[must_use]
    pub fn adaptive_window(&self) -> Option<Duration> {
        self.adaptive.as_ref().map(|adaptive| adaptive.window())
    }

    /// The adaptive admission coefficient in `[0, 1]`, or `None` when adaptive
    /// control is disabled. Computed live, so it reflects window decay.
    #[must_use]
    pub fn admission_coefficient(&self) -> Option<f64> {
        self.adaptive
            .as_ref()
            .map(|adaptive| adaptive.admission_coefficient())
    }

    /// The real-valued weight (in scaled cells) one request wants to charge right
    /// now, before any per-limiter capacity clamp. `resolution` cells at full
    /// health (`1` when adaptive control is disabled), scaling up as the origin
    /// fails via [`AdaptiveController::acquire_weight`]. Each limiter clamps this
    /// to its own (already `resolution`-scaled) capacity via [`clamp_weight`] at
    /// the point of acquisition.
    fn adaptive_desired_weight(&self) -> f64 {
        f64::from(self.resolution)
            * self
                .adaptive
                .as_ref()
                .map_or(1.0, |adaptive| adaptive.acquire_weight())
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
    /// See [`Self::acquire_weighted`]. Additionally returns
    /// [`Error::AcquireTimeout`] if a bound is configured and the wait for
    /// capacity exceeds it.
    pub async fn acquire_weighted_opt(self: &Arc<Self>, weight: Option<u32>) -> Result<Permit> {
        let wait_start = Instant::now();
        let result = within(
            self.acquire_timeout,
            &self.target,
            self.acquire_inner(weight),
        )
        .await;

        // Only the bound above produces `AcquireTimeout`, and it cancels
        // `acquire_inner` at the await it was parked on, so the inner call
        // records no outcome — attribute the failure here.
        //
        // Cluster mode: each leased-bucket acquire charges exactly one token,
        // and a token already spent when the bound fires is not returned to the
        // window. The bound is therefore exact for local (single-node) rate
        // control and best-effort for cluster rate control.
        if matches!(&result, Err(Error::AcquireTimeout { .. })) {
            self.metrics.record_acquire_error(wait_start.elapsed());
        }

        result
    }

    async fn acquire_inner(self: &Arc<Self>, weight: Option<u32>) -> Result<Permit> {
        let self_cloned = Arc::clone(self);
        let wait_start = Instant::now();

        // Snapshot the adaptive weight once for this acquire. A rounded charge
        // above the healthy baseline (`resolution` cells) means adaptive is
        // charging this request extra against at least one limiter — i.e. the
        // request actually paid for the origin's failures, which the
        // admission-coefficient gauge (intensity) does not count.
        let desired_weight = self.adaptive_desired_weight();
        if desired_weight.round() > f64::from(self.resolution) {
            self.metrics.record_adaptive_throttle();
        }

        // Concurrency cap first — we may end up waiting long enough that
        // rate-limiter slots open up. Adaptive control holds `permits` permits per
        // request (1 when disabled or healthy), scaling concurrency by the same
        // coefficient as the rate quotas, clamped to this semaphore's capacity.
        let semaphore = if let Some((semaphore, capacity)) = &self.semaphore {
            let permits = clamp_weight(desired_weight, *capacity);
            match Arc::clone(semaphore).acquire_many_owned(permits).await {
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

/// Convert a real-valued desired weight into the whole cells/permits one request
/// charges against a limiter of the given `capacity`: clamp to the capacity, then
/// round, floored at 1. Clamping before the cast keeps the value finite and in
/// `[1, capacity]`, so a near-zero coefficient (desired == +inf) resolves to the
/// capacity — the limiter's deepest throttle — rather than overflowing.
fn clamp_weight(desired: f64, capacity: u32) -> u32 {
    let weight = desired.min(f64::from(capacity)).round();
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to [1, capacity] before the cast"
    )]
    let weight = weight as u32;
    weight.clamp(1, capacity.max(1))
}

/// Scale a governor quota's rate by `factor`: replenish `factor`x faster and
/// allow a `factor`x burst, so the bucket admits `factor`x the cells in the same
/// time. Charging `factor` cells per healthy request then nets the original rate,
/// while a throttled request charging more cells throttles at `1/factor`
/// resolution. `factor <= 1` returns the quota unchanged.
fn scale_quota_rate(quota: Quota, factor: u32) -> Quota {
    if factor <= 1 {
        return quota;
    }
    let period = quota.replenish_interval() / factor;
    let burst =
        NonZeroU32::new(quota.burst_size().get().saturating_mul(factor)).unwrap_or(NonZeroU32::MIN);
    // `with_period` only returns `None` if `period` underflowed to zero (an
    // extreme rate); keep the unscaled quota in that case.
    Quota::with_period(period).map_or(quota, |scaled| scaled.allow_burst(burst))
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

    /// Drive the adaptive window to a target admission coefficient, then read the
    /// desired per-request weight. With the resolution multiplier a coefficient of
    /// 0.9 charges ~`M/0.9` cells (throttles), where the old integer scheme would
    /// have rounded `1/0.9` to weight 1 and thrown the throttle away.
    #[tokio::test]
    async fn resolution_makes_mild_throttling_effective() {
        // failure_threshold 0.5 => k = 2. Recording 450 accepts / 1000 requests
        // gives coefficient min(1, (2*450+1)/(1000+1)) ≈ 0.9.
        let control =
            AdaptiveRateControl::new(0.5, DEFAULT_ADAPTIVE_WINDOW).expect("valid control");
        let controller = RateControllerBuilder::new()
            .with_jitter(JitterConfig::zero())
            .add_quota(Quota::per_second(NonZeroU32::new(100).expect("non-zero")))
            .with_adaptive(control, "https://origin.example.com")
            .build();

        for i in 0..1000 {
            controller.record_outcome(if i < 450 {
                RequestOutcome::Success
            } else {
                RequestOutcome::Failure
            });
        }

        let coefficient = controller
            .admission_coefficient()
            .expect("adaptive is enabled");
        assert!(
            (coefficient - 0.9).abs() < 0.02,
            "coefficient should sit near 0.9, got {coefficient}"
        );

        // Mild throttling now engages: desired charge exceeds the healthy baseline
        // (`M` cells), and the effective rate ratio `M / desired` tracks ~0.9.
        let desired = controller.adaptive_desired_weight();
        let baseline = f64::from(ADAPTIVE_WEIGHT_RESOLUTION);
        assert!(
            desired.round() > baseline,
            "expected throttling (desired {desired} > baseline {baseline})"
        );
        let effective_ratio = baseline / desired;
        assert!(
            (effective_ratio - coefficient).abs() < 0.01,
            "effective rate ratio {effective_ratio} should track coefficient {coefficient}"
        );
    }

    #[test]
    fn adaptive_outcomes_count_each_kind() {
        let controller = RateController::builder()
            .with_max_concurrent_requests(4)
            .with_adaptive(
                AdaptiveRateControl::new(0.1, DEFAULT_ADAPTIVE_WINDOW).expect("adaptive config"),
                "https://api.example.com",
            )
            .build();
        for outcome in RequestOutcome::ALL {
            assert_eq!(controller.metrics().adaptive_outcomes_total(outcome), 0);
            for _ in 0..3 {
                controller.record_outcome(outcome);
            }
        }
        for outcome in RequestOutcome::ALL {
            assert_eq!(controller.metrics().adaptive_outcomes_total(outcome), 3);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn persistent_slow_responses_trigger_diagnostics_with_combined_limits() {
        let window = DEFAULT_ADAPTIVE_WINDOW;
        for concurrency in [Some(4), None] {
            let mut builder = RateController::builder()
                .with_quotas(vec![
                    Quota::per_second(NonZeroU32::new(10).expect("nonzero")),
                    Quota::per_minute(NonZeroU32::new(300).expect("nonzero")),
                ])
                .with_jitter(JitterConfig::new(
                    Duration::from_millis(5),
                    Duration::from_millis(10),
                ))
                .with_acquire_timeout(Duration::from_secs(15))
                .with_adaptive(
                    AdaptiveRateControl::new(0.1, window).expect("adaptive config"),
                    "https://api.example.com",
                );
            if let Some(capacity) = concurrency {
                builder = builder.with_max_concurrent_requests(capacity);
            }
            let controller = builder.build();
            for _ in 0..20 {
                controller.record_outcome(RequestOutcome::Slow);
                tokio::time::advance(Duration::from_secs(3)).await;
            }
            assert!(
                controller.throttled_for(window * 2),
                "persistent slow responses need a diagnostic with concurrency {concurrency:?}; coefficient {:?}",
                controller.admission_coefficient()
            );
        }
    }

    /// However deep the throttle, a request never charges more than a limiter's
    /// whole capacity: the origin still gets one request per full-bucket period,
    /// so recovery is always probed.
    #[test]
    fn weights_are_floored_at_one_and_capped_at_capacity() {
        assert_eq!(clamp_weight(1.0, 100), 1);
        assert_eq!(clamp_weight(0.0, 100), 1);
        assert_eq!(clamp_weight(150.0, 100), 100);
        assert_eq!(clamp_weight(f64::INFINITY, 100), 100);
    }

    /// The concurrency semaphore is built at `M`x and a healthy request holds `M`
    /// permits, but `available_permits` must report the LOGICAL count the user
    /// configured — never the internal scaled value.
    #[tokio::test]
    async fn available_permits_reports_logical_units() {
        let control =
            AdaptiveRateControl::new(0.5, DEFAULT_ADAPTIVE_WINDOW).expect("valid control");
        let controller = RateControllerBuilder::new()
            .with_jitter(JitterConfig::zero())
            .with_max_concurrent_requests(8)
            .add_quota(Quota::per_second(NonZeroU32::new(100).expect("non-zero")))
            .with_adaptive(control, "https://origin.example.com")
            .build();

        // Healthy origin, nothing held: logical 8, not the scaled 800.
        assert_eq!(controller.available_permits(), Some(8));

        // A healthy acquire holds M permits (1 logical); logical availability drops
        // to 7 while the permit is held, and returns to 8 once dropped.
        let permit = controller.acquire().await.expect("acquire");
        assert_eq!(controller.available_permits(), Some(7));
        drop(permit);
        assert_eq!(controller.available_permits(), Some(8));
    }

    /// The bound is per attempt. A `Permit::until_ready` re-check gets the whole
    /// bound of its own, however much time the original acquire used: it never
    /// inherits a shared, already-spent deadline.
    #[tokio::test(start_paused = true)]
    async fn until_ready_gets_the_whole_bound_for_its_own_attempt() {
        let bound = Duration::from_secs(10);
        let controller = RateControllerBuilder::new()
            .with_jitter(JitterConfig::zero())
            // One request per minute: the first passes, the next must wait.
            .add_quota(Quota::per_minute(NonZeroU32::new(1).expect("non-zero")))
            .with_acquire_timeout(bound)
            .with_origin("https://api.example.com")
            .build();

        let permit = controller
            .acquire()
            .await
            .expect("the first request passes");

        // Spend most of the bound doing the request itself, then retry.
        tokio::time::sleep(Duration::from_secs(8)).await;
        let recheck_start = Instant::now();
        let error = permit
            .until_ready()
            .await
            .expect_err("the quota is empty, so the re-check must hit the bound");

        assert!(
            recheck_start.elapsed() >= bound,
            "the re-check must wait its own full bound, not the remainder of an earlier one"
        );
        let message = error.to_string();
        let Error::AcquireTimeout { target, waited } = &error else {
            panic!("expected an acquire timeout, got {error:?}");
        };
        assert_eq!(target.as_origin(), Some("https://api.example.com"));
        assert_eq!(*waited, bound);
        assert!(
            message.contains("api.example.com"),
            "the error must name the origin: {message}"
        );
    }

    /// Without an origin the message stays grammatical: the clause is dropped.
    #[test]
    fn an_unknown_target_leaves_the_message_grammatical() {
        let message = Error::AcquireTimeout {
            target: RateControlTarget::unknown(),
            waited: Duration::from_secs(5),
        }
        .to_string();
        assert!(
            message.contains("rate-control capacity to admit the request"),
            "unexpected message: {message}"
        );
    }
}
