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

use std::{
    collections::{HashMap, HashSet},
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use std::sync::atomic::{AtomicU64, Ordering};

use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    nanos::Nanos,
    state::{NotKeyed, StateStore},
};
use object_store::ObjectStore;
use object_store_occ::{InsertResult, ObjectState, UpdateResult};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION: u32 = 1;
const DEFAULT_PERSISTED_INSTANCE_TTL: Duration = Duration::from_secs(90);
const MAX_PERSISTENCE_WRITE_ATTEMPTS: usize = 5;

type GovernorRateLimiter = RateLimiter<NotKeyed, SharedGovernorState, DefaultClock, NoOpMiddleware>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to acquire semaphore permit. {source}"))]
    SemaphoreAcquireError { source: tokio::sync::AcquireError },

    #[snafu(display(
        "Failed to refresh persisted rate-control state for origin {origin}. {source}"
    ))]
    PersistenceRefresh {
        origin: String,
        source: Box<object_store_occ::Error>,
    },

    #[snafu(display("Failed to persist rate-control state for origin {origin}. {source}"))]
    PersistenceWrite {
        origin: String,
        source: Box<object_store_occ::Error>,
    },

    #[snafu(display(
        "Unsupported persisted rate-control state schema version {version} for origin {origin}"
    ))]
    UnsupportedPersistedStateVersion { origin: String, version: u32 },

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
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct PersistedRateControlState {
    schema_version: u32,
    updated_at_unix_ms: u64,
    limiters: HashMap<String, PersistedLimiterState>,
    #[serde(default)]
    instances: HashMap<String, PersistedInstanceState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct PersistedInstanceState {
    updated_at_unix_ms: u64,
    limiters: HashMap<String, PersistedLimiterState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedLimiterState {
    theoretical_arrival_time_unix_nanos: u64,
}

#[derive(Clone)]
struct SharedGovernorState {
    inner: Arc<SharedGovernorStateInner>,
}

struct SharedGovernorStateInner {
    theoretical_arrival_time_nanos: AtomicU64,
    start_unix_nanos: u64,
}

impl std::fmt::Debug for SharedGovernorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nanos = self
            .inner
            .theoretical_arrival_time_nanos
            .load(Ordering::Relaxed);
        f.debug_struct("SharedGovernorState")
            .field("theoretical_arrival_time", &Duration::from_nanos(nanos))
            .finish_non_exhaustive()
    }
}

impl Default for SharedGovernorState {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedGovernorState {
    fn new() -> Self {
        Self {
            inner: Arc::new(SharedGovernorStateInner {
                theoretical_arrival_time_nanos: AtomicU64::new(0),
                start_unix_nanos: unix_nanos_now(),
            }),
        }
    }

    fn merge_absolute_nanos(&self, absolute_nanos: u64) {
        let relative_nanos = absolute_nanos.saturating_sub(self.inner.start_unix_nanos);
        self.inner
            .theoretical_arrival_time_nanos
            .fetch_max(relative_nanos, Ordering::AcqRel);
    }

    fn snapshot_absolute_nanos(&self) -> Option<u64> {
        let relative_nanos = self
            .inner
            .theoretical_arrival_time_nanos
            .load(Ordering::Acquire);
        if relative_nanos == 0 {
            None
        } else {
            Some(self.inner.start_unix_nanos.saturating_add(relative_nanos))
        }
    }
}

impl StateStore for SharedGovernorState {
    type Key = NotKeyed;

    fn measure_and_replace<T, F, E>(&self, _key: &Self::Key, f: F) -> std::result::Result<T, E>
    where
        F: Fn(Option<Nanos>) -> std::result::Result<(T, Nanos), E>,
    {
        let mut previous = self
            .inner
            .theoretical_arrival_time_nanos
            .load(Ordering::Acquire);
        let mut decision = f(NonZeroU64::new(previous).map(|nanos| Nanos::new(nanos.get())));

        while let Ok((result, next_state)) = decision {
            let next_state_nanos = u64::from(next_state);
            match self
                .inner
                .theoretical_arrival_time_nanos
                .compare_exchange_weak(
                    previous,
                    next_state_nanos,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                Ok(_) => return Ok(result),
                Err(next_previous) => previous = next_previous,
            }
            decision = f(NonZeroU64::new(previous).map(|nanos| Nanos::new(nanos.get())));
        }

        decision.map(|(result, _)| result)
    }
}

#[derive(Clone, Debug)]
struct PersistedLimiterBinding {
    key: String,
    state: SharedGovernorState,
}

enum PersistedWriteResult {
    Written,
    Conflict(Option<PersistedRateControlState>),
}

#[derive(Clone)]
struct RateControllerPersistenceConfig {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    object_key: String,
    origin: String,
    instance_id: String,
    instance_ttl: Duration,
}

impl std::fmt::Debug for RateControllerPersistenceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateControllerPersistenceConfig")
            .field("prefix", &self.prefix)
            .field("object_key", &self.object_key)
            .field("origin", &self.origin)
            .field("instance_id", &self.instance_id)
            .field("instance_ttl", &self.instance_ttl)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RateControllerPersistence {
    object_state: Arc<ObjectState<PersistedRateControlState>>,
    object_key: String,
    origin: String,
    instance_id: String,
    instance_ttl: Duration,
    limiters: Vec<PersistedLimiterBinding>,
}

#[derive(Debug, PartialEq, Eq)]
enum PersistResult {
    Persisted,
    Conflict,
}

impl RateControllerPersistence {
    fn new(
        config: RateControllerPersistenceConfig,
        limiters: Vec<PersistedLimiterBinding>,
    ) -> Arc<Self> {
        let prefix = normalize_object_state_prefix(&config.prefix);
        let object_state = Arc::new(ObjectState::new(config.store).with_prefix(prefix));
        Arc::new(Self {
            object_state,
            object_key: config.object_key,
            origin: config.origin,
            instance_id: config.instance_id,
            instance_ttl: config.instance_ttl,
            limiters,
        })
    }

    async fn refresh(&self) -> Result<()> {
        let remote = self.remote_state().await?;

        if let Some(remote) = remote {
            self.apply_remote(&remote)?;
        }

        Ok(())
    }

    async fn refresh_and_persist_snapshot(&self) -> Result<PersistResult> {
        self.persist_snapshot().await
    }

    async fn persist_snapshot(&self) -> Result<PersistResult> {
        let mut remote = self.remote_state().await?;

        for attempt in 0..MAX_PERSISTENCE_WRITE_ATTEMPTS {
            if let Some(remote) = &remote {
                self.apply_remote(remote)?;
            }

            let snapshot = self.snapshot(remote.as_ref())?;
            match self.write_snapshot(remote.is_some(), &snapshot).await? {
                PersistedWriteResult::Written => {
                    self.apply_remote(&snapshot)?;
                    return Ok(PersistResult::Persisted);
                }
                PersistedWriteResult::Conflict(current) => {
                    if let Some(current) = &current {
                        self.apply_remote(current)?;
                    }
                    remote = current;
                    if attempt + 1 == MAX_PERSISTENCE_WRITE_ATTEMPTS {
                        return Ok(PersistResult::Conflict);
                    }
                }
            }
        }

        Ok(PersistResult::Conflict)
    }

    async fn write_snapshot(
        &self,
        remote_exists: bool,
        snapshot: &PersistedRateControlState,
    ) -> Result<PersistedWriteResult> {
        if remote_exists {
            return match self
                .object_state
                .update(&self.object_key, snapshot)
                .await
                .map_err(|source| Error::PersistenceWrite {
                    origin: self.origin.clone(),
                    source: Box::new(source),
                })? {
                UpdateResult::Ok => Ok(PersistedWriteResult::Written),
                UpdateResult::NotFound => Ok(PersistedWriteResult::Conflict(None)),
                UpdateResult::Conflict { current } => {
                    Ok(PersistedWriteResult::Conflict(Some(current)))
                }
            };
        }

        match self
            .object_state
            .insert(&self.object_key, snapshot)
            .await
            .map_err(|source| Error::PersistenceWrite {
                origin: self.origin.clone(),
                source: Box::new(source),
            })? {
            InsertResult::Ok => Ok(PersistedWriteResult::Written),
            InsertResult::AlreadyExists => self
                .remote_state()
                .await
                .map(PersistedWriteResult::Conflict),
        }
    }

    async fn remote_state(&self) -> Result<Option<PersistedRateControlState>> {
        self.object_state
            .get(&self.object_key)
            .await
            .map_err(|source| Error::PersistenceRefresh {
                origin: self.origin.clone(),
                source: Box::new(source),
            })
    }

    fn snapshot(
        &self,
        remote: Option<&PersistedRateControlState>,
    ) -> Result<PersistedRateControlState> {
        if let Some(remote) = remote {
            self.validate_remote(remote)?;
        }

        let now_ms = unix_millis_now();
        let mut snapshot = remote
            .cloned()
            .unwrap_or_else(|| PersistedRateControlState {
                schema_version: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
                ..Default::default()
            });
        snapshot.schema_version = PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION;
        snapshot.updated_at_unix_ms = now_ms;
        let limiter_keys = self
            .limiters
            .iter()
            .map(|binding| binding.key.as_str())
            .collect::<HashSet<_>>();
        snapshot
            .limiters
            .retain(|limiter_key, _| limiter_keys.contains(limiter_key.as_str()));
        snapshot.instances.retain(|instance_id, instance| {
            instance_id == &self.instance_id || !self.is_stale_instance(instance, now_ms)
        });
        snapshot
            .instances
            .insert(self.instance_id.clone(), self.instance_snapshot(now_ms));
        if remote.is_some_and(|remote| !remote.instances.is_empty()) {
            snapshot.limiters =
                self.effective_limiter_snapshot(&snapshot, now_ms, unix_nanos_now());
        } else {
            self.merge_local_limiter_snapshot(&mut snapshot.limiters);
        }

        Ok(snapshot)
    }

    fn effective_limiter_snapshot(
        &self,
        persisted_state: &PersistedRateControlState,
        now_ms: u64,
        now_nanos: u64,
    ) -> HashMap<String, PersistedLimiterState> {
        if persisted_state.instances.is_empty() {
            return persisted_state.limiters.clone();
        }

        let limiter_keys = self
            .limiters
            .iter()
            .map(|binding| binding.key.as_str())
            .collect::<HashSet<_>>();
        let mut effective_limiters: HashMap<String, PersistedLimiterState> =
            HashMap::with_capacity(limiter_keys.len());

        for instance in persisted_state
            .instances
            .values()
            .filter(|instance| !self.is_stale_instance(instance, now_ms))
        {
            for (limiter_key, limiter) in &instance.limiters {
                if !limiter_keys.contains(limiter_key.as_str())
                    || limiter.theoretical_arrival_time_unix_nanos <= now_nanos
                {
                    continue;
                }

                effective_limiters
                    .entry(limiter_key.clone())
                    .and_modify(|effective_limiter| {
                        effective_limiter.theoretical_arrival_time_unix_nanos = effective_limiter
                            .theoretical_arrival_time_unix_nanos
                            .max(limiter.theoretical_arrival_time_unix_nanos);
                    })
                    .or_insert_with(|| limiter.clone());
            }
        }

        effective_limiters
    }

    fn merge_local_limiter_snapshot(
        &self,
        persisted_limiters: &mut HashMap<String, PersistedLimiterState>,
    ) {
        for (limiter_key, local_limiter) in self.local_limiter_snapshot() {
            persisted_limiters
                .entry(limiter_key)
                .and_modify(|persisted_limiter| {
                    persisted_limiter.theoretical_arrival_time_unix_nanos = persisted_limiter
                        .theoretical_arrival_time_unix_nanos
                        .max(local_limiter.theoretical_arrival_time_unix_nanos);
                })
                .or_insert(local_limiter);
        }
    }

    fn local_limiter_snapshot(&self) -> HashMap<String, PersistedLimiterState> {
        let mut limiters = HashMap::with_capacity(self.limiters.len());

        for binding in &self.limiters {
            if let Some(theoretical_arrival_time_unix_nanos) =
                binding.state.snapshot_absolute_nanos()
            {
                limiters.insert(
                    binding.key.clone(),
                    PersistedLimiterState {
                        theoretical_arrival_time_unix_nanos,
                    },
                );
            }
        }

        limiters
    }

    fn instance_snapshot(&self, now_ms: u64) -> PersistedInstanceState {
        PersistedInstanceState {
            updated_at_unix_ms: now_ms,
            limiters: self.local_limiter_snapshot(),
        }
    }

    fn apply_remote(&self, remote: &PersistedRateControlState) -> Result<()> {
        self.validate_remote(remote)?;

        let effective_limiters =
            self.effective_limiter_snapshot(remote, unix_millis_now(), unix_nanos_now());

        for binding in &self.limiters {
            if let Some(remote_state) = effective_limiters.get(&binding.key) {
                binding
                    .state
                    .merge_absolute_nanos(remote_state.theoretical_arrival_time_unix_nanos);
            }
        }

        Ok(())
    }

    fn validate_remote(&self, remote: &PersistedRateControlState) -> Result<()> {
        if remote.schema_version != PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION {
            return Err(Error::UnsupportedPersistedStateVersion {
                origin: self.origin.clone(),
                version: remote.schema_version,
            });
        }

        Ok(())
    }

    fn is_stale_instance(&self, instance: &PersistedInstanceState, now_ms: u64) -> bool {
        now_ms.saturating_sub(instance.updated_at_unix_ms) > duration_millis_u64(self.instance_ttl)
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
    persistence: Option<RateControllerPersistenceConfig>,
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

    #[must_use]
    pub fn with_object_store_persistence_for_instance(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        object_key: impl Into<String>,
        origin: impl Into<String>,
        instance_id: impl Into<String>,
        instance_ttl: Duration,
    ) -> Self {
        let instance_id = instance_id.into();
        self.persistence = Some(RateControllerPersistenceConfig {
            store,
            prefix: prefix.into(),
            object_key: object_key.into(),
            origin: origin.into(),
            instance_id: if instance_id.trim().is_empty() {
                "default".to_string()
            } else {
                instance_id
            },
            instance_ttl: if instance_ttl.is_zero() {
                DEFAULT_PERSISTED_INSTANCE_TTL
            } else {
                instance_ttl
            },
        });
        self
    }

    #[must_use]
    pub fn build(self) -> Arc<RateController> {
        let jitter = self.jitter;
        let mut persisted_limiters = Vec::new();
        let rate_limiters = self
            .quotas
            .into_iter()
            .enumerate()
            .map(|(index, quota_definition)| {
                let state = SharedGovernorState::new();
                let fallback_name = format!("quota-{index}");
                persisted_limiters.push(PersistedLimiterBinding {
                    key: quota_definition.persistence_key(&fallback_name),
                    state: state.clone(),
                });
                Arc::new(RateLimiter::new(
                    quota_definition.quota,
                    state,
                    DefaultClock::default(),
                ))
            })
            .collect::<Vec<_>>();

        let weighted_rate_limiter = self.weighted_quota.map(|quota_definition| {
            let state = SharedGovernorState::new();
            persisted_limiters.push(PersistedLimiterBinding {
                key: quota_definition.persistence_key("weighted"),
                state: state.clone(),
            });
            Arc::new(RateLimiter::new(
                quota_definition.quota,
                state,
                DefaultClock::default(),
            ))
        });

        let persistence = if persisted_limiters.is_empty() {
            None
        } else {
            self.persistence
                .map(|config| RateControllerPersistence::new(config, persisted_limiters))
        };

        let semaphore = self
            .max_concurrent_requests
            .map(|max_concurrent_requests| Arc::new(Semaphore::new(max_concurrent_requests)));

        RateController::new(
            jitter,
            rate_limiters,
            weighted_rate_limiter,
            semaphore,
            self.metrics.unwrap_or_default(),
            persistence,
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
    rate_limiters: Vec<Arc<GovernorRateLimiter>>,
    weighted_rate_limiter: Option<Arc<GovernorRateLimiter>>,
    semaphore: Option<Arc<Semaphore>>,
    metrics: Arc<RateControllerMetrics>,
    persistence: Option<Arc<RateControllerPersistence>>,
}

impl std::fmt::Debug for RateController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RateController")
            .field("jitter_config", &self.jitter_config)
            .field("rate_limiters", &self.rate_limiters.len())
            .field(
                "weighted_rate_limiter",
                &self.weighted_rate_limiter.is_some(),
            )
            .field("semaphore", &self.semaphore.is_some())
            .field("metrics", &self.metrics)
            .field("persistence", &self.persistence.is_some())
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
    /// Re-check the quotas from an existing permit.
    /// For example, a request was permitted but has failed and needs to be retried.
    /// The caller retains their permit, but needs to ensure the rate limiters are still ready.
    ///
    /// # Errors
    ///
    /// - If the weighted quota does not have sufficient capacity for the provided weight, this will return an `InsufficientCapacity` error.
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

    #[must_use]
    pub fn available_permits(&self) -> Option<usize> {
        self.semaphore
            .as_ref()
            .map(|semaphore| semaphore.available_permits())
    }

    /// Refreshes this controller's persisted governor state, if persistence is configured.
    ///
    /// This performs object-store I/O and is intended for background tasks, not request paths.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted state cannot be read or applied.
    pub async fn refresh_persisted_state(&self) -> Result<()> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };

        persistence.refresh().await
    }

    /// Refreshes this controller's persisted governor state and persists a fresh snapshot, if persistence is configured.
    ///
    /// This performs object-store I/O and is intended for background tasks, not request paths.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted state cannot be read, applied, or written.
    pub async fn refresh_and_persist_state_snapshot(&self) -> Result<()> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };

        match persistence.refresh_and_persist_snapshot().await? {
            PersistResult::Persisted => {}
            PersistResult::Conflict => {
                tracing::debug!(
                    origin = persistence.origin.as_str(),
                    "Persisted rate-control state changed concurrently; merged remote state and will persist on the next interval"
                );
            }
        }

        Ok(())
    }

    async fn until_ready(self: Arc<Self>) -> Result<()> {
        futures::future::join_all(
            self.rate_limiters
                .iter()
                .map(|limiter| limiter.until_ready()),
        )
        .await;

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
        rate_limiters: Vec<Arc<GovernorRateLimiter>>,
        weighted_rate_limiter: Option<Arc<GovernorRateLimiter>>,
        semaphore: Option<Arc<Semaphore>>,
        metrics: Arc<RateControllerMetrics>,
        persistence: Option<Arc<RateControllerPersistence>>,
    ) -> Arc<Self> {
        let jitter_config = jitter.unwrap_or(JitterConfig {
            min: Duration::ZERO,
            max: Duration::ZERO,
        });

        Arc::new(Self {
            jitter_config,
            rate_limiters,
            weighted_rate_limiter,
            semaphore,
            metrics,
            persistence,
        })
    }

    async fn wait_for_rate_limiters(self: &Arc<Self>, weight: Option<u32>) -> Result<()> {
        Arc::clone(self).until_weighted_ready(weight).await
    }

    /// Acquires a permit from the rate controller with a specified weight.
    /// Asynchronously waits for the rate limiters to be ready and optionally acquires a semaphore permit for maximum concurrency if configured.
    ///
    /// The provided weight is used to check against the weighted quota, if configured.
    ///
    /// # Errors
    ///
    /// - If the semaphore has been closed, this will return an error.
    /// - If the weighted quota does not have sufficient capacity for the provided weight, this will return an `InsufficientCapacity` error.
    pub async fn acquire_weighted(self: &Arc<Self>, weight: u32) -> Result<Permit> {
        self.acquire_weighted_opt(Some(weight)).await
    }

    /// Acquires a permit from the rate controller.
    /// Asynchronously waits for the rate limiters to be ready and optionally acquires a semaphore permit for maximum concurrency if configured.
    ///
    /// # Errors
    ///
    /// If the semaphore has been closed, this will return an error.
    pub async fn acquire(self: &Arc<Self>) -> Result<Permit> {
        self.acquire_weighted_opt(None).await
    }

    /// Acquires a permit from the rate controller with an optional weight.
    /// Asynchronously waits for the rate limiters to be ready and optionally acquires a semaphore permit for maximum concurrency if configured.
    ///
    /// The provided weight is used to check against the weighted quota, if configured.
    ///
    /// # Errors
    ///
    /// - If the semaphore has been closed, this will return an error.
    /// - If the weighted quota does not have sufficient capacity for the provided weight, this will return an `InsufficientCapacity` error.
    pub async fn acquire_weighted_opt(self: &Arc<Self>, weight: Option<u32>) -> Result<Permit> {
        let self_cloned = Arc::clone(self);
        let wait_start = tokio::time::Instant::now();

        // check for concurrency first - we may end up waiting for a concurrent request long enough that the rate limits clear
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

        // check all of the rate limiters async
        if let Err(error) = self.wait_for_rate_limiters(weight).await {
            self.metrics.record_acquire_error(wait_start.elapsed());
            return Err(error);
        }

        // add jitter
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

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn normalize_object_state_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

fn unix_nanos_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_nanos_u64(duration),
        Err(error) => {
            tracing::warn!("Failed to read system time for rate-control state: {error}");
            0
        }
    }
}

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
    use std::{num::NonZeroU32, time::Instant};

    use super::*;
    use object_store::memory::InMemory;
    use object_store_occ::WriteResult;

    #[tokio::test]
    async fn test_rate_limiter_acquire() {
        let rate_controller = RateControllerBuilder::new()
            .with_jitter(JitterConfig {
                min: Duration::from_millis(100),
                max: Duration::from_millis(200),
            })
            .with_max_concurrent_requests(5)
            .add_quota(Quota::per_second(
                NonZeroU32::new(10).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        let permit = rate_controller.acquire().await;
        assert!(
            permit.is_ok(),
            "Failed to acquire permit: {:?}",
            permit.err()
        );
        let permit = permit.expect("should be Ok");
        assert!(
            permit.semaphore.is_some(),
            "Semaphore permit should be Some if semaphore is configured"
        );

        // Test that semaphore restricts concurrency
        drop(permit);
        let permits = (0..5)
            .map(|_| rate_controller.acquire())
            .collect::<Vec<_>>();
        let mut results = futures::future::try_join_all(permits)
            .await
            .expect("Should acquire all permits");

        // the next request should block until one of the permits is dropped
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected semaphore to block, but it did not.");
            },
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        };

        // dropping one permit should allow the next request to immediately acquire a permit
        drop(
            results
                .pop()
                .expect("Should have at least one permit to drop"),
        );

        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after dropping one: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_some(), "Semaphore permit should be Some if semaphore is configured");
            },
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Expected to acquire a permit after dropping one, but timed out.");
            }
        }
    }

    async fn wait_for_persisted_limiter_state(
        object_state: &ObjectState<PersistedRateControlState>,
        object_key: &str,
        timeout: Duration,
    ) -> PersistedRateControlState {
        let start = Instant::now();
        loop {
            if let Some(state) = object_state
                .get(object_key)
                .await
                .expect("read persisted state")
                && !state.limiters.is_empty()
            {
                return state;
            }

            assert!(
                start.elapsed() < timeout,
                "persisted state was not written within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn acquiring_permit_does_not_write_persisted_state_on_query_path() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_key = "test_spicepod/api.example.com_443";
        let object_state: ObjectState<PersistedRateControlState> =
            ObjectState::new(Arc::clone(&store));
        let quota = Quota::with_period(Duration::from_millis(500))
            .expect("quota period should be non-zero")
            .allow_burst(NonZeroU32::new(1).expect("burst should be non-zero"));

        let rate_controller = RateControllerBuilder::new()
            .add_quota_with_name("requests", quota)
            .with_object_store_persistence(store, "", object_key, "https://api.example.com:443")
            .build();

        let permit = rate_controller
            .acquire()
            .await
            .expect("controller should acquire immediately");
        drop(permit);

        assert!(
            object_state
                .get(object_key)
                .await
                .expect("read persisted state")
                .is_none(),
            "acquiring a permit must not write persisted state on the query path"
        );
    }

    #[tokio::test]
    async fn background_persistence_writes_persisted_state() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_key = "test_spicepod/api.example.com_443";
        let object_state: ObjectState<PersistedRateControlState> =
            ObjectState::new(Arc::clone(&store));
        let quota = Quota::with_period(Duration::from_millis(500))
            .expect("quota period should be non-zero")
            .allow_burst(NonZeroU32::new(1).expect("burst should be non-zero"));

        let rate_controller = RateControllerBuilder::new()
            .add_quota_with_name("requests", quota)
            .with_object_store_persistence(store, "", object_key, "https://api.example.com:443")
            .build();

        let permit = rate_controller
            .acquire()
            .await
            .expect("controller should acquire immediately");
        drop(permit);
        rate_controller
            .refresh_and_persist_state_snapshot()
            .await
            .expect("background persistence caller should write persisted state");

        let persisted = object_state
            .get(object_key)
            .await
            .expect("read persisted state")
            .expect("background persistence should write persisted state");
        assert!(!persisted.limiters.is_empty());
        assert!(persisted.instances.contains_key("default"));
    }

    #[tokio::test]
    async fn persisted_governor_state_can_be_written_by_background_caller() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_key = "test_spicepod/api.example.com_443";
        let object_state: ObjectState<PersistedRateControlState> =
            ObjectState::new(Arc::clone(&store));
        let quota = Quota::with_period(Duration::from_millis(200))
            .expect("quota period should be non-zero")
            .allow_burst(NonZeroU32::new(1).expect("burst should be non-zero"));
        let quota_definition = QuotaDefinition::new(Some("requests".to_string()), quota);
        let limiter_key = quota_definition.persistence_key("requests");

        let rate_controller = RateControllerBuilder::new()
            .add_quota_with_name("requests", quota)
            .with_object_store_persistence(store, "", object_key, "https://api.example.com:443")
            .build();

        let permit = rate_controller
            .acquire()
            .await
            .expect("controller should acquire immediately");
        drop(permit);

        rate_controller
            .refresh_and_persist_state_snapshot()
            .await
            .expect("background persistence caller should write a snapshot");

        let persisted =
            wait_for_persisted_limiter_state(&object_state, object_key, Duration::from_secs(1))
                .await;
        assert!(persisted.limiters.contains_key(&limiter_key));
    }

    #[tokio::test]
    async fn persisted_governor_state_is_applied_after_controller_recreation() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let quota = Quota::with_period(Duration::from_millis(200))
            .expect("quota period should be non-zero")
            .allow_burst(NonZeroU32::new(1).expect("burst should be non-zero"));
        let object_key = "test_spicepod/api.example.com_443";
        let quota_definition = QuotaDefinition::new(Some("requests".to_string()), quota);
        let limiter_key = quota_definition.persistence_key("requests");

        let persisted_state = PersistedRateControlState {
            schema_version: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
            updated_at_unix_ms: unix_millis_now(),
            limiters: HashMap::from([(
                limiter_key,
                PersistedLimiterState {
                    theoretical_arrival_time_unix_nanos: unix_nanos_now()
                        .saturating_add(500_000_000),
                },
            )]),
            instances: HashMap::new(),
        };
        let object_state: ObjectState<PersistedRateControlState> =
            ObjectState::new(Arc::clone(&store));
        assert_eq!(
            object_state
                .insert_or_update(object_key, &persisted_state)
                .await
                .expect("seed persisted state"),
            WriteResult::Inserted
        );

        let recreated_controller = RateControllerBuilder::new()
            .add_quota_with_name("requests", quota)
            .with_object_store_persistence(store, "", object_key, "https://api.example.com:443")
            .build();
        recreated_controller
            .refresh_persisted_state()
            .await
            .expect("background persistence caller should refresh persisted state");

        tokio::select! {
            recreated = recreated_controller.acquire() => {
                panic!("recreated controller should wait on persisted state, got: {recreated:?}");
            }
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }

        tokio::select! {
            recreated = recreated_controller.acquire() => {
                let permit = recreated.expect("recreated controller should acquire after shared quota replenishes");
                drop(permit);
            }
            () = tokio::time::sleep(Duration::from_millis(600)) => {
                panic!("recreated controller did not honor persisted Unix-epoch governor state");
            }
        }
    }

    #[tokio::test]
    async fn persisted_snapshot_prunes_stale_limiter_keys() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_key = "test_spicepod/api.example.com_443";
        let limiter_state = SharedGovernorState::new();
        limiter_state.merge_absolute_nanos(unix_nanos_now().saturating_add(1_000_000));

        let persistence = RateControllerPersistence::new(
            RateControllerPersistenceConfig {
                store,
                prefix: String::new(),
                object_key: object_key.to_string(),
                origin: "https://api.example.com:443".to_string(),
                instance_id: "test-instance".to_string(),
                instance_ttl: DEFAULT_PERSISTED_INSTANCE_TTL,
            },
            vec![PersistedLimiterBinding {
                key: "current-limiter".to_string(),
                state: limiter_state,
            }],
        );

        let remote = PersistedRateControlState {
            schema_version: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
            updated_at_unix_ms: unix_millis_now(),
            limiters: HashMap::from([
                (
                    "current-limiter".to_string(),
                    PersistedLimiterState {
                        theoretical_arrival_time_unix_nanos: unix_nanos_now(),
                    },
                ),
                (
                    "stale-limiter".to_string(),
                    PersistedLimiterState {
                        theoretical_arrival_time_unix_nanos: unix_nanos_now(),
                    },
                ),
            ]),
            instances: HashMap::new(),
        };
        persistence
            .object_state
            .insert_or_update(object_key, &remote)
            .await
            .expect("seed persisted state");

        assert_eq!(
            persistence
                .persist_snapshot()
                .await
                .expect("persist snapshot"),
            PersistResult::Persisted
        );

        let persisted = persistence
            .object_state
            .get(object_key)
            .await
            .expect("read persisted state")
            .expect("persisted state should exist");

        assert!(persisted.limiters.contains_key("current-limiter"));
        assert!(!persisted.limiters.contains_key("stale-limiter"));
    }

    #[tokio::test]
    async fn persisted_background_refresh_uses_shared_global_limiter_state() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_key = "test_spicepod/api.example.com_443";
        let quota = Quota::with_period(Duration::from_secs(1))
            .expect("quota period should be non-zero")
            .allow_burst(NonZeroU32::new(1).expect("burst should be non-zero"));

        let first_controller = RateControllerBuilder::new()
            .add_quota_with_name("requests", quota)
            .with_object_store_persistence_for_instance(
                Arc::clone(&store),
                "",
                object_key,
                "https://api.example.com:443",
                "first-instance",
                DEFAULT_PERSISTED_INSTANCE_TTL,
            )
            .build();
        let second_controller = RateControllerBuilder::new()
            .add_quota_with_name("requests", quota)
            .with_object_store_persistence_for_instance(
                Arc::clone(&store),
                "",
                object_key,
                "https://api.example.com:443",
                "second-instance",
                DEFAULT_PERSISTED_INSTANCE_TTL,
            )
            .build();

        let first_permit = first_controller
            .acquire()
            .await
            .expect("first acquire should update local governor state");
        drop(first_permit);
        first_controller
            .refresh_and_persist_state_snapshot()
            .await
            .expect("background persistence should write first instance state");
        second_controller
            .refresh_persisted_state()
            .await
            .expect("second instance should refresh persisted state");

        tokio::select! {
            second = second_controller.acquire() => {
                panic!("second instance should wait on the shared persisted limiter, got: {second:?}");
            }
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }

        tokio::select! {
            second = second_controller.acquire() => {
                let permit = second.expect("second instance should acquire after the shared quota replenishes");
                drop(permit);
            }
            () = tokio::time::sleep(Duration::from_millis(1_200)) => {
                panic!("second instance waited longer than one quota interval");
            }
        }
        second_controller
            .refresh_and_persist_state_snapshot()
            .await
            .expect("background persistence should write second instance state");

        let persisted = ObjectState::<PersistedRateControlState>::new(store)
            .get(object_key)
            .await
            .expect("read persisted state")
            .expect("persisted state should exist");
        assert!(persisted.instances.contains_key("first-instance"));
        assert!(persisted.instances.contains_key("second-instance"));
    }

    #[tokio::test]
    async fn expired_persisted_instance_does_not_hold_shared_budget() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let object_key = "test_spicepod/api.example.com_443";
        let quota = Quota::with_period(Duration::from_millis(200))
            .expect("quota period should be non-zero")
            .allow_burst(NonZeroU32::new(1).expect("burst should be non-zero"));
        let quota_definition = QuotaDefinition::new(Some("requests".to_string()), quota);
        let limiter_key = quota_definition.persistence_key("requests");
        let stale_limiter = PersistedLimiterState {
            theoretical_arrival_time_unix_nanos: unix_nanos_now().saturating_add(10_000_000_000),
        };
        let expired_at_ms = unix_millis_now().saturating_sub(1_000);
        let seeded_state = PersistedRateControlState {
            schema_version: PERSISTED_RATE_CONTROL_STATE_SCHEMA_VERSION,
            updated_at_unix_ms: expired_at_ms,
            limiters: HashMap::from([(limiter_key.clone(), stale_limiter.clone())]),
            instances: HashMap::from([(
                "offline-instance".to_string(),
                PersistedInstanceState {
                    updated_at_unix_ms: expired_at_ms,
                    limiters: HashMap::from([(limiter_key, stale_limiter)]),
                },
            )]),
        };

        let object_state = ObjectState::<PersistedRateControlState>::new(Arc::clone(&store));
        assert_eq!(
            object_state
                .insert_or_update(object_key, &seeded_state)
                .await
                .expect("seed stale persisted state"),
            WriteResult::Inserted
        );
        let controller = RateControllerBuilder::new()
            .add_quota_with_name("requests", quota)
            .with_object_store_persistence_for_instance(
                store,
                "",
                object_key,
                "https://api.example.com:443",
                "active-instance",
                Duration::from_millis(10),
            )
            .build();
        controller
            .refresh_persisted_state()
            .await
            .expect("background refresh should ignore stale persisted state");

        tokio::select! {
            acquired = controller.acquire() => {
                let permit = acquired.expect("active instance should acquire after stale state expires");
                drop(permit);
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {
                panic!("expired instance state should not hold the shared budget");
            }
        }
        controller
            .refresh_and_persist_state_snapshot()
            .await
            .expect("background persistence should prune stale instance state");

        let persisted = object_state
            .get(object_key)
            .await
            .expect("read persisted state")
            .expect("persisted state should exist");
        assert!(!persisted.instances.contains_key("offline-instance"));
        assert!(persisted.instances.contains_key("active-instance"));
    }

    #[tokio::test]
    async fn test_rate_controller_metrics_track_permits() {
        let metrics = Arc::new(RateControllerMetrics::default());
        let rate_controller = RateControllerBuilder::new()
            .with_max_concurrent_requests(1)
            .with_metrics(Arc::clone(&metrics))
            .build();

        assert_eq!(rate_controller.available_permits(), Some(1));
        let permit = rate_controller
            .acquire()
            .await
            .expect("permit should be acquired");

        assert_eq!(metrics.permits_acquired_total(), 1);
        assert_eq!(metrics.acquire_errors_total(), 0);
        assert_eq!(metrics.inflight_permits(), 1);
        assert_eq!(rate_controller.available_permits(), Some(0));

        drop(permit);

        assert_eq!(metrics.inflight_permits(), 0);
        assert_eq!(rate_controller.available_permits(), Some(1));
    }

    #[tokio::test]
    async fn test_rate_limiter_permit_waits() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                NonZeroU32::new(2).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        let permit = rate_controller.acquire().await;
        assert!(
            permit.is_ok(),
            "Failed to acquire permit: {:?}",
            permit.err()
        );
        let permit = permit.expect("should be Ok");

        // Make 1 more wait to fill the quota from the permit (2 per second)
        permit.until_ready().await.expect("should wait until ready");

        // The next request should wait until the rate limit is reset (quota exhausted)
        tokio::select! {
            _ = permit.until_ready() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            // 2/second means 1 every 500ms, wait less than that
            () = tokio::time::sleep(Duration::from_millis(300)) => {}
        }

        // permit should be able to be ready after the rate limit resets
        tokio::select! {
            _ = permit.until_ready() => {}
            // Give plenty of time for the 500ms reset
            () = tokio::time::sleep(Duration::from_millis(800)) => {
                panic!("Expected to be able to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_per_second() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                NonZeroU32::new(2).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        // acquire all 2 permits at once, which should exhaust the rate limit
        futures::future::try_join_all((0..2).map(|_| rate_controller.acquire()))
            .await
            .expect("Should acquire all permits");

        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            () = tokio::time::sleep(Duration::from_millis(400)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(400)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_with_multiple_quotas() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // purposely set a high per-second limit which should not be hit
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .add_quota(Quota::per_second(
                // should result in this quota being hit (2 per second = 1 every 500ms)
                NonZeroU32::new(2).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        // acquire both permits at once, which should exhaust the stricter rate limit
        futures::future::try_join_all((0..2).map(|_| rate_controller.acquire()))
            .await
            .expect("Should acquire all permits");

        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            // 2/second is 1 every 500ms, wait less than that
            () = tokio::time::sleep(Duration::from_millis(300)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            // Give plenty of time for the 500ms reset
            () = tokio::time::sleep(Duration::from_millis(800)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_hits_multiple_quotas() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // per-second will get hit first
                NonZeroU32::new(4).expect("NonZeroU32 should be non-zero"),
            ))
            .add_quota(Quota::per_minute(
                // then per-minute will get hit
                NonZeroU32::new(6).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        // acquire all 4 permits at once, which should exhaust the per-second rate limit
        futures::future::try_join_all((0..4).map(|_| rate_controller.acquire()))
            .await
            .expect("Should acquire all permits");

        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(200)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }

        // now we've hit the per-minute limit
        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            // 6/minute is 1 every 10 seconds
            () = tokio::time::sleep(Duration::from_secs(9)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_secs(2)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_jitter() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // purposely set a high per-second limit which should not be hit
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .with_jitter(JitterConfig {
                min: Duration::from_millis(1000),
                max: Duration::from_millis(2000),
            })
            .build();

        // acquiring a permit should wait at least for the jitter minimum duration
        let start = Instant::now();
        tokio::select! {
            permit = rate_controller.acquire() => {
                let end = Instant::now();
                let elapsed = end.duration_since(start);
                assert!(elapsed >= Duration::from_millis(1000), "Expected at least 1000ms of jitter, but got {elapsed:?}");
                assert!(permit.is_ok(), "Failed to acquire permit: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(2000)) => {
                panic!("Expected to wait for up to 2000ms, but timed out.");
            }
        }

        // a rate limit without jitter should complete near immediately
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // purposely set a high per-second limit which should not be hit
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(10)) => {
                panic!("Expected to acquire a permit immediately, but timed out.");
            }
        }

        // rate limiter with multiple quotas should apply jitter only once
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .add_quota(Quota::per_minute(
                NonZeroU32::new(10).expect("NonZeroU32 should be non-zero"),
            ))
            .with_jitter(JitterConfig {
                min: Duration::from_millis(1000),
                max: Duration::from_millis(2000),
            })
            .build();

        let start = Instant::now();
        tokio::select! {
            permit = rate_controller.acquire() => {
                let end = Instant::now();
                let elapsed = end.duration_since(start);
                assert!(elapsed >= Duration::from_millis(1000), "Expected at least 1000ms of jitter, but got {elapsed:?}");
                assert!(permit.is_ok(), "Failed to acquire permit: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.semaphore.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(2000)) => {
                panic!("Expected to wait for up to 2000ms, but timed out.");
            }
        }
    }
}
