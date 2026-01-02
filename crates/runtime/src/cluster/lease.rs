/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Lease-based task slot reservation for executors.
//!
//! This module implements a lease management system that allows schedulers to
//! reserve task slots on executors before dispatching work. This prevents
//! over-subscription and enables better load balancing.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// TTL for request ID cache entries (idempotency).
const REQUEST_ID_CACHE_TTL: Duration = Duration::from_secs(60);

/// Default interval for the background cleanup loop.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);

/// Unique identifier for a lease.
pub type LeaseId = String;

/// Unique identifier for a request (used for idempotency).
pub type RequestId = String;

/// A lease representing reserved task slots.
#[derive(Debug, Clone)]
pub struct Lease {
    /// Unique lease identifier.
    pub id: LeaseId,
    /// Scheduler that owns this lease.
    pub scheduler_id: String,
    /// Number of reserved slots.
    pub slots: u32,
    /// Absolute expiry time (system time).
    pub expires_at: SystemTime,
    /// Slots that have been claimed for active tasks.
    pub slots_in_use: u32,
}

impl Lease {
    /// Returns true if this lease has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        SystemTime::now() >= self.expires_at
    }

    /// Returns the expiry time as milliseconds since UNIX epoch.
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Returns the number of slots available for use (reserved but not yet claimed).
    #[must_use]
    pub fn available_slots(&self) -> u32 {
        self.slots.saturating_sub(self.slots_in_use)
    }
}

/// Cached response for idempotent request handling.
#[derive(Debug, Clone)]
struct CachedResponse {
    /// The lease ID that was created.
    lease_id: LeaseId,
    /// Number of slots reserved.
    slots_reserved: u32,
    /// Absolute expiry time as ms since epoch.
    expires_at_ms: u64,
    /// When this cache entry expires.
    cache_expires_at: Instant,
}

/// Result of a slot reservation attempt.
#[derive(Debug, Clone)]
pub struct ReserveResult {
    /// The lease ID.
    pub lease_id: LeaseId,
    /// Number of slots reserved.
    pub slots_reserved: u32,
    /// Absolute expiry time as ms since epoch.
    pub expires_at_ms: u64,
    /// True if served from idempotency cache.
    pub from_cache: bool,
}

/// Callback trait for handling lease expiration.
///
/// Implementations can use this to cancel tasks when their lease expires.
#[async_trait::async_trait]
pub trait LeaseExpirationHandler: Send + Sync {
    /// Called when a lease expires with tasks still using its slots.
    async fn on_lease_expired(&self, lease_id: &str, scheduler_id: &str, slots_in_use: u32);
}

/// Internal state for the lease manager.
struct LeaseManagerInner {
    /// Active leases indexed by lease ID.
    leases: HashMap<LeaseId, Lease>,
    /// Request ID to response cache for idempotency.
    request_cache: HashMap<RequestId, CachedResponse>,
    /// Executor ID for logging.
    executor_id: String,
    /// Total configured task slots.
    total_slots: u32,
}

impl LeaseManagerInner {
    /// Calculates the total number of leased (reserved) slots.
    fn leased_slots(&self) -> u32 {
        self.leases.values().map(|l| l.slots).sum()
    }

    /// Calculates the total number of in-use slots across all leases.
    fn in_use_slots(&self) -> u32 {
        self.leases.values().map(|l| l.slots_in_use).sum()
    }

    /// Calculates the number of available slots for new leases.
    fn available_for_lease(&self) -> u32 {
        self.total_slots.saturating_sub(self.leased_slots())
    }
}

/// Manages task slot leases for an executor.
///
/// The `LeaseManager` allows schedulers to reserve task slots before dispatching
/// tasks. This prevents over-subscription and enables better load balancing
/// across the cluster.
pub struct LeaseManager {
    inner: Arc<RwLock<LeaseManagerInner>>,
    /// Handler for lease expiration events.
    expiration_handler: Option<Arc<dyn LeaseExpirationHandler>>,
    /// Token for canceling the background cleanup task.
    shutdown_token: CancellationToken,
    /// Atomic counter for available slots (for fast reads).
    available_slots: AtomicU32,
}

impl LeaseManager {
    /// Creates a new `LeaseManager` with the given executor ID and total slots.
    #[must_use]
    pub fn new(executor_id: String, total_slots: u32) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LeaseManagerInner {
                leases: HashMap::new(),
                request_cache: HashMap::new(),
                executor_id,
                total_slots,
            })),
            expiration_handler: None,
            shutdown_token: CancellationToken::new(),
            available_slots: AtomicU32::new(total_slots),
        }
    }

    /// Sets the lease expiration handler.
    pub fn set_expiration_handler(&mut self, handler: Arc<dyn LeaseExpirationHandler>) {
        self.expiration_handler = Some(handler);
    }

    /// Returns the executor ID.
    pub async fn executor_id(&self) -> String {
        self.inner.read().await.executor_id.clone()
    }

    /// Returns the total configured slots.
    pub async fn total_slots(&self) -> u32 {
        self.inner.read().await.total_slots
    }

    /// Returns the current number of available slots (fast atomic read).
    pub fn available_slots_fast(&self) -> u32 {
        self.available_slots.load(Ordering::Acquire)
    }

    /// Returns detailed capacity information.
    pub async fn get_capacity(&self) -> (u32, u32, u32, u32) {
        let inner = self.inner.read().await;
        let total = inner.total_slots;
        let leased = inner.leased_slots();
        let in_use = inner.in_use_slots();
        let available = total.saturating_sub(leased);
        (total, available, leased, in_use)
    }

    /// Reserves task slots with idempotency support.
    ///
    /// If a request with the same `request_id` was made within the cache TTL,
    /// returns the cached response.
    pub async fn reserve_slots(
        &self,
        request_id: &str,
        scheduler_id: &str,
        slots: u32,
        lease_duration: Duration,
    ) -> ReserveResult {
        let mut inner = self.inner.write().await;

        // Check idempotency cache first
        if let Some(cached) = inner.request_cache.get(request_id) {
            if cached.cache_expires_at > Instant::now() {
                tracing::debug!(
                    request_id = %request_id,
                    lease_id = %cached.lease_id,
                    "Returning cached lease response"
                );
                return ReserveResult {
                    lease_id: cached.lease_id.clone(),
                    slots_reserved: cached.slots_reserved,
                    expires_at_ms: cached.expires_at_ms,
                    from_cache: true,
                };
            }
            // Cache entry expired, remove it
            inner.request_cache.remove(request_id);
        }

        // Calculate how many slots we can actually reserve
        let available = inner.available_for_lease();
        let slots_to_reserve = slots.min(available);

        // Generate lease ID and calculate expiry
        let lease_id = Uuid::new_v4().to_string();
        let expires_at = SystemTime::now() + lease_duration;
        #[expect(clippy::cast_possible_truncation)]
        let expires_at_ms = expires_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Create the lease
        let lease = Lease {
            id: lease_id.clone(),
            scheduler_id: scheduler_id.to_string(),
            slots: slots_to_reserve,
            expires_at,
            slots_in_use: 0,
        };

        tracing::info!(
            lease_id = %lease_id,
            scheduler_id = %scheduler_id,
            slots_requested = slots,
            slots_reserved = slots_to_reserve,
            expires_at_ms = expires_at_ms,
            "Created new lease"
        );

        inner.leases.insert(lease_id.clone(), lease);

        // Cache the response for idempotency
        let cached = CachedResponse {
            lease_id: lease_id.clone(),
            slots_reserved: slots_to_reserve,
            expires_at_ms,
            cache_expires_at: Instant::now() + REQUEST_ID_CACHE_TTL,
        };
        inner.request_cache.insert(request_id.to_string(), cached);

        // Update atomic counter
        let new_available = inner.available_for_lease();
        self.available_slots.store(new_available, Ordering::Release);

        ReserveResult {
            lease_id,
            slots_reserved: slots_to_reserve,
            expires_at_ms,
            from_cache: false,
        }
    }

    /// Renews an existing lease, extending its expiry time.
    ///
    /// Returns `Ok(new_expires_at_ms)` on success, `Err` if lease not found or
    /// owned by a different scheduler.
    pub async fn renew_lease(
        &self,
        lease_id: &str,
        scheduler_id: &str,
        lease_duration: Duration,
    ) -> Result<u64, &'static str> {
        let mut inner = self.inner.write().await;

        let Some(lease) = inner.leases.get_mut(lease_id) else {
            return Err("Lease not found");
        };

        if lease.scheduler_id != scheduler_id {
            return Err("Lease owned by different scheduler");
        }

        // Extend the lease
        let new_expires_at = SystemTime::now() + lease_duration;
        #[expect(clippy::cast_possible_truncation)]
        let new_expires_at_ms = new_expires_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        lease.expires_at = new_expires_at;

        tracing::debug!(
            lease_id = %lease_id,
            scheduler_id = %scheduler_id,
            new_expires_at_ms = new_expires_at_ms,
            "Renewed lease"
        );

        Ok(new_expires_at_ms)
    }

    /// Releases a lease, returning its slots to the available pool.
    ///
    /// Returns the number of slots that were released.
    pub async fn release_lease(
        &self,
        lease_id: &str,
        scheduler_id: &str,
    ) -> Result<u32, &'static str> {
        let mut inner = self.inner.write().await;

        let Some(lease) = inner.leases.remove(lease_id) else {
            // Already released or expired - not an error
            return Ok(0);
        };

        if lease.scheduler_id != scheduler_id {
            // Put it back and return error
            inner.leases.insert(lease_id.to_string(), lease);
            return Err("Lease owned by different scheduler");
        }

        let slots_released = lease.slots;

        tracing::info!(
            lease_id = %lease_id,
            scheduler_id = %scheduler_id,
            slots_released = slots_released,
            "Released lease"
        );

        // Update atomic counter
        let new_available = inner.available_for_lease();
        self.available_slots.store(new_available, Ordering::Release);

        Ok(slots_released)
    }

    /// Tries to claim a slot from a lease for task execution.
    ///
    /// Returns `Ok(())` if a slot was claimed, `Err` if no slots available.
    pub async fn try_use_slot(&self, lease_id: &str) -> Result<(), &'static str> {
        let mut inner = self.inner.write().await;

        let Some(lease) = inner.leases.get_mut(lease_id) else {
            return Err("Lease not found");
        };

        if lease.is_expired() {
            return Err("Lease expired");
        }

        if lease.available_slots() == 0 {
            return Err("No available slots in lease");
        }

        lease.slots_in_use += 1;

        tracing::trace!(
            lease_id = %lease_id,
            slots_in_use = lease.slots_in_use,
            slots_total = lease.slots,
            "Claimed slot from lease"
        );

        Ok(())
    }

    /// Returns a slot to a lease after task completion.
    ///
    /// If the lease no longer exists (expired), this is a no-op.
    pub async fn return_slot(&self, lease_id: &str) {
        let mut inner = self.inner.write().await;

        if let Some(lease) = inner.leases.get_mut(lease_id) {
            lease.slots_in_use = lease.slots_in_use.saturating_sub(1);

            tracing::trace!(
                lease_id = %lease_id,
                slots_in_use = lease.slots_in_use,
                slots_total = lease.slots,
                "Returned slot to lease"
            );
        }
    }

    /// Starts the background cleanup loop.
    ///
    /// This loop periodically removes expired leases and stale cache entries.
    pub fn start_cleanup_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(&self);
        let shutdown = self.shutdown_token.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    () = shutdown.cancelled() => {
                        tracing::debug!("LeaseManager cleanup loop shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        manager.cleanup_expired().await;
                    }
                }
            }
        })
    }

    /// Removes expired leases and cache entries.
    async fn cleanup_expired(&self) {
        let mut inner = self.inner.write().await;
        let now = Instant::now();
        let sys_now = SystemTime::now();

        // Collect expired leases
        let expired_leases: Vec<_> = inner
            .leases
            .iter()
            .filter(|(_, lease)| lease.expires_at <= sys_now)
            .map(|(id, lease)| (id.clone(), lease.scheduler_id.clone(), lease.slots_in_use))
            .collect();

        // Remove expired leases and notify handler
        for (lease_id, scheduler_id, slots_in_use) in expired_leases {
            inner.leases.remove(&lease_id);

            if slots_in_use > 0 {
                tracing::warn!(
                    lease_id = %lease_id,
                    scheduler_id = %scheduler_id,
                    slots_in_use = slots_in_use,
                    "Lease expired with slots still in use"
                );

                // Notify expiration handler if configured
                if let Some(handler) = &self.expiration_handler {
                    let handler = Arc::clone(handler);
                    let lease_id = lease_id.clone();
                    let scheduler_id = scheduler_id.clone();
                    // Spawn to avoid holding the lock during callback
                    tokio::spawn(async move {
                        handler
                            .on_lease_expired(&lease_id, &scheduler_id, slots_in_use)
                            .await;
                    });
                }
            } else {
                tracing::debug!(
                    lease_id = %lease_id,
                    scheduler_id = %scheduler_id,
                    "Removed expired lease"
                );
            }
        }

        // Remove expired cache entries
        inner
            .request_cache
            .retain(|_, cached| cached.cache_expires_at > now);

        // Update atomic counter
        let new_available = inner.available_for_lease();
        drop(inner); // Release lock before atomic store
        self.available_slots.store(new_available, Ordering::Release);
    }

    /// Shuts down the lease manager, canceling the cleanup loop.
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }
}

impl Drop for LeaseManager {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reserve_and_release() {
        let manager = LeaseManager::new("test-executor".to_string(), 10);

        // Reserve 5 slots
        let result = manager
            .reserve_slots("req-1", "scheduler-1", 5, Duration::from_secs(60))
            .await;
        assert_eq!(result.slots_reserved, 5);
        assert!(!result.from_cache);

        // Check capacity
        let (total, available, leased, in_use) = manager.get_capacity().await;
        assert_eq!(total, 10);
        assert_eq!(available, 5);
        assert_eq!(leased, 5);
        assert_eq!(in_use, 0);

        // Release the lease
        let released = manager
            .release_lease(&result.lease_id, "scheduler-1")
            .await
            .unwrap();
        assert_eq!(released, 5);

        // Check capacity after release
        let (_, available, leased, _) = manager.get_capacity().await;
        assert_eq!(available, 10);
        assert_eq!(leased, 0);
    }

    #[tokio::test]
    async fn test_idempotency() {
        let manager = LeaseManager::new("test-executor".to_string(), 10);

        // First request
        let result1 = manager
            .reserve_slots("req-1", "scheduler-1", 5, Duration::from_secs(60))
            .await;
        assert!(!result1.from_cache);

        // Same request ID should return cached response
        let result2 = manager
            .reserve_slots("req-1", "scheduler-1", 5, Duration::from_secs(60))
            .await;
        assert!(result2.from_cache);
        assert_eq!(result1.lease_id, result2.lease_id);
        assert_eq!(result1.slots_reserved, result2.slots_reserved);

        // Different request ID should create new lease
        let result3 = manager
            .reserve_slots("req-2", "scheduler-1", 3, Duration::from_secs(60))
            .await;
        assert!(!result3.from_cache);
        assert_ne!(result1.lease_id, result3.lease_id);
    }

    #[tokio::test]
    async fn test_try_use_and_return_slot() {
        let manager = LeaseManager::new("test-executor".to_string(), 10);

        let result = manager
            .reserve_slots("req-1", "scheduler-1", 2, Duration::from_secs(60))
            .await;

        // Use both slots
        manager.try_use_slot(&result.lease_id).await.unwrap();
        manager.try_use_slot(&result.lease_id).await.unwrap();

        // Third attempt should fail
        assert!(manager.try_use_slot(&result.lease_id).await.is_err());

        // Return one slot
        manager.return_slot(&result.lease_id).await;

        // Now we can use another
        manager.try_use_slot(&result.lease_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_renew_lease() {
        let manager = LeaseManager::new("test-executor".to_string(), 10);

        let result = manager
            .reserve_slots("req-1", "scheduler-1", 5, Duration::from_secs(10))
            .await;

        // Renew with longer duration
        let new_expires = manager
            .renew_lease(&result.lease_id, "scheduler-1", Duration::from_secs(60))
            .await
            .unwrap();

        assert!(new_expires > result.expires_at_ms);

        // Wrong scheduler should fail
        assert!(
            manager
                .renew_lease(&result.lease_id, "scheduler-2", Duration::from_secs(60))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_capacity_limits() {
        let manager = LeaseManager::new("test-executor".to_string(), 10);

        // Reserve all 10 slots
        let result1 = manager
            .reserve_slots("req-1", "scheduler-1", 10, Duration::from_secs(60))
            .await;
        assert_eq!(result1.slots_reserved, 10);

        // Next request should get 0 slots
        let result2 = manager
            .reserve_slots("req-2", "scheduler-1", 5, Duration::from_secs(60))
            .await;
        assert_eq!(result2.slots_reserved, 0);

        // Release first lease
        manager
            .release_lease(&result1.lease_id, "scheduler-1")
            .await
            .unwrap();

        // Now we can reserve again
        let result3 = manager
            .reserve_slots("req-3", "scheduler-1", 5, Duration::from_secs(60))
            .await;
        assert_eq!(result3.slots_reserved, 5);
    }
}
