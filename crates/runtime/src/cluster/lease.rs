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

//! Lease-based task slot management for executors.
//!
//! This module implements executor-owned lease management, allowing schedulers
//! to reserve task slots before dispatching tasks. Key properties:
//!
//! - **Executor-owned state**: The executor is the source of truth for capacity
//! - **Lease TTL with automatic expiration**: Leases expire if not renewed
//! - **Idempotent reservation**: Same `request_id` returns same lease
//! - **Enforcement at task execution**: Tasks check lease validity before running

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// TTL for `request_id` idempotency cache entries (60 seconds).
const REQUEST_ID_CACHE_TTL: Duration = Duration::from_secs(60);

/// Default lease TTL if not specified (30 seconds).
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// Maximum allowed lease TTL (5 minutes).
const MAX_LEASE_TTL: Duration = Duration::from_secs(300);

/// Minimum allowed lease TTL (5 seconds).
const MIN_LEASE_TTL: Duration = Duration::from_secs(5);

/// Interval for the background expiration cleanup loop.
const EXPIRATION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Unique identifier for a lease.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseId(String);

impl LeaseId {
    /// Creates a new random lease ID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Returns the lease ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for LeaseId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<String> for LeaseId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl std::fmt::Display for LeaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A lease representing reserved task slots on an executor.
#[derive(Debug, Clone)]
pub struct Lease {
    /// Unique identifier for this lease.
    pub id: LeaseId,
    /// Scheduler that owns this lease.
    pub scheduler_id: String,
    /// Number of slots reserved by this lease.
    pub slots: u32,
    /// When this lease expires.
    pub expires_at: Instant,
    /// Number of slots currently in use from this lease.
    pub slots_in_use: u32,
}

impl Lease {
    /// Creates a new lease.
    fn new(scheduler_id: String, slots: u32, ttl: Duration) -> Self {
        Self {
            id: LeaseId::new(),
            scheduler_id,
            slots,
            expires_at: Instant::now() + ttl,
            slots_in_use: 0,
        }
    }

    /// Returns true if this lease has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Returns the remaining TTL for this lease.
    #[must_use]
    pub fn remaining_ttl(&self) -> Duration {
        self.expires_at.saturating_duration_since(Instant::now())
    }

    /// Returns the number of unused slots in this lease.
    #[must_use]
    pub fn available_slots(&self) -> u32 {
        self.slots.saturating_sub(self.slots_in_use)
    }
}

/// Cached result from a previous reservation request (for idempotency).
#[derive(Debug, Clone)]
struct IdempotencyEntry {
    /// The lease ID that was created for this request.
    lease_id: LeaseId,
    /// When this cache entry expires.
    expires_at: Instant,
}

/// Result of a slot reservation attempt.
#[derive(Debug)]
pub enum ReservationResult {
    /// Reservation was granted with the given lease.
    Granted {
        lease_id: LeaseId,
        slots_granted: u32,
        ttl: Duration,
    },
    /// Reservation was denied.
    Denied { reason: String },
    /// Request was a duplicate (idempotent replay).
    Duplicate {
        lease_id: LeaseId,
        slots_granted: u32,
        ttl: Duration,
    },
}

/// Result of a lease renewal attempt.
#[derive(Debug)]
pub enum RenewalResult {
    /// Renewal was successful.
    Renewed { new_ttl: Duration },
    /// Renewal failed.
    Failed { reason: String },
}

/// Result of a lease release attempt.
#[derive(Debug)]
pub enum ReleaseResult {
    /// Release was successful.
    Released { slots_released: u32 },
    /// Release failed.
    Failed { reason: String },
}

/// Result of trying to use a slot from a lease.
#[derive(Debug)]
pub enum UseSlotResult {
    /// Slot was successfully acquired.
    Acquired,
    /// Could not acquire slot.
    Failed { reason: String },
}

/// Callback for handling lease expiration events.
pub trait LeaseExpirationHandler: Send + Sync {
    /// Called when a lease expires. This can be used to cancel tasks
    /// associated with the lease.
    fn on_lease_expired(&self, lease: &Lease);
}

/// A no-op expiration handler for testing or when cancellation isn't needed.
pub struct NoOpExpirationHandler;

impl LeaseExpirationHandler for NoOpExpirationHandler {
    fn on_lease_expired(&self, _lease: &Lease) {
        // No-op
    }
}

/// Internal state for the lease manager.
struct LeaseManagerState {
    /// Total number of task slots on this executor.
    total_slots: u32,
    /// Active leases indexed by lease ID.
    leases: HashMap<LeaseId, Lease>,
    /// Idempotency cache: `request_id` -> lease info.
    idempotency_cache: HashMap<String, IdempotencyEntry>,
    /// Number of slots currently in use (executing tasks).
    slots_in_use: u32,
}

impl LeaseManagerState {
    fn new(total_slots: u32) -> Self {
        Self {
            total_slots,
            leases: HashMap::new(),
            idempotency_cache: HashMap::new(),
            slots_in_use: 0,
        }
    }

    /// Returns the number of slots reserved by active (non-expired) leases.
    fn reserved_slots(&self) -> u32 {
        self.leases
            .values()
            .filter(|l| !l.is_expired())
            .map(|l| l.slots)
            .sum()
    }

    /// Returns the number of available (unreserved and not in use) slots.
    fn available_slots(&self) -> u32 {
        self.total_slots
            .saturating_sub(self.reserved_slots())
            .saturating_sub(self.slots_in_use)
    }

    /// Cleans up expired leases and idempotency cache entries.
    /// Returns the expired leases for notification.
    fn cleanup_expired(&mut self) -> Vec<Lease> {
        let now = Instant::now();

        // Collect expired leases
        let expired_lease_ids: Vec<LeaseId> = self
            .leases
            .iter()
            .filter(|(_, l)| l.is_expired())
            .map(|(id, _)| id.clone())
            .collect();

        let mut expired_leases = Vec::with_capacity(expired_lease_ids.len());
        for id in expired_lease_ids {
            if let Some(lease) = self.leases.remove(&id) {
                expired_leases.push(lease);
            }
        }

        // Clean up expired idempotency cache entries
        self.idempotency_cache
            .retain(|_, entry| entry.expires_at > now);

        expired_leases
    }
}

/// Manages task slot leases for an executor.
///
/// The `LeaseManager` is the executor's source of truth for capacity management.
/// Schedulers request leases to reserve slots before dispatching tasks.
pub struct LeaseManager {
    state: Arc<Mutex<LeaseManagerState>>,
    expiration_handler: Arc<dyn LeaseExpirationHandler>,
    shutdown_token: CancellationToken,
}

impl LeaseManager {
    /// Creates a new `LeaseManager` with the given total slots and expiration handler.
    ///
    /// This also spawns a background task for cleaning up expired leases.
    #[must_use]
    pub fn new(total_slots: u32, expiration_handler: Arc<dyn LeaseExpirationHandler>) -> Arc<Self> {
        let state = Arc::new(Mutex::new(LeaseManagerState::new(total_slots)));
        let shutdown_token = CancellationToken::new();

        let manager = Arc::new(Self {
            state,
            expiration_handler,
            shutdown_token,
        });

        // Spawn background expiration loop
        let manager_clone = Arc::clone(&manager);
        tokio::spawn(async move {
            manager_clone.expiration_loop().await;
        });

        manager
    }

    /// Creates a new `LeaseManager` without an expiration handler.
    #[must_use]
    pub fn new_without_handler(total_slots: u32) -> Arc<Self> {
        Self::new(total_slots, Arc::new(NoOpExpirationHandler))
    }

    /// Shuts down the lease manager, stopping the background expiration loop.
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    /// Background loop that periodically cleans up expired leases.
    async fn expiration_loop(&self) {
        let mut interval = tokio::time::interval(EXPIRATION_CHECK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = self.shutdown_token.cancelled() => {
                    tracing::debug!("LeaseManager expiration loop shutting down");
                    break;
                }
                _ = interval.tick() => {
                    let expired_leases = {
                        let mut state = self.state.lock().await;
                        state.cleanup_expired()
                    };

                    // Notify handler of expired leases (outside the lock)
                    for lease in expired_leases {
                        tracing::info!(
                            lease_id = %lease.id,
                            scheduler_id = %lease.scheduler_id,
                            slots = lease.slots,
                            "Lease expired"
                        );
                        self.expiration_handler.on_lease_expired(&lease);
                    }
                }
            }
        }
    }

    /// Reserves task slots for a scheduler.
    ///
    /// This operation is idempotent: if the same `request_id` is seen within
    /// the cache TTL, the same lease is returned.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "TTL is clamped to max 5 minutes, fits in u64"
    )]
    pub async fn reserve_slots(
        &self,
        request_id: &str,
        scheduler_id: &str,
        slots: u32,
        ttl_ms: u64,
    ) -> ReservationResult {
        let mut state = self.state.lock().await;

        // Check idempotency cache first
        if let Some(entry) = state.idempotency_cache.get(request_id)
            && entry.expires_at > Instant::now()
        {
            // Return cached result
            if let Some(lease) = state.leases.get(&entry.lease_id) {
                return ReservationResult::Duplicate {
                    lease_id: lease.id.clone(),
                    slots_granted: lease.slots,
                    ttl: lease.remaining_ttl(),
                };
            }
        }

        // Validate and clamp TTL
        let ttl = if ttl_ms == 0 {
            DEFAULT_LEASE_TTL
        } else {
            Duration::from_millis(ttl_ms).clamp(MIN_LEASE_TTL, MAX_LEASE_TTL)
        };

        // Check available capacity
        let available = state.available_slots();
        if slots == 0 {
            return ReservationResult::Denied {
                reason: "Cannot reserve zero slots".to_string(),
            };
        }

        if available == 0 {
            return ReservationResult::Denied {
                reason: format!(
                    "No slots available (total: {}, reserved: {}, in_use: {})",
                    state.total_slots,
                    state.reserved_slots(),
                    state.slots_in_use
                ),
            };
        }

        // Grant as many slots as possible up to the request
        let slots_granted = slots.min(available);
        let lease = Lease::new(scheduler_id.to_string(), slots_granted, ttl);
        let lease_id = lease.id.clone();

        tracing::info!(
            lease_id = %lease_id,
            scheduler_id = %scheduler_id,
            request_id = %request_id,
            slots_requested = slots,
            slots_granted = slots_granted,
            ttl_ms = ttl.as_millis() as u64,
            "Lease granted"
        );

        // Store lease
        state.leases.insert(lease_id.clone(), lease);

        // Store in idempotency cache
        state.idempotency_cache.insert(
            request_id.to_string(),
            IdempotencyEntry {
                lease_id: lease_id.clone(),
                expires_at: Instant::now() + REQUEST_ID_CACHE_TTL,
            },
        );

        ReservationResult::Granted {
            lease_id,
            slots_granted,
            ttl,
        }
    }

    /// Renews an existing lease to extend its TTL.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "TTL is clamped to max 5 minutes, fits in u64"
    )]
    pub async fn renew_lease(
        &self,
        lease_id: &str,
        scheduler_id: &str,
        ttl_ms: u64,
    ) -> RenewalResult {
        let mut state = self.state.lock().await;

        let lease_id = LeaseId::from(lease_id.to_string());
        let Some(lease) = state.leases.get_mut(&lease_id) else {
            return RenewalResult::Failed {
                reason: "Lease not found".to_string(),
            };
        };

        // Verify ownership
        if lease.scheduler_id != scheduler_id {
            return RenewalResult::Failed {
                reason: "Scheduler ID mismatch".to_string(),
            };
        }

        // Check if already expired
        if lease.is_expired() {
            return RenewalResult::Failed {
                reason: "Lease has expired".to_string(),
            };
        }

        // Validate and clamp TTL
        let ttl = if ttl_ms == 0 {
            DEFAULT_LEASE_TTL
        } else {
            Duration::from_millis(ttl_ms).clamp(MIN_LEASE_TTL, MAX_LEASE_TTL)
        };

        // Extend the lease
        lease.expires_at = Instant::now() + ttl;

        tracing::debug!(
            lease_id = %lease.id,
            scheduler_id = %scheduler_id,
            new_ttl_ms = ttl.as_millis() as u64,
            "Lease renewed"
        );

        RenewalResult::Renewed { new_ttl: ttl }
    }

    /// Releases a lease, returning reserved slots to the pool.
    pub async fn release_lease(&self, lease_id: &str, scheduler_id: &str) -> ReleaseResult {
        let mut state = self.state.lock().await;

        let lease_id = LeaseId::from(lease_id.to_string());
        let Some(lease) = state.leases.get(&lease_id) else {
            return ReleaseResult::Failed {
                reason: "Lease not found".to_string(),
            };
        };

        // Verify ownership
        if lease.scheduler_id != scheduler_id {
            return ReleaseResult::Failed {
                reason: "Scheduler ID mismatch".to_string(),
            };
        }

        let slots_released = lease.slots;

        // Check for slots still in use
        if lease.slots_in_use > 0 {
            tracing::warn!(
                lease_id = %lease.id,
                slots_in_use = lease.slots_in_use,
                "Releasing lease with slots still in use"
            );
        }

        state.leases.remove(&lease_id);

        tracing::info!(
            lease_id = %lease_id,
            scheduler_id = %scheduler_id,
            slots_released = slots_released,
            "Lease released"
        );

        ReleaseResult::Released { slots_released }
    }

    /// Attempts to use a slot from a lease for task execution.
    ///
    /// This is called when a task is about to start execution. It verifies
    /// the lease is valid and marks a slot as in use.
    pub async fn try_use_slot(&self, lease_id: &str) -> UseSlotResult {
        let mut state = self.state.lock().await;

        let lease_id_key = LeaseId::from(lease_id.to_string());

        // Get the lease mutably and check in one step
        let Some(lease) = state.leases.get_mut(&lease_id_key) else {
            return UseSlotResult::Failed {
                reason: "Lease not found".to_string(),
            };
        };

        if lease.is_expired() {
            return UseSlotResult::Failed {
                reason: "Lease has expired".to_string(),
            };
        }

        if lease.available_slots() == 0 {
            return UseSlotResult::Failed {
                reason: "No available slots in lease".to_string(),
            };
        }

        // Update the lease
        lease.slots_in_use += 1;
        let slots_after = lease.slots_in_use;
        state.slots_in_use += 1;

        tracing::trace!(
            lease_id = %lease_id,
            slots_in_use = slots_after,
            "Slot acquired from lease"
        );

        UseSlotResult::Acquired
    }

    /// Returns a slot to a lease after task completion.
    ///
    /// This is called when a task finishes (successfully or not).
    pub async fn return_slot(&self, lease_id: &str) {
        let mut state = self.state.lock().await;

        let lease_id_key = LeaseId::from(lease_id.to_string());

        // Try to get the lease and return the slot
        if let Some(lease) = state.leases.get_mut(&lease_id_key) {
            if lease.slots_in_use > 0 {
                lease.slots_in_use -= 1;
                let slots_after = lease.slots_in_use;
                state.slots_in_use = state.slots_in_use.saturating_sub(1);

                tracing::trace!(
                    lease_id = %lease_id,
                    slots_in_use = slots_after,
                    "Slot returned to lease"
                );
            }
            // else: Lease exists but no slots in use - nothing to do
        } else {
            // Lease was already removed (expired or released), just decrement global count
            state.slots_in_use = state.slots_in_use.saturating_sub(1);
            tracing::trace!(
                lease_id = %lease_id,
                "Slot returned (lease already removed)"
            );
        }
    }

    /// Returns the current capacity information.
    pub async fn get_capacity(&self) -> CapacityInfo {
        let state = self.state.lock().await;
        CapacityInfo {
            total_slots: state.total_slots,
            available_slots: state.available_slots(),
            reserved_slots: state.reserved_slots(),
            in_use_slots: state.slots_in_use,
        }
    }

    /// Updates the total number of slots (e.g., after configuration change).
    pub async fn set_total_slots(&self, total_slots: u32) {
        let mut state = self.state.lock().await;
        state.total_slots = total_slots;
        tracing::info!(
            total_slots = total_slots,
            "LeaseManager total slots updated"
        );
    }
}

/// Capacity information returned by `get_capacity`.
#[derive(Debug, Clone)]
pub struct CapacityInfo {
    /// Total task slots on this executor.
    pub total_slots: u32,
    /// Currently available (unreserved and not in use) slots.
    pub available_slots: u32,
    /// Slots reserved by active leases.
    pub reserved_slots: u32,
    /// Slots currently in use (executing tasks).
    pub in_use_slots: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reserve_and_release() {
        let manager = LeaseManager::new_without_handler(4);

        // Reserve 2 slots
        let result = manager
            .reserve_slots("req-1", "scheduler-1", 2, 30_000)
            .await;
        let lease_id = match result {
            ReservationResult::Granted {
                lease_id,
                slots_granted,
                ..
            } => {
                assert_eq!(slots_granted, 2);
                lease_id
            }
            _ => panic!("Expected Granted"),
        };

        // Check capacity
        let capacity = manager.get_capacity().await;
        assert_eq!(capacity.total_slots, 4);
        assert_eq!(capacity.reserved_slots, 2);
        assert_eq!(capacity.available_slots, 2);

        // Release the lease
        let result = manager
            .release_lease(lease_id.as_str(), "scheduler-1")
            .await;
        match result {
            ReleaseResult::Released { slots_released } => {
                assert_eq!(slots_released, 2);
            }
            _ => panic!("Expected Released"),
        }

        // Check capacity after release
        let capacity = manager.get_capacity().await;
        assert_eq!(capacity.reserved_slots, 0);
        assert_eq!(capacity.available_slots, 4);

        manager.shutdown();
    }

    #[tokio::test]
    async fn test_idempotency() {
        let manager = LeaseManager::new_without_handler(4);

        // First request
        let result1 = manager
            .reserve_slots("req-1", "scheduler-1", 2, 30_000)
            .await;
        let lease_id1 = match result1 {
            ReservationResult::Granted { lease_id, .. } => lease_id,
            _ => panic!("Expected Granted"),
        };

        // Same request ID should return duplicate
        let result2 = manager
            .reserve_slots("req-1", "scheduler-1", 2, 30_000)
            .await;
        let lease_id2 = match result2 {
            ReservationResult::Duplicate { lease_id, .. } => lease_id,
            _ => panic!("Expected Duplicate"),
        };

        assert_eq!(lease_id1, lease_id2);

        // Only 2 slots should be reserved (not 4)
        let capacity = manager.get_capacity().await;
        assert_eq!(capacity.reserved_slots, 2);

        manager.shutdown();
    }

    #[tokio::test]
    async fn test_partial_grant() {
        let manager = LeaseManager::new_without_handler(2);

        // Request more slots than available
        let result = manager
            .reserve_slots("req-1", "scheduler-1", 5, 30_000)
            .await;
        match result {
            ReservationResult::Granted { slots_granted, .. } => {
                assert_eq!(slots_granted, 2); // Only 2 available
            }
            _ => panic!("Expected Granted with partial allocation"),
        }

        manager.shutdown();
    }

    #[tokio::test]
    async fn test_use_and_return_slots() {
        let manager = LeaseManager::new_without_handler(4);

        // Reserve 2 slots
        let result = manager
            .reserve_slots("req-1", "scheduler-1", 2, 30_000)
            .await;
        let lease_id = match result {
            ReservationResult::Granted { lease_id, .. } => lease_id,
            _ => panic!("Expected Granted"),
        };

        // Use a slot
        let use_result = manager.try_use_slot(lease_id.as_str()).await;
        assert!(matches!(use_result, UseSlotResult::Acquired));

        // Check capacity
        let capacity = manager.get_capacity().await;
        assert_eq!(capacity.in_use_slots, 1);

        // Return the slot
        manager.return_slot(lease_id.as_str()).await;

        // Check capacity after return
        let capacity = manager.get_capacity().await;
        assert_eq!(capacity.in_use_slots, 0);

        manager.shutdown();
    }

    #[tokio::test]
    async fn test_renew_lease() {
        let manager = LeaseManager::new_without_handler(4);

        // Reserve slots
        let result = manager
            .reserve_slots("req-1", "scheduler-1", 2, 5_000) // 5 second TTL
            .await;
        let lease_id = match result {
            ReservationResult::Granted { lease_id, .. } => lease_id,
            _ => panic!("Expected Granted"),
        };

        // Renew with longer TTL
        let renew_result = manager
            .renew_lease(lease_id.as_str(), "scheduler-1", 30_000)
            .await;
        match renew_result {
            RenewalResult::Renewed { new_ttl } => {
                assert!(new_ttl >= Duration::from_secs(29)); // Should be close to 30s
            }
            _ => panic!("Expected Renewed"),
        }

        manager.shutdown();
    }

    #[tokio::test]
    async fn test_ownership_verification() {
        let manager = LeaseManager::new_without_handler(4);

        // Reserve slots as scheduler-1
        let result = manager
            .reserve_slots("req-1", "scheduler-1", 2, 30_000)
            .await;
        let lease_id = match result {
            ReservationResult::Granted { lease_id, .. } => lease_id,
            _ => panic!("Expected Granted"),
        };

        // Try to release as different scheduler
        let release_result = manager
            .release_lease(lease_id.as_str(), "scheduler-2")
            .await;
        assert!(matches!(release_result, ReleaseResult::Failed { .. }));

        // Try to renew as different scheduler
        let renew_result = manager
            .renew_lease(lease_id.as_str(), "scheduler-2", 30_000)
            .await;
        assert!(matches!(renew_result, RenewalResult::Failed { .. }));

        manager.shutdown();
    }
}
