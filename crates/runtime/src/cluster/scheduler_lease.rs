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

//! Scheduler-side lease management for task slot reservation.
//!
//! This module provides the [`SchedulerLeaseClient`] which allows the scheduler
//! to reserve task slots on executors before dispatching work.

use runtime_proto::executor_lease_service_client::ExecutorLeaseServiceClient;
use runtime_proto::{
    GetCapacityRequest, ReleaseLeaseRequest, RenewLeaseRequest, ReserveSlotsRequest,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use uuid::Uuid;

/// Default lease duration in seconds.
const DEFAULT_LEASE_TTL_SECS: u64 = 30;

/// Default renewal interval in seconds.
const DEFAULT_RENEWAL_INTERVAL_SECS: u64 = 10;

/// Buffer time before expiry to trigger renewal (in seconds).
const RENEWAL_BUFFER_SECS: u64 = 5;

/// Represents an active lease held by the scheduler on an executor.
#[derive(Debug, Clone)]
pub struct ActiveLease {
    /// Unique lease identifier.
    pub lease_id: String,
    /// ID of the executor holding this lease.
    pub executor_id: String,
    /// Executor's gRPC endpoint URL.
    pub executor_url: String,
    /// Number of reserved slots.
    pub slots: u32,
    /// Absolute expiry time as milliseconds since UNIX epoch.
    pub expires_at_ms: u64,
    /// Local timestamp when the lease was last renewed.
    pub last_renewed: Instant,
}

impl ActiveLease {
    /// Returns true if this lease is expired.
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn is_expired(&self) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now_ms >= self.expires_at_ms
    }

    /// Returns true if this lease should be renewed.
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn should_renew(&self) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let buffer_ms = RENEWAL_BUFFER_SECS * 1000;
        now_ms + buffer_ms >= self.expires_at_ms
    }
}

/// Configuration for the scheduler lease client.
#[derive(Debug, Clone)]
pub struct SchedulerLeaseConfig {
    /// Lease duration to request (in milliseconds).
    pub lease_ttl_ms: u64,
    /// Interval between renewal attempts (in milliseconds).
    pub renewal_interval_ms: u64,
    /// TLS configuration for connecting to executors.
    pub tls_config: Option<ClientTlsConfig>,
}

impl Default for SchedulerLeaseConfig {
    fn default() -> Self {
        Self {
            lease_ttl_ms: DEFAULT_LEASE_TTL_SECS * 1000,
            renewal_interval_ms: DEFAULT_RENEWAL_INTERVAL_SECS * 1000,
            tls_config: None,
        }
    }
}

/// Capacity information for an executor.
#[derive(Debug, Clone)]
pub struct ExecutorCapacity {
    /// Total configured task slots.
    pub total_slots: u32,
    /// Slots available for new leases.
    pub available_slots: u32,
    /// Slots currently leased.
    pub leased_slots: u32,
    /// Slots currently in use.
    pub in_use_slots: u32,
}

/// Internal state for the scheduler lease client.
struct SchedulerLeaseClientInner {
    /// Scheduler ID for identifying ourselves to executors.
    scheduler_id: String,
    /// Active leases indexed by lease ID.
    leases: HashMap<String, ActiveLease>,
    /// Leases indexed by executor ID for quick lookup.
    leases_by_executor: HashMap<String, Vec<String>>,
    /// Configuration.
    config: SchedulerLeaseConfig,
}

/// Scheduler-side client for managing task slot leases on executors.
///
/// The `SchedulerLeaseClient` allows the scheduler to reserve task slots on
/// executors before dispatching work, preventing over-subscription and enabling
/// better load balancing.
pub struct SchedulerLeaseClient {
    inner: Arc<RwLock<SchedulerLeaseClientInner>>,
    /// Token for canceling the background renewal task.
    shutdown_token: CancellationToken,
}

impl SchedulerLeaseClient {
    /// Creates a new `SchedulerLeaseClient` with the given scheduler ID and config.
    #[must_use]
    pub fn new(scheduler_id: String, config: SchedulerLeaseConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SchedulerLeaseClientInner {
                scheduler_id,
                leases: HashMap::new(),
                leases_by_executor: HashMap::new(),
                config,
            })),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Returns the scheduler ID.
    pub async fn scheduler_id(&self) -> String {
        self.inner.read().await.scheduler_id.clone()
    }

    /// Gets the current capacity of an executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn get_capacity(&self, executor_url: &str) -> Result<ExecutorCapacity, String> {
        let client = self.create_client(executor_url).await?;
        self.get_capacity_with_client(client).await
    }

    /// Gets executor capacity using an existing client.
    async fn get_capacity_with_client(
        &self,
        mut client: ExecutorLeaseServiceClient<Channel>,
    ) -> Result<ExecutorCapacity, String> {
        let response = client
            .get_capacity(GetCapacityRequest {})
            .await
            .map_err(|e| format!("Failed to get capacity: {e}"))?;

        let inner = response.into_inner();
        Ok(ExecutorCapacity {
            total_slots: inner.total_slots,
            available_slots: inner.available_slots,
            leased_slots: inner.leased_slots,
            in_use_slots: inner.in_use_slots,
        })
    }

    /// Reserves task slots on an executor.
    ///
    /// This method first queries the executor's capacity, then reserves slots
    /// if available. The reservation is tracked as an active lease.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails or no slots are available.
    pub async fn reserve_slots(
        &self,
        executor_id: &str,
        executor_url: &str,
        slots: u32,
    ) -> Result<ActiveLease, String> {
        let client = self.create_client(executor_url).await?;

        // First check capacity
        let capacity = self.get_capacity_with_client(client.clone()).await?;
        if capacity.available_slots == 0 {
            return Err(format!(
                "No slots available on executor {executor_id} (total: {}, leased: {})",
                capacity.total_slots, capacity.leased_slots
            ));
        }

        // Reserve slots
        let inner = self.inner.read().await;
        let request_id = Uuid::new_v4().to_string();
        let scheduler_id = inner.scheduler_id.clone();
        let lease_ttl_ms = inner.config.lease_ttl_ms;
        drop(inner);

        let request = ReserveSlotsRequest {
            request_id,
            scheduler_id: scheduler_id.clone(),
            slots: slots.min(capacity.available_slots),
            lease_duration_ms: lease_ttl_ms,
        };

        let response = client
            .clone()
            .reserve_slots(request)
            .await
            .map_err(|e| format!("Failed to reserve slots: {e}"))?;

        let response = response.into_inner();

        if response.slots_reserved == 0 {
            return Err(format!(
                "No slots could be reserved on executor {executor_id}"
            ));
        }

        let lease = ActiveLease {
            lease_id: response.lease_id.clone(),
            executor_id: executor_id.to_string(),
            executor_url: executor_url.to_string(),
            slots: response.slots_reserved,
            expires_at_ms: response.expires_at_ms,
            last_renewed: Instant::now(),
        };

        // Track the lease
        let mut inner = self.inner.write().await;
        inner
            .leases
            .insert(response.lease_id.clone(), lease.clone());
        inner
            .leases_by_executor
            .entry(executor_id.to_string())
            .or_default()
            .push(response.lease_id);

        tracing::info!(
            lease_id = %lease.lease_id,
            executor_id = %executor_id,
            slots_reserved = lease.slots,
            expires_at_ms = lease.expires_at_ms,
            "Reserved slots on executor"
        );

        Ok(lease)
    }

    /// Renews an existing lease.
    ///
    /// # Errors
    ///
    /// Returns an error if the lease doesn't exist or the gRPC call fails.
    pub async fn renew_lease(&self, lease_id: &str) -> Result<u64, String> {
        let inner = self.inner.read().await;
        let lease = inner
            .leases
            .get(lease_id)
            .ok_or_else(|| format!("Lease not found: {lease_id}"))?
            .clone();
        let scheduler_id = inner.scheduler_id.clone();
        let lease_ttl_ms = inner.config.lease_ttl_ms;
        drop(inner);

        let client = self.create_client(&lease.executor_url).await?;

        let request = RenewLeaseRequest {
            lease_id: lease_id.to_string(),
            scheduler_id,
            lease_duration_ms: lease_ttl_ms,
        };

        let response = client
            .clone()
            .renew_lease(request)
            .await
            .map_err(|e| format!("Failed to renew lease: {e}"))?;

        let new_expires_at_ms = response.into_inner().expires_at_ms;

        // Update the lease
        let mut inner = self.inner.write().await;
        if let Some(lease) = inner.leases.get_mut(lease_id) {
            lease.expires_at_ms = new_expires_at_ms;
            lease.last_renewed = Instant::now();
        }

        tracing::debug!(
            lease_id = %lease_id,
            new_expires_at_ms = new_expires_at_ms,
            "Renewed lease"
        );

        Ok(new_expires_at_ms)
    }

    /// Releases a lease, returning its slots to the executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn release_lease(&self, lease_id: &str) -> Result<u32, String> {
        let inner = self.inner.read().await;
        let lease = inner.leases.get(lease_id).cloned();
        let scheduler_id = inner.scheduler_id.clone();
        drop(inner);

        let Some(lease) = lease else {
            // Already released
            return Ok(0);
        };

        let client = self.create_client(&lease.executor_url).await?;

        let request = ReleaseLeaseRequest {
            lease_id: lease_id.to_string(),
            scheduler_id,
        };

        let response = client
            .clone()
            .release_lease(request)
            .await
            .map_err(|e| format!("Failed to release lease: {e}"))?;

        let slots_released = response.into_inner().slots_released;

        // Remove from tracking
        let mut inner = self.inner.write().await;
        inner.leases.remove(lease_id);
        if let Some(executor_leases) = inner.leases_by_executor.get_mut(&lease.executor_id) {
            executor_leases.retain(|id| id != lease_id);
            if executor_leases.is_empty() {
                inner.leases_by_executor.remove(&lease.executor_id);
            }
        }

        tracing::info!(
            lease_id = %lease_id,
            executor_id = %lease.executor_id,
            slots_released = slots_released,
            "Released lease"
        );

        Ok(slots_released)
    }

    /// Returns all active leases.
    pub async fn active_leases(&self) -> Vec<ActiveLease> {
        self.inner.read().await.leases.values().cloned().collect()
    }

    /// Returns active leases for a specific executor.
    pub async fn leases_for_executor(&self, executor_id: &str) -> Vec<ActiveLease> {
        let inner = self.inner.read().await;
        inner
            .leases_by_executor
            .get(executor_id)
            .map(|lease_ids| {
                lease_ids
                    .iter()
                    .filter_map(|id| inner.leases.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Gets a specific lease by ID.
    pub async fn get_lease(&self, lease_id: &str) -> Option<ActiveLease> {
        self.inner.read().await.leases.get(lease_id).cloned()
    }

    /// Starts the background lease renewal loop.
    ///
    /// This loop periodically checks for leases that need renewal and renews them.
    #[must_use]
    pub fn start_renewal_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let client = Arc::clone(&self);
        let shutdown = self.shutdown_token.clone();

        tokio::spawn(async move {
            let renewal_interval = {
                let inner = client.inner.read().await;
                Duration::from_millis(inner.config.renewal_interval_ms)
            };

            let mut interval = tokio::time::interval(renewal_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    () = shutdown.cancelled() => {
                        tracing::debug!("SchedulerLeaseClient renewal loop shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        client.renew_expiring_leases().await;
                    }
                }
            }
        })
    }

    /// Renews all leases that are close to expiring.
    async fn renew_expiring_leases(&self) {
        let leases_to_renew: Vec<ActiveLease> = {
            let inner = self.inner.read().await;
            inner
                .leases
                .values()
                .filter(|lease| lease.should_renew() && !lease.is_expired())
                .cloned()
                .collect()
        };

        for lease in leases_to_renew {
            if let Err(e) = self.renew_lease(&lease.lease_id).await {
                tracing::warn!(
                    lease_id = %lease.lease_id,
                    executor_id = %lease.executor_id,
                    error = %e,
                    "Failed to renew lease"
                );

                // If renewal fails and lease is expired, remove it
                if lease.is_expired() {
                    let mut inner = self.inner.write().await;
                    inner.leases.remove(&lease.lease_id);
                    if let Some(executor_leases) =
                        inner.leases_by_executor.get_mut(&lease.executor_id)
                    {
                        executor_leases.retain(|id| id != &lease.lease_id);
                        if executor_leases.is_empty() {
                            inner.leases_by_executor.remove(&lease.executor_id);
                        }
                    }
                    tracing::warn!(
                        lease_id = %lease.lease_id,
                        executor_id = %lease.executor_id,
                        "Removed expired lease after failed renewal"
                    );
                }
            }
        }
    }

    /// Creates a gRPC client for the given executor URL.
    async fn create_client(
        &self,
        executor_url: &str,
    ) -> Result<ExecutorLeaseServiceClient<Channel>, String> {
        let inner = self.inner.read().await;
        let tls_config = inner.config.tls_config.clone();
        drop(inner);

        let mut endpoint = Endpoint::from_shared(executor_url.to_string())
            .map_err(|e| format!("Invalid executor URL: {e}"))?;

        if let Some(tls_config) = tls_config {
            endpoint = endpoint
                .tls_config(tls_config)
                .map_err(|e| format!("Failed to configure TLS: {e}"))?;
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to executor at {executor_url}: {e}"))?;

        Ok(ExecutorLeaseServiceClient::new(channel))
    }

    /// Shuts down the lease client, canceling the renewal loop.
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }
}

impl Drop for SchedulerLeaseClient {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_active_lease_should_renew() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Lease expiring soon should be renewed
        let lease = ActiveLease {
            lease_id: "test-lease".to_string(),
            executor_id: "executor-1".to_string(),
            executor_url: "http://localhost:50051".to_string(),
            slots: 5,
            expires_at_ms: now_ms + 3000, // expires in 3 seconds
            last_renewed: Instant::now(),
        };
        assert!(lease.should_renew()); // within buffer of 5 seconds

        // Lease with plenty of time should not be renewed
        let lease2 = ActiveLease {
            lease_id: "test-lease-2".to_string(),
            executor_id: "executor-1".to_string(),
            executor_url: "http://localhost:50051".to_string(),
            slots: 5,
            expires_at_ms: now_ms + 30000, // expires in 30 seconds
            last_renewed: Instant::now(),
        };
        assert!(!lease2.should_renew());
    }

    #[test]
    fn test_active_lease_is_expired() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Lease that's already expired
        let expired_lease = ActiveLease {
            lease_id: "expired-lease".to_string(),
            executor_id: "executor-1".to_string(),
            executor_url: "http://localhost:50051".to_string(),
            slots: 5,
            expires_at_ms: now_ms - 1000, // expired 1 second ago
            last_renewed: Instant::now(),
        };
        assert!(expired_lease.is_expired());

        // Lease that's not expired
        let valid_lease = ActiveLease {
            lease_id: "valid-lease".to_string(),
            executor_id: "executor-1".to_string(),
            executor_url: "http://localhost:50051".to_string(),
            slots: 5,
            expires_at_ms: now_ms + 30000, // expires in 30 seconds
            last_renewed: Instant::now(),
        };
        assert!(!valid_lease.is_expired());
    }
}
