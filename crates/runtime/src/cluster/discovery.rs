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

//! Executor discovery for scheduler-driven cluster mode.
//!
//! This module implements DNS SRV-based executor discovery, allowing schedulers to
//! automatically discover and register executors from Kubernetes headless services.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use ns_lookup::{SrvRecord, lookup_srv};
use runtime_proto::DescribeExecutorRequest;
use runtime_proto::executor_service_client::ExecutorServiceClient;
use tokio_util::sync::CancellationToken;
use tonic::transport::{ClientTlsConfig, Endpoint};
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

use crate::Runtime;

/// Maximum interval between executor discovery attempts (steady state).
const MAX_DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// Timeout for connecting to an executor during discovery.
const EXECUTOR_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts the executor discovery loop for scheduler mode.
///
/// This function periodically queries the DNS SRV record specified by `--executor-discovery-dns`
/// to discover executors. For each discovered executor, it calls the `DescribeExecutor` RPC
/// to get registration information and registers the executor with the scheduler.
///
/// Discovery runs immediately on startup, then uses Fibonacci backoff starting at 1 second
/// and gradually increasing to 30 seconds for steady-state polling.
///
/// # Arguments
///
/// * `rt` - The runtime instance containing cluster configuration
/// * `shutdown` - Cancellation token to stop the discovery loop
///
/// # Errors
///
/// Returns an error if the discovery loop encounters an unrecoverable error.
pub async fn start_executor_discovery_loop(
    rt: Arc<Runtime>,
    shutdown: CancellationToken,
) -> crate::Result<()> {
    let Some(executor_discovery_dns) = rt.df.cluster_config.executor_discovery_dns() else {
        tracing::debug!("No --executor-discovery-dns configured, skipping executor discovery");
        return Ok(());
    };

    tracing::info!(
        "Starting executor discovery loop for SRV record: {}",
        executor_discovery_dns
    );

    let executor_discovery_dns = executor_discovery_dns.to_string();
    let client_tls_config = rt.df.cluster_config.client_tls_config().cloned();
    let tls_enabled = rt.df.cluster_config.tls_enabled();

    // Track known executors to avoid re-registering
    let mut known_executors: HashSet<String> = HashSet::new();

    // Use Fibonacci backoff: starts at 1s, increases to max 30s
    let mut backoff = FibonacciBackoffBuilder::new()
        .max_duration(Some(MAX_DISCOVERY_INTERVAL))
        .build();

    // Discover immediately on startup, then enter the backoff loop
    loop {
        // Run discovery and check if executor state changed (new registrations or removals)
        let executor_changes = match discover_and_register_executors(
            &rt,
            &executor_discovery_dns,
            client_tls_config.as_ref(),
            tls_enabled,
            &mut known_executors,
        )
        .await
        {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!("Executor discovery error: {}", e);
                0
            }
        };

        // Reset backoff if executor state changed (more changes likely coming)
        if executor_changes > 0 {
            backoff.reset();
        }

        // Get next backoff interval (will always return Some since max_retries is None)
        let interval = backoff.next_backoff().unwrap_or(MAX_DISCOVERY_INTERVAL);

        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("Executor discovery loop shutting down");
                return Ok(());
            }
            () = tokio::time::sleep(interval) => {
                // Continue to next discovery iteration
            }
        }
    }
}

/// Discovers executors via DNS SRV and registers them with the scheduler.
///
/// Returns the number of executor changes (new registrations + removals).
/// Also removes executors that are no longer present in DNS SRV records.
async fn discover_and_register_executors(
    rt: &Runtime,
    srv_name: &str,
    client_tls_config: Option<&ClientTlsConfig>,
    tls_enabled: bool,
    known_executors: &mut HashSet<String>,
) -> crate::Result<usize> {
    tracing::debug!("Performing SRV lookup for: {}", srv_name);

    let srv_records = match lookup_srv(srv_name).await {
        Ok(records) => records,
        Err(e) => {
            tracing::debug!("SRV lookup failed for {}: {}", srv_name, e);
            return Ok(0);
        }
    };

    // Get scheduler server to register/unregister executors
    let scheduler_server = rt
        .df
        .scheduler_server
        .read()
        .ok()
        .and_then(|guard| guard.clone());

    let Some(scheduler) = scheduler_server else {
        tracing::warn!("Scheduler server not available for executor registration");
        return Ok(0);
    };

    // Handle empty SRV records: remove all known executors
    if srv_records.is_empty() {
        tracing::debug!("No SRV records found for {}", srv_name);

        let removed_count = known_executors.len();
        for executor_id in known_executors.drain() {
            tracing::info!("Removing stale executor (no SRV records): {}", executor_id);
            if let Err(e) = scheduler
                .state
                .executor_manager
                .remove_executor(
                    &executor_id,
                    Some("Removed from DNS SRV records".to_string()),
                )
                .await
            {
                tracing::warn!("Failed to remove executor {}: {}", executor_id, e);
            }
        }
        return Ok(removed_count);
    }

    tracing::debug!("Found {} SRV records for {}", srv_records.len(), srv_name);

    // Collect futures for all executor discovery attempts
    let futures: Vec<_> = srv_records
        .into_iter()
        .map(|srv| discover_single_executor(srv, client_tls_config.cloned(), tls_enabled))
        .collect();

    // Execute all discovery attempts concurrently
    let results = futures::future::join_all(futures).await;

    // Track executors discovered in this round
    let mut discovered_executors: HashSet<String> = HashSet::new();

    // Register discovered executors
    let mut change_count = 0;
    for result in results {
        match result {
            Ok(Some((metadata, data))) => {
                let executor_id = metadata.id.clone();
                discovered_executors.insert(executor_id.clone());

                // Skip if already known
                if known_executors.contains(&executor_id) {
                    tracing::trace!("Executor {} already registered, skipping", executor_id);
                    continue;
                }

                tracing::info!(
                    "Discovered new executor: {} at {}:{} with {} task slots",
                    executor_id,
                    metadata.host,
                    metadata.port,
                    data.total_task_slots
                );

                // Register with the scheduler's executor manager
                if let Err(e) = scheduler
                    .state
                    .executor_manager
                    .register_executor(metadata, data)
                    .await
                {
                    tracing::warn!("Failed to register executor {}: {}", executor_id, e);
                } else {
                    known_executors.insert(executor_id);
                    change_count += 1;
                }
            }
            Ok(None) => {
                // Executor not ready, skip
            }
            Err(e) => {
                tracing::debug!("Failed to discover executor: {}", e);
            }
        }
    }

    // Remove executors that are no longer in DNS SRV records
    let stale_executors: Vec<String> = known_executors
        .difference(&discovered_executors)
        .cloned()
        .collect();

    for executor_id in stale_executors {
        tracing::info!(
            "Removing stale executor (no longer in DNS SRV): {}",
            executor_id
        );
        if let Err(e) = scheduler
            .state
            .executor_manager
            .remove_executor(
                &executor_id,
                Some("Removed from DNS SRV records".to_string()),
            )
            .await
        {
            tracing::warn!("Failed to remove stale executor {}: {}", executor_id, e);
        } else {
            known_executors.remove(&executor_id);
            change_count += 1;
        }
    }

    Ok(change_count)
}

/// Discovers a single executor by connecting to it and calling `DescribeExecutor`.
async fn discover_single_executor(
    srv: SrvRecord,
    client_tls_config: Option<ClientTlsConfig>,
    tls_enabled: bool,
) -> Result<Option<(ExecutorMetadata, ExecutorData)>, Box<dyn std::error::Error + Send + Sync>> {
    let scheme = if tls_enabled { "https" } else { "http" };
    let endpoint_url = format!("{}://{}:{}", scheme, srv.target, srv.port);

    tracing::debug!("Attempting to connect to executor at {}", endpoint_url);

    let mut endpoint =
        Endpoint::from_shared(endpoint_url.clone())?.connect_timeout(EXECUTOR_CONNECT_TIMEOUT);

    if let Some(tls_config) = client_tls_config {
        endpoint = endpoint.tls_config(tls_config)?;
    }

    let channel = match endpoint.connect().await {
        Ok(ch) => ch,
        Err(e) => {
            tracing::debug!("Failed to connect to {}: {}", endpoint_url, e);
            return Ok(None);
        }
    };

    let mut client = ExecutorServiceClient::new(channel);

    let response = match client.describe_executor(DescribeExecutorRequest {}).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            tracing::debug!("DescribeExecutor RPC failed for {}: {}", endpoint_url, e);
            return Ok(None);
        }
    };

    tracing::debug!("Successfully described executor {response:?} at {endpoint_url}");

    #[expect(clippy::cast_possible_truncation)]
    let metadata = ExecutorMetadata {
        id: response.executor_id.clone(),
        host: response.host.clone(),
        port: response.port as u16,
        grpc_port: response.grpc_port as u16,
        specification: ballista_core::serde::scheduler::ExecutorSpecification {
            task_slots: response.task_slots,
        },
    };

    let data = ExecutorData {
        executor_id: response.executor_id,
        total_task_slots: response.task_slots,
        available_task_slots: response.task_slots,
    };

    Ok(Some((metadata, data)))
}
