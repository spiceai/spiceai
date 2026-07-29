/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Runtime component status tracking.
//!
//! Holds [`RuntimeStatus`], the shared tracker of per-component load status
//! (`dataset:`, `model:`, `catalog:`, …), overall runtime readiness
//! ([`RuntimeReadyState`]), and the shutdown [`tokio_util::sync::CancellationToken`].
//! It depends only on below-runtime crates (the status enum and the metrics
//! definitions), so components below the `runtime` orchestrator can report and
//! await status without depending on `runtime`.

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use datafusion_common::TableReference;
use opentelemetry::KeyValue;

// Re-export ComponentStatus from the shared API types crate
pub use runtime_api_types::v1::ComponentStatus;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RuntimeReadyState {
    #[default]
    OnLoad,
    OnRegistration,
}

/// Why a readiness wait returned.
///
/// Every `wait_for_*` helper races the awaited status against the runtime's
/// shutdown token, so a wait always returns — but only [`WaitOutcome::Reached`]
/// means the status was actually observed. Callers that go on to do work must
/// check this: treating [`WaitOutcome::ShuttingDown`] as ready would start work
/// (a refresh, a checkpoint, a scheduler ack) on a runtime that is tearing down.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The awaited status was observed.
    Reached,
    /// The runtime began shutting down before the awaited status was observed
    /// (or the status tracker itself was dropped, which only happens at
    /// teardown). The awaited status was never reached.
    ShuttingDown,
}

/// Per-component state stored in the `statuses` map.
///
/// The `notifier` is created lazily the first time a caller subscribes via
/// `get_or_create_notifier`; components that are never waited on carry no
/// watch-channel overhead.
#[derive(Debug)]
struct ComponentState {
    status: ComponentStatus,
    notifier: Option<watch::Sender<ComponentStatus>>,
}

#[derive(Clone, Debug)]
pub struct RuntimeStatus {
    /// Stores the current status of all components with optional notifiers.
    statuses: Arc<RwLock<HashMap<String, ComponentState>>>,
    /// Tracks components that have been in the Ready state at least once.
    ever_ready_components: Arc<RwLock<HashSet<String>>>,
    /// Tracks if the runtime is in the process of shutting down.
    is_shutdown: Arc<AtomicBool>,
    /// Controls how runtime readiness is computed.
    ready_state: Arc<RwLock<RuntimeReadyState>>,
    /// Cancellation token that is cancelled when the runtime is shutting down.
    /// Used to make background retry loops promptly exit on shutdown.
    shutdown_token: CancellationToken,
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            statuses: Arc::new(RwLock::new(HashMap::new())),
            ever_ready_components: Arc::new(RwLock::new(HashSet::new())),
            is_shutdown: Arc::new(AtomicBool::new(false)),
            ready_state: Arc::new(RwLock::new(RuntimeReadyState::default())),
            shutdown_token: CancellationToken::new(),
        }
    }
}

impl RuntimeStatus {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            statuses: Arc::new(RwLock::new(HashMap::new())),
            ever_ready_components: Arc::new(RwLock::new(HashSet::new())),
            is_shutdown: Arc::new(AtomicBool::new(false)),
            ready_state: Arc::new(RwLock::new(RuntimeReadyState::default())),
            shutdown_token: CancellationToken::new(),
        })
    }

    pub fn set_ready_state(&self, ready_state: RuntimeReadyState) {
        let mut configured_ready_state = self
            .ready_state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *configured_ready_state = ready_state;
    }

    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }

    /// Updates the status of a component (by its full `kind:name` key) and tracks
    /// if it has ever been ready.
    #[expect(clippy::needless_pass_by_value)]
    pub fn update_component_status(&self, component_name: &str, status: ComponentStatus) {
        let mut statuses = self
            .statuses
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        match statuses.entry(component_name.to_string()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                state.status = status.clone();
                if let Some(tx) = &state.notifier {
                    // send_replace stores the new value even when no receivers exist.
                    tx.send_replace(status.clone());
                }
            }
            Entry::Vacant(e) => {
                e.insert(ComponentState {
                    status: status.clone(),
                    notifier: None,
                });
            }
        }

        drop(statuses);

        if status == ComponentStatus::Ready {
            self.ever_ready_components
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(component_name.to_string());
        }
    }

    pub fn update_catalog(&self, catalog_name: impl Into<String>, status: ComponentStatus) {
        let catalog_name = catalog_name.into();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("catalog:{catalog_name}"), status);
        runtime_metrics::catalogs::STATUS
            .record(metric_value, &[KeyValue::new("catalog", catalog_name)]);
    }

    pub fn update_dataset(&self, dataset: &TableReference, status: ComponentStatus) {
        let ds_name = dataset.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("dataset:{ds_name}"), status);
        runtime_metrics::datasets::STATUS
            .record(metric_value, &[KeyValue::new("dataset", ds_name)]);
    }

    pub fn update_model(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("model:{model_name}"), status);
        runtime_metrics::models::STATUS.record(metric_value, &[KeyValue::new("model", model_name)]);
    }

    pub fn update_tool(&self, tool_name: &str, status: ComponentStatus) {
        let tool_name = tool_name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("tool:{tool_name}"), status);
        runtime_metrics::tools::STATUS.record(metric_value, &[KeyValue::new("tool", tool_name)]);
    }

    pub fn update_tool_catalog(&self, catalog_name: &str, status: ComponentStatus) {
        let name = catalog_name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("tool_catalog:{name}"), status);
        runtime_metrics::tools::STATUS.record(metric_value, &[KeyValue::new("tool_catalog", name)]);
    }

    pub fn update_llm(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("llm:{model_name}"), status);
        runtime_metrics::llms::STATUS.record(metric_value, &[KeyValue::new("model", model_name)]);
    }

    pub fn update_embedding(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("embedding:{model_name}"), status);
        runtime_metrics::embeddings::STATUS
            .record(metric_value, &[KeyValue::new("model", model_name)]);
    }

    pub fn update_reranker(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("reranker:{model_name}"), status);
        runtime_metrics::rerankers::STATUS
            .record(metric_value, &[KeyValue::new("model", model_name)]);
    }
    pub fn update_view(&self, view_name: &TableReference, status: ComponentStatus) {
        let view_name = view_name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("view:{view_name}"), status);
        runtime_metrics::views::STATUS.record(metric_value, &[KeyValue::new("view", view_name)]);
    }

    /// Update the status of a worker
    pub fn update_worker(&self, name: &str, status: ComponentStatus) {
        let worker_name = name.to_string();
        let metric_value = status.discriminant();
        self.update_component_status(&format!("worker:{worker_name}"), status);
        runtime_metrics::workers::STATUS
            .record(metric_value, &[KeyValue::new("worker", worker_name)]);
    }

    /// Update the status of a cluster node
    pub fn update_cluster(&self, node_name: &str, status: ComponentStatus) {
        let cluster_node_name = node_name.to_string();

        // Record cluster node status metric
        // Map ComponentStatus to cluster status values: 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining
        let status_value = match &status {
            ComponentStatus::Initializing | ComponentStatus::NotLoaded => 0,
            ComponentStatus::Ready | ComponentStatus::Refreshing => 1, // Refreshing is still healthy
            ComponentStatus::Disabled | ComponentStatus::Error(_) => 2,
            ComponentStatus::ShuttingDown => 3, // Draining
        };

        self.update_component_status(&format!("cluster:{cluster_node_name}"), status);
        runtime_metrics::cluster::set_node_status(&cluster_node_name, node_name, status_value);
    }

    /// Get the status of a worker
    #[must_use]
    pub fn worker_status(&self, name: &str) -> Option<ComponentStatus> {
        let full_name = format!("worker:{name}");
        self.get_component_status(&full_name)
    }

    /// Checks if all registered components have been ready at least once and the runtime is not shutting down.
    ///
    /// This function returns `true` if all components that have ever been registered
    /// have reached the `Ready` state at least once.
    /// Once this state is reached, it will continue to return `true` regardless of the
    /// current state of any component.
    ///
    /// This is intentionally conservative - in the accelerated datasets case, we can
    /// continue to serve data from the acceleration layer even if the source dataset
    /// is in an error state.
    ///
    /// Returns `false` if:
    /// - No components have been registered yet.
    /// - There are one or more registered components that have never been in the `Ready` state.
    /// - The runtime is in the process of shutting down.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        if self.is_shutdown() {
            return false;
        }

        let ready_state = *self
            .ready_state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let statuses = match self.statuses.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if statuses.is_empty() {
            return false; // No components registered yet
        }

        match ready_state {
            RuntimeReadyState::OnLoad => {
                let ever_ready = match self.ever_ready_components.read() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };

                // OnLoad readiness: a component counts as ready if it has been ready at least once.
                // All registered components must appear in the ever-ready set before we report ready.
                statuses
                    .keys()
                    .all(|component| ever_ready.contains(component))
            }
            // OnRegistration readiness: treat Error/Disabled/Initializing as ready-enough.
            // Only components in ShuttingDown state block overall readiness.
            RuntimeReadyState::OnRegistration => statuses
                .values()
                .all(|state| !matches!(state.status, ComponentStatus::ShuttingDown)),
        }
    }

    /// Returns the status of all registered components.
    #[must_use]
    pub fn get_all_statuses(&self) -> HashMap<String, ComponentStatus> {
        let statuses = match self.statuses.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        statuses
            .iter()
            .map(|(k, state)| (k.clone(), state.status.clone()))
            .collect()
    }

    /// Returns the status of all registered models.
    ///
    /// Keys are the `model_name`, not the format from [`RuntimeStatus::get_all_statuses`] (i.e. `model:<model_name>`).
    #[must_use]
    pub fn get_model_statuses(&self) -> HashMap<String, ComponentStatus> {
        self.get_statuses_of_prefix("model:")
    }

    /// Returns the status of all registered catalogs.
    #[must_use]
    pub fn get_catalog_statuses(&self) -> HashMap<String, ComponentStatus> {
        self.get_statuses_of_prefix("catalog:")
    }

    /// Returns the status of all registered datasets.
    #[must_use]
    pub fn get_dataset_statuses(&self) -> HashMap<TableReference, ComponentStatus> {
        self.get_statuses_of_prefix("dataset:")
    }

    /// Returns the current status of a single dataset, if registered.
    #[must_use]
    pub fn get_dataset_status(&self, dataset: &TableReference) -> Option<ComponentStatus> {
        self.get_component_status(&format!("dataset:{dataset}"))
    }

    /// Returns the status of all registered views.
    #[must_use]
    pub fn get_view_statuses(&self) -> HashMap<TableReference, ComponentStatus> {
        self.get_statuses_of_prefix("view:")
    }

    /// Returns the status of all registered workers.
    #[must_use]
    pub fn get_worker_statuses(&self) -> HashMap<String, ComponentStatus> {
        self.get_statuses_of_prefix("worker:")
    }

    #[must_use]
    fn get_statuses_of_prefix<S>(&self, prefix: &'static str) -> HashMap<S, ComponentStatus>
    where
        S: for<'a> From<&'a str> + Eq + std::hash::Hash,
    {
        let statuses = match self.statuses.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        statuses
            .iter()
            .filter_map(|(k, state)| {
                k.strip_prefix(prefix)
                    .map(|name| (name.into(), state.status.clone()))
            })
            .collect()
    }

    /// Sets the runtime to the shutting down state.
    pub fn mark_shutdown(&self) {
        self.is_shutdown.store(true, Ordering::SeqCst);
        self.shutdown_token.cancel();
    }

    /// Returns a child of the shutdown cancellation token.
    ///
    /// The returned token is cancelled when the runtime shuts down (via
    /// `mark_shutdown`), but calling `cancel()` on it will **not** cancel
    /// the runtime's own token. This preserves the invariant that only
    /// `mark_shutdown` triggers a runtime-wide shutdown.
    ///
    /// Use `token.cancelled()` in `tokio::select!` to make async operations
    /// (e.g. backoff sleeps) immediately interruptible on shutdown.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.child_token()
    }

    /// Returns the status of a specific component by its full name.
    #[must_use]
    pub fn get_component_status(&self, component_name: &str) -> Option<ComponentStatus> {
        let statuses = self
            .statuses
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        statuses
            .get(component_name)
            .map(|state| state.status.clone())
    }

    /// Gets or creates a notifier for a component, returning a receiver to watch for status changes.
    fn get_or_create_notifier(&self, component_name: &str) -> watch::Receiver<ComponentStatus> {
        let mut statuses = self
            .statuses
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        match statuses.entry(component_name.to_string()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                if let Some(tx) = &state.notifier {
                    tx.subscribe()
                } else {
                    let (tx, rx) = watch::channel(state.status.clone());
                    state.notifier = Some(tx);
                    rx
                }
            }
            Entry::Vacant(e) => {
                let (tx, rx) = watch::channel(ComponentStatus::Initializing);
                e.insert(ComponentState {
                    status: ComponentStatus::Initializing,
                    notifier: Some(tx),
                });
                rx
            }
        }
    }

    /// Waits until a component's status satisfies `is_satisfied`, or the runtime
    /// starts shutting down — whichever happens first.
    async fn wait_for_component_status(
        &self,
        component_name: &str,
        is_satisfied: impl Fn(&ComponentStatus) -> bool,
    ) -> WaitOutcome {
        let mut receiver = self.get_or_create_notifier(component_name);

        loop {
            // Check the current value first, so an already-satisfied component
            // returns without awaiting anything.
            if is_satisfied(&receiver.borrow()) {
                return WaitOutcome::Reached;
            }

            // `mark_shutdown` cancels the shutdown token but does NOT close the
            // per-component watch channels — their senders live in the `statuses`
            // map, which the still-alive tracker owns and never removes from — so
            // without racing the token a component that never reaches its target
            // status parks this task forever. `watch::Receiver::changed` is
            // cancel-safe, so losing the race drops no status change.
            tokio::select! {
                changed = receiver.changed() => {
                    if changed.is_err() {
                        // The only sender was dropped, i.e. the tracker itself is
                        // gone: the awaited status can never arrive.
                        return WaitOutcome::ShuttingDown;
                    }
                }
                () = self.shutdown_token.cancelled() => return WaitOutcome::ShuttingDown,
            }
        }
    }

    /// Internal helper to wait for a component to become ready.
    async fn wait_for_component_ready(&self, component_name: &str) -> WaitOutcome {
        self.wait_for_component_status(component_name, |status| *status == ComponentStatus::Ready)
            .await
    }

    /// Waits for a component to leave the `Initializing` state — used by
    /// callers that only need the component registered, not fully ready.
    async fn wait_for_component_registered(&self, component_name: &str) -> WaitOutcome {
        self.wait_for_component_status(component_name, |status| {
            !matches!(status, ComponentStatus::Initializing)
        })
        .await
    }

    /// Waits for a dataset to become ready.
    pub async fn wait_for_dataset_ready(&self, dataset: &TableReference) -> WaitOutcome {
        let component_name = format!("dataset:{dataset}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for a dataset to be registered (any status other than
    /// `Initializing`). Useful when the caller only needs the table
    /// provider to exist, not for the dataset to be fully loaded — e.g.
    /// scheduler-side partition discovery, where waiting for `Ready`
    /// would deadlock because `Ready` is gated on executor data loads.
    pub async fn wait_for_dataset_registered(&self, dataset: &TableReference) -> WaitOutcome {
        let component_name = format!("dataset:{dataset}");
        self.wait_for_component_registered(&component_name).await
    }

    /// Waits for a model to become ready.
    pub async fn wait_for_model_ready(&self, model_name: &str) -> WaitOutcome {
        let component_name = format!("model:{model_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for a catalog to become ready.
    pub async fn wait_for_catalog_ready(&self, catalog_name: &str) -> WaitOutcome {
        let component_name = format!("catalog:{catalog_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for a tool to become ready.
    pub async fn wait_for_tool_ready(&self, tool_name: &str) -> WaitOutcome {
        let component_name = format!("tool:{tool_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for a tool catalog to become ready.
    pub async fn wait_for_tool_catalog_ready(&self, catalog_name: &str) -> WaitOutcome {
        let component_name = format!("tool_catalog:{catalog_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for an LLM to become ready.
    pub async fn wait_for_llm_ready(&self, model_name: &str) -> WaitOutcome {
        let component_name = format!("llm:{model_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for an embedding model to become ready.
    pub async fn wait_for_embedding_ready(&self, model_name: &str) -> WaitOutcome {
        let component_name = format!("embedding:{model_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for a view to become ready.
    pub async fn wait_for_view_ready(&self, view_name: &TableReference) -> WaitOutcome {
        let component_name = format!("view:{view_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for a worker to become ready.
    pub async fn wait_for_worker_ready(&self, worker_name: &str) -> WaitOutcome {
        let component_name = format!("worker:{worker_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for a cluster node to become ready.
    pub async fn wait_for_cluster_ready(&self, node_name: &str) -> WaitOutcome {
        let component_name = format!("cluster:{node_name}");
        self.wait_for_component_ready(&component_name).await
    }

    /// Waits for the entire runtime to be ready (all registered components have been ready at least once).
    ///
    /// This polls the `is_ready()` status at a regular interval until the runtime is ready.
    /// If the runtime is already ready, this returns immediately.
    ///
    /// Returns [`WaitOutcome::ShuttingDown`] instead of polling forever once
    /// shutdown starts: `is_ready()` reports `false` for the rest of the process
    /// lifetime from that point, so the poll could never succeed.
    pub async fn wait_for_ready(&self) -> WaitOutcome {
        const POLL_INTERVAL: Duration = Duration::from_millis(100);
        loop {
            if self.is_ready() {
                return WaitOutcome::Reached;
            }
            tokio::select! {
                () = tokio::time::sleep(POLL_INTERVAL) => {}
                () = self.shutdown_token.cancelled() => return WaitOutcome::ShuttingDown,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// How long a shutdown-aware wait gets before the test calls it hung. Only an
    /// upper bound on a wake-up that should be immediate — before the fix these
    /// waits never returned at all, so any finite bound fails on the old code.
    const SHUTDOWN_WAIT_BOUND: Duration = Duration::from_secs(5);

    #[test]
    fn test_get_component_status() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        // Initially no status
        assert!(
            status
                .get_component_status("dataset:test_dataset")
                .is_none()
        );

        // Set status
        status.update_dataset(&dataset, ComponentStatus::Initializing);
        assert_eq!(
            status.get_component_status("dataset:test_dataset"),
            Some(ComponentStatus::Initializing)
        );

        // Update status
        status.update_dataset(&dataset, ComponentStatus::Ready);
        assert_eq!(
            status.get_component_status("dataset:test_dataset"),
            Some(ComponentStatus::Ready)
        );
    }

    #[test]
    fn test_is_ready_on_registration_requires_registered_component() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        status.set_ready_state(RuntimeReadyState::OnRegistration);

        // Empty statuses are not ready.
        assert!(!status.is_ready());

        // Initializing still counts as registered.
        status.update_dataset(&dataset, ComponentStatus::Initializing);
        assert!(status.is_ready());

        // Refreshing stays ready.
        status.update_dataset(&dataset, ComponentStatus::Refreshing);
        assert!(status.is_ready());

        // Error still counts as registered for on_registration mode.
        status.update_dataset(&dataset, ComponentStatus::error());
        assert!(status.is_ready());

        // Explicit shutting down state for a component is not ready.
        status.update_dataset(&dataset, ComponentStatus::ShuttingDown);
        assert!(!status.is_ready());

        // Runtime-level shutdown always forces not ready.
        status.update_dataset(&dataset, ComponentStatus::Ready);
        assert!(status.is_ready());
        status.mark_shutdown();
        assert!(!status.is_ready());
    }

    #[test]
    fn test_is_ready_on_registration_allows_mixed_component_states() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        status.set_ready_state(RuntimeReadyState::OnRegistration);

        status.update_dataset(&dataset, ComponentStatus::error());
        status.update_model("test_model", ComponentStatus::Initializing);
        status.update_tool("test_tool", ComponentStatus::Disabled);

        assert!(status.is_ready());

        status.update_tool("test_tool", ComponentStatus::ShuttingDown);
        assert!(!status.is_ready());
    }

    #[test]
    fn test_is_ready_on_registration_all_component_types() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("dataset_a");
        let view = TableReference::bare("view_a");

        status.set_ready_state(RuntimeReadyState::OnRegistration);

        status.update_catalog("catalog_a", ComponentStatus::Initializing);
        status.update_dataset(&dataset, ComponentStatus::error());
        status.update_model("model_a", ComponentStatus::Disabled);
        status.update_tool("tool_a", ComponentStatus::Refreshing);
        status.update_tool_catalog("tool_catalog_a", ComponentStatus::Ready);
        status.update_llm("llm_a", ComponentStatus::error());
        status.update_embedding("embedding_a", ComponentStatus::Initializing);
        status.update_view(&view, ComponentStatus::Ready);
        status.update_worker("worker_a", ComponentStatus::Disabled);
        status.update_cluster("cluster_node_a", ComponentStatus::error());

        assert!(
            status.is_ready(),
            "on_registration should be ready when all components are registered and none are ShuttingDown"
        );

        status.update_embedding("embedding_a", ComponentStatus::ShuttingDown);
        assert!(
            !status.is_ready(),
            "any component in ShuttingDown should make runtime not ready"
        );
    }

    #[test]
    fn test_is_ready_on_load_still_requires_ever_ready() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        status.set_ready_state(RuntimeReadyState::OnLoad);

        status.update_dataset(&dataset, ComponentStatus::Initializing);
        assert!(!status.is_ready());

        status.update_dataset(&dataset, ComponentStatus::Ready);
        assert!(status.is_ready());

        // Once ever-ready is achieved, transient non-ready states still satisfy OnLoad mode.
        status.update_dataset(&dataset, ComponentStatus::Refreshing);
        assert!(status.is_ready());
    }

    #[tokio::test]
    async fn test_wait_for_dataset_ready_already_ready() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        // Set dataset to ready before waiting
        status.update_dataset(&dataset, ComponentStatus::Ready);

        // Should return immediately
        assert_eq!(
            status.wait_for_dataset_ready(&dataset).await,
            WaitOutcome::Reached
        );
    }

    #[tokio::test]
    async fn test_wait_for_dataset_ready_becomes_ready() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        // Set dataset to initializing
        status.update_dataset(&dataset, ComponentStatus::Initializing);

        // Spawn a task to set the dataset ready after a short delay
        let status_clone = Arc::clone(&status);
        let dataset_clone = dataset.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            status_clone.update_dataset(&dataset_clone, ComponentStatus::Ready);
        });

        // Wait for ready
        assert_eq!(
            status.wait_for_dataset_ready(&dataset).await,
            WaitOutcome::Reached
        );
    }

    #[tokio::test]
    async fn test_wait_for_dataset_ready_not_yet_registered() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        // Dataset not registered - should start with Initializing and wait
        // Spawn a task to register and set ready after a delay
        let status_clone = Arc::clone(&status);
        let dataset_clone = dataset.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            status_clone.update_dataset(&dataset_clone, ComponentStatus::Ready);
        });

        assert_eq!(
            status.wait_for_dataset_ready(&dataset).await,
            WaitOutcome::Reached
        );
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        status.update_dataset(&dataset, ComponentStatus::Initializing);

        // Create multiple waiters
        let status1 = Arc::clone(&status);
        let status2 = Arc::clone(&status);
        let dataset1 = dataset.clone();
        let dataset2 = dataset.clone();

        let handle1 = tokio::spawn(async move { status1.wait_for_dataset_ready(&dataset1).await });

        let handle2 = tokio::spawn(async move { status2.wait_for_dataset_ready(&dataset2).await });

        // Give tasks time to start waiting
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Set ready - both should wake up
        status.update_dataset(&dataset, ComponentStatus::Ready);

        assert_eq!(
            handle1.await.expect("task 1 should complete"),
            WaitOutcome::Reached
        );
        assert_eq!(
            handle2.await.expect("task 2 should complete"),
            WaitOutcome::Reached
        );
    }

    #[tokio::test]
    async fn test_wait_for_dataset_ready_waits_indefinitely() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        // Set dataset to initializing
        status.update_dataset(&dataset, ComponentStatus::Initializing);

        // Spawn a task to set the dataset ready after a short delay
        let status_clone = Arc::clone(&status);
        let dataset_clone = dataset.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            status_clone.update_dataset(&dataset_clone, ComponentStatus::Ready);
        });

        // Wait indefinitely
        assert_eq!(
            status.wait_for_dataset_ready(&dataset).await,
            WaitOutcome::Reached
        );
    }

    /// A component wait must return once shutdown starts, even though the
    /// component never reaches `Ready`: `mark_shutdown` cancels the shutdown
    /// token but leaves the per-component watch channel open, so a wait that only
    /// awaited `changed()` parked forever.
    #[tokio::test]
    async fn test_wait_for_dataset_ready_returns_on_shutdown() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("never_ready");

        status.update_dataset(&dataset, ComponentStatus::Initializing);

        let waiter = {
            let status = Arc::clone(&status);
            let dataset = dataset.clone();
            tokio::spawn(async move { status.wait_for_dataset_ready(&dataset).await })
        };

        status.mark_shutdown();

        let outcome = tokio::time::timeout(SHUTDOWN_WAIT_BOUND, waiter)
            .await
            .expect("wait_for_dataset_ready should return once shutdown starts")
            .expect("waiter task should not panic");
        assert_eq!(outcome, WaitOutcome::ShuttingDown);
        // The component is still Initializing, so the caller must be told the
        // status was never reached rather than being allowed to proceed.
        assert_eq!(
            status.get_component_status("dataset:never_ready"),
            Some(ComponentStatus::Initializing)
        );
    }

    /// Same for the registered-only wait, whose loop had the same shape.
    #[tokio::test]
    async fn test_wait_for_dataset_registered_returns_on_shutdown() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("never_registered");

        status.update_dataset(&dataset, ComponentStatus::Initializing);

        let waiter = {
            let status = Arc::clone(&status);
            let dataset = dataset.clone();
            tokio::spawn(async move { status.wait_for_dataset_registered(&dataset).await })
        };

        status.mark_shutdown();

        let outcome = tokio::time::timeout(SHUTDOWN_WAIT_BOUND, waiter)
            .await
            .expect("wait_for_dataset_registered should return once shutdown starts")
            .expect("waiter task should not panic");
        assert_eq!(outcome, WaitOutcome::ShuttingDown);
    }

    /// A wait entered *after* shutdown has already started must return without
    /// awaiting a status change that can never come.
    #[tokio::test]
    async fn test_wait_for_dataset_ready_returns_when_already_shut_down() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("never_ready");

        status.update_dataset(&dataset, ComponentStatus::Initializing);
        status.mark_shutdown();

        let outcome =
            tokio::time::timeout(SHUTDOWN_WAIT_BOUND, status.wait_for_dataset_ready(&dataset))
                .await
                .expect("wait_for_dataset_ready should return immediately when already shut down");
        assert_eq!(outcome, WaitOutcome::ShuttingDown);
    }

    /// A component that is already `Ready` when shutdown has started still
    /// reports `Reached` — shutdown only reports the status it prevented.
    #[tokio::test]
    async fn test_wait_for_dataset_ready_after_shutdown_still_reports_ready_component() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("ready_dataset");

        status.update_dataset(&dataset, ComponentStatus::Ready);
        status.mark_shutdown();

        assert_eq!(
            status.wait_for_dataset_ready(&dataset).await,
            WaitOutcome::Reached
        );
    }

    /// `wait_for_ready` polls `is_ready()`, which returns `false` for the rest of
    /// the process once shutdown starts — so without racing the shutdown token it
    /// busy-polled forever.
    #[tokio::test]
    async fn test_wait_for_ready_returns_on_shutdown() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("never_ready");

        status.update_dataset(&dataset, ComponentStatus::Initializing);
        assert!(!status.is_ready());

        let waiter = {
            let status = Arc::clone(&status);
            tokio::spawn(async move { status.wait_for_ready().await })
        };

        status.mark_shutdown();

        let outcome = tokio::time::timeout(SHUTDOWN_WAIT_BOUND, waiter)
            .await
            .expect("wait_for_ready should return once shutdown starts")
            .expect("waiter task should not panic");
        assert_eq!(outcome, WaitOutcome::ShuttingDown);
    }

    /// Shutdown must not be the only way out: a wait still blocks while the
    /// runtime is running and not yet ready.
    #[tokio::test]
    async fn test_wait_for_ready_still_blocks_while_not_ready() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("slow_dataset");

        status.update_dataset(&dataset, ComponentStatus::Initializing);

        assert!(
            tokio::time::timeout(Duration::from_millis(250), status.wait_for_ready())
                .await
                .is_err(),
            "wait_for_ready should still block while the runtime is running and not ready"
        );

        status.update_dataset(&dataset, ComponentStatus::Ready);
        assert_eq!(status.wait_for_ready().await, WaitOutcome::Reached);
    }

    #[tokio::test]
    async fn test_notifier_updates_on_status_change() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        // Get a receiver before any status is set
        let mut receiver = status.get_or_create_notifier("dataset:test_dataset");
        assert_eq!(*receiver.borrow(), ComponentStatus::Initializing);

        // Update status
        status.update_dataset(&dataset, ComponentStatus::Refreshing);

        // Wait for change
        receiver.changed().await.expect("should receive change");
        assert_eq!(*receiver.borrow(), ComponentStatus::Refreshing);

        // Update to ready
        status.update_dataset(&dataset, ComponentStatus::Ready);
        receiver.changed().await.expect("should receive change");
        assert_eq!(*receiver.borrow(), ComponentStatus::Ready);
    }
}
