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
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::watch;

use datafusion::sql::TableReference;
use opentelemetry::KeyValue;

use crate::metrics;

// Re-export ComponentStatus from the shared API types crate
pub use runtime_api_types::v1::ComponentStatus;

#[derive(Clone, Debug, Default)]
pub struct RuntimeStatus {
    /// Stores the current status of all components.
    statuses: Arc<RwLock<HashMap<String, ComponentStatus>>>,
    /// Tracks components that have been in the Ready state at least once.
    ever_ready_components: Arc<RwLock<HashSet<String>>>,
    /// Tracks if the runtime is in the process of shutting down.
    is_shutdown: Arc<AtomicBool>,
    /// Per-component notifiers for status change subscriptions.
    notifiers: Arc<RwLock<HashMap<String, watch::Sender<ComponentStatus>>>>,
}

impl RuntimeStatus {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            statuses: Arc::new(RwLock::new(HashMap::new())),
            ever_ready_components: Arc::new(RwLock::new(HashSet::new())),
            is_shutdown: Arc::new(AtomicBool::new(false)),
            notifiers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }

    /// Updates the status of a component and tracks if it has ever been ready.
    fn update_component_status(&self, component_name: &str, status: ComponentStatus) {
        let mut statuses = match self.statuses.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        statuses.insert(component_name.to_string(), status);

        if status == ComponentStatus::Ready {
            let mut ever_ready = match self.ever_ready_components.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            ever_ready.insert(component_name.to_string());
        }

        // Notify subscribers of the status change
        let notifiers = self
            .notifiers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(sender) = notifiers.get(component_name) {
            let _ = sender.send(status); // Ignore error if no receivers
        }
    }

    pub fn update_catalog(&self, catalog_name: impl Into<String>, status: ComponentStatus) {
        let catalog_name = catalog_name.into();
        self.update_component_status(&format!("catalog:{catalog_name}"), status);
        metrics::catalogs::STATUS.record(status as u64, &[KeyValue::new("catalog", catalog_name)]);
    }

    pub fn update_dataset(&self, dataset: &TableReference, status: ComponentStatus) {
        let ds_name = dataset.to_string();
        self.update_component_status(&format!("dataset:{ds_name}"), status);
        metrics::datasets::STATUS.record(status as u64, &[KeyValue::new("dataset", ds_name)]);
    }

    pub fn update_model(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        self.update_component_status(&format!("model:{model_name}"), status);
        metrics::models::STATUS.record(status as u64, &[KeyValue::new("model", model_name)]);
    }

    pub fn update_tool(&self, tool_name: &str, status: ComponentStatus) {
        let tool_name = tool_name.to_string();
        self.update_component_status(&format!("tool:{tool_name}"), status);
        metrics::tools::STATUS.record(status as u64, &[KeyValue::new("tool", tool_name)]);
    }

    pub fn update_tool_catalog(&self, catalog_name: &str, status: ComponentStatus) {
        let name = catalog_name.to_string();
        self.update_component_status(&format!("tool_catalog:{name}"), status);
        metrics::tools::STATUS.record(status as u64, &[KeyValue::new("tool_catalog", name)]);
    }

    pub fn update_llm(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        self.update_component_status(&format!("llm:{model_name}"), status);
        metrics::llms::STATUS.record(status as u64, &[KeyValue::new("model", model_name)]);
    }

    pub fn update_embedding(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        self.update_component_status(&format!("embedding:{model_name}"), status);
        metrics::embeddings::STATUS.record(status as u64, &[KeyValue::new("model", model_name)]);
    }
    pub fn update_view(&self, view_name: &TableReference, status: ComponentStatus) {
        let view_name = view_name.to_string();
        self.update_component_status(&format!("view:{view_name}"), status);
        metrics::views::STATUS.record(status as u64, &[KeyValue::new("view", view_name)]);
    }

    /// Update the status of a worker
    pub fn update_worker(&self, name: &str, status: ComponentStatus) {
        let worker_name = name.to_string();
        self.update_component_status(&format!("worker:{worker_name}"), status);
        metrics::models::STATUS.record(status as u64, &[KeyValue::new("worker", worker_name)]);
    }

    /// Update the status of a cluster node
    pub fn update_cluster(&self, node_name: &str, status: ComponentStatus) {
        let cluster_node_name = node_name.to_string();
        self.update_component_status(&format!("cluster:{cluster_node_name}"), status);

        // Record cluster node status metric
        // Map ComponentStatus to cluster status values: 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining
        let status_value = match status {
            ComponentStatus::Initializing => 0,
            ComponentStatus::Ready | ComponentStatus::Refreshing => 1, // Refreshing is still healthy
            ComponentStatus::Disabled | ComponentStatus::Error => 2,
            ComponentStatus::ShuttingDown => 3, // Draining
        };
        metrics::cluster::set_node_status(&cluster_node_name, node_name, status_value);
    }

    /// Get the status of a worker
    #[must_use]
    pub fn worker_status(&self, name: &str) -> Option<ComponentStatus> {
        let components = match self.statuses.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let full_name = format!("worker:{name}");
        components.get(&full_name).copied()
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

        let statuses = match self.statuses.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let ever_ready = match self.ever_ready_components.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if statuses.is_empty() {
            return false; // No components registered yet
        }

        // Check if all registered components have been ready at least once
        statuses
            .keys()
            .all(|component| ever_ready.contains(component))
    }

    /// Returns the status of all registered components.
    #[must_use]
    pub fn get_all_statuses(&self) -> HashMap<String, ComponentStatus> {
        let statuses = match self.statuses.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        statuses.clone()
    }

    /// Returns the status of all registered models.
    ///
    /// Keys are the `model_name`, not the format from [`RuntimeStatus::get_all_statuses`] (i.e. `model:<model_name>`).
    #[must_use]
    pub fn get_model_statuses(&self) -> HashMap<String, ComponentStatus> {
        self.get_statuses_of_prefix("model:")
    }

    /// Returns the status of all registered datasets.
    #[must_use]
    pub fn get_dataset_statuses(&self) -> HashMap<TableReference, ComponentStatus> {
        self.get_statuses_of_prefix("dataset:")
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
            .filter_map(|(k, v)| k.strip_prefix(prefix).map(|name| (name.into(), *v)))
            .collect()
    }

    /// Sets the runtime to the shutting down state.
    pub fn mark_shutdown(&self) {
        self.is_shutdown.store(true, Ordering::SeqCst);
    }

    /// Returns the status of a specific component by its full name.
    #[must_use]
    pub fn get_component_status(&self, component_name: &str) -> Option<ComponentStatus> {
        let statuses = self
            .statuses
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        statuses.get(component_name).copied()
    }

    /// Gets or creates a notifier for a component, returning a receiver to watch for status changes.
    fn get_or_create_notifier(&self, component_name: &str) -> watch::Receiver<ComponentStatus> {
        let mut notifiers = self
            .notifiers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        match notifiers.entry(component_name.to_string()) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let current = self
                    .get_component_status(component_name)
                    .unwrap_or(ComponentStatus::Initializing);
                let (tx, rx) = watch::channel(current);
                e.insert(tx);
                rx
            }
        }
    }

    /// Internal helper to wait for a component to become ready.
    async fn wait_for_component_ready(&self, component_name: &str) {
        let mut receiver = self.get_or_create_notifier(component_name);

        loop {
            // Check current value (handles already-ready case)
            if *receiver.borrow() == ComponentStatus::Ready {
                return;
            }

            // Wait for next change; return if channel closed (runtime shutting down)
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }

    /// Waits for a dataset to become ready.
    pub async fn wait_for_dataset_ready(&self, dataset: &TableReference) {
        let component_name = format!("dataset:{dataset}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for a model to become ready.
    pub async fn wait_for_model_ready(&self, model_name: &str) {
        let component_name = format!("model:{model_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for a catalog to become ready.
    pub async fn wait_for_catalog_ready(&self, catalog_name: &str) {
        let component_name = format!("catalog:{catalog_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for a tool to become ready.
    pub async fn wait_for_tool_ready(&self, tool_name: &str) {
        let component_name = format!("tool:{tool_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for a tool catalog to become ready.
    pub async fn wait_for_tool_catalog_ready(&self, catalog_name: &str) {
        let component_name = format!("tool_catalog:{catalog_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for an LLM to become ready.
    pub async fn wait_for_llm_ready(&self, model_name: &str) {
        let component_name = format!("llm:{model_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for an embedding model to become ready.
    pub async fn wait_for_embedding_ready(&self, model_name: &str) {
        let component_name = format!("embedding:{model_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for a view to become ready.
    pub async fn wait_for_view_ready(&self, view_name: &TableReference) {
        let component_name = format!("view:{view_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for a worker to become ready.
    pub async fn wait_for_worker_ready(&self, worker_name: &str) {
        let component_name = format!("worker:{worker_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for a cluster node to become ready.
    pub async fn wait_for_cluster_ready(&self, node_name: &str) {
        let component_name = format!("cluster:{node_name}");
        self.wait_for_component_ready(&component_name).await;
    }

    /// Waits for the entire runtime to be ready (all registered components have been ready at least once).
    ///
    /// This polls the `is_ready()` status at a regular interval until the runtime is ready.
    /// If the runtime is already ready, this returns immediately.
    pub async fn wait_for_ready(&self) {
        const POLL_INTERVAL: Duration = Duration::from_millis(100);
        loop {
            if self.is_ready() {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

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

    #[tokio::test]
    async fn test_wait_for_dataset_ready_already_ready() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("test_dataset");

        // Set dataset to ready before waiting
        status.update_dataset(&dataset, ComponentStatus::Ready);

        // Should return immediately
        status.wait_for_dataset_ready(&dataset).await;
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
        status.wait_for_dataset_ready(&dataset).await;
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

        status.wait_for_dataset_ready(&dataset).await;
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

        handle1.await.expect("task 1 should complete");
        handle2.await.expect("task 2 should complete");
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
        status.wait_for_dataset_ready(&dataset).await;
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
