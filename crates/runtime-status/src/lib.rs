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

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt::Write as _,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use datafusion::sql::{ResolvedTableReference, TableReference};
use opentelemetry::KeyValue;
use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

// Re-export ComponentStatus from the shared API types crate
pub use runtime_api_types::v1::ComponentStatus;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RuntimeReadyState {
    #[default]
    OnLoad,
    OnRegistration,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ComponentKey {
    Catalog(String),
    Dataset(TableReference),
    Model(String),
    Tool(String),
    ToolCatalog(String),
    Llm(String),
    Embedding(String),
    Reranker(String),
    View(TableReference),
    Worker(String),
    Cluster(String),
    Internal(String),
}

impl ComponentKey {
    #[must_use]
    pub fn catalog(name: impl Into<String>) -> Self {
        Self::Catalog(name.into())
    }

    #[must_use]
    pub fn dataset(name: &TableReference) -> Self {
        Self::Dataset(name.clone())
    }

    #[must_use]
    pub fn model(name: impl Into<String>) -> Self {
        Self::Model(name.into())
    }

    #[must_use]
    pub fn tool(name: impl Into<String>) -> Self {
        Self::Tool(name.into())
    }

    #[must_use]
    pub fn tool_catalog(name: impl Into<String>) -> Self {
        Self::ToolCatalog(name.into())
    }

    #[must_use]
    pub fn llm(name: impl Into<String>) -> Self {
        Self::Llm(name.into())
    }

    #[must_use]
    pub fn embedding(name: impl Into<String>) -> Self {
        Self::Embedding(name.into())
    }

    #[must_use]
    pub fn reranker(name: impl Into<String>) -> Self {
        Self::Reranker(name.into())
    }

    #[must_use]
    pub fn view(name: &TableReference) -> Self {
        Self::View(name.clone())
    }

    #[must_use]
    pub fn worker(name: impl Into<String>) -> Self {
        Self::Worker(name.into())
    }

    #[must_use]
    pub fn cluster(name: impl Into<String>) -> Self {
        Self::Cluster(name.into())
    }

    #[must_use]
    pub fn internal(name: impl Into<String>) -> Self {
        Self::Internal(name.into())
    }

    #[must_use]
    pub fn full_name(&self) -> String {
        match self {
            Self::Catalog(name) => format!("catalog:{name}"),
            Self::Dataset(name) => format!("dataset:{name}"),
            Self::Model(name) => format!("model:{name}"),
            Self::Tool(name) => format!("tool:{name}"),
            Self::ToolCatalog(name) => format!("tool_catalog:{name}"),
            Self::Llm(name) => format!("llm:{name}"),
            Self::Embedding(name) => format!("embedding:{name}"),
            Self::Reranker(name) => format!("reranker:{name}"),
            Self::View(name) => format!("view:{name}"),
            Self::Worker(name) => format!("worker:{name}"),
            Self::Cluster(name) => format!("cluster:{name}"),
            Self::Internal(name) => name.clone(),
        }
    }

    #[must_use]
    pub fn parse(component_name: &str) -> Self {
        const DATASET_PREFIX: &str = "dataset:";
        const VIEW_PREFIX: &str = "view:";
        const TOOL_CATALOG_PREFIX: &str = "tool_catalog:";
        const CATALOG_PREFIX: &str = "catalog:";
        const MODEL_PREFIX: &str = "model:";
        const TOOL_PREFIX: &str = "tool:";
        const LLM_PREFIX: &str = "llm:";
        const EMBEDDING_PREFIX: &str = "embedding:";
        const RERANKER_PREFIX: &str = "reranker:";
        const WORKER_PREFIX: &str = "worker:";
        const CLUSTER_PREFIX: &str = "cluster:";

        if let Some(name) = component_name.strip_prefix(DATASET_PREFIX) {
            return Self::Dataset(TableReference::parse_str(name));
        }
        if let Some(name) = component_name.strip_prefix(VIEW_PREFIX) {
            return Self::View(TableReference::parse_str(name));
        }
        if let Some(name) = component_name.strip_prefix(TOOL_CATALOG_PREFIX) {
            return Self::ToolCatalog(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(CATALOG_PREFIX) {
            return Self::Catalog(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(MODEL_PREFIX) {
            return Self::Model(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(TOOL_PREFIX) {
            return Self::Tool(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(LLM_PREFIX) {
            return Self::Llm(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(EMBEDDING_PREFIX) {
            return Self::Embedding(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(RERANKER_PREFIX) {
            return Self::Reranker(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(WORKER_PREFIX) {
            return Self::Worker(name.to_string());
        }
        if let Some(name) = component_name.strip_prefix(CLUSTER_PREFIX) {
            return Self::Cluster(name.to_string());
        }

        Self::Internal(component_name.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutorReadiness {
    pub ready: usize,
    pub registered: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadinessPolicy {
    pub min_ready_executors: Option<u32>,
    pub min_ready_executors_percent: Option<u8>,
}

impl ReadinessPolicy {
    #[must_use]
    pub fn count_gate_active(&self) -> bool {
        matches!(self.min_ready_executors, Some(n) if n > 0)
    }

    #[must_use]
    pub fn percent_gate_active(&self) -> bool {
        matches!(self.min_ready_executors_percent, Some(p) if p > 0)
    }

    #[must_use]
    pub fn any_executor_gate_active(&self) -> bool {
        self.count_gate_active() || self.percent_gate_active()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadinessError {
    InvalidReadyExecutorsPercent,
    MissingExecutorState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadinessGateOutcome {
    NotSet,
    Skipped,
    Pass,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadinessReport {
    pub components_ok: bool,
    pub count_gate: ReadinessGateOutcome,
    pub percent_gate: ReadinessGateOutcome,
    pub executor_state: Option<ExecutorReadiness>,
    pub overall_ok: bool,
    pub policy: ReadinessPolicy,
}

impl ReadinessReport {
    #[must_use]
    pub fn render(&self, verbose: bool) -> String {
        if !verbose {
            return if self.overall_ok {
                "ready".to_string()
            } else {
                "not ready".to_string()
            };
        }

        let mut out = String::new();
        let _ = writeln!(
            out,
            "{} components {}",
            marker(if self.components_ok {
                ReadinessGateOutcome::Pass
            } else {
                ReadinessGateOutcome::Fail
            }),
            if self.components_ok {
                "ok"
            } else {
                "not ready"
            }
        );

        if self.count_gate != ReadinessGateOutcome::NotSet
            || self.percent_gate != ReadinessGateOutcome::NotSet
        {
            let (ready, registered) = self
                .executor_state
                .map_or((0, 0), |s| (s.ready, s.registered));
            let pct: u128 = if registered == 0 {
                0
            } else {
                u128::try_from(ready)
                    .unwrap_or(u128::MAX)
                    .saturating_mul(100)
                    / u128::try_from(registered).unwrap_or(u128::MAX)
            };
            let mut detail =
                format!("executors: {ready}/{registered} ready ({pct}%, registered={registered}");
            if let Some(n) = self.policy.min_ready_executors {
                let _ = write!(detail, ", min={n}");
            }
            if let Some(p) = self.policy.min_ready_executors_percent {
                let _ = write!(detail, ", min_percent={p}%");
            }
            detail.push(')');

            let worst = match (self.count_gate, self.percent_gate) {
                (ReadinessGateOutcome::Fail, _) | (_, ReadinessGateOutcome::Fail) => {
                    ReadinessGateOutcome::Fail
                }
                (ReadinessGateOutcome::Pass, _) | (_, ReadinessGateOutcome::Pass) => {
                    ReadinessGateOutcome::Pass
                }
                _ => ReadinessGateOutcome::Skipped,
            };
            let _ = writeln!(out, "{} {detail}", marker(worst));
        }

        out.push_str(if self.overall_ok {
            "ready"
        } else {
            "not ready"
        });
        out
    }
}

fn marker(outcome: ReadinessGateOutcome) -> &'static str {
    match outcome {
        ReadinessGateOutcome::Pass => "[+]",
        ReadinessGateOutcome::Fail => "[-]",
        ReadinessGateOutcome::Skipped | ReadinessGateOutcome::NotSet => "[ ]",
    }
}

fn resolve_table_reference(table: TableReference) -> ResolvedTableReference {
    table.resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
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

    pub fn mark_initializing(&self, key: &ComponentKey) {
        self.update(key, &ComponentStatus::Initializing);
    }

    pub fn mark_ready(&self, key: &ComponentKey) {
        self.update(key, &ComponentStatus::Ready);
    }

    pub fn mark_disabled(&self, key: &ComponentKey) {
        self.update(key, &ComponentStatus::Disabled);
    }

    pub fn mark_refreshing(&self, key: &ComponentKey) {
        self.update(key, &ComponentStatus::Refreshing);
    }

    pub fn mark_shutting_down(&self, key: &ComponentKey) {
        self.update(key, &ComponentStatus::ShuttingDown);
    }

    pub fn mark_not_loaded(&self, key: &ComponentKey) {
        self.update(key, &ComponentStatus::NotLoaded);
    }

    pub fn mark_error(&self, key: &ComponentKey) {
        self.update(key, &ComponentStatus::error());
    }

    pub fn mark_error_with_message(&self, key: &ComponentKey, message: impl Into<String>) {
        self.update(key, &ComponentStatus::error_with_message(message));
    }

    pub fn update(&self, key: &ComponentKey, status: &ComponentStatus) {
        self.update_component_status(&key.full_name(), status.clone());
        Self::record_metrics(key, status);
    }

    /// Updates the status of a component and tracks if it has ever been ready.
    #[expect(clippy::needless_pass_by_value)]
    fn update_component_status(&self, component_name: &str, status: ComponentStatus) {
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

    fn record_metrics(key: &ComponentKey, status: &ComponentStatus) {
        let metric_value = status.discriminant();

        match key {
            ComponentKey::Catalog(name) => {
                runtime_metrics::catalogs::STATUS
                    .record(metric_value, &[KeyValue::new("catalog", name.clone())]);
            }
            ComponentKey::Dataset(name) => {
                runtime_metrics::datasets::STATUS
                    .record(metric_value, &[KeyValue::new("dataset", name.to_string())]);
            }
            ComponentKey::Model(name) => {
                runtime_metrics::models::STATUS
                    .record(metric_value, &[KeyValue::new("model", name.clone())]);
            }
            ComponentKey::Tool(name) => {
                runtime_metrics::tools::STATUS
                    .record(metric_value, &[KeyValue::new("tool", name.clone())]);
            }
            ComponentKey::ToolCatalog(name) => {
                runtime_metrics::tools::STATUS
                    .record(metric_value, &[KeyValue::new("tool_catalog", name.clone())]);
            }
            ComponentKey::Llm(name) => {
                runtime_metrics::llms::STATUS
                    .record(metric_value, &[KeyValue::new("model", name.clone())]);
            }
            ComponentKey::Embedding(name) => {
                runtime_metrics::embeddings::STATUS
                    .record(metric_value, &[KeyValue::new("model", name.clone())]);
            }
            ComponentKey::Reranker(name) => {
                runtime_metrics::rerankers::STATUS
                    .record(metric_value, &[KeyValue::new("model", name.clone())]);
            }
            ComponentKey::View(name) => {
                runtime_metrics::views::STATUS
                    .record(metric_value, &[KeyValue::new("view", name.to_string())]);
            }
            ComponentKey::Worker(name) => {
                runtime_metrics::workers::STATUS
                    .record(metric_value, &[KeyValue::new("worker", name.clone())]);
            }
            ComponentKey::Cluster(name) => {
                let status_value = match status {
                    ComponentStatus::Initializing | ComponentStatus::NotLoaded => 0,
                    ComponentStatus::Ready | ComponentStatus::Refreshing => 1,
                    ComponentStatus::Disabled | ComponentStatus::Error(_) => 2,
                    ComponentStatus::ShuttingDown => 3,
                };
                runtime_metrics::cluster::set_node_status(name, name, status_value);
            }
            ComponentKey::Internal(_) => {}
        }
    }

    pub fn update_catalog(&self, catalog_name: impl Into<String>, status: &ComponentStatus) {
        self.update(&ComponentKey::catalog(catalog_name), status);
    }

    pub fn update_dataset(&self, dataset: &TableReference, status: &ComponentStatus) {
        self.update(&ComponentKey::dataset(dataset), status);
    }

    pub fn update_model(&self, model_name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::model(model_name), status);
    }

    pub fn update_tool(&self, tool_name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::tool(tool_name), status);
    }

    pub fn update_tool_catalog(&self, catalog_name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::tool_catalog(catalog_name), status);
    }

    pub fn update_llm(&self, model_name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::llm(model_name), status);
    }

    pub fn update_embedding(&self, model_name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::embedding(model_name), status);
    }

    pub fn update_reranker(&self, model_name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::reranker(model_name), status);
    }
    pub fn update_view(&self, view_name: &TableReference, status: &ComponentStatus) {
        self.update(&ComponentKey::view(view_name), status);
    }

    /// Update the status of a worker
    pub fn update_worker(&self, name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::worker(name), status);
    }

    /// Update the status of a cluster node
    pub fn update_cluster(&self, node_name: &str, status: &ComponentStatus) {
        self.update(&ComponentKey::cluster(node_name), status);
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

    /// # Errors
    ///
    /// Returns [`ReadinessError::InvalidReadyExecutorsPercent`] if the policy's
    /// `min_ready_executors_percent` exceeds 100.
    pub fn readiness_report(
        &self,
        policy: ReadinessPolicy,
        executor_state: Option<ExecutorReadiness>,
    ) -> Result<ReadinessReport, ReadinessError> {
        if policy
            .min_ready_executors_percent
            .is_some_and(|percent| percent > 100)
        {
            return Err(ReadinessError::InvalidReadyExecutorsPercent);
        }

        if policy.any_executor_gate_active() && executor_state.is_none() {
            return Err(ReadinessError::MissingExecutorState);
        }

        let components_ok = self.is_ready();

        let count_gate = match (policy.min_ready_executors, executor_state) {
            (Some(0), _) => ReadinessGateOutcome::Skipped,
            (None, _) | (Some(_), None) => ReadinessGateOutcome::NotSet,
            (Some(n), Some(state)) => {
                if u64::try_from(state.ready).unwrap_or(u64::MAX) >= u64::from(n) {
                    ReadinessGateOutcome::Pass
                } else {
                    ReadinessGateOutcome::Fail
                }
            }
        };

        let percent_gate = match (policy.min_ready_executors_percent, executor_state) {
            (Some(0), _) => ReadinessGateOutcome::Skipped,
            (None, _) | (Some(_), None) => ReadinessGateOutcome::NotSet,
            (Some(_), Some(state)) if state.registered == 0 => ReadinessGateOutcome::Fail,
            (Some(p), Some(state)) => {
                let lhs = u128::try_from(state.ready)
                    .unwrap_or(u128::MAX)
                    .saturating_mul(100);
                let rhs = u128::from(p)
                    .saturating_mul(u128::try_from(state.registered).unwrap_or(u128::MAX));
                if lhs >= rhs {
                    ReadinessGateOutcome::Pass
                } else {
                    ReadinessGateOutcome::Fail
                }
            }
        };

        let overall_ok = components_ok
            && !matches!(count_gate, ReadinessGateOutcome::Fail)
            && !matches!(percent_gate, ReadinessGateOutcome::Fail);

        Ok(ReadinessReport {
            components_ok,
            count_gate,
            percent_gate,
            executor_state,
            overall_ok,
            policy,
        })
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

    #[must_use]
    pub fn get_all_component_statuses(&self) -> HashMap<ComponentKey, ComponentStatus> {
        self.get_all_statuses()
            .into_iter()
            .map(|(name, status)| (ComponentKey::parse(&name), status))
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
    pub fn get_table_statuses(&self) -> HashMap<TableReference, ComponentStatus> {
        let mut statuses = self.get_dataset_statuses();
        statuses.extend(self.get_view_statuses());
        statuses
    }

    #[must_use]
    pub fn get_component_status_by_key(&self, key: &ComponentKey) -> Option<ComponentStatus> {
        self.get_component_status(&key.full_name())
    }

    #[must_use]
    pub fn find_dataset_statuses_matching_resolved(
        &self,
        resolved: &ResolvedTableReference,
    ) -> Vec<(TableReference, ComponentStatus)> {
        self.get_dataset_statuses()
            .into_iter()
            .filter(|(key, _)| resolve_table_reference(key.clone()) == *resolved)
            .collect()
    }

    #[must_use]
    pub fn dataset_ready_update_targets(
        &self,
        resolved: &ResolvedTableReference,
    ) -> Option<Vec<TableReference>> {
        let matching = self.find_dataset_statuses_matching_resolved(resolved);
        let pending: Vec<TableReference> = matching
            .iter()
            .filter(|(_, status)| !matches!(status, ComponentStatus::Ready))
            .map(|(key, _)| key.clone())
            .collect();

        if pending.is_empty() && !matching.is_empty() {
            None
        } else {
            Some(pending)
        }
    }

    #[must_use]
    pub fn mark_resolved_dataset_ready(&self, resolved: &ResolvedTableReference) -> bool {
        let Some(pending) = self.dataset_ready_update_targets(resolved) else {
            return false;
        };

        if pending.is_empty() {
            let canonical = TableReference::full(
                resolved.catalog.to_string(),
                resolved.schema.to_string(),
                resolved.table.to_string(),
            );
            self.mark_ready(&ComponentKey::dataset(&canonical));
        } else {
            for key in pending {
                self.mark_ready(&ComponentKey::dataset(&key));
            }
        }

        true
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

    fn first_unready_dependency(
        &self,
        dependent_tables: &[ResolvedTableReference],
    ) -> Option<ResolvedTableReference> {
        let statuses = self
            .get_table_statuses()
            .into_iter()
            .map(|(key, value)| (resolve_table_reference(key), value))
            .collect::<HashMap<_, _>>();
        let catalog_statuses = self.get_catalog_statuses();

        dependent_tables.iter().find_map(|dependent_table| {
            let is_ready = if let Some(status) = statuses.get(dependent_table) {
                status == &ComponentStatus::Ready
            } else {
                let catalog = dependent_table.catalog.as_ref();
                catalog_statuses.get(catalog) == Some(&ComponentStatus::Ready)
            };

            (!is_ready).then(|| dependent_table.clone())
        })
    }

    /// Internal helper to wait for a component to become ready.
    async fn wait_for_component_ready(&self, key: &ComponentKey) {
        let mut receiver = self.get_or_create_notifier(&key.full_name());

        loop {
            // Check current value (handles already-ready case)
            if *receiver.borrow() == ComponentStatus::Ready {
                return;
            }

            tokio::select! {
                result = receiver.changed() => {
                    if result.is_err() { return; }
                }
                () = self.shutdown_token.cancelled() => return,
            }
        }
    }

    /// Waits for a component to leave the `Initializing` state — used by
    /// callers that only need the component registered, not fully ready.
    async fn wait_for_component_registered(&self, key: &ComponentKey) {
        let mut receiver = self.get_or_create_notifier(&key.full_name());

        loop {
            if !matches!(*receiver.borrow(), ComponentStatus::Initializing) {
                return;
            }
            tokio::select! {
                result = receiver.changed() => {
                    if result.is_err() { return; }
                }
                () = self.shutdown_token.cancelled() => return,
            }
        }
    }

    /// Waits for a dataset to become ready.
    pub async fn wait_for_dataset_ready(&self, dataset: &TableReference) {
        self.wait_for_component_ready(&ComponentKey::dataset(dataset))
            .await;
    }

    /// Waits for a dataset to be registered (any status other than
    /// `Initializing`). Useful when the caller only needs the table
    /// provider to exist, not for the dataset to be fully loaded — e.g.
    /// scheduler-side partition discovery, where waiting for `Ready`
    /// would deadlock because `Ready` is gated on executor data loads.
    pub async fn wait_for_dataset_registered(&self, dataset: &TableReference) {
        self.wait_for_component_registered(&ComponentKey::dataset(dataset))
            .await;
    }

    /// Waits for a model to become ready.
    pub async fn wait_for_model_ready(&self, model_name: &str) {
        self.wait_for_component_ready(&ComponentKey::model(model_name))
            .await;
    }

    /// Waits for a catalog to become ready.
    pub async fn wait_for_catalog_ready(&self, catalog_name: &str) {
        self.wait_for_component_ready(&ComponentKey::catalog(catalog_name))
            .await;
    }

    /// Waits for a tool to become ready.
    pub async fn wait_for_tool_ready(&self, tool_name: &str) {
        self.wait_for_component_ready(&ComponentKey::tool(tool_name))
            .await;
    }

    /// Waits for a tool catalog to become ready.
    pub async fn wait_for_tool_catalog_ready(&self, catalog_name: &str) {
        self.wait_for_component_ready(&ComponentKey::tool_catalog(catalog_name))
            .await;
    }

    /// Waits for an LLM to become ready.
    pub async fn wait_for_llm_ready(&self, model_name: &str) {
        self.wait_for_component_ready(&ComponentKey::llm(model_name))
            .await;
    }

    /// Waits for an embedding model to become ready.
    pub async fn wait_for_embedding_ready(&self, model_name: &str) {
        self.wait_for_component_ready(&ComponentKey::embedding(model_name))
            .await;
    }

    /// Waits for a view to become ready.
    pub async fn wait_for_view_ready(&self, view_name: &TableReference) {
        self.wait_for_component_ready(&ComponentKey::view(view_name))
            .await;
    }

    /// Waits for a worker to become ready.
    pub async fn wait_for_worker_ready(&self, worker_name: &str) {
        self.wait_for_component_ready(&ComponentKey::worker(worker_name))
            .await;
    }

    /// Waits for a cluster node to become ready.
    pub async fn wait_for_cluster_ready(&self, node_name: &str) {
        self.wait_for_component_ready(&ComponentKey::cluster(node_name))
            .await;
    }

    pub async fn wait_until_dependent_tables_ready(&self, dependent_tables: &[TableReference]) {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(None)
            .max_duration(Some(Duration::from_secs(10)))
            .build();
        let dependent_tables = dependent_tables
            .iter()
            .cloned()
            .map(resolve_table_reference)
            .collect::<Vec<_>>();

        tokio::select! {
            _ = retry(retry_strategy, || async {
                if self.first_unready_dependency(&dependent_tables).is_some() {
                    return Err(RetryError::transient(()));
                }
                Ok(())
            }) => {}
            () = self.shutdown_token.cancelled() => {}
        }
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
            tokio::select! {
                () = tokio::time::sleep(POLL_INTERVAL) => {}
                () = self.shutdown_token.cancelled() => return,
            }
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
