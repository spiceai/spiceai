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
    fmt::Display,
    sync::{Arc, RwLock},
};

use datafusion::sql::TableReference;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};

use crate::metrics;

/// Represents the status of a component (e.g. dataset, model, etc).
#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ComponentStatus {
    /// The component is initializing and not yet ready
    Initializing,

    /// The component is ready to accept connections
    Ready,

    /// The component is disabled and not running
    Disabled,

    /// An error occurred in the component
    Error,

    /// The component is in the process of refreshing its state
    Refreshing,
}

impl Display for ComponentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentStatus::Initializing => write!(f, "Initializing"),
            ComponentStatus::Ready => write!(f, "Ready"),
            ComponentStatus::Disabled => write!(f, "Disabled"),
            ComponentStatus::Error => write!(f, "Error"),
            ComponentStatus::Refreshing => write!(f, "Refreshing"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeStatus {
    /// Stores the current status of all components.
    statuses: Arc<RwLock<HashMap<String, ComponentStatus>>>,
    /// Tracks components that have been in the Ready state at least once.
    ever_ready_components: Arc<RwLock<HashSet<String>>>,
}

impl RuntimeStatus {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            statuses: Arc::new(RwLock::new(HashMap::new())),
            ever_ready_components: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Updates the status of a component and tracks if it has ever been ready.
    fn update_component_status(&self, component_name: String, status: ComponentStatus) {
        let mut statuses = match self.statuses.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        statuses.insert(component_name.clone(), status);

        if status == ComponentStatus::Ready {
            let mut ever_ready = match self.ever_ready_components.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            ever_ready.insert(component_name);
        }
    }

    pub fn update_catalog(&self, catalog_name: impl Into<String>, status: ComponentStatus) {
        let catalog_name = catalog_name.into();
        self.update_component_status(format!("catalog:{catalog_name}"), status);
        metrics::catalogs::STATUS.record(status as u64, &[KeyValue::new("catalog", catalog_name)]);
    }

    pub fn update_dataset(&self, dataset: &TableReference, status: ComponentStatus) {
        let ds_name = dataset.to_string();
        self.update_component_status(format!("dataset:{ds_name}"), status);
        metrics::datasets::STATUS.record(status as u64, &[KeyValue::new("dataset", ds_name)]);
    }

    pub fn update_model(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        self.update_component_status(format!("model:{model_name}"), status);
        metrics::models::STATUS.record(status as u64, &[KeyValue::new("model", model_name)]);
    }

    pub fn update_tool(&self, tool_name: &str, status: ComponentStatus) {
        let tool_name = tool_name.to_string();
        self.update_component_status(format!("tool:{tool_name}"), status);
        metrics::tools::STATUS.record(status as u64, &[KeyValue::new("tool", tool_name)]);
    }

    pub fn update_tool_catalog(&self, catalog_name: &str, status: ComponentStatus) {
        let name = catalog_name.to_string();
        self.update_component_status(format!("tool_catalog:{name}"), status);
        metrics::tools::STATUS.record(status as u64, &[KeyValue::new("tool_catalog", name)]);
    }

    pub fn update_llm(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        self.update_component_status(format!("llm:{model_name}"), status);
        metrics::llms::STATUS.record(status as u64, &[KeyValue::new("model", model_name)]);
    }

    pub fn update_embedding(&self, model_name: &str, status: ComponentStatus) {
        let model_name = model_name.to_string();
        self.update_component_status(format!("embedding:{model_name}"), status);
        metrics::embeddings::STATUS.record(status as u64, &[KeyValue::new("model", model_name)]);
    }
    pub fn update_view(&self, view_name: &TableReference, status: ComponentStatus) {
        let view_name = view_name.to_string();
        self.update_component_status(format!("view:{view_name}"), status);
        metrics::views::STATUS.record(status as u64, &[KeyValue::new("view", view_name)]);
    }

    /// Checks if all registered components have been ready at least once.
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
    #[must_use]
    pub fn is_ready(&self) -> bool {
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
        let statuses = match self.statuses.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        statuses
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("model:")
                    .map(|model_name| (model_name.to_string(), *v))
            })
            .collect()
    }
}
