/*
Copyright 2024 The Spice.ai OSS Authors

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
    sync::{Arc, LazyLock, RwLock},
};

use datafusion::sql::TableReference;
use opentelemetry::Key;
use serde::{Deserialize, Serialize};

use crate::metrics;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub enum ComponentStatus {
    Initializing = 1,
    Ready = 2,
    Disabled = 3,
    Error = 4,
    Refreshing = 5,
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

/// Stores the current status of all components.
static COMPONENT_STATUSES: LazyLock<Arc<RwLock<HashMap<String, ComponentStatus>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Tracks components that have been in the Ready state at least once.
static EVER_READY_COMPONENTS: LazyLock<Arc<RwLock<HashSet<String>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashSet::new())));

/// Updates the status of a component and tracks if it has ever been ready.
fn update_component_status(component_name: String, status: ComponentStatus) {
    let mut statuses = match COMPONENT_STATUSES.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    statuses.insert(component_name.clone(), status);

    if status == ComponentStatus::Ready {
        let mut ever_ready = match EVER_READY_COMPONENTS.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        ever_ready.insert(component_name);
    }
}

pub fn update_catalog(catalog_name: impl Into<String>, status: ComponentStatus) {
    let catalog_name = catalog_name.into();
    update_component_status(format!("catalog:{catalog_name}"), status);
    metrics::catalogs::STATUS.record(
        status as u64,
        &[Key::from_static_str("catalog").string(catalog_name)],
    );
}

pub fn update_dataset(dataset: &TableReference, status: ComponentStatus) {
    let ds_name = dataset.to_string();
    update_component_status(format!("dataset:{ds_name}"), status);
    metrics::datasets::STATUS.record(
        status as u64,
        &[Key::from_static_str("dataset").string(ds_name)],
    );
}

pub fn update_model(model_name: &str, status: ComponentStatus) {
    let model_name = model_name.to_string();
    update_component_status(format!("model:{model_name}"), status);
    metrics::models::STATUS.record(
        status as u64,
        &[Key::from_static_str("model").string(model_name)],
    );
}

pub fn update_tool(tool_name: &str, status: ComponentStatus) {
    let tool_name = tool_name.to_string();
    update_component_status(format!("tool:{tool_name}"), status);
    metrics::tools::STATUS.record(
        status as u64,
        &[Key::from_static_str("tool").string(tool_name)],
    );
}

pub fn update_llm(model_name: &str, status: ComponentStatus) {
    let model_name = model_name.to_string();
    update_component_status(format!("llm:{model_name}"), status);
    metrics::llms::STATUS.record(
        status as u64,
        &[Key::from_static_str("model").string(model_name)],
    );
}

pub fn update_embedding(model_name: &str, status: ComponentStatus) {
    let model_name = model_name.to_string();
    update_component_status(format!("embedding:{model_name}"), status);
    metrics::embeddings::STATUS.record(
        status as u64,
        &[Key::from_static_str("model").string(model_name)],
    );
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
pub fn is_ready() -> bool {
    let statuses = match COMPONENT_STATUSES.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let ever_ready = match EVER_READY_COMPONENTS.read() {
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
