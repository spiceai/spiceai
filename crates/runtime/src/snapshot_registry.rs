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

//! Registry for managing `SnapshotManager` instances associated with accelerated datasets.
//!
//! This module provides a centralized registry that allows the runtime to track and access
//! snapshot managers for datasets that have snapshots enabled. It is used by the HTTP API
//! to provide snapshot management operations like listing, rollback, etc.

use runtime_acceleration::snapshot::SnapshotManager;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// A registry that stores `SnapshotManager` instances keyed by dataset name.
#[derive(Debug, Default)]
pub struct SnapshotManagerRegistry {
    managers: RwLock<HashMap<String, Arc<SnapshotManager>>>,
}

impl SnapshotManagerRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            managers: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a snapshot manager for a dataset.
    ///
    /// If a manager already exists for the dataset, it will be replaced.
    pub async fn register(&self, dataset_name: String, manager: Arc<SnapshotManager>) {
        let mut managers = self.managers.write().await;
        managers.insert(dataset_name, manager);
    }

    /// Removes the snapshot manager for a dataset.
    pub async fn deregister(&self, dataset_name: &str) {
        let mut managers = self.managers.write().await;
        managers.remove(dataset_name);
    }

    /// Gets the snapshot manager for a dataset, if one exists.
    pub async fn get(&self, dataset_name: &str) -> Option<Arc<SnapshotManager>> {
        let managers = self.managers.read().await;
        managers.get(dataset_name).cloned()
    }

    /// Returns a list of all dataset names that have snapshot managers registered.
    pub async fn list_datasets(&self) -> Vec<String> {
        let managers = self.managers.read().await;
        managers.keys().cloned().collect()
    }
}
