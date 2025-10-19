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

//! Snapshot management for Pepper.

use super::catalog::{CatalogResult, MetadataCatalog};
use super::metadata::Snapshot;
use std::sync::Arc;

/// Manager for snapshot operations.
pub struct SnapshotManager {
    catalog: Arc<dyn MetadataCatalog>,
}

impl SnapshotManager {
    /// Create a new snapshot manager.
    pub fn new(catalog: Arc<dyn MetadataCatalog>) -> Self {
        Self { catalog }
    }

    /// Create a new snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be created.
    pub async fn create_snapshot(&self) -> CatalogResult<i64> {
        self.catalog.create_snapshot().await
    }

    /// Get the current snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the current snapshot cannot be retrieved.
    pub async fn current_snapshot(&self) -> CatalogResult<Snapshot> {
        self.catalog.get_current_snapshot().await
    }

    /// Get a specific snapshot by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be retrieved.
    pub async fn get_snapshot(&self, snapshot_id: i64) -> CatalogResult<Snapshot> {
        self.catalog.get_snapshot(snapshot_id).await
    }

    /// Create a snapshot with a specific action.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction fails.
    pub async fn snapshot_with_action<F, T>(&self, action: F) -> CatalogResult<T>
    where
        F: FnOnce() -> CatalogResult<T>,
    {
        self.catalog.begin_transaction().await?;

        match action() {
            Ok(result) => {
                self.catalog.commit_transaction().await?;
                Ok(result)
            }
            Err(e) => {
                let _ = self.catalog.rollback_transaction().await;
                Err(e)
            }
        }
    }
}
