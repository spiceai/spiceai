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

//! Where an append refresh keeps the source commit it last loaded.

use std::sync::Arc;

use datafusion::common::TableReference;
use parking_lot::RwLock;
use runtime_checkpoint_api::{BlobCheckpointStore, CheckpointError};

/// The sidecar table the cursor is checkpointed in, one row per dataset.
pub const APPEND_CURSOR_TABLE: &str = "spice_sys_append_cursor";

/// Keeps one dataset's append cursor, text only its source reads, across refreshes.
///
/// A file-backed acceleration checkpoints it in the accelerator's sidecar, so it
/// survives a restart with the data it describes. An acceleration whose engine hosts
/// no sidecar (memory mode, Arrow) keeps it in process memory: its data does not
/// survive a restart either, so the next start takes an initial load with or without
/// the cursor.
#[derive(Clone)]
pub enum AppendCursorStore {
    Persisted(Arc<dyn BlobCheckpointStore>),
    InMemory(Arc<RwLock<Option<String>>>),
}

impl std::fmt::Debug for AppendCursorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Persisted(_) => f.write_str("AppendCursorStore::Persisted"),
            Self::InMemory(_) => f.write_str("AppendCursorStore::InMemory"),
        }
    }
}

impl AppendCursorStore {
    #[must_use]
    pub fn in_memory() -> Self {
        Self::InMemory(Arc::new(RwLock::new(None)))
    }

    #[must_use]
    pub fn persisted(store: Arc<dyn BlobCheckpointStore>) -> Self {
        Self::Persisted(store)
    }

    /// The saved cursor, or `None` when none is saved or the store cannot be read; the
    /// refresh then takes an initial load, which is always safe.
    pub async fn load(&self, dataset: &TableReference) -> Option<String> {
        match self {
            Self::InMemory(cursor) => cursor.read().clone(),
            Self::Persisted(store) => match store.get().await {
                Ok(Some(checkpoint)) => Some(checkpoint.data),
                Ok(None) => None,
                Err(e) => {
                    tracing::debug!(
                        "Dataset '{dataset}' could not read its saved append cursor and reads the source by time: {e}"
                    );
                    None
                }
            },
        }
    }

    /// Saves `cursor` as the commit the acceleration now holds.
    ///
    /// # Errors
    ///
    /// Returns the store's error when the checkpoint cannot be written.
    pub async fn save(&self, cursor: &str) -> Result<(), CheckpointError> {
        match self {
            Self::InMemory(slot) => {
                *slot.write() = Some(cursor.to_string());
                Ok(())
            }
            Self::Persisted(store) => store.upsert(cursor).await,
        }
    }

    /// Discards the saved cursor, so the next refresh takes an initial load.
    ///
    /// # Errors
    ///
    /// Returns the store's error when the checkpoint cannot be removed.
    pub async fn clear(&self) -> Result<(), CheckpointError> {
        match self {
            Self::InMemory(slot) => {
                *slot.write() = None;
                Ok(())
            }
            Self::Persisted(store) => store.delete().await,
        }
    }
}
