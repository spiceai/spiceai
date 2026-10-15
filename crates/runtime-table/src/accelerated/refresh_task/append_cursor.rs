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
use serde::{Deserialize, Serialize};

/// The sidecar table the cursor is checkpointed in, one row per dataset.
pub const APPEND_CURSOR_TABLE: &str = "spice_sys_append_cursor";

/// What the store holds for one dataset: the commit whose rows are written, and the
/// commit a write is in flight for.
///
/// `pending` is saved before a write and cleared by `committed` after it. A refresh
/// that finds `pending` set, and different from `committed`, knows a write for that
/// commit may have landed in part: a crash between the write and the save. It then
/// compares what it re-reads with the stored rows, which it otherwise does not do for a
/// dataset without a primary key, since the rows a commit added are new rows and a
/// legitimate duplicate among them must be kept.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedCursor {
    version: u32,
    pub committed: Option<String>,
    pub pending: Option<String>,
}

const SAVED_CURSOR_VERSION: u32 = 1;

impl SavedCursor {
    /// Whether a write for a commit after `committed` may have landed in part.
    #[must_use]
    pub fn write_may_have_landed(&self) -> bool {
        self.pending.is_some() && self.pending != self.committed
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|e| unreachable!("a cursor of plain fields serializes: {e}"))
    }

    fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str::<Self>(json)
            .ok()
            .filter(|saved| saved.version == SAVED_CURSOR_VERSION)
    }
}

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
    InMemory(Arc<RwLock<SavedCursor>>),
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
        Self::InMemory(Arc::new(RwLock::new(SavedCursor::default())))
    }

    #[must_use]
    pub fn persisted(store: Arc<dyn BlobCheckpointStore>) -> Self {
        Self::Persisted(store)
    }

    /// The saved cursor; empty when none is saved or the store cannot be read, which
    /// makes the refresh take an initial load, always safe.
    pub async fn load(&self, dataset: &TableReference) -> SavedCursor {
        match self {
            Self::InMemory(cursor) => cursor.read().clone(),
            Self::Persisted(store) => match store.get().await {
                Ok(Some(checkpoint)) => SavedCursor::from_json(&checkpoint.data).unwrap_or_else(|| {
                    tracing::debug!(
                        "Dataset '{dataset}' ignores its saved append cursor, which it cannot read, and reads the source by time"
                    );
                    SavedCursor::default()
                }),
                Ok(None) => SavedCursor::default(),
                Err(e) => {
                    tracing::debug!(
                        "Dataset '{dataset}' could not read its saved append cursor and reads the source by time: {e}"
                    );
                    SavedCursor::default()
                }
            },
        }
    }

    /// Records that a write for `cursor` is about to start.
    ///
    /// # Errors
    ///
    /// Returns the store's error when the checkpoint cannot be written.
    pub async fn save_pending(&self, cursor: &str) -> Result<(), CheckpointError> {
        self.update(|saved| saved.pending = Some(cursor.to_string()))
            .await
    }

    /// Records that the rows of `cursor` are written.
    ///
    /// # Errors
    ///
    /// Returns the store's error when the checkpoint cannot be written.
    pub async fn save_committed(&self, cursor: &str) -> Result<(), CheckpointError> {
        self.update(|saved| {
            saved.committed = Some(cursor.to_string());
            saved.pending = None;
        })
        .await
    }

    async fn update(&self, change: impl FnOnce(&mut SavedCursor)) -> Result<(), CheckpointError> {
        match self {
            Self::InMemory(slot) => {
                change(&mut slot.write());
                Ok(())
            }
            Self::Persisted(store) => {
                let mut saved = store
                    .get()
                    .await?
                    .and_then(|checkpoint| SavedCursor::from_json(&checkpoint.data))
                    .unwrap_or_default();
                saved.version = SAVED_CURSOR_VERSION;
                change(&mut saved);
                store.upsert(&saved.to_json()).await
            }
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
                *slot.write() = SavedCursor::default();
                Ok(())
            }
            Self::Persisted(store) => store.delete().await,
        }
    }
}
