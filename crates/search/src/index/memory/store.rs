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

//! Arrow-native row store backing [`super::MemoryVectorIndex`].
//!
//! Rows are held as [`RecordBatch`]es conforming to a fixed stored schema
//! (primary-key fields + metadata fields + the embedding column). Each batch
//! keeps the formatted primary key of every row alongside it so upserts can
//! drop superseded rows with a single Arrow `filter` pass — zero-copy for
//! untouched rows.

use std::collections::HashSet;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;

/// A stored batch and the formatted primary key of each of its rows.
///
/// `keys.len() == batch.num_rows()`; every key is valid (rows with null
/// primary keys are rejected before storage).
#[derive(Debug, Clone)]
struct StoredBatch {
    batch: RecordBatch,
    keys: Vec<String>,
}

/// In-memory, MemTable-equivalent store of indexed rows.
///
/// Memory growth is unbounded by design: the store is a caller-managed
/// building block, and eviction/compaction policy belongs to the caller.
#[derive(Debug)]
pub(crate) struct MemoryVectorStore {
    /// Primary-key fields + metadata fields + `{search_column}_embedding`,
    /// alphabetically sorted by name (the same order the index's `write()`
    /// output uses, as required by `VectorScanTableProvider`).
    pub(crate) stored_schema: SchemaRef,
    batches: Vec<StoredBatch>,
    /// Rows written since a replace window opened, held aside from [`Self::batches`].
    ///
    /// `None` outside a replace window, which is every append and every CDC write. While it
    /// is `Some`, writes land here and reads still see [`Self::batches`], so the wipe and the
    /// repopulation of a full refresh become visible together at
    /// [`Self::commit_replace_window`] rather than a searcher observing an empty index for
    /// the length of the refresh.
    replacement: Option<Vec<StoredBatch>>,
}

impl MemoryVectorStore {
    pub(crate) fn new(stored_schema: SchemaRef) -> Self {
        Self {
            stored_schema,
            batches: Vec::new(),
            replacement: None,
        }
    }

    /// Open a replace window: stage subsequent writes instead of adding them to the rows
    /// readers see.
    ///
    /// A replacing write reproduces the table's whole contents, so every row this store
    /// already holds is either re-sent inside the window or belongs to a row the source
    /// dropped. Discards anything staged by a window that was abandoned without either
    /// terminator running, so it cannot be swept into this one.
    pub(crate) fn begin_replace_window(&mut self) {
        self.replacement = Some(Vec::new());
    }

    /// Close a replace window by publishing what it staged, replacing the previous contents
    /// in one step. A no-op when no window is open — the terminators run after an append too.
    pub(crate) fn commit_replace_window(&mut self) {
        if let Some(staged) = self.replacement.take() {
            self.batches = staged;
        }
    }

    /// Close a replace window by discarding what it staged, leaving the previous contents
    /// readable. A no-op when no window is open.
    pub(crate) fn abandon_replace_window(&mut self) {
        self.replacement = None;
    }

    /// The batches a write acts on: the staged set inside a replace window, else the rows
    /// readers see.
    fn write_target(&mut self) -> &mut Vec<StoredBatch> {
        self.replacement.as_mut().unwrap_or(&mut self.batches)
    }

    /// Replace-on-rewrite insert: drops any stored row whose formatted primary
    /// key appears in `keys`, then appends the new batch. `keys` must be
    /// parallel to `batch` rows.
    pub(crate) fn upsert(
        &mut self,
        batch: RecordBatch,
        keys: Vec<String>,
    ) -> Result<(), DataFusionError> {
        debug_assert_eq!(batch.num_rows(), keys.len());

        self.delete_by_keys(&keys)?;
        if batch.num_rows() > 0 {
            self.write_target().push(StoredBatch { batch, keys });
        }
        Ok(())
    }

    /// Remove every stored row whose formatted primary key appears in `keys`.
    pub(crate) fn delete_by_keys(&mut self, keys: &[String]) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }

        let delete_keys: HashSet<&str> = keys.iter().map(String::as_str).collect();
        let target = self.write_target();
        let mut retained = Vec::with_capacity(target.len() + 1);
        for stored in target.drain(..) {
            if !stored
                .keys
                .iter()
                .any(|key| delete_keys.contains(key.as_str()))
            {
                // No overlap — keep the batch untouched (zero-copy).
                retained.push(stored);
                continue;
            }
            let mask: BooleanArray = stored
                .keys
                .iter()
                .map(|key| Some(!delete_keys.contains(key.as_str())))
                .collect();
            let filtered = filter_record_batch(&stored.batch, &mask)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            if filtered.num_rows() == 0 {
                continue;
            }
            let kept_keys = stored
                .keys
                .into_iter()
                .filter(|key| !delete_keys.contains(key.as_str()))
                .collect::<Vec<_>>();
            retained.push(StoredBatch {
                batch: filtered,
                keys: kept_keys,
            });
        }
        *self.write_target() = retained;
        Ok(())
    }

    /// Current contents as batches conforming to [`Self::stored_schema`].
    /// Cheap: Arrow buffers are shared, not copied.
    ///
    /// Always the published rows. Rows staged by an open replace window are deliberately
    /// invisible here: a query during a full refresh reads the previous contents rather than
    /// the partially rebuilt ones.
    pub(crate) fn batches(&self) -> Vec<RecordBatch> {
        self.batches.iter().map(|s| s.batch.clone()).collect()
    }
}
