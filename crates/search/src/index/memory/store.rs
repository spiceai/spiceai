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
    stored_schema: SchemaRef,
    batches: Vec<StoredBatch>,
}

impl MemoryVectorStore {
    pub(crate) fn new(stored_schema: SchemaRef) -> Self {
        Self {
            stored_schema,
            batches: Vec::new(),
        }
    }

    pub(crate) fn stored_schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.stored_schema)
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

        let new_keys: HashSet<&str> = keys.iter().map(String::as_str).collect();
        let mut retained = Vec::with_capacity(self.batches.len() + 1);
        for stored in self.batches.drain(..) {
            if !stored.keys.iter().any(|k| new_keys.contains(k.as_str())) {
                // No overlap — keep the batch untouched (zero-copy).
                retained.push(stored);
                continue;
            }
            let mask: BooleanArray = stored
                .keys
                .iter()
                .map(|k| Some(!new_keys.contains(k.as_str())))
                .collect();
            let filtered = filter_record_batch(&stored.batch, &mask)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            if filtered.num_rows() == 0 {
                continue;
            }
            let kept_keys = stored
                .keys
                .into_iter()
                .filter(|k| !new_keys.contains(k.as_str()))
                .collect::<Vec<_>>();
            retained.push(StoredBatch {
                batch: filtered,
                keys: kept_keys,
            });
        }
        if batch.num_rows() > 0 {
            retained.push(StoredBatch { batch, keys });
        }
        self.batches = retained;
        Ok(())
    }

    /// Current contents as batches conforming to [`Self::stored_schema`].
    /// Cheap: Arrow buffers are shared, not copied.
    pub(crate) fn batches(&self) -> Vec<RecordBatch> {
        self.batches.iter().map(|s| s.batch.clone()).collect()
    }

    /// Total number of stored rows.
    pub(crate) fn len(&self) -> usize {
        self.batches.iter().map(|s| s.batch.num_rows()).sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.batches.iter().all(|s| s.batch.num_rows() == 0)
    }
}
