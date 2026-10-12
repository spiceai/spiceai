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

//! The primary-key existence filter a first load builds while it writes.
//!
//! A load into an empty table writes every key the table will then hold: the
//! copies of a repeated key that the post-write resolution folds away share
//! their key with the copy it keeps. A filter of every written key therefore
//! has no false negatives, and stands in for the one a keyset rebuild would
//! otherwise scan the whole table for (#15071).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::RecordBatch;
use parking_lot::Mutex;
use vortex_datafusion::VortexWriteObserver;

use super::Result;
use super::pk_index::{PkBloom, min_pk_keyset_entry_bytes};
use super::table::CayenneTableProvider;
use crate::row_converter::RowConverter;

pub(super) struct FirstLoadPkObserver {
    table: String,
    indices: Vec<usize>,
    converter: RowConverter,
    budget: usize,
    /// `None` when the filter cannot be built from the encode shards (a legacy
    /// layout); the load then leaves the index to the post-load warm.
    block_mask: Option<u64>,
    bloom: Mutex<PkBloom>,
    /// Set when any written batch could not be added: the filter would then
    /// miss its keys, so it is never installed.
    incomplete: AtomicBool,
}

impl std::fmt::Debug for FirstLoadPkObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirstLoadPkObserver")
            .field("table", &self.table)
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl FirstLoadPkObserver {
    pub(super) fn new(table: &CayenneTableProvider, indices: Vec<usize>) -> Result<Self> {
        // The dimensions the serial rebuild gives the table-wide filter when it
        // degrades (`BoundedShardedPkIndexBuilder` at one shard), so the
        // installed filter has the false-positive rate a rebuilt one would.
        let budget = table.effective_single_keyset_budget();
        let bloom = PkBloom::with_byte_budget(budget);
        Ok(Self {
            table: table.table_name().to_string(),
            converter: table.build_pk_converter(&indices)?,
            indices,
            budget,
            block_mask: bloom.split_block_mask(),
            bloom: Mutex::new(bloom),
            incomplete: AtomicBool::new(false),
        })
    }

    /// The filter to install, once every shard writer has completed: `None`
    /// when a batch could not be added, or when an exact keyset of `live_rows`
    /// keys would fit the budget, which keeps a small table's exact index (built
    /// by the post-load warm) exactly as before.
    pub(super) fn finish(&self, live_rows: u64) -> Option<PkBloom> {
        if self.incomplete.load(Ordering::Acquire) || self.block_mask.is_none() {
            return None;
        }
        // A lower bound on the exact keyset's size (encoded key bytes excluded),
        // so a borderline table keeps the exact path.
        if live_rows == 0
            || u128::from(live_rows) * min_pk_keyset_entry_bytes() as u128 <= self.budget as u128
        {
            return None;
        }
        Some(std::mem::replace(
            &mut *self.bloom.lock(),
            PkBloom::with_byte_budget(0),
        ))
    }

    fn mark_incomplete(&self, cause: &dyn std::fmt::Display) {
        if !self.incomplete.swap(true, Ordering::AcqRel) {
            tracing::debug!(
                table = self.table.as_str(),
                %cause,
                "The first load could not record its primary keys; the index is built after the load instead"
            );
        }
    }
}

impl VortexWriteObserver for FirstLoadPkObserver {
    fn batch_written(&self, _path: &object_store::path::Path, _position: u64, batch: &RecordBatch) {
        if self.incomplete.load(Ordering::Acquire) {
            return;
        }
        let Some(block_mask) = self.block_mask else {
            return;
        };
        let columns: Vec<_> = self
            .indices
            .iter()
            .map(|index| Arc::clone(batch.column(*index)))
            .collect();
        // The converter and column order the rebuild and the validation probe
        // use. Encoding and hashing run on the calling encode shard, outside the
        // lock, which covers only setting the bits.
        let rows = match self.converter.convert_columns(&columns) {
            Ok(rows) => rows,
            Err(error) => {
                self.mark_incomplete(&error);
                return;
            }
        };
        let located: Vec<(usize, [u32; 8])> = rows
            .iter()
            .map(|row| PkBloom::locate_split_block(block_mask, row.as_ref()))
            .collect();
        if !self.bloom.lock().insert_located(block_mask, &located) {
            self.mark_incomplete(&"the filter changed layout during the load");
        }
    }
}
